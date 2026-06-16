#include "sqlite3_driver.hpp"
#include "db_error.hpp"
#include "sql_placeholders.hpp"

#include <sqlite3.h>
#include <qbuem/core/task.hpp>
#include <qbuem/db/driver.hpp>
#include <qbuem/db/value.hpp>

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <string_view>
#include <vector>

using namespace qbuem;
using namespace qbuem::db;

namespace qbuem_routine {
namespace {

// $N → ? placeholder conversion: shared, string-literal-aware implementation.
using qbuem_routine::db_detail::convert_placeholders;

// Map a busy/locked step result to a transient (retryable) DbError, else the
// given fallback. Lets with_transaction() retry contended writes.
static DbError sqlite_transient_or(int rc, DbError fallback) {
    const int primary = rc & 0xFF; // strip extended-result-code bits
    if (primary == SQLITE_BUSY || primary == SQLITE_LOCKED) return DbError::LockTimeout;
    return fallback;
}

// ─── CellData — stores a single column value ─────────────────────────────────

struct CellData {
    Value::Type type = Value::Type::Null;
    int64_t     i64  = 0;
    double      f64  = 0.0;
    std::string text; // holds both TEXT and BLOB (owns the bytes)
};

// ─── Row ─────────────────────────────────────────────────────────────────────

class SqliteRow final : public IRow {
public:
    SqliteRow(std::shared_ptr<std::vector<std::string>> col_names,
              std::vector<CellData>                     cells)
        : col_names_(std::move(col_names))
        , cells_(std::move(cells)) {}

    uint16_t column_count() const noexcept override {
        return static_cast<uint16_t>(cells_.size());
    }

    std::string_view column_name(uint16_t idx) const noexcept override {
        if (idx >= col_names_->size()) return {};
        return (*col_names_)[idx];
    }

    Value get(uint16_t idx) const noexcept override {
        if (idx >= cells_.size()) return null;
        const auto& c = cells_[idx];
        switch (c.type) {
            case Value::Type::Null:    return null;
            case Value::Type::Int64:   return c.i64;
            case Value::Type::Float64: return c.f64;
            case Value::Type::Bool:    return static_cast<bool>(c.i64);
            case Value::Type::Text:    return std::string_view{c.text};
            case Value::Type::Blob: {
                BufferView bv{reinterpret_cast<const unsigned char*>(c.text.data()),
                              c.text.size()};
                return bv;
            }
        }
        return null;
    }

    Value get(std::string_view name) const noexcept override {
        for (uint16_t i = 0; i < col_names_->size(); ++i)
            if ((*col_names_)[i] == name) return get(i);
        return null;
    }

private:
    std::shared_ptr<std::vector<std::string>> col_names_;
    std::vector<CellData>                     cells_;
};

// ─── ResultSet ────────────────────────────────────────────────────────────────

class SqliteResultSet final : public IResultSet {
public:
    SqliteResultSet(std::vector<std::vector<CellData>>       rows,
                    std::shared_ptr<std::vector<std::string>> col_names,
                    uint64_t                                  affected,
                    uint64_t                                  last_id)
        : rows_(std::move(rows))
        , col_names_(std::move(col_names))
        , affected_(affected)
        , last_id_(last_id) {}

    Task<const IRow*> next() override {
        if (cursor_ >= rows_.size()) co_return nullptr;
        current_ = std::make_unique<SqliteRow>(col_names_,
                                               std::move(rows_[cursor_++]));
        co_return current_.get();
    }

    uint64_t affected_rows()   const noexcept override { return affected_; }
    uint64_t last_insert_id()  const noexcept override { return last_id_;  }

private:
    std::vector<std::vector<CellData>>        rows_;
    std::shared_ptr<std::vector<std::string>> col_names_;
    size_t                                    cursor_{0};
    std::unique_ptr<SqliteRow>                current_;
    uint64_t                                  affected_;
    uint64_t                                  last_id_;
};

// ─── Helper: parameter binding ────────────────────────────────────────────────

static void bind_value(sqlite3_stmt* stmt, int idx, const Value& v) {
    switch (v.type()) {
        case Value::Type::Null:
            sqlite3_bind_null(stmt, idx);
            break;
        case Value::Type::Int64:
        case Value::Type::Bool:
            sqlite3_bind_int64(stmt, idx, v.get<int64_t>());
            break;
        case Value::Type::Float64:
            sqlite3_bind_double(stmt, idx, v.get<double>());
            break;
        case Value::Type::Text: {
            auto sv = v.get<std::string_view>();
            // SQLITE_STATIC: the params span stays valid until sqlite3_step completes
            sqlite3_bind_text(stmt, idx, sv.data(),
                              static_cast<int>(sv.size()), SQLITE_STATIC);
            break;
        }
        case Value::Type::Blob: {
            auto bv = v.get<BufferView>();
            sqlite3_bind_blob(stmt, idx, bv.data(),
                              static_cast<int>(bv.size()), SQLITE_STATIC);
            break;
        }
    }
}

// ─── Helper: read a column value ──────────────────────────────────────────────

static CellData read_column(sqlite3_stmt* stmt, int col) {
    CellData c;
    switch (sqlite3_column_type(stmt, col)) {
        case SQLITE_INTEGER:
            c.type = Value::Type::Int64;
            c.i64  = sqlite3_column_int64(stmt, col);
            break;
        case SQLITE_FLOAT:
            c.type = Value::Type::Float64;
            c.f64  = sqlite3_column_double(stmt, col);
            break;
        case SQLITE_TEXT: {
            c.type = Value::Type::Text;
            const auto* txt = sqlite3_column_text(stmt, col);
            int len         = sqlite3_column_bytes(stmt, col);
            if (txt) c.text.assign(reinterpret_cast<const char*>(txt), len);
            break;
        }
        case SQLITE_BLOB: {
            c.type = Value::Type::Blob;
            const void* data = sqlite3_column_blob(stmt, col);
            int len          = sqlite3_column_bytes(stmt, col);
            if (data) c.text.assign(static_cast<const char*>(data), len);
            break;
        }
        default: // SQLITE_NULL
            c.type = Value::Type::Null;
            break;
    }
    return c;
}

// ─── Helper: execute stmt → build ResultSet ──────────────────────────────────

static Result<std::unique_ptr<IResultSet>>
run_stmt(sqlite3* db, sqlite3_stmt* stmt, std::span<const Value> params) {
    for (int i = 0; i < static_cast<int>(params.size()); ++i)
        bind_value(stmt, i + 1, params[i]);

    int ncols = sqlite3_column_count(stmt);
    auto col_names = std::make_shared<std::vector<std::string>>();
    col_names->reserve(ncols);
    for (int i = 0; i < ncols; ++i) {
        const char* name = sqlite3_column_name(stmt, i);
        col_names->emplace_back(name ? name : "");
    }

    std::vector<std::vector<CellData>> rows;
    while (true) {
        int rc = sqlite3_step(stmt);
        if (rc == SQLITE_ROW) {
            std::vector<CellData> row;
            row.reserve(ncols);
            for (int i = 0; i < ncols; ++i)
                row.push_back(read_column(stmt, i));
            rows.push_back(std::move(row));
        } else if (rc == SQLITE_DONE) {
            break;
        } else {
            return unexpected(db_error(DbError::QueryFailed));
        }
    }

    uint64_t affected = static_cast<uint64_t>(sqlite3_changes(db));
    uint64_t last_id  = static_cast<uint64_t>(sqlite3_last_insert_rowid(db));
    return std::make_unique<SqliteResultSet>(
        std::move(rows), std::move(col_names), affected, last_id);
}

// ─── Forward declaration ──────────────────────────────────────────────────────

class SqliteConnectionPool;

// ─── Slot ─────────────────────────────────────────────────────────────────────
// Owns one sqlite3* connection + its serializing mutex.  Held via shared_ptr so a
// connection (and its statements/transactions) checked out of the pool keeps the
// slot alive even if drain() clears the pool concurrently — otherwise a live
// handle would touch a freed sqlite3/mutex (use-after-free).  Each pooled
// connection is an independent sqlite3*, so under WAL multiple connections read
// concurrently; the per-slot mutex (not a single global one) serializes only the
// statements of one connection.

struct SqliteSlot {
    sqlite3*   conn{nullptr};
    std::mutex mutex;

    explicit SqliteSlot(sqlite3* c) noexcept : conn(c) {}
    ~SqliteSlot() { if (conn) sqlite3_close_v2(conn); }

    SqliteSlot(const SqliteSlot&)            = delete;
    SqliteSlot& operator=(const SqliteSlot&) = delete;
};

// ─── Statement ────────────────────────────────────────────────────────────────
// Holds a shared_ptr<SqliteSlot> keepalive (so the sqlite3*/mutex outlive it);
// db_ and mx_ are cached from the slot for unchanged method bodies.

class SqliteStatement final : public IStatement {
public:
    SqliteStatement(std::shared_ptr<SqliteSlot> slot, sqlite3_stmt* stmt)
        : slot_(std::move(slot)), db_(slot_->conn), stmt_(stmt), mx_(slot_->mutex) {}

    ~SqliteStatement() override {
        if (stmt_) sqlite3_finalize(stmt_);
    }

    Task<Result<std::unique_ptr<IResultSet>>>
    execute(std::span<const Value> params) override {
        std::lock_guard lock{mx_};
        sqlite3_reset(stmt_);
        sqlite3_clear_bindings(stmt_);
        co_return run_stmt(db_, stmt_, params);
    }

    Task<Result<uint64_t>>
    execute_dml(std::span<const Value> params) override {
        std::lock_guard lock{mx_};
        sqlite3_reset(stmt_);
        sqlite3_clear_bindings(stmt_);
        for (int i = 0; i < static_cast<int>(params.size()); ++i)
            bind_value(stmt_, i + 1, params[i]);

        int rc = sqlite3_step(stmt_);
        if (rc != SQLITE_DONE && rc != SQLITE_ROW)
            co_return unexpected(db_error(DbError::QueryFailed));
        co_return static_cast<uint64_t>(sqlite3_changes(db_));
    }

private:
    std::shared_ptr<SqliteSlot> slot_; // keepalive — declared first (init order)
    sqlite3*      db_;
    sqlite3_stmt* stmt_;
    std::mutex&   mx_;
};

// ─── Transaction ──────────────────────────────────────────────────────────────

class SqliteTransaction final : public ITransaction {
public:
    explicit SqliteTransaction(std::shared_ptr<SqliteSlot> slot)
        : slot_(std::move(slot)), db_(slot_->conn), mx_(slot_->mutex) {}

    Task<Result<void>> commit() override {
        co_return exec_sql("COMMIT");
    }

    Task<Result<void>> rollback() override {
        co_return exec_sql("ROLLBACK");
    }

    Task<Result<void>> savepoint(std::string_view name) override {
        if (!db_detail::is_safe_ident(name))
            co_return unexpected(db_error(DbError::QueryFailed));
        co_return exec_sql("SAVEPOINT " + std::string(name));
    }

    Task<Result<void>> rollback_to(std::string_view name) override {
        if (!db_detail::is_safe_ident(name))
            co_return unexpected(db_error(DbError::QueryFailed));
        co_return exec_sql("ROLLBACK TO " + std::string(name));
    }

    Task<Result<uint64_t>>
    execute(std::string_view sql, std::span<const Value> params) override {
        std::lock_guard lock{mx_};
        const auto converted = convert_placeholders(sql);
        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, converted.c_str(),
                                    static_cast<int>(converted.size()), &stmt, nullptr);
        if (rc != SQLITE_OK || !stmt)
            co_return unexpected(db_error(DbError::QueryFailed));

        for (int i = 0; i < static_cast<int>(params.size()); ++i)
            bind_value(stmt, i + 1, params[i]);

        rc = sqlite3_step(stmt);
        uint64_t affected = (rc == SQLITE_DONE || rc == SQLITE_ROW)
                          ? static_cast<uint64_t>(sqlite3_changes(db_)) : 0;
        sqlite3_finalize(stmt);

        if (rc != SQLITE_DONE && rc != SQLITE_ROW)
            co_return unexpected(db_error(sqlite_transient_or(rc, DbError::QueryFailed)));
        co_return affected;
    }

private:
    Result<void> exec_sql(const std::string& sql) {
        std::lock_guard lock{mx_};
        char* err = nullptr;
        int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err);
        if (rc != SQLITE_OK) {
            sqlite3_free(err);
            return unexpected(db_error(sqlite_transient_or(rc, DbError::TransactionFailed)));
        }
        return {};
    }

    std::shared_ptr<SqliteSlot> slot_; // keepalive — declared first (init order)
    sqlite3*    db_;
    std::mutex& mx_;
};

// ─── Connection ───────────────────────────────────────────────────────────────

class SqliteConnection final : public IConnection {
public:
    SqliteConnection(std::shared_ptr<SqliteSlot> slot, size_t slot_idx,
                     SqliteConnectionPool* pool)
        : slot_(std::move(slot)), db_(slot_->conn), mx_(slot_->mutex),
          slot_idx_(slot_idx), pool_(pool) {}

    ~SqliteConnection() override;  // defined below (after SqliteConnectionPool)

    ConnectionState state() const noexcept override { return state_; }

    Task<Result<std::unique_ptr<IStatement>>>
    prepare(std::string_view sql) override {
        std::lock_guard lock{mx_};
        const auto converted = convert_placeholders(sql);
        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, converted.c_str(),
                                    static_cast<int>(converted.size()), &stmt, nullptr);
        if (rc != SQLITE_OK || !stmt)
            co_return unexpected(db_error(DbError::PrepareStatementFailed));
        co_return std::make_unique<SqliteStatement>(slot_, stmt);
    }

    Task<Result<std::unique_ptr<IResultSet>>>
    query(std::string_view sql, std::span<const Value> params) override {
        std::lock_guard lock{mx_};
        const auto converted = convert_placeholders(sql);
        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, converted.c_str(),
                                    static_cast<int>(converted.size()), &stmt, nullptr);
        if (rc != SQLITE_OK || !stmt)
            co_return unexpected(db_error(DbError::QueryFailed));
        auto result = run_stmt(db_, stmt, params);
        sqlite3_finalize(stmt);
        co_return result;
    }

    Task<Result<std::unique_ptr<ITransaction>>>
    begin(IsolationLevel /*level*/) override {
        std::lock_guard lock{mx_};
        char* err = nullptr;
        int rc = sqlite3_exec(db_, "BEGIN", nullptr, nullptr, &err);
        if (rc != SQLITE_OK) {
            sqlite3_free(err);
            co_return unexpected(db_error(DbError::TransactionFailed));
        }
        state_ = ConnectionState::Transaction;
        co_return std::make_unique<SqliteTransaction>(slot_);
    }

    Task<Result<void>> close() override {
        state_ = ConnectionState::Idle;
        co_return {};
    }

    Task<bool> ping() override {
        std::lock_guard lock{mx_};
        int rc = sqlite3_exec(db_, "SELECT 1", nullptr, nullptr, nullptr);
        co_return rc == SQLITE_OK;
    }

private:
    std::shared_ptr<SqliteSlot> slot_; // keepalive — declared first (init order)
    sqlite3*              db_;
    std::mutex&           mx_;
    size_t                slot_idx_;
    SqliteConnectionPool* pool_;
    ConnectionState       state_{ConnectionState::Idle};
};

// ─── ConnectionPool ───────────────────────────────────────────────────────────
//
// A real multi-connection pool: each slot is an independent sqlite3* on the same
// database.  Under WAL this gives concurrent readers (writes still serialize —
// inherent to SQLite).  A private in-memory database (":memory:") is per-handle,
// so pooling it would hand out connections to *different* empty databases; for
// that case the pool collapses to a single connection (max_size = 1).

// Opens one sqlite3* and applies the standard pragmas. Returns nullptr on failure.
static sqlite3* open_sqlite_connection(const std::string& path) {
    const int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                    | SQLITE_OPEN_FULLMUTEX | SQLITE_OPEN_URI;
    sqlite3* db = nullptr;
    if (sqlite3_open_v2(path.c_str(), &db, flags, nullptr) != SQLITE_OK) {
        if (db) sqlite3_close_v2(db);
        return nullptr;
    }
    // Wait (instead of failing immediately with SQLITE_BUSY) when another
    // writer holds the lock — essential under WAL with concurrent writers,
    // otherwise transient contention surfaces as hard query failures.
    sqlite3_busy_timeout(db, 5000); // 5s
    // WAL mode: allows concurrent readers, serializes writers
    sqlite3_exec(db, "PRAGMA journal_mode=WAL",        nullptr, nullptr, nullptr);
    sqlite3_exec(db, "PRAGMA synchronous=NORMAL",      nullptr, nullptr, nullptr);
    sqlite3_exec(db, "PRAGMA foreign_keys=ON",         nullptr, nullptr, nullptr);
    // Performance tuning
    sqlite3_exec(db, "PRAGMA cache_size=-65536",       nullptr, nullptr, nullptr); // 64MB page cache
    sqlite3_exec(db, "PRAGMA temp_store=MEMORY",       nullptr, nullptr, nullptr); // temp tables in memory
    sqlite3_exec(db, "PRAGMA mmap_size=268435456",     nullptr, nullptr, nullptr); // 256MB mmap
    sqlite3_exec(db, "PRAGMA page_size=4096",          nullptr, nullptr, nullptr); // 4KB pages (new DB only)
    return db;
}

// True for a private in-memory database, where each connection is a distinct
// empty DB and pooling would be incoherent (callers would get different DBs).  A
// shared-cache URI (file:...?cache=shared) names a database multiple connections
// share, so it IS poolable and excluded here.
static bool is_private_memory(std::string_view path) {
    if (path == ":memory:" || path.empty()) return true;
    const bool memory = path.find("mode=memory") != std::string_view::npos
                     || path.find(":memory:")    != std::string_view::npos;
    const bool shared = path.find("cache=shared") != std::string_view::npos;
    return memory && !shared;
}

class SqliteConnectionPool final : public IConnectionPool {
public:
    SqliteConnectionPool(std::string path, PoolConfig config)
        : path_(std::move(path)) {
        // A private in-memory DB can only ever be a single connection.
        const bool single = is_private_memory(path_);
        max_size_ = single ? 1 : (config.max_size > 0 ? config.max_size : 16);
        size_t min_sz = single ? 1 : (config.min_size > 0 ? config.min_size : 2);
        if (min_sz > max_size_) min_sz = max_size_;

        std::lock_guard lock{mutex_};
        for (size_t i = 0; i < min_sz; ++i) {
            sqlite3* c = open_sqlite_connection(path_);
            if (!c) break;
            idle_.push(slots_.size());
            slots_.push_back(std::make_shared<SqliteSlot>(c));
        }
    }

    ~SqliteConnectionPool() override = default;

    bool is_valid() const noexcept { return !slots_.empty(); }

    Task<Result<std::unique_ptr<IConnection>>> acquire() override {
        std::unique_lock lock{mutex_};

        // 1. idle slot
        if (!idle_.empty()) {
            const size_t idx = idle_.front(); idle_.pop();
            std::shared_ptr<SqliteSlot> slot = slots_[idx];
            active_.fetch_add(1, std::memory_order_relaxed);
            lock.unlock();
            co_return std::make_unique<SqliteConnection>(std::move(slot), idx, this);
        }

        // 2. create a new slot
        if (slots_.size() < max_size_) {
            const size_t idx = slots_.size();
            slots_.push_back(nullptr); // reserve the index
            lock.unlock();

            sqlite3* c = open_sqlite_connection(path_);
            if (!c) {
                lock.lock(); slots_.pop_back();
                co_return unexpected(db_error(DbError::ConnectionFailed));
            }
            auto slot = std::make_shared<SqliteSlot>(c);
            lock.lock();
            slots_[idx] = slot;
            lock.unlock();
            active_.fetch_add(1, std::memory_order_relaxed);
            co_return std::make_unique<SqliteConnection>(std::move(slot), idx, this);
        }

        // 3. pool exhausted → immediate error (synchronous driver)
        co_return unexpected(db_error(DbError::PoolExhausted));
    }

    void release(size_t idx) noexcept {
        active_.fetch_sub(1, std::memory_order_relaxed);
        std::lock_guard lock{mutex_};
        // If the pool was drained while this connection was checked out, its slot
        // entry is gone; the connection's own shared_ptr<SqliteSlot> still owns
        // (and will close) the sqlite3*, so there is no use-after-free.
        if (idx >= slots_.size() || !slots_[idx]) return;
        idle_.push(idx);
    }

    // return_connection is handled by the unique_ptr destructor path:
    // ~SqliteConnection → release().
    void return_connection(std::unique_ptr<IConnection>) noexcept override {}

    size_t active_count() const noexcept override {
        return active_.load(std::memory_order_relaxed);
    }
    size_t idle_count() const noexcept override {
        std::lock_guard lock{mutex_}; return idle_.size();
    }
    size_t max_size() const noexcept override { return max_size_; }

    Task<void> drain() override {
        std::lock_guard lock{mutex_};
        while (!idle_.empty()) idle_.pop();
        slots_.clear();
        active_.store(0, std::memory_order_relaxed);
        co_return;
    }

private:
    std::string                              path_;
    size_t                                   max_size_{1};
    mutable std::mutex                       mutex_;
    // shared_ptr so a checked-out connection keeps its SqliteSlot (sqlite3* +
    // mutex) alive even if drain() clears the pool concurrently.
    std::vector<std::shared_ptr<SqliteSlot>> slots_;
    std::queue<size_t>                       idle_;
    std::atomic<size_t>                      active_{0};
};

// ─── SqliteConnection::~SqliteConnection ──────────────────────────────────────

SqliteConnection::~SqliteConnection() {
    // Roll back a transaction the holder left open (BEGIN with no COMMIT/ROLLBACK)
    // before the slot returns to the pool, so transaction state never bleeds into
    // the next user of this pooled connection.  sqlite3_get_autocommit()==0 means
    // a transaction is still active; it costs nothing on the normal path.
    if (slot_ && db_) {
        std::lock_guard lock{mx_};
        if (sqlite3_get_autocommit(db_) == 0)
            sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr);
    }
    if (pool_) pool_->release(slot_idx_);
}

// ─── Driver ───────────────────────────────────────────────────────────────────

class SqliteDriver final : public IDBDriver {
public:
    std::string_view driver_name() const noexcept override { return "sqlite"; }

    bool accepts(std::string_view dsn) const noexcept override {
        return dsn.starts_with("sqlite://");
    }

    // sqlite:///path/to/file.db  or  sqlite://:memory:  or a URI
    // (sqlite://file:name?mode=memory&cache=shared — poolable shared-cache DB).
    Task<Result<std::unique_ptr<IConnectionPool>>>
    pool(std::string_view dsn, PoolConfig config) override {
        std::string path{dsn.substr(9)}; // strip "sqlite://"
        auto p = std::make_unique<SqliteConnectionPool>(std::move(path),
                                                        std::move(config));
        if (!p->is_valid())
            co_return unexpected(db_error(DbError::ConnectionFailed));
        co_return p;
    }
};

} // anonymous namespace

// ─── Factory ──────────────────────────────────────────────────────────────────

std::unique_ptr<qbuem::db::IDBDriver> make_sqlite_driver() {
    return std::make_unique<SqliteDriver>();
}

} // namespace qbuem_routine
