#include "sqlite3_driver.hpp"
#include "db_error.hpp"

#include <sqlite3.h>
#include <qbuem/core/task.hpp>
#include <qbuem/db/driver.hpp>
#include <qbuem/db/value.hpp>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

using namespace qbuem;
using namespace qbuem::db;

namespace qbuem_routine {
namespace {

// ─── $N → ? 플레이스홀더 변환 (ORM 호환) ─────────────────────────────────────

static std::string convert_placeholders(std::string_view sql) {
    std::string result;
    result.reserve(sql.size());
    std::size_t i = 0;
    while (i < sql.size()) {
        if (sql[i] == '$' && i + 1 < sql.size() && std::isdigit(sql[i + 1])) {
            result += '?';
            ++i;
            while (i < sql.size() && std::isdigit(sql[i])) ++i;
        } else {
            result += sql[i++];
        }
    }
    return result;
}

// ─── CellData — 단일 컬럼 값 저장 ───────────────────────────────────────────

struct CellData {
    Value::Type type = Value::Type::Null;
    int64_t     i64  = 0;
    double      f64  = 0.0;
    std::string text; // TEXT / BLOB 모두 여기에 저장 (소유권 보장)
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

// ─── 헬퍼: 파라미터 바인딩 ────────────────────────────────────────────────────

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
            // SQLITE_STATIC: params span은 sqlite3_step 완료까지 유효
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

// ─── 헬퍼: 컬럼 값 읽기 ──────────────────────────────────────────────────────

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

// ─── 헬퍼: stmt 실행 → ResultSet 빌드 ───────────────────────────────────────

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

// ─── Statement ────────────────────────────────────────────────────────────────

class SqliteStatement final : public IStatement {
public:
    SqliteStatement(sqlite3* db, sqlite3_stmt* stmt, std::mutex& mx)
        : db_(db), stmt_(stmt), mx_(mx) {}

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
    sqlite3*      db_;
    sqlite3_stmt* stmt_;
    std::mutex&   mx_;
};

// ─── Transaction ──────────────────────────────────────────────────────────────

class SqliteTransaction final : public ITransaction {
public:
    SqliteTransaction(sqlite3* db, std::mutex& mx)
        : db_(db), mx_(mx) {}

    Task<Result<void>> commit() override {
        co_return exec_sql("COMMIT");
    }

    Task<Result<void>> rollback() override {
        co_return exec_sql("ROLLBACK");
    }

    Task<Result<void>> savepoint(std::string_view name) override {
        co_return exec_sql("SAVEPOINT " + std::string(name));
    }

    Task<Result<void>> rollback_to(std::string_view name) override {
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
            co_return unexpected(db_error(DbError::QueryFailed));
        co_return affected;
    }

private:
    Result<void> exec_sql(const std::string& sql) {
        std::lock_guard lock{mx_};
        char* err = nullptr;
        int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err);
        if (rc != SQLITE_OK) {
            sqlite3_free(err);
            return unexpected(db_error(DbError::TransactionFailed));
        }
        return {};
    }

    sqlite3*    db_;
    std::mutex& mx_;
};

// ─── Connection ───────────────────────────────────────────────────────────────

class SqliteConnection final : public IConnection {
public:
    SqliteConnection(sqlite3* db, std::mutex& mx)
        : db_(db), mx_(mx) {}

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
        co_return std::make_unique<SqliteStatement>(db_, stmt, mx_);
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
        co_return std::make_unique<SqliteTransaction>(db_, mx_);
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
    sqlite3*        db_;
    std::mutex&     mx_;
    ConnectionState state_{ConnectionState::Idle};
};

// ─── ConnectionPool ───────────────────────────────────────────────────────────

class SqliteConnectionPool final : public IConnectionPool {
public:
    explicit SqliteConnectionPool(std::string_view path) {
        int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                  | SQLITE_OPEN_FULLMUTEX;
        if (sqlite3_open_v2(std::string(path).c_str(), &db_, flags, nullptr)
                != SQLITE_OK) {
            db_ = nullptr;
            return;
        }
        // WAL 모드: 동시 읽기 허용, 쓰기 직렬화
        sqlite3_exec(db_, "PRAGMA journal_mode=WAL",        nullptr, nullptr, nullptr);
        sqlite3_exec(db_, "PRAGMA synchronous=NORMAL",      nullptr, nullptr, nullptr);
        sqlite3_exec(db_, "PRAGMA foreign_keys=ON",         nullptr, nullptr, nullptr);
        // 성능 최적화
        sqlite3_exec(db_, "PRAGMA cache_size=-65536",       nullptr, nullptr, nullptr); // 64MB 페이지 캐시
        sqlite3_exec(db_, "PRAGMA temp_store=MEMORY",       nullptr, nullptr, nullptr); // 임시 테이블 메모리
        sqlite3_exec(db_, "PRAGMA mmap_size=268435456",     nullptr, nullptr, nullptr); // 256MB mmap
        sqlite3_exec(db_, "PRAGMA page_size=4096",          nullptr, nullptr, nullptr); // 4KB 페이지 (신규 DB만)
    }

    ~SqliteConnectionPool() override {
        if (db_) sqlite3_close_v2(db_);
    }

    bool is_valid() const noexcept { return db_ != nullptr; }

    Task<Result<std::unique_ptr<IConnection>>> acquire() override {
        if (!db_)
            co_return unexpected(
                db_error(DbError::ConnectionFailed));
        active_.fetch_add(1, std::memory_order_relaxed);
        co_return std::make_unique<SqliteConnection>(db_, mutex_);
    }

    void return_connection(std::unique_ptr<IConnection>) noexcept override {
        active_.fetch_sub(1, std::memory_order_relaxed);
    }

    size_t active_count() const noexcept override {
        return active_.load(std::memory_order_relaxed);
    }
    size_t idle_count() const noexcept override { return 0; }
    size_t max_size()   const noexcept override { return 1; }

    Task<void> drain() override {
        if (db_) { sqlite3_close_v2(db_); db_ = nullptr; }
        co_return;
    }

private:
    sqlite3*            db_    = nullptr;
    std::mutex          mutex_;
    std::atomic<size_t> active_{0};
};

// ─── Driver ───────────────────────────────────────────────────────────────────

class SqliteDriver final : public IDBDriver {
public:
    std::string_view driver_name() const noexcept override { return "sqlite"; }

    bool accepts(std::string_view dsn) const noexcept override {
        return dsn.starts_with("sqlite://");
    }

    // sqlite:///path/to/file.db  또는  sqlite://:memory:
    Task<Result<std::unique_ptr<IConnectionPool>>>
    pool(std::string_view dsn, PoolConfig /*config*/) override {
        std::string_view path = dsn.substr(9); // "sqlite://" 제거
        auto p = std::make_unique<SqliteConnectionPool>(path);
        if (!p->is_valid())
            co_return unexpected(
                db_error(DbError::ConnectionFailed));
        co_return p;
    }
};

} // anonymous namespace

// ─── 팩토리 ────────────────────────────────────────────────────────────────────

std::unique_ptr<qbuem::db::IDBDriver> make_sqlite_driver() {
    return std::make_unique<SqliteDriver>();
}

} // namespace qbuem_routine
