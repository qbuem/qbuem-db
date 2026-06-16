#include "mysql_driver.hpp"
#include "db_error.hpp"
#include "sql_placeholders.hpp"

#include <mysql/mysql.h>
#include <qbuem/core/task.hpp>
#include <qbuem/db/driver.hpp>
#include <qbuem/db/value.hpp>

#include <atomic>
#include <charconv>
#include <chrono>
#include <memory>
#include <type_traits>
#include <utility>
#include <mutex>
#include <queue>
#include <string>
#include <string_view>
#include <vector>

using namespace qbuem;
using namespace qbuem::db;

namespace qbuem_routine {
namespace {

// `my_bool` was removed in MySQL 8.0 (MYSQL_BIND::is_null is now `bool*`); derive
// the element type from the field itself so this builds on old and new client.
using MyBool = std::remove_pointer_t<decltype(std::declval<MYSQL_BIND>().is_null)>;

// ─── DSN parser ───────────────────────────────────────────────────────────────

struct MysqlDsn {
    std::string user;
    std::string password;
    std::string host;
    uint16_t    port{3306};
    std::string dbname;
    bool        use_ssl{false};
};

static MysqlDsn parse_dsn(std::string_view dsn) {
    MysqlDsn result;

    if (dsn.starts_with("mysql://"))    dsn.remove_prefix(8);
    else if (dsn.starts_with("mariadb://")) dsn.remove_prefix(10);

    // ssl option
    if (auto pos = dsn.find('?'); pos != std::string_view::npos) {
        auto opts = dsn.substr(pos + 1);
        dsn       = dsn.substr(0, pos);
        if (opts.find("ssl=true") != std::string_view::npos) result.use_ssl = true;
    }

    // user:pass@
    if (auto at = dsn.find('@'); at != std::string_view::npos) {
        auto user_info = dsn.substr(0, at);
        dsn = dsn.substr(at + 1);
        if (auto colon = user_info.find(':'); colon != std::string_view::npos) {
            result.user     = std::string(user_info.substr(0, colon));
            result.password = std::string(user_info.substr(colon + 1));
        } else {
            result.user = std::string(user_info);
        }
    }

    // host:port/dbname
    if (auto slash = dsn.find('/'); slash != std::string_view::npos) {
        auto host_port = dsn.substr(0, slash);
        result.dbname  = std::string(dsn.substr(slash + 1));
        if (auto colon = host_port.find(':'); colon != std::string_view::npos) {
            result.host = std::string(host_port.substr(0, colon));
            std::from_chars(host_port.data() + colon + 1,
                            host_port.data() + host_port.size(),
                            result.port);
        } else {
            result.host = std::string(host_port);
        }
    }

    return result;
}

// $N → ? placeholder conversion: shared, string-literal-aware implementation.
using qbuem_routine::db_detail::convert_placeholders;

// Map MySQL's native error number to a transient (retryable) DbError where it
// applies, else `fallback`. Lets with_transaction() retry real deadlocks /
// lock-wait timeouts. 1213 = ER_LOCK_DEADLOCK, 1205 = ER_LOCK_WAIT_TIMEOUT.
// 1213 = ER_LOCK_DEADLOCK, 1205 = ER_LOCK_WAIT_TIMEOUT, 3024 = ER_QUERY_TIMEOUT
// (max_execution_time), 2013/2006 = CR_SERVER_LOST/GONE (a query that overran the
// MYSQL_OPT_READ_TIMEOUT deadline aborts the connection with these).
static DbError mysql_transient_or(MYSQL* db, DbError fallback) {
    switch (mysql_errno(db)) {
        case 1213: return DbError::Deadlock;
        case 1205: return DbError::LockTimeout;
        case 3024: return DbError::StatementTimeout;
        case 2013: case 2006: return DbError::StatementTimeout;
        default:   return fallback;
    }
}
static DbError mysql_stmt_transient_or(MYSQL_STMT* stmt, DbError fallback) {
    switch (mysql_stmt_errno(stmt)) {
        case 1213: return DbError::Deadlock;
        case 1205: return DbError::LockTimeout;
        case 3024: return DbError::StatementTimeout;
        case 2013: case 2006: return DbError::StatementTimeout;
        default:   return fallback;
    }
}

// ─── CellData ─────────────────────────────────────────────────────────────────

struct CellData {
    Value::Type type{Value::Type::Null};
    int64_t     i64{0};
    double      f64{0.0};
    std::string text;
};

// ─── Column-value decode (from a text/string representation) ──────────────────

static CellData cell_from_text(const char* val, unsigned long len,
                                 MYSQL_FIELD* field) noexcept {
    CellData c;
    if (!val || !field) return c; // Null

    switch (field->type) {
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
            c.type = Value::Type::Int64;
            std::from_chars(val, val + len, c.i64);
            break;
        case MYSQL_TYPE_FLOAT:
        case MYSQL_TYPE_DOUBLE:
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL:
            c.type = Value::Type::Float64;
            std::from_chars(val, val + len, c.f64);
            break;
        case MYSQL_TYPE_BIT:
            c.type = Value::Type::Int64;
            c.i64  = 0;
            for (unsigned long bi = 0; bi < len; ++bi)
                c.i64 = (c.i64 << 8) | static_cast<uint8_t>(val[bi]);
            break;
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_BLOB:
            if (field->flags & BINARY_FLAG) {
                c.type = Value::Type::Blob;
                c.text.assign(val, len);
            } else {
                c.type = Value::Type::Text;
                c.text.assign(val, len);
            }
            break;
        default:
            c.type = Value::Type::Text;
            c.text.assign(val, len);
            break;
    }
    return c;
}

// ─── Row ─────────────────────────────────────────────────────────────────────

class MysqlRow final : public IRow {
public:
    MysqlRow(std::shared_ptr<std::vector<std::string>> col_names,
             std::vector<CellData> cells)
        : col_names_(std::move(col_names)), cells_(std::move(cells)) {}

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
            case Value::Type::Blob:
                return BufferView{reinterpret_cast<const unsigned char*>(c.text.data()),
                                  c.text.size()};
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

class MysqlResultSet final : public IResultSet {
public:
    MysqlResultSet(std::vector<std::vector<CellData>>       rows,
                   std::shared_ptr<std::vector<std::string>> col_names,
                   uint64_t affected,
                   uint64_t last_id)
        : rows_(std::move(rows))
        , col_names_(std::move(col_names))
        , affected_(affected)
        , last_id_(last_id) {}

    Task<const IRow*> next() override {
        if (cursor_ >= rows_.size()) co_return nullptr;
        current_ = std::make_unique<MysqlRow>(col_names_, std::move(rows_[cursor_++]));
        co_return current_.get();
    }

    uint64_t affected_rows()  const noexcept override { return affected_; }
    uint64_t last_insert_id() const noexcept override { return last_id_; }

private:
    std::vector<std::vector<CellData>>        rows_;
    std::shared_ptr<std::vector<std::string>> col_names_;
    std::size_t                               cursor_{0};
    std::unique_ptr<MysqlRow>                 current_;
    uint64_t                                  affected_;
    uint64_t                                  last_id_;
};

// ─── Forward declaration ──────────────────────────────────────────────────────

class MysqlConnectionPool;

// ─── Slot ─────────────────────────────────────────────────────────────────────
// Owns one MYSQL* + its serializing mutex.  Held via shared_ptr so a connection
// (and its statements/transactions/result sets) checked out of the pool keeps the
// slot alive even if drain() clears the pool concurrently — otherwise a live
// handle would touch a freed MYSQL/mutex (use-after-free).

struct MysqlSlot {
    using Clock = std::chrono::steady_clock;

    MYSQL*             conn{nullptr};
    std::mutex         mutex;
    Clock::time_point  idle_since{Clock::now()};
    // Set when begin() opens a transaction, cleared on commit()/rollback().  Lets
    // the connection destructor roll back a transaction the holder left open so it
    // never bleeds into the next user of this pooled connection.  (libmysqlclient
    // exposes no public "in transaction?" query, hence this explicit flag.)
    bool               in_txn{false};

    explicit MysqlSlot(MYSQL* c) noexcept : conn(c) {}
    ~MysqlSlot() { if (conn) { mysql_close(conn); conn = nullptr; } }

    MysqlSlot(const MysqlSlot&)            = delete;
    MysqlSlot& operator=(const MysqlSlot&) = delete;

    void mark_idle() noexcept { idle_since = Clock::now(); }

    bool needs_liveness_check() const noexcept {
        // Only ping connections that have been idle for more than 30s
        return (Clock::now() - idle_since) > std::chrono::seconds{30};
    }
};

// ─── Streaming ResultSet ──────────────────────────────────────────────────────
// Lazy, bounded-memory result for SELECTs from conn->query(): the statement uses
// a read-only server-side cursor (no client-side store_result), and next() pulls
// one row at a time via mysql_stmt_fetch. Owns its MYSQL_STMT + MYSQL_RES (closed
// on exhaustion or destruction, so abandoning a partial scan is safe) and holds a
// shared_ptr<MysqlSlot> keepalive. Bound result buffers are members (never
// reallocated after bind), with oversized columns re-fetched into a scratch
// buffer — same invariant as the buffered path.

class MysqlStreamResultSet final : public IResultSet {
public:
    MysqlStreamResultSet(std::shared_ptr<MysqlSlot> slot, MYSQL_STMT* stmt,
                         MYSQL_RES* meta,
                         std::shared_ptr<std::vector<std::string>> col_names,
                         uint64_t affected, uint64_t last_id)
        : slot_(std::move(slot)), stmt_(stmt), meta_(meta),
          fields_(mysql_fetch_fields(meta)), col_names_(std::move(col_names)),
          ncols_(mysql_num_fields(meta)), affected_(affected), last_id_(last_id),
          res_binds_(ncols_), col_bufs_(ncols_), col_lens_(ncols_, 0),
          col_nulls_(std::make_unique<MyBool[]>(ncols_)) {}

    ~MysqlStreamResultSet() override { cleanup(); }

    MysqlStreamResultSet(const MysqlStreamResultSet&)            = delete;
    MysqlStreamResultSet& operator=(const MysqlStreamResultSet&) = delete;

    // Bind the member result buffers to the statement (once, before fetching).
    // Members are stable for our lifetime, so the captured pointers stay valid.
    [[nodiscard]] bool bind() {
        for (unsigned int i = 0; i < ncols_; ++i) {
            auto& b = res_binds_[i];
            std::memset(&b, 0, sizeof(b));
            const unsigned long init_sz =
                fields_[i].max_length > 0 ? fields_[i].max_length + 1 : 256;
            col_bufs_[i].resize(init_sz);
            b.buffer_type   = MYSQL_TYPE_STRING;
            b.buffer        = col_bufs_[i].data();
            b.buffer_length = static_cast<unsigned long>(col_bufs_[i].size());
            b.length        = &col_lens_[i];
            b.is_null       = &col_nulls_[i];
        }
        return ncols_ == 0 || mysql_stmt_bind_result(stmt_, res_binds_.data()) == 0;
    }

    Task<const IRow*> next() override {
        std::lock_guard lock{slot_->mutex};
        if (!stmt_) co_return nullptr;
        const int fetch_rc = mysql_stmt_fetch(stmt_);
        if (fetch_rc != 0 && fetch_rc != MYSQL_DATA_TRUNCATED) {
            cleanup(); // MYSQL_NO_DATA (100) or error → end of stream
            co_return nullptr;
        }
        std::vector<CellData> row;
        row.reserve(ncols_);
        for (unsigned int i = 0; i < ncols_; ++i) {
            if (col_nulls_[i]) { row.push_back(CellData{}); continue; }
            if (col_lens_[i] > col_bufs_[i].size()) {
                if (scratch_.size() < col_lens_[i]) scratch_.resize(col_lens_[i]);
                MYSQL_BIND    rb{};
                unsigned long rlen   = 0;
                MyBool        rnull  = 0;
                MyBool        rerror = 0;
                std::memset(&rb, 0, sizeof(rb));
                rb.buffer_type   = MYSQL_TYPE_STRING;
                rb.buffer        = scratch_.data();
                rb.buffer_length = col_lens_[i];
                rb.length        = &rlen;
                rb.is_null       = &rnull;
                rb.error         = &rerror;
                if (mysql_stmt_fetch_column(stmt_, &rb, i, 0) == 0)
                    row.push_back(cell_from_text(scratch_.data(), col_lens_[i], &fields_[i]));
                else
                    row.push_back(CellData{});
            } else {
                row.push_back(cell_from_text(col_bufs_[i].data(), col_lens_[i], &fields_[i]));
            }
        }
        current_ = std::make_unique<MysqlRow>(col_names_, std::move(row));
        co_return current_.get();
    }

    uint64_t affected_rows()  const noexcept override { return affected_; }
    uint64_t last_insert_id() const noexcept override { return last_id_; }

private:
    void cleanup() noexcept {
        if (meta_) { mysql_free_result(meta_); meta_ = nullptr; }
        if (stmt_) { mysql_stmt_close(stmt_); stmt_ = nullptr; }
    }

    std::shared_ptr<MysqlSlot>                slot_; // keepalive
    MYSQL_STMT*                               stmt_;
    MYSQL_RES*                                meta_;
    MYSQL_FIELD*                              fields_;
    std::shared_ptr<std::vector<std::string>> col_names_;
    unsigned int                              ncols_;
    uint64_t                                  affected_, last_id_;
    std::vector<MYSQL_BIND>                   res_binds_;
    std::vector<std::vector<char>>            col_bufs_;
    std::vector<unsigned long>                col_lens_;
    std::unique_ptr<MyBool[]>                 col_nulls_;
    std::vector<char>                         scratch_;
    std::unique_ptr<MysqlRow>                 current_;
};

// ─── Connection-creation helper ───────────────────────────────────────────────

static MYSQL* create_connection(const MysqlDsn& dsn, unsigned query_timeout_ms = 0) {
    MYSQL* conn = mysql_init(nullptr);
    if (!conn) return nullptr;

    mysql_options(conn, MYSQL_SET_CHARSET_NAME, "utf8mb4");
    // Hard query-timeout backstop. max_execution_time (set after connect) does NOT
    // apply to the prepared-statement (binary) protocol this driver uses, so rely
    // on a network read timeout: if the server hasn't produced the result within
    // the deadline the client aborts (errno CR_SERVER_LOST 2013 → StatementTimeout).
    // Granularity is whole seconds, so sub-second timeouts round up to 1s.
    if (query_timeout_ms > 0) {
        unsigned read_timeout_sec = (query_timeout_ms + 999) / 1000;
        if (read_timeout_sec < 1) read_timeout_sec = 1;
        mysql_options(conn, MYSQL_OPT_READ_TIMEOUT, &read_timeout_sec);
    }
    // NOTE: MYSQL_OPT_RECONNECT is intentionally NOT enabled.  libmysqlclient's
    // auto-reconnect silently drops session state (open transaction, temp tables,
    // prepared statements, SET vars) mid-flight, which can corrupt transactional
    // guarantees; the pool reconnects explicitly via ping().  It is also removed
    // in MySQL 8.3+.
    if (dsn.use_ssl) {
        // MYSQL_OPT_SSL_MODE (a mysql_option ENUMERATOR, not a macro — guard on
        // the library version, not #ifdef) replaced the removed
        // MYSQL_OPT_SSL_ENFORCE.  Takes an unsigned-int enum value.
#if MYSQL_VERSION_ID >= 50711
        unsigned int ssl_mode = SSL_MODE_REQUIRED;
        mysql_options(conn, MYSQL_OPT_SSL_MODE, &ssl_mode);
#endif
    }

    // Allow caching_sha2_password (the MySQL 8+/9 DEFAULT auth plugin) to
    // complete over a non-TLS connection by fetching the server's RSA public
    // key.  Without this, the very first connection to a stock MySQL 8/9 server
    // fails unless SSL is configured — a common production foot-gun.  The
    // option is a MySQL-8.0.4+ enumerator (absent on MariaDB).
#if MYSQL_VERSION_ID >= 80004 && !defined(MARIADB_VERSION_ID) && !defined(MARIADB_BASE_VERSION)
    bool get_server_public_key = true;
    mysql_options(conn, MYSQL_OPT_GET_SERVER_PUBLIC_KEY, &get_server_public_key);
#endif

    // TCP keepalive — counters cloud-firewall dropping of idle connections
#ifdef MYSQL_OPT_TCP_KEEPIDLE
    const unsigned int keepidle     = 60;  // start probing after 60s idle
    const unsigned int keepinterval = 10;  // 10s between probes
    const unsigned int keepcount    = 3;   // close the connection after 3 failed probes
    mysql_options(conn, MYSQL_OPT_TCP_KEEPIDLE,     &keepidle);
    mysql_options(conn, MYSQL_OPT_TCP_KEEPINTERVAL, &keepinterval);
    mysql_options(conn, MYSQL_OPT_TCP_KEEPCOUNT,    &keepcount);
#endif

    const char* pass_ptr = dsn.password.empty() ? nullptr : dsn.password.c_str();
    if (!mysql_real_connect(conn,
                            dsn.host.c_str(),
                            dsn.user.c_str(),
                            pass_ptr,
                            dsn.dbname.c_str(),
                            dsn.port,
                            nullptr, 0)) {
        mysql_close(conn);
        return nullptr;
    }
    // Enforce the query timeout server-side: max_execution_time cancels a runaway
    // SELECT (→ errno 3024, mapped to StatementTimeout). It applies to read-only
    // SELECTs only — MySQL has no per-statement timeout for writes — but covers the
    // common runaway-query case. MySQL 5.7.8+; ignored (harmless) on MariaDB.
    if (query_timeout_ms > 0) {
        const std::string sql =
            "SET SESSION max_execution_time=" + std::to_string(query_timeout_ms);
        mysql_query(conn, sql.c_str());
    }
    return conn;
}

// ─── Parameter-binding buffers (one independent numeric buffer per parameter) ──

struct ParamBuf {
    int64_t i64{};
    double  f64{};
};

// ─── Prepared-statement execution helper ──────────────────────────────────────
// The caller MUST hold the owning slot's mutex when calling this.
// stmt must already be prepared; resetting it is the caller's responsibility.

static Result<std::unique_ptr<IResultSet>>
exec_stmt(MYSQL_STMT* stmt, std::span<const Value> params) {
    // Parameter binding — zero-copy: Text/Blob reference the Value data directly
    std::vector<ParamBuf>   num_bufs(params.size());
    std::vector<MYSQL_BIND> binds(params.size());

    for (std::size_t i = 0; i < params.size(); ++i) {
        auto& b   = binds[i];
        auto& buf = num_bufs[i];
        const auto& v = params[i];
        std::memset(&b, 0, sizeof(b));
        switch (v.type()) {
            case Value::Type::Null:
                b.buffer_type = MYSQL_TYPE_NULL;
                break;
            case Value::Type::Bool:
            case Value::Type::Int64:
                buf.i64         = v.get<int64_t>();
                b.buffer_type   = MYSQL_TYPE_LONGLONG;
                b.buffer        = &buf.i64;
                b.buffer_length = sizeof(int64_t);
                break;
            case Value::Type::Float64:
                buf.f64         = v.get<double>();
                b.buffer_type   = MYSQL_TYPE_DOUBLE;
                b.buffer        = &buf.f64;
                b.buffer_length = sizeof(double);
                break;
            case Value::Type::Text: {
                auto sv = v.get<std::string_view>();
                b.buffer_type   = MYSQL_TYPE_STRING;
                // zero-copy: the params span stays valid until exec completes
                b.buffer        = const_cast<void*>(static_cast<const void*>(sv.data()));
                b.buffer_length = static_cast<unsigned long>(sv.size());
                break;
            }
            case Value::Type::Blob: {
                auto bv = v.get<BufferView>();
                b.buffer_type   = MYSQL_TYPE_BLOB;
                b.buffer        = const_cast<void*>(static_cast<const void*>(bv.data()));
                b.buffer_length = static_cast<unsigned long>(bv.size());
                break;
            }
        }
    }

    if (!params.empty() && mysql_stmt_bind_param(stmt, binds.data()))
        return unexpected(db_error(DbError::QueryFailed));

    if (mysql_stmt_execute(stmt))
        return unexpected(db_error(mysql_stmt_transient_or(stmt, DbError::QueryFailed)));

    const uint64_t affected = static_cast<uint64_t>(mysql_stmt_affected_rows(stmt));
    const uint64_t last_id  = static_cast<uint64_t>(mysql_stmt_insert_id(stmt));

    MYSQL_RES* meta = mysql_stmt_result_metadata(stmt);
    if (!meta) {
        return std::make_unique<MysqlResultSet>(
            std::vector<std::vector<CellData>>{},
            std::make_shared<std::vector<std::string>>(),
            affected, last_id);
    }

    const unsigned int ncols  = mysql_num_fields(meta);
    MYSQL_FIELD*       fields = mysql_fetch_fields(meta);

    auto col_names = std::make_shared<std::vector<std::string>>();
    col_names->reserve(ncols);
    for (unsigned int i = 0; i < ncols; ++i)
        col_names->emplace_back(fields[i].name ? fields[i].name : "");

    // Result binding (received in TEXT form)
    std::vector<MYSQL_BIND>        res_binds(ncols);
    std::vector<std::vector<char>> col_bufs(ncols);
    std::vector<unsigned long>     col_lens(ncols, 0);
    auto col_nulls = std::make_unique<MyBool[]>(ncols); // bool[]/my_bool[], all false

    for (unsigned int i = 0; i < ncols; ++i) {
        auto& b = res_binds[i];
        std::memset(&b, 0, sizeof(b));
        const unsigned long init_sz =
            fields[i].max_length > 0 ? fields[i].max_length + 1 : 256;
        col_bufs[i].resize(init_sz);
        b.buffer_type   = MYSQL_TYPE_STRING;
        b.buffer        = col_bufs[i].data();
        b.buffer_length = static_cast<unsigned long>(col_bufs[i].size());
        b.length        = &col_lens[i];
        b.is_null       = &col_nulls[i];
    }

    if (mysql_stmt_bind_result(stmt, res_binds.data())) {
        mysql_free_result(meta);
        return unexpected(db_error(DbError::QueryFailed));
    }

    if (mysql_stmt_store_result(stmt)) {
        mysql_free_result(meta);
        return unexpected(db_error(DbError::QueryFailed));
    }

    // Scratch buffer for columns whose value exceeds the bound buffer (see below).
    // It grows monotonically and is reused across cells; it is deliberately NOT one
    // of the bound col_bufs.
    std::vector<char> scratch;

    std::vector<std::vector<CellData>> rows;
    int fetch_rc;
    while ((fetch_rc = mysql_stmt_fetch(stmt)) == 0 || fetch_rc == MYSQL_DATA_TRUNCATED) {
        std::vector<CellData> row;
        row.reserve(ncols);
        for (unsigned int i = 0; i < ncols; ++i) {
            if (col_nulls[i]) {
                row.push_back(CellData{});
                continue;
            }
            // The library captured the bound buffer pointers in mysql_stmt_bind_result;
            // it writes into THOSE on every mysql_stmt_fetch.  We therefore must never
            // reallocate a bound col_bufs[i] inside this loop — doing so frees a buffer
            // the library still holds, so the next fetch is a use-after-free.  When a
            // value was truncated (its true length, reported via col_lens[i], exceeds
            // the bound buffer), re-fetch the full value into a separate scratch buffer
            // through a local MYSQL_BIND; the bound buffers stay untouched and valid.
            if (col_lens[i] > col_bufs[i].size()) {
                if (scratch.size() < col_lens[i]) scratch.resize(col_lens[i]);
                MYSQL_BIND    rb{};
                unsigned long rlen   = 0;
                MyBool        rnull  = 0;
                MyBool        rerror = 0;
                std::memset(&rb, 0, sizeof(rb));
                rb.buffer_type   = MYSQL_TYPE_STRING;
                rb.buffer        = scratch.data();
                rb.buffer_length = col_lens[i];
                rb.length        = &rlen;
                rb.is_null       = &rnull;
                rb.error         = &rerror;
                if (mysql_stmt_fetch_column(stmt, &rb, i, 0) == 0)
                    row.push_back(cell_from_text(scratch.data(), col_lens[i], &fields[i]));
                else
                    row.push_back(CellData{}); // re-fetch failed → treat as NULL
            } else {
                row.push_back(cell_from_text(col_bufs[i].data(), col_lens[i], &fields[i]));
            }
        }
        rows.push_back(std::move(row));
    }

    mysql_free_result(meta);
    return std::make_unique<MysqlResultSet>(
        std::move(rows), std::move(col_names), affected, last_id);
}

// ─── Query execution helper (prepare → exec_stmt → close) ─────────────────────

static Result<std::unique_ptr<IResultSet>>
exec_query(MYSQL* conn, std::string_view sql, std::span<const Value> params) {
    const auto converted = convert_placeholders(sql);

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt)
        return unexpected(db_error(DbError::QueryFailed));

    if (mysql_stmt_prepare(stmt, converted.c_str(),
                           static_cast<unsigned long>(converted.size()))) {
        mysql_stmt_close(stmt);
        return unexpected(db_error(DbError::QueryFailed));
    }

    auto result = exec_stmt(stmt, params);
    mysql_stmt_close(stmt);
    return result;
}

// ─── Streaming query helper (prepare → execute → stream rows) ─────────────────
// One-shot statement owned by the returned MysqlStreamResultSet. DML (no result
// set) is executed eagerly and returned as a buffered empty result; SELECTs use a
// read-only server-side cursor and stream rows lazily.
static Result<std::unique_ptr<IResultSet>>
exec_query_streaming(std::shared_ptr<MysqlSlot> slot, std::string_view sql,
                     std::span<const Value> params) {
    const auto converted = convert_placeholders(sql);

    MYSQL_STMT* stmt = mysql_stmt_init(slot->conn);
    if (!stmt) return unexpected(db_error(DbError::QueryFailed));

    if (mysql_stmt_prepare(stmt, converted.c_str(),
                           static_cast<unsigned long>(converted.size()))) {
        const DbError e = mysql_stmt_transient_or(stmt, DbError::QueryFailed);
        mysql_stmt_close(stmt);
        return unexpected(db_error(e));
    }

    // Read-only server-side cursor for result-producing statements so rows are
    // streamed rather than buffered client-side. (field_count==0 → DML, no cursor.)
    if (mysql_stmt_field_count(stmt) > 0) {
        unsigned long cursor_type = CURSOR_TYPE_READ_ONLY;
        mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, &cursor_type);
    }

    // Bind parameters (zero-copy; num_bufs/binds outlive execute()).
    std::vector<ParamBuf>   num_bufs(params.size());
    std::vector<MYSQL_BIND> binds(params.size());
    for (std::size_t i = 0; i < params.size(); ++i) {
        auto& b = binds[i]; auto& buf = num_bufs[i]; const auto& v = params[i];
        std::memset(&b, 0, sizeof(b));
        switch (v.type()) {
            case Value::Type::Null: b.buffer_type = MYSQL_TYPE_NULL; break;
            case Value::Type::Bool:
            case Value::Type::Int64:
                buf.i64 = v.get<int64_t>(); b.buffer_type = MYSQL_TYPE_LONGLONG;
                b.buffer = &buf.i64; b.buffer_length = sizeof(int64_t); break;
            case Value::Type::Float64:
                buf.f64 = v.get<double>(); b.buffer_type = MYSQL_TYPE_DOUBLE;
                b.buffer = &buf.f64; b.buffer_length = sizeof(double); break;
            case Value::Type::Text: {
                auto sv = v.get<std::string_view>();
                b.buffer_type = MYSQL_TYPE_STRING;
                b.buffer = const_cast<void*>(static_cast<const void*>(sv.data()));
                b.buffer_length = static_cast<unsigned long>(sv.size()); break;
            }
            case Value::Type::Blob: {
                auto bv = v.get<BufferView>();
                b.buffer_type = MYSQL_TYPE_BLOB;
                b.buffer = const_cast<void*>(static_cast<const void*>(bv.data()));
                b.buffer_length = static_cast<unsigned long>(bv.size()); break;
            }
        }
    }
    if (!params.empty() && mysql_stmt_bind_param(stmt, binds.data())) {
        mysql_stmt_close(stmt);
        return unexpected(db_error(DbError::QueryFailed));
    }

    if (mysql_stmt_execute(stmt)) { // eager — DML side effects happen here
        const DbError e = mysql_stmt_transient_or(stmt, DbError::QueryFailed);
        mysql_stmt_close(stmt);
        return unexpected(db_error(e));
    }

    const uint64_t affected = static_cast<uint64_t>(mysql_stmt_affected_rows(stmt));
    const uint64_t last_id  = static_cast<uint64_t>(mysql_stmt_insert_id(stmt));

    MYSQL_RES* meta = mysql_stmt_result_metadata(stmt);
    if (!meta) { // no result set (DML) — buffered empty result
        mysql_stmt_close(stmt);
        return std::make_unique<MysqlResultSet>(
            std::vector<std::vector<CellData>>{},
            std::make_shared<std::vector<std::string>>(), affected, last_id);
    }

    const unsigned int ncols  = mysql_num_fields(meta);
    MYSQL_FIELD*       fields = mysql_fetch_fields(meta);
    auto col_names = std::make_shared<std::vector<std::string>>();
    col_names->reserve(ncols);
    for (unsigned int i = 0; i < ncols; ++i)
        col_names->emplace_back(fields[i].name ? fields[i].name : "");

    auto rs = std::make_unique<MysqlStreamResultSet>(
        std::move(slot), stmt, meta, std::move(col_names), affected, last_id);
    if (!rs->bind()) // rs destructor frees meta + closes stmt
        return unexpected(db_error(DbError::QueryFailed));
    return rs;
}

// ─── Statement ────────────────────────────────────────────────────────────────
// Holds a shared_ptr<MysqlSlot> keepalive (so the MYSQL*/mutex outlive it); db_
// and mx_ are cached from the slot for unchanged method bodies.

class MysqlStatement final : public IStatement {
public:
    MysqlStatement(std::shared_ptr<MysqlSlot> slot, std::string sql)
        : slot_(std::move(slot)), db_(slot_->conn), sql_(std::move(sql)),
          mx_(slot_->mutex) {}

    ~MysqlStatement() override {
        if (stmt_) mysql_stmt_close(stmt_);
    }

    Task<Result<std::unique_ptr<IResultSet>>>
    execute(std::span<const Value> params) override {
        std::lock_guard lock{mx_};
        if (!ensure_prepared())
            co_return unexpected(db_error(DbError::PrepareStatementFailed));
        mysql_stmt_reset(stmt_);
        co_return exec_stmt(stmt_, params);
    }

    Task<Result<uint64_t>>
    execute_dml(std::span<const Value> params) override {
        auto rs = co_await execute(params);
        if (!rs) co_return unexpected(rs.error());
        co_return (*rs)->affected_rows();
    }

private:
    bool ensure_prepared() {
        if (stmt_) return true;
        const auto converted = convert_placeholders(sql_);
        stmt_ = mysql_stmt_init(db_);
        if (!stmt_) return false;
        if (mysql_stmt_prepare(stmt_, converted.c_str(),
                               static_cast<unsigned long>(converted.size()))) {
            mysql_stmt_close(stmt_); stmt_ = nullptr;
            return false;
        }
        return true;
    }

    std::shared_ptr<MysqlSlot> slot_; // keepalive — declared first (init order)
    MYSQL_STMT* stmt_{nullptr};
    MYSQL*      db_;
    std::string sql_;
    std::mutex& mx_;
};

// ─── Transaction ──────────────────────────────────────────────────────────────

class MysqlTransaction final : public ITransaction {
public:
    explicit MysqlTransaction(std::shared_ptr<MysqlSlot> slot)
        : slot_(std::move(slot)), db_(slot_->conn), mx_(slot_->mutex) {}

    Task<Result<void>> commit() override {
        std::lock_guard lock{mx_};
        auto r = exec_sql("COMMIT");
        slot_->in_txn = false; // transaction finalized (even on error: it is over)
        co_return r;
    }
    Task<Result<void>> rollback() override {
        std::lock_guard lock{mx_};
        auto r = exec_sql("ROLLBACK");
        slot_->in_txn = false;
        co_return r;
    }
    Task<Result<void>> savepoint(std::string_view name) override {
        if (!db_detail::is_safe_ident(name))
            co_return unexpected(db_error(DbError::QueryFailed));
        std::lock_guard lock{mx_};
        co_return exec_sql("SAVEPOINT " + std::string(name));
    }
    Task<Result<void>> rollback_to(std::string_view name) override {
        if (!db_detail::is_safe_ident(name))
            co_return unexpected(db_error(DbError::QueryFailed));
        std::lock_guard lock{mx_};
        co_return exec_sql("ROLLBACK TO SAVEPOINT " + std::string(name));
    }
    Task<Result<uint64_t>>
    execute(std::string_view sql, std::span<const Value> params) override {
        std::lock_guard lock{mx_};
        auto rs = exec_query(db_, sql, params);
        if (!rs) co_return unexpected(rs.error());
        co_return (*rs)->affected_rows();
    }

private:
    Result<void> exec_sql(const std::string& sql) {
        if (mysql_query(db_, sql.c_str()))
            return unexpected(db_error(mysql_transient_or(db_, DbError::TransactionFailed)));
        return {};
    }

    std::shared_ptr<MysqlSlot> slot_; // keepalive — declared first (init order)
    MYSQL*      db_;
    std::mutex& mx_;
};

// ─── Connection ───────────────────────────────────────────────────────────────

class MysqlConnection final : public IConnection {
public:
    MysqlConnection(std::shared_ptr<MysqlSlot> slot, size_t slot_idx,
                    MysqlConnectionPool* pool)
        : slot_(std::move(slot)), db_(slot_->conn), mx_(slot_->mutex),
          slot_idx_(slot_idx), pool_(pool) {}

    ~MysqlConnection() override;  // defined below (after MysqlConnectionPool is complete)

    ConnectionState state() const noexcept override { return state_; }

    Task<Result<std::unique_ptr<IStatement>>>
    prepare(std::string_view sql) override {
        co_return std::make_unique<MysqlStatement>(slot_, std::string(sql));
    }

    Task<Result<std::unique_ptr<IResultSet>>>
    query(std::string_view sql, std::span<const Value> params) override {
        std::lock_guard lock{mx_};
        co_return exec_query_streaming(slot_, sql, params);
    }

    Task<Result<std::unique_ptr<ITransaction>>>
    begin(IsolationLevel level) override {
        static constexpr std::string_view kLevel[] = {
            "READ UNCOMMITTED", "READ COMMITTED",
            "REPEATABLE READ",  "SERIALIZABLE",
        };
        const auto li = static_cast<uint8_t>(level);
        {
            std::lock_guard lock{mx_};
            const std::string set_sql =
                std::string("SET TRANSACTION ISOLATION LEVEL ")
                + std::string(kLevel[li < 4 ? li : 1]);
            // Surface a failed isolation-level set instead of silently running at
            // the session default (a SERIALIZABLE request must not be downgraded).
            if (mysql_query(db_, set_sql.c_str()))
                co_return unexpected(db_error(DbError::TransactionFailed));
            if (mysql_query(db_, "START TRANSACTION"))
                co_return unexpected(db_error(DbError::TransactionFailed));
            slot_->in_txn = true;
        }
        state_ = ConnectionState::Transaction;
        co_return std::make_unique<MysqlTransaction>(slot_);
    }

    Task<Result<void>> close() override {
        state_ = ConnectionState::Idle;
        co_return {};
    }

    Task<bool> ping() override {
        std::lock_guard lock{mx_};
        co_return mysql_ping(db_) == 0;
    }

private:
    std::shared_ptr<MysqlSlot> slot_; // keepalive — declared first (init order)
    MYSQL*               db_;
    std::mutex&          mx_;
    size_t               slot_idx_;
    MysqlConnectionPool* pool_;
    ConnectionState      state_{ConnectionState::Idle};
};

// ─── ConnectionPool ───────────────────────────────────────────────────────────

class MysqlConnectionPool final : public IConnectionPool {
public:
    MysqlConnectionPool(MysqlDsn dsn, PoolConfig config)
        : dsn_(std::move(dsn))
        , max_size_(config.max_size > 0 ? config.max_size : 16)
        , query_timeout_ms_(config.query_timeout_ms) {
        const size_t min_sz = config.min_size > 0 ? config.min_size : 2;
        std::lock_guard lock{mutex_};
        for (size_t i = 0; i < min_sz; ++i) {
            MYSQL* c = create_connection(dsn_, query_timeout_ms_);
            if (!c) break;
            idle_.push(slots_.size());
            slots_.push_back(std::make_shared<MysqlSlot>(c));
        }
    }

    ~MysqlConnectionPool() override = default;

    [[nodiscard]] bool is_valid() const noexcept { return !slots_.empty(); }

    Task<Result<std::unique_ptr<IConnection>>> acquire() override {
        std::unique_lock lock{mutex_};

        // 1. idle slot
        if (!idle_.empty()) {
            const size_t idx = idle_.front(); idle_.pop();
            lock.unlock();

            std::shared_ptr<MysqlSlot> slot = slots_[idx];
            // Only ping connections idle for more than 30s (avoid pinging every acquire)
            if (slot->needs_liveness_check() && mysql_ping(slot->conn) != 0) {
                MYSQL* fresh = create_connection(dsn_, query_timeout_ms_);
                if (!fresh) {
                    lock.lock(); idle_.push(idx);
                    co_return unexpected(db_error(DbError::ConnectionFailed));
                }
                mysql_close(slot->conn);
                slot->conn = fresh;
            }
            active_.fetch_add(1, std::memory_order_relaxed);
            co_return std::make_unique<MysqlConnection>(std::move(slot), idx, this);
        }

        // 2. create a new slot
        if (slots_.size() < max_size_) {
            const size_t idx = slots_.size();
            slots_.push_back(nullptr); // reserve the index
            lock.unlock();

            MYSQL* c = create_connection(dsn_, query_timeout_ms_);
            if (!c) {
                lock.lock(); slots_.pop_back();
                co_return unexpected(db_error(DbError::ConnectionFailed));
            }
            auto slot = std::make_shared<MysqlSlot>(c);
            lock.lock();
            slots_[idx] = slot;
            lock.unlock();
            active_.fetch_add(1, std::memory_order_relaxed);
            co_return std::make_unique<MysqlConnection>(std::move(slot), idx, this);
        }

        // 3. pool exhausted → immediate error (synchronous driver)
        co_return unexpected(db_error(DbError::PoolExhausted));
    }

    void release(size_t idx) noexcept {
        active_.fetch_sub(1, std::memory_order_relaxed);
        std::lock_guard lock{mutex_};
        // If the pool was drained while this connection was checked out, its slot
        // entry is gone — there is nothing to return to the idle list. The
        // connection's own shared_ptr<MysqlSlot> already owns (and will free) the
        // underlying MYSQL* + mutex, so no use-after-free occurs.
        if (idx >= slots_.size() || !slots_[idx]) return;
        slots_[idx]->mark_idle();
        idle_.push(idx);
    }

    // return_connection is handled by the unique_ptr destructor path:
    // ~MysqlConnection → release().
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
    MysqlDsn                                  dsn_;
    size_t                                    max_size_;
    unsigned                                  query_timeout_ms_{0};
    mutable std::mutex                        mutex_;
    // shared_ptr so a connection checked out of the pool keeps its MysqlSlot
    // (MYSQL* + mutex) alive even if drain() clears the pool concurrently —
    // otherwise a live connection would dereference a freed Slot (use-after-free).
    std::vector<std::shared_ptr<MysqlSlot>>   slots_;
    std::queue<size_t>                        idle_;
    std::atomic<size_t>                       active_{0};
};

// ─── MysqlConnection::~MysqlConnection implementation ─────────────────────────

MysqlConnection::~MysqlConnection() {
    // Roll back a transaction the holder left open (begin() with no commit/
    // rollback) before the slot returns to the pool, so transaction state never
    // bleeds into the next user of this pooled connection.  Only touches the wire
    // when in_txn is set, so the normal path costs no extra round-trip.
    if (slot_ && slot_->conn) {
        std::lock_guard lk{slot_->mutex};
        if (slot_->in_txn) {
            mysql_query(slot_->conn, "ROLLBACK");
            slot_->in_txn = false;
        }
    }
    if (pool_) pool_->release(slot_idx_);
}

// ─── Driver ───────────────────────────────────────────────────────────────────

class MysqlDriver final : public IDBDriver {
public:
    MysqlDriver()  { mysql_library_init(0, nullptr, nullptr); }
    ~MysqlDriver() override { mysql_library_end(); }

    std::string_view driver_name() const noexcept override { return "mysql"; }

    bool accepts(std::string_view dsn) const noexcept override {
        return dsn.starts_with("mysql://") || dsn.starts_with("mariadb://");
    }

    Task<Result<std::unique_ptr<IConnectionPool>>>
    pool(std::string_view dsn, PoolConfig config) override {
        auto parsed = parse_dsn(dsn);
        auto p = std::make_unique<MysqlConnectionPool>(std::move(parsed),
                                                        std::move(config));
        if (!p->is_valid())
            co_return unexpected(db_error(DbError::ConnectionFailed));
        co_return p;
    }
};

} // anonymous namespace

std::unique_ptr<qbuem::db::IDBDriver> make_mysql_driver() {
    return std::make_unique<MysqlDriver>();
}

} // namespace qbuem_routine
