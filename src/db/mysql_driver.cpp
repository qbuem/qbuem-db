#include "mysql_driver.hpp"

#include <mysql/mysql.h>
#include <qbuem/core/task.hpp>
#include <qbuem/db/driver.hpp>
#include <qbuem/db/value.hpp>

#include <atomic>
#include <charconv>
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

// ─── 에러 헬퍼 ───────────────────────────────────────────────────────────────

struct MysqlErrorCategory : std::error_category {
    const char* name() const noexcept override { return "mysql"; }
    std::string message(int code) const override {
        return std::format("mysql error: {}", code);
    }
};

inline std::error_code mysql_error_code(unsigned int code = 1) {
    static MysqlErrorCategory cat;
    return {static_cast<int>(code), cat};
}

// ─── DSN 파서 ────────────────────────────────────────────────────────────────

struct MysqlDsn {
    std::string user;
    std::string password;
    std::string host;
    uint16_t    port{3306};
    std::string dbname;
    bool        use_ssl{false};
};

/// mysql://user:pass@host:port/dbname[?ssl=true]
static MysqlDsn parse_dsn(std::string_view dsn) {
    MysqlDsn result;

    // strip scheme
    if (dsn.starts_with("mysql://"))  dsn.remove_prefix(8);
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

// ─── $N → ? 플레이스홀더 변환 ────────────────────────────────────────────────

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

// ─── CellData ─────────────────────────────────────────────────────────────────

struct CellData {
    Value::Type type{Value::Type::Null};
    int64_t     i64{0};
    double      f64{0.0};
    std::string text;
};

// ─── 커넥션 생성 헬퍼 ─────────────────────────────────────────────────────────

static MYSQL* create_connection(const MysqlDsn& dsn) {
    MYSQL* conn = mysql_init(nullptr);
    if (!conn) return nullptr;

    // UTF8MB4 문자셋 설정
    mysql_options(conn, MYSQL_SET_CHARSET_NAME, "utf8mb4");
    // 재연결 허용
    bool reconnect = true;
    mysql_options(conn, MYSQL_OPT_RECONNECT, &reconnect);

    if (dsn.use_ssl) {
        mysql_options(conn, MYSQL_OPT_SSL_ENFORCE, &reconnect);
    }

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
    return conn;
}

// ─── 결과 읽기 ────────────────────────────────────────────────────────────────

static CellData cell_from_field(MYSQL_ROW row, MYSQL_FIELD* field,
                                 unsigned long len, int col) {
    CellData c;
    if (!row[col]) {
        c.type = Value::Type::Null;
        return c;
    }

    const char* val = row[col];
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
            // from_chars for float is C++17, use stod for compatibility
            try { c.f64 = std::stod(std::string(val, len)); } catch (...) {}
            break;
        case MYSQL_TYPE_BIT:
            c.type = Value::Type::Int64;
            // BIT fields come as binary
            c.i64 = 0;
            for (unsigned long i = 0; i < len; ++i)
                c.i64 = (c.i64 << 8) | static_cast<uint8_t>(val[i]);
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
                return BufferView{reinterpret_cast<const std::byte*>(c.text.data()),
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

// ─── 쿼리 실행 헬퍼 ──────────────────────────────────────────────────────────

static Result<std::unique_ptr<IResultSet>>
exec_query(MYSQL* conn, std::string_view sql, std::span<const Value> params) {
    // $N → ? 변환
    const auto converted = convert_placeholders(sql);

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt)
        return unexpected(mysql_error_code(mysql_errno(conn)));

    if (mysql_stmt_prepare(stmt, converted.c_str(),
                           static_cast<unsigned long>(converted.size()))) {
        unsigned int err = mysql_stmt_errno(stmt);
        mysql_stmt_close(stmt);
        return unexpected(mysql_error_code(err));
    }

    // 파라미터 바인딩
    std::vector<MYSQL_BIND> binds(params.size());
    std::vector<std::string> str_bufs(params.size());

    for (std::size_t i = 0; i < params.size(); ++i) {
        auto& b = binds[i];
        const auto& v = params[i];
        std::memset(&b, 0, sizeof(b));
        switch (v.type()) {
            case Value::Type::Null:
                b.buffer_type = MYSQL_TYPE_NULL;
                break;
            case Value::Type::Bool:
            case Value::Type::Int64: {
                static thread_local int64_t ibuf;
                ibuf = v.get<int64_t>();
                b.buffer_type = MYSQL_TYPE_LONGLONG;
                b.buffer       = &ibuf;
                b.buffer_length = sizeof(int64_t);
                break;
            }
            case Value::Type::Float64: {
                static thread_local double dbuf;
                dbuf = v.get<double>();
                b.buffer_type = MYSQL_TYPE_DOUBLE;
                b.buffer       = &dbuf;
                b.buffer_length = sizeof(double);
                break;
            }
            case Value::Type::Text: {
                str_bufs[i] = std::string{v.get<std::string_view>()};
                b.buffer_type   = MYSQL_TYPE_STRING;
                b.buffer        = str_bufs[i].data();
                b.buffer_length = static_cast<unsigned long>(str_bufs[i].size());
                break;
            }
            case Value::Type::Blob: {
                auto bv = v.get<BufferView>();
                str_bufs[i].assign(reinterpret_cast<const char*>(bv.data()), bv.size());
                b.buffer_type   = MYSQL_TYPE_BLOB;
                b.buffer        = str_bufs[i].data();
                b.buffer_length = static_cast<unsigned long>(str_bufs[i].size());
                break;
            }
        }
    }

    if (!params.empty() && mysql_stmt_bind_param(stmt, binds.data())) {
        unsigned int err = mysql_stmt_errno(stmt);
        mysql_stmt_close(stmt);
        return unexpected(mysql_error_code(err));
    }

    if (mysql_stmt_execute(stmt)) {
        unsigned int err = mysql_stmt_errno(stmt);
        mysql_stmt_close(stmt);
        return unexpected(mysql_error_code(err));
    }

    uint64_t affected = static_cast<uint64_t>(mysql_stmt_affected_rows(stmt));
    uint64_t last_id  = static_cast<uint64_t>(mysql_stmt_insert_id(stmt));

    // 결과 메타데이터
    MYSQL_RES* meta = mysql_stmt_result_metadata(stmt);
    if (!meta) {
        mysql_stmt_close(stmt);
        return std::make_unique<MysqlResultSet>(
            std::vector<std::vector<CellData>>{},
            std::make_shared<std::vector<std::string>>(),
            affected, last_id);
    }

    const unsigned int ncols = mysql_num_fields(meta);
    auto col_names = std::make_shared<std::vector<std::string>>();
    col_names->reserve(ncols);
    MYSQL_FIELD* fields = mysql_fetch_fields(meta);
    for (unsigned int i = 0; i < ncols; ++i)
        col_names->emplace_back(fields[i].name ? fields[i].name : "");

    // 결과 바인딩
    std::vector<MYSQL_BIND> res_binds(ncols);
    std::vector<std::vector<char>> col_bufs(ncols);
    std::vector<unsigned long>     col_lens(ncols, 0);
    std::vector<my_bool>           col_nulls(ncols, 0);

    for (unsigned int i = 0; i < ncols; ++i) {
        auto& b = res_binds[i];
        std::memset(&b, 0, sizeof(b));
        col_bufs[i].resize(fields[i].max_length ? fields[i].max_length + 1 : 256);
        b.buffer_type   = MYSQL_TYPE_STRING;
        b.buffer        = col_bufs[i].data();
        b.buffer_length = static_cast<unsigned long>(col_bufs[i].size());
        b.length        = &col_lens[i];
        b.is_null       = &col_nulls[i];
    }

    if (mysql_stmt_bind_result(stmt, res_binds.data())) {
        mysql_free_result(meta);
        mysql_stmt_close(stmt);
        return unexpected(mysql_error_code(mysql_stmt_errno(stmt)));
    }

    if (mysql_stmt_store_result(stmt)) {
        mysql_free_result(meta);
        mysql_stmt_close(stmt);
        return unexpected(mysql_error_code(mysql_stmt_errno(stmt)));
    }

    std::vector<std::vector<CellData>> rows;
    while (mysql_stmt_fetch(stmt) == 0) {
        std::vector<CellData> row;
        row.reserve(ncols);
        for (unsigned int i = 0; i < ncols; ++i) {
            CellData c;
            if (col_nulls[i]) {
                c.type = Value::Type::Null;
            } else {
                // Re-fetch with correct length if truncated
                if (col_lens[i] > col_bufs[i].size()) {
                    col_bufs[i].resize(col_lens[i] + 1);
                    res_binds[i].buffer        = col_bufs[i].data();
                    res_binds[i].buffer_length = col_lens[i];
                    mysql_stmt_fetch_column(stmt, &res_binds[i], i, 0);
                }
                const std::string_view raw{col_bufs[i].data(), col_lens[i]};
                c = cell_from_field(
                    // adapt: create a temporary MYSQL_ROW-like interface
                    [](const std::string_view sv, MYSQL_FIELD* f, unsigned long l,
                       int /*col*/) -> CellData {
                        CellData cd;
                        switch (f->type) {
                            case MYSQL_TYPE_TINY:
                            case MYSQL_TYPE_SHORT:
                            case MYSQL_TYPE_INT24:
                            case MYSQL_TYPE_LONG:
                            case MYSQL_TYPE_LONGLONG:
                                cd.type = Value::Type::Int64;
                                std::from_chars(sv.data(), sv.data() + l, cd.i64);
                                break;
                            case MYSQL_TYPE_FLOAT:
                            case MYSQL_TYPE_DOUBLE:
                            case MYSQL_TYPE_DECIMAL:
                            case MYSQL_TYPE_NEWDECIMAL:
                                cd.type = Value::Type::Float64;
                                try { cd.f64 = std::stod(std::string(sv.data(), l)); }
                                catch (...) {}
                                break;
                            case MYSQL_TYPE_TINY_BLOB:
                            case MYSQL_TYPE_MEDIUM_BLOB:
                            case MYSQL_TYPE_LONG_BLOB:
                            case MYSQL_TYPE_BLOB:
                                if (f->flags & BINARY_FLAG) {
                                    cd.type = Value::Type::Blob;
                                    cd.text.assign(sv.data(), l);
                                } else {
                                    cd.type = Value::Type::Text;
                                    cd.text.assign(sv.data(), l);
                                }
                                break;
                            default:
                                cd.type = Value::Type::Text;
                                cd.text.assign(sv.data(), l);
                                break;
                        }
                        return cd;
                    }(raw, &fields[i], col_lens[i], static_cast<int>(i)));
            }
            row.push_back(std::move(c));
        }
        rows.push_back(std::move(row));
    }

    mysql_free_result(meta);
    mysql_stmt_close(stmt);
    return std::make_unique<MysqlResultSet>(
        std::move(rows), std::move(col_names), affected, last_id);
}

// ─── Statement ────────────────────────────────────────────────────────────────

class MysqlStatement final : public IStatement {
public:
    MysqlStatement(MYSQL* db, std::string sql, std::mutex& mx)
        : db_(db), sql_(std::move(sql)), mx_(mx) {}

    Task<Result<std::unique_ptr<IResultSet>>>
    execute(std::span<const Value> params) override {
        std::lock_guard lock{mx_};
        co_return exec_query(db_, sql_, params);
    }

    Task<Result<uint64_t>>
    execute_dml(std::span<const Value> params) override {
        auto rs = co_await execute(params);
        if (!rs) co_return unexpected(rs.error());
        co_return (*rs)->affected_rows();
    }

private:
    MYSQL*      db_;
    std::string sql_;
    std::mutex& mx_;
};

// ─── Transaction ──────────────────────────────────────────────────────────────

class MysqlTransaction final : public ITransaction {
public:
    MysqlTransaction(MYSQL* db, std::mutex& mx) : db_(db), mx_(mx) {}

    Task<Result<void>> commit() override {
        std::lock_guard lock{mx_};
        co_return exec_sql("COMMIT");
    }
    Task<Result<void>> rollback() override {
        std::lock_guard lock{mx_};
        co_return exec_sql("ROLLBACK");
    }
    Task<Result<void>> savepoint(std::string_view name) override {
        std::lock_guard lock{mx_};
        co_return exec_sql("SAVEPOINT " + std::string(name));
    }
    Task<Result<void>> rollback_to(std::string_view name) override {
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
            return unexpected(mysql_error_code(mysql_errno(db_)));
        return {};
    }

    MYSQL*      db_;
    std::mutex& mx_;
};

// ─── Connection ───────────────────────────────────────────────────────────────

class MysqlConnection final : public IConnection {
public:
    MysqlConnection(MYSQL* db, std::mutex& mx) : db_(db), mx_(mx) {}

    ConnectionState state() const noexcept override { return state_; }

    Task<Result<std::unique_ptr<IStatement>>>
    prepare(std::string_view sql) override {
        co_return std::make_unique<MysqlStatement>(db_, std::string(sql), mx_);
    }

    Task<Result<std::unique_ptr<IResultSet>>>
    query(std::string_view sql, std::span<const Value> params) override {
        std::lock_guard lock{mx_};
        co_return exec_query(db_, sql, params);
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
            const std::string set_sql = std::string("SET TRANSACTION ISOLATION LEVEL ")
                                      + std::string(kLevel[li < 4 ? li : 1]);
            mysql_query(db_, set_sql.c_str());
            if (mysql_query(db_, "START TRANSACTION"))
                co_return unexpected(mysql_error_code(mysql_errno(db_)));
        }
        state_ = ConnectionState::Transaction;
        co_return std::make_unique<MysqlTransaction>(db_, mx_);
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
    MYSQL*          db_;
    std::mutex&     mx_;
    ConnectionState state_{ConnectionState::Idle};
};

// ─── Slot (커넥션 슬롯) ───────────────────────────────────────────────────────

struct MysqlSlot {
    MYSQL*     conn{nullptr};
    std::mutex mutex;

    explicit MysqlSlot(MYSQL* c) noexcept : conn(c) {}
    ~MysqlSlot() { if (conn) { mysql_close(conn); conn = nullptr; } }

    MysqlSlot(const MysqlSlot&)            = delete;
    MysqlSlot& operator=(const MysqlSlot&) = delete;
};

// ─── ConnectionPool ───────────────────────────────────────────────────────────

class MysqlConnectionPool final : public IConnectionPool {
public:
    MysqlConnectionPool(MysqlDsn dsn, PoolConfig config)
        : dsn_(std::move(dsn))
        , max_size_(config.max_size > 0 ? config.max_size : 16) {
        const size_t min_sz = config.min_size > 0 ? config.min_size : 2;
        std::lock_guard lock{mutex_};
        for (size_t i = 0; i < min_sz; ++i) {
            MYSQL* c = create_connection(dsn_);
            if (!c) break;
            idle_.push(slots_.size());
            slots_.push_back(std::make_unique<MysqlSlot>(c));
        }
    }

    ~MysqlConnectionPool() override = default;

    bool is_valid() const noexcept { return !slots_.empty(); }

    Task<Result<std::unique_ptr<IConnection>>> acquire() override {
        std::unique_lock lock{mutex_};

        // 1. idle 슬롯
        if (!idle_.empty()) {
            const size_t idx = idle_.front(); idle_.pop();
            lock.unlock();
            auto* slot = slots_[idx].get();
            // 연결 유효성 확인 (ping)
            if (mysql_ping(slot->conn) != 0) {
                // 재연결 시도
                MYSQL* fresh = create_connection(dsn_);
                if (!fresh) {
                    lock.lock(); idle_.push(idx);
                    co_return unexpected(mysql_error_code(2003));
                }
                mysql_close(slot->conn);
                slot->conn = fresh;
            }
            active_.fetch_add(1, std::memory_order_relaxed);
            co_return std::make_unique<MysqlConnection>(slot->conn, slot->mutex);
        }

        // 2. 새 슬롯
        if (slots_.size() < max_size_) {
            lock.unlock();
            MYSQL* c = create_connection(dsn_);
            if (!c) co_return unexpected(mysql_error_code(2003));
            lock.lock();
            const size_t idx = slots_.size();
            slots_.push_back(std::make_unique<MysqlSlot>(c));
            lock.unlock();
            active_.fetch_add(1, std::memory_order_relaxed);
            co_return std::make_unique<MysqlConnection>(
                slots_[idx]->conn, slots_[idx]->mutex);
        }

        // 3. 풀 소진 — 즉시 에러 반환 (MySQL은 동기 드라이버)
        co_return unexpected(mysql_error_code(2003)); // CR_CONN_HOST_ERROR
    }

    void return_connection(std::unique_ptr<IConnection>) noexcept override {
        // 슬롯을 직접 관리하지 않으므로 idle로 돌려보내지 않음
        // (단순화: 각 IConnection은 슬롯 인덱스를 보유하지 않음)
        active_.fetch_sub(1, std::memory_order_relaxed);
    }

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
    mutable std::mutex                        mutex_;
    std::vector<std::unique_ptr<MysqlSlot>>   slots_;
    std::queue<size_t>                        idle_;
    std::atomic<size_t>                       active_{0};
};

// ─── Driver ───────────────────────────────────────────────────────────────────

class MysqlDriver final : public IDBDriver {
public:
    MysqlDriver() { mysql_library_init(0, nullptr, nullptr); }
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
            co_return unexpected(mysql_error_code(2003));
        co_return p;
    }
};

} // anonymous namespace

std::unique_ptr<qbuem::db::IDBDriver> make_mysql_driver() {
    return std::make_unique<MysqlDriver>();
}

} // namespace qbuem_routine
