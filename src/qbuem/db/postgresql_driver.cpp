#include "postgresql_driver.hpp"
#include "db_error.hpp"

#include <libpq-fe.h>
#include <qbuem/core/reactor.hpp>
#include <qbuem/core/task.hpp>
#include <qbuem/db/driver.hpp>
#include <qbuem/db/value.hpp>

#include <atomic>
#include <bit>        // std::byteswap, std::endian (C++23)
#include <charconv>
#include <chrono>
#include <coroutine>
#include <cstring>
#include <fcntl.h>    // fcntl, O_NONBLOCK
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

using namespace qbuem;
using namespace qbuem::db;

namespace qbuem_routine {
namespace {

// ─── PostgreSQL OID 상수 ──────────────────────────────────────────────────────

static constexpr Oid OID_BOOL   = 16;
static constexpr Oid OID_BYTEA  = 17;
static constexpr Oid OID_INT8   = 20;
static constexpr Oid OID_INT2   = 21;
static constexpr Oid OID_INT4   = 23;
static constexpr Oid OID_FLOAT4 = 700;
static constexpr Oid OID_FLOAT8 = 701;

// ─── 바이너리 big-endian 역직렬화 ─────────────────────────────────────────────

template<std::integral T>
static T from_be(const char* data) noexcept {
    T v;
    std::memcpy(&v, data, sizeof(T));
    if constexpr (std::endian::native != std::endian::big)
        v = std::byteswap(v);
    return v;
}

// ─── CellData ─────────────────────────────────────────────────────────────────

struct CellData {
    Value::Type type = Value::Type::Null;
    int64_t     i64  = 0;
    double      f64  = 0.0;
    std::string text;
};

static CellData read_pg_column_binary(PGresult* res, int row, int col) noexcept {
    CellData c;
    if (PQgetisnull(res, row, col)) return c;

    const Oid   oid = PQftype(res, col);
    const char* val = PQgetvalue(res, row, col);
    const int   len = PQgetlength(res, row, col);

    switch (oid) {
        case OID_INT2:
            c.type = Value::Type::Int64; c.i64 = from_be<int16_t>(val); break;
        case OID_INT4:
            c.type = Value::Type::Int64; c.i64 = from_be<int32_t>(val); break;
        case OID_INT8:
            c.type = Value::Type::Int64; c.i64 = from_be<int64_t>(val); break;
        case OID_FLOAT4: {
            uint32_t bits = from_be<uint32_t>(val); float f;
            std::memcpy(&f, &bits, 4);
            c.type = Value::Type::Float64; c.f64 = static_cast<double>(f);
            break;
        }
        case OID_FLOAT8: {
            uint64_t bits = from_be<uint64_t>(val); double d;
            std::memcpy(&d, &bits, 8);
            c.type = Value::Type::Float64; c.f64 = d;
            break;
        }
        case OID_BOOL:
            c.type = Value::Type::Bool; c.i64 = (len > 0 && val[0] != 0) ? 1 : 0;
            break;
        case OID_BYTEA:
            c.type = Value::Type::Blob;
            c.text.assign(val, static_cast<size_t>(len));
            break;
        default:
            c.type = Value::Type::Text;
            c.text.assign(val, static_cast<size_t>(len));
            break;
    }
    return c;
}

// ─── PgParams — zero-alloc 파라미터 배열 ─────────────────────────────────────
//
// 설계 원칙:
//  • Text  → string_view 포인터 직접 사용 (zero-copy, 할당 없음)
//  • Null  → nullptr (할당 없음)
//  • Bool  → 정적 리터럴 "t"/"f" (할당 없음)
//  • Int64 / Float64 → 스택 고정 버퍼에 to_chars (할당 없음)
//  • Blob  → hex 변환만 blob_bufs_ 에 std::string 할당 (드문 경우)
//
// ≤16 파라미터: 완전 스택 (힙 0회)
// >16 파라미터: values/lengths/formats + num_bufs 만 힙 1회

struct PgParams {
    static constexpr size_t kInline   = 16;
    static constexpr size_t kNumBufSz = 32; // int64_t 최대 20자 + double 최대 ~24자

    // ── 인라인 스토리지 (≤kInline) ──────────────────────────────────────────
    char        num_buf_[kInline][kNumBufSz]{};  // 숫자/bool 직렬화 버퍼
    const char* values_inline_[kInline]{};
    int         lengths_inline_[kInline]{};
    int         formats_inline_[kInline]{};

    // ── 힙 스토리지 (>kInline) ──────────────────────────────────────────────
    std::vector<std::array<char, kNumBufSz>> num_buf_heap_;
    std::vector<const char*>                 values_heap_;
    std::vector<int>                         lengths_heap_;
    std::vector<int>                         formats_heap_;

    // ── Blob 변환 버퍼 (드문 경우만) ───────────────────────────────────────
    std::vector<std::string> blob_bufs_;

    const char** values_{nullptr};
    int*         lengths_{nullptr};
    int*         formats_{nullptr};
    int          count_{0};

    void build(std::span<const Value> params) {
        count_          = static_cast<int>(params.size());
        const size_t n  = static_cast<size_t>(count_);

        char* nbuf_base = nullptr;
        if (n <= kInline) {
            values_  = values_inline_;
            lengths_ = lengths_inline_;
            formats_ = formats_inline_;
            nbuf_base = num_buf_[0];
        } else {
            num_buf_heap_.resize(n);
            values_heap_.resize(n);
            lengths_heap_.resize(n);
            formats_heap_.resize(n);
            values_  = values_heap_.data();
            lengths_ = lengths_heap_.data();
            formats_ = formats_heap_.data();
        }

        blob_bufs_.reserve(n); // Blob 있을 때 재할당 방지 (포인터 안정성)

        static constexpr char kHex[] = "0123456789abcdef";

        for (int i = 0; i < count_; ++i) {
            formats_[i]       = 0;
            char* nb          = (n <= kInline) ? num_buf_[i] : num_buf_heap_[i].data();
            const auto& v     = params[static_cast<size_t>(i)];

            switch (v.type()) {
                case Value::Type::Null:
                    values_[i] = nullptr; lengths_[i] = 0;
                    break;
                case Value::Type::Text: {
                    // zero-copy: params span은 PQsend* 완료까지 유효
                    auto sv    = v.get<std::string_view>();
                    values_[i] = sv.data();
                    lengths_[i]= static_cast<int>(sv.size());
                    break;
                }
                case Value::Type::Bool:
                    values_[i] = v.get<int64_t>() ? "t" : "f";
                    lengths_[i]= 1;
                    break;
                case Value::Type::Int64: {
                    auto [ptr, _] = std::to_chars(nb, nb + kNumBufSz, v.get<int64_t>());
                    values_[i] = nb;
                    lengths_[i]= static_cast<int>(ptr - nb);
                    break;
                }
                case Value::Type::Float64: {
                    // chars_format::general: 최단 round-trip 표현
                    auto [ptr, _] = std::to_chars(nb, nb + kNumBufSz,
                                                  v.get<double>(),
                                                  std::chars_format::general);
                    values_[i] = nb;
                    lengths_[i]= static_cast<int>(ptr - nb);
                    break;
                }
                case Value::Type::Blob: {
                    auto bv = v.get<BufferView>();
                    blob_bufs_.emplace_back();
                    auto& s = blob_bufs_.back();
                    s.reserve(2 + bv.size() * 2);
                    s = "\\x";
                    for (auto b : bv) {
                        const uint8_t byte = static_cast<uint8_t>(b);
                        s += kHex[(byte >> 4) & 0xF];
                        s += kHex[byte & 0xF];
                    }
                    values_[i] = s.c_str();
                    lengths_[i]= static_cast<int>(s.size());
                    break;
                }
            }
        }
    }

    int          n()       const noexcept { return count_; }
    const char** values()  const noexcept { return values_; }
    const int*   lengths() const noexcept { return lengths_; }
    const int*   formats() const noexcept { return formats_; }
};

// ─── Row / ResultSet ──────────────────────────────────────────────────────────

class PgRow final : public IRow {
public:
    PgRow(std::shared_ptr<std::vector<std::string>> col_names,
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

class PgResultSet final : public IResultSet {
public:
    PgResultSet(std::vector<std::vector<CellData>> rows,
                std::shared_ptr<std::vector<std::string>> col_names,
                uint64_t affected, uint64_t last_id)
        : rows_(std::move(rows)), col_names_(std::move(col_names))
        , affected_(affected), last_id_(last_id) {}

    Task<const IRow*> next() override {
        if (cursor_ >= rows_.size()) co_return nullptr;
        current_ = std::make_unique<PgRow>(col_names_, std::move(rows_[cursor_++]));
        co_return current_.get();
    }
    uint64_t affected_rows()  const noexcept override { return affected_; }
    uint64_t last_insert_id() const noexcept override { return last_id_; }
private:
    std::vector<std::vector<CellData>>        rows_;
    std::shared_ptr<std::vector<std::string>> col_names_;
    size_t cursor_{0};
    std::unique_ptr<PgRow> current_;
    uint64_t affected_, last_id_;
};

static Result<std::unique_ptr<IResultSet>> build_result_set(PGresult* res) {
    const ExecStatusType status = PQresultStatus(res);
    if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
        PQclear(res); return unexpected(db_error(DbError::QueryFailed));
    }
    const int nrows = PQntuples(res), ncols = PQnfields(res);
    auto col_names = std::make_shared<std::vector<std::string>>();
    col_names->reserve(static_cast<size_t>(ncols));
    for (int i = 0; i < ncols; ++i)
        col_names->emplace_back(PQfname(res, i) ? PQfname(res, i) : "");

    std::vector<std::vector<CellData>> rows;
    rows.reserve(static_cast<size_t>(nrows));
    for (int r = 0; r < nrows; ++r) {
        std::vector<CellData> row;
        row.reserve(static_cast<size_t>(ncols));
        for (int c = 0; c < ncols; ++c)
            row.push_back(read_pg_column_binary(res, r, c));
        rows.push_back(std::move(row));
    }
    uint64_t affected = 0;
    if (const char* tag = PQcmdTuples(res); tag && *tag)
        std::from_chars(tag, tag + std::strlen(tag), affected);
    PQclear(res);
    return std::make_unique<PgResultSet>(std::move(rows), std::move(col_names),
                                         affected, 0);
}

// ─────────────────────────────────────────────────────────────────────────────
// 비동기 Awaiter — libpq 소켓을 Reactor 이벤트에 통합
// ─────────────────────────────────────────────────────────────────────────────

// ─── PgReadAwaiter ────────────────────────────────────────────────────────────
//
// [핵심 최적화] PQexecParams(블로킹) → PQsendQueryParams + co_await PgReadAwaiter
//
// PQisBusy() == true 이면 reactor 이벤트 등록 후 coroutine yield.
// 소켓이 readable 이 될 때마다 PQconsumeInput → PQisBusy 체크 반복.
// 결과 준비 완료 시 unregister_event → reactor->post(resume) 로 깨움.
// → reactor thread 를 전혀 block 하지 않음.

struct PgReadAwaiter {
    PGconn*   conn;
    PGresult* result{nullptr};

    bool await_ready() noexcept {
        PQconsumeInput(conn);
        if (PQisBusy(conn)) return false;
        result = PQgetResult(conn);
        while (PGresult* r = PQgetResult(conn)) PQclear(r); // 잔여 결과 drain
        return true;
    }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        auto* reactor = Reactor::current();
        const int fd  = PQsocket(conn);

        reactor->register_event(fd, EventType::Read,
            [this, h, fd, reactor](int) mutable {
                PQconsumeInput(conn);
                if (PQisBusy(conn)) return; // 데이터 더 필요, 이벤트 유지

                reactor->unregister_event(fd, EventType::Read);
                result = PQgetResult(conn);
                while (PGresult* r = PQgetResult(conn)) PQclear(r);
                reactor->post([h]() mutable { h.resume(); });
            });
    }

    PGresult* await_resume() noexcept { return result; }
};

// ─── PgFlushAwaiter ───────────────────────────────────────────────────────────
//
// PQflush() == 1 (송신 버퍼 가득) 이면 소켓 writable 이벤트를 기다린다.
// 실제로는 작은 쿼리에서 PQflush == 0 이 즉시 반환되므로 await_ready == true.

struct PgFlushAwaiter {
    PGconn* conn;

    bool await_ready() noexcept { return PQflush(conn) != 1; }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        auto* reactor = Reactor::current();
        const int fd  = PQsocket(conn);
        reactor->register_event(fd, EventType::Write,
            [h, fd, reactor](int) mutable {
                reactor->unregister_event(fd, EventType::Write);
                reactor->post([h]() mutable { h.resume(); });
            });
    }

    void await_resume() noexcept {}
};

// ─── async_flush — 송신 버퍼가 완전히 비워질 때까지 반복 flush ────────────────

static Task<bool> async_flush(PGconn* conn) {
    while (true) {
        const int rc = PQflush(conn);
        if (rc ==  0) co_return true;   // 전송 완료
        if (rc == -1) co_return false;  // 에러
        co_await PgFlushAwaiter{conn};  // 소켓 쓰기 대기 후 재시도
    }
}

// ─── 비동기 쿼리 헬퍼 ─────────────────────────────────────────────────────────
//
// PQsend* → async_flush → PgReadAwaiter 의 3단계 패턴을 묶음.
// 모두 resultFormat=1 (binary) 로 요청.

static Task<PGresult*> async_simple(PGconn* conn, const char* sql) {
    if (PQsendQuery(conn, sql) == 0) co_return nullptr;
    if (!co_await async_flush(conn))  co_return nullptr;
    co_return co_await PgReadAwaiter{conn};
}

static Task<PGresult*> async_params(PGconn* conn, const char* sql,
                                    const PgParams& pg) {
    if (PQsendQueryParams(conn, sql,
                          pg.n(), nullptr,
                          pg.values(), pg.lengths(), pg.formats(),
                          1 /* binary */) == 0)
        co_return nullptr;
    if (!co_await async_flush(conn)) co_return nullptr;
    co_return co_await PgReadAwaiter{conn};
}

static Task<PGresult*> async_prepared(PGconn* conn, const char* name,
                                      const PgParams& pg) {
    if (PQsendQueryPrepared(conn, name,
                            pg.n(),
                            pg.values(), pg.lengths(), pg.formats(),
                            1 /* binary */) == 0)
        co_return nullptr;
    if (!co_await async_flush(conn)) co_return nullptr;
    co_return co_await PgReadAwaiter{conn};
}

static Task<PGresult*> async_prepare(PGconn* conn, const char* name,
                                     const char* sql) {
    if (PQsendPrepare(conn, name, sql, 0, nullptr) == 0) co_return nullptr;
    if (!co_await async_flush(conn)) co_return nullptr;
    co_return co_await PgReadAwaiter{conn};
}

// ─── 연결 소켓 non-blocking 설정 ─────────────────────────────────────────────

static void make_nonblocking(PGconn* conn) noexcept {
    const int fd = PQsocket(conn);
    if (fd < 0) return;
    if (const int flags = fcntl(fd, F_GETFL, 0); flags >= 0)
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    PQsetnonblocking(conn, 1);
}

// ─── Slot ─────────────────────────────────────────────────────────────────────

struct Slot {
    using Clock = std::chrono::steady_clock;

    PGconn*          conn{nullptr};
    std::mutex       mutex;          // PQsend* 호출 직렬화 (비동기 대기 구간 제외)
    uint64_t         stmt_counter{0};
    Clock::time_point idle_since{Clock::now()};

    explicit Slot(PGconn* c) noexcept : conn(c) { make_nonblocking(c); }
    ~Slot() { if (conn) PQfinish(conn); }

    Slot(const Slot&)            = delete;
    Slot& operator=(const Slot&) = delete;

    void mark_idle() noexcept { idle_since = Clock::now(); }

    // 30초 이상 idle 상태 → 클라우드 방화벽/로드밸런서가 연결을 끊었을 수 있음
    [[nodiscard]] bool needs_liveness_check() const noexcept {
        return (Clock::now() - idle_since) > std::chrono::seconds{30};
    }
};

// ─── PgAcquireAwaiter — 풀 소진 시 대기 ─────────────────────────────────────
//
// [동시접속 최적화] acquire() 가 풀 소진을 만나면 즉시 에러 대신 yield.
// release() 가 호출될 때 대기 큐의 첫 waiter 에게 슬롯을 직접 전달하고
// 해당 waiter 의 reactor thread 에서 resume.
// → 불필요한 에러 반환 없이 max_size 개 동시 쿼리 지원.

class PgConnectionPool; // 전방 선언

class PgConnection; // forward declaration

struct PgAcquireAwaiter {
    PgConnectionPool*                     pool;
    Result<std::unique_ptr<IConnection>>  result{unexpected(db_error(DbError::ConnectionFailed))};

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept;   // 정의는 아래
    Result<std::unique_ptr<IConnection>> await_resume() noexcept {
        return std::move(result);
    }
};

// ─── Statement ────────────────────────────────────────────────────────────────

class PgStatement final : public IStatement {
public:
    PgStatement(Slot* slot, std::string name)
        : slot_(slot), name_(std::move(name)) {}

    ~PgStatement() override {
        // DEALLOCATE 는 동기 — 드라이버 소멸 경로이므로 빈도 낮음
        std::lock_guard lock{slot_->mutex};
        const std::string sql = "DEALLOCATE \"" + name_ + "\"";
        PGresult* res = PQexec(slot_->conn, sql.c_str());
        if (res) PQclear(res);
    }

    Task<Result<std::unique_ptr<IResultSet>>>
    execute(std::span<const Value> params) override {
        PgParams pg; pg.build(params);
        PGresult* res;
        {
            std::lock_guard lock{slot_->mutex};
            // PQsend* 만 락 보호; co_await 구간은 락 해제 상태
            if (PQsendQueryPrepared(slot_->conn, name_.c_str(),
                                    pg.n(), pg.values(), pg.lengths(),
                                    pg.formats(), 1) == 0)
                co_return unexpected(db_error(DbError::QueryFailed));
        }
        if (!co_await async_flush(slot_->conn)) co_return unexpected(db_error(DbError::QueryFailed));
        res = co_await PgReadAwaiter{slot_->conn};
        if (!res) co_return unexpected(db_error(DbError::QueryFailed));
        co_return build_result_set(res);
    }

    Task<Result<uint64_t>>
    execute_dml(std::span<const Value> params) override {
        auto rs_r = co_await execute(params);
        if (!rs_r) co_return unexpected(rs_r.error());
        co_return (*rs_r)->affected_rows();
    }

private:
    Slot*       slot_;
    std::string name_;
};

// ─── Transaction ──────────────────────────────────────────────────────────────

class PgTransaction final : public ITransaction {
public:
    explicit PgTransaction(Slot* slot) : slot_(slot) {}

    Task<Result<void>> commit()   override { co_return co_await exec_simple("COMMIT"); }
    Task<Result<void>> rollback() override { co_return co_await exec_simple("ROLLBACK"); }

    Task<Result<void>> savepoint(std::string_view name) override {
        co_return co_await exec_simple("SAVEPOINT \"" + std::string(name) + "\"");
    }
    Task<Result<void>> rollback_to(std::string_view name) override {
        co_return co_await exec_simple(
            "ROLLBACK TO SAVEPOINT \"" + std::string(name) + "\"");
    }

    Task<Result<uint64_t>>
    execute(std::string_view sql, std::span<const Value> params) override {
        PgParams pg; pg.build(params);
        PGresult* res;
        {
            std::lock_guard lock{slot_->mutex};
            if (PQsendQueryParams(slot_->conn, std::string(sql).c_str(),
                                  pg.n(), nullptr,
                                  pg.values(), pg.lengths(), pg.formats(), 1) == 0)
                co_return unexpected(db_error(DbError::QueryFailed));
        }
        if (!co_await async_flush(slot_->conn)) co_return unexpected(db_error(DbError::QueryFailed));
        res = co_await PgReadAwaiter{slot_->conn};
        if (!res) co_return unexpected(db_error(DbError::QueryFailed));
        auto r = build_result_set(res);
        if (!r) co_return unexpected(r.error());
        co_return (*r)->affected_rows();
    }

private:
    Task<Result<void>> exec_simple(const std::string& sql) {
        PGresult* res;
        {
            std::lock_guard lock{slot_->mutex};
            if (PQsendQuery(slot_->conn, sql.c_str()) == 0)
                co_return unexpected(db_error(DbError::TransactionFailed));
        }
        if (!co_await async_flush(slot_->conn)) co_return unexpected(db_error(DbError::TransactionFailed));
        res = co_await PgReadAwaiter{slot_->conn};
        if (!res) co_return unexpected(db_error(DbError::TransactionFailed));
        const ExecStatusType status = PQresultStatus(res);
        PQclear(res);
        if (status != PGRES_COMMAND_OK) co_return unexpected(db_error(DbError::TransactionFailed));
        co_return {};
    }

    Slot* slot_;
};

// ─── Connection ───────────────────────────────────────────────────────────────

class PgConnection final : public IConnection {
public:
    PgConnection(Slot* slot, size_t idx, PgConnectionPool* pool) noexcept
        : slot_(slot), idx_(idx), pool_(pool) {}

    ~PgConnection() override;

    ConnectionState state() const noexcept override { return state_; }

    Task<Result<std::unique_ptr<IStatement>>>
    prepare(std::string_view sql) override {
        std::string name;
        PGresult* res;
        {
            std::lock_guard lock{slot_->mutex};
            name = "s" + std::to_string(slot_->stmt_counter++);
            if (PQsendPrepare(slot_->conn, name.c_str(),
                              std::string(sql).c_str(), 0, nullptr) == 0)
                co_return unexpected(db_error(DbError::PrepareStatementFailed));
        }
        if (!co_await async_flush(slot_->conn)) co_return unexpected(db_error(DbError::PrepareStatementFailed));
        res = co_await PgReadAwaiter{slot_->conn};
        if (!res) co_return unexpected(db_error(DbError::PrepareStatementFailed));
        const ExecStatusType status = PQresultStatus(res);
        PQclear(res);
        if (status != PGRES_COMMAND_OK) co_return unexpected(db_error(DbError::PrepareStatementFailed));
        co_return std::make_unique<PgStatement>(slot_, std::move(name));
    }

    Task<Result<std::unique_ptr<IResultSet>>>
    query(std::string_view sql, std::span<const Value> params) override {
        PgParams pg; pg.build(params);
        PGresult* res;
        {
            std::lock_guard lock{slot_->mutex};
            if (PQsendQueryParams(slot_->conn, std::string(sql).c_str(),
                                  pg.n(), nullptr,
                                  pg.values(), pg.lengths(), pg.formats(), 1) == 0)
                co_return unexpected(db_error(DbError::QueryFailed));
        }
        if (!co_await async_flush(slot_->conn)) co_return unexpected(db_error(DbError::QueryFailed));
        res = co_await PgReadAwaiter{slot_->conn};
        if (!res) co_return unexpected(db_error(DbError::QueryFailed));
        co_return build_result_set(res);
    }

    Task<Result<std::unique_ptr<ITransaction>>>
    begin(IsolationLevel level) override {
        static constexpr std::string_view kLevel[] = {
            "READ UNCOMMITTED", "READ COMMITTED",
            "REPEATABLE READ",  "SERIALIZABLE",
        };
        const auto li = static_cast<uint8_t>(level);
        const std::string sql = std::string("BEGIN ISOLATION LEVEL ")
                              + std::string(kLevel[li < 4 ? li : 1]);
        PGresult* res;
        {
            std::lock_guard lock{slot_->mutex};
            if (PQsendQuery(slot_->conn, sql.c_str()) == 0)
                co_return unexpected(db_error(DbError::TransactionFailed));
        }
        if (!co_await async_flush(slot_->conn)) co_return unexpected(db_error(DbError::TransactionFailed));
        res = co_await PgReadAwaiter{slot_->conn};
        if (!res) co_return unexpected(db_error(DbError::TransactionFailed));
        const ExecStatusType status = PQresultStatus(res);
        PQclear(res);
        if (status != PGRES_COMMAND_OK) co_return unexpected(db_error(DbError::TransactionFailed));
        state_ = ConnectionState::Transaction;
        co_return std::make_unique<PgTransaction>(slot_);
    }

    Task<Result<void>> close() override { state_ = ConnectionState::Idle; co_return {}; }

    Task<bool> ping() override {
        PGresult* res;
        {
            std::lock_guard lock{slot_->mutex};
            if (PQsendQuery(slot_->conn, "SELECT 1") == 0) co_return false;
        }
        if (!co_await async_flush(slot_->conn)) co_return false;
        res = co_await PgReadAwaiter{slot_->conn};
        const bool ok = res && (PQresultStatus(res) == PGRES_TUPLES_OK);
        if (res) PQclear(res);
        co_return ok;
    }

private:
    Slot*             slot_;
    size_t            idx_;
    PgConnectionPool* pool_;
    ConnectionState   state_{ConnectionState::Idle};
};

// ─── ConnectionPool ───────────────────────────────────────────────────────────

struct WaiterRecord {
    PgAcquireAwaiter*       awaiter;
    std::coroutine_handle<> handle;
    Reactor*                reactor;
};

class PgConnectionPool final : public IConnectionPool {
public:
    PgConnectionPool(std::string conninfo, PoolConfig config)
        : conninfo_(std::move(conninfo))
        , max_size_(config.max_size > 0 ? config.max_size : 16) {
        const size_t min_sz = config.min_size > 0 ? config.min_size : 2;
        std::lock_guard lock{mutex_};
        for (size_t i = 0; i < min_sz; ++i) {
            PGconn* c = PQconnectdb(conninfo_.c_str());
            if (!c || PQstatus(c) != CONNECTION_OK) { if (c) PQfinish(c); break; }
            idle_.push(slots_.size());
            slots_.push_back(std::make_unique<Slot>(c));
        }
    }

    bool is_valid() const noexcept { return !slots_.empty(); }

    // ─── 커넥션 획득 ─────────────────────────────────────────────────────────
    //
    // [최적화 흐름]
    // 1. idle 슬롯 존재 → 즉시 반환 (O(1), lock 해제 후 진행)
    // 2. idle 없고 max_size 미만 → 새 슬롯 생성
    // 3. 풀 소진 → co_await PgAcquireAwaiter → reactor yield
    //    release() 가 호출되면 waiter 에게 슬롯을 직접 전달하고 resume

    Task<Result<std::unique_ptr<IConnection>>> acquire() override {
        std::unique_lock lock{mutex_};

        // 1. idle 슬롯 탐색
        while (!idle_.empty()) {
            const size_t idx = idle_.front(); idle_.pop();
            Slot* slot = slots_[idx].get();
            lock.unlock();

            if (PQstatus(slot->conn) != CONNECTION_OK) PQreset(slot->conn);
            if (PQstatus(slot->conn) == CONNECTION_OK) {
                // 30초 이상 idle → 클라우드 방화벽이 TCP를 silently drop 했을 수 있음.
                // PQstatus()는 로컬 상태만 반영하므로 비동기 SELECT 1로 실제 활성 여부 확인.
                if (slot->needs_liveness_check()) {
                    bool alive = false;
                    {
                        std::lock_guard lk{slot->mutex};
                        alive = (PQsendQuery(slot->conn, "SELECT 1") != 0);
                    }
                    if (alive) alive = co_await async_flush(slot->conn);
                    if (alive) {
                        PGresult* res = co_await PgReadAwaiter{slot->conn};
                        alive = res && (PQresultStatus(res) == PGRES_TUPLES_OK);
                        if (res) PQclear(res);
                    }
                    if (!alive) {
                        // ping 실패 → 재연결 시도
                        PGconn* fresh = PQconnectdb(conninfo_.c_str());
                        if (fresh && PQstatus(fresh) == CONNECTION_OK) {
                            PQfinish(slot->conn); slot->conn = fresh; make_nonblocking(fresh);
                        } else {
                            // 재연결 불가 → 슬롯을 idle로 돌려놓고 즉시 에러 반환
                            // (MySQL 일관성: 슬롯 고아화 방지 + 호출자가 핸들링 가능)
                            if (fresh) PQfinish(fresh);
                            lock.lock(); idle_.push(idx);
                            co_return unexpected(db_error(DbError::ConnectionFailed));
                        }
                    }
                }
                active_.fetch_add(1, std::memory_order_relaxed);
                co_return std::make_unique<PgConnection>(slot, idx, this);
            }
            // PQreset 실패 → 새 연결로 교체
            PGconn* fresh = PQconnectdb(conninfo_.c_str());
            if (fresh && PQstatus(fresh) == CONNECTION_OK) {
                PQfinish(slot->conn); slot->conn = fresh; make_nonblocking(fresh);
                active_.fetch_add(1, std::memory_order_relaxed);
                co_return std::make_unique<PgConnection>(slot, idx, this);
            }
            // 신규 연결도 실패 → 슬롯을 idle로 돌려놓고 즉시 에러 반환
            // (MySQL 일관성: 슬롯 고아화 방지 + DB down 시 hang 방지)
            if (fresh) PQfinish(fresh);
            lock.lock(); idle_.push(idx);
            co_return unexpected(db_error(DbError::ConnectionFailed));
        }

        // 2. 새 슬롯 생성
        if (slots_.size() < max_size_) {
            const size_t idx = slots_.size();
            slots_.push_back(nullptr); // 자리 예약
            lock.unlock();

            PGconn* c = PQconnectdb(conninfo_.c_str());
            if (!c || PQstatus(c) != CONNECTION_OK) {
                if (c) PQfinish(c);
                lock.lock(); slots_.pop_back();
                co_return unexpected(db_error(DbError::ConnectionFailed));
            }
            auto s = std::make_unique<Slot>(c);
            Slot* slot = s.get();
            lock.lock(); slots_[idx] = std::move(s); lock.unlock();
            active_.fetch_add(1, std::memory_order_relaxed);
            co_return std::make_unique<PgConnection>(slot, idx, this);
        }

        // 3. 풀 소진 → yield (reactor thread 를 block 하지 않음)
        lock.unlock();
        co_return co_await PgAcquireAwaiter{this};
    }

    // release() — PgConnection 소멸자에서 호출
    // waiter 가 있으면 슬롯을 직접 넘기고 reactor 에서 resume
    void release(size_t idx) noexcept {
        active_.fetch_sub(1, std::memory_order_relaxed);
        std::unique_lock lock{mutex_};
        if (!waiters_.empty()) {
            auto [awaiter, h, reactor] = std::move(waiters_.front());
            waiters_.pop();
            lock.unlock();
            // waiter 에게 슬롯 직접 전달
            awaiter->result = std::make_unique<PgConnection>(
                slots_[idx].get(), idx, this);
            active_.fetch_add(1, std::memory_order_relaxed);
            // waiter 의 reactor thread 에서 resume
            reactor->post([h]() mutable { h.resume(); });
        } else {
            slots_[idx]->mark_idle();
            idle_.push(idx);
        }
    }

    void push_waiter(PgAcquireAwaiter* a,
                     std::coroutine_handle<> h, Reactor* r) noexcept {
        std::lock_guard lock{mutex_};
        waiters_.push({a, h, r});
    }

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
    std::string                        conninfo_;
    size_t                             max_size_;
    mutable std::mutex                 mutex_;
    std::vector<std::unique_ptr<Slot>> slots_;
    std::queue<size_t>                 idle_;
    std::queue<WaiterRecord>           waiters_;
    std::atomic<size_t>                active_{0};
};

// ─── PgAcquireAwaiter::await_suspend 구현 (PgConnectionPool 완전 정의 후) ─────

void PgAcquireAwaiter::await_suspend(std::coroutine_handle<> h) noexcept {
    pool->push_waiter(this, h, Reactor::current());
}

// ─── PgConnection::~PgConnection 구현 ────────────────────────────────────────

PgConnection::~PgConnection() {
    if (pool_) pool_->release(idx_);
}

// ─── Driver ───────────────────────────────────────────────────────────────────

class PgDriver final : public IDBDriver {
public:
    std::string_view driver_name() const noexcept override { return "postgresql"; }

    bool accepts(std::string_view dsn) const noexcept override {
        return dsn.starts_with("postgresql://") || dsn.starts_with("postgres://");
    }

    Task<Result<std::unique_ptr<IConnectionPool>>>
    pool(std::string_view dsn, PoolConfig config) override {
        auto p = std::make_unique<PgConnectionPool>(std::string(dsn),
                                                    std::move(config));
        if (!p->is_valid()) co_return unexpected(db_error(DbError::ConnectionFailed));
        co_return p;
    }
};

} // anonymous namespace

std::unique_ptr<qbuem::db::IDBDriver> make_postgresql_driver() {
    return std::make_unique<PgDriver>();
}

} // namespace qbuem_routine
