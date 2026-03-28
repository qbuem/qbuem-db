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
#include <format>
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

// ─── PostgreSQL OID constants ────────────────────────────────────────────────

static constexpr Oid OID_BOOL        = 16;
static constexpr Oid OID_BYTEA       = 17;
static constexpr Oid OID_INT8        = 20;
static constexpr Oid OID_INT2        = 21;
static constexpr Oid OID_INT4        = 23;
static constexpr Oid OID_FLOAT4      = 700;
static constexpr Oid OID_FLOAT8      = 701;
static constexpr Oid OID_DATE        = 1082;
static constexpr Oid OID_TIME        = 1083;
static constexpr Oid OID_TIMESTAMP   = 1114;
static constexpr Oid OID_TIMESTAMPTZ = 1184;

// ─── Binary big-endian deserialization ───────────────────────────────────────

template<std::integral T>
static T from_be(const char* data) noexcept {
    T v;
    std::memcpy(&v, data, sizeof(T));
    if constexpr (std::endian::native != std::endian::big)
        v = std::byteswap(v);
    return v;
}

// ─── PostgreSQL binary date/time conversion helpers ──────────────────────────

// PostgreSQL binary epoch: days/microseconds since 2000-01-01
static const std::chrono::sys_days kPgDateEpoch{
    std::chrono::year{2000} / std::chrono::January / std::chrono::day{1}};

static std::string pg_days_to_date_str(int32_t days) noexcept {
    auto dp  = kPgDateEpoch + std::chrono::days{days};
    auto ymd = std::chrono::year_month_day{dp};
    return std::format("{:04d}-{:02d}-{:02d}",
        static_cast<int>(ymd.year()),
        static_cast<unsigned>(ymd.month()),
        static_cast<unsigned>(ymd.day()));
}

static std::string pg_usec_to_time_str(int64_t usec) noexcept {
    int64_t secs = usec / 1'000'000;
    int64_t us   = usec % 1'000'000;
    int h = static_cast<int>(secs / 3600);
    int m = static_cast<int>((secs % 3600) / 60);
    int s = static_cast<int>(secs % 60);
    if (us == 0) return std::format("{:02d}:{:02d}:{:02d}", h, m, s);
    return std::format("{:02d}:{:02d}:{:02d}.{:06d}", h, m, s, static_cast<int>(us));
}

static std::string pg_usec_to_ts_str(int64_t usec) noexcept {
    int64_t secs     = usec / 1'000'000;
    int64_t us       = usec % 1'000'000;
    if (us < 0) { --secs; us += 1'000'000; }
    int64_t days_n   = secs / 86400;
    int64_t day_secs = secs % 86400;
    if (day_secs < 0) { --days_n; day_secs += 86400; }
    auto dp  = kPgDateEpoch + std::chrono::days{days_n};
    auto ymd = std::chrono::year_month_day{dp};
    int h = static_cast<int>(day_secs / 3600);
    int mn = static_cast<int>((day_secs % 3600) / 60);
    int sc = static_cast<int>(day_secs % 60);
    if (us == 0)
        return std::format("{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}",
            static_cast<int>(ymd.year()), static_cast<unsigned>(ymd.month()),
            static_cast<unsigned>(ymd.day()), h, mn, sc);
    return std::format("{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:06d}",
        static_cast<int>(ymd.year()), static_cast<unsigned>(ymd.month()),
        static_cast<unsigned>(ymd.day()), h, mn, sc, static_cast<int>(us));
}

// ─── CellData ────────────────────────────────────────────────────────────────

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
        case OID_DATE:
            c.type = Value::Type::Text;
            c.text = pg_days_to_date_str(from_be<int32_t>(val));
            break;
        case OID_TIME:
            c.type = Value::Type::Text;
            c.text = pg_usec_to_time_str(from_be<int64_t>(val));
            break;
        case OID_TIMESTAMP:
        case OID_TIMESTAMPTZ:
            c.type = Value::Type::Text;
            c.text = pg_usec_to_ts_str(from_be<int64_t>(val));
            break;
        default:
            c.type = Value::Type::Text;
            c.text.assign(val, static_cast<size_t>(len));
            break;
    }
    return c;
}

// ─── PgParams — zero-alloc parameter array ───────────────────────────────────
//
// Design principles:
//  • Text    → use string_view pointer directly (zero-copy, no allocation)
//  • Null    → nullptr (no allocation)
//  • Bool    → static literal "t"/"f" (no allocation)
//  • Int64 / Float64 → to_chars into a fixed stack buffer (no allocation)
//  • Blob    → only blob_bufs_ allocates a std::string for hex encoding (rare)
//
// ≤16 params: fully stack-allocated (0 heap allocations)
// >16 params: one heap allocation for values/lengths/formats + num_bufs

struct PgParams {
    static constexpr size_t kInline   = 16;
    static constexpr size_t kNumBufSz = 32; // int64_t max 20 chars + double max ~24 chars

    // ── Inline storage (≤kInline) ───────────────────────────────────────────
    char        num_buf_[kInline][kNumBufSz]{};  // numeric/bool serialization buffer
    const char* values_inline_[kInline]{};
    int         lengths_inline_[kInline]{};
    int         formats_inline_[kInline]{};

    // ── Heap storage (>kInline) ──────────────────────────────────────────────
    std::vector<std::array<char, kNumBufSz>> num_buf_heap_;
    std::vector<const char*>                 values_heap_;
    std::vector<int>                         lengths_heap_;
    std::vector<int>                         formats_heap_;

    // ── Blob conversion buffer (only for Blob values) ───────────────────────
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

        blob_bufs_.reserve(n); // prevent realloc on Blob values (pointer stability)

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
                    // zero-copy: params span remains valid until PQsend* completes
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
                    // chars_format::general: shortest round-trip representation
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
// Async awaiters — integrate libpq socket with Reactor events
// ─────────────────────────────────────────────────────────────────────────────

// ─── PgReadAwaiter ────────────────────────────────────────────────────────────
//
// Non-blocking replacement for PQexecParams (blocking):
//   PQsendQueryParams → co_await PgFlushAwaiter → co_await PgReadAwaiter
//
// await_suspend registers a kqueue Read event. When the socket becomes readable,
// we call PQconsumeInput in a loop (up to kMaxDrainIter times) to drain all
// available data — including records already buffered inside OpenSSL's internal
// layer that SSL_read can return without a new OS recv().
//
// The kqueue reactor uses EV_CLEAR (edge-triggered): after the event fires it
// will not re-fire unless new bytes arrive at the OS socket level. If the server
// sends NOTICE + query result in the same TCP segment, both arrive together.
// PQconsumeInput processes one SSL record per call, so a single call may only
// consume the NOTICE, leaving the result in SSL's buffer. Without the drain loop,
// PQisBusy stays true, we return, and kqueue never fires again (the OS socket
// buffer is already empty) — causing a permanent hang.
//
// The drain loop fixes this: we keep calling PQconsumeInput until PQisBusy is
// false (result ready) or until all kMaxDrainIter calls have been exhausted
// (genuinely waiting for more bytes from the server — kqueue event stays
// registered and will fire when new OS-level data arrives).

struct PgReadAwaiter {
    static constexpr int kMaxDrainIter = 16;

    PGconn*   conn;
    PGresult* result{nullptr};

    bool await_ready() noexcept {
        PQconsumeInput(conn);
        if (PQisBusy(conn)) return false;
        result = PQgetResult(conn);
        while (PGresult* r = PQgetResult(conn)) PQclear(r); // drain trailing results
        return true;
    }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        auto* reactor = Reactor::current();
        const int fd  = PQsocket(conn);

        reactor->register_event(fd, EventType::Read,
            [this, h, fd, reactor](int) mutable {
                // IMPORTANT: copy captured values to stack-locals FIRST.
                //
                // unregister_event() calls entry->read_cb = nullptr, which destroys
                // this std::function closure. Our captures (this, h, reactor, fd) are
                // stored inside that closure. If the closure is heap-allocated (total
                // captures > libc++ SBO of 24 bytes — which they are: 3 ptrs + int =
                // 28 bytes), the heap block is freed by the destructor. Accessing any
                // captured variable after that point is use-after-free.
                //
                // Stack-local copies are unaffected by the closure's destruction and
                // remain valid for the rest of this call frame.
                PgReadAwaiter* pga      = this;     // PgReadAwaiter lives in coroutine frame
                Reactor*       lreactor = reactor;
                int            lfd      = fd;
                auto           lh       = h;

                // Drain all SSL-buffered records. Each PQconsumeInput call processes
                // one SSL record. With EV_CLEAR kqueue, we must drain here rather than
                // waiting for another kqueue event: SSL may have already consumed all
                // OS socket bytes into its internal buffer, so EV_CLEAR won't re-fire.
                for (int i = 0; i < kMaxDrainIter; ++i) {
                    PQconsumeInput(pga->conn);
                    if (!PQisBusy(pga->conn)) {
                        // Collect the result BEFORE unregistering (closure still valid).
                        pga->result = PQgetResult(pga->conn);
                        while (PGresult* r = PQgetResult(pga->conn)) PQclear(r);
                        // unregister_event may free this closure — use stack-locals only below.
                        lreactor->unregister_event(lfd, EventType::Read);
                        lreactor->post([lh]() mutable { lh.resume(); });
                        return;
                    }
                }
                // Still busy after kMaxDrainIter attempts: all SSL records consumed,
                // but the server hasn't sent a complete response yet.
                // The kqueue event remains registered and will re-fire when new
                // OS-level bytes arrive.
            });
    }

    PGresult* await_resume() noexcept { return result; }
};

// ─── PgFlushAwaiter ───────────────────────────────────────────────────────────
//
// If PQflush() == 1 (send buffer full), wait for the socket to become writable.
// For small queries PQflush() returns 0 immediately, so await_ready() == true.

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

// ─── async_flush — repeatedly flush until the send buffer is fully drained ───

static Task<bool> async_flush(PGconn* conn) {
    while (true) {
        const int rc = PQflush(conn);
        if (rc ==  0) co_return true;   // send complete
        if (rc == -1) co_return false;  // error
        co_await PgFlushAwaiter{conn};  // wait for socket writable, then retry
    }
}

// ─── Async query helpers ──────────────────────────────────────────────────────
//
// Wraps the three-step pattern: PQsend* → async_flush → PgReadAwaiter.
// All queries request resultFormat=1 (binary).

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

// ─── Set connection socket to non-blocking ───────────────────────────────────

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
    std::mutex       mutex;          // serializes PQsend* calls (released during co_await)
    uint64_t         stmt_counter{0};
    Clock::time_point idle_since{Clock::now()};

    explicit Slot(PGconn* c) noexcept : conn(c) { make_nonblocking(c); }
    ~Slot() { if (conn) PQfinish(conn); }

    Slot(const Slot&)            = delete;
    Slot& operator=(const Slot&) = delete;

    void mark_idle() noexcept { idle_since = Clock::now(); }

    // Idle for 30+ seconds — cloud firewall/load-balancer may have silently closed the TCP conn.
    [[nodiscard]] bool needs_liveness_check() const noexcept {
        return (Clock::now() - idle_since) > std::chrono::seconds{30};
    }
};

// ─── PgAcquireAwaiter — wait when pool is exhausted ──────────────────────────
//
// When acquire() finds no idle slot, yield instead of returning an error.
// When release() is called it hands the slot directly to the first waiter and
// resumes it on that waiter's reactor thread.
// → supports max_size concurrent queries without spurious errors.

class PgConnectionPool; // forward declaration

class PgConnection; // forward declaration

struct PgAcquireAwaiter {
    PgConnectionPool*                     pool;
    Result<std::unique_ptr<IConnection>>  result{unexpected(db_error(DbError::ConnectionFailed))};

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept;   // defined below
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
        // DEALLOCATE is synchronous — infrequent (only on statement destruction)
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
            // Lock covers only PQsend*; lock is released before co_await
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

    // ─── acquire — connection checkout ───────────────────────────────────────
    //
    // Fast path (in order):
    // 1. Idle slot exists → return immediately (O(1), lock released first)
    // 2. No idle slot, below max_size → create a new slot
    // 3. Pool exhausted → co_await PgAcquireAwaiter → reactor yield
    //    When release() is called it hands the slot to the first waiter and resumes it.

    Task<Result<std::unique_ptr<IConnection>>> acquire() override {
        std::unique_lock lock{mutex_};

        // 1. scan idle slots
        while (!idle_.empty()) {
            const size_t idx = idle_.front(); idle_.pop();
            Slot* slot = slots_[idx].get();
            lock.unlock();

            if (PQstatus(slot->conn) != CONNECTION_OK) PQreset(slot->conn);
            if (PQstatus(slot->conn) == CONNECTION_OK) {
                // Idle 30+ seconds — cloud firewall may have silently dropped the TCP conn.
                // PQstatus() reflects only local state; verify liveness with async SELECT 1.
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
                        // Ping failed — try to reconnect.
                        PGconn* fresh = PQconnectdb(conninfo_.c_str());
                        if (fresh && PQstatus(fresh) == CONNECTION_OK) {
                            PQfinish(slot->conn); slot->conn = fresh; make_nonblocking(fresh);
                        } else {
                            // Reconnect failed — return slot to idle and surface the error.
                            // (Consistent with MySQL driver: prevents slot orphaning.)
                            if (fresh) PQfinish(fresh);
                            lock.lock(); idle_.push(idx);
                            co_return unexpected(db_error(DbError::ConnectionFailed));
                        }
                    }
                }
                active_.fetch_add(1, std::memory_order_relaxed);
                co_return std::make_unique<PgConnection>(slot, idx, this);
            }
            // PQreset failed — replace with a new connection
            PGconn* fresh = PQconnectdb(conninfo_.c_str());
            if (fresh && PQstatus(fresh) == CONNECTION_OK) {
                PQfinish(slot->conn); slot->conn = fresh; make_nonblocking(fresh);
                active_.fetch_add(1, std::memory_order_relaxed);
                co_return std::make_unique<PgConnection>(slot, idx, this);
            }
            // New connection also failed — return slot to idle and surface the error.
            // (Consistent with MySQL driver: prevents slot orphaning and hangs on DB down.)
            if (fresh) PQfinish(fresh);
            lock.lock(); idle_.push(idx);
            co_return unexpected(db_error(DbError::ConnectionFailed));
        }

        // 2. create a new slot
        if (slots_.size() < max_size_) {
            const size_t idx = slots_.size();
            slots_.push_back(nullptr); // reserve index
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

        // 3. pool exhausted — yield without blocking the reactor thread
        lock.unlock();
        co_return co_await PgAcquireAwaiter{this};
    }

    // release() — called from PgConnection destructor.
    // If there is a waiter, hand the slot directly to it and resume on its reactor thread.
    void release(size_t idx) noexcept {
        active_.fetch_sub(1, std::memory_order_relaxed);
        std::unique_lock lock{mutex_};
        if (!waiters_.empty()) {
            auto [awaiter, h, reactor] = std::move(waiters_.front());
            waiters_.pop();
            lock.unlock();
            // hand the slot directly to the waiter
            awaiter->result = std::make_unique<PgConnection>(
                slots_[idx].get(), idx, this);
            active_.fetch_add(1, std::memory_order_relaxed);
            // resume on the waiter's reactor thread
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

// ─── PgAcquireAwaiter::await_suspend — defined after PgConnectionPool is complete ──

void PgAcquireAwaiter::await_suspend(std::coroutine_handle<> h) noexcept {
    pool->push_waiter(this, h, Reactor::current());
}

// ─── PgConnection::~PgConnection ─────────────────────────────────────────────

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
