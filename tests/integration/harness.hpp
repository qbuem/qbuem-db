#pragma once

// Shared integration-test harness: runs the async DB drivers on a one-thread
// Dispatcher and exercises a common, driver-agnostic suite (every driver
// implements qbuem::db::IDBDriver).  Each driver test is its own executable, so
// the inline counters are per-binary.

#include <qbuem/core/dispatcher.hpp>
#include <qbuem/core/task.hpp>
#include <qbuem/db/driver.hpp>
#include <qbuem/db/value.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>

namespace qbuem_db_it {

using namespace qbuem;
using namespace qbuem::db;

inline int g_fail = 0, g_total = 0;
#define IT_CHECK(cond)                                                         \
    do {                                                                      \
        ++::qbuem_db_it::g_total;                                             \
        if (!(cond)) { ++::qbuem_db_it::g_fail;                               \
            std::printf("FAIL %s:%d  %s\n", __FILE__, __LINE__, #cond); }     \
    } while (0)

// One-thread Dispatcher harness (pattern from qbuem-stack's own RunGuard).
struct RunGuard {
    Dispatcher   dispatcher{1};
    std::jthread thread{[this] { dispatcher.run(); }};
    bool         stopped_ = false;
    void shutdown() {
        if (stopped_) return;
        stopped_ = true;
        dispatcher.stop();
        if (thread.joinable()) thread.join();
    }
    ~RunGuard() { shutdown(); }
    template <typename F>
    static Task<void> run_coro(F f, std::shared_ptr<std::atomic<bool>> done) {
        co_await f();
        done->store(true, std::memory_order_release);
    }
    template <typename F>
    bool run_and_wait(F&& f,
                      std::chrono::milliseconds timeout = std::chrono::seconds{15}) {
        auto done = std::make_shared<std::atomic<bool>>(false);
        dispatcher.spawn(run_coro(std::forward<F>(f), done));
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (!done->load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
        return done->load();
    }
};

// Driver-agnostic suite: connect → DDL → parameterized CRUD → value-injection
// safety → transaction + savepoint-injection guard.  `create_sql` is the only
// dialect-specific input; all placeholders use `$1`/`$2` (drivers convert to `?`
// for MySQL/SQLite).
inline Task<void> basic_suite(IDBDriver& driver, std::string dsn, std::string create_sql) {
    auto pool_r = co_await driver.pool(dsn);
    IT_CHECK(pool_r.has_value());
    if (!pool_r) co_return;
    auto pool = std::move(*pool_r);

    auto conn_r = co_await pool->acquire();
    IT_CHECK(conn_r.has_value());
    if (!conn_r) co_return;
    auto conn = std::move(*conn_r);

    co_await conn->query("DROP TABLE IF EXISTS itest");
    IT_CHECK((co_await conn->query(create_sql)).has_value());

    const Value r1[] = {Value{int64_t{1}}, Value{std::string_view{"alice"}}};
    IT_CHECK((co_await conn->query("INSERT INTO itest(id, name) VALUES($1, $2)", r1)).has_value());
    const Value r2[] = {Value{int64_t{2}}, Value{std::string_view{"bob"}}};
    IT_CHECK((co_await conn->query("INSERT INTO itest(id, name) VALUES($1, $2)", r2)).has_value());

    const Value sel[] = {Value{int64_t{1}}};
    auto rs_r = co_await conn->query("SELECT id, name FROM itest WHERE id = $1", sel);
    IT_CHECK(rs_r.has_value());
    if (rs_r) {
        auto rs = std::move(*rs_r);
        const IRow* row = co_await rs->next();
        IT_CHECK(row != nullptr);
        if (row) {
            IT_CHECK(row->get(0).get<int64_t>() == 1);
            IT_CHECK(row->get(1).get<std::string_view>() == "alice");
        }
        IT_CHECK((co_await rs->next()) == nullptr);
    }

    // A value that looks like SQL is bound as data, not executed.
    const Value evil[] = {Value{std::string_view{"x'); DROP TABLE itest; --"}}};
    IT_CHECK((co_await conn->query("INSERT INTO itest(id, name) VALUES(99, $1)", evil)).has_value());
    auto cnt_r = co_await conn->query("SELECT COUNT(*) FROM itest");
    IT_CHECK(cnt_r.has_value());
    if (cnt_r) {
        auto rs = std::move(*cnt_r);
        const IRow* row = co_await rs->next();
        IT_CHECK(row && row->get(0).get<int64_t>() == 3); // table intact
    }

    // Transaction + savepoint-injection guard (runtime check of the driver fix).
    auto tx_r = co_await conn->begin();
    IT_CHECK(tx_r.has_value());
    if (tx_r) {
        auto tx = std::move(*tx_r);
        IT_CHECK((co_await tx->savepoint("ok_sp")).has_value());        // safe → OK
        IT_CHECK(!(co_await tx->savepoint("a\"; DROP TABLE itest; --")).has_value()); // unsafe → error
        IT_CHECK((co_await tx->rollback()).has_value());
    }
    co_return;
}

// Pool-shutdown safety: a connection acquired from the pool must remain safe to
// use (and destroy) even after the pool is drained — draining must not free the
// underlying handle out from under a live connection (use-after-free).
inline Task<void> drain_safety_suite(IDBDriver& driver, std::string dsn) {
    auto pool_r = co_await driver.pool(dsn);
    IT_CHECK(pool_r.has_value());
    if (!pool_r) co_return;
    auto pool = std::move(*pool_r);

    auto conn_r = co_await pool->acquire();
    IT_CHECK(conn_r.has_value());
    if (!conn_r) co_return;
    auto conn = std::move(*conn_r);

    // Drain the pool while `conn` is still alive.
    co_await pool->drain();

    // Using the connection after drain must not be a use-after-free.  It may
    // return an error, but it must not touch freed memory / crash.
    auto r = co_await conn->query("SELECT 1");
    IT_CHECK(true); // reaching here without an ASan abort is the real assertion
    (void)r;
    // conn destructs here (also must not UAF on the drained handle).
    co_return;
}

// Large-value round-trip: exercises the result path for column values that span
// (and exceed) the fixed buffer a driver pre-sizes per column.  The MySQL driver
// binds 256-byte result buffers before store_result knows the real widths, so a
// value > 256 bytes forces its truncate -> resize -> mysql_stmt_fetch_column
// re-fetch path; any buffer/length mismatch there surfaces here as an ASan heap
// overflow or a corrupted (non-byte-exact) read.  DDL is portable across all
// three SQL backends (BIGINT PRIMARY KEY + TEXT).
inline Task<void> large_value_suite(IDBDriver& driver, std::string dsn) {
    auto pool_r = co_await driver.pool(dsn);
    IT_CHECK(pool_r.has_value());
    if (!pool_r) co_return;
    auto pool = std::move(*pool_r);

    auto conn_r = co_await pool->acquire();
    IT_CHECK(conn_r.has_value());
    if (!conn_r) co_return;
    auto conn = std::move(*conn_r);

    co_await conn->query("DROP TABLE IF EXISTS bigtest");
    IT_CHECK((co_await conn->query(
        "CREATE TABLE bigtest(id BIGINT PRIMARY KEY, body TEXT)")).has_value());

    // Sizes straddle the 256-byte boundary in both directions so the same result
    // set mixes re-fetched (truncated) and inline columns row to row.
    const int sizes[] = {1, 200, 255, 256, 257, 1000, 5000};
    const int nsizes  = static_cast<int>(sizeof(sizes) / sizeof(sizes[0]));
    for (int i = 0; i < nsizes; ++i) {
        std::string body(static_cast<size_t>(sizes[i]),
                         static_cast<char>('A' + (sizes[i] % 26)));
        const Value row[] = {Value{int64_t{i + 1}}, Value{std::string_view{body}}};
        IT_CHECK((co_await conn->query(
            "INSERT INTO bigtest(id, body) VALUES($1, $2)", row)).has_value());
    }

    auto rs_r = co_await conn->query("SELECT id, body FROM bigtest ORDER BY id");
    IT_CHECK(rs_r.has_value());
    if (rs_r) {
        auto rs  = std::move(*rs_r);
        int  idx = 0;
        while (const IRow* r = co_await rs->next()) {
            if (idx >= nsizes) { IT_CHECK(false); break; }
            const std::string expect(static_cast<size_t>(sizes[idx]),
                                     static_cast<char>('A' + (sizes[idx] % 26)));
            IT_CHECK(r->get(0).get<int64_t>() == idx + 1);
            IT_CHECK(r->get(1).get<std::string_view>() == expect); // byte-exact
            ++idx;
        }
        IT_CHECK(idx == nsizes); // every row returned
    }
    co_return;
}

// Run `body` to completion; returns process exit code (0 = all checks passed).
template <typename F>
inline int run_main(F&& body) {
    {
        RunGuard guard;
        const bool finished = guard.run_and_wait(std::forward<F>(body));
        IT_CHECK(finished);
        guard.shutdown();
    }
    std::printf("\n%d/%d checks passed\n", g_total - g_fail, g_total);
    return g_fail ? 1 : 0;
}

} // namespace qbuem_db_it
