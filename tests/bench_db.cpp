// Performance benchmark (release build, real servers). Reports sequential
// single-connection throughput per driver — latency-bound over loopback — plus a
// CPU-only RESP-parser throughput figure. Not wired into CI; run manually.
#include "qbuem/db/sqlite3_driver.hpp"
#include "qbuem/db/postgresql_driver.hpp"
#include "qbuem/db/mysql_driver.hpp"
#include "qbuem/db/redis_client.hpp"
#include "qbuem/db/resp_parser.hpp"
#include "harness.hpp" // RunGuard

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>

using namespace qbuem;
using namespace qbuem_routine;
using Clock = std::chrono::steady_clock;

static double ms_since(Clock::time_point t) {
    return std::chrono::duration<double, std::milli>(Clock::now() - t).count();
}

template <typename T>
static T must(Result<T>&& r, const char* what) {
    if (!r) { std::fprintf(stderr, "FATAL: %s failed\n", what); std::abort(); }
    return std::move(*r);
}

static void must_ok(Result<void>&& r, const char* what) {
    if (!r) { std::fprintf(stderr, "FATAL: %s failed\n", what); std::abort(); }
}

static Task<void> bench_sql(db::IDBDriver& drv, std::string dsn,
                            const char* label, std::string create_sql, int N) {
    auto pool = must(co_await drv.pool(dsn), "pool");
    auto conn = must(co_await pool->acquire(), "acquire");
    co_await conn->query("DROP TABLE IF EXISTS bench");
    must(co_await conn->query(create_sql), "create");

    auto t0 = Clock::now();
    for (int i = 0; i < N; ++i) {
        const db::Value row[] = {db::Value{static_cast<int64_t>(i)},
                                 db::Value{std::string_view{"value-data"}}};
        must(co_await conn->query("INSERT INTO bench(id, val) VALUES($1, $2)", row),
             "insert");
    }
    const double ins_ms = ms_since(t0);

    t0 = Clock::now();
    for (int i = 0; i < N; ++i) {
        const db::Value k[] = {db::Value{static_cast<int64_t>(i)}};
        auto rs = must(co_await conn->query("SELECT val FROM bench WHERE id = $1", k),
                       "select");
        co_await rs->next();
    }
    const double sel_ms = ms_since(t0);

    // Batched INSERT inside ONE transaction — amortizes per-statement durability
    // (fsync/WAL), exposing the driver's real CPU/round-trip ceiling.
    t0 = Clock::now();
    {
        auto tx = must(co_await conn->begin(), "begin");
        for (int i = 0; i < N; ++i) {
            const db::Value row[] = {db::Value{static_cast<int64_t>(N + i)},
                                     db::Value{std::string_view{"value-data"}}};
            must(co_await tx->execute("INSERT INTO bench(id, val) VALUES($1, $2)", row),
                 "tx insert");
        }
        must_ok(co_await tx->commit(), "commit");
    }
    const double txins_ms = ms_since(t0);

    // pool acquire/release churn
    t0 = Clock::now();
    const int M = 5000;
    for (int i = 0; i < M; ++i) {
        auto c = must(co_await pool->acquire(), "acq");
        (void)c; // returned to pool on scope exit
    }
    const double acq_ms = ms_since(t0);

    std::printf("%-11s INSERT(autocommit) %7.0f/s | INSERT(1 txn) %8.0f/s | "
                "SELECT %8.0f/s | acquire/release %9.0f/s\n",
                label, N * 1000.0 / ins_ms, N * 1000.0 / txins_ms,
                N * 1000.0 / sel_ms, M * 1000.0 / acq_ms);
    co_await conn->query("DROP TABLE IF EXISTS bench");
    co_return;
}

static Task<void> bench_redis(std::string dsn, int N) {
    auto cli = must(co_await redis::RedisClient::connect(dsn), "redis connect");
    co_await cli->flushdb();

    auto t0 = Clock::now();
    for (int i = 0; i < N; ++i)
        must(co_await cli->set("k" + std::to_string(i & 1023), "v"), "set");
    const double set_ms = ms_since(t0);

    t0 = Clock::now();
    for (int i = 0; i < N; ++i)
        must(co_await cli->get("k" + std::to_string(i & 1023)), "get");
    const double get_ms = ms_since(t0);

    std::printf("%-11s SET    %5d: %7.0f ops/s (%.3f ms/op) | GET    %5d: %7.0f ops/s (%.3f ms/op)\n",
                "redis", N, N * 1000.0 / set_ms, set_ms / N,
                N, N * 1000.0 / get_ms, get_ms / N);
    co_return;
}

// CPU-only: how fast the hardened RESP parser ingests replies (no socket).
static void bench_resp(int N) {
    // A representative small bulk-string reply: "$5\r\nhello\r\n"
    std::string msg = "$5\r\nhello\r\n";
    auto t0 = Clock::now();
    int parsed = 0;
    for (int i = 0; i < N; ++i) {
        redis::RespParser p;
        p.feed(msg.data(), msg.size());
        if (p.has_complete()) { p.parse(); ++parsed; }
    }
    const double ms = ms_since(t0);
    std::printf("%-11s parse  %5d msgs: %8.0f msgs/s (%.0f MB/s)   [parsed=%d]\n",
                "resp", N, N * 1000.0 / ms,
                (double)N * msg.size() / 1024.0 / 1024.0 / (ms / 1000.0), parsed);
}

int main() {
    const char* pg    = std::getenv("PG_DSN");
    const char* mysql = std::getenv("MYSQL_DSN");
    const char* redis_dsn = std::getenv("REDIS_DSN");
    const std::string pg_dsn    = pg    ? pg    : "postgresql://postgres:test@localhost:5433/testdb";
    const std::string mysql_dsn = mysql ? mysql : "mysql://root:test@127.0.0.1:3307/test";
    const std::string redis_url = redis_dsn ? redis_dsn : "redis://127.0.0.1:6380";

    auto sqlite = make_sqlite_driver();
    auto pgd    = make_postgresql_driver();
    auto myd    = make_mysql_driver();

    qbuem_db_it::RunGuard guard;
    guard.run_and_wait([&]() -> Task<void> {
        std::puts("--- SQL drivers (sequential, single connection, loopback) ---");
        co_await bench_sql(*sqlite, "sqlite:///tmp/claude/bench.db", "sqlite",
                           "CREATE TABLE bench(id BIGINT PRIMARY KEY, val TEXT)", 2000);
        co_await bench_sql(*pgd, pg_dsn, "postgresql",
                           "CREATE TABLE bench(id BIGINT PRIMARY KEY, val TEXT)", 2000);
        co_await bench_sql(*myd, mysql_dsn, "mysql",
                           "CREATE TABLE bench(id BIGINT PRIMARY KEY, val VARCHAR(64))", 2000);
        std::puts("--- Redis ---");
        co_await bench_redis(redis_url, 5000);
        co_return;
    }, std::chrono::seconds{120});
    guard.shutdown();

    std::puts("--- RESP parser (CPU only) ---");
    bench_resp(500000);
    return 0;
}
