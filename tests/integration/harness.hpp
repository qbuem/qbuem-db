#pragma once

// Shared integration-test harness: runs the async DB drivers on a one-thread
// Dispatcher and exercises a common, driver-agnostic suite (every driver
// implements qbuem::db::IDBDriver).  Each driver test is its own executable, so
// the inline counters are per-binary.

#include <qbuem/core/dispatcher.hpp>
#include <qbuem/core/task.hpp>
#include <qbuem/db/driver.hpp>
#include <qbuem/db/value.hpp>
#include "qbuem/db/orm.hpp"
#include "qbuem/db/migration/migrator.hpp"
#include "qbuem/db/transaction.hpp"

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

// Transaction / session state must not bleed across pooled connection reuse: a
// connection returned to the pool with an open (or aborted) transaction must be
// reset before the next holder receives it.  Uses a single-slot pool so the same
// physical connection is guaranteed to be reused.
inline Task<void> txn_isolation_suite(IDBDriver& driver, std::string dsn) {
    PoolConfig cfg;
    cfg.min_size = 1;
    cfg.max_size = 1;
    auto pool_r = co_await driver.pool(dsn, cfg);
    IT_CHECK(pool_r.has_value());
    if (!pool_r) co_return;
    auto pool = std::move(*pool_r);

    // Fresh table.
    {
        auto c0 = co_await pool->acquire();
        IT_CHECK(c0.has_value());
        if (!c0) co_return;
        auto conn0 = std::move(*c0);
        co_await conn0->query("DROP TABLE IF EXISTS itxn");
        IT_CHECK((co_await conn0->query(
            "CREATE TABLE itxn(id BIGINT PRIMARY KEY)")).has_value());
    }

    // Holder leaves a transaction open: BEGIN + INSERT, then drops the handles
    // without COMMIT or ROLLBACK — the connection returns to the pool mid-txn.
    {
        auto c1 = co_await pool->acquire();
        IT_CHECK(c1.has_value());
        if (!c1) co_return;
        auto conn1 = std::move(*c1);
        auto tx_r = co_await conn1->begin();
        IT_CHECK(tx_r.has_value());
        if (tx_r) {
            auto tx = std::move(*tx_r);
            const Value ins[] = {Value{int64_t{777}}};
            (void)co_await tx->execute("INSERT INTO itxn(id) VALUES($1)", ins);
        }
    }

    // Next holder reuses the same connection.  It must see clean state:
    //   * the uncommitted INSERT is gone (rolled back), and
    //   * queries are not blocked by a leftover open/aborted transaction.
    {
        auto c2 = co_await pool->acquire();
        IT_CHECK(c2.has_value());
        if (!c2) co_return;
        auto conn2 = std::move(*c2);
        auto rs_r = co_await conn2->query("SELECT COUNT(*) FROM itxn");
        IT_CHECK(rs_r.has_value()); // not blocked by a leftover transaction
        if (rs_r) {
            auto rs = std::move(*rs_r);
            const IRow* row = co_await rs->next();
            IT_CHECK(row && row->get(0).get<int64_t>() == 0); // INSERT rolled back
        }
        co_await conn2->query("DROP TABLE IF EXISTS itxn");
    }
    co_return;
}

// Proves the pool hands out multiple INDEPENDENT physical connections: two
// connections, acquired at once, can each hold their own open transaction
// simultaneously.  That is impossible on a single shared connection — a second
// BEGIN on the same handle errors ("cannot start a transaction within a
// transaction" on SQLite; the others would share one session).  Also checks that
// the configured max_size is honored.  For SQLite pass a poolable DSN (a file or
// a shared-cache memory URI), not a private ":memory:" database.
inline Task<void> multi_connection_suite(IDBDriver& driver, std::string dsn) {
    PoolConfig cfg;
    cfg.min_size = 2;
    cfg.max_size = 4;
    auto pool_r = co_await driver.pool(dsn, cfg);
    IT_CHECK(pool_r.has_value());
    if (!pool_r) co_return;
    auto pool = std::move(*pool_r);
    IT_CHECK(pool->max_size() == 4); // config honored (not hardcoded to 1)

    auto c1 = co_await pool->acquire();
    auto c2 = co_await pool->acquire();
    IT_CHECK(c1.has_value());
    IT_CHECK(c2.has_value()); // a second live connection (not the same one)
    if (!c1 || !c2) co_return;
    auto conn1 = std::move(*c1);
    auto conn2 = std::move(*c2);

    // Two independent transactions held at the same time ⇒ two distinct sessions.
    auto t1 = co_await conn1->begin();
    IT_CHECK(t1.has_value());
    auto t2 = co_await conn2->begin();
    IT_CHECK(t2.has_value()); // would fail if conn2 were the same connection as conn1
    if (t1) IT_CHECK((co_await (*t1)->rollback()).has_value());
    if (t2) IT_CHECK((co_await (*t2)->rollback()).has_value());
    co_return;
}

// ── ORM end-to-end ────────────────────────────────────────────────────────────
// Entity for the ORM suite. Registered once per test binary via register_table.
struct OrmUser {
    int64_t     id{};
    std::string name;
    int64_t     age{};
};

// Drives the generic ORM (qbuem_routine::orm) through full CRUD + a transaction
// against a real driver, verifying that SQL generation, parameter binding and
// row mapping all work end-to-end (the ORM was previously only unit-tested at the
// SQL-string level).  `dialect` must match the driver; `create_sql` supplies the
// dialect-specific auto-increment DDL.
inline Task<void> orm_suite(IDBDriver& driver, std::string dsn,
                            qbuem_routine::orm::Dialect dialect,
                            std::string create_sql) {
    namespace orm = qbuem_routine::orm;
    auto& m = orm::register_table<OrmUser>("orm_user")
                  .dialect(dialect)
                  .pk ("id",   &OrmUser::id)
                  .col("name", &OrmUser::name)
                  .col("age",  &OrmUser::age);

    auto pool_r = co_await driver.pool(dsn);
    IT_CHECK(pool_r.has_value());
    if (!pool_r) co_return;
    auto pool = std::move(*pool_r);

    auto conn_r = co_await pool->acquire();
    IT_CHECK(conn_r.has_value());
    if (!conn_r) co_return;
    auto conn = std::move(*conn_r);

    co_await conn->query("DROP TABLE IF EXISTS orm_user");
    IT_CHECK((co_await conn->query(create_sql)).has_value());

    // CREATE — INSERT via the ORM (PK excluded; DB auto-assigns it).
    OrmUser u{};
    u.name = "alice";
    u.age  = 30;
    int64_t new_id = 0;
    {
        auto rs_r = co_await conn->query(m.sql_insert(), m.bind_insert(u));
        IT_CHECK(rs_r.has_value());
        if (!rs_r) co_return;
        auto rs = std::move(*rs_r);
        if (dialect == orm::Dialect::PostgreSQL) {
            const IRow* row = co_await rs->next(); // RETURNING *
            IT_CHECK(row != nullptr);
            if (row) new_id = m.read_row(*row).id;
        } else {
            new_id = static_cast<int64_t>(rs->last_insert_id());
        }
    }
    IT_CHECK(new_id > 0);

    // READ — SELECT by PK, map back to the struct.
    {
        auto rs_r = co_await conn->query(m.sql_select_where("id"), m.bind_val(new_id));
        IT_CHECK(rs_r.has_value());
        if (rs_r) {
            auto rs = std::move(*rs_r);
            const IRow* row = co_await rs->next();
            IT_CHECK(row != nullptr);
            if (row) {
                OrmUser got = m.read_row(*row);
                IT_CHECK(got.id == new_id);
                IT_CHECK(got.name == "alice");
                IT_CHECK(got.age == 30);
            }
        }
    }

    // UPDATE — change fields, persist via the ORM, re-read to confirm.
    u.id   = new_id;
    u.name = "alice2";
    u.age  = 31;
    IT_CHECK((co_await conn->query(m.sql_update_pk(), m.bind_update(u))).has_value());
    {
        auto rs_r = co_await conn->query(m.sql_select_where("id"), m.bind_val(new_id));
        IT_CHECK(rs_r.has_value());
        if (rs_r) {
            auto rs = std::move(*rs_r);
            const IRow* row = co_await rs->next();
            IT_CHECK(row != nullptr);
            if (row) {
                OrmUser got = m.read_row(*row);
                IT_CHECK(got.name == "alice2");
                IT_CHECK(got.age == 31);
            }
        }
    }

    // COUNT — aggregate via the ORM.
    {
        auto rs_r = co_await conn->query(m.sql_count());
        IT_CHECK(rs_r.has_value());
        if (rs_r) {
            auto rs = std::move(*rs_r);
            const IRow* row = co_await rs->next();
            IT_CHECK(row && row->get(0).get<int64_t>() == 1);
        }
    }

    // TRANSACTION — ORM INSERT inside a transaction, then ROLLBACK: must not persist.
    {
        auto tx_r = co_await conn->begin();
        IT_CHECK(tx_r.has_value());
        if (tx_r) {
            auto tx = std::move(*tx_r);
            OrmUser u2{};
            u2.name = "bob";
            u2.age  = 40;
            IT_CHECK((co_await tx->execute(m.sql_insert(), m.bind_insert(u2))).has_value());
            IT_CHECK((co_await tx->rollback()).has_value());
        }
        auto rs_r = co_await conn->query(m.sql_count());
        IT_CHECK(rs_r.has_value());
        if (rs_r) {
            auto rs = std::move(*rs_r);
            const IRow* row = co_await rs->next();
            IT_CHECK(row && row->get(0).get<int64_t>() == 1); // bob rolled back
        }
    }

    // DELETE — remove via the ORM, confirm the table is empty.
    IT_CHECK((co_await conn->query(m.sql_delete_pk(), m.bind_pk(u))).has_value());
    {
        auto rs_r = co_await conn->query(m.sql_count());
        IT_CHECK(rs_r.has_value());
        if (rs_r) {
            auto rs = std::move(*rs_r);
            const IRow* row = co_await rs->next();
            IT_CHECK(row && row->get(0).get<int64_t>() == 0);
        }
    }
    co_return;
}

// Drives the schema-migration runner end-to-end: apply, status, rollback and
// re-apply against a real driver. `style` selects the placeholder/timestamp
// dialect (Dollar for PostgreSQL, Question for MySQL/SQLite). DDL is portable
// across all three backends.
inline Task<void> migration_suite(IDBDriver& driver, std::string dsn,
                                  qbuem_routine::migration::PlaceholderStyle style) {
    namespace mig = qbuem_routine::migration;

    auto pool_r = co_await driver.pool(dsn);
    IT_CHECK(pool_r.has_value());
    if (!pool_r) co_return;
    auto pool = std::move(*pool_r);
    auto conn_r = co_await pool->acquire();
    IT_CHECK(conn_r.has_value());
    if (!conn_r) co_return;
    auto conn_ptr = std::move(*conn_r);
    IConnection& conn = *conn_ptr;

    // Clean slate — real servers persist tables across runs.
    co_await conn.query("DROP TABLE IF EXISTS mig_users");
    co_await conn.query("DROP TABLE IF EXISTS __schema_migrations");

    std::vector<mig::Migration> migrations = {
        mig::Migration{
            .version     = 1,
            .description = "create mig_users",
            .up   = "CREATE TABLE mig_users (id BIGINT PRIMARY KEY, email TEXT NOT NULL)",
            .down = "DROP TABLE mig_users",
        },
        mig::Migration{
            .version     = 2,
            .description = "add nickname",
            .up   = "ALTER TABLE mig_users ADD COLUMN nickname TEXT",
            .down = "ALTER TABLE mig_users DROP COLUMN nickname",
        },
    };

    mig::MigrationRunner runner{migrations, conn, style};

    // Apply all pending migrations.
    {
        auto r = co_await runner.migrate();
        IT_CHECK(r.has_value());
        if (r) {
            IT_CHECK(r->applied == 2);
            IT_CHECK(r->latest == 2);
        }
        auto v = co_await runner.current_version();
        IT_CHECK(v.has_value() && *v == 2);
    }

    // The migrated schema works (insert uses the v2 column). $N is converted to ?
    // by the MySQL/SQLite drivers, so the same SQL works on every backend.
    {
        const Value row[] = {Value{int64_t{1}}, Value{std::string_view{"a@b.c"}},
                             Value{std::string_view{"nick"}}};
        auto ins = co_await conn.query(
            "INSERT INTO mig_users(id, email, nickname) VALUES($1, $2, $3)", row);
        IT_CHECK(ins.has_value());
    }

    // Status reports both as applied.
    {
        auto s = co_await runner.status();
        IT_CHECK(s.has_value() && s->size() == 2);
        if (s && s->size() == 2) IT_CHECK((*s)[0].applied && (*s)[1].applied);
    }

    // Roll back the last migration → current version drops to 1.
    {
        auto r = co_await runner.rollback();
        IT_CHECK(r.has_value());
        auto v = co_await runner.current_version();
        IT_CHECK(v.has_value() && *v == 1);
    }

    // Re-migrate → v2 is re-applied, v1 skipped.
    {
        auto r = co_await runner.migrate();
        IT_CHECK(r.has_value());
        if (r) {
            IT_CHECK(r->applied == 1);
            IT_CHECK(r->skipped == 1);
        }
    }

    co_await conn.query("DROP TABLE IF EXISTS mig_users");
    co_await conn.query("DROP TABLE IF EXISTS __schema_migrations");
    co_return;
}

// Exercises the with_transaction() unit-of-work helper end-to-end on a real
// driver: commit on success, rollback on a returned error, and retry on a
// transient error (begin → fn → rollback → re-begin → commit), driving the real
// driver transaction machinery each attempt.
inline Task<void> with_transaction_suite(IDBDriver& driver, std::string dsn,
                                         std::string create_sql) {
    namespace tdb = qbuem_routine::db;
    using qbuem_routine::DbError;
    using qbuem_routine::db_error;

    auto pool_r = co_await driver.pool(dsn);
    IT_CHECK(pool_r.has_value());
    if (!pool_r) co_return;
    auto pool = std::move(*pool_r);
    auto conn_r = co_await pool->acquire();
    IT_CHECK(conn_r.has_value());
    if (!conn_r) co_return;
    auto conn = std::move(*conn_r);

    co_await conn->query("DROP TABLE IF EXISTS wtx");
    IT_CHECK((co_await conn->query(create_sql)).has_value());

    auto count = [&conn]() -> Task<int64_t> {
        auto rs = co_await conn->query("SELECT COUNT(*) FROM wtx");
        if (!rs) co_return -1;
        const IRow* row = co_await (*rs)->next();
        co_return row ? row->get(0).get<int64_t>() : -1;
    };

    // 1. Commit path: an insert inside the block is persisted.
    {
        auto r = co_await tdb::with_transaction(*conn,
            [](tdb::ITransaction& tx) -> Task<Result<void>> {
                const Value row[] = {Value{int64_t{1}}, Value{int64_t{100}}};
                auto e = co_await tx.execute("INSERT INTO wtx(id, n) VALUES($1, $2)", row);
                if (!e) co_return unexpected(e.error());
                co_return Result<void>{};
            });
        IT_CHECK(r.has_value());
        IT_CHECK((co_await count()) == 1);
    }

    // 2. Rollback path: the block inserts but then returns an error → not persisted.
    {
        auto r = co_await tdb::with_transaction(*conn,
            [](tdb::ITransaction& tx) -> Task<Result<void>> {
                const Value row[] = {Value{int64_t{2}}, Value{int64_t{200}}};
                (void)co_await tx.execute("INSERT INTO wtx(id, n) VALUES($1, $2)", row);
                co_return unexpected(db_error(DbError::QueryFailed)); // force rollback
            });
        IT_CHECK(!r.has_value());
        IT_CHECK((co_await count()) == 1); // id=2 rolled back
    }

    // 3. Retry path: transient-fail twice, succeed on the 3rd attempt.
    {
        int attempts = 0;
        auto r = co_await tdb::with_transaction(*conn,
            [&attempts](tdb::ITransaction& tx) -> Task<Result<void>> {
                ++attempts;
                const Value row[] = {Value{int64_t{3}}, Value{int64_t{300}}};
                auto e = co_await tx.execute("INSERT INTO wtx(id, n) VALUES($1, $2)", row);
                if (!e) co_return unexpected(e.error());
                if (attempts < 3) // transient → with_transaction rolls back + retries
                    co_return unexpected(db_error(DbError::Deadlock));
                co_return Result<void>{};
            });
        IT_CHECK(r.has_value());
        IT_CHECK(attempts == 3);            // retried exactly twice
        IT_CHECK((co_await count()) == 2);  // id=3 committed once (ids 1 and 3)
    }

    co_await conn->query("DROP TABLE IF EXISTS wtx");
    co_return;
}

// Verifies query-timeout enforcement: a deliberately slow query on a pool
// configured with a 300ms query_timeout_ms is cancelled by the server and
// surfaces as DbError::StatementTimeout (rather than hanging for the full sleep).
inline Task<void> query_timeout_suite(IDBDriver& driver, std::string dsn,
                                      std::string sleep_sql) {
    using qbuem_routine::DbError;
    using qbuem_routine::db_error;
    PoolConfig cfg;
    cfg.query_timeout_ms = 300; // 0.3s
    auto pool_r = co_await driver.pool(dsn, cfg);
    IT_CHECK(pool_r.has_value());
    if (!pool_r) co_return;
    auto pool = std::move(*pool_r);
    auto conn_r = co_await pool->acquire();
    IT_CHECK(conn_r.has_value());
    if (!conn_r) co_return;
    auto conn = std::move(*conn_r);

    auto r = co_await conn->query(sleep_sql); // sleeps ~2s; must be cut at ~0.3s
    IT_CHECK(!r.has_value());
    if (!r) IT_CHECK(r.error() == db_error(DbError::StatementTimeout));
    co_return;
}

// Verifies the configured query timeout propagates to the session. `probe_sql`
// must SELECT the driver's session timeout variable; it should equal the
// configured query_timeout_ms. (Used for MySQL, whose per-query enforcement of
// max_execution_time is unreliable over the prepared-statement protocol — see the
// driver comment — so we verify the setting is applied rather than that it fires.)
inline Task<void> timeout_configured_suite(IDBDriver& driver, std::string dsn,
                                           std::string probe_sql, int64_t expected_ms) {
    PoolConfig cfg;
    cfg.query_timeout_ms = static_cast<unsigned>(expected_ms);
    auto pool_r = co_await driver.pool(dsn, cfg);
    IT_CHECK(pool_r.has_value());
    if (!pool_r) co_return;
    auto pool = std::move(*pool_r);
    auto conn_r = co_await pool->acquire();
    IT_CHECK(conn_r.has_value());
    if (!conn_r) co_return;
    auto conn = std::move(*conn_r);

    auto rs = co_await conn->query(probe_sql);
    IT_CHECK(rs.has_value());
    if (rs) {
        const IRow* row = co_await (*rs)->next();
        IT_CHECK(row && row->get(0).get<int64_t>() == expected_ms);
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
