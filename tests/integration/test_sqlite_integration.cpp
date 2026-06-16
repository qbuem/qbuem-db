// Integration test: SQLite3 driver end-to-end (in-memory, no server).
#include "qbuem/db/sqlite3_driver.hpp"
#include "harness.hpp"

int main() {
    auto driver = qbuem_routine::make_sqlite_driver();
    return qbuem_db_it::run_main([&driver]() -> qbuem::Task<void> {
        co_await qbuem_db_it::basic_suite(
            *driver, "sqlite://:memory:",
            "CREATE TABLE itest(id INTEGER PRIMARY KEY, name TEXT)");
        co_await qbuem_db_it::large_value_suite(*driver, "sqlite://:memory:");
        co_await qbuem_db_it::orm_suite(
            *driver, "sqlite://:memory:", qbuem_routine::orm::Dialect::SQLite,
            "CREATE TABLE orm_user(id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "name TEXT, age BIGINT)");
        co_await qbuem_db_it::txn_isolation_suite(*driver, "sqlite://:memory:");
        // Poolable shared-cache memory DB (multiple connections share one DB) —
        // proves the SQLite pool hands out independent connections, unlike a
        // private ":memory:" database.
        co_await qbuem_db_it::multi_connection_suite(
            *driver, "sqlite://file:qbuem_it_multiconn?mode=memory&cache=shared");
        co_await qbuem_db_it::migration_suite(
            *driver, "sqlite://:memory:",
            qbuem_routine::migration::PlaceholderStyle::Question);
        co_await qbuem_db_it::with_transaction_suite(
            *driver, "sqlite://:memory:",
            "CREATE TABLE wtx(id BIGINT PRIMARY KEY, n BIGINT)");
        co_await qbuem_db_it::streaming_suite(
            *driver, "sqlite://:memory:",
            "CREATE TABLE strm(id BIGINT PRIMARY KEY, n BIGINT)");
        co_await qbuem_db_it::drain_safety_suite(*driver, "sqlite://:memory:");
    });
}
