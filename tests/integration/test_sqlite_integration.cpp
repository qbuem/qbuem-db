// Integration test: SQLite3 driver end-to-end (in-memory, no server).
#include "qbuem/db/sqlite3_driver.hpp"
#include "harness.hpp"

int main() {
    auto driver = qbuem_routine::make_sqlite_driver();
    return qbuem_db_it::run_main([&driver]() -> qbuem::Task<void> {
        co_await qbuem_db_it::basic_suite(
            *driver, "sqlite://:memory:",
            "CREATE TABLE itest(id INTEGER PRIMARY KEY, name TEXT)");
    });
}
