// Integration test: PostgreSQL driver end-to-end (needs a running server).
// DSN from PG_DSN env, default points at the CI/docker test server.
#include "qbuem/db/postgresql_driver.hpp"
#include "harness.hpp"

#include <cstdlib>

int main() {
    const char* dsn = std::getenv("PG_DSN");
    auto driver = qbuem_routine::make_postgresql_driver();
    return qbuem_db_it::run_main([&driver, dsn]() -> qbuem::Task<void> {
        co_await qbuem_db_it::basic_suite(
            *driver,
            dsn ? dsn : "postgresql://postgres:test@localhost:5433/testdb",
            "CREATE TABLE itest(id BIGINT PRIMARY KEY, name TEXT)");
        co_await qbuem_db_it::drain_safety_suite(
            *driver, dsn ? dsn : "postgresql://postgres:test@localhost:5433/testdb");
    });
}
