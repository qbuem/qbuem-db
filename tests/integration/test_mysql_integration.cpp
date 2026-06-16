// Integration test: MySQL driver end-to-end (needs a running server).
// DSN from MYSQL_DSN env, default points at the CI/docker test server.
#include "qbuem/db/mysql_driver.hpp"
#include "harness.hpp"

#include <cstdlib>

int main() {
    const char* dsn = std::getenv("MYSQL_DSN");
    auto driver = qbuem_routine::make_mysql_driver();
    return qbuem_db_it::run_main([&driver, dsn]() -> qbuem::Task<void> {
        co_await qbuem_db_it::basic_suite(
            *driver,
            // NB: 127.0.0.1 (TCP), not "localhost" — libmysqlclient maps
            // host="localhost" to a unix socket, not the TCP port.
            dsn ? dsn : "mysql://root:test@127.0.0.1:3306/test",
            "CREATE TABLE itest(id BIGINT PRIMARY KEY, name VARCHAR(255))");
    });
}
