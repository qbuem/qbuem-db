// Integration test: MySQL driver end-to-end (needs a running server).
// DSN from MYSQL_DSN env, default points at the CI/docker test server.
#include "qbuem/db/mysql_driver.hpp"
#include "harness.hpp"

#include <cstdlib>

int main() {
    const char* dsn = std::getenv("MYSQL_DSN");
    auto driver = qbuem_routine::make_mysql_driver();
    return qbuem_db_it::run_main([&driver, dsn]() -> qbuem::Task<void> {
        // NB: 127.0.0.1 (TCP), not "localhost" — libmysqlclient maps
        // host="localhost" to a unix socket, not the TCP port.
        const char* default_dsn = "mysql://root:test@127.0.0.1:3306/test";
        co_await qbuem_db_it::basic_suite(
            *driver,
            dsn ? dsn : default_dsn,
            "CREATE TABLE itest(id BIGINT PRIMARY KEY, name VARCHAR(255))");
        co_await qbuem_db_it::large_value_suite(*driver, dsn ? dsn : default_dsn);
        co_await qbuem_db_it::orm_suite(
            *driver, dsn ? dsn : default_dsn, qbuem_routine::orm::Dialect::MySQL,
            "CREATE TABLE orm_user(id BIGINT AUTO_INCREMENT PRIMARY KEY, "
            "name VARCHAR(255), age BIGINT)");
        co_await qbuem_db_it::txn_isolation_suite(*driver, dsn ? dsn : default_dsn);
        co_await qbuem_db_it::multi_connection_suite(*driver, dsn ? dsn : default_dsn);
        co_await qbuem_db_it::drain_safety_suite(*driver, dsn ? dsn : default_dsn);
    });
}
