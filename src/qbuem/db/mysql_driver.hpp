#pragma once

/**
 * @file db/mysql_driver.hpp
 * @brief MySQL / MariaDB driver — qbuem::db::IDBDriver implementation.
 *
 * Uses the system libmysqlclient.
 *   apt install libmysqlclient-dev   (Debian/Ubuntu)
 *   dnf install mysql-devel          (RHEL/Fedora)
 *
 * DSN format:
 *   mysql://user:password@host:port/dbname
 *   mysql://user:password@host/dbname        (default port 3306)
 *   mysql://user@host/dbname                 (no password)
 *
 * Cloud compatibility:
 *   - Amazon RDS for MySQL / Aurora MySQL
 *   - Google Cloud SQL (MySQL)
 *   - Azure Database for MySQL
 *   - PlanetScale (MySQL compatible)
 *   - TiDB Cloud (MySQL compatible)
 *
 * Features:
 *   - Multi-connection pool (honors PoolConfig.max_size)
 *   - Prepared statement support (MYSQL_STMT)
 *   - Transaction / savepoint support
 *   - Automatic PostgreSQL $N placeholder → MySQL ? conversion
 *   - TLS/SSL connection support (add ssl=true to the DSN)
 *   - UTF8MB4 default character set
 *
 * Note: the MySQL C API is synchronous and blocking.
 *       This driver uses a dedicated MYSQL* handle per connection and
 *       processes concurrent queries through the connection pool.
 *       For high-load asynchronous environments, the PostgreSQL driver is recommended.
 */

#include <qbuem/db/driver.hpp>
#include <memory>

namespace qbuem_routine {

/**
 * @brief MySQL/MariaDB driver factory.
 * Use the returned driver directly or register it in the registry.
 */
std::unique_ptr<qbuem::db::IDBDriver> make_mysql_driver();

} // namespace qbuem_routine
