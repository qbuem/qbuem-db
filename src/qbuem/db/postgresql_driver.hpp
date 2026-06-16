#pragma once

/**
 * @file db/postgresql_driver.hpp
 * @brief PostgreSQL driver implementing qbuem::db::IDBDriver.
 *
 * Uses the system libpq (libpq-dev).
 *   apt install libpq-dev
 *
 * DSN format:
 *   postgresql://user:password@host:port/dbname
 *   postgresql://host/dbname
 *   postgresql:///dbname                   — Unix socket (localhost)
 *
 * Connection pool:
 *   Each acquire() call creates a new PGconn, and
 *   return_connection() cleans it up via PQfinish.
 *   Actual pooling that honors PoolConfig.max_size can be added in the future.
 */

#include <qbuem/db/driver.hpp>
#include <memory>

namespace qbuem_routine {

/**
 * @brief PostgreSQL driver factory.
 * Register the returned driver in db::DriverRegistry or use it directly.
 */
std::unique_ptr<qbuem::db::IDBDriver> make_postgresql_driver();

} // namespace qbuem_routine
