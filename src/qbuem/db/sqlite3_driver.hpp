#pragma once

/**
 * @file db/sqlite3_driver.hpp
 * @brief SQLite3 driver implementing qbuem::db::IDBDriver.
 *
 * The SQLite3 amalgamation is compiled directly via FetchContent.
 * No system library (libsqlite3-dev) required.
 *
 * DSN format:
 *   sqlite:///path/to/file.db   — file-backed DB
 *   sqlite://:memory:           — in-memory DB (for testing)
 *
 * Note: the SQLite3 C API is synchronous and blocking.
 *       This driver serializes access with a single connection + mutex,
 *       making it suitable for development/test environments.
 */

#include <qbuem/db/driver.hpp>
#include <memory>

namespace qbuem_routine {

/**
 * @brief SQLite3 driver factory.
 * Register the returned driver in db::DriverRegistry or use it directly.
 */
std::unique_ptr<qbuem::db::IDBDriver> make_sqlite_driver();

} // namespace qbuem_routine
