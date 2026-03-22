#pragma once

/**
 * @file db/sqlite3_driver.hpp
 * @brief SQLite3 driver implementing qbuem::db::IDBDriver.
 *
 * SQLite3 amalgamation은 FetchContent로 직접 컴파일됩니다.
 * 시스템 라이브러리(libsqlite3-dev) 불필요.
 *
 * DSN 형식:
 *   sqlite:///path/to/file.db   — 파일 DB
 *   sqlite://:memory:           — 인메모리 DB (테스트용)
 *
 * 주의: SQLite3 C API는 동기 블로킹입니다.
 *       이 드라이버는 단일 커넥션 + mutex로 직렬화하며,
 *       개발/테스트 환경에 적합합니다.
 */

#include <qbuem/db/driver.hpp>
#include <memory>

namespace qbuem_routine {

/**
 * @brief SQLite3 드라이버 팩토리.
 * 반환된 드라이버를 db::DriverRegistry에 등록하거나 직접 사용합니다.
 */
std::unique_ptr<qbuem::db::IDBDriver> make_sqlite_driver();

} // namespace qbuem_routine
