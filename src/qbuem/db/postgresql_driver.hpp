#pragma once

/**
 * @file db/postgresql_driver.hpp
 * @brief PostgreSQL 드라이버 implementing qbuem::db::IDBDriver.
 *
 * 시스템 libpq (libpq-dev) 를 사용합니다.
 *   apt install libpq-dev
 *
 * DSN 형식:
 *   postgresql://user:password@host:port/dbname
 *   postgresql://host/dbname
 *   postgresql:///dbname                   — Unix 소켓 (로컬호스트)
 *
 * 연결 풀:
 *   각 acquire() 호출 시 새 PGconn을 생성하고,
 *   return_connection() 시 PQfinish로 정리합니다.
 *   PoolConfig.max_size 를 반영한 실제 풀링은 향후 확장 가능.
 */

#include <qbuem/db/driver.hpp>
#include <memory>

namespace qbuem_routine {

/**
 * @brief PostgreSQL 드라이버 팩토리.
 * 반환된 드라이버를 db::DriverRegistry에 등록하거나 직접 사용합니다.
 */
std::unique_ptr<qbuem::db::IDBDriver> make_postgresql_driver();

} // namespace qbuem_routine
