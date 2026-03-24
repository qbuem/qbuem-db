#pragma once

/**
 * @file db/mysql_driver.hpp
 * @brief MySQL / MariaDB 드라이버 — qbuem::db::IDBDriver 구현.
 *
 * 시스템 libmysqlclient 를 사용합니다.
 *   apt install libmysqlclient-dev   (Debian/Ubuntu)
 *   dnf install mysql-devel          (RHEL/Fedora)
 *
 * DSN 형식:
 *   mysql://user:password@host:port/dbname
 *   mysql://user:password@host/dbname        (포트 기본값 3306)
 *   mysql://user@host/dbname                 (비밀번호 없음)
 *
 * 클라우드 호환:
 *   - Amazon RDS for MySQL / Aurora MySQL
 *   - Google Cloud SQL (MySQL)
 *   - Azure Database for MySQL
 *   - PlanetScale (MySQL 호환)
 *   - TiDB Cloud (MySQL 호환)
 *
 * 특징:
 *   - 멀티 커넥션 풀 (PoolConfig.max_size 반영)
 *   - Prepared statement 지원 (MYSQL_STMT)
 *   - 트랜잭션 / 세이브포인트 지원
 *   - PostgreSQL $N 플레이스홀더 → MySQL ? 자동 변환
 *   - TLS/SSL 연결 지원 (DSN에 ssl=true 추가)
 *   - UTF8MB4 기본 문자셋
 *
 * 주의: MySQL C API는 동기 블로킹입니다.
 *       이 드라이버는 각 커넥션에 고유 MYSQL* 핸들을 사용하며
 *       연결 풀로 동시 쿼리를 처리합니다.
 *       고부하 비동기 환경에서는 PostgreSQL 드라이버를 권장합니다.
 */

#include <qbuem/db/driver.hpp>
#include <memory>

namespace qbuem_routine {

/**
 * @brief MySQL/MariaDB 드라이버 팩토리.
 * 반환된 드라이버를 직접 사용하거나 레지스트리에 등록합니다.
 */
std::unique_ptr<qbuem::db::IDBDriver> make_mysql_driver();

} // namespace qbuem_routine
