#pragma once

#include <system_error>
#include <string>

namespace qbuem_routine {

// ─── 공통 DB 에러 코드 ────────────────────────────────────────────────────────
//
// IDBDriver / IConnectionPool / IConnection / ITransaction / IStatement 인터페이스를
// 구현하는 모든 드라이버(PostgreSQL, MySQL, SQLite3)는 이 에러 코드를 반환한다.
// 호출자는 드라이버 종류에 무관하게 동일한 방식으로 에러를 처리할 수 있다.

enum class DbError : int {
    // ── 풀 / 연결 수준 ──────────────────────────────────────────────────────
    ConnectionFailed       = 1,  // pool() / acquire(): DB 서버 연결 불가
    PoolExhausted          = 2,  // acquire(): 가용 연결 없음 (동기 드라이버)
    // ── 쿼리 수준 ──────────────────────────────────────────────────────────
    PrepareStatementFailed = 3,  // prepare(): prepared statement 생성 실패
    QueryFailed            = 4,  // query() / execute() / execute_dml(): SQL 실행 실패
    // ── 트랜잭션 수준 ──────────────────────────────────────────────────────
    TransactionFailed      = 5,  // begin() / commit() / rollback() / savepoint() 실패
};

struct DbErrorCategory final : std::error_category {
    const char* name() const noexcept override { return "qbuem.db"; }
    std::string message(int code) const override {
        switch (static_cast<DbError>(code)) {
            case DbError::ConnectionFailed:       return "db: connection failed";
            case DbError::PoolExhausted:          return "db: connection pool exhausted";
            case DbError::PrepareStatementFailed: return "db: prepare statement failed";
            case DbError::QueryFailed:            return "db: query failed";
            case DbError::TransactionFailed:      return "db: transaction failed";
        }
        return "db: unknown error";
    }
};

inline std::error_code db_error(DbError e) noexcept {
    static DbErrorCategory cat;
    return {static_cast<int>(e), cat};
}

} // namespace qbuem_routine
