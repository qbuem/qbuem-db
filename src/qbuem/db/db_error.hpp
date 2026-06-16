#pragma once

#include <system_error>
#include <string>

namespace qbuem_routine {

// ─── Common DB error codes ────────────────────────────────────────────────────
//
// Every driver implementing the IDBDriver / IConnectionPool / IConnection /
// ITransaction / IStatement interfaces (PostgreSQL, MySQL, SQLite3) returns these
// error codes. Callers can handle errors the same way regardless of driver type.

enum class DbError : int {
    // ── Pool / connection level ─────────────────────────────────────────────
    ConnectionFailed       = 1,  // pool() / acquire(): cannot connect to the DB server
    PoolExhausted          = 2,  // acquire(): no available connection (synchronous driver)
    // ── Query level ─────────────────────────────────────────────────────────
    PrepareStatementFailed = 3,  // prepare(): failed to create the prepared statement
    QueryFailed            = 4,  // query() / execute() / execute_dml(): SQL execution failed
    // ── Transaction level ───────────────────────────────────────────────────
    TransactionFailed      = 5,  // begin() / commit() / rollback() / savepoint() failed
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
