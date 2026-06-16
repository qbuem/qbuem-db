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
    // ── Transient / retryable ───────────────────────────────────────────────
    // The DB rejected the statement/transaction due to concurrency, not a logic
    // error; the same operation will usually succeed if retried.  Mapped from the
    // driver's native code (PG SQLSTATE 40001/40P01, MySQL 1213/1205, SQLITE_BUSY/
    // _LOCKED).  with_transaction() retries automatically on these.
    SerializationFailure   = 6,  // could not serialize access (PG 40001)
    Deadlock               = 7,  // deadlock detected (PG 40P01, MySQL 1213)
    LockTimeout            = 8,  // lock wait timed out / busy (MySQL 1205, SQLITE_BUSY)
    // ── Timeout ─────────────────────────────────────────────────────────────
    StatementTimeout       = 9,  // query exceeded the configured timeout
};

// True for errors that are worth retrying (transient concurrency conflicts).
[[nodiscard]] inline bool is_transient(DbError e) noexcept {
    return e == DbError::SerializationFailure
        || e == DbError::Deadlock
        || e == DbError::LockTimeout;
}

struct DbErrorCategory final : std::error_category {
    const char* name() const noexcept override { return "qbuem.db"; }
    std::string message(int code) const override {
        switch (static_cast<DbError>(code)) {
            case DbError::ConnectionFailed:       return "db: connection failed";
            case DbError::PoolExhausted:          return "db: connection pool exhausted";
            case DbError::PrepareStatementFailed: return "db: prepare statement failed";
            case DbError::QueryFailed:            return "db: query failed";
            case DbError::TransactionFailed:      return "db: transaction failed";
            case DbError::SerializationFailure:   return "db: serialization failure (retryable)";
            case DbError::Deadlock:               return "db: deadlock detected (retryable)";
            case DbError::LockTimeout:            return "db: lock wait timeout (retryable)";
            case DbError::StatementTimeout:       return "db: statement timeout";
        }
        return "db: unknown error";
    }
};

inline std::error_code db_error(DbError e) noexcept {
    static DbErrorCategory cat;
    return {static_cast<int>(e), cat};
}

// True if the error_code is a qbuem.db transient (retryable) error.
[[nodiscard]] inline bool is_transient(const std::error_code& ec) noexcept {
    return ec.category() == db_error(DbError::QueryFailed).category()
        && is_transient(static_cast<DbError>(ec.value()));
}

} // namespace qbuem_routine
