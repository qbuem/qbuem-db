#pragma once

/**
 * @file db/transaction.hpp
 * @brief Unit-of-work helper: run a closure inside a transaction with automatic
 *        commit/rollback and retry on transient (concurrency) errors.
 *
 * ## Usage
 * ```cpp
 * auto r = co_await db::with_transaction(*conn,
 *     [&](db::ITransaction& tx) -> qbuem::Task<qbuem::Result<void>> {
 *         auto a = co_await tx.execute("UPDATE acct SET bal=bal-$1 WHERE id=$2", from);
 *         if (!a) co_return qbuem::unexpected(a.error());
 *         auto b = co_await tx.execute("UPDATE acct SET bal=bal+$1 WHERE id=$2", to);
 *         if (!b) co_return qbuem::unexpected(b.error());
 *         co_return qbuem::Result<void>{};
 *     },
 *     db::IsolationLevel::Serializable);   // retries on 40001/40P01/deadlock/busy
 * ```
 *
 * The closure must be idempotent across retries — on a transient failure the
 * whole block re-runs from a fresh BEGIN. Values produced inside should be
 * captured by reference (assigned only on the committed attempt) or re-read after
 * with_transaction returns success.
 */

#include "db_error.hpp"

#include <qbuem/core/task.hpp>
#include <qbuem/db/driver.hpp>

#include <system_error>
#include <utility>

namespace qbuem_routine::db {

using qbuem::Task;
using qbuem::Result;
using qbuem::unexpected;
using qbuem::db::IConnection;
using qbuem::db::ITransaction;
using qbuem::db::IsolationLevel;

/**
 * Run `fn(ITransaction&)` inside a transaction: BEGIN → fn → COMMIT on success,
 * ROLLBACK on a returned error. If BEGIN, fn, or COMMIT fails with a transient
 * error (serialization failure / deadlock / lock timeout — see is_transient),
 * the whole transaction is retried, up to `max_retries` additional attempts.
 *
 * @param conn        Connection to run on (caller owns its lifetime).
 * @param fn          (ITransaction&) -> Task<Result<void>>; the unit of work.
 * @param level       Isolation level for each attempt.
 * @param max_retries Extra attempts after the first on a transient error.
 * @returns Result<void>{} on commit; the last error otherwise.
 */
template <typename Fn>
Task<Result<void>> with_transaction(IConnection& conn, Fn fn,
                                    IsolationLevel level = IsolationLevel::ReadCommitted,
                                    int max_retries = 3) {
    std::error_code last;
    for (int attempt = 0; attempt <= max_retries; ++attempt) {
        auto tx_r = co_await conn.begin(level);
        if (!tx_r) {
            last = tx_r.error();
            if (is_transient(last)) continue; // contended BEGIN — retry
            co_return unexpected(last);
        }
        auto tx = std::move(*tx_r);

        auto body = co_await fn(*tx);
        if (!body) {
            co_await tx->rollback();
            last = body.error();
            if (is_transient(last)) continue;
            co_return unexpected(last);
        }

        auto commit_r = co_await tx->commit();
        if (commit_r) co_return Result<void>{}; // committed
        // A serialization failure is frequently reported at COMMIT (PostgreSQL).
        // The txn is already aborted server-side; rollback() is best-effort.
        co_await tx->rollback();
        last = commit_r.error();
        if (is_transient(last)) continue;
        co_return unexpected(last);
    }
    co_return unexpected(last); // retries exhausted
}

} // namespace qbuem_routine::db
