#pragma once

/**
 * @file db/migration/migrator.hpp
 * @brief Driver-independent DB migration framework.
 *
 * ## Concepts
 * - **Migration**: version number + description + up SQL (+ optional down SQL)
 * - **MigrationRunner**: applies and rolls back migrations through IConnection
 * - Applied history is stored in the `__schema_migrations` table
 * - Version numbers are unsigned integers (typically a timestamp or sequence number)
 *
 * ## Usage
 * ```cpp
 * #include "db/migration/migrator.hpp"
 *
 * // Define migrations
 * using namespace qbuem_routine::migration;
 *
 * static const std::vector<Migration> kMigrations = {
 *     {
 *         .version = 1,
 *         .description = "create users table",
 *         .up = R"sql(
 *             CREATE TABLE users (
 *                 id    BIGSERIAL PRIMARY KEY,
 *                 email TEXT NOT NULL UNIQUE,
 *                 name  TEXT NOT NULL
 *             )
 *         )sql",
 *         .down = "DROP TABLE users",
 *     },
 *     {
 *         .version = 2,
 *         .description = "add created_at to users",
 *         .up = "ALTER TABLE users ADD COLUMN created_at TIMESTAMPTZ DEFAULT NOW()",
 *         .down = "ALTER TABLE users DROP COLUMN created_at",
 *     },
 * };
 *
 * // Run migrations
 * auto conn_r = co_await pool->acquire();
 * MigrationRunner runner{kMigrations, conn_r->get()};
 * auto result = co_await runner.migrate();
 * if (!result) { handle_error(result.error()); }
 *
 * // Check status
 * auto status = co_await runner.status();
 * for (auto& s : *status) {
 *     fmt::print("{:04d} {:30s} {}\n",
 *                s.version, s.description,
 *                s.applied ? "applied" : "pending");
 * }
 *
 * // Roll back to a specific version
 * co_await runner.rollback_to(1);
 * ```
 *
 * ## Placeholder differences by driver
 * - PostgreSQL: $1, $2, ...  (default)
 * - MySQL:      ?, ?, ...    (PlaceholderStyle::Question)
 * - SQLite:     ?, ?, ...    (PlaceholderStyle::Question)
 *
 * Specify it via the style argument when constructing a MigrationRunner.
 */

#include <qbuem/core/task.hpp>
#include <qbuem/db/driver.hpp>

#include <algorithm>
#include <chrono>
#include <format>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace qbuem_routine::migration {

using qbuem::Task;
using qbuem::Result;
using qbuem::unexpected;
using qbuem::db::IConnection;
using qbuem::db::Value;
using qbuem::db::IsolationLevel;

// ── Placeholder style ───────────────────────────────────────────────────────

enum class PlaceholderStyle {
    Dollar,    ///< PostgreSQL: $1, $2, ...
    Question,  ///< MySQL / SQLite: ?, ?, ...
};

// ── Migration definition ──────────────────────────────────────────────────────

struct Migration {
    uint64_t    version;         ///< Unique version number (timestamp or sequence number)
    std::string description;     ///< Description (human-readable string)
    std::string up;              ///< Apply SQL (supports multiple semicolon-separated statements)
    std::string down;            ///< Rollback SQL (optional)
};

// ── MigrationStatus ───────────────────────────────────────────────────────────

struct MigrationStatus {
    uint64_t    version;
    std::string description;
    bool        applied{false};
    std::optional<std::string> applied_at; ///< ISO8601 timestamp (if applied)
};

// ── MigrationResult ───────────────────────────────────────────────────────────

struct MigrationResult {
    uint32_t applied{0};   ///< Number of migrations applied in this run
    uint32_t skipped{0};   ///< Number skipped because already applied
    uint64_t latest{0};    ///< Latest version number (0 = none)
};

// ── Error category ────────────────────────────────────────────────────────────

struct MigrationErrorCategory : std::error_category {
    const char* name() const noexcept override { return "migration"; }
    std::string message(int code) const override {
        switch (code) {
            case 1: return "migration: schema table init failed";
            case 2: return "migration: apply failed";
            case 3: return "migration: rollback failed";
            case 4: return "migration: version not found";
            case 5: return "migration: no down migration defined";
            default: return "migration: unknown error";
        }
    }
};

inline std::error_code migration_error(int code) {
    static MigrationErrorCategory cat;
    return {code, cat};
}

// ── SQL splitting (semicolon-separated) ───────────────────────────────────────

inline std::vector<std::string_view> split_sql(std::string_view sql) {
    std::vector<std::string_view> stmts;
    std::size_t start = 0;
    bool in_str = false;
    for (std::size_t i = 0; i < sql.size(); ++i) {
        const char c = sql[i];
        if (c == '\'' && (i == 0 || sql[i - 1] != '\\')) in_str = !in_str;
        if (!in_str && c == ';') {
            auto stmt = sql.substr(start, i - start);
            // Skip statements that contain only whitespace
            if (stmt.find_first_not_of(" \t\r\n") != std::string_view::npos)
                stmts.push_back(stmt);
            start = i + 1;
        }
    }
    auto last = sql.substr(start);
    if (last.find_first_not_of(" \t\r\n") != std::string_view::npos)
        stmts.push_back(last);
    return stmts;
}

// ── MigrationRunner ───────────────────────────────────────────────────────────

class MigrationRunner {
public:
    /**
     * @param migrations  Sorted list of migrations (ascending by version)
     * @param conn        DB connection (lifetime managed by the caller)
     * @param style       Placeholder style (default: Dollar/$N)
     */
    MigrationRunner(std::span<const Migration> migrations,
                    IConnection&               conn,
                    PlaceholderStyle           style = PlaceholderStyle::Dollar)
        : migrations_(migrations.begin(), migrations.end())
        , conn_(conn)
        , style_(style) {
        // Ensure ordering by version
        std::ranges::sort(migrations_,
                          [](const Migration& a, const Migration& b){
                              return a.version < b.version;
                          });
    }

    // ── Initialization — create the __schema_migrations table ────────────────

    Task<Result<void>> init() {
        // NB: description is TEXT NOT NULL with no DEFAULT — MySQL forbids a
        // DEFAULT on TEXT/BLOB columns, and apply_one always supplies it anyway.
        const std::string sql = std::format(R"sql(
            CREATE TABLE IF NOT EXISTS __schema_migrations (
                version     BIGINT      NOT NULL PRIMARY KEY,
                description TEXT        NOT NULL,
                applied_at  {}  NOT NULL DEFAULT {}
            )
        )sql",
        timestamp_type(), current_timestamp());

        for (auto stmt : split_sql(sql)) {
            auto r = co_await conn_.query(stmt, {});
            if (!r) co_return unexpected(migration_error(1));
        }
        co_return {};
    }

    // ── Current maximum applied version ──────────────────────────────────────

    Task<Result<uint64_t>> current_version() {
        const std::string sql =
            "SELECT COALESCE(MAX(version), 0) FROM __schema_migrations";
        auto r = co_await conn_.query(sql, {});
        if (!r) co_return unexpected(r.error());
        auto* row = co_await (*r)->next();
        if (!row) co_return uint64_t{0};
        co_return static_cast<uint64_t>(row->get(uint16_t{0}).get<int64_t>());
    }

    // ── Check whether applied ────────────────────────────────────────────────

    Task<Result<bool>> is_applied(uint64_t version) {
        const std::string sql = std::format(
            "SELECT 1 FROM __schema_migrations WHERE version = {}",
            ph(1));
        const Value params[] = {Value{static_cast<int64_t>(version)}};
        auto r = co_await conn_.query(sql, params);
        if (!r) co_return unexpected(r.error());
        co_return (co_await (*r)->next()) != nullptr;
    }

    // ── Migration status list ────────────────────────────────────────────────

    Task<Result<std::vector<MigrationStatus>>> status() {
        auto init_r = co_await init();
        if (!init_r) co_return unexpected(init_r.error());

        // Load applied versions
        const std::string sql =
            "SELECT version, applied_at FROM __schema_migrations ORDER BY version";
        auto r = co_await conn_.query(sql, {});
        if (!r) co_return unexpected(r.error());

        std::vector<std::pair<uint64_t, std::string>> applied;
        while (auto* row = co_await (*r)->next()) {
            auto ver = static_cast<uint64_t>(row->get(uint16_t{0}).get<int64_t>());
            auto at  = std::string{row->get(uint16_t{1}).get<std::string_view>()};
            applied.emplace_back(ver, std::move(at));
        }

        std::vector<MigrationStatus> result;
        for (const auto& m : migrations_) {
            MigrationStatus s;
            s.version     = m.version;
            s.description = m.description;
            for (const auto& [v, at] : applied) {
                if (v == m.version) {
                    s.applied    = true;
                    s.applied_at = at;
                    break;
                }
            }
            result.push_back(std::move(s));
        }
        co_return result;
    }

    // ── Apply all migrations (all pending) ───────────────────────────────────

    Task<Result<MigrationResult>> migrate() {
        auto init_r = co_await init();
        if (!init_r) co_return unexpected(init_r.error());

        auto applied_set_r = co_await load_applied_set();
        if (!applied_set_r) co_return unexpected(applied_set_r.error());
        const auto& applied_set = *applied_set_r;

        MigrationResult result;
        for (const auto& m : migrations_) {
            if (applied_set.contains(m.version)) {
                ++result.skipped;
                result.latest = m.version;
                continue;
            }
            auto r = co_await apply_one(m);
            if (!r) co_return unexpected(r.error());
            ++result.applied;
            result.latest = m.version;
        }
        co_return result;
    }

    // ── Apply only up to a specific version ──────────────────────────────────

    Task<Result<MigrationResult>> migrate_to(uint64_t target_version) {
        auto init_r = co_await init();
        if (!init_r) co_return unexpected(init_r.error());

        auto applied_set_r = co_await load_applied_set();
        if (!applied_set_r) co_return unexpected(applied_set_r.error());
        const auto& applied_set = *applied_set_r;

        MigrationResult result;
        for (const auto& m : migrations_) {
            if (m.version > target_version) break;
            if (applied_set.contains(m.version)) {
                ++result.skipped;
                result.latest = m.version;
                continue;
            }
            auto r = co_await apply_one(m);
            if (!r) co_return unexpected(r.error());
            ++result.applied;
            result.latest = m.version;
        }
        co_return result;
    }

    // ── Roll back the last migration ─────────────────────────────────────────

    Task<Result<void>> rollback() {
        auto ver_r = co_await current_version();
        if (!ver_r) co_return unexpected(ver_r.error());
        if (*ver_r == 0) co_return {}; // No applied migrations

        for (auto it = migrations_.rbegin(); it != migrations_.rend(); ++it) {
            if (it->version == *ver_r) {
                co_return co_await rollback_one(*it);
            }
        }
        co_return unexpected(migration_error(4));
    }

    // ── Roll back to a specific version (removing that version too) ──────────

    Task<Result<void>> rollback_to(uint64_t target_version) {
        auto ver_r = co_await current_version();
        if (!ver_r) co_return unexpected(ver_r.error());

        for (auto it = migrations_.rbegin(); it != migrations_.rend(); ++it) {
            if (it->version <= target_version) break;
            if (it->version > *ver_r) continue;
            auto r = co_await rollback_one(*it);
            if (!r) co_return unexpected(r.error());
        }
        co_return {};
    }

private:
    std::vector<Migration> migrations_;
    IConnection&           conn_;
    PlaceholderStyle       style_;

    // ── Bulk-load applied versions (avoids N+1 queries) ──────────────────────

    Task<Result<std::unordered_set<uint64_t>>> load_applied_set() {
        const std::string sql =
            "SELECT version FROM __schema_migrations";
        auto r = co_await conn_.query(sql, {});
        if (!r) co_return unexpected(r.error());

        std::unordered_set<uint64_t> applied;
        while (auto* row = co_await (*r)->next()) {
            applied.insert(
                static_cast<uint64_t>(row->get(uint16_t{0}).get<int64_t>()));
        }
        co_return applied;
    }

    // ── Placeholder ──────────────────────────────────────────────────────────

    [[nodiscard]] std::string ph(int n) const {
        if (style_ == PlaceholderStyle::Dollar)
            return std::format("${}", n);
        return "?";
    }

    // ── Timestamp type (per driver) ──────────────────────────────────────────

    [[nodiscard]] std::string_view timestamp_type() const noexcept {
        return (style_ == PlaceholderStyle::Dollar)
            ? "TIMESTAMPTZ"    // PostgreSQL
            : "DATETIME";      // MySQL / SQLite
    }

    [[nodiscard]] std::string_view current_timestamp() const noexcept {
        return (style_ == PlaceholderStyle::Dollar)
            ? "NOW()"           // PostgreSQL
            : "CURRENT_TIMESTAMP"; // MySQL / SQLite
    }

    // ── Apply a single migration ─────────────────────────────────────────────

    Task<Result<void>> apply_one(const Migration& m) {
        auto txn_r = co_await conn_.begin(IsolationLevel::ReadCommitted);
        if (!txn_r) co_return unexpected(migration_error(2));
        auto& txn = *txn_r;

        // Execute the up SQL statements
        for (auto stmt : split_sql(m.up)) {
            auto r = co_await txn->execute(stmt, {});
            if (!r) {
                co_await txn->rollback();
                co_return unexpected(migration_error(2));
            }
        }

        // Record history
        const std::string insert = std::format(
            "INSERT INTO __schema_migrations(version, description) VALUES ({}, {})",
            ph(1), ph(2));
        const Value ins_params[] = {
            Value{static_cast<int64_t>(m.version)},
            Value{std::string_view{m.description}},
        };
        auto ins_r = co_await txn->execute(insert, ins_params);
        if (!ins_r) {
            co_await txn->rollback();
            co_return unexpected(migration_error(2));
        }

        auto commit_r = co_await txn->commit();
        if (!commit_r) co_return unexpected(migration_error(2));
        co_return {};
    }

    // ── Roll back a single migration ─────────────────────────────────────────

    Task<Result<void>> rollback_one(const Migration& m) {
        if (m.down.empty())
            co_return unexpected(migration_error(5));

        auto txn_r = co_await conn_.begin(IsolationLevel::ReadCommitted);
        if (!txn_r) co_return unexpected(migration_error(3));
        auto& txn = *txn_r;

        for (auto stmt : split_sql(m.down)) {
            auto r = co_await txn->execute(stmt, {});
            if (!r) {
                co_await txn->rollback();
                co_return unexpected(migration_error(3));
            }
        }

        // Remove history
        const std::string del = std::format(
            "DELETE FROM __schema_migrations WHERE version = {}", ph(1));
        const Value del_params[] = {Value{static_cast<int64_t>(m.version)}};
        auto del_r = co_await txn->execute(del, del_params);
        if (!del_r) {
            co_await txn->rollback();
            co_return unexpected(migration_error(3));
        }

        auto commit_r = co_await txn->commit();
        if (!commit_r) co_return unexpected(migration_error(3));
        co_return {};
    }
};

} // namespace qbuem_routine::migration
