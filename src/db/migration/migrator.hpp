#pragma once

/**
 * @file db/migration/migrator.hpp
 * @brief 드라이버 독립적 DB 마이그레이션 프레임워크.
 *
 * ## 개념
 * - **Migration**: 버전 번호 + 설명 + up SQL (+ 선택적 down SQL)
 * - **MigrationRunner**: IConnection을 통해 마이그레이션을 적용·롤백
 * - 적용 이력은 `__schema_migrations` 테이블에 저장
 * - 버전 번호는 unsigned integer (보통 타임스탬프 또는 순번 사용)
 *
 * ## 사용법
 * ```cpp
 * #include "db/migration/migrator.hpp"
 *
 * // 마이그레이션 정의
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
 * // 마이그레이션 실행
 * auto conn_r = co_await pool->acquire();
 * MigrationRunner runner{kMigrations, conn_r->get()};
 * auto result = co_await runner.migrate();
 * if (!result) { handle_error(result.error()); }
 *
 * // 상태 확인
 * auto status = co_await runner.status();
 * for (auto& s : *status) {
 *     fmt::print("{:04d} {:30s} {}\n",
 *                s.version, s.description,
 *                s.applied ? "applied" : "pending");
 * }
 *
 * // 특정 버전으로 롤백
 * co_await runner.rollback_to(1);
 * ```
 *
 * ## 드라이버별 플레이스홀더 차이
 * - PostgreSQL: $1, $2, ...  (기본값)
 * - MySQL:      ?, ?, ...    (PlaceholderStyle::Question)
 * - SQLite:     ?, ?, ...    (PlaceholderStyle::Question)
 *
 * MigrationRunner 생성 시 style 인자로 지정합니다.
 */

#include <qbuem/core/task.hpp>
#include <qbuem/db/driver.hpp>

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

// ── 플레이스홀더 스타일 ──────────────────────────────────────────────────────

enum class PlaceholderStyle {
    Dollar,    ///< PostgreSQL: $1, $2, ...
    Question,  ///< MySQL / SQLite: ?, ?, ...
};

// ── Migration 정의 ────────────────────────────────────────────────────────────

struct Migration {
    uint64_t    version;         ///< 고유 버전 번호 (타임스탬프 또는 순번)
    std::string description;     ///< 설명 (사람이 읽기 위한 문자열)
    std::string up;              ///< 적용 SQL (세미콜론으로 구분된 여러 문장 지원)
    std::string down;            ///< 롤백 SQL (생략 가능)
};

// ── MigrationStatus ───────────────────────────────────────────────────────────

struct MigrationStatus {
    uint64_t    version;
    std::string description;
    bool        applied{false};
    std::optional<std::string> applied_at; ///< ISO8601 timestamp (적용된 경우)
};

// ── MigrationResult ───────────────────────────────────────────────────────────

struct MigrationResult {
    uint32_t applied{0};   ///< 이번 실행에서 적용된 마이그레이션 수
    uint32_t skipped{0};   ///< 이미 적용되어 건너뛴 수
    uint64_t latest{0};    ///< 최종 버전 번호 (0 = 없음)
};

// ── 에러 카테고리 ─────────────────────────────────────────────────────────────

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

// ── SQL 분할 (세미콜론 구분) ──────────────────────────────────────────────────

inline std::vector<std::string_view> split_sql(std::string_view sql) {
    std::vector<std::string_view> stmts;
    std::size_t start = 0;
    bool in_str = false;
    for (std::size_t i = 0; i < sql.size(); ++i) {
        const char c = sql[i];
        if (c == '\'' && (i == 0 || sql[i - 1] != '\\')) in_str = !in_str;
        if (!in_str && c == ';') {
            auto stmt = sql.substr(start, i - start);
            // 공백만 있는 구문은 건너뜀
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
     * @param migrations  정렬된 마이그레이션 목록 (version 오름차순)
     * @param conn        DB 커넥션 (호출자가 수명 관리)
     * @param style       플레이스홀더 스타일 (기본: Dollar/$N)
     */
    MigrationRunner(std::span<const Migration> migrations,
                    IConnection&               conn,
                    PlaceholderStyle           style = PlaceholderStyle::Dollar)
        : migrations_(migrations.begin(), migrations.end())
        , conn_(conn)
        , style_(style) {
        // version 순 정렬 보장
        std::ranges::sort(migrations_,
                          [](const Migration& a, const Migration& b){
                              return a.version < b.version;
                          });
    }

    // ── 초기화 — __schema_migrations 테이블 생성 ─────────────────────────────

    Task<Result<void>> init() {
        const std::string sql = std::format(R"sql(
            CREATE TABLE IF NOT EXISTS __schema_migrations (
                version     BIGINT      NOT NULL PRIMARY KEY,
                description TEXT        NOT NULL DEFAULT '',
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

    // ── 현재 최대 적용 버전 ──────────────────────────────────────────────────

    Task<Result<uint64_t>> current_version() {
        const std::string sql =
            "SELECT COALESCE(MAX(version), 0) FROM __schema_migrations";
        auto r = co_await conn_.query(sql, {});
        if (!r) co_return unexpected(r.error());
        auto* row = co_await (*r)->next();
        if (!row) co_return uint64_t{0};
        co_return static_cast<uint64_t>(row->get(uint16_t{0}).get<int64_t>());
    }

    // ── 적용 여부 확인 ───────────────────────────────────────────────────────

    Task<Result<bool>> is_applied(uint64_t version) {
        const std::string sql = std::format(
            "SELECT 1 FROM __schema_migrations WHERE version = {}",
            ph(1));
        auto r = co_await conn_.query(sql, {Value{static_cast<int64_t>(version)}});
        if (!r) co_return unexpected(r.error());
        co_return (co_await (*r)->next()) != nullptr;
    }

    // ── 마이그레이션 상태 목록 ───────────────────────────────────────────────

    Task<Result<std::vector<MigrationStatus>>> status() {
        auto init_r = co_await init();
        if (!init_r) co_return unexpected(init_r.error());

        // 적용된 버전 로드
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

    // ── 전체 마이그레이션 적용 (pending 모두) ────────────────────────────────

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

    // ── 특정 버전까지만 적용 ─────────────────────────────────────────────────

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

    // ── 마지막 마이그레이션 롤백 ─────────────────────────────────────────────

    Task<Result<void>> rollback() {
        auto ver_r = co_await current_version();
        if (!ver_r) co_return unexpected(ver_r.error());
        if (*ver_r == 0) co_return {}; // 적용된 마이그레이션 없음

        for (auto it = migrations_.rbegin(); it != migrations_.rend(); ++it) {
            if (it->version == *ver_r) {
                co_return co_await rollback_one(*it);
            }
        }
        co_return unexpected(migration_error(4));
    }

    // ── 특정 버전까지 롤백 (그 버전 포함하여 제거) ──────────────────────────

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

    // ── 적용된 버전 일괄 로드 (N+1 쿼리 방지) ────────────────────────────────

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

    // ── 플레이스홀더 ─────────────────────────────────────────────────────────

    [[nodiscard]] std::string ph(int n) const {
        if (style_ == PlaceholderStyle::Dollar)
            return std::format("${}", n);
        return "?";
    }

    // ── 타임스탬프 타입 (드라이버별) ─────────────────────────────────────────

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

    // ── 단일 마이그레이션 적용 ───────────────────────────────────────────────

    Task<Result<void>> apply_one(const Migration& m) {
        auto txn_r = co_await conn_.begin(IsolationLevel::ReadCommitted);
        if (!txn_r) co_return unexpected(migration_error(2));
        auto& txn = *txn_r;

        // up SQL 문장들 실행
        for (auto stmt : split_sql(m.up)) {
            auto r = co_await txn->execute(stmt, {});
            if (!r) {
                co_await txn->rollback();
                co_return unexpected(migration_error(2));
            }
        }

        // 이력 기록
        const std::string insert = std::format(
            "INSERT INTO __schema_migrations(version, description) VALUES ({}, {})",
            ph(1), ph(2));
        auto ins_r = co_await txn->execute(insert, {
            Value{static_cast<int64_t>(m.version)},
            Value{std::string_view{m.description}},
        });
        if (!ins_r) {
            co_await txn->rollback();
            co_return unexpected(migration_error(2));
        }

        auto commit_r = co_await txn->commit();
        if (!commit_r) co_return unexpected(migration_error(2));
        co_return {};
    }

    // ── 단일 마이그레이션 롤백 ───────────────────────────────────────────────

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

        // 이력 제거
        const std::string del = std::format(
            "DELETE FROM __schema_migrations WHERE version = {}", ph(1));
        auto del_r = co_await txn->execute(del, {
            Value{static_cast<int64_t>(m.version)},
        });
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
