#pragma once

/**
 * @file db/orm.hpp
 * @brief 제너릭 ORM 유틸리티 — qbuem::db::IRow 기반 (드라이버 독립)
 *
 * ## 드라이버별 Dialect 설정
 * ```cpp
 * using namespace qbuem_routine::orm;
 *
 * // PostgreSQL (기본값)
 * orm::register_table<User>("users")
 *     .pk("id",    &User::id)
 *     .col("email", &User::email);
 *
 * // MySQL / MariaDB
 * orm::register_table<User>("users")
 *     .dialect(Dialect::MySQL)
 *     .pk("id",    &User::id)
 *     .col("email", &User::email);
 *
 * // SQLite
 * orm::register_table<User>("users")
 *     .dialect(Dialect::SQLite)
 *     .pk("id",    &User::id)
 *     .col("email", &User::email);
 * ```
 *
 * Dialect에 따라 자동으로:
 * - 플레이스홀더: $N (PG) / ? (MySQL·SQLite)
 * - RETURNING *: PG만 포함
 * - UPSERT: ON CONFLICT … EXCLUDED (PG·SQLite) / ON DUPLICATE KEY UPDATE (MySQL)
 */

#include <qbuem/db/driver.hpp>
#include <qbuem/db/value.hpp>

#include <algorithm>
#include <cassert>
#include <concepts>
#include <format>
#include <functional>
#include <optional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace qbuem_routine::orm {

// ── 타입 별칭 ─────────────────────────────────────────────────────────────────
using Value = qbuem::db::Value;

// ── Dialect — DB 방언 ─────────────────────────────────────────────────────────

enum class Dialect {
    PostgreSQL,  ///< $N 플레이스홀더, RETURNING *, ON CONFLICT … EXCLUDED
    MySQL,       ///< ? 플레이스홀더, RETURNING 없음, ON DUPLICATE KEY UPDATE
    SQLite,      ///< ? 플레이스홀더, RETURNING 없음, ON CONFLICT … EXCLUDED
};

// ── 지원 스칼라 타입 컨셉 ────────────────────────────────────────────────────

template<typename T>
concept OrmInt = std::same_as<T, int64_t> || std::same_as<T, int32_t> ||
                 std::same_as<T, int16_t> || std::same_as<T, uint32_t>;

template<typename T>
concept OrmFloat = std::same_as<T, double> || std::same_as<T, float>;

template<typename T>
concept OrmText = std::same_as<T, std::string>;

template<typename T>
concept OrmBool = std::same_as<T, bool>;

template<typename T>
concept OrmScalar = OrmInt<T> || OrmFloat<T> || OrmText<T> || OrmBool<T>;

// std::optional<OrmScalar> 도 지원
template<typename T>
concept OrmOptional = requires { typename T::value_type; } &&
                      OrmScalar<typename T::value_type> &&
                      std::same_as<T, std::optional<typename T::value_type>>;

template<typename T>
concept OrmField = OrmScalar<T> || OrmOptional<T>;

// ── Value 변환 헬퍼 ──────────────────────────────────────────────────────────

// 스칼라 → Value
template<OrmScalar F>
[[nodiscard]] Value to_value(const F& v) {
    if constexpr (OrmInt<F>)   return Value{static_cast<int64_t>(v)};
    if constexpr (OrmFloat<F>) return Value{static_cast<double>(v)};
    if constexpr (OrmBool<F>)  return Value{v};
    if constexpr (OrmText<F>)  return Value{std::string_view{v}};
}

// optional<Scalar> → Value (null 가능)
template<OrmScalar F>
[[nodiscard]] Value to_value(const std::optional<F>& v) {
    if (!v.has_value()) return qbuem::db::null;
    return to_value(*v);
}

// Value → 스칼라 (타입별 추출)
template<OrmScalar F>
[[nodiscard]] F from_value(const Value& v) {
    if constexpr (OrmInt<F>)   return static_cast<F>(v.get<int64_t>());
    if constexpr (OrmFloat<F>) return static_cast<F>(v.get<double>());
    if constexpr (OrmBool<F>)  return v.get<bool>();
    if constexpr (OrmText<F>)  return std::string{v.get<std::string_view>()};
}

// Value → optional<Scalar>
template<OrmScalar F>
[[nodiscard]] std::optional<F> from_value_opt(const Value& v) {
    if (v.type() == Value::Type::Null) return std::nullopt;
    return from_value<F>(v);
}

// ── FieldDef — 필드 메타데이터 ────────────────────────────────────────────────

template<typename T>
struct FieldDef {
    std::string  col;    ///< 컬럼명
    bool         is_pk;  ///< 기본 키 여부

    std::function<Value(const T&)>          to_val;   ///< 구조체 → Value
    std::function<void(T&, const Value&)>   from_val; ///< Value → 구조체 필드
};

// ── SortOrder ─────────────────────────────────────────────────────────────────

enum class SortOrder { Asc, Desc };

// ── TableMeta<T> ─────────────────────────────────────────────────────────────

template<typename T>
class TableMeta {
public:
    explicit TableMeta(std::string table) : table_(std::move(table)) {}

    // ── 빌더 API ─────────────────────────────────────────────────────────────

    /// DB 방언 설정 (기본값: PostgreSQL)
    TableMeta& dialect(Dialect d) { dialect_ = d; return *this; }

    template<OrmField F>
    TableMeta& pk(std::string col, F T::* ptr) {
        pk_ = col;
        fields_.push_back(make_field<F>(std::move(col), ptr, true));
        return *this;
    }

    template<OrmField F>
    TableMeta& col(std::string col_name, F T::* ptr) {
        fields_.push_back(make_field<F>(std::move(col_name), ptr, false));
        ++non_pk_count_;
        return *this;
    }

    // ── 메타 접근자 ──────────────────────────────────────────────────────────

    [[nodiscard]] std::string_view table_name()  const noexcept { return table_; }
    [[nodiscard]] std::string_view pk_col()      const noexcept { return pk_; }
    [[nodiscard]] std::size_t      field_count() const noexcept { return fields_.size(); }
    [[nodiscard]] Dialect          get_dialect() const noexcept { return dialect_; }

    // ── SQL 생성 — 기본 SELECT ────────────────────────────────────────────────

    /// SELECT col1, col2, ... FROM table
    [[nodiscard]] std::string sql_select_all() const {
        return std::format("SELECT {} FROM {}", col_list_all(), table_);
    }

    /// SELECT ... FROM table WHERE col = $p / ?
    [[nodiscard]] std::string sql_select_where(std::string_view col, int p = 1) const {
        return std::format("{} WHERE {} = {}", sql_select_all(), col, ph(p));
    }

    /// SELECT ... FROM table WHERE col = $p ORDER BY order_col [ASC|DESC]
    [[nodiscard]] std::string sql_select_ordered(std::string_view col,
                                                  std::string_view order_col,
                                                  int p = 1,
                                                  SortOrder order = SortOrder::Asc) const {
        return std::format("{} ORDER BY {}{}", sql_select_where(col, p), order_col, sort_dir(order));
    }

    /// SELECT ... FROM table ORDER BY order_col [ASC|DESC]
    [[nodiscard]] std::string sql_select_all_ordered(std::string_view order_col,
                                                      SortOrder order = SortOrder::Asc) const {
        return std::format("{} ORDER BY {}{}", sql_select_all(), order_col, sort_dir(order));
    }

    /// SELECT ... FROM table WHERE raw_where (직접 작성)
    [[nodiscard]] std::string sql_select_raw_where(std::string_view raw_where) const {
        return std::format("{} WHERE {}", sql_select_all(), raw_where);
    }

    /// SELECT ... FROM table WHERE col IN ($1, $2, ...) — n개 값
    [[nodiscard]] std::string sql_select_where_in(std::string_view col,
                                                   std::size_t n,
                                                   int start_p = 1) const {
        return std::format("{} WHERE {} IN ({})", sql_select_all(), col, in_placeholders(n, start_p));
    }

    /// SELECT ... FROM table WHERE col IS NULL
    [[nodiscard]] std::string sql_select_where_null(std::string_view col) const {
        return std::format("{} WHERE {} IS NULL", sql_select_all(), col);
    }

    /// SELECT ... FROM table WHERE col IS NOT NULL
    [[nodiscard]] std::string sql_select_where_not_null(std::string_view col) const {
        return std::format("{} WHERE {} IS NOT NULL", sql_select_all(), col);
    }

    /// SELECT ... FROM table WHERE col BETWEEN $1 AND $2
    [[nodiscard]] std::string sql_select_where_between(std::string_view col,
                                                        int p1 = 1, int p2 = 2) const {
        return std::format("{} WHERE {} BETWEEN {} AND {}",
                           sql_select_all(), col, ph(p1), ph(p2));
    }

    /// SELECT ... FROM table WHERE col LIKE $1
    [[nodiscard]] std::string sql_select_where_like(std::string_view col, int p = 1) const {
        return std::format("{} WHERE {} LIKE {}", sql_select_all(), col, ph(p));
    }

    // ── SQL 생성 — 페이지네이션 ───────────────────────────────────────────────

    /// SELECT ... FROM table LIMIT $limit_p OFFSET $offset_p
    [[nodiscard]] std::string sql_select_paged(int limit_p = 1, int offset_p = 2) const {
        return std::format("{} LIMIT {} OFFSET {}", sql_select_all(), ph(limit_p), ph(offset_p));
    }

    /// SELECT ... FROM table WHERE col=$p LIMIT $lp OFFSET $op
    [[nodiscard]] std::string sql_select_where_paged(std::string_view col,
                                                      int where_p  = 1,
                                                      int limit_p  = 2,
                                                      int offset_p = 3) const {
        return std::format("{} LIMIT {} OFFSET {}",
                           sql_select_where(col, where_p), ph(limit_p), ph(offset_p));
    }

    /// SELECT ... FROM table ORDER BY order_col LIMIT $l OFFSET $o
    [[nodiscard]] std::string sql_select_all_ordered_paged(std::string_view order_col,
                                                            SortOrder order = SortOrder::Asc,
                                                            int limit_p  = 1,
                                                            int offset_p = 2) const {
        return std::format("{} LIMIT {} OFFSET {}",
                           sql_select_all_ordered(order_col, order), ph(limit_p), ph(offset_p));
    }

    // ── SQL 생성 — COUNT ─────────────────────────────────────────────────────

    /// SELECT COUNT(*) FROM table
    [[nodiscard]] std::string sql_count() const {
        return std::format("SELECT COUNT(*) FROM {}", table_);
    }

    /// SELECT COUNT(*) FROM table WHERE col = $p
    [[nodiscard]] std::string sql_count_where(std::string_view col, int p = 1) const {
        return std::format("SELECT COUNT(*) FROM {} WHERE {} = {}", table_, col, ph(p));
    }

    /// SELECT COUNT(*) FROM table WHERE raw_where
    [[nodiscard]] std::string sql_count_raw_where(std::string_view raw_where) const {
        return std::format("SELECT COUNT(*) FROM {} WHERE {}", table_, raw_where);
    }

    /// SELECT COUNT(*) FROM table WHERE col IN ($1, ..., $n)
    [[nodiscard]] std::string sql_count_where_in(std::string_view col,
                                                  std::size_t n,
                                                  int start_p = 1) const {
        return std::format("SELECT COUNT(*) FROM {} WHERE {} IN ({})",
                           table_, col, in_placeholders(n, start_p));
    }

    // ── SQL 생성 — 멀티 컬럼 WHERE ───────────────────────────────────────────

    /// SELECT ... WHERE c1=$1 AND c2=$2 AND ...  (가변 컬럼)
    [[nodiscard]] std::string sql_select_where_multi(std::span<const std::string_view> cols,
                                                      int start_p = 1) const {
        std::string where;
        int p = start_p;
        for (auto c : cols) {
            if (!where.empty()) where += " AND ";
            where += std::format("{} = {}", c, ph(p++));
        }
        return std::format("{} WHERE {}", sql_select_all(), where);
    }

    /// SELECT ... WHERE col1=$1 AND col2=$2
    [[nodiscard]] std::string sql_select_where2(std::string_view col1,
                                                 std::string_view col2,
                                                 int p1 = 1, int p2 = 2) const {
        return std::format("{} WHERE {} = {} AND {} = {}",
                           sql_select_all(), col1, ph(p1), col2, ph(p2));
    }

    /// SELECT ... WHERE col1=$1 AND col2=$2 AND col3=$3
    [[nodiscard]] std::string sql_select_where3(std::string_view col1,
                                                 std::string_view col2,
                                                 std::string_view col3,
                                                 int p1=1, int p2=2, int p3=3) const {
        return std::format("{} WHERE {} = {} AND {} = {} AND {} = {}",
                           sql_select_all(), col1, ph(p1), col2, ph(p2), col3, ph(p3));
    }

    // ── SQL 생성 — INSERT / UPSERT ────────────────────────────────────────────

    /// INSERT INTO table(col1, col2) VALUES ($1, $2) [RETURNING *]
    /// RETURNING은 PostgreSQL에서만 포함됨.
    [[nodiscard]] std::string sql_insert(bool returning = true) const {
        assert(non_pk_count_ > 0 && "INSERT requires at least one non-PK column");
        auto sql = std::format("INSERT INTO {}({}) VALUES ({})",
                               table_, col_list_no_pk(), placeholders_no_pk());
        if (returning && supports_returning()) sql += " RETURNING *";
        return sql;
    }

    /// INSERT ... VALUES ($1,$2),($3,$4),... [RETURNING *]
    /// @param n  삽입할 행 수 (1 이상)
    [[nodiscard]] std::string sql_insert_batch(std::size_t n, bool returning = true) const {
        assert(n > 0 && "batch size must be > 0");
        assert(non_pk_count_ > 0 && "INSERT requires at least one non-PK column");
        const auto cols    = col_list_no_pk();
        const auto nfields = non_pk_count_;
        std::string values_clause;
        for (std::size_t row = 0; row < n; ++row) {
            if (row > 0) values_clause += ", ";
            values_clause += '(';
            for (std::size_t f = 0; f < nfields; ++f) {
                if (f > 0) values_clause += ", ";
                values_clause += ph(static_cast<int>(row * nfields + f + 1));
            }
            values_clause += ')';
        }
        auto sql = std::format("INSERT INTO {}({}) VALUES {}", table_, cols, values_clause);
        if (returning && supports_returning()) sql += " RETURNING *";
        return sql;
    }

    /// UPSERT (PK 충돌 시 UPDATE)
    /// - PostgreSQL / SQLite: ON CONFLICT (pk) DO UPDATE SET col=EXCLUDED.col, ...
    /// - MySQL: ON DUPLICATE KEY UPDATE col=VALUES(col), ...
    [[nodiscard]] std::string sql_upsert_pk(bool returning = true) const {
        assert(non_pk_count_ > 0 && "UPSERT requires at least one non-PK column");
        const auto cols = col_list_no_pk();
        const auto phs  = placeholders_no_pk();

        std::string sql;
        if (dialect_ == Dialect::MySQL) {
            // MySQL: ON DUPLICATE KEY UPDATE col = VALUES(col)
            std::string update_clause;
            for (const auto& f : fields_) {
                if (f.is_pk) continue;
                if (!update_clause.empty()) update_clause += ", ";
                update_clause += std::format("{} = VALUES({})", f.col, f.col);
            }
            sql = std::format("INSERT INTO {}({}) VALUES ({}) ON DUPLICATE KEY UPDATE {}",
                              table_, cols, phs, update_clause);
            // MySQL: RETURNING 미지원
        } else {
            // PostgreSQL / SQLite: ON CONFLICT (pk) DO UPDATE SET col=EXCLUDED.col
            std::string update_clause;
            for (const auto& f : fields_) {
                if (f.is_pk) continue;
                if (!update_clause.empty()) update_clause += ", ";
                update_clause += std::format("{} = EXCLUDED.{}", f.col, f.col);
            }
            sql = std::format(
                "INSERT INTO {}({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
                table_, cols, phs, pk_, update_clause);
            if (returning && supports_returning()) sql += " RETURNING *";
        }
        return sql;
    }

    // ── SQL 생성 — UPDATE ─────────────────────────────────────────────────────

    /// UPDATE table SET col1=$1, col2=$2 WHERE pk=$N [RETURNING *]
    [[nodiscard]] std::string sql_update_pk(bool returning = true) const {
        int next_p = 1;
        std::string set_clause;
        for (const auto& f : fields_) {
            if (f.is_pk) continue;
            if (!set_clause.empty()) set_clause += ", ";
            set_clause += std::format("{} = {}", f.col, ph(next_p++));
        }
        auto sql = std::format("UPDATE {} SET {} WHERE {} = {}",
                               table_, set_clause, pk_, ph(next_p));
        if (returning && supports_returning()) sql += " RETURNING *";
        return sql;
    }

    /// UPDATE table SET col=$1 WHERE pk=$2 [RETURNING *]  (단일 컬럼)
    [[nodiscard]] std::string sql_update_col_pk(std::string_view col,
                                                 bool returning = true) const {
        auto sql = std::format("UPDATE {} SET {} = {} WHERE {} = {}",
                               table_, col, ph(1), pk_, ph(2));
        if (returning && supports_returning()) sql += " RETURNING *";
        return sql;
    }

    // ── SQL 생성 — DELETE ─────────────────────────────────────────────────────

    /// DELETE FROM table WHERE pk = $1
    [[nodiscard]] std::string sql_delete_pk() const {
        return std::format("DELETE FROM {} WHERE {} = {}", table_, pk_, ph(1));
    }

    /// DELETE FROM table WHERE col = $1
    [[nodiscard]] std::string sql_delete_where(std::string_view col) const {
        return std::format("DELETE FROM {} WHERE {} = {}", table_, col, ph(1));
    }

    /// DELETE FROM table WHERE col1=$1 AND col2=$2
    [[nodiscard]] std::string sql_delete_where2(std::string_view col1,
                                                  std::string_view col2) const {
        return std::format("DELETE FROM {} WHERE {} = {} AND {} = {}",
                           table_, col1, ph(1), col2, ph(2));
    }

    // ── 파라미터 바인딩 ───────────────────────────────────────────────────────

    /// INSERT용: PK 제외, 등록 순서대로
    [[nodiscard]] std::vector<Value> bind_insert(const T& v) const {
        std::vector<Value> params;
        params.reserve(non_pk_count_);
        for (const auto& f : fields_) {
            if (!f.is_pk) params.push_back(f.to_val(v));
        }
        return params;
    }

    /// UPDATE용: non-PK 필드 먼저, PK를 마지막에 추가
    [[nodiscard]] std::vector<Value> bind_update(const T& v) const {
        std::vector<Value> params;
        params.reserve(fields_.size());
        for (const auto& f : fields_) {
            if (!f.is_pk) params.push_back(f.to_val(v));
        }
        for (const auto& f : fields_) {
            if (f.is_pk) params.push_back(f.to_val(v));
        }
        return params;
    }

    /// 배치 INSERT용: 여러 객체를 flat하게 바인딩 (non-PK만, 행 단위 순서)
    [[nodiscard]] std::vector<Value> bind_batch(std::span<const T> objects) const {
        assert(!objects.empty() && "bind_batch: objects must not be empty");
        std::vector<Value> params;
        params.reserve(objects.size() * non_pk_count_);
        for (const auto& obj : objects) {
            for (const auto& f : fields_) {
                if (!f.is_pk) params.push_back(f.to_val(obj));
            }
        }
        return params;
    }

    /// PK만 바인딩
    [[nodiscard]] std::vector<Value> bind_pk(const T& v) const {
        for (const auto& f : fields_) {
            if (f.is_pk) return {f.to_val(v)};
        }
        return {};
    }

    /// 단일 값 바인딩 (WHERE 조건용)
    template<OrmScalar F>
    [[nodiscard]] std::vector<Value> bind_val(const F& v) const {
        return {to_value(v)};
    }

    /// 2개 값 바인딩
    template<OrmScalar F1, OrmScalar F2>
    [[nodiscard]] std::vector<Value> bind_val2(const F1& v1, const F2& v2) const {
        return {to_value(v1), to_value(v2)};
    }

    /// 3개 값 바인딩
    template<OrmScalar F1, OrmScalar F2, OrmScalar F3>
    [[nodiscard]] std::vector<Value> bind_val3(const F1& v1, const F2& v2, const F3& v3) const {
        return {to_value(v1), to_value(v2), to_value(v3)};
    }

    /// WHERE + LIMIT + OFFSET 바인딩 (페이지네이션)
    template<OrmScalar F>
    [[nodiscard]] std::vector<Value> bind_paged(const F& where_val,
                                                 int64_t limit,
                                                 int64_t offset) const {
        return {to_value(where_val), Value{limit}, Value{offset}};
    }

    /// IN 절용 바인딩: 임의 범위 컨테이너 (vector, span, initializer_list 등)
    template<std::ranges::input_range R>
        requires OrmScalar<std::ranges::range_value_t<R>>
    [[nodiscard]] std::vector<Value> bind_in(R&& values) const {
        std::vector<Value> params;
        if constexpr (std::ranges::sized_range<R>)
            params.reserve(std::ranges::size(values));
        for (const auto& v : values)
            params.push_back(to_value(v));
        return params;
    }

    // ── 결과 행 읽기 ─────────────────────────────────────────────────────────

    /// IRow → T: 컬럼명 기반 매핑 (INSERT RETURNING 등에서도 안전)
    [[nodiscard]] T read_row(const qbuem::db::IRow& row) const {
        T obj{};
        for (const auto& f : fields_) {
            auto val = row.get(std::string_view{f.col});
            if (val.type() != Value::Type::Null)
                f.from_val(obj, val);
        }
        return obj;
    }

    /// IRow → T: 인덱스 기반 매핑 (등록 순서 == SELECT 컬럼 순서일 때, 더 빠름)
    [[nodiscard]] T read_row_indexed(const qbuem::db::IRow& row) const {
        T obj{};
        for (std::size_t i = 0; i < fields_.size(); ++i) {
            auto val = row.get(static_cast<uint16_t>(i));
            if (val.type() != Value::Type::Null)
                fields_[i].from_val(obj, val);
        }
        return obj;
    }

private:
    std::string              table_;
    std::string              pk_;
    std::vector<FieldDef<T>> fields_;
    std::size_t              non_pk_count_{0};
    Dialect                  dialect_{Dialect::PostgreSQL};

    // ── 방언 헬퍼 ────────────────────────────────────────────────────────────

    /// 플레이스홀더 생성: PostgreSQL → $N, MySQL/SQLite → ?
    [[nodiscard]] std::string ph(int n) const {
        if (dialect_ == Dialect::PostgreSQL) return std::format("${}", n);
        return "?";
    }

    /// RETURNING * 지원 여부 (PostgreSQL만)
    [[nodiscard]] bool supports_returning() const noexcept {
        return dialect_ == Dialect::PostgreSQL;
    }

    [[nodiscard]] static std::string_view sort_dir(SortOrder order) noexcept {
        return (order == SortOrder::Desc) ? " DESC" : " ASC";
    }

    // ── 내부 SQL 조각 생성 ───────────────────────────────────────────────────

    [[nodiscard]] std::string in_placeholders(std::size_t n, int start_p) const {
        std::string r;
        for (std::size_t i = 0; i < n; ++i) {
            if (i > 0) r += ", ";
            r += ph(start_p + static_cast<int>(i));
        }
        return r;
    }

    [[nodiscard]] std::string col_list_all() const {
        std::string r;
        for (const auto& f : fields_) {
            if (!r.empty()) r += ", ";
            r += f.col;
        }
        return r;
    }

    [[nodiscard]] std::string col_list_no_pk() const {
        std::string r;
        for (const auto& f : fields_) {
            if (f.is_pk) continue;
            if (!r.empty()) r += ", ";
            r += f.col;
        }
        return r;
    }

    [[nodiscard]] std::string placeholders_no_pk() const {
        std::string r;
        int p = 1;
        for (const auto& f : fields_) {
            if (f.is_pk) continue;
            if (!r.empty()) r += ", ";
            r += ph(p++);
        }
        return r;
    }

    // ── 필드 팩토리 ──────────────────────────────────────────────────────────

    template<OrmScalar F>
    static FieldDef<T> make_field(std::string col_name, F T::* ptr, bool is_pk) {
        return FieldDef<T>{
            .col      = std::move(col_name),
            .is_pk    = is_pk,
            .to_val   = [ptr](const T& v) -> Value { return to_value(v.*ptr); },
            .from_val = [ptr](T& v, const Value& val) { v.*ptr = from_value<F>(val); },
        };
    }

    template<OrmScalar F>
    static FieldDef<T> make_field(std::string col_name,
                                   std::optional<F> T::* ptr, bool is_pk) {
        return FieldDef<T>{
            .col      = std::move(col_name),
            .is_pk    = is_pk,
            .to_val   = [ptr](const T& v) -> Value { return to_value(v.*ptr); },
            .from_val = [ptr](T& v, const Value& val) { v.*ptr = from_value_opt<F>(val); },
        };
    }
};

// ── 전역 레지스트리 ─────────────────────────────────────────────────────────

template<typename T>
inline std::optional<TableMeta<T>> g_meta;

/// 타입 T에 대한 테이블 메타를 등록하고 빌더를 반환
template<typename T>
[[nodiscard]] TableMeta<T>& register_table(std::string table_name) {
    g_meta<T>.emplace(std::move(table_name));
    return *g_meta<T>;
}

/// 등록된 메타 접근 (미등록 시 runtime_error)
template<typename T>
[[nodiscard]] const TableMeta<T>& meta() {
    if (!g_meta<T>)
        throw std::runtime_error(
            std::format("orm: type '{}' not registered", typeid(T).name()));
    return *g_meta<T>;
}

/// 등록 여부 확인
template<typename T>
[[nodiscard]] bool is_registered() noexcept {
    return g_meta<T>.has_value();
}

} // namespace qbuem_routine::orm
