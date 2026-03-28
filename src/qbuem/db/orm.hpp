#pragma once

/**
 * @file db/orm.hpp
 * @brief 제너릭 ORM 유틸리티 — qbuem::db::IRow 기반 (드라이버 독립)
 *
 * ## 성능 설계
 * - col_list, placeholder, upsert 구문은 빌더 단계에서 1회 계산, 이후 O(1) 접근
 * - 자주 쓰는 SQL (select_all, insert, update_pk, delete_pk, upsert_pk) 도 캐싱
 * - sql_insert_batch() / in_placeholders() 은 사전 할당 + 직접 문자 삽입으로 최적화
 * - ph(n)은 $1..$64 구간 정적 배열, 이외 fallback to to_string
 *
 * ## 드라이버별 Dialect 설정
 * ```cpp
 * orm::register_table<User>("users")
 *     .dialect(Dialect::MySQL)   // 기본값: PostgreSQL
 *     .pk("id",    &User::id)
 *     .col("email", &User::email);
 * ```
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

template<typename T>
concept OrmOptional = requires { typename T::value_type; } &&
                      OrmScalar<typename T::value_type> &&
                      std::same_as<T, std::optional<typename T::value_type>>;

template<typename T>
concept OrmField = OrmScalar<T> || OrmOptional<T>;

// ── Value 변환 헬퍼 ──────────────────────────────────────────────────────────

template<OrmScalar F>
[[nodiscard]] Value to_value(const F& v) {
    if constexpr (OrmInt<F>)   return Value{static_cast<int64_t>(v)};
    if constexpr (OrmFloat<F>) return Value{static_cast<double>(v)};
    if constexpr (OrmBool<F>)  return Value{v};
    if constexpr (OrmText<F>)  return Value{std::string_view{v}};
}

template<OrmScalar F>
[[nodiscard]] Value to_value(const std::optional<F>& v) {
    if (!v.has_value()) return qbuem::db::null;
    return to_value(*v);
}

template<OrmScalar F>
[[nodiscard]] F from_value(const Value& v) {
    if constexpr (OrmInt<F>)   return static_cast<F>(v.get<int64_t>());
    if constexpr (OrmFloat<F>) return static_cast<F>(v.get<double>());
    if constexpr (OrmBool<F>)  return v.get<bool>();
    if constexpr (OrmText<F>)  return std::string{v.get<std::string_view>()};
}

template<OrmScalar F>
[[nodiscard]] std::optional<F> from_value_opt(const Value& v) {
    if (v.type() == Value::Type::Null) return std::nullopt;
    return from_value<F>(v);
}

// ── FieldDef — 필드 메타데이터 ────────────────────────────────────────────────

template<typename T>
struct FieldDef {
    std::string  col;
    bool         is_pk       = false;
    /// True for server-generated columns (created_at, updated_at).
    /// Included in SELECT col list and read back via read_row(), but
    /// excluded from INSERT / UPDATE / batch binding so the DB uses DEFAULT.
    bool         is_readonly = false;

    std::function<Value(const T&)>        to_val;
    std::function<void(T&, const Value&)> from_val;
};

// ── SortOrder ─────────────────────────────────────────────────────────────────

enum class SortOrder { Asc, Desc };

// ── TableMeta<T> ─────────────────────────────────────────────────────────────

template<typename T>
class TableMeta {
public:
    explicit TableMeta(std::string table) : table_(std::move(table)) {}

    // ── 빌더 API ─────────────────────────────────────────────────────────────

    /// DB 방언 설정 (기본값: PostgreSQL). dialect() 호출 후 placeholder 캐시 재생성.
    TableMeta& dialect(Dialect d) {
        dialect_ = d;
        rebuild_dialect_caches();
        return *this;
    }

    template<OrmField F>
    TableMeta& pk(std::string col, F T::* ptr) {
        pk_ = col;
        // col_list_all에 PK도 포함
        if (!cache_.col_list_all.empty()) cache_.col_list_all += ", ";
        cache_.col_list_all += col;
        fields_.push_back(make_field<F>(std::move(col), ptr, true));
        rebuild_stable_sqls();
        return *this;
    }

    template<OrmField F>
    TableMeta& col(std::string col_name, F T::* ptr) {
        // col_list_all
        if (!cache_.col_list_all.empty()) cache_.col_list_all += ", ";
        cache_.col_list_all += col_name;

        // col_list_no_pk
        if (!cache_.col_list_no_pk.empty()) cache_.col_list_no_pk += ", ";
        cache_.col_list_no_pk += col_name;

        // placeholders_no_pk (dialect-dependent)
        const int p = static_cast<int>(non_pk_count_) + 1;
        if (!cache_.placeholders_no_pk.empty()) cache_.placeholders_no_pk += ", ";
        cache_.placeholders_no_pk += ph_sv(p);

        // upsert update clauses (dialect-independent text parts, dialect handled in sql gen)
        if (!cache_.upsert_excluded.empty()) cache_.upsert_excluded += ", ";
        cache_.upsert_excluded += col_name;
        cache_.upsert_excluded += " = EXCLUDED.";
        cache_.upsert_excluded += col_name;

        if (!cache_.upsert_values.empty()) cache_.upsert_values += ", ";
        cache_.upsert_values += col_name;
        cache_.upsert_values += " = VALUES(";
        cache_.upsert_values += col_name;
        cache_.upsert_values += ")";

        fields_.push_back(make_field<F>(std::move(col_name), ptr, false));
        ++non_pk_count_;
        rebuild_stable_sqls();
        return *this;
    }

    /// Register a server-default (read-only) column.
    /// Included in SELECT col list so read_row() can populate it from RETURNING *.
    /// Excluded from INSERT / UPDATE / batch SQL and bindings — the DB uses DEFAULT.
    /// Typical use: created_at, updated_at (PostgreSQL DEFAULT NOW()).
    template<OrmField F>
    TableMeta& col_server_default(std::string col_name, F T::* ptr) {
        // Add to col_list_all so SELECT * reads it back
        if (!cache_.col_list_all.empty()) cache_.col_list_all += ", ";
        cache_.col_list_all += col_name;

        // NOT added to col_list_no_pk / placeholders_no_pk / upsert caches
        // NOT counted in non_pk_count_

        auto fd = make_field<F>(std::move(col_name), ptr, false);
        fd.is_readonly = true;
        fields_.push_back(std::move(fd));

        rebuild_stable_sqls();
        return *this;
    }

    // ── 메타 접근자 ──────────────────────────────────────────────────────────

    [[nodiscard]] std::string_view table_name()  const noexcept { return table_; }
    [[nodiscard]] std::string_view pk_col()      const noexcept { return pk_; }
    [[nodiscard]] std::size_t      field_count() const noexcept { return fields_.size(); }
    [[nodiscard]] Dialect          get_dialect() const noexcept { return dialect_; }

    // ── SQL 생성 — 기본 SELECT ────────────────────────────────────────────────

    /// SELECT col1, col2, ... FROM table  [캐싱됨]
    [[nodiscard]] const std::string& sql_select_all() const noexcept {
        return cache_.sql_select_all;
    }

    /// SELECT ... FROM table WHERE col = $p
    [[nodiscard]] std::string sql_select_where(std::string_view col, int p = 1) const {
        return cache_.sql_select_all + " WHERE " + std::string(col) + " = " + ph_sv(p);
    }

    /// SELECT ... FROM table WHERE col = $p ORDER BY order_col [ASC|DESC]
    [[nodiscard]] std::string sql_select_ordered(std::string_view col,
                                                  std::string_view order_col,
                                                  int p = 1,
                                                  SortOrder order = SortOrder::Asc) const {
        return sql_select_where(col, p) + " ORDER BY " + std::string(order_col) + std::string{sort_dir(order)};
    }

    /// SELECT ... FROM table ORDER BY order_col [ASC|DESC]
    [[nodiscard]] std::string sql_select_all_ordered(std::string_view order_col,
                                                      SortOrder order = SortOrder::Asc) const {
        return cache_.sql_select_all + " ORDER BY " + std::string(order_col) + std::string{sort_dir(order)};
    }

    /// SELECT ... FROM table WHERE raw_where
    [[nodiscard]] std::string sql_select_raw_where(std::string_view raw_where) const {
        return cache_.sql_select_all + " WHERE " + std::string(raw_where);
    }

    /// SELECT ... FROM table WHERE col IN ($1, $2, ..., $n)
    [[nodiscard]] std::string sql_select_where_in(std::string_view col,
                                                   std::size_t n,
                                                   int start_p = 1) const {
        return cache_.sql_select_all + " WHERE " + std::string(col) +
               " IN (" + in_placeholders(n, start_p) + ")";
    }

    /// SELECT ... FROM table WHERE col IS NULL
    [[nodiscard]] std::string sql_select_where_null(std::string_view col) const {
        return cache_.sql_select_all + " WHERE " + std::string(col) + " IS NULL";
    }

    /// SELECT ... FROM table WHERE col IS NOT NULL
    [[nodiscard]] std::string sql_select_where_not_null(std::string_view col) const {
        return cache_.sql_select_all + " WHERE " + std::string(col) + " IS NOT NULL";
    }

    /// SELECT ... FROM table WHERE col BETWEEN $1 AND $2
    [[nodiscard]] std::string sql_select_where_between(std::string_view col,
                                                        int p1 = 1, int p2 = 2) const {
        return cache_.sql_select_all + " WHERE " + std::string(col) +
               " BETWEEN " + ph_sv(p1) + " AND " + ph_sv(p2);
    }

    /// SELECT ... FROM table WHERE col LIKE $1
    [[nodiscard]] std::string sql_select_where_like(std::string_view col, int p = 1) const {
        return cache_.sql_select_all + " WHERE " + std::string(col) + " LIKE " + ph_sv(p);
    }

    // ── SQL 생성 — 페이지네이션 ───────────────────────────────────────────────

    /// SELECT ... FROM table LIMIT $lp OFFSET $op
    [[nodiscard]] std::string sql_select_paged(int limit_p = 1, int offset_p = 2) const {
        return cache_.sql_select_all + " LIMIT " + ph_sv(limit_p) + " OFFSET " + ph_sv(offset_p);
    }

    /// SELECT ... FROM table WHERE col=$wp LIMIT $lp OFFSET $op
    [[nodiscard]] std::string sql_select_where_paged(std::string_view col,
                                                      int where_p  = 1,
                                                      int limit_p  = 2,
                                                      int offset_p = 3) const {
        return sql_select_where(col, where_p) +
               " LIMIT " + ph_sv(limit_p) + " OFFSET " + ph_sv(offset_p);
    }

    /// SELECT ... FROM table ORDER BY order_col LIMIT $lp OFFSET $op
    [[nodiscard]] std::string sql_select_all_ordered_paged(std::string_view order_col,
                                                            SortOrder order = SortOrder::Asc,
                                                            int limit_p  = 1,
                                                            int offset_p = 2) const {
        return sql_select_all_ordered(order_col, order) +
               " LIMIT " + ph_sv(limit_p) + " OFFSET " + ph_sv(offset_p);
    }

    // ── SQL 생성 — COUNT ─────────────────────────────────────────────────────

    /// SELECT COUNT(*) FROM table
    [[nodiscard]] std::string sql_count() const {
        return "SELECT COUNT(*) FROM " + table_;
    }

    /// SELECT COUNT(*) FROM table WHERE col = $p
    [[nodiscard]] std::string sql_count_where(std::string_view col, int p = 1) const {
        return "SELECT COUNT(*) FROM " + table_ + " WHERE " + std::string(col) + " = " + ph_sv(p);
    }

    /// SELECT COUNT(*) FROM table WHERE raw_where
    [[nodiscard]] std::string sql_count_raw_where(std::string_view raw_where) const {
        return "SELECT COUNT(*) FROM " + table_ + " WHERE " + std::string(raw_where);
    }

    /// SELECT COUNT(*) FROM table WHERE col IN ($1, ..., $n)
    [[nodiscard]] std::string sql_count_where_in(std::string_view col,
                                                  std::size_t n,
                                                  int start_p = 1) const {
        return "SELECT COUNT(*) FROM " + table_ + " WHERE " + std::string(col) +
               " IN (" + in_placeholders(n, start_p) + ")";
    }

    // ── SQL 생성 — 멀티 컬럼 WHERE ───────────────────────────────────────────

    /// SELECT ... WHERE c1=$1 AND c2=$2 AND ...
    [[nodiscard]] std::string sql_select_where_multi(std::span<const std::string_view> cols,
                                                      int start_p = 1) const {
        std::string where;
        where.reserve(cols.size() * 16);
        int p = start_p;
        for (auto c : cols) {
            if (!where.empty()) where += " AND ";
            where += c;
            where += " = ";
            where += ph_sv(p++);
        }
        return cache_.sql_select_all + " WHERE " + where;
    }

    /// SELECT ... WHERE col1=$1 AND col2=$2
    [[nodiscard]] std::string sql_select_where2(std::string_view col1,
                                                 std::string_view col2,
                                                 int p1 = 1, int p2 = 2) const {
        return cache_.sql_select_all + " WHERE " + std::string(col1) + " = " + ph_sv(p1) +
               " AND " + std::string(col2) + " = " + ph_sv(p2);
    }

    /// SELECT ... WHERE col1=$1 AND col2=$2 AND col3=$3
    [[nodiscard]] std::string sql_select_where3(std::string_view col1,
                                                 std::string_view col2,
                                                 std::string_view col3,
                                                 int p1=1, int p2=2, int p3=3) const {
        return cache_.sql_select_all + " WHERE " +
               std::string(col1) + " = " + ph_sv(p1) + " AND " +
               std::string(col2) + " = " + ph_sv(p2) + " AND " +
               std::string(col3) + " = " + ph_sv(p3);
    }

    // ── SQL 생성 — INSERT / UPSERT ────────────────────────────────────────────

    /// INSERT INTO table(…) VALUES (…) [RETURNING *]  [캐싱됨]
    [[nodiscard]] const std::string& sql_insert() const noexcept {
        return cache_.sql_insert;
    }

    /// INSERT INTO table(…) VALUES (…) [RETURNING *] (returning 명시 오버로드)
    [[nodiscard]] std::string sql_insert(bool returning) const {
        if (returning == supports_returning()) return cache_.sql_insert;
        // Rare: caller explicitly requests opposite of dialect default
        assert(non_pk_count_ > 0 && "INSERT requires at least one non-PK column");
        auto sql = "INSERT INTO " + table_ + "(" + cache_.col_list_no_pk +
                   ") VALUES (" + cache_.placeholders_no_pk + ")";
        if (returning && supports_returning()) sql += " RETURNING *";
        return sql;
    }

    /// INSERT … VALUES (…),(…),…  [RETURNING * on PG]
    [[nodiscard]] std::string sql_insert_batch(std::size_t n, bool returning = true) const {
        assert(n > 0 && "batch size must be > 0");
        assert(non_pk_count_ > 0 && "INSERT requires at least one non-PK column");
        const std::size_t nf = non_pk_count_;

        // Pre-allocate: per row = "($1,$2,…)" ≈ nf*(4+2)+3 chars
        std::string values_clause;
        values_clause.reserve(n * (nf * 5 + 4));

        const bool is_pg = (dialect_ == Dialect::PostgreSQL);
        for (std::size_t row = 0; row < n; ++row) {
            if (row > 0) values_clause += ", ";
            values_clause += '(';
            for (std::size_t f = 0; f < nf; ++f) {
                if (f > 0) values_clause += ", ";
                if (is_pg) {
                    values_clause += ph_sv_int(static_cast<int>(row * nf + f + 1));
                } else {
                    values_clause += '?';
                }
            }
            values_clause += ')';
        }

        auto sql = "INSERT INTO " + table_ + "(" + cache_.col_list_no_pk +
                   ") VALUES " + values_clause;
        if (returning && supports_returning()) sql += " RETURNING *";
        return sql;
    }

    /// UPSERT — 방언별 자동 선택  [캐싱됨]
    [[nodiscard]] const std::string& sql_upsert_pk() const noexcept {
        return cache_.sql_upsert_pk;
    }

    /// UPSERT (returning 명시 오버로드)
    [[nodiscard]] std::string sql_upsert_pk(bool returning) const {
        if (returning == supports_returning()) return cache_.sql_upsert_pk;
        return build_upsert_pk(returning);
    }

    // ── SQL 생성 — UPDATE ─────────────────────────────────────────────────────

    /// UPDATE table SET … WHERE pk=$N [RETURNING *]  [캐싱됨]
    [[nodiscard]] const std::string& sql_update_pk() const noexcept {
        return cache_.sql_update_pk;
    }

    /// UPDATE table SET … WHERE pk=$N [RETURNING *] (returning 명시)
    [[nodiscard]] std::string sql_update_pk(bool returning) const {
        if (returning == supports_returning()) return cache_.sql_update_pk;
        return build_update_pk(returning);
    }

    /// UPDATE table SET col=$1 WHERE pk=$2 [RETURNING *]
    [[nodiscard]] std::string sql_update_col_pk(std::string_view col,
                                                 bool returning = true) const {
        auto sql = "UPDATE " + table_ + " SET " + std::string(col) +
                   " = " + ph_sv(1) + " WHERE " + pk_ + " = " + ph_sv(2);
        if (returning && supports_returning()) sql += " RETURNING *";
        return sql;
    }

    // ── SQL 생성 — DELETE ─────────────────────────────────────────────────────

    /// DELETE FROM table WHERE pk=$1  [캐싱됨]
    [[nodiscard]] const std::string& sql_delete_pk() const noexcept {
        return cache_.sql_delete_pk;
    }

    /// DELETE FROM table WHERE col=$1
    [[nodiscard]] std::string sql_delete_where(std::string_view col) const {
        return "DELETE FROM " + table_ + " WHERE " + std::string(col) + " = " + ph_sv(1);
    }

    /// DELETE FROM table WHERE col1=$1 AND col2=$2
    [[nodiscard]] std::string sql_delete_where2(std::string_view col1,
                                                  std::string_view col2) const {
        return "DELETE FROM " + table_ + " WHERE " + std::string(col1) + " = " + ph_sv(1) +
               " AND " + std::string(col2) + " = " + ph_sv(2);
    }

    // ── 파라미터 바인딩 ───────────────────────────────────────────────────────

    // INSERT: non-PK, non-readonly fields only (registration order)
    [[nodiscard]] std::vector<Value> bind_insert(const T& v) const {
        std::vector<Value> params;
        params.reserve(non_pk_count_);
        for (const auto& f : fields_) {
            if (!f.is_pk && !f.is_readonly) params.push_back(f.to_val(v));
        }
        return params;
    }

    // UPDATE: non-PK, non-readonly fields first; PK last
    [[nodiscard]] std::vector<Value> bind_update(const T& v) const {
        std::vector<Value> params;
        params.reserve(fields_.size());
        for (const auto& f : fields_) {
            if (!f.is_pk && !f.is_readonly) params.push_back(f.to_val(v));
        }
        for (const auto& f : fields_) {
            if (f.is_pk) params.push_back(f.to_val(v));
        }
        return params;
    }

    // Batch INSERT: flat bindings (non-PK, non-readonly, row-major order)
    [[nodiscard]] std::vector<Value> bind_batch(std::span<const T> objects) const {
        assert(!objects.empty() && "bind_batch: objects must not be empty");
        std::vector<Value> params;
        params.reserve(objects.size() * non_pk_count_);
        for (const auto& obj : objects) {
            for (const auto& f : fields_) {
                if (!f.is_pk && !f.is_readonly) params.push_back(f.to_val(obj));
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

    /// 단일 값 바인딩
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

    /// WHERE + LIMIT + OFFSET 바인딩
    template<OrmScalar F>
    [[nodiscard]] std::vector<Value> bind_paged(const F& where_val,
                                                 int64_t limit,
                                                 int64_t offset) const {
        return {to_value(where_val), Value{limit}, Value{offset}};
    }

    /// IN 절용 바인딩: 임의 범위 컨테이너 지원
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

    /// IRow → T: 컬럼명 기반 매핑
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
    // ── 멤버 ─────────────────────────────────────────────────────────────────

    std::string              table_;
    std::string              pk_;
    std::vector<FieldDef<T>> fields_;
    std::size_t              non_pk_count_{0};
    Dialect                  dialect_{Dialect::PostgreSQL};

    // 캐싱된 빌딩 블록 + 완성 SQL (빌더 단계에서 1회 계산)
    struct Cache {
        // 헬퍼 조각들
        std::string col_list_all;
        std::string col_list_no_pk;
        std::string placeholders_no_pk;
        std::string upsert_excluded;   // "col = EXCLUDED.col, …"
        std::string upsert_values;     // "col = VALUES(col), …"
        // 완성 SQL
        std::string sql_select_all;
        std::string sql_insert;
        std::string sql_upsert_pk;
        std::string sql_update_pk;
        std::string sql_delete_pk;
    } cache_;

    // ── 플레이스홀더 헬퍼 ────────────────────────────────────────────────────

    // PostgreSQL 플레이스홀더 정적 배열 ($1..$64 공통 범위)
    static constexpr std::string_view kPgPh[] = {
        "$1","$2","$3","$4","$5","$6","$7","$8","$9","$10",
        "$11","$12","$13","$14","$15","$16","$17","$18","$19","$20",
        "$21","$22","$23","$24","$25","$26","$27","$28","$29","$30",
        "$31","$32","$33","$34","$35","$36","$37","$38","$39","$40",
        "$41","$42","$43","$44","$45","$46","$47","$48","$49","$50",
        "$51","$52","$53","$54","$55","$56","$57","$58","$59","$60",
        "$61","$62","$63","$64",
    };

    /// 플레이스홀더 std::string 반환 (캐시에 append할 때 사용)
    [[nodiscard]] std::string ph_sv(int n) const {
        if (dialect_ == Dialect::PostgreSQL) {
            if (n >= 1 && n <= 64) return std::string{kPgPh[n - 1]};
            return "$" + std::to_string(n);
        }
        return "?";
    }

    /// 플레이스홀더 반환 — sql_insert_batch 내부 루프용 (반환값 string)
    [[nodiscard]] static std::string_view ph_sv_int(int n) noexcept {
        if (n >= 1 && n <= 64) return kPgPh[n - 1];
        return {};  // 호출자가 fallback 처리
    }

    [[nodiscard]] bool supports_returning() const noexcept {
        return dialect_ == Dialect::PostgreSQL;
    }

    [[nodiscard]] static std::string_view sort_dir(SortOrder order) noexcept {
        return (order == SortOrder::Desc) ? " DESC" : " ASC";
    }

    // ── IN 플레이스홀더 빌더 (최적화) ────────────────────────────────────────

    [[nodiscard]] std::string in_placeholders(std::size_t n, int start_p) const {
        if (n == 0) return {};
        if (dialect_ != Dialect::PostgreSQL) {
            // 모두 '?' — "?,?,?" 패턴 직접 구성
            std::string r;
            r.reserve(n * 2 - 1);
            for (std::size_t i = 0; i < n; ++i) {
                if (i > 0) r += ", ";
                r += '?';
            }
            return r;
        }
        // PostgreSQL: "$start_p, $start_p+1, ..."
        std::string r;
        r.reserve(n * 4);
        for (std::size_t i = 0; i < n; ++i) {
            if (i > 0) r += ", ";
            const int p = start_p + static_cast<int>(i);
            if (p >= 1 && p <= 64) {
                r += kPgPh[p - 1];
            } else {
                r += '$';
                r += std::to_string(p);
            }
        }
        return r;
    }

    // ── 캐시 재생성 ──────────────────────────────────────────────────────────

    /// dialect 변경 시 dialect-dependent 캐시만 재생성
    void rebuild_dialect_caches() {
        // placeholders_no_pk 재생성
        cache_.placeholders_no_pk.clear();
        int p = 1;
        for (const auto& f : fields_) {
            if (f.is_pk) continue;
            if (!cache_.placeholders_no_pk.empty()) cache_.placeholders_no_pk += ", ";
            cache_.placeholders_no_pk += ph_sv(p++);
        }
        rebuild_stable_sqls();
    }

    /// 모든 안정적 SQL 재생성 (pk/col 추가 또는 dialect 변경 시 호출)
    void rebuild_stable_sqls() {
        // SELECT *
        if (!table_.empty() && !cache_.col_list_all.empty())
            cache_.sql_select_all = "SELECT " + cache_.col_list_all + " FROM " + table_;

        // INSERT
        if (non_pk_count_ > 0) {
            cache_.sql_insert = "INSERT INTO " + table_ + "(" + cache_.col_list_no_pk +
                                ") VALUES (" + cache_.placeholders_no_pk + ")";
            if (supports_returning()) cache_.sql_insert += " RETURNING *";
        }

        // UPSERT
        if (!pk_.empty() && non_pk_count_ > 0) {
            cache_.sql_upsert_pk = build_upsert_pk(true);
        }

        // UPDATE pk
        if (!pk_.empty() && non_pk_count_ > 0) {
            cache_.sql_update_pk = build_update_pk(true);
        }

        // DELETE pk
        if (!pk_.empty()) {
            cache_.sql_delete_pk = "DELETE FROM " + table_ + " WHERE " + pk_ + " = " + ph_sv(1);
        }
    }

    [[nodiscard]] std::string build_upsert_pk(bool returning) const {
        assert(non_pk_count_ > 0);
        std::string sql;
        if (dialect_ == Dialect::MySQL) {
            sql = "INSERT INTO " + table_ + "(" + cache_.col_list_no_pk +
                  ") VALUES (" + cache_.placeholders_no_pk +
                  ") ON DUPLICATE KEY UPDATE " + cache_.upsert_values;
        } else {
            sql = "INSERT INTO " + table_ + "(" + cache_.col_list_no_pk +
                  ") VALUES (" + cache_.placeholders_no_pk +
                  ") ON CONFLICT (" + pk_ + ") DO UPDATE SET " + cache_.upsert_excluded;
            if (returning && supports_returning()) sql += " RETURNING *";
        }
        return sql;
    }

    [[nodiscard]] std::string build_update_pk(bool returning) const {
        int next_p = 1;
        std::string set_clause;
        set_clause.reserve(cache_.col_list_no_pk.size() * 2);
        for (const auto& f : fields_) {
            if (f.is_pk || f.is_readonly) continue;
            if (!set_clause.empty()) set_clause += ", ";
            set_clause += f.col;
            set_clause += " = ";
            set_clause += ph_sv(next_p++);
        }
        auto sql = "UPDATE " + table_ + " SET " + set_clause +
                   " WHERE " + pk_ + " = " + ph_sv(next_p);
        if (returning && supports_returning()) sql += " RETURNING *";
        return sql;
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

template<typename T>
[[nodiscard]] TableMeta<T>& register_table(std::string table_name) {
    g_meta<T>.emplace(std::move(table_name));
    return *g_meta<T>;
}

template<typename T>
[[nodiscard]] const TableMeta<T>& meta() {
    if (!g_meta<T>)
        throw std::runtime_error(
            std::format("orm: type '{}' not registered", typeid(T).name()));
    return *g_meta<T>;
}

template<typename T>
[[nodiscard]] bool is_registered() noexcept {
    return g_meta<T>.has_value();
}

} // namespace qbuem_routine::orm
