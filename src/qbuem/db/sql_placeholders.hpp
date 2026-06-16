#pragma once

/**
 * @file db/sql_placeholders.hpp
 * @brief Convert PostgreSQL `$N` placeholders to `?` for MySQL / SQLite.
 *
 * Shared by the MySQL and SQLite drivers (previously duplicated, with two bugs):
 *   1. `std::isdigit(char)` is UB when the char is negative (high-bit / UTF-8
 *      bytes) — fixed by casting to unsigned char.
 *   2. `$N` was rewritten even inside SQL string literals, corrupting any query
 *      containing a literal `$1` (e.g. `WHERE note = '$1 discount'`) — fixed by
 *      tracking single-quoted-literal state and skipping conversion inside it.
 */

#include <cctype>
#include <string>
#include <string_view>

namespace qbuem_routine::db_detail {

/// Rewrite `$1`, `$2`, … (outside string literals) to `?`.  Bytes inside a
/// single-quoted literal — including a literal `$N` — are passed through
/// verbatim.  `''` inside a literal (the SQL escaped quote) is handled correctly
/// because conversion only happens while NOT inside a literal.
[[nodiscard]] inline std::string convert_placeholders(std::string_view sql) {
    std::string result;
    result.reserve(sql.size());
    bool in_str = false;
    std::size_t i = 0;
    while (i < sql.size()) {
        const char c = sql[i];
        if (in_str) {
            result += c;
            if (c == '\'') in_str = false; // leave the literal (re-enters on '')
            ++i;
            continue;
        }
        if (c == '\'') {
            in_str = true;
            result += c;
            ++i;
            continue;
        }
        if (c == '$' && i + 1 < sql.size() &&
            std::isdigit(static_cast<unsigned char>(sql[i + 1]))) {
            result += '?';
            ++i;
            while (i < sql.size() &&
                   std::isdigit(static_cast<unsigned char>(sql[i])))
                ++i;
        } else {
            result += c;
            ++i;
        }
    }
    return result;
}

/// True if `s` is a safe plain SQL identifier — `[A-Za-z_][A-Za-z0-9_]*`, length
/// ≤ 128.  Used to reject injection through interpolated names (e.g. savepoint
/// names, which SQL cannot parameterize).
[[nodiscard]] inline bool is_safe_ident(std::string_view s) noexcept {
    if (s.empty() || s.size() > 128) return false;
    const auto is_start = [](char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
    };
    if (!is_start(s.front())) return false;
    for (char c : s.substr(1))
        if (!is_start(c) && !(c >= '0' && c <= '9')) return false;
    return true;
}

} // namespace qbuem_routine::db_detail
