// Unit tests for the shared $N→? placeholder converter (sql_placeholders.hpp).
// Standalone — no qbuem-stack needed.
//   g++ -std=c++23 -I../src test_placeholders.cpp -o t && ./t

#include "qbuem/db/sql_placeholders.hpp"

#include <cstdio>
#include <string>

using qbuem_routine::db_detail::convert_placeholders;

static int g_fail = 0, g_total = 0;
#define CHECK_EQ(a, b)                                                         \
    do {                                                                      \
        ++g_total;                                                            \
        std::string _a = (a); std::string _b = (b);                          \
        if (_a != _b) { ++g_fail;                                            \
            std::printf("FAIL %s:%d\n   got: %s\n   exp: %s\n",               \
                        __FILE__, __LINE__, _a.c_str(), _b.c_str()); }        \
    } while (0)

int main() {
    // Basic conversion outside literals.
    CHECK_EQ(convert_placeholders("SELECT * FROM t WHERE id = $1"),
             "SELECT * FROM t WHERE id = ?");
    CHECK_EQ(convert_placeholders("a=$1 AND b=$2 AND c=$10"),
             "a=? AND b=? AND c=?");
    CHECK_EQ(convert_placeholders("VALUES ($1, $2, $3)"),
             "VALUES (?, ?, ?)");

    // $N inside a string literal must be preserved (was corrupted before).
    CHECK_EQ(convert_placeholders("WHERE note = '$1 discount' AND id = $2"),
             "WHERE note = '$1 discount' AND id = ?");
    CHECK_EQ(convert_placeholders("SELECT '$1$2$3'"), "SELECT '$1$2$3'");

    // '' escaped quote inside a literal, with a real placeholder after.
    CHECK_EQ(convert_placeholders("WHERE x = 'it''s $5' AND y = $1"),
             "WHERE x = 'it''s $5' AND y = ?");

    // '$' not followed by a digit is left alone ($$ dollar-quote, $foo).
    CHECK_EQ(convert_placeholders("a $$ b"), "a $$ b");
    CHECK_EQ(convert_placeholders("$foo"), "$foo");

    // High-bit (UTF-8) byte after '$' must NOT trigger UB or conversion.
    std::string utf8 = "$"; utf8 += static_cast<char>(0xED); utf8 += "x";
    CHECK_EQ(convert_placeholders(utf8), utf8);

    // No placeholders → identity.
    CHECK_EQ(convert_placeholders("SELECT 1"), "SELECT 1");
    CHECK_EQ(convert_placeholders(""), "");

    // is_safe_ident — used to reject savepoint-name injection.
    using qbuem_routine::db_detail::is_safe_ident;
    auto ok  = [](bool b) { ++g_total; if (!b) { ++g_fail; std::printf("FAIL ident ok\n"); } };
    auto bad = [](bool b) { ++g_total; if (b)  { ++g_fail; std::printf("FAIL ident bad\n"); } };
    ok(is_safe_ident("sp1"));
    ok(is_safe_ident("_my_savepoint"));
    ok(is_safe_ident("a9"));
    bad(is_safe_ident(""));
    bad(is_safe_ident("9abc"));           // starts with digit
    bad(is_safe_ident("a b"));            // space
    bad(is_safe_ident("a\"; DROP--"));    // injection
    bad(is_safe_ident("a'b"));            // quote
    bad(is_safe_ident("a.b"));            // dotted (savepoints are single idents)

    std::printf("\n%d/%d checks passed\n", g_total - g_fail, g_total);
    return g_fail ? 1 : 0;
}
