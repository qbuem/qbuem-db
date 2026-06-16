// Unit tests for the ORM SQL generator (orm.hpp) — pure string generation, no DB.
// Focus: generated-SQL correctness + the identifier-injection defense.
//
// Build:
//   g++ -std=c++23 -I<qbuem-stack>/include test_orm.cpp -o t && ./t

#include "qbuem/db/orm.hpp"

#include <cstdio>
#include <stdexcept>
#include <string>

namespace orm = qbuem_routine::orm;

struct User {
    int64_t     id{};
    std::string email{};
    int64_t     age{};
};

static int g_fail = 0, g_total = 0;
#define CHECK(cond)                                                            \
    do {                                                                      \
        ++g_total;                                                            \
        if (!(cond)) { ++g_fail; std::printf("FAIL %s:%d  %s\n", __FILE__, __LINE__, #cond); } \
    } while (0)
#define CHECK_EQ(a, b)                                                        \
    do {                                                                      \
        ++g_total;                                                            \
        auto _a = (a); auto _b = (b);                                         \
        if (!(_a == _b)) { ++g_fail;                                          \
            std::printf("FAIL %s:%d\n   got: %s\n   exp: %s\n",               \
                        __FILE__, __LINE__, std::string(_a).c_str(),          \
                        std::string(_b).c_str()); }                           \
    } while (0)

// Does calling `fn` throw std::invalid_argument?
template <class F> static bool throws_invalid(F&& fn) {
    try { fn(); return false; }
    catch (const std::invalid_argument&) { return true; }
    catch (...) { return false; }
}

static void test_sql_gen() {
    auto& m = orm::register_table<User>("users")
                  .pk("id", &User::id)
                  .col("email", &User::email)
                  .col("age", &User::age);

    CHECK_EQ(m.sql_select_all(), std::string("SELECT id, email, age FROM users"));
    CHECK_EQ(m.sql_select_where("email"),
             std::string("SELECT id, email, age FROM users WHERE email = $1"));
    CHECK_EQ(m.sql_count_where("age"),
             std::string("SELECT COUNT(*) FROM users WHERE age = $1"));
    CHECK_EQ(m.sql_delete_where("email"),
             std::string("DELETE FROM users WHERE email = $1"));
    CHECK_EQ(m.sql_insert(),
             std::string("INSERT INTO users(email, age) VALUES ($1, $2) RETURNING *"));
    CHECK_EQ(m.sql_select_all_ordered("age", orm::SortOrder::Desc),
             std::string("SELECT id, email, age FROM users ORDER BY age DESC"));

    // Schema-qualified + quoted identifiers are accepted.
    CHECK(!throws_invalid([&] { (void)m.sql_select_where("public.users_email"); }));
    CHECK(!throws_invalid([&] { (void)m.sql_select_where("\"email\""); }));
}

static void test_identifier_injection() {
    const auto& m = orm::meta<User>();

    // Classic injection payloads in a column/sort position must be REJECTED.
    CHECK(throws_invalid([&] { (void)m.sql_select_where("email = 1 OR 1=1"); }));
    CHECK(throws_invalid([&] { (void)m.sql_select_where("email;DROP TABLE users--"); }));
    CHECK(throws_invalid([&] { (void)m.sql_select_where("(SELECT 1)"); }));
    CHECK(throws_invalid([&] { (void)m.sql_select_where("email)"); }));
    CHECK(throws_invalid([&] { (void)m.sql_select_where(""); }));
    CHECK(throws_invalid([&] { (void)m.sql_count_where("a OR b"); }));
    CHECK(throws_invalid([&] { (void)m.sql_delete_where("x'); DROP"); }));
    CHECK(throws_invalid([&] { (void)m.sql_select_all_ordered("age; DELETE FROM users"); }));

    // A table with an unsafe name is rejected at registration.
    CHECK(throws_invalid([] { (void)orm::register_table<User>("users; DROP TABLE x"); }));

    // The explicitly-raw escape hatch is NOT validated (documented as dangerous).
    CHECK(!throws_invalid([&] { (void)m.sql_select_raw_where("age > 18 AND email IS NOT NULL"); }));
}

int main() {
    test_sql_gen();
    test_identifier_injection();
    std::printf("\n%d/%d checks passed\n", g_total - g_fail, g_total);
    return g_fail ? 1 : 0;
}
