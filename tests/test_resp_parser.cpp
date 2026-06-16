// Unit tests for the hardened RESP2 parser (resp_parser.hpp).  No Redis server
// needed — the parser is fed crafted byte sequences, including the malicious
// inputs an untrusted server could send.  Verifies bounds-safety (no crash, no
// hang, no mis-parse) and correctness.
//
// Build:
//   g++ -std=c++23 -I../src -I<qbuem-stack>/include test_resp_parser.cpp -o t && ./t

#include "qbuem/db/resp_parser.hpp"

#include <cstdio>
#include <string>

using qbuem_routine::redis::RespParser;
using RV = qbuem_routine::redis::RedisValue;

static int g_fail = 0, g_total = 0;
#define CHECK(cond)                                                            \
    do {                                                                      \
        ++g_total;                                                            \
        if (!(cond)) { ++g_fail; std::printf("FAIL %s:%d  %s\n", __FILE__, __LINE__, #cond); } \
    } while (0)

static RespParser feed1(const std::string& bytes) {
    RespParser p;
    p.feed(bytes.data(), bytes.size());
    return p;
}

// ── Well-formed framing ──────────────────────────────────────────────────────
static void test_valid() {
    { auto p = feed1("+OK\r\n");        CHECK(p.has_complete()); auto v = p.parse();
      CHECK(v.type == RV::Type::String); CHECK(v.str == "OK"); CHECK(p.empty()); }
    { auto p = feed1("-WRONGTYPE x\r\n"); CHECK(p.has_complete()); auto v = p.parse();
      CHECK(v.type == RV::Type::Error); CHECK(v.str == "WRONGTYPE x"); }
    { auto p = feed1(":12345\r\n");     CHECK(p.has_complete()); auto v = p.parse();
      CHECK(v.type == RV::Type::Integer); CHECK(v.integer == 12345); }
    { auto p = feed1(":-9\r\n");        CHECK(p.has_complete()); CHECK(p.parse().integer == -9); }
    { auto p = feed1("$5\r\nhello\r\n"); CHECK(p.has_complete()); auto v = p.parse();
      CHECK(v.type == RV::Type::String); CHECK(v.str == "hello"); }
    { auto p = feed1("$0\r\n\r\n");     CHECK(p.has_complete()); auto v = p.parse();
      CHECK(v.type == RV::Type::String); CHECK(v.str.empty()); }
    { auto p = feed1("$-1\r\n");        CHECK(p.has_complete()); CHECK(p.parse().type == RV::Type::Null); }
    { auto p = feed1("*-1\r\n");        CHECK(p.has_complete()); CHECK(p.parse().type == RV::Type::Null); }
    // Bulk containing CRLF in its payload (length-delimited, must be preserved).
    { auto p = feed1("$4\r\na\r\nb\r\n"); CHECK(p.has_complete()); CHECK(p.parse().str == "a\r\nb"); }
    // Array of two bulks.
    { auto p = feed1("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"); CHECK(p.has_complete());
      auto v = p.parse(); CHECK(v.type == RV::Type::Array); CHECK(v.array.size() == 2);
      CHECK(v.array[0].str == "foo"); CHECK(v.array[1].str == "bar"); }
}

// ── Incremental / streaming ──────────────────────────────────────────────────
static void test_partial() {
    RespParser p;
    p.feed("$5\r\nhel", 7);
    CHECK(!p.has_complete());           // bulk body incomplete
    p.feed("lo\r\n", 4);
    CHECK(p.has_complete());
    CHECK(p.parse().str == "hello");

    // Two replies buffered: parse consumes only the first.
    auto q = feed1("+A\r\n+B\r\n");
    CHECK(q.has_complete());
    CHECK(q.parse().str == "A");
    CHECK(q.has_complete());
    CHECK(q.parse().str == "B");
    CHECK(q.empty());
}

// ── Malicious / malformed: must NOT crash, hang, or mis-parse ────────────────
static void test_malicious() {
    auto is_proto_err = [](RespParser& p) {
        CHECK(p.has_complete());              // ready (malformed) — never blocks
        auto v = p.parse();
        CHECK(v.type == RV::Type::Error);     // surfaced as protocol error
    };

    // Overflow attempt: $len near INT64_MAX must not wrap the bounds check.
    { auto p = feed1("$9223372036854775000\r\nshort\r\n"); is_proto_err(p); }
    // Bulk length above the 512 MiB cap.
    { auto p = feed1("$999999999\r\n"); is_proto_err(p); }
    // Non-numeric / trailing-garbage lengths.
    { auto p = feed1("$abc\r\n");  is_proto_err(p); }
    { auto p = feed1("$10x\r\n");  is_proto_err(p); }
    { auto p = feed1("$\r\n");     is_proto_err(p); }
    // Wrong trailing terminator after a complete-length bulk.
    { auto p = feed1("$3\r\nfooXX"); is_proto_err(p); }
    // Unknown prefix byte.
    { auto p = feed1("@nope\r\n"); is_proto_err(p); }
    // Array count above the cap.
    { auto p = feed1("*99999999\r\n"); is_proto_err(p); }

    // Deeply nested arrays must hit the depth cap, NOT overflow the stack.
    { std::string deep; for (int i = 0; i < 5000; ++i) deep += "*1\r\n"; deep += ":1\r\n";
      auto p = feed1(deep); is_proto_err(p); }  // completes without crashing
}

// ── feed() buffer cap ────────────────────────────────────────────────────────
static void test_buffer_cap() {
    RespParser p;
    // A declared-but-never-completed bulk: feed must cap total buffer growth and
    // then surface a protocol error rather than growing without bound.
    std::string header = "$500000000\r\n"; // 500 MB declared (< cap), body never sent
    p.feed(header.data(), header.size());
    std::string chunk(1 << 20, 'x');        // 1 MiB
    for (int i = 0; i < 600; ++i) p.feed(chunk.data(), chunk.size()); // try to exceed 512 MiB
    CHECK(p.has_complete());                // buffer full + incomplete → ready (error)
    CHECK(p.parse().type == RV::Type::Error);
}

int main() {
    test_valid();
    test_partial();
    test_malicious();
    test_buffer_cap();
    std::printf("\n%d/%d checks passed\n", g_total - g_fail, g_total);
    return g_fail ? 1 : 0;
}
