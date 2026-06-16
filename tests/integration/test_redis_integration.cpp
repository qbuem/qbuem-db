// Integration test: Redis client end-to-end (needs a running server).
// DSN from REDIS_DSN env, default points at the CI/docker test server.
#include "qbuem/db/redis_client.hpp"
#include "harness.hpp" // RunGuard, IT_CHECK, run_main

#include <cstdlib>
#include <optional>
#include <string>

using namespace qbuem_routine;

// Exercises CRUD across every Redis data type the client supports: strings,
// hashes, lists, sets and sorted sets, plus key/TTL management.  Proves the
// client actually talks to a real server (it was previously only unit-tested at
// the RESP-parser level).
static qbuem::Task<void> redis_crud_suite(redis::RedisClient& c) {
    co_await c.flushdb();

    // ── strings ──────────────────────────────────────────────────────────────
    IT_CHECK((co_await c.set("k1", "v1")).has_value());
    {
        auto g = co_await c.get("k1");
        IT_CHECK(g.has_value() && g->as_string() == "v1");
    }
    {
        auto miss = co_await c.get("nope");
        IT_CHECK(miss.has_value() && miss->is_null());
    }
    {
        auto ex = co_await c.exists("k1");
        IT_CHECK(ex.has_value() && *ex == true);
    }
    {
        auto n = co_await c.incr("counter");
        IT_CHECK(n.has_value() && *n == 1);
        auto n2 = co_await c.incrby("counter", 5);
        IT_CHECK(n2.has_value() && *n2 == 6);
        auto n3 = co_await c.decr("counter");
        IT_CHECK(n3.has_value() && *n3 == 5);
    }
    {
        IT_CHECK((co_await c.set("ttlkey", "x", 100)).has_value());
        auto t = co_await c.ttl("ttlkey");
        IT_CHECK(t.has_value() && *t > 0 && *t <= 100);
    }
    {
        IT_CHECK((co_await c.set("a", "1")).has_value());
        IT_CHECK((co_await c.set("b", "2")).has_value());
        auto m = co_await c.mget({"a", "b", "missing"});
        IT_CHECK(m.has_value());
        if (m) {
            IT_CHECK(m->size() == 3);
            IT_CHECK((*m)[0] == std::optional<std::string>{"1"});
            IT_CHECK((*m)[1] == std::optional<std::string>{"2"});
            IT_CHECK(!(*m)[2].has_value()); // missing → nullopt
        }
    }
    {
        auto d = co_await c.del("k1");
        IT_CHECK(d.has_value() && *d == 1);
        auto ex = co_await c.exists("k1");
        IT_CHECK(ex.has_value() && *ex == false);
    }

    // ── hashes ───────────────────────────────────────────────────────────────
    IT_CHECK((co_await c.hset("h", "f1", "hv1")).has_value());
    IT_CHECK((co_await c.hset("h", "f2", "hv2")).has_value());
    {
        auto v = co_await c.hget("h", "f1");
        IT_CHECK(v.has_value() && *v == std::optional<std::string>{"hv1"});
    }
    {
        auto he = co_await c.hexists("h", "f2");
        IT_CHECK(he.has_value() && *he == true);
    }
    {
        auto len = co_await c.hlen("h");
        IT_CHECK(len.has_value() && *len == 2);
    }
    {
        auto all = co_await c.hgetall("h");
        IT_CHECK(all.has_value() && all->size() == 2);
    }
    {
        auto d = co_await c.hdel("h", "f1");
        IT_CHECK(d.has_value() && *d == 1);
    }

    // ── lists ────────────────────────────────────────────────────────────────
    IT_CHECK((co_await c.rpush("l", "a")).has_value());
    IT_CHECK((co_await c.rpush("l", "b")).has_value());
    IT_CHECK((co_await c.lpush("l", "z")).has_value()); // z, a, b
    {
        auto len = co_await c.llen("l");
        IT_CHECK(len.has_value() && *len == 3);
    }
    {
        auto r = co_await c.lrange("l", 0, -1);
        IT_CHECK(r.has_value() && r->size() == 3);
        if (r && r->size() == 3) {
            IT_CHECK((*r)[0] == "z");
            IT_CHECK((*r)[2] == "b");
        }
    }
    {
        auto p = co_await c.lpop("l");
        IT_CHECK(p.has_value() && *p == std::optional<std::string>{"z"});
    }

    // ── sets ─────────────────────────────────────────────────────────────────
    IT_CHECK((co_await c.sadd("s", "m1")).has_value());
    IT_CHECK((co_await c.sadd("s", "m2")).has_value());
    {
        auto card = co_await c.scard("s");
        IT_CHECK(card.has_value() && *card == 2);
    }
    {
        auto is = co_await c.sismember("s", "m1");
        IT_CHECK(is.has_value() && *is == true);
    }
    {
        auto mem = co_await c.smembers("s");
        IT_CHECK(mem.has_value() && mem->size() == 2);
    }
    {
        auto r = co_await c.srem("s", "m1");
        IT_CHECK(r.has_value() && *r == 1);
    }

    // ── sorted sets ──────────────────────────────────────────────────────────
    IT_CHECK((co_await c.zadd("z", 1.0, "one")).has_value());
    IT_CHECK((co_await c.zadd("z", 2.0, "two")).has_value());
    {
        auto card = co_await c.zcard("z");
        IT_CHECK(card.has_value() && *card == 2);
    }
    {
        auto sc = co_await c.zscore("z", "two");
        IT_CHECK(sc.has_value() && sc->has_value() && **sc == 2.0);
    }
    {
        auto rk = co_await c.zrank("z", "one");
        IT_CHECK(rk.has_value() && rk->has_value() && **rk == 0);
    }
    {
        auto r = co_await c.zrange("z", 0, -1);
        IT_CHECK(r.has_value() && r->size() == 2);
        if (r && r->size() == 2) {
            IT_CHECK((*r)[0] == "one"); // ascending by score
            IT_CHECK((*r)[1] == "two");
        }
    }

    // ── connection liveness ────────────────────────────────────────────────────
    {
        auto p = co_await c.ping();
        IT_CHECK(p.has_value());
    }
    co_return;
}

int main() {
    const char* dsn = std::getenv("REDIS_DSN");
    std::string url = dsn ? dsn : "redis://127.0.0.1:6380";
    return qbuem_db_it::run_main([url]() -> qbuem::Task<void> {
        auto cli_r = co_await redis::RedisClient::connect(url);
        IT_CHECK(cli_r.has_value());
        if (!cli_r) co_return;
        auto cli = std::move(*cli_r);
        co_await redis_crud_suite(*cli);
    });
}
