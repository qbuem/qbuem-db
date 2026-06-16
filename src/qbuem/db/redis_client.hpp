#pragma once

/**
 * @file db/redis_client.hpp
 * @brief Async Redis client — RESP2 over the qbuem Reactor.
 *
 * No external dependencies: raw POSIX TCP sockets + a hand-written RESP2
 * protocol. Integrates with the qbuem Reactor event loop via coroutines.
 *
 * DSN forms:
 *   redis://host:port                  (default port 6379)
 *   redis://host                       (port omitted)
 *   redis://:password@host:port        (auth)
 *   redis://user:password@host:port/db (user + auth + DB number)
 *   redis://host:port/2                (select DB number 2)
 *
 * Cloud-compatible:
 *   - Amazon ElastiCache (Redis OSS)
 *   - Upstash (serverless Redis)
 *   - Redis Cloud
 *   - Google Cloud Memorystore (Redis)
 *   - Azure Cache for Redis
 *
 * Supported commands:
 *   strings:     GET, SET (with TTL), GETSET, SETNX, DEL, EXISTS, EXPIRE,
 *                TTL, PTTL, INCR, INCRBY, DECR, DECRBY, MGET, MSET
 *   hashes:      HGET, HSET, HSETNX, HDEL, HGETALL, HKEYS, HVALS, HEXISTS, HLEN
 *   lists:       LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX
 *   sets:        SADD, SREM, SMEMBERS, SCARD, SISMEMBER
 *   sorted sets: ZADD, ZREM, ZRANGE, ZRANGEBYSCORE, ZSCORE, ZCARD, ZRANK
 *   transactions: MULTI, EXEC, DISCARD, WATCH, UNWATCH
 *   misc:        KEYS, SCAN, TYPE, RENAME, PERSIST, PING, FLUSHDB, INFO
 *
 * Usage:
 * ```cpp
 * #include "db/redis_client.hpp"
 *
 * auto client = co_await redis::RedisClient::connect("redis://localhost:6379");
 * if (!client) { handle_error(client.error()); }
 *
 * co_await (*client)->set("key", "value", 3600);  // TTL: 1 hour
 * auto val = co_await (*client)->get("key");
 * // val → RedisValue{Type::String, "value"}
 * ```
 */

#include "db_error.hpp"

#include <qbuem/core/task.hpp>
#include <qbuem/db/driver.hpp>  // Result, unexpected

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace qbuem_routine::redis {

using qbuem::Result;
using qbuem::unexpected;

// ── RedisValue — a Redis reply ───────────────────────────────────────────────

struct RedisValue {
    enum class Type {
        Null,       ///< nil / null bulk
        String,     ///< bulk string or simple string
        Integer,    ///< :integer
        Array,      ///< *n array
        Error,      ///< -error
    };

    Type                          type{Type::Null};
    std::string                   str;           ///< String / Error value
    int64_t                       integer{0};    ///< Integer value
    std::vector<RedisValue>       array;         ///< Array elements

    [[nodiscard]] bool   is_null()    const noexcept { return type == Type::Null; }
    [[nodiscard]] bool   is_ok()      const noexcept {
        return type == Type::String && str == "OK";
    }
    [[nodiscard]] bool   is_error()   const noexcept { return type == Type::Error; }
    [[nodiscard]] bool   ok_or_bool() const noexcept {
        if (type == Type::Integer) return integer != 0;
        return is_ok();
    }

    // convenience accessors
    [[nodiscard]] std::string_view   as_string()  const noexcept { return str; }
    [[nodiscard]] int64_t            as_int()     const noexcept { return integer; }
    [[nodiscard]] const std::vector<RedisValue>& as_array() const noexcept { return array; }
};

// ── RedisClient ───────────────────────────────────────────────────────────────

class RedisClientImpl;

class RedisClient {
public:
    explicit RedisClient(std::unique_ptr<RedisClientImpl> impl);
    ~RedisClient();

    RedisClient(RedisClient&&) noexcept;
    RedisClient& operator=(RedisClient&&) noexcept;
    RedisClient(const RedisClient&)            = delete;
    RedisClient& operator=(const RedisClient&) = delete;

    /// Async connect to a Redis server (requires Reactor::current()).
    [[nodiscard]] static qbuem::Task<Result<std::unique_ptr<RedisClient>>>
    connect(std::string_view dsn);

    // ── Connection management ─────────────────────────────────────────────────

    [[nodiscard]] qbuem::Task<Result<RedisValue>> ping();

    // ── String commands ──────────────────────────────────────────────────────

    [[nodiscard]] qbuem::Task<Result<RedisValue>> get(std::string_view key);

    /// SET key value [EX seconds]
    [[nodiscard]] qbuem::Task<Result<RedisValue>>
    set(std::string_view key, std::string_view value,
        std::optional<int64_t> ttl_sec = {});

    /// SETNX key value (set if not exists)
    [[nodiscard]] qbuem::Task<Result<bool>>
    setnx(std::string_view key, std::string_view value);

    [[nodiscard]] qbuem::Task<Result<int64_t>> del(std::string_view key);
    [[nodiscard]] qbuem::Task<Result<bool>>    exists(std::string_view key);
    [[nodiscard]] qbuem::Task<Result<bool>>    expire(std::string_view key, int64_t seconds);
    [[nodiscard]] qbuem::Task<Result<int64_t>> ttl(std::string_view key);
    [[nodiscard]] qbuem::Task<Result<int64_t>> incr(std::string_view key);
    [[nodiscard]] qbuem::Task<Result<int64_t>> incrby(std::string_view key, int64_t delta);
    [[nodiscard]] qbuem::Task<Result<int64_t>> decr(std::string_view key);
    [[nodiscard]] qbuem::Task<Result<int64_t>> decrby(std::string_view key, int64_t delta);

    /// MGET key [key ...] → vector of optional<string>, positionally aligned
    [[nodiscard]] qbuem::Task<Result<std::vector<std::optional<std::string>>>>
    mget(std::vector<std::string> keys);

    // ── Hash commands ─────────────────────────────────────────────────────────

    [[nodiscard]] qbuem::Task<Result<std::optional<std::string>>>
    hget(std::string_view key, std::string_view field);

    [[nodiscard]] qbuem::Task<Result<bool>>
    hset(std::string_view key, std::string_view field, std::string_view value);

    [[nodiscard]] qbuem::Task<Result<bool>>
    hsetnx(std::string_view key, std::string_view field, std::string_view value);

    [[nodiscard]] qbuem::Task<Result<int64_t>>
    hdel(std::string_view key, std::string_view field);

    [[nodiscard]] qbuem::Task<Result<bool>>
    hexists(std::string_view key, std::string_view field);

    [[nodiscard]] qbuem::Task<Result<std::vector<std::pair<std::string, std::string>>>>
    hgetall(std::string_view key);

    [[nodiscard]] qbuem::Task<Result<int64_t>>
    hlen(std::string_view key);

    // ── List commands ────────────────────────────────────────────────────────

    [[nodiscard]] qbuem::Task<Result<int64_t>>
    lpush(std::string_view key, std::string_view value);

    [[nodiscard]] qbuem::Task<Result<int64_t>>
    rpush(std::string_view key, std::string_view value);

    [[nodiscard]] qbuem::Task<Result<std::optional<std::string>>>
    lpop(std::string_view key);

    [[nodiscard]] qbuem::Task<Result<std::optional<std::string>>>
    rpop(std::string_view key);

    [[nodiscard]] qbuem::Task<Result<std::vector<std::string>>>
    lrange(std::string_view key, int64_t start, int64_t stop);

    [[nodiscard]] qbuem::Task<Result<int64_t>>
    llen(std::string_view key);

    // ── Set commands ─────────────────────────────────────────────────────────

    [[nodiscard]] qbuem::Task<Result<int64_t>>
    sadd(std::string_view key, std::string_view member);

    [[nodiscard]] qbuem::Task<Result<int64_t>>
    srem(std::string_view key, std::string_view member);

    [[nodiscard]] qbuem::Task<Result<std::vector<std::string>>>
    smembers(std::string_view key);

    [[nodiscard]] qbuem::Task<Result<int64_t>>
    scard(std::string_view key);

    [[nodiscard]] qbuem::Task<Result<bool>>
    sismember(std::string_view key, std::string_view member);

    // ── Sorted-set commands ──────────────────────────────────────────────────

    [[nodiscard]] qbuem::Task<Result<int64_t>>
    zadd(std::string_view key, double score, std::string_view member);

    [[nodiscard]] qbuem::Task<Result<int64_t>>
    zrem(std::string_view key, std::string_view member);

    [[nodiscard]] qbuem::Task<Result<std::vector<std::string>>>
    zrange(std::string_view key, int64_t start, int64_t stop);

    [[nodiscard]] qbuem::Task<Result<std::optional<double>>>
    zscore(std::string_view key, std::string_view member);

    [[nodiscard]] qbuem::Task<Result<int64_t>>
    zcard(std::string_view key);

    [[nodiscard]] qbuem::Task<Result<std::optional<int64_t>>>
    zrank(std::string_view key, std::string_view member);

    // ── Transactions (MULTI / EXEC) ──────────────────────────────────────────

    /// MULTI — begin a transaction block. Subsequent commands are queued by the
    /// server (each replies "QUEUED") until exec() or discard().
    [[nodiscard]] qbuem::Task<Result<RedisValue>> multi();

    /// EXEC — atomically run the queued commands. The reply is an Array with one
    /// element per queued command (in order), each its own result or error.
    [[nodiscard]] qbuem::Task<Result<RedisValue>> exec();

    /// DISCARD — abandon a transaction block; queued commands are not run.
    [[nodiscard]] qbuem::Task<Result<RedisValue>> discard();

    /// WATCH key [key ...] — mark keys for optimistic locking. A following EXEC
    /// fails (nil reply) if any watched key changed since the WATCH.
    [[nodiscard]] qbuem::Task<Result<RedisValue>> watch(std::vector<std::string> keys);

    /// UNWATCH — forget all watched keys.
    [[nodiscard]] qbuem::Task<Result<RedisValue>> unwatch();

    /// Convenience: run `commands` atomically (MULTI → queue each → EXEC).
    /// Returns the EXEC reply (an Array of per-command results). If queueing a
    /// command fails, the transaction is DISCARDed and the error is returned.
    [[nodiscard]] qbuem::Task<Result<RedisValue>>
    transaction(std::vector<std::vector<std::string>> commands);

    // ── Misc ──────────────────────────────────────────────────────────────────

    [[nodiscard]] qbuem::Task<Result<std::string>> type(std::string_view key);
    [[nodiscard]] qbuem::Task<Result<std::vector<std::string>>> keys(std::string_view pattern);
    [[nodiscard]] qbuem::Task<Result<RedisValue>>  info();
    [[nodiscard]] qbuem::Task<Result<RedisValue>>  flushdb();

    /// Run an arbitrary command (extension point).
    [[nodiscard]] qbuem::Task<Result<RedisValue>>
    command(std::vector<std::string> args);

private:
    std::unique_ptr<RedisClientImpl> impl_;
};

// ── RedisPool — connection pool for concurrent access ─────────────────────────
//
// A Redis connection processes commands serially, so concurrent workloads need
// multiple connections.  RedisPool keeps a set of independent RedisClient
// connections and hands them out via RAII PooledRedisConnection handles that
// return to the pool when destroyed.

class RedisPool;

/// RAII handle to a pooled connection.  Use `->` / `*` to issue commands; the
/// connection returns to the pool when this handle is destroyed.
class PooledRedisConnection {
public:
    PooledRedisConnection(std::unique_ptr<RedisClient> client, RedisPool* pool) noexcept
        : client_(std::move(client)), pool_(pool) {}
    ~PooledRedisConnection();

    PooledRedisConnection(PooledRedisConnection&&) noexcept            = default;
    PooledRedisConnection& operator=(PooledRedisConnection&&) noexcept = default;
    PooledRedisConnection(const PooledRedisConnection&)            = delete;
    PooledRedisConnection& operator=(const PooledRedisConnection&) = delete;

    [[nodiscard]] RedisClient* operator->() const noexcept { return client_.get(); }
    [[nodiscard]] RedisClient& operator*()  const noexcept { return *client_; }
    [[nodiscard]] RedisClient* get()        const noexcept { return client_.get(); }

private:
    std::unique_ptr<RedisClient> client_;
    RedisPool*                   pool_;
};

class RedisPool {
public:
    /// Connect `size` clients to `dsn` up front.  Fails only if not even one
    /// connection can be established (a partial pool is accepted).
    [[nodiscard]] static qbuem::Task<Result<std::unique_ptr<RedisPool>>>
    create(std::string_view dsn, std::size_t size = 4);

    /// Check out a connection.  Reuses an idle one, else opens a new one up to the
    /// pool's max size, else returns PoolExhausted.
    [[nodiscard]] qbuem::Task<Result<PooledRedisConnection>> acquire();

    /// Return a connection to the idle set (called by ~PooledRedisConnection).
    void release(std::unique_ptr<RedisClient> client) noexcept;

    [[nodiscard]] std::size_t max_size()   const noexcept { return max_size_; }
    [[nodiscard]] std::size_t idle_count() const noexcept;
    [[nodiscard]] std::size_t size()       const noexcept; ///< total live connections

private:
    RedisPool(std::string dsn, std::size_t max) noexcept
        : dsn_(std::move(dsn)), max_size_(max) {}

    std::string                               dsn_;
    std::size_t                               max_size_;
    mutable std::mutex                        mutex_;
    std::vector<std::unique_ptr<RedisClient>> idle_;
    std::size_t                               total_{0};
};

} // namespace qbuem_routine::redis
