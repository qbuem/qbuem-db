#pragma once

/**
 * @file db/redis_client.hpp
 * @brief 비동기 Redis 클라이언트 — qbuem Reactor 기반 RESP2 구현.
 *
 * 외부 의존성 없음: 순수 POSIX TCP 소켓 + RESP2 프로토콜 직접 구현.
 * qbuem Reactor 이벤트 루프와 코루틴으로 통합됩니다.
 *
 * DSN 형식:
 *   redis://host:port                  (기본 포트 6379)
 *   redis://host                       (포트 생략)
 *   redis://:password@host:port        (인증)
 *   redis://user:password@host:port/db (사용자 + 인증 + DB 번호)
 *   redis://host:port/2                (DB 번호 2 선택)
 *
 * 클라우드 호환:
 *   - Amazon ElastiCache (Redis OSS)
 *   - Upstash (서버리스 Redis)
 *   - Redis Cloud
 *   - Google Cloud Memorystore (Redis)
 *   - Azure Cache for Redis
 *
 * 지원 명령:
 *   문자열: GET, SET (TTL 포함), GETSET, SETNX, DEL, EXISTS, EXPIRE,
 *           TTL, PTTL, INCR, INCRBY, DECR, DECRBY, MGET, MSET
 *   해시:   HGET, HSET, HSETNX, HDEL, HGETALL, HKEYS, HVALS, HEXISTS, HLEN
 *   리스트: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX
 *   셋:    SADD, SREM, SMEMBERS, SCARD, SISMEMBER
 *   정렬셋: ZADD, ZREM, ZRANGE, ZRANGEBYSCORE, ZSCORE, ZCARD, ZRANK
 *   기타:  KEYS, SCAN, TYPE, RENAME, PERSIST, PING, FLUSHDB, INFO
 *
 * 사용 예:
 * ```cpp
 * #include "db/redis_client.hpp"
 *
 * auto client = co_await redis::RedisClient::connect("redis://localhost:6379");
 * if (!client) { handle_error(client.error()); }
 *
 * co_await (*client)->set("key", "value", 3600);  // TTL 1시간
 * auto val = co_await (*client)->get("key");
 * // val → RedisValue{Type::String, "value"}
 * ```
 */

#include <qbuem/core/task.hpp>
#include <qbuem/db/driver.hpp>  // Result, unexpected

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace qbuem_routine::redis {

using qbuem::Result;
using qbuem::unexpected;

// ── RedisValue — Redis 응답 타입 ─────────────────────────────────────────────

struct RedisValue {
    enum class Type {
        Null,       ///< nil / null bulk
        String,     ///< bulk string 또는 simple string
        Integer,    ///< :integer
        Array,      ///< *n 배열
        Error,      ///< -error
    };

    Type                          type{Type::Null};
    std::string                   str;           ///< String / Error 값
    int64_t                       integer{0};    ///< Integer 값
    std::vector<RedisValue>       array;         ///< Array 원소

    [[nodiscard]] bool   is_null()    const noexcept { return type == Type::Null; }
    [[nodiscard]] bool   is_ok()      const noexcept {
        return type == Type::String && str == "OK";
    }
    [[nodiscard]] bool   is_error()   const noexcept { return type == Type::Error; }
    [[nodiscard]] bool   ok_or_bool() const noexcept {
        if (type == Type::Integer) return integer != 0;
        return is_ok();
    }

    // 편의 접근자
    [[nodiscard]] std::string_view   as_string()  const noexcept { return str; }
    [[nodiscard]] int64_t            as_int()     const noexcept { return integer; }
    [[nodiscard]] const std::vector<RedisValue>& as_array() const noexcept { return array; }
};

// ── 에러 코드 ─────────────────────────────────────────────────────────────────

struct RedisErrorCategory : std::error_category {
    const char* name() const noexcept override { return "redis"; }
    std::string message(int code) const override;
};

enum class RedisError : int {
    Ok = 0,
    ConnectionFailed    = 1,
    ConnectionClosed    = 2,
    ProtocolError       = 3,
    AuthFailed          = 4,
    CommandError        = 5,
    Timeout             = 6,
};

std::error_code make_error(RedisError e);

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

    /// Redis 서버에 비동기 연결 (Reactor::current() 필요)
    [[nodiscard]] static qbuem::Task<Result<std::unique_ptr<RedisClient>>>
    connect(std::string_view dsn);

    // ── 연결 관리 ────────────────────────────────────────────────────────────

    [[nodiscard]] qbuem::Task<Result<RedisValue>> ping();

    // ── 문자열 명령 ──────────────────────────────────────────────────────────

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

    /// MGET key [key ...] → 순서 대응 optional<string> 벡터
    [[nodiscard]] qbuem::Task<Result<std::vector<std::optional<std::string>>>>
    mget(std::vector<std::string> keys);

    // ── 해시 명령 ─────────────────────────────────────────────────────────────

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

    // ── 리스트 명령 ──────────────────────────────────────────────────────────

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

    // ── 셋 명령 ──────────────────────────────────────────────────────────────

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

    // ── 정렬 셋 명령 ─────────────────────────────────────────────────────────

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

    // ── 기타 ─────────────────────────────────────────────────────────────────

    [[nodiscard]] qbuem::Task<Result<std::string>> type(std::string_view key);
    [[nodiscard]] qbuem::Task<Result<std::vector<std::string>>> keys(std::string_view pattern);
    [[nodiscard]] qbuem::Task<Result<RedisValue>>  info();
    [[nodiscard]] qbuem::Task<Result<RedisValue>>  flushdb();

    /// 임의 명령 실행 (확장용)
    [[nodiscard]] qbuem::Task<Result<RedisValue>>
    command(std::vector<std::string> args);

private:
    std::unique_ptr<RedisClientImpl> impl_;
};

} // namespace qbuem_routine::redis
