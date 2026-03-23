#include "redis_client.hpp"

#include <qbuem/core/reactor.hpp>
#include <qbuem/core/task.hpp>

#include <arpa/inet.h>
#include <charconv>
#include <coroutine>
#include <cstring>
#include <fcntl.h>
#include <format>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

using namespace qbuem;

namespace qbuem_routine::redis {

// ── 에러 ─────────────────────────────────────────────────────────────────────

std::string RedisErrorCategory::message(int code) const {
    switch (static_cast<RedisError>(code)) {
        case RedisError::Ok:               return "ok";
        case RedisError::ConnectionFailed: return "redis: connection failed";
        case RedisError::ConnectionClosed: return "redis: connection closed";
        case RedisError::ProtocolError:    return "redis: protocol error";
        case RedisError::AuthFailed:       return "redis: auth failed";
        case RedisError::CommandError:     return "redis: command error";
        case RedisError::Timeout:          return "redis: timeout";
    }
    return "redis: unknown error";
}

std::error_code make_error(RedisError e) {
    static RedisErrorCategory cat;
    return {static_cast<int>(e), cat};
}

// ── DSN 파서 ─────────────────────────────────────────────────────────────────

struct RedisDsn {
    std::string host{"127.0.0.1"};
    uint16_t    port{6379};
    std::string password;
    std::string username;
    int         db{0};
};

static RedisDsn parse_dsn(std::string_view dsn) {
    RedisDsn result;
    if (dsn.starts_with("redis://"))  dsn.remove_prefix(8);

    // user:pass@
    if (auto at = dsn.find('@'); at != std::string_view::npos) {
        auto creds = dsn.substr(0, at);
        dsn = dsn.substr(at + 1);
        if (auto colon = creds.find(':'); colon != std::string_view::npos) {
            result.username = std::string(creds.substr(0, colon));
            result.password = std::string(creds.substr(colon + 1));
        } else {
            result.password = std::string(creds);
        }
    }

    // host:port/db
    if (auto slash = dsn.find('/'); slash != std::string_view::npos) {
        auto db_str = dsn.substr(slash + 1);
        dsn = dsn.substr(0, slash);
        if (!db_str.empty())
            std::from_chars(db_str.data(), db_str.data() + db_str.size(), result.db);
    }

    if (auto colon = dsn.rfind(':'); colon != std::string_view::npos) {
        result.host = std::string(dsn.substr(0, colon));
        auto port_sv = dsn.substr(colon + 1);
        std::from_chars(port_sv.data(), port_sv.data() + port_sv.size(), result.port);
    } else if (!dsn.empty()) {
        result.host = std::string(dsn);
    }

    return result;
}

// ── RESP2 인코더 ─────────────────────────────────────────────────────────────

static std::string encode_resp(const std::vector<std::string>& args) {
    std::string buf;
    // *<N>\r\n  +  per arg: $<len>\r\n<data>\r\n  ≈ 16 + 8*N + sum(sizes)
    std::size_t total = 16;
    for (const auto& a : args) total += 8 + a.size();
    buf.reserve(total);
    buf += '*';
    buf += std::to_string(args.size());
    buf += "\r\n";
    for (const auto& arg : args) {
        buf += '$';
        buf += std::to_string(arg.size());
        buf += "\r\n";
        buf += arg;
        buf += "\r\n";
    }
    return buf;
}

// ── RESP2 파서 ───────────────────────────────────────────────────────────────

class RespParser {
public:
    /// 버퍼에 데이터 추가
    void feed(const char* data, size_t len) {
        buf_.append(data, len);
    }

    /// 완전한 응답이 파싱 가능한지 확인
    [[nodiscard]] bool has_complete() const {
        std::size_t pos = 0;
        return can_parse(pos);
    }

    /// 응답 파싱 (has_complete() == true 이어야 함)
    [[nodiscard]] RedisValue parse() {
        std::size_t pos = 0;
        auto val = parse_value(pos);
        buf_.erase(0, pos);
        return val;
    }

    [[nodiscard]] bool empty() const noexcept { return buf_.empty(); }

private:
    std::string buf_;

    [[nodiscard]] bool can_parse(std::size_t& pos) const {
        if (pos >= buf_.size()) return false;
        const char prefix = buf_[pos];
        switch (prefix) {
            case '+': case '-': case ':': {
                auto end = buf_.find("\r\n", pos + 1);
                if (end == std::string::npos) return false;
                pos = end + 2;
                return true;
            }
            case '$': {
                auto end = buf_.find("\r\n", pos + 1);
                if (end == std::string::npos) return false;
                int64_t len = -1;
                std::from_chars(buf_.data() + pos + 1,
                                buf_.data() + end, len);
                if (len < 0) { pos = end + 2; return true; } // null bulk
                if (pos + (end - pos) + 2 + len + 2 > buf_.size()) return false;
                pos = end + 2 + len + 2;
                return true;
            }
            case '*': {
                auto end = buf_.find("\r\n", pos + 1);
                if (end == std::string::npos) return false;
                int64_t count = -1;
                std::from_chars(buf_.data() + pos + 1,
                                buf_.data() + end, count);
                pos = end + 2;
                if (count < 0) return true; // null array
                for (int64_t i = 0; i < count; ++i)
                    if (!can_parse(pos)) return false;
                return true;
            }
            default: return false;
        }
    }

    [[nodiscard]] RedisValue parse_value(std::size_t& pos) {
        if (pos >= buf_.size()) return {};
        const char prefix = buf_[pos++];
        switch (prefix) {
            case '+': {
                auto end = buf_.find("\r\n", pos);
                if (end == std::string::npos) return {};
                RedisValue v; v.type = RedisValue::Type::String;
                v.str = buf_.substr(pos, end - pos);
                pos = end + 2;
                return v;
            }
            case '-': {
                auto end = buf_.find("\r\n", pos);
                if (end == std::string::npos) return {};
                RedisValue v; v.type = RedisValue::Type::Error;
                v.str = buf_.substr(pos, end - pos);
                pos = end + 2;
                return v;
            }
            case ':': {
                auto end = buf_.find("\r\n", pos);
                if (end == std::string::npos) return {};
                RedisValue v; v.type = RedisValue::Type::Integer;
                std::from_chars(buf_.data() + pos,
                                buf_.data() + end, v.integer);
                pos = end + 2;
                return v;
            }
            case '$': {
                auto end = buf_.find("\r\n", pos);
                if (end == std::string::npos) return {};
                int64_t len = -1;
                std::from_chars(buf_.data() + pos, buf_.data() + end, len);
                pos = end + 2;
                if (len < 0) return {}; // null bulk
                RedisValue v; v.type = RedisValue::Type::String;
                v.str = buf_.substr(pos, static_cast<size_t>(len));
                pos += static_cast<size_t>(len) + 2;
                return v;
            }
            case '*': {
                auto end = buf_.find("\r\n", pos);
                if (end == std::string::npos) return {};
                int64_t count = -1;
                std::from_chars(buf_.data() + pos, buf_.data() + end, count);
                pos = end + 2;
                if (count < 0) return {}; // null array
                RedisValue v; v.type = RedisValue::Type::Array;
                v.array.reserve(static_cast<size_t>(count));
                for (int64_t i = 0; i < count; ++i)
                    v.array.push_back(parse_value(pos));
                return v;
            }
            default:
                return {};
        }
    }
};

// ── Reactor 기반 비동기 Awaiter ──────────────────────────────────────────────

struct RedisReadAwaiter {
    int          fd;
    RespParser*  parser;
    RedisValue   result;
    bool         error{false};

    bool await_ready() noexcept {
        // 이미 버퍼에 완성된 응답이 있으면 즉시 반환
        return parser->has_complete();
    }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        auto* reactor = Reactor::current();
        reactor->register_event(fd, EventType::Read,
            [this, h, reactor](int) mutable {
                char tmp[4096];
                const ssize_t n = ::read(fd, tmp, sizeof(tmp));
                if (n <= 0) {
                    error = true;
                    reactor->unregister_event(fd, EventType::Read);
                    reactor->post([h]() mutable { h.resume(); });
                    return;
                }
                parser->feed(tmp, static_cast<size_t>(n));
                if (parser->has_complete()) {
                    reactor->unregister_event(fd, EventType::Read);
                    reactor->post([h]() mutable { h.resume(); });
                }
            });
    }

    RedisValue await_resume() noexcept {
        if (error) return {};
        return parser->parse();
    }
};

struct RedisWriteAwaiter {
    int         fd;
    std::string data;
    std::size_t sent{0};
    bool        error{false};

    bool await_ready() noexcept {
        // 즉시 쓰기 시도
        while (sent < data.size()) {
            const ssize_t n = ::write(fd, data.data() + sent, data.size() - sent);
            if (n > 0) { sent += static_cast<size_t>(n); continue; }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return false;
            error = true; return true;
        }
        return true;
    }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        auto* reactor = Reactor::current();
        reactor->register_event(fd, EventType::Write,
            [this, h, reactor](int) mutable {
                while (sent < data.size()) {
                    const ssize_t n = ::write(fd, data.data() + sent,
                                              data.size() - sent);
                    if (n > 0) { sent += static_cast<size_t>(n); continue; }
                    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
                    error = true; break;
                }
                if (sent >= data.size() || error) {
                    reactor->unregister_event(fd, EventType::Write);
                    reactor->post([h]() mutable { h.resume(); });
                }
            });
    }

    bool await_resume() noexcept { return !error && sent == data.size(); }
};

// ── RedisClientImpl ───────────────────────────────────────────────────────────

class RedisClientImpl {
public:
    RedisClientImpl(int fd, RedisDsn dsn) noexcept
        : fd_(fd), dsn_(std::move(dsn)) {}

    ~RedisClientImpl() { close_fd(); }

    RedisClientImpl(const RedisClientImpl&)            = delete;
    RedisClientImpl& operator=(const RedisClientImpl&) = delete;

    // 연결 끊김 시 재연결 + AUTH + SELECT 재수행
    Task<bool> reconnect() {
        close_fd();
        parser_ = RespParser{};

        const int fd = connect_tcp(dsn_);
        if (fd < 0) co_return false;

        RedisConnectAwaiter conn_aw{fd};
        if (!conn_aw.await_ready()) {
            if (!co_await conn_aw) { ::close(fd); co_return false; }
        }
        fd_ = fd;

        if (!dsn_.password.empty()) {
            std::vector<std::string> auth_args;
            if (!dsn_.username.empty())
                auth_args = {"AUTH", dsn_.username, dsn_.password};
            else
                auth_args = {"AUTH", dsn_.password};
            auto r = co_await send_once(std::move(auth_args));
            if (!r || !(*r).is_ok()) { close_fd(); co_return false; }
        }
        if (dsn_.db > 0) {
            auto r = co_await send_once({"SELECT", std::to_string(dsn_.db)});
            if (!r) { close_fd(); co_return false; }
        }
        co_return true;
    }

    // ConnectionClosed 시 재연결 후 1회 재시도
    Task<Result<RedisValue>> send(std::vector<std::string> args) {
        auto r = co_await send_once(args);
        if (r || r.error() != make_error(RedisError::ConnectionClosed))
            co_return r;

        // 재연결 후 재시도
        if (!co_await reconnect())
            co_return unexpected(make_error(RedisError::ConnectionClosed));
        co_return co_await send_once(std::move(args));
    }

    [[nodiscard]] int fd() const noexcept { return fd_; }

private:
    void close_fd() noexcept {
        if (fd_ < 0) return;
        auto* reactor = Reactor::current();
        if (reactor) {
            reactor->unregister_event(fd_, EventType::Read);
            reactor->unregister_event(fd_, EventType::Write);
        }
        ::close(fd_);
        fd_ = -1;
    }

    Task<Result<RedisValue>> send_once(std::vector<std::string> args) {
        auto encoded = encode_resp(args);

        // 쓰기
        RedisWriteAwaiter write_aw{fd_, std::move(encoded)};
        if (!write_aw.await_ready()) {
            if (!co_await write_aw)
                co_return unexpected(make_error(RedisError::ConnectionClosed));
        } else if (write_aw.error) {
            co_return unexpected(make_error(RedisError::ConnectionClosed));
        }

        // 읽기
        RedisReadAwaiter read_aw{fd_, &parser_};
        RedisValue val;
        if (read_aw.await_ready()) {
            val = parser_.parse();
        } else {
            val = co_await read_aw;
            if (read_aw.error)
                co_return unexpected(make_error(RedisError::ConnectionClosed));
        }

        if (val.is_error())
            co_return unexpected(make_error(RedisError::CommandError));

        co_return val;
    }

    int        fd_;
    RedisDsn   dsn_;
    RespParser parser_;
};

// ── 연결 헬퍼 ─────────────────────────────────────────────────────────────────

static int connect_tcp(const RedisDsn& dsn) {
    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    const auto port_str = std::to_string(dsn.port);
    if (::getaddrinfo(dsn.host.c_str(), port_str.c_str(), &hints, &res) != 0)
        return -1;

    int fd = -1;
    for (auto* rp = res; rp; rp = rp->ai_next) {
        fd = ::socket(rp->ai_family, rp->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                      rp->ai_protocol);
        if (fd < 0) continue;

        // TCP_NODELAY: 레이턴시 최소화 (Redis는 작은 패킷 다수)
        const int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        const int rc = ::connect(fd, rp->ai_addr, rp->ai_addrlen);
        if (rc == 0 || errno == EINPROGRESS) break;
        ::close(fd); fd = -1;
    }
    ::freeaddrinfo(res);
    return fd;
}

struct RedisConnectAwaiter {
    int fd;
    bool ready{false};
    bool error{false};

    bool await_ready() noexcept {
        // connect가 즉시 완료되는 경우 (localhost)
        int err = 0;
        socklen_t len = sizeof(err);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) == 0 && err == 0) {
            ready = true;
            return true;
        }
        return false;
    }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        auto* reactor = Reactor::current();
        reactor->register_event(fd, EventType::Write,
            [this, h, reactor](int) mutable {
                int err = 0;
                socklen_t len = sizeof(err);
                ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
                error = (err != 0);
                reactor->unregister_event(fd, EventType::Write);
                reactor->post([h]() mutable { h.resume(); });
            });
    }

    bool await_resume() noexcept { return !error; }
};

// ── RedisClient 구현 ──────────────────────────────────────────────────────────

RedisClient::RedisClient(std::unique_ptr<RedisClientImpl> impl)
    : impl_(std::move(impl)) {}

RedisClient::~RedisClient() = default;

RedisClient::RedisClient(RedisClient&&) noexcept = default;
RedisClient& RedisClient::operator=(RedisClient&&) noexcept = default;

Task<Result<std::unique_ptr<RedisClient>>>
RedisClient::connect(std::string_view dsn) {
    const auto parsed = parse_dsn(dsn);

    const int fd = connect_tcp(parsed);
    if (fd < 0)
        co_return unexpected(make_error(RedisError::ConnectionFailed));

    // 비동기 연결 완료 대기
    RedisConnectAwaiter conn_aw{fd};
    if (!conn_aw.await_ready()) {
        if (!co_await conn_aw) {
            ::close(fd);
            co_return unexpected(make_error(RedisError::ConnectionFailed));
        }
    }

    auto impl = std::make_unique<RedisClientImpl>(fd, parsed);
    auto client = std::make_unique<RedisClient>(std::move(impl));

    // AUTH (비밀번호가 있을 때)
    if (!parsed.password.empty()) {
        std::vector<std::string> auth_args;
        if (!parsed.username.empty()) {
            auth_args = {"AUTH", parsed.username, parsed.password};
        } else {
            auth_args = {"AUTH", parsed.password};
        }
        auto res = co_await client->impl_->send(std::move(auth_args));
        if (!res || !(*res).is_ok())
            co_return unexpected(make_error(RedisError::AuthFailed));
    }

    // SELECT db
    if (parsed.db > 0) {
        auto res = co_await client->impl_->send({"SELECT", std::to_string(parsed.db)});
        if (!res)
            co_return unexpected(res.error());
    }

    co_return client;
}

// ── 헬퍼 매크로 대체 ─────────────────────────────────────────────────────────

#define RC_SEND(...) co_await impl_->send({__VA_ARGS__})
#define RC_CHECK(r)  if (!(r)) co_return unexpected((r).error())

// ── 명령 구현 ─────────────────────────────────────────────────────────────────

Task<Result<RedisValue>> RedisClient::ping() {
    auto r = RC_SEND("PING");
    RC_CHECK(r);
    co_return *r;
}

Task<Result<RedisValue>> RedisClient::get(std::string_view key) {
    auto r = RC_SEND("GET", std::string(key));
    RC_CHECK(r);
    co_return *r;
}

Task<Result<RedisValue>> RedisClient::set(std::string_view key,
                                           std::string_view value,
                                           std::optional<int64_t> ttl_sec) {
    std::vector<std::string> args = {"SET", std::string(key), std::string(value)};
    if (ttl_sec) {
        args.push_back("EX");
        args.push_back(std::to_string(*ttl_sec));
    }
    auto r = co_await impl_->send(std::move(args));
    RC_CHECK(r);
    co_return *r;
}

Task<Result<bool>> RedisClient::setnx(std::string_view key, std::string_view value) {
    auto r = RC_SEND("SETNX", std::string(key), std::string(value));
    RC_CHECK(r);
    co_return (*r).as_int() == 1;
}

Task<Result<int64_t>> RedisClient::del(std::string_view key) {
    auto r = RC_SEND("DEL", std::string(key));
    RC_CHECK(r);
    co_return (*r).as_int();
}

Task<Result<bool>> RedisClient::exists(std::string_view key) {
    auto r = RC_SEND("EXISTS", std::string(key));
    RC_CHECK(r);
    co_return (*r).as_int() > 0;
}

Task<Result<bool>> RedisClient::expire(std::string_view key, int64_t seconds) {
    auto r = RC_SEND("EXPIRE", std::string(key), std::to_string(seconds));
    RC_CHECK(r);
    co_return (*r).as_int() == 1;
}

Task<Result<int64_t>> RedisClient::ttl(std::string_view key) {
    auto r = RC_SEND("TTL", std::string(key));
    RC_CHECK(r);
    co_return (*r).as_int();
}

Task<Result<int64_t>> RedisClient::incr(std::string_view key) {
    auto r = RC_SEND("INCR", std::string(key));
    RC_CHECK(r);
    co_return (*r).as_int();
}

Task<Result<int64_t>> RedisClient::incrby(std::string_view key, int64_t delta) {
    auto r = RC_SEND("INCRBY", std::string(key), std::to_string(delta));
    RC_CHECK(r);
    co_return (*r).as_int();
}

Task<Result<int64_t>> RedisClient::decr(std::string_view key) {
    auto r = RC_SEND("DECR", std::string(key));
    RC_CHECK(r);
    co_return (*r).as_int();
}

Task<Result<int64_t>> RedisClient::decrby(std::string_view key, int64_t delta) {
    auto r = RC_SEND("DECRBY", std::string(key), std::to_string(delta));
    RC_CHECK(r);
    co_return (*r).as_int();
}

Task<Result<std::vector<std::optional<std::string>>>>
RedisClient::mget(std::vector<std::string> keys) {
    std::vector<std::string> args = {"MGET"};
    for (auto& k : keys) args.push_back(std::move(k));
    auto r = co_await impl_->send(std::move(args));
    RC_CHECK(r);
    std::vector<std::optional<std::string>> result;
    for (auto& v : (*r).as_array()) {
        if (v.is_null()) result.push_back(std::nullopt);
        else             result.push_back(v.str);
    }
    co_return result;
}

// ── 해시 ─────────────────────────────────────────────────────────────────────

Task<Result<std::optional<std::string>>>
RedisClient::hget(std::string_view key, std::string_view field) {
    auto r = RC_SEND("HGET", std::string(key), std::string(field));
    RC_CHECK(r);
    if ((*r).is_null()) co_return std::nullopt;
    co_return (*r).str;
}

Task<Result<bool>>
RedisClient::hset(std::string_view key, std::string_view field, std::string_view value) {
    auto r = RC_SEND("HSET", std::string(key), std::string(field), std::string(value));
    RC_CHECK(r);
    co_return true;
}

Task<Result<bool>>
RedisClient::hsetnx(std::string_view key, std::string_view field, std::string_view value) {
    auto r = RC_SEND("HSETNX", std::string(key), std::string(field), std::string(value));
    RC_CHECK(r);
    co_return (*r).as_int() == 1;
}

Task<Result<int64_t>>
RedisClient::hdel(std::string_view key, std::string_view field) {
    auto r = RC_SEND("HDEL", std::string(key), std::string(field));
    RC_CHECK(r);
    co_return (*r).as_int();
}

Task<Result<bool>>
RedisClient::hexists(std::string_view key, std::string_view field) {
    auto r = RC_SEND("HEXISTS", std::string(key), std::string(field));
    RC_CHECK(r);
    co_return (*r).as_int() == 1;
}

Task<Result<std::vector<std::pair<std::string, std::string>>>>
RedisClient::hgetall(std::string_view key) {
    auto r = RC_SEND("HGETALL", std::string(key));
    RC_CHECK(r);
    std::vector<std::pair<std::string, std::string>> result;
    const auto& arr = (*r).as_array();
    for (std::size_t i = 0; i + 1 < arr.size(); i += 2)
        result.emplace_back(arr[i].str, arr[i + 1].str);
    co_return result;
}

Task<Result<int64_t>> RedisClient::hlen(std::string_view key) {
    auto r = RC_SEND("HLEN", std::string(key));
    RC_CHECK(r);
    co_return (*r).as_int();
}

// ── 리스트 ───────────────────────────────────────────────────────────────────

Task<Result<int64_t>> RedisClient::lpush(std::string_view key, std::string_view value) {
    auto r = RC_SEND("LPUSH", std::string(key), std::string(value));
    RC_CHECK(r);
    co_return (*r).as_int();
}

Task<Result<int64_t>> RedisClient::rpush(std::string_view key, std::string_view value) {
    auto r = RC_SEND("RPUSH", std::string(key), std::string(value));
    RC_CHECK(r);
    co_return (*r).as_int();
}

Task<Result<std::optional<std::string>>> RedisClient::lpop(std::string_view key) {
    auto r = RC_SEND("LPOP", std::string(key));
    RC_CHECK(r);
    if ((*r).is_null()) co_return std::nullopt;
    co_return (*r).str;
}

Task<Result<std::optional<std::string>>> RedisClient::rpop(std::string_view key) {
    auto r = RC_SEND("RPOP", std::string(key));
    RC_CHECK(r);
    if ((*r).is_null()) co_return std::nullopt;
    co_return (*r).str;
}

Task<Result<std::vector<std::string>>>
RedisClient::lrange(std::string_view key, int64_t start, int64_t stop) {
    auto r = RC_SEND("LRANGE", std::string(key),
                      std::to_string(start), std::to_string(stop));
    RC_CHECK(r);
    std::vector<std::string> result;
    for (auto& v : (*r).as_array()) result.push_back(v.str);
    co_return result;
}

Task<Result<int64_t>> RedisClient::llen(std::string_view key) {
    auto r = RC_SEND("LLEN", std::string(key));
    RC_CHECK(r);
    co_return (*r).as_int();
}

// ── 셋 ───────────────────────────────────────────────────────────────────────

Task<Result<int64_t>> RedisClient::sadd(std::string_view key, std::string_view member) {
    auto r = RC_SEND("SADD", std::string(key), std::string(member));
    RC_CHECK(r);
    co_return (*r).as_int();
}

Task<Result<int64_t>> RedisClient::srem(std::string_view key, std::string_view member) {
    auto r = RC_SEND("SREM", std::string(key), std::string(member));
    RC_CHECK(r);
    co_return (*r).as_int();
}

Task<Result<std::vector<std::string>>> RedisClient::smembers(std::string_view key) {
    auto r = RC_SEND("SMEMBERS", std::string(key));
    RC_CHECK(r);
    std::vector<std::string> result;
    for (auto& v : (*r).as_array()) result.push_back(v.str);
    co_return result;
}

Task<Result<int64_t>> RedisClient::scard(std::string_view key) {
    auto r = RC_SEND("SCARD", std::string(key));
    RC_CHECK(r);
    co_return (*r).as_int();
}

Task<Result<bool>> RedisClient::sismember(std::string_view key, std::string_view member) {
    auto r = RC_SEND("SISMEMBER", std::string(key), std::string(member));
    RC_CHECK(r);
    co_return (*r).as_int() == 1;
}

// ── 정렬 셋 ──────────────────────────────────────────────────────────────────

Task<Result<int64_t>>
RedisClient::zadd(std::string_view key, double score, std::string_view member) {
    auto r = RC_SEND("ZADD", std::string(key), std::to_string(score), std::string(member));
    RC_CHECK(r);
    co_return (*r).as_int();
}

Task<Result<int64_t>> RedisClient::zrem(std::string_view key, std::string_view member) {
    auto r = RC_SEND("ZREM", std::string(key), std::string(member));
    RC_CHECK(r);
    co_return (*r).as_int();
}

Task<Result<std::vector<std::string>>>
RedisClient::zrange(std::string_view key, int64_t start, int64_t stop) {
    auto r = RC_SEND("ZRANGE", std::string(key),
                      std::to_string(start), std::to_string(stop));
    RC_CHECK(r);
    std::vector<std::string> result;
    for (auto& v : (*r).as_array()) result.push_back(v.str);
    co_return result;
}

Task<Result<std::optional<double>>>
RedisClient::zscore(std::string_view key, std::string_view member) {
    auto r = RC_SEND("ZSCORE", std::string(key), std::string(member));
    RC_CHECK(r);
    if ((*r).is_null()) co_return std::nullopt;
    try {
        co_return std::stod((*r).str);
    } catch (...) {
        co_return std::nullopt;
    }
}

Task<Result<int64_t>> RedisClient::zcard(std::string_view key) {
    auto r = RC_SEND("ZCARD", std::string(key));
    RC_CHECK(r);
    co_return (*r).as_int();
}

Task<Result<std::optional<int64_t>>>
RedisClient::zrank(std::string_view key, std::string_view member) {
    auto r = RC_SEND("ZRANK", std::string(key), std::string(member));
    RC_CHECK(r);
    if ((*r).is_null()) co_return std::nullopt;
    co_return (*r).as_int();
}

// ── 기타 ─────────────────────────────────────────────────────────────────────

Task<Result<std::string>> RedisClient::type(std::string_view key) {
    auto r = RC_SEND("TYPE", std::string(key));
    RC_CHECK(r);
    co_return (*r).str;
}

Task<Result<std::vector<std::string>>> RedisClient::keys(std::string_view pattern) {
    auto r = RC_SEND("KEYS", std::string(pattern));
    RC_CHECK(r);
    std::vector<std::string> result;
    for (auto& v : (*r).as_array()) result.push_back(v.str);
    co_return result;
}

Task<Result<RedisValue>> RedisClient::info() {
    auto r = RC_SEND("INFO");
    RC_CHECK(r);
    co_return *r;
}

Task<Result<RedisValue>> RedisClient::flushdb() {
    auto r = RC_SEND("FLUSHDB");
    RC_CHECK(r);
    co_return *r;
}

Task<Result<RedisValue>> RedisClient::command(std::vector<std::string> args) {
    auto r = co_await impl_->send(std::move(args));
    RC_CHECK(r);
    co_return *r;
}

#undef RC_SEND
#undef RC_CHECK

} // namespace qbuem_routine::redis
