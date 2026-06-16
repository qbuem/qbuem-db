#include "redis_client.hpp"
#include "resp_parser.hpp"

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

// ── DSN parser ───────────────────────────────────────────────────────────────

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

// ── RESP2 encoder ────────────────────────────────────────────────────────────

static std::string encode_resp(const std::vector<std::string>& args) {
    std::string buf;
    // *<N>\r\n  +  per arg: $<len>\r\n<data>\r\n  ≈ 16 + 8*N + sum(sizes)
    std::size_t total = 16;
    for (const auto& a : args) total += 8 + a.size();
    buf.reserve(total);
    char tmp[24]; // to_chars stack buffer — avoids std::to_string temporary allocation
    buf += '*';
    auto [e1, _1] = std::to_chars(tmp, tmp + sizeof(tmp), args.size());
    buf.append(tmp, e1);
    buf += "\r\n";
    for (const auto& arg : args) {
        buf += '$';
        auto [e2, _2] = std::to_chars(tmp, tmp + sizeof(tmp), arg.size());
        buf.append(tmp, e2);
        buf += "\r\n";
        buf += arg;
        buf += "\r\n";
    }
    return buf;
}

// ── RESP2 parser ─────────────────────────────────────────────────────────────
// The hardened, bounds-checked RespParser lives in resp_parser.hpp (extracted so
// it can be unit-tested against crafted/untrusted server input without a socket).

// ── Reactor-based async awaiters ─────────────────────────────────────────────

struct RedisReadAwaiter {
    int          fd;
    RespParser*  parser;
    RedisValue   result;
    bool         error{false};

    bool await_ready() noexcept {
        // return immediately if a complete response is already buffered
        return parser->has_complete();
    }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        auto* reactor = Reactor::current();
        reactor->register_event(fd, EventType::Read,
            [this, h, reactor](int) mutable {
                char tmp[4096];
                const ssize_t n = ::read(fd, tmp, sizeof(tmp));
                if (n > 0) {
                    parser->feed(tmp, static_cast<size_t>(n));
                    if (!parser->has_complete()) return; // wait for the rest
                } else {
                    error = true;
                }
                // unregister_event destroys THIS lambda (the reactor owns it), so
                // copy everything still needed to stack locals first and touch no
                // captured state afterwards — otherwise it is a use-after-free.
                auto* r      = reactor;
                auto  handle = h;
                r->unregister_event(fd, EventType::Read);
                r->post([handle]() mutable { handle.resume(); });
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
        // attempt immediate write
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
                    // See RedisReadAwaiter: unregister_event frees this lambda, so
                    // copy what we need to locals before calling it.
                    auto* r      = reactor;
                    auto  handle = h;
                    r->unregister_event(fd, EventType::Write);
                    r->post([handle]() mutable { handle.resume(); });
                }
            });
    }

    bool await_resume() noexcept { return !error && sent == data.size(); }
};

// ── Connection helpers ────────────────────────────────────────────────────────

static int connect_tcp(const RedisDsn& dsn) {
    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    const auto port_str = std::to_string(dsn.port);
    if (::getaddrinfo(dsn.host.c_str(), port_str.c_str(), &hints, &res) != 0)
        return -1;

    int fd = -1;
    for (auto* rp = res; rp; rp = rp->ai_next) {
#ifdef __linux__
        fd = ::socket(rp->ai_family, rp->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                      rp->ai_protocol);
#else
        fd = ::socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd >= 0) {
            ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
            ::fcntl(fd, F_SETFD, ::fcntl(fd, F_GETFD) | FD_CLOEXEC);
        }
#endif
        if (fd < 0) continue;

        // TCP_NODELAY: minimize latency (Redis sends many small packets)
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
    bool error{false};

    // Always wait for the socket to become writable before proceeding.  A
    // non-blocking connect() returns EINPROGRESS, and getsockopt(SO_ERROR) reads 0
    // *while still connecting* — so a SO_ERROR fast-path would report "ready"
    // before the handshake completes.  Writing to a still-connecting socket then
    // fails (ENOTCONN on macOS/BSD), which surfaced as a spurious ConnectionFailed
    // on the very first command.  The writable event fires once connect() resolves
    // (immediately for an already-connected loopback socket), and await_suspend
    // verifies SO_ERROR at that point.
    bool await_ready() noexcept { return false; }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        auto* reactor = Reactor::current();
        reactor->register_event(fd, EventType::Write,
            [this, h, reactor](int) mutable {
                int err = 0;
                socklen_t len = sizeof(err);
                ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
                error = (err != 0);
                // See RedisReadAwaiter: unregister_event frees this lambda, so
                // copy what we need to locals before calling it.
                auto* r      = reactor;
                auto  handle = h;
                r->unregister_event(fd, EventType::Write);
                r->post([handle]() mutable { handle.resume(); });
            });
    }

    bool await_resume() noexcept { return !error; }
};

// ── RedisClientImpl ───────────────────────────────────────────────────────────

class RedisClientImpl {
public:
    RedisClientImpl(int fd, RedisDsn dsn) noexcept
        : fd_(fd), dsn_(std::move(dsn)) {}

    ~RedisClientImpl() { close_fd(); }

    RedisClientImpl(const RedisClientImpl&)            = delete;
    RedisClientImpl& operator=(const RedisClientImpl&) = delete;

    // On connection drop: reconnect and re-run AUTH + SELECT
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

    // On ConnectionFailed: reconnect then retry once
    Task<Result<RedisValue>> send(std::vector<std::string> args) {
        auto r = co_await send_once(args);
        if (r || r.error() != db_error(DbError::ConnectionFailed))
            co_return r;

        // retry after reconnect
        if (!co_await reconnect())
            co_return unexpected(db_error(DbError::ConnectionFailed));
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

        // write
        RedisWriteAwaiter write_aw{fd_, std::move(encoded)};
        if (!write_aw.await_ready()) {
            if (!co_await write_aw)
                co_return unexpected(db_error(DbError::ConnectionFailed));
        } else if (write_aw.error) {
            co_return unexpected(db_error(DbError::ConnectionFailed));
        }

        // read
        RedisReadAwaiter read_aw{fd_, &parser_};
        RedisValue val;
        if (read_aw.await_ready()) {
            val = parser_.parse();
        } else {
            val = co_await read_aw;
            if (read_aw.error)
                co_return unexpected(db_error(DbError::ConnectionFailed));
        }

        if (val.is_error())
            co_return unexpected(db_error(DbError::QueryFailed));

        co_return val;
    }

    int        fd_;
    RedisDsn   dsn_;
    RespParser parser_;
};

// ── RedisClient implementation ────────────────────────────────────────────────

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
        co_return unexpected(db_error(DbError::ConnectionFailed));

    // wait for async connect to complete
    RedisConnectAwaiter conn_aw{fd};
    if (!conn_aw.await_ready()) {
        if (!co_await conn_aw) {
            ::close(fd);
            co_return unexpected(db_error(DbError::ConnectionFailed));
        }
    }

    auto impl = std::make_unique<RedisClientImpl>(fd, parsed);
    auto client = std::make_unique<RedisClient>(std::move(impl));

    // AUTH (when a password is set)
    if (!parsed.password.empty()) {
        std::vector<std::string> auth_args;
        if (!parsed.username.empty()) {
            auth_args = {"AUTH", parsed.username, parsed.password};
        } else {
            auth_args = {"AUTH", parsed.password};
        }
        auto res = co_await client->impl_->send(std::move(auth_args));
        if (!res || !(*res).is_ok())
            co_return unexpected(db_error(DbError::ConnectionFailed));
    }

    // SELECT db
    if (parsed.db > 0) {
        auto res = co_await client->impl_->send({"SELECT", std::to_string(parsed.db)});
        if (!res)
            co_return unexpected(res.error());
    }

    co_return client;
}

// ── Helper macro shorthands ───────────────────────────────────────────────────

#define RC_SEND(...) co_await impl_->send({__VA_ARGS__})
#define RC_CHECK(r)  if (!(r)) co_return unexpected((r).error())

// ── Command implementations ───────────────────────────────────────────────────

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

// ── Hash commands ────────────────────────────────────────────────────────────

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

// ── List commands ────────────────────────────────────────────────────────────

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

// ── Set commands ─────────────────────────────────────────────────────────────

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

// ── Sorted set commands ───────────────────────────────────────────────────────

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

// ── Miscellaneous commands ────────────────────────────────────────────────────

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

// ── Transactions (MULTI / EXEC) ───────────────────────────────────────────────

Task<Result<RedisValue>> RedisClient::multi() {
    auto r = RC_SEND("MULTI");
    RC_CHECK(r);
    co_return *r;
}

Task<Result<RedisValue>> RedisClient::exec() {
    auto r = RC_SEND("EXEC");
    RC_CHECK(r);
    co_return *r;
}

Task<Result<RedisValue>> RedisClient::discard() {
    auto r = RC_SEND("DISCARD");
    RC_CHECK(r);
    co_return *r;
}

Task<Result<RedisValue>> RedisClient::watch(std::vector<std::string> keys) {
    std::vector<std::string> args;
    args.reserve(keys.size() + 1);
    args.emplace_back("WATCH");
    for (auto& k : keys) args.push_back(std::move(k));
    auto r = co_await impl_->send(std::move(args));
    RC_CHECK(r);
    co_return *r;
}

Task<Result<RedisValue>> RedisClient::unwatch() {
    auto r = RC_SEND("UNWATCH");
    RC_CHECK(r);
    co_return *r;
}

Task<Result<RedisValue>>
RedisClient::transaction(std::vector<std::vector<std::string>> commands) {
    auto begin = co_await impl_->send({"MULTI"});
    RC_CHECK(begin);
    if (!begin->is_ok())
        co_return unexpected(db_error(DbError::TransactionFailed));

    // Queue each command (the server replies "QUEUED" for each).
    for (auto& cmd : commands) {
        auto q = co_await impl_->send(cmd);
        if (!q) {
            // A command rejected at queue time (e.g. bad arity) aborts the block;
            // DISCARD to leave the connection in a clean state, then surface it.
            co_await impl_->send({"DISCARD"});
            co_return unexpected(q.error());
        }
    }

    // EXEC runs the queued commands atomically and returns an Array of results.
    auto result = co_await impl_->send({"EXEC"});
    RC_CHECK(result);
    co_return *result;
}

#undef RC_SEND
#undef RC_CHECK

} // namespace qbuem_routine::redis
