#pragma once

/**
 * @file db/resp_parser.hpp
 * @brief RESP2 wire-protocol parser for the Redis client (extracted for testing).
 *
 * The server response is treated as UNTRUSTED — a malicious or buggy server, or
 * a MITM, can send arbitrary bytes.  The parser is bounded against the classic
 * RESP attack classes:
 *   - nesting depth cap                  → no stack overflow from nested arrays
 *   - bulk-length / array-count caps      → no over-reserve / memory blow-up
 *   - overflow-safe length arithmetic     → a huge $len cannot wrap the bounds check
 *   - total buffer cap                    → no unbounded growth (slow-loris / giant reply)
 *   - strict numeric + trailing-CRLF check → malformed framing becomes a protocol
 *                                            error, not a silent mis-parse / desync
 *
 * On a protocol violation a sticky error flag is raised; parse() then drops the
 * poisoned buffer and returns a RedisValue of type Error so the client surfaces
 * a DbError instead of crashing or returning a wrong value.
 */

#include "redis_client.hpp" // RedisValue

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <string>

namespace qbuem_routine::redis {

class RespParser {
public:
    // RESP framing limits — generous (well above any real reply) yet far below
    // the values an attacker would use to exhaust memory or the stack.
    static constexpr int64_t kMaxBulkLen    = 512LL * 1024 * 1024;  ///< 512 MiB bulk
    static constexpr int64_t kMaxArrayCount = 16LL * 1024 * 1024;   ///< 16M elements
    static constexpr int     kMaxDepth      = 64;                   ///< nesting cap
    static constexpr size_t  kMaxBuffer     = 512ULL * 1024 * 1024; ///< total buffer

    /// Append received bytes, never growing past kMaxBuffer.
    void feed(const char* data, size_t len) {
        if (buf_.size() >= kMaxBuffer) return;
        buf_.append(data, std::min(len, kMaxBuffer - buf_.size()));
    }

    /// True when a full response is buffered OR the stream is malformed (in which
    /// case parse() will surface the protocol error rather than block forever).
    [[nodiscard]] bool has_complete() const {
        fatal_ = false;
        size_t pos = 0;
        const bool complete = scan(pos, 0);
        if (!complete && !fatal_ && buf_.size() >= kMaxBuffer)
            fatal_ = true; // buffer full but still no complete value
        return complete || fatal_;
    }

    /// Parse and consume one response.  has_complete() must have returned true.
    /// parse_value() is the single source of truth: it sets fatal_ on any framing
    /// violation (bad length / count / depth / trailing-CRLF), and on fatal_ we
    /// drop the poisoned buffer and surface an Error value.
    [[nodiscard]] RedisValue parse() {
        fatal_ = false;
        size_t pos = 0;
        RedisValue val = parse_value(pos, 0);
        if (fatal_ || pos > buf_.size()) {
            buf_.clear(); // resync: discard the poisoned stream
            RedisValue v;
            v.type = RedisValue::Type::Error;
            v.str  = "RESP protocol error";
            return v;
        }
        if (pos >= buf_.size()) buf_.clear();
        else                    buf_.erase(0, pos);
        return val;
    }

    [[nodiscard]] bool empty() const noexcept { return buf_.empty(); }
    [[nodiscard]] bool protocol_error() const noexcept { return fatal_; }

private:
    std::string  buf_;
    mutable bool fatal_ = false;

    // Returns true if a complete value starts at `pos` (advancing pos past it),
    // false if more bytes are needed.  Sets fatal_ on a protocol violation — the
    // caller treats fatal_ as "ready, but malformed".
    [[nodiscard]] bool scan(size_t& pos, int depth) const {
        if (depth > kMaxDepth) { fatal_ = true; return true; }
        if (pos >= buf_.size()) return false;
        switch (buf_[pos]) {
            case '+': case '-': case ':': {
                const auto end = buf_.find("\r\n", pos + 1);
                if (end == std::string::npos) return false;
                if (buf_[pos] == ':' && !valid_int(pos + 1, end)) { fatal_ = true; return true; }
                pos = end + 2;
                return true;
            }
            case '$': {
                const auto end = buf_.find("\r\n", pos + 1);
                if (end == std::string::npos) return false;
                int64_t len;
                if (!parse_len(pos + 1, end, len)) { fatal_ = true; return true; }
                if (len < 0) { pos = end + 2; return true; }       // null bulk
                if (len > kMaxBulkLen) { fatal_ = true; return true; }
                const size_t need = end + 2 + static_cast<size_t>(len) + 2; // len capped → no wrap
                if (need > buf_.size()) return false;
                pos = need;
                return true;
            }
            case '*': {
                const auto end = buf_.find("\r\n", pos + 1);
                if (end == std::string::npos) return false;
                int64_t count;
                if (!parse_len(pos + 1, end, count)) { fatal_ = true; return true; }
                pos = end + 2;
                if (count < 0) return true;                         // null array
                if (count > kMaxArrayCount) { fatal_ = true; return true; }
                for (int64_t i = 0; i < count; ++i) {
                    if (!scan(pos, depth + 1)) return false;
                    if (fatal_) return true;
                }
                return true;
            }
            default: fatal_ = true; return true;                    // unknown prefix byte
        }
    }

    RedisValue parse_value(size_t& pos, int depth) {
        if (depth > kMaxDepth || pos >= buf_.size()) { fatal_ = true; return {}; }
        const char prefix = buf_[pos++];
        switch (prefix) {
            case '+': case '-': {
                const auto end = buf_.find("\r\n", pos);
                if (end == std::string::npos) { fatal_ = true; return {}; }
                RedisValue v;
                v.type = (prefix == '+') ? RedisValue::Type::String : RedisValue::Type::Error;
                v.str  = buf_.substr(pos, end - pos);
                pos = end + 2;
                return v;
            }
            case ':': {
                const auto end = buf_.find("\r\n", pos);
                if (end == std::string::npos) { fatal_ = true; return {}; }
                RedisValue v; v.type = RedisValue::Type::Integer;
                const auto [ptr, ec] =
                    std::from_chars(buf_.data() + pos, buf_.data() + end, v.integer);
                if (ec != std::errc{} || ptr != buf_.data() + end) { fatal_ = true; return {}; }
                pos = end + 2;
                return v;
            }
            case '$': {
                const auto end = buf_.find("\r\n", pos);
                if (end == std::string::npos) { fatal_ = true; return {}; }
                int64_t len;
                if (!parse_len(pos, end, len)) { fatal_ = true; return {}; }
                pos = end + 2;
                if (len < 0) return {};                             // null bulk
                if (len > kMaxBulkLen ||
                    pos + static_cast<size_t>(len) + 2 > buf_.size()) { fatal_ = true; return {}; }
                if (buf_[pos + len] != '\r' || buf_[pos + len + 1] != '\n') { fatal_ = true; return {}; }
                RedisValue v; v.type = RedisValue::Type::String;
                v.str = buf_.substr(pos, static_cast<size_t>(len));
                pos += static_cast<size_t>(len) + 2;
                return v;
            }
            case '*': {
                const auto end = buf_.find("\r\n", pos);
                if (end == std::string::npos) { fatal_ = true; return {}; }
                int64_t count;
                if (!parse_len(pos, end, count)) { fatal_ = true; return {}; }
                pos = end + 2;
                if (count < 0) return {};                           // null array
                if (count > kMaxArrayCount) { fatal_ = true; return {}; }
                RedisValue v; v.type = RedisValue::Type::Array;
                v.array.reserve(static_cast<size_t>(std::min<int64_t>(count, 4096)));
                for (int64_t i = 0; i < count; ++i) {
                    v.array.push_back(parse_value(pos, depth + 1));
                    if (fatal_) return v;
                }
                return v;
            }
            default: fatal_ = true; return {};
        }
    }

    // Parse a signed integer field in [a, b); require the WHOLE field to be a
    // valid number (rejects "$10x", "$abc", "$", trailing garbage).
    [[nodiscard]] bool parse_len(size_t a, size_t b, int64_t& out) const {
        out = 0;
        const auto [ptr, ec] = std::from_chars(buf_.data() + a, buf_.data() + b, out);
        return ec == std::errc{} && ptr == buf_.data() + b && b > a;
    }
    [[nodiscard]] bool valid_int(size_t a, size_t b) const {
        int64_t tmp;
        return parse_len(a, b, tmp);
    }
};

} // namespace qbuem_routine::redis
