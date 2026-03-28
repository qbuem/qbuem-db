# CLAUDE.md — qbuem-db AI Context

This file provides structured context for AI coding assistants working in this repository.

---

## Language Policy

**All code, comments, documentation, and user-facing strings MUST be written in English.**

Korean or other non-English text in code comments, docs, or strings is a review failure.
Existing Korean comments in legacy files should be translated to English when touched.

---

## Project Overview

C++23 DB driver library built on qbuem-stack.
Async ORM + multiple DB drivers + migration framework.

## Code Structure

```
src/db/
├── orm.hpp                    # Header-only ORM (C++23 Concepts-based)
├── postgresql_driver.hpp/cpp  # Async PostgreSQL (libpq)
├── sqlite3_driver.hpp/cpp     # SQLite3 (development / test)
├── mysql_driver.hpp/cpp       # MySQL/MariaDB (cloud SQL)
├── redis_client.hpp/cpp       # Async Redis (RESP2, no external deps)
└── migration/
    └── migrator.hpp           # Driver-independent migration framework
```

---

## Core Interfaces (provided by qbuem-stack)

- `IDBDriver`       — driver factory (`pool()`)
- `IConnectionPool` — connection pool (`acquire()`, `return_connection()`)
- `IConnection`     — connection (`query()`, `prepare()`, `begin()`, `ping()`)
- `ITransaction`    — transaction (`commit()`, `rollback()`, `savepoint()`)
- `IStatement`      — prepared statement (`execute()`, `execute_dml()`)
- `IResultSet`      — result iterator (`next()`)
- `IRow`            — row access (`get(idx)`, `get(name)`)
- `Value`           — Null / Int64 / Float64 / Bool / Text / Blob
- `Result<T>`       — error propagation (`qbuem::unexpected()`)
- `Task<T>`         — coroutine return type

---

## Async Pattern (PostgreSQL / Redis)

All async drivers use the qbuem Reactor event loop:

```cpp
// Pattern: PQsend* → async_flush → PgReadAwaiter
static Task<PGresult*> async_params(PGconn* conn, ...) {
    PQsendQueryParams(...);
    co_await async_flush(conn);           // PgFlushAwaiter
    co_return co_await PgReadAwaiter{conn}; // wait for socket readable
}
```

Awaiter rules:
1. `await_ready()` — return true if immediately complete (no lock needed)
2. `await_suspend()` — register event with Reactor, then yield
3. `await_resume()` — return result

**Critical**: call `reactor->post()` to resume from event handler — never call `handle.resume()` directly from an event handler.

---

## ORM Usage

**Dialect must be set for MySQL/SQLite.** Default is PostgreSQL.

```cpp
// 1. Registration (once at app startup)
orm::register_table<User>("users")
    .pk("id", &User::id)
    .col("email", &User::email)
    .col("name", &User::name);

// MySQL / SQLite — must set dialect
orm::register_table<User>("users")
    .dialect(Dialect::MySQL)   // or Dialect::SQLite
    .pk("id", &User::id)
    .col("email", &User::email);

// 2. SQL generation
auto& m = orm::meta<User>();
m.sql_insert()                                  // PG: VALUES ($1,$2) RETURNING * / MySQL: VALUES (?,?)
m.sql_select_where("email")                     // WHERE email=$1 / ?
m.sql_select_where_in("id", ids.size())         // WHERE id IN ($1,$2,$3)
m.sql_select_where_null("bio")                  // WHERE bio IS NULL
m.sql_select_where_between("age")               // WHERE age BETWEEN $1 AND $2
m.sql_select_where_like("name")                 // WHERE name LIKE $1
m.sql_select_where_paged("email", 1, 2, 3)     // WHERE email=$1 LIMIT $2 OFFSET $3
m.sql_count_where("email")                      // SELECT COUNT(*) WHERE email=$1
m.sql_count_where_in("id", n)
m.sql_upsert_pk()                               // PG/SQLite: ON CONFLICT / MySQL: ON DUPLICATE KEY
m.sql_insert_batch(3)                           // INSERT VALUES ($1,…),($4,…),($7,…)

// 3. Parameter binding
auto params = m.bind_insert(user);              // non-PK values
auto params = m.bind_update(user);              // non-PK first, PK last
auto params = m.bind_batch({u1, u2, u3});       // flat params for batch INSERT
auto params = m.bind_paged(email, 20, 0);       // WHERE + LIMIT + OFFSET
auto params = m.bind_in(std::vector<int64_t>{1,2,3}); // IN clause

// 4. Result reading
User u = m.read_row(*row);          // column-name based
User u = m.read_row_indexed(*row);  // index-based (faster)
```

---

## Driver DSN Format

| Driver | DSN |
|--------|-----|
| PostgreSQL | `postgresql://user:pass@host:5432/db` |
| SQLite3 | `sqlite:///path/to/file.db` or `sqlite://:memory:` |
| MySQL | `mysql://user:pass@host:3306/db[?ssl=true]` |
| Redis | `redis://[:pass@]host[:port][/db]` |

---

## Migration

```cpp
#include "db/migration/migrator.hpp"
using namespace qbuem::db::migration;

static const std::vector<Migration> kMigrations = {
    { .version=1, .description="init schema",
      .up="CREATE TABLE ...",
      .down="DROP TABLE ..." },
};

// PlaceholderStyle::Dollar   → PostgreSQL ($1, $2)
// PlaceholderStyle::Question → MySQL/SQLite (?, ?)
MigrationRunner runner{kMigrations, *conn, PlaceholderStyle::Dollar};
co_await runner.migrate();           // apply all pending
co_await runner.migrate_to(3);       // apply up to version 3
co_await runner.rollback();          // roll back latest one
co_await runner.rollback_to(1);      // roll back all above version 1
auto status = co_await runner.status();
```

---

## Adding a New Driver — Checklist

1. `src/db/<name>_driver.hpp` — declare `make_<name>_driver()` factory
2. `src/db/<name>_driver.cpp` — implement `IDBDriver` / `IConnectionPool` / `IConnection` / `ITransaction` / `IStatement`
3. `CMakeLists.txt` — add `add_library` target + `qbuem-db::<name>_driver` alias
4. `README.md` — add driver table entry, DSN format, and usage example
5. `llms.txt` / `CLAUDE.md` — update

---

## Build

```bash
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_CXX_COMPILER=g++-13 \
      -B build
cmake --build build
```

**Options:**
- `-DQBUEM_DB_MYSQL=OFF` — disable MySQL driver
- `-DQBUEM_DB_REDIS=OFF` — disable Redis client

**Requirements:** GCC ≥ 13 / Clang ≥ 17, CMake ≥ 3.20, ninja-build, C compiler (for libpq source build), `libmysqlclient-dev` (optional, MySQL driver).

---

## Coding Style

- Use C++23 features aggressively (`std::byteswap`, `std::format`, Concepts)
- `[[nodiscard]]` on all public functions
- Errors: return `Result<T>`, minimize exceptions
- Sync drivers: `std::mutex` serialization (SQLite3, MySQL pattern)
- Async drivers: Reactor events + coroutine awaiters (PG, Redis pattern)
- Resume via `reactor->post()` from event handler — never call `handle.resume()` directly

---

## Performance Design

### Zero-Copy Parameter Binding

- **MySQL**: `exec_stmt()` sets `MYSQL_BIND.buffer` directly to the `Value`'s `string_view` / `BufferView` pointer (`const_cast<void*>`). No `str_bufs` vector allocation. The params span remains valid until `mysql_stmt_execute()` completes.
- **SQLite**: `bind_value()` uses `SQLITE_STATIC` — params span valid until `sqlite3_step()` completes. Zero allocs vs. `SQLITE_TRANSIENT`.
- **PostgreSQL**: `PgParams` struct references params as `string_view` array — zero-copy.

### Prepared Statement Caching

- **MySQL `MysqlStatement`**: caches `MYSQL_STMT*` for object lifetime. `mysql_stmt_reset()` before reuse — no re-prepare overhead.
- **SQLite `SqliteStatement`**: caches `sqlite3_stmt*` with `sqlite3_reset()` + `sqlite3_clear_bindings()`.
- **PostgreSQL**: named prepared statements (`PREPARE` / `EXECUTE`).

### TCP Latency

- **PostgreSQL / Redis**: `TCP_NODELAY` — immediate small packet transmission (Nagle disabled).
- **MySQL**: `MYSQL_OPT_TCP_KEEPIDLE` / `KEEPINTERVAL` / `KEEPCOUNT` — prevents cloud firewall idle drops.
- **Redis**: `SOCK_NONBLOCK` + Reactor — non-blocking event loop integration.

### SQLite Performance PRAGMAs

| PRAGMA | Value | Effect |
|--------|-------|--------|
| `journal_mode` | `WAL` | Allows concurrent reads |
| `synchronous` | `NORMAL` | Reduces fsync frequency |
| `cache_size` | `-65536` | 64MB page cache |
| `temp_store` | `MEMORY` | Temporary tables in memory |
| `mmap_size` | `268435456` | 256MB mmap I/O |
| `page_size` | `4096` | Page size for new databases |

---

## Common Mistakes

- `PQsend*` / MySQL send functions must be called **inside the lock**; `co_await` (flush, read) must happen **outside the lock**
- SQLite3 uses FULLMUTEX mode — beware of double-locking
- ORM `bind_batch()` requires **n matching** `sql_insert_batch(n)`
- **Missing ORM dialect**: MySQL/SQLite without `.dialect()` generates `$N` placeholders + `RETURNING *` → runtime SQL error
- **`sql_upsert_pk()` MySQL**: generates `ON DUPLICATE KEY UPDATE` not `ON CONFLICT` — dialect must be set correctly
- **`bind_in(ids)`**: empty `ids` produces `WHERE col IN ()` → SQL error; check `empty()` before calling
- **`sql_insert_batch(n)`**: asserts on n=0 or tables with no non-PK columns
- **`SUM(BIGINT)` returns NUMERIC (OID 1700)**: PostgreSQL `SUM(bigint)` returns `NUMERIC`, not `BIGINT`. The binary driver only handles `OID_INT8` — NUMERIC falls through to `default:` and returns garbage bytes interpreted as int64_t. Always cast: `SUM(col)::BIGINT` when the column is BIGINT. (`SUM(integer)` → BIGINT is fine without cast.)
- **ORM INSERT with TIMESTAMPTZ/DATE columns**: `to_value(std::string{""})` binds empty string as TEXT `''`. PostgreSQL rejects `''::timestamptz`. Use custom INSERT SQL with `NULLIF($N,'')::timestamptz` for nullable timestamp columns, and exclude `DEFAULT NOW()` columns entirely from the INSERT.
- **Binary DATE/TIME format**: PostgreSQL binary format for DATE=int32 days since 2000-01-01, TIME=int64 µsec since midnight, TIMESTAMP/TIMESTAMPTZ=int64 µsec since 2000-01-01 UTC. These are now decoded in `read_pg_column_binary` (OID 1082/1083/1114/1184) as formatted strings.

---

## Cross-Repo Guidelines

### Ecosystem Map

| Repo | Role | Key headers |
|------|------|-------------|
| **qbuem-stack** | Platform: async I/O, HTTP, pipelines, crypto, middleware | `<qbuem/http/*>`, `<qbuem/middleware/*>`, `<qbuem/crypto/*>` |
| **qbuem-auth** | Auth layer: JWT, HTTPS client, OAuth2 | `src/auth/jwt.hpp`, `src/auth/https_client.hpp`, `src/auth/oauth.hpp` |
| **qbuem-db** | DB layer: async drivers, ORM, migrations | `src/db/orm.hpp`, `src/db/*_driver.hpp` |
| **application repos** | WAS applications built on the above | depend on stack + auth + db; must not duplicate platform code |

### Filing Platform-Level Issues

When implementing a feature here requires functionality that belongs in a lower-level library,
**file an issue in the correct repo** rather than implementing it locally.

| Need | File issue at |
|------|--------------|
| New async primitive, protocol, or middleware | [qbuem-stack issues](https://github.com/qbuem/qbuem-stack/issues) |
| HTTPS client feature, JWT, OAuth provider | [qbuem-auth issues](https://github.com/qbuem/qbuem-auth/issues) |
| DB driver, ORM, migration feature | [qbuem-db issues](https://github.com/qbuem/qbuem-db/issues) |

If a temporary local workaround is necessary while waiting for an upstream fix, mark it:
```cpp
// TODO: remove after qbuem-db#NNN is merged
```

> **Cross-repo development workflow and documentation update policy:**
> See root workspace CLAUDE.md at `/Users/goodboy/Projects/qbuem/CLAUDE.md`.
