# qbuem-db

C++23 DB driver library built on qbuem-stack.
PostgreSQL · MySQL/MariaDB (async / cloud) · SQLite3 · Redis drivers,
a template-based ORM, and a driver-independent migration framework.

## Components

| Target | File | Description |
|--------|------|-------------|
| `qbuem-db::orm` | `src/db/orm.hpp` | Header-only generic ORM — SQL generation + batch/paged/count + result mapping |
| `qbuem-db::migration` | `src/db/migration/migrator.hpp` | Header-only migration framework |
| `qbuem-db::postgresql_driver` | `src/db/postgresql_driver.{hpp,cpp}` | libpq-based **async** PostgreSQL driver |
| `qbuem-db::mysql_driver` | `src/db/mysql_driver.{hpp,cpp}` | libmysqlclient-based MySQL/MariaDB driver |
| `qbuem-db::sqlite3_driver` | `src/db/sqlite3_driver.{hpp,cpp}` | SQLite3 amalgamation driver (development / test) |
| `qbuem-db::redis_client` | `src/db/redis_client.{hpp,cpp}` | **Async** Redis client (RESP2, no external deps) |

## Build Dependencies

| Dependency | Management | Purpose |
|------------|-----------|---------|
| qbuem-stack | `FetchContent` (GIT) | Interfaces, Reactor, Task |
| SQLite3 amalgamation 3.49.1 | `FetchContent` (ZIP) | SQLite3 driver |
| libpq (PostgreSQL 17.2) | `ExternalProject_Add` (source build) | PostgreSQL driver |
| libmysqlclient (system) | `find_library` | MySQL driver (optional) |

> MySQL driver: `apt install libmysqlclient-dev` (or `dnf install mysql-devel`)
> PostgreSQL / SQLite3 / Redis: no external system libraries required

## Usage (FetchContent)

```cmake
FetchContent_Declare(
    qbuem_db
    GIT_REPOSITORY https://github.com/qbuem/qbuem-db.git
    GIT_TAG        main
)
FetchContent_MakeAvailable(qbuem_db)

target_link_libraries(your_target PRIVATE
    qbuem-db::orm
    qbuem-db::migration
    qbuem-db::postgresql_driver   # production (async)
    qbuem-db::mysql_driver        # MySQL/MariaDB cloud
    qbuem-db::sqlite3_driver      # development / test
    qbuem-db::redis_client        # cache / session
)
```

---

## ORM (`src/db/orm.hpp`)

Header-only ORM based on C++23 Concepts. **Compatible with all drivers** — Dialect setting automatically adjusts placeholders, RETURNING, and UPSERT syntax.

### Dialect Configuration

| Dialect | Placeholder | RETURNING | UPSERT |
|---------|------------|-----------|--------|
| `Dialect::PostgreSQL` (default) | `$1, $2, …` | included | `ON CONFLICT … EXCLUDED` |
| `Dialect::MySQL` | `?` | excluded | `ON DUPLICATE KEY UPDATE VALUES(col)` |
| `Dialect::SQLite` | `?` | excluded | `ON CONFLICT … EXCLUDED` |

```cpp
// For MySQL/SQLite — set dialect explicitly
orm::register_table<User>("users")
    .dialect(Dialect::MySQL)   // or Dialect::SQLite
    .pk("id",    &User::id)
    .col("email", &User::email)
    .col("name",  &User::name);
// → sql_insert() = "INSERT INTO users(email,name) VALUES (?,?)"  (no RETURNING)
// → sql_upsert_pk() = "INSERT … ON DUPLICATE KEY UPDATE email=VALUES(email),…"
```

### Supported Field Types

| C++ Type | DB Type | Concept |
|---------|---------|---------|
| `int16_t`, `int32_t`, `int64_t`, `uint32_t` | INTEGER | `OrmInt` |
| `double`, `float` | REAL | `OrmFloat` |
| `std::string` | TEXT | `OrmText` |
| `bool` | BOOLEAN | `OrmBool` |
| `std::optional<T>` | NULL-able | `OrmOptional` |

### SQL Generation Methods

> SQL examples below use `Dialect::PostgreSQL`. MySQL/SQLite automatically converts `$N` → `?`.

| Method | Generated SQL (PostgreSQL) |
|--------|---------------------------|
| `sql_select_all()` | `SELECT id,… FROM users` |
| `sql_select_where("email")` | `SELECT … WHERE email=$1` |
| `sql_select_ordered("email","id")` | `SELECT … WHERE email=$1 ORDER BY id ASC` |
| `sql_select_all_ordered("id", Desc)` | `SELECT … ORDER BY id DESC` |
| `sql_select_raw_where("id > 100")` | `SELECT … WHERE id > 100` |
| `sql_select_where_in("id", 3)` | `SELECT … WHERE id IN ($1,$2,$3)` |
| `sql_select_where_null("bio")` | `SELECT … WHERE bio IS NULL` |
| `sql_select_where_not_null("bio")` | `SELECT … WHERE bio IS NOT NULL` |
| `sql_select_where_between("age")` | `SELECT … WHERE age BETWEEN $1 AND $2` |
| `sql_select_where_like("name")` | `SELECT … WHERE name LIKE $1` |
| `sql_select_paged()` | `SELECT … LIMIT $1 OFFSET $2` |
| `sql_select_where_paged("email")` | `SELECT … WHERE email=$1 LIMIT $2 OFFSET $3` |
| `sql_select_all_ordered_paged("id")` | `SELECT … ORDER BY id ASC LIMIT $1 OFFSET $2` |
| `sql_count()` | `SELECT COUNT(*) FROM users` |
| `sql_count_where("email")` | `SELECT COUNT(*) … WHERE email=$1` |
| `sql_count_raw_where("id > 100")` | `SELECT COUNT(*) … WHERE id > 100` |
| `sql_count_where_in("id", 3)` | `SELECT COUNT(*) … WHERE id IN ($1,$2,$3)` |
| `sql_select_where2("email","name")` | `SELECT … WHERE email=$1 AND name=$2` |
| `sql_select_where3("a","b","c")` | `SELECT … WHERE a=$1 AND b=$2 AND c=$3` |
| `sql_insert()` | `INSERT INTO users(email,name,bio) VALUES ($1,$2,$3) RETURNING *` |
| `sql_insert_batch(3)` | `INSERT … VALUES ($1,$2,$3),($4,$5,$6),($7,$8,$9) RETURNING *` |
| `sql_upsert_pk()` | `INSERT … ON CONFLICT (id) DO UPDATE SET … RETURNING *` |
| `sql_update_pk()` | `UPDATE users SET email=$1,… WHERE id=$N RETURNING *` |
| `sql_update_col_pk("name")` | `UPDATE users SET name=$1 WHERE id=$2 RETURNING *` |
| `sql_delete_pk()` | `DELETE FROM users WHERE id=$1` |
| `sql_delete_where("email")` | `DELETE FROM users WHERE email=$1` |
| `sql_delete_where2("email","name")` | `DELETE … WHERE email=$1 AND name=$2` |

### Parameter Binding

```cpp
auto& m = orm::meta<User>();

// Single INSERT
auto params = m.bind_insert(user);               // non-PK values in order

// UPDATE (non-PK first, PK last)
auto params = m.bind_update(user);

// Batch INSERT (3 rows)
auto params = m.bind_batch(std::span{users, 3}); // pair with sql_insert_batch(3)

// WHERE condition
auto params = m.bind_val(email);                 // 1 value
auto params = m.bind_val2(email, name);          // 2 values
auto params = m.bind_val3(a, b, c);              // 3 values

// WHERE + LIMIT + OFFSET (pagination)
auto params = m.bind_paged(email, 20, 0);        // email=$1, limit=20, offset=0

// IN clause — accepts any range (vector, span, initializer_list, etc.)
std::vector<int64_t> ids = {1, 2, 3};
auto params = m.bind_in(ids);
auto params = m.bind_in(std::vector<std::string>{"a@b.com", "c@d.com"});
```

### Reading Results

```cpp
while (auto* row = co_await rs->next()) {
    User u = m.read_row(*row);         // column-name based (safe)
    User u = m.read_row_indexed(*row); // index-based (faster)
}
```

### Full Driver Usage Example

```cpp
// PostgreSQL (default — RETURNING gives inserted row immediately)
auto& m = orm::meta<User>();
auto rs = co_await conn->query(m.sql_insert(), m.bind_insert(user));
User inserted = m.read_row(*co_await (*rs)->next());

// MySQL (no RETURNING — obtain PK via last_insert_id)
auto& m = orm::meta<User>();  // registered with dialect(Dialect::MySQL)
auto rs = co_await conn->query(m.sql_insert(), m.bind_insert(user));
user.id = static_cast<int64_t>((*rs)->last_insert_id());

// IN query (same for all drivers)
std::vector<int64_t> ids = {10, 20, 30};
auto sql    = m.sql_select_where_in("id", ids.size());
auto params = m.bind_in(ids);
auto rs2    = co_await conn->query(sql, params);
```

---

## PostgreSQL Driver (`postgresql_driver`)

### Async Architecture

```
PgDriver (IDBDriver)
  └─ PgConnectionPool (IConnectionPool)
       ├─ Slot[]          — PGconn + mutex + stmt_counter
       ├─ PgConnection    (IConnection)   — acquire/release pair
       ├─ PgTransaction   (ITransaction)  — BEGIN/COMMIT/ROLLBACK/SAVEPOINT
       └─ PgStatement     (IStatement)    — PQsendQueryPrepared
```

```
PQsendQueryParams()  ─→  PgFlushAwaiter  ─→  PgReadAwaiter
    (non-blocking send)   (wait socket writable)  (socket readable + result complete)
```

### DSN

```
postgresql://user:pass@host:5432/dbname
postgresql://host/dbname
postgresql:///dbname    — Unix socket
```

### Binary Protocol OID Mapping

| PG OID | C++ Type | Value Type |
|--------|---------|-----------|
| 16 (bool) | `bool` | `Value::Bool` |
| 20 (int8) | `int64_t` | `Value::Int64` |
| 21 (int2) | `int16_t` | `Value::Int64` |
| 23 (int4) | `int32_t` | `Value::Int64` |
| 700 (float4) | `float` | `Value::Float64` |
| 701 (float8) | `double` | `Value::Float64` |
| 17 (bytea) | blob | `Value::Blob` |
| others | text | `Value::Text` |

---

## MySQL/MariaDB Driver (`mysql_driver`)

### Cloud Compatibility

| Service | Notes |
|---------|-------|
| Amazon RDS for MySQL | Fully compatible |
| Amazon Aurora MySQL | Fully compatible |
| Google Cloud SQL (MySQL) | Fully compatible |
| Azure Database for MySQL | Fully compatible |
| PlanetScale | MySQL-compatible |
| TiDB Cloud | MySQL-compatible |
| MariaDB SkySQL | MariaDB DSN |

### DSN

```
mysql://user:pass@host:3306/dbname
mysql://user:pass@host/dbname      — default port 3306
mysql://user:pass@host/db?ssl=true — force TLS
mariadb://user:pass@host/dbname    — MariaDB
```

### Features

- `$N` → `?` placeholder auto-conversion (ORM-compatible)
- UTF8MB4 default charset
- Multi-connection pool (respects PoolConfig.max_size)
- Prepared statements (MYSQL_STMT)
- Transactions / savepoints
- TLS/SSL options

```cpp
#include "db/mysql_driver.hpp"

auto driver = qbuem_routine::make_mysql_driver();
auto pool_r = co_await driver->pool(
    "mysql://user:pass@db.example.com/mydb",
    { .min_size=2, .max_size=10 });
// Same IConnection interface as PostgreSQL from here
```

---

## SQLite3 Driver (`sqlite3_driver`)

For development and test environments only. Same interface as PostgreSQL / MySQL.

```
sqlite:///path/to/file.db   — file database
sqlite://:memory:           — in-memory (tests)
```

- SQLite3 amalgamation compiled directly (no system library required)
- WAL mode + `synchronous=NORMAL` + `foreign_keys=ON`

---

## Redis Client (`redis_client`)

Pure POSIX sockets + RESP2 protocol. No external dependencies.
Fully async, integrated with the qbuem Reactor.

### Cloud Compatibility

| Service | Notes |
|---------|-------|
| Amazon ElastiCache (Redis OSS) | Fully compatible |
| Upstash (Serverless Redis) | Fully compatible |
| Redis Cloud | Fully compatible |
| Google Cloud Memorystore | Fully compatible |
| Azure Cache for Redis | Fully compatible |

### DSN

```
redis://host:6379
redis://:password@host:6379
redis://user:password@host:6379/2   — select DB 2
```

### Usage

```cpp
#include "db/redis_client.hpp"

using namespace qbuem_routine::redis;

// Connect
auto client_r = co_await RedisClient::connect("redis://localhost:6379");
auto& redis = **client_r;

// Strings
co_await redis.set("key", "value", 3600);   // EX 1 hour
auto val = co_await redis.get("key");        // RedisValue{String, "value"}
auto cnt = co_await redis.incr("counter");   // int64_t
auto ttl = co_await redis.ttl("key");        // int64_t (seconds)
auto many = co_await redis.mget({"k1", "k2"}); // vector<optional<string>>

// Hash
co_await redis.hset("user:1", "name", "Alice");
auto name = co_await redis.hget("user:1", "name"); // optional<string>
auto all  = co_await redis.hgetall("user:1");       // vector<pair<string,string>>

// List
co_await redis.rpush("queue", "task1");
auto item = co_await redis.lpop("queue");    // optional<string>

// Set
co_await redis.sadd("tags", "cpp");
auto members = co_await redis.smembers("tags"); // vector<string>

// Sorted Set
co_await redis.zadd("leaderboard", 100.0, "alice");
auto rank = co_await redis.zrank("leaderboard", "alice");

// Arbitrary command
auto r = co_await redis.command({"CONFIG", "GET", "maxmemory"});
```

### Supported Commands

| Category | Commands |
|----------|---------|
| Strings | GET · SET(TTL) · GETSET · SETNX · DEL · EXISTS · EXPIRE · TTL · PTTL · INCR · INCRBY · DECR · DECRBY · MGET · MSET |
| Hash | HGET · HSET · HSETNX · HDEL · HEXISTS · HGETALL · HKEYS · HVALS · HLEN |
| List | LPUSH · RPUSH · LPOP · RPOP · LRANGE · LLEN · LINDEX |
| Set | SADD · SREM · SMEMBERS · SCARD · SISMEMBER |
| Sorted Set | ZADD · ZREM · ZRANGE · ZRANGEBYSCORE · ZSCORE · ZCARD · ZRANK |
| Other | TYPE · KEYS · SCAN · RENAME · PERSIST · PING · FLUSHDB · INFO · command(args) |

---

## Migration Framework (`migration/migrator.hpp`)

Driver-independent schema version management. History stored in the `__schema_migrations` table.

### Defining Migrations

```cpp
#include "db/migration/migrator.hpp"
using namespace qbuem_routine::migration;

static const std::vector<Migration> kMigrations = {
    {
        .version = 1,
        .description = "create users table",
        .up = R"sql(
            CREATE TABLE users (
                id    BIGSERIAL PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                name  TEXT NOT NULL
            )
        )sql",
        .down = "DROP TABLE users",
    },
    {
        .version = 2,
        .description = "add bio column",
        .up = "ALTER TABLE users ADD COLUMN bio TEXT",
        .down = "ALTER TABLE users DROP COLUMN bio",
    },
};
```

### Running Migrations

```cpp
auto conn_r = co_await pool->acquire();
MigrationRunner runner{kMigrations, **conn_r};  // PostgreSQL: Dollar (default)
// MigrationRunner runner{kMigrations, **conn_r, PlaceholderStyle::Question}; // MySQL/SQLite

// Apply all pending migrations
auto result = co_await runner.migrate();
// → MigrationResult{ .applied=2, .skipped=0, .latest=2 }

// Apply up to a specific version
co_await runner.migrate_to(1);

// Roll back the latest migration
co_await runner.rollback();

// Roll back all migrations above a version
co_await runner.rollback_to(1);

// Check status
auto status = co_await runner.status();
for (auto& s : *status)
    std::print("{:04d} {:30s} {}\n",
               s.version, s.description, s.applied ? "applied" : "pending");
```

---

## Build

```bash
# Default build (MySQL auto-detected)
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_CXX_COMPILER=g++-13 \
      -B build
cmake --build build

# Disable MySQL
cmake -G Ninja -DQBUEM_DB_MYSQL=OFF -B build

# Disable Redis
cmake -G Ninja -DQBUEM_DB_REDIS=OFF -B build
```

**Requirements**: GCC ≥ 13 / Clang ≥ 17, CMake ≥ 3.20, ninja-build, C compiler (for libpq source build)
