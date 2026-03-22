# qbuem-db

qbuem-stack 기반 C++23 DB 드라이버 라이브러리.
PostgreSQL · MySQL/MariaDB (비동기/클라우드) · SQLite3 · Redis 드라이버와
템플릿 ORM, 드라이버 독립 마이그레이션 프레임워크를 제공합니다.

## 제공 컴포넌트

| 타겟 | 파일 | 설명 |
|------|------|------|
| `qbuem-db::orm` | `src/db/orm.hpp` | 헤더 전용 제너릭 ORM — SQL 생성 + 배치/페이지/카운트 + 결과 매핑 |
| `qbuem-db::migration` | `src/db/migration/migrator.hpp` | 헤더 전용 마이그레이션 프레임워크 |
| `qbuem-db::postgresql_driver` | `src/db/postgresql_driver.{hpp,cpp}` | libpq 기반 **비동기** PostgreSQL 드라이버 |
| `qbuem-db::mysql_driver` | `src/db/mysql_driver.{hpp,cpp}` | libmysqlclient 기반 MySQL/MariaDB 드라이버 |
| `qbuem-db::sqlite3_driver` | `src/db/sqlite3_driver.{hpp,cpp}` | SQLite3 amalgamation 기반 드라이버 (개발·테스트용) |
| `qbuem-db::redis_client` | `src/db/redis_client.{hpp,cpp}` | **비동기** Redis 클라이언트 (RESP2, 의존성 없음) |

## 빌드 의존성

| 의존성 | 관리 방법 | 용도 |
|--------|---------|------|
| qbuem-stack | `FetchContent` (GIT) | 인터페이스, Reactor, Task |
| SQLite3 amalgamation 3.49.1 | `FetchContent` (ZIP) | SQLite3 드라이버 |
| libpq (PostgreSQL 17.2) | `ExternalProject_Add` (소스 빌드) | PostgreSQL 드라이버 |
| libmysqlclient (시스템) | `find_library` | MySQL 드라이버 (선택) |

> MySQL 드라이버: `apt install libmysqlclient-dev` (또는 `dnf install mysql-devel`)
> PostgreSQL / SQLite3 / Redis: 외부 시스템 라이브러리 **불필요**

## 사용법 (FetchContent)

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
    qbuem-db::postgresql_driver   # 프로덕션 (비동기)
    qbuem-db::mysql_driver        # MySQL/MariaDB 클라우드
    qbuem-db::sqlite3_driver      # 개발/테스트
    qbuem-db::redis_client        # 캐시/세션
)
```

---

## ORM (`src/db/orm.hpp`)

C++23 Concepts 기반 헤더 전용 ORM. **모든 드라이버 호환** — Dialect 설정으로 플레이스홀더·RETURNING·UPSERT 구문을 자동 조정.

### Dialect 설정 (드라이버 호환)

| Dialect | 플레이스홀더 | RETURNING | UPSERT |
|---------|------------|-----------|--------|
| `Dialect::PostgreSQL` (기본) | `$1, $2, …` | 포함 | `ON CONFLICT … EXCLUDED` |
| `Dialect::MySQL` | `?` | 제외 | `ON DUPLICATE KEY UPDATE VALUES(col)` |
| `Dialect::SQLite` | `?` | 제외 | `ON CONFLICT … EXCLUDED` |

```cpp
// MySQL/SQLite 사용 시 dialect 지정
orm::register_table<User>("users")
    .dialect(Dialect::MySQL)   // 또는 Dialect::SQLite
    .pk("id",    &User::id)
    .col("email", &User::email)
    .col("name",  &User::name);
// → sql_insert() = "INSERT INTO users(email,name) VALUES (?,?)"  (RETURNING 없음)
// → sql_upsert_pk() = "INSERT … ON DUPLICATE KEY UPDATE email=VALUES(email),…"
```

### 지원 필드 타입

| C++ 타입 | DB 타입 | Concept |
|---------|---------|---------|
| `int16_t`, `int32_t`, `int64_t`, `uint32_t` | INTEGER | `OrmInt` |
| `double`, `float` | REAL | `OrmFloat` |
| `std::string` | TEXT | `OrmText` |
| `bool` | BOOLEAN | `OrmBool` |
| `std::optional<T>` | NULL 허용 | `OrmOptional` |

### SQL 생성 메서드 전체

> 아래 SQL 예시는 `Dialect::PostgreSQL` 기준. MySQL/SQLite는 `$N` → `?` 자동 변환.

| 메서드 | 생성 SQL (PostgreSQL) |
|--------|-----------------------|
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

### 파라미터 바인딩

```cpp
auto& m = orm::meta<User>();

// 단건 INSERT
auto params = m.bind_insert(user);               // non-PK 값 순서대로

// UPDATE (non-PK 먼저, PK 마지막)
auto params = m.bind_update(user);

// 배치 INSERT (3행)
auto params = m.bind_batch(std::span{users, 3}); // bind_batch + sql_insert_batch(3) 쌍

// WHERE 조건용
auto params = m.bind_val(email);                 // 1개
auto params = m.bind_val2(email, name);          // 2개
auto params = m.bind_val3(a, b, c);              // 3개

// WHERE + LIMIT + OFFSET (페이지네이션)
auto params = m.bind_paged(email, 20, 0);        // email=$1, limit=20, offset=0

// IN 절용: 임의 범위 (vector, span, initializer_list 등)
std::vector<int64_t> ids = {1, 2, 3};
auto params = m.bind_in(ids);                    // [1, 2, 3]
auto params = m.bind_in(std::vector<std::string>{"a@b.com", "c@d.com"});
```

### 결과 읽기

```cpp
while (auto* row = co_await rs->next()) {
    User u = m.read_row(*row);         // 컬럼명 기반 (안전)
    User u = m.read_row_indexed(*row); // 인덱스 기반 (고성능)
}
```

### 전체 드라이버 사용 예시

```cpp
// PostgreSQL (기본 — RETURNING으로 삽입된 행 즉시 수신)
auto& m = orm::meta<User>();
auto rs = co_await conn->query(m.sql_insert(), m.bind_insert(user));
User inserted = m.read_row(*co_await (*rs)->next());

// MySQL (RETURNING 없음 — last_insert_id로 PK 획득)
auto& m = orm::meta<User>();  // dialect(Dialect::MySQL) 등록됨
auto rs = co_await conn->query(m.sql_insert(), m.bind_insert(user));
user.id = static_cast<int64_t>((*rs)->last_insert_id());

// IN 쿼리 (모든 드라이버 동일)
std::vector<int64_t> ids = {10, 20, 30};
auto sql    = m.sql_select_where_in("id", ids.size());
auto params = m.bind_in(ids);
auto rs2    = co_await conn->query(sql, params);
```

---

## PostgreSQL 드라이버 (`postgresql_driver`)

### 비동기 아키텍처

```
PgDriver (IDBDriver)
  └─ PgConnectionPool (IConnectionPool)
       ├─ Slot[]          — PGconn + mutex + stmt_counter
       ├─ PgConnection    (IConnection)   — acquire/release 쌍
       ├─ PgTransaction   (ITransaction)  — BEGIN/COMMIT/ROLLBACK/SAVEPOINT
       └─ PgStatement     (IStatement)    — PQsendQueryPrepared
```

```
PQsendQueryParams()  ─→  PgFlushAwaiter  ─→  PgReadAwaiter
     (비블로킹 전송)      (소켓 writable 대기)   (소켓 readable + 결과 완료)
```

### DSN

```
postgresql://user:pass@host:5432/dbname
postgresql://host/dbname
postgresql:///dbname    — Unix 소켓
```

### 바이너리 프로토콜 OID 매핑

| PG OID | C++ 타입 | Value 타입 |
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

## MySQL/MariaDB 드라이버 (`mysql_driver`)

### 클라우드 호환

| 서비스 | 비고 |
|--------|------|
| Amazon RDS for MySQL | 완전 호환 |
| Amazon Aurora MySQL | 완전 호환 |
| Google Cloud SQL (MySQL) | 완전 호환 |
| Azure Database for MySQL | 완전 호환 |
| PlanetScale | MySQL 호환 |
| TiDB Cloud | MySQL 호환 |
| MariaDB SkySQL | MariaDB DSN |

### DSN

```
mysql://user:pass@host:3306/dbname
mysql://user:pass@host/dbname      — 포트 기본값 3306
mysql://user:pass@host/db?ssl=true — TLS 강제
mariadb://user:pass@host/dbname    — MariaDB
```

### 특징

- `$N` → `?` 플레이스홀더 자동 변환 (ORM과 호환)
- UTF8MB4 기본 문자셋
- 멀티 커넥션 풀 (PoolConfig.max_size 반영)
- Prepared statement (MYSQL_STMT)
- 트랜잭션 / 세이브포인트
- TLS/SSL 옵션

```cpp
#include "db/mysql_driver.hpp"

auto driver = qbuem_routine::make_mysql_driver();
auto pool_r = co_await driver->pool(
    "mysql://user:pass@db.example.com/mydb",
    { .min_size=2, .max_size=10 });
// 이후 PostgreSQL과 동일한 IConnection 인터페이스
```

---

## SQLite3 드라이버 (`sqlite3_driver`)

개발·테스트 환경 전용. PostgreSQL / MySQL과 동일한 인터페이스.

```
sqlite:///path/to/file.db   — 파일 DB
sqlite://:memory:           — 인메모리 (테스트)
```

- SQLite3 amalgamation 직접 컴파일 (시스템 라이브러리 불필요)
- WAL 모드 + `synchronous=NORMAL` + `foreign_keys=ON`

---

## Redis 클라이언트 (`redis_client`)

순수 POSIX 소켓 + RESP2 프로토콜 구현. 외부 의존성 없음.
qbuem Reactor와 완전 비동기 통합.

### 클라우드 호환

| 서비스 | 비고 |
|--------|------|
| Amazon ElastiCache (Redis OSS) | 완전 호환 |
| Upstash (서버리스 Redis) | 완전 호환 |
| Redis Cloud | 완전 호환 |
| Google Cloud Memorystore | 완전 호환 |
| Azure Cache for Redis | 완전 호환 |

### DSN

```
redis://host:6379
redis://:password@host:6379
redis://user:password@host:6379/2   — DB 2 선택
```

### 사용법

```cpp
#include "db/redis_client.hpp"

using namespace qbuem_routine::redis;

// 연결
auto client_r = co_await RedisClient::connect("redis://localhost:6379");
auto& redis = **client_r;

// 문자열
co_await redis.set("key", "value", 3600);   // EX 1시간
auto val = co_await redis.get("key");        // RedisValue{String, "value"}
auto cnt = co_await redis.incr("counter");   // int64_t

// 해시
co_await redis.hset("user:1", "name", "Alice");
auto name = co_await redis.hget("user:1", "name"); // optional<string>
auto all  = co_await redis.hgetall("user:1");       // vector<pair<string,string>>

// 리스트
co_await redis.rpush("queue", "task1");
auto item = co_await redis.lpop("queue");    // optional<string>

// 셋
co_await redis.sadd("tags", "cpp");
auto members = co_await redis.smembers("tags"); // vector<string>

// 정렬 셋
co_await redis.zadd("leaderboard", 100.0, "alice");
auto rank = co_await redis.zrank("leaderboard", "alice");

// 임의 명령
auto r = co_await redis.command({"CONFIG", "GET", "maxmemory"});
```

---

## 마이그레이션 (`migration/migrator.hpp`)

드라이버 독립적 스키마 버전 관리. `__schema_migrations` 테이블에 이력 저장.

### 마이그레이션 정의

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

### 실행

```cpp
auto conn_r = co_await pool->acquire();
MigrationRunner runner{kMigrations, **conn_r};  // PostgreSQL: Dollar (기본값)
// MigrationRunner runner{kMigrations, **conn_r, PlaceholderStyle::Question}; // MySQL/SQLite

// 모든 미적용 마이그레이션 적용
auto result = co_await runner.migrate();
// → MigrationResult{ .applied=2, .skipped=0, .latest=2 }

// 특정 버전까지 적용
co_await runner.migrate_to(1);

// 최신 1개 롤백
co_await runner.rollback();

// 특정 버전 이후 전부 롤백
co_await runner.rollback_to(1);

// 현황 조회
auto status = co_await runner.status();
for (auto& s : *status)
    std::print("{:04d} {:30s} {}\n",
               s.version, s.description, s.applied ? "applied" : "pending");
```

---

## 빌드

```bash
# 기본 빌드 (MySQL 자동 탐색)
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_CXX_COMPILER=g++-13 \
      -B build
cmake --build build

# MySQL 비활성화
cmake -G Ninja -DQBUEM_DB_MYSQL=OFF -B build

# Redis 비활성화
cmake -G Ninja -DQBUEM_DB_REDIS=OFF -B build
```

**요구사항**: GCC ≥ 13 / Clang ≥ 17, CMake ≥ 3.20, ninja-build, C 컴파일러
