# qbuem-db — Claude 작업 가이드

## 프로젝트 개요

C++23 기반 DB 드라이버 라이브러리. qbuem-stack 위에서 동작하며
비동기 ORM + 여러 DB 드라이버 + 마이그레이션 툴을 제공합니다.

## 코드 구조

```
src/db/
├── orm.hpp                  # 헤더 전용 ORM (Concepts 기반)
├── postgresql_driver.hpp/cpp  # 비동기 PostgreSQL (libpq)
├── sqlite3_driver.hpp/cpp     # SQLite3 (테스트/개발용)
├── mysql_driver.hpp/cpp       # MySQL/MariaDB (클라우드 SQL)
├── redis_client.hpp/cpp       # 비동기 Redis (RESP2, 외부 의존성 없음)
└── migration/
    └── migrator.hpp           # 드라이버 독립 마이그레이션 프레임워크
```

## 핵심 인터페이스 (qbuem-stack 제공)

- `IDBDriver`      — 드라이버 팩토리 (`pool()`)
- `IConnectionPool`— 연결 풀 (`acquire()`, `return_connection()`)
- `IConnection`    — 연결 (`query()`, `prepare()`, `begin()`, `ping()`)
- `ITransaction`   — 트랜잭션 (`commit()`, `rollback()`, `savepoint()`)
- `IStatement`     — Prepared statement (`execute()`, `execute_dml()`)
- `IResultSet`     — 결과 반복자 (`next()`)
- `IRow`           — 행 접근 (`get(idx)`, `get(name)`)
- `Value`          — Null/Int64/Float64/Bool/Text/Blob 타입
- `Result<T>`      — 에러 반환 (`qbuem::unexpected()`)
- `Task<T>`        — 코루틴 반환 타입

## 비동기 패턴 (PostgreSQL / Redis)

모든 비동기 드라이버는 qbuem Reactor 이벤트 루프를 사용합니다:

```cpp
// 패턴: PQsend* → async_flush → PgReadAwaiter
static Task<PGresult*> async_params(PGconn* conn, ...) {
    PQsendQueryParams(...);
    co_await async_flush(conn);          // PgFlushAwaiter
    co_return co_await PgReadAwaiter{conn}; // 소켓 readable 대기
}
```

Awaiter 작성 규칙:
1. `await_ready()` — 즉시 완료 가능하면 true 반환 (락 없이)
2. `await_suspend()` — Reactor에 이벤트 등록 후 yield
3. `await_resume()` — 결과 반환

## ORM 사용 패턴

```cpp
// 1. 등록 (app startup, 1회)
orm::register_table<User>("users")
    .pk("id", &User::id)
    .col("email", &User::email)
    .col("name", &User::name);

// 2. SQL 생성
auto& m = orm::meta<User>();
m.sql_insert()                                  // INSERT INTO users(email,name) VALUES ($1,$2) RETURNING *
m.sql_select_where("email")                     // SELECT ... WHERE email=$1
m.sql_select_where_paged("email", 1, 2, 3)     // SELECT ... WHERE email=$1 LIMIT $2 OFFSET $3
m.sql_count_where("email")                      // SELECT COUNT(*) FROM users WHERE email=$1
m.sql_upsert_pk()                               // INSERT ... ON CONFLICT(id) DO UPDATE SET ...
m.sql_insert_batch(3)                           // INSERT ... VALUES ($1,$2),($3,$4),($5,$6)

// 3. 바인딩
auto params = m.bind_insert(user);              // non-PK 값
auto params = m.bind_update(user);              // non-PK + PK (마지막)
auto params = m.bind_batch({u1, u2, u3});       // 배치 INSERT 파라미터
auto params = m.bind_paged(email, 20, 0);       // WHERE + LIMIT + OFFSET

// 4. 결과 읽기
User u = m.read_row(*row);                      // 컬럼명 기반
User u = m.read_row_indexed(*row);              // 인덱스 기반 (더 빠름)
```

## 드라이버 DSN 형식

| 드라이버 | DSN |
|---------|-----|
| PostgreSQL | `postgresql://user:pass@host:5432/db` |
| SQLite3 | `sqlite:///path/to/file.db` 또는 `sqlite://:memory:` |
| MySQL | `mysql://user:pass@host:3306/db[?ssl=true]` |
| Redis | `redis://[:pass@]host[:port][/db]` |

## 마이그레이션

```cpp
#include "db/migration/migrator.hpp"
using namespace qbuem_routine::migration;

static const std::vector<Migration> kMigrations = {
    { .version=1, .description="init schema",
      .up="CREATE TABLE ...",
      .down="DROP TABLE ..." },
};

// PlaceholderStyle::Dollar  → PostgreSQL ($1, $2)
// PlaceholderStyle::Question → MySQL/SQLite (?, ?)
MigrationRunner runner{kMigrations, *conn, PlaceholderStyle::Dollar};
co_await runner.migrate();           // 미적용 전부
co_await runner.migrate_to(3);       // 3버전까지
co_await runner.rollback();          // 최신 1개 롤백
co_await runner.rollback_to(1);      // 1버전 초과분 전부 롤백
auto status = co_await runner.status(); // 적용 현황
```

## 새 드라이버 추가 시 체크리스트

1. `src/db/<name>_driver.hpp` — `make_<name>_driver()` 팩토리 선언
2. `src/db/<name>_driver.cpp` — `IDBDriver` / `IConnectionPool` / `IConnection` /
   `ITransaction` / `IStatement` 구현
3. `CMakeLists.txt` — 새 `add_library` 타겟 추가 및 `qbuem-db::<name>_driver` alias
4. `README.md` — 드라이버 표와 DSN 형식, 사용법 추가
5. `llms.txt` / `llms_full.txt` — 업데이트

## 빌드

```bash
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_CXX_COMPILER=g++-13 \
      -B build
cmake --build build
```

**옵션:**
- `-DQBUEM_DB_MYSQL=OFF`  — MySQL 드라이버 비활성화
- `-DQBUEM_DB_REDIS=OFF`  — Redis 클라이언트 비활성화

**요구사항:**
- GCC ≥ 13 / Clang ≥ 17
- CMake ≥ 3.20
- ninja-build
- C 컴파일러 (libpq 소스 빌드)
- `libmysqlclient-dev` (MySQL 드라이버, 선택)

## 코딩 스타일

- C++23 기능 적극 활용 (`std::byteswap`, `std::format`, Concepts)
- 모든 공개 함수 `[[nodiscard]]` 적용
- 에러: `Result<T>` 반환, 예외 최소화
- 동기 드라이버: `std::mutex` 직렬화 (SQLite3, MySQL 패턴)
- 비동기 드라이버: Reactor 이벤트 + coroutine awaiter (PG, Redis 패턴)
- Awaiter에서 `reactor->post()` 로 resume — 이벤트 핸들러에서 직접 resume 금지

## 흔한 실수

- `PQsend*` / MySQL send 함수는 반드시 **락 안에서** 호출,
  `co_await` (flush, read) 는 **락 밖에서** 수행
- SQLite3는 FULLMUTEX 모드이므로 이중 잠금 주의
- ORM `bind_batch()` 는 `sql_insert_batch(n)` 과 **n이 일치**해야 함
- `sql_insert_batch()` RETURNING은 PostgreSQL 전용 — MySQL/SQLite에서는 `returning=false`
