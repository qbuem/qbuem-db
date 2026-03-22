# qbuem-db

qbuem-stack 기반 C++23 DB 드라이버 라이브러리.
PostgreSQL (libpq 비동기) + SQLite3 (amalgamation) 드라이버와 템플릿 ORM을 제공합니다.

## 제공 컴포넌트

| 타겟 | 파일 | 설명 |
|------|------|------|
| `qbuem-db::orm` | `src/db/orm.hpp` | 헤더 전용 제너릭 ORM — SQL 생성 + 파라미터 바인딩 + 결과 매핑 |
| `qbuem-db::postgresql_driver` | `src/db/postgresql_driver.{hpp,cpp}` | libpq 기반 비동기 PostgreSQL 드라이버 |
| `qbuem-db::sqlite3_driver` | `src/db/sqlite3_driver.{hpp,cpp}` | SQLite3 amalgamation 기반 드라이버 (개발·테스트용) |

## 빌드 의존성

| 의존성 | 관리 방법 |
|--------|---------|
| qbuem-stack | `FetchContent` (GIT) |
| SQLite3 amalgamation 3.49.1 | `FetchContent` (ZIP, 직접 컴파일) |
| libpq (PostgreSQL 17.2) | `ExternalProject_Add` (소스 빌드, libpq only) |

> `apt install libpq-dev` 또는 `libsqlite3-dev` 불필요. 모두 CMake에서 자동 빌드됩니다.

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
    qbuem-db::sqlite3_driver      # 개발/테스트
    qbuem-db::postgresql_driver   # 프로덕션
)
```

## ORM (`src/db/orm.hpp`)

C++23 Concepts 기반 헤더 전용 ORM. `qbuem::db::IRow` / `IConnection` 인터페이스에 의존합니다.

### 지원 필드 타입

| C++ 타입 | DB 타입 | Concept |
|---------|---------|---------|
| `int16_t`, `int32_t`, `int64_t`, `uint32_t` | INTEGER | `OrmInt` |
| `double`, `float` | REAL | `OrmFloat` |
| `std::string` | TEXT | `OrmText` |
| `bool` | BOOLEAN | `OrmBool` |
| `std::optional<T>` | NULL 허용 | `OrmOptional` |

### 등록 및 기본 사용

```cpp
#include "db/orm.hpp"

struct Child {
    int64_t     id;
    int64_t     parent_id;
    std::string name;
    std::string birth_date;
};

// 1. 앱 시작 시 1회 등록
orm::register_table<Child>("children")
    .pk("id",          &Child::id)
    .col("parent_id",  &Child::parent_id)
    .col("name",       &Child::name)
    .col("birth_date", &Child::birth_date);

// 2. SQL 생성
auto& m = orm::meta<Child>();

m.sql_select_where("parent_id")
// → "SELECT id, parent_id, name, birth_date FROM children WHERE parent_id = $1"

m.sql_insert()
// → "INSERT INTO children(parent_id, name, birth_date) VALUES ($1,$2,$3) RETURNING *"

m.sql_update_pk()
// → "UPDATE children SET parent_id=$1, name=$2, birth_date=$3 WHERE id=$4 RETURNING *"

m.sql_delete_pk()
// → "DELETE FROM children WHERE id = $1"

// 3. 파라미터 바인딩
auto params = m.bind_insert(child);   // PK 제외
auto params = m.bind_update(child);   // non-PK 먼저, PK 마지막
auto params = m.bind_pk(child);       // PK만

// 4. 결과 읽기 (컬럼명 기반)
while (auto* row = co_await rs->next()) {
    Child c = m.read_row(*row);
}
```

### SQL 생성 메서드 전체

| 메서드 | 생성 SQL |
|--------|---------|
| `sql_select_all()` | `SELECT col1,… FROM table` |
| `sql_select_where(col)` | `SELECT … FROM table WHERE col = $1` |
| `sql_select_ordered(col, order)` | `SELECT … WHERE col=$1 ORDER BY order` |
| `sql_select_all_ordered(order)` | `SELECT … ORDER BY order` |
| `sql_select_raw_where(expr)` | `SELECT … WHERE <expr>` |
| `sql_insert()` | `INSERT INTO table(…) VALUES (…) RETURNING *` |
| `sql_update_pk()` | `UPDATE table SET …=$N WHERE pk=$N+1 RETURNING *` |
| `sql_delete_pk()` | `DELETE FROM table WHERE pk = $1` |
| `sql_delete_where(col)` | `DELETE FROM table WHERE col = $1` |

## PostgreSQL 드라이버 (`postgresql_driver`)

### 아키텍처

```
PgDriver (IDBDriver)
  └─ PgConnectionPool (IConnectionPool)
       ├─ Slot[]          — PGconn + mutex + stmt_counter
       ├─ PgConnection    (IConnection)   — acquire/release 쌍
       ├─ PgTransaction   (ITransaction)  — BEGIN/COMMIT/ROLLBACK/SAVEPOINT
       └─ PgStatement     (IStatement)    — PQsendQueryPrepared
```

### 비동기 구현 핵심

libpq를 **비블로킹 모드**로 구성하여 qbuem Reactor와 통합합니다.

```
PQsendQueryParams()  ─→  PgFlushAwaiter  ─→  PgReadAwaiter
     (비블로킹 전송)      (소켓 writable 대기)   (소켓 readable + 결과 완료 대기)
```

- **`PgFlushAwaiter`**: `PQflush() == 1` (송신 버퍼 가득)이면 소켓 writable 이벤트 등록 후 yield
- **`PgReadAwaiter`**: `PQisBusy() == true`이면 소켓 readable 이벤트 등록 후 yield, 완료 시 `Reactor::post(resume)`
- **`PgAcquireAwaiter`**: 연결 풀 소진 시 yield → `release()` 호출 시 직접 슬롯 전달 후 resume

### 연결 풀 동작

```
acquire() 호출
  ├─ idle 슬롯 있음  → 즉시 반환 (O(1))
  ├─ slots.size() < max_size → 새 PGconn 생성
  └─ 풀 소진         → PgAcquireAwaiter yield
                         release() 호출 시 waiter에게 슬롯 직접 전달
```

### 사용법

```cpp
#include "db/postgresql_driver.hpp"

auto driver = qbuem_routine::make_postgresql_driver();
auto pool_r = co_await driver->pool(
    "postgresql://user:pass@localhost:5432/mydb",
    { .min_size = 2, .max_size = 16 });
auto& pool = *pool_r;

// 쿼리
auto conn_r = co_await pool->acquire();
auto& conn  = *conn_r;
auto rs_r = co_await conn->query(
    "SELECT id, name FROM children WHERE parent_id = $1",
    { db::Value{int64_t{42}} });

while (auto* row = co_await (*rs_r)->next()) {
    auto id   = row->get(0).get<int64_t>();
    auto name = std::string{row->get("name").get<std::string_view>()};
}

// 트랜잭션
auto txn_r = co_await conn->begin();
co_await (*txn_r)->execute("INSERT INTO ...", params);
co_await (*txn_r)->commit();

// Prepared Statement
auto stmt_r = co_await conn->prepare("SELECT ... WHERE id = $1");
auto rs2_r  = co_await (*stmt_r)->execute({ db::Value{int64_t{1}} });
```

### 바이너리 프로토콜

모든 쿼리는 `resultFormat=1` (binary)로 요청합니다.
컬럼 타입은 PostgreSQL OID로 판별하며 big-endian 역직렬화에 `std::byteswap` (C++23)을 사용합니다.

| PG OID | C++ 타입 | Value 타입 |
|--------|---------|-----------|
| 16 (bool) | `bool` | `Value::Bool` |
| 20 (int8) | `int64_t` | `Value::Int64` |
| 21 (int2) | `int16_t` → int64 | `Value::Int64` |
| 23 (int4) | `int32_t` → int64 | `Value::Int64` |
| 700 (float4) | `float` → double | `Value::Float64` |
| 701 (float8) | `double` | `Value::Float64` |
| 17 (bytea) | blob | `Value::Blob` |
| others | text | `Value::Text` |

## SQLite3 드라이버 (`sqlite3_driver`)

개발·테스트 환경에 사용하는 단일 커넥션 드라이버입니다.

### 특징

- SQLite3 amalgamation을 FetchContent로 직접 컴파일 (시스템 라이브러리 불필요)
- 단일 `sqlite3*` + `std::mutex` 직렬화 (FULLMUTEX)
- WAL 모드 + `synchronous=NORMAL` + `foreign_keys=ON` 자동 설정
- PostgreSQL 드라이버와 동일한 `IConnection` / `ITransaction` / `IStatement` 인터페이스

### DSN 형식

```
sqlite:///path/to/file.db   — 파일 DB
sqlite://:memory:           — 인메모리 DB (테스트)
```

### 사용법

```cpp
#include "db/sqlite3_driver.hpp"

auto driver = qbuem_routine::make_sqlite_driver();
auto pool_r = co_await driver->pool("sqlite://:memory:", {});
// 이후 PostgreSQL 드라이버와 동일한 인터페이스
```

## 빌드

```bash
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_CXX_COMPILER=g++-13 \
      -DCMAKE_CXX_STANDARD=23 \
      -B build
cmake --build build
```

**요구사항**: GCC ≥ 13 / Clang ≥ 17, CMake ≥ 3.20, ninja-build, C 컴파일러 (libpq 빌드용)
