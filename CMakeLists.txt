cmake_minimum_required(VERSION 3.20)
project(qbuem-db VERSION 2.0.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(FetchContent)
include(ExternalProject)

# PostgreSQL uses an autoconf/make build — it does not use the outer CMake
# generator.  Always build it with make regardless of CMAKE_MAKE_PROGRAM.
find_program(MAKE_EXECUTABLE NAMES make gmake REQUIRED)

# ── 옵션 ──────────────────────────────────────────────────────────────────────
option(QBUEM_DB_MYSQL   "MySQL/MariaDB 드라이버 빌드" ON)
option(QBUEM_DB_REDIS   "Redis 클라이언트 빌드"       ON)

# ── qbuem-stack ───────────────────────────────────────────────────────────────
set(QBUEM_BUILD_TESTS    OFF CACHE BOOL "" FORCE)
set(QBUEM_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(QBUEM_BUILD_BENCH    OFF CACHE BOOL "" FORCE)
set(QBUEM_INSTALL        OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
    qbuem_stack
    GIT_REPOSITORY https://github.com/qbuem/qbuem-stack.git
    GIT_TAG        main
)
FetchContent_MakeAvailable(qbuem_stack)

# ── SQLite3 amalgamation ──────────────────────────────────────────────────────
FetchContent_Declare(
    sqlite3_amalgam
    URL https://www.sqlite.org/2025/sqlite-amalgamation-3490100.zip
)
FetchContent_MakeAvailable(sqlite3_amalgam)

add_library(sqlite3 STATIC ${sqlite3_amalgam_SOURCE_DIR}/sqlite3.c)
target_include_directories(sqlite3 PUBLIC ${sqlite3_amalgam_SOURCE_DIR})
target_compile_definitions(sqlite3 PUBLIC
    SQLITE_THREADSAFE=2
    SQLITE_DEFAULT_WAL_SYNCHRONOUS=1
)
set_target_properties(sqlite3 PROPERTIES LINKER_LANGUAGE C)

# ── SQLite3 driver ────────────────────────────────────────────────────────────
add_library(qbuem_db_sqlite3 STATIC
    src/db/sqlite3_driver.cpp
)
target_include_directories(qbuem_db_sqlite3 PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(qbuem_db_sqlite3 PUBLIC
    sqlite3
    qbuem-stack::reactor
    qbuem-stack::result
)
add_library(qbuem-db::sqlite3_driver ALIAS qbuem_db_sqlite3)

# ── libpq (PostgreSQL) ────────────────────────────────────────────────────────
set(_PG_VER  "17.2")
set(_PG_INST "${CMAKE_BINARY_DIR}/_libpq_prefix")

ExternalProject_Add(libpq_build
    URL      "https://ftp.postgresql.org/pub/source/v${_PG_VER}/postgresql-${_PG_VER}.tar.bz2"
    SOURCE_DIR   "${CMAKE_BINARY_DIR}/_deps/postgresql-src"
    INSTALL_DIR  "${_PG_INST}"
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND
        <SOURCE_DIR>/configure
            "--prefix=<INSTALL_DIR>"
            --with-pic
            --without-readline
            --without-zlib
            --without-icu
            --without-openssl
            --disable-nls
            # GCC 15+ defaults to -std=gnu23 where 'bool' is a keyword.
            # PostgreSQL 17.x c.h still uses 'typedef unsigned char bool'
            # which is rejected in C23.  Force C17 for the libpq build.
            "CFLAGS=-std=gnu17 -O2"
    BUILD_COMMAND
        ${MAKE_EXECUTABLE} -C <SOURCE_DIR>/src/interfaces/libpq
    INSTALL_COMMAND
        ${MAKE_EXECUTABLE} -C <SOURCE_DIR>/src/interfaces/libpq install
        COMMAND ${CMAKE_COMMAND} -E copy
            <SOURCE_DIR>/src/include/postgres_ext.h
            <INSTALL_DIR>/include/postgres_ext.h
        COMMAND ${CMAKE_COMMAND} -E copy
            <SOURCE_DIR>/src/include/pg_config_ext.h
            <INSTALL_DIR>/include/pg_config_ext.h
    BUILD_BYPRODUCTS
        "${_PG_INST}/lib/libpq.a"
)

add_library(PostgreSQL::PostgreSQL STATIC IMPORTED GLOBAL)
set_target_properties(PostgreSQL::PostgreSQL PROPERTIES
    IMPORTED_LOCATION             "${_PG_INST}/lib/libpq.a"
    INTERFACE_INCLUDE_DIRECTORIES "${_PG_INST}/include"
)
add_dependencies(PostgreSQL::PostgreSQL libpq_build)

# ── PostgreSQL driver ─────────────────────────────────────────────────────────
add_library(qbuem_db_postgresql STATIC
    src/db/postgresql_driver.cpp
)
target_include_directories(qbuem_db_postgresql PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(qbuem_db_postgresql PUBLIC
    PostgreSQL::PostgreSQL
    qbuem-stack::reactor
    qbuem-stack::result
)
add_library(qbuem-db::postgresql_driver ALIAS qbuem_db_postgresql)

# ── MySQL / MariaDB driver ────────────────────────────────────────────────────
if(QBUEM_DB_MYSQL)
    # 시스템 libmysqlclient 탐색
    find_library(MYSQL_CLIENT_LIB NAMES mysqlclient mysqlclient_r
        HINTS
            /usr/lib/x86_64-linux-gnu
            /usr/lib/aarch64-linux-gnu
            /usr/local/lib
            /usr/lib/mysql
            /usr/local/mysql/lib
    )
    find_path(MYSQL_INCLUDE_DIR NAMES mysql/mysql.h mysql.h
        HINTS
            /usr/include
            /usr/local/include
            /usr/include/mysql
            /usr/local/mysql/include
    )

    if(MYSQL_CLIENT_LIB AND MYSQL_INCLUDE_DIR)
        message(STATUS "MySQL client found: ${MYSQL_CLIENT_LIB}")
        add_library(MySQL::MySQL UNKNOWN IMPORTED)
        set_target_properties(MySQL::MySQL PROPERTIES
            IMPORTED_LOCATION             "${MYSQL_CLIENT_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${MYSQL_INCLUDE_DIR}"
        )

        add_library(qbuem_db_mysql STATIC
            src/db/mysql_driver.cpp
        )
        target_include_directories(qbuem_db_mysql PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
        target_link_libraries(qbuem_db_mysql PUBLIC
            MySQL::MySQL
            qbuem-stack::reactor
            qbuem-stack::result
        )
        add_library(qbuem-db::mysql_driver ALIAS qbuem_db_mysql)
        message(STATUS "qbuem-db::mysql_driver enabled")
    else()
        message(STATUS "MySQL client not found — qbuem-db::mysql_driver skipped")
        message(STATUS "  apt install libmysqlclient-dev  (Debian/Ubuntu)")
        message(STATUS "  dnf install mysql-devel         (RHEL/Fedora)")
        set(QBUEM_DB_MYSQL OFF)
    endif()
endif()

# ── Redis client ──────────────────────────────────────────────────────────────
if(QBUEM_DB_REDIS)
    add_library(qbuem_db_redis STATIC
        src/db/redis_client.cpp
    )
    target_include_directories(qbuem_db_redis PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_link_libraries(qbuem_db_redis PUBLIC
        qbuem-stack::reactor
        qbuem-stack::result
    )
    add_library(qbuem-db::redis_client ALIAS qbuem_db_redis)
    message(STATUS "qbuem-db::redis_client enabled")
endif()

# ── ORM (header-only) ─────────────────────────────────────────────────────────
add_library(qbuem_db_orm INTERFACE)
target_include_directories(qbuem_db_orm INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(qbuem_db_orm INTERFACE
    qbuem-stack::reactor
    qbuem-stack::result
)
add_library(qbuem-db::orm ALIAS qbuem_db_orm)

# ── Migration (header-only) ───────────────────────────────────────────────────
add_library(qbuem_db_migration INTERFACE)
target_include_directories(qbuem_db_migration INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(qbuem_db_migration INTERFACE
    qbuem-stack::reactor
    qbuem-stack::result
)
add_library(qbuem-db::migration ALIAS qbuem_db_migration)

# ── 요약 출력 ─────────────────────────────────────────────────────────────────
message(STATUS "")
message(STATUS "── qbuem-db v${PROJECT_VERSION} ─────────────────────")
message(STATUS "  qbuem-db::orm              (header-only ORM)")
message(STATUS "  qbuem-db::migration        (header-only migration runner)")
message(STATUS "  qbuem-db::sqlite3_driver   (SQLite3 amalgamation)")
message(STATUS "  qbuem-db::postgresql_driver(async libpq)")
if(QBUEM_DB_MYSQL)
    message(STATUS "  qbuem-db::mysql_driver     (MySQL/MariaDB libmysqlclient)")
endif()
if(QBUEM_DB_REDIS)
    message(STATUS "  qbuem-db::redis_client     (async Redis RESP2)")
endif()
message(STATUS "─────────────────────────────────────────────")
message(STATUS "")
