cmake_minimum_required(VERSION 3.20)
project(qbuem-db VERSION 1.0.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(FetchContent)
include(ExternalProject)

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
    BUILD_COMMAND
        ${CMAKE_MAKE_PROGRAM} -C <SOURCE_DIR>/src/interfaces/libpq
    INSTALL_COMMAND
        ${CMAKE_MAKE_PROGRAM} -C <SOURCE_DIR>/src/interfaces/libpq install
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

# ── ORM (header-only) ─────────────────────────────────────────────────────────
add_library(qbuem_db_orm INTERFACE)
target_include_directories(qbuem_db_orm INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(qbuem_db_orm INTERFACE
    qbuem-stack::reactor
    qbuem-stack::result
)
add_library(qbuem-db::orm ALIAS qbuem_db_orm)
