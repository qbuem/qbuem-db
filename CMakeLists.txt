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

# ── Options ───────────────────────────────────────────────────────────────────
option(QBUEM_DB_MYSQL   "Build MySQL/MariaDB driver"                        ON)
option(QBUEM_DB_REDIS   "Build Redis client"                                ON)
option(QBUEM_DB_PG_SSL  "Build libpq with OpenSSL (required for SSL/TLS)"   ON)

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
    src/qbuem/db/sqlite3_driver.cpp
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

# OpenSSL: always try to find it (needed for SCRAM-SHA-256 / TLS on Neon/cloud
# PostgreSQL). Fall back to no-SSL only when OpenSSL is genuinely unavailable.
find_package(OpenSSL QUIET)
if(OPENSSL_FOUND)
    set(_PG_SSL_FLAG --with-openssl)
    set(_PG_SSL_CPPFLAGS "CPPFLAGS=-I${OPENSSL_INCLUDE_DIR}")
    # Pass OpenSSL lib dir so pg configure can find -lssl -lcrypto.
    get_filename_component(_OPENSSL_LIB_DIR "${OPENSSL_SSL_LIBRARY}" DIRECTORY)
    set(_PG_SSL_LDFLAGS "LDFLAGS=-L${_OPENSSL_LIB_DIR}")
    message(STATUS "qbuem-db: libpq will be built with OpenSSL (${OPENSSL_SSL_LIBRARY})")
else()
    set(_PG_SSL_FLAG --without-openssl)
    set(_PG_SSL_CPPFLAGS "")
    set(_PG_SSL_LDFLAGS "")
    message(WARNING "qbuem-db: OpenSSL not found — libpq built without TLS support. "
                    "Cloud PostgreSQL (Neon, RDS, etc.) will refuse connections.")
endif()

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
            --without-ldap
            ${_PG_SSL_FLAG}
            --disable-nls
            # GCC 15+ defaults to -std=gnu23 where 'bool' is a keyword.
            # PostgreSQL 17.x c.h still uses 'typedef unsigned char bool'
            # which is rejected in C23.  Force C17 for the libpq build.
            "CFLAGS=-std=gnu17 -O2"
            ${_PG_SSL_CPPFLAGS}
            ${_PG_SSL_LDFLAGS}
    BUILD_COMMAND
        # Build generated headers first (errcodes.h etc.)
        ${MAKE_EXECUTABLE} -j -C <SOURCE_DIR>/src/include
        # Build libpq and its companion archives (pgcommon, pgport contain
        # SCRAM/HMAC/strong-random symbols required for SCRAM-SHA-256 auth).
        COMMAND ${MAKE_EXECUTABLE} -j -C <SOURCE_DIR>/src/interfaces/libpq libpq.a
        COMMAND ${MAKE_EXECUTABLE} -j -C <SOURCE_DIR>/src/common libpgcommon_shlib.a
        COMMAND ${MAKE_EXECUTABLE} -j -C <SOURCE_DIR>/src/port   libpgport_shlib.a
    INSTALL_COMMAND
        ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>/lib
        COMMAND ${CMAKE_COMMAND} -E make_directory <INSTALL_DIR>/include
        COMMAND ${CMAKE_COMMAND} -E copy
            <SOURCE_DIR>/src/interfaces/libpq/libpq.a
            <INSTALL_DIR>/lib/libpq.a
        # Merge companion archives (provide pg_saslprep, pg_strong_random, etc.)
        COMMAND ${CMAKE_COMMAND}
            -DLIBPQ_A=<INSTALL_DIR>/lib/libpq.a
            -DLIBPGPORT_A=<SOURCE_DIR>/src/port/libpgport_shlib.a
            -DLIBPGCOMMON_A=<SOURCE_DIR>/src/common/libpgcommon_shlib.a
            -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/merge_libpq.cmake
        COMMAND ${CMAKE_COMMAND} -E copy
            <SOURCE_DIR>/src/interfaces/libpq/libpq-fe.h
            <INSTALL_DIR>/include/libpq-fe.h
        COMMAND ${CMAKE_COMMAND} -E copy
            <SOURCE_DIR>/src/interfaces/libpq/libpq-events.h
            <INSTALL_DIR>/include/libpq-events.h
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
    src/qbuem/db/postgresql_driver.cpp
)
target_include_directories(qbuem_db_postgresql PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(qbuem_db_postgresql PUBLIC
    PostgreSQL::PostgreSQL
    qbuem-stack::reactor
    qbuem-stack::result
)
if(OPENSSL_FOUND)
    target_link_libraries(qbuem_db_postgresql PUBLIC OpenSSL::SSL OpenSSL::Crypto)
endif()
# ExternalProject_Add는 imported target의 INTERFACE_INCLUDE_DIRECTORIES만으로는
# 빌드 순서를 보장하지 않음. add_dependencies로 libpq.a 설치 완료 후
# postgresql_driver.cpp가 컴파일되도록 명시적 의존성 선언.
add_dependencies(qbuem_db_postgresql libpq_build)
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
            src/qbuem/db/mysql_driver.cpp
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
        src/qbuem/db/redis_client.cpp
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
