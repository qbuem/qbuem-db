# cmake/merge_libpq.cmake
# Merges libpgport_shlib.a and libpgcommon_shlib.a into libpq.a using a
# portable extract-then-combine approach that works on both GNU ar (Linux)
# and BSD ar / macOS libtool (macOS).
#
# Required variables (pass via -D):
#   LIBPQ_A        — path to libpq.a (modified in-place)
#   LIBPGPORT_A    — path to libpgport_shlib.a
#   LIBPGCOMMON_A  — path to libpgcommon_shlib.a
#
# Required tool (found automatically):
#   AR             — path to ar (passed by ExternalProject caller, or
#                    detected from CMAKE_AR)

foreach(var LIBPQ_A LIBPGPORT_A LIBPGCOMMON_A)
    if(NOT DEFINED ${var})
        message(FATAL_ERROR "merge_libpq.cmake: ${var} is not defined")
    endif()
endforeach()

# Resolve the ar executable: prefer what was passed in, then CMAKE_AR, then
# find it in PATH.
if(NOT DEFINED AR OR AR STREQUAL "")
    find_program(AR NAMES ar REQUIRED)
endif()
find_program(RANLIB_EXE NAMES ranlib REQUIRED)

# Create an isolated temp directory so object name collisions across archives
# cannot happen (each archive gets its own subdirectory).
set(_tmpdir "/tmp/libpq_merge_work")
file(REMOVE_RECURSE "${_tmpdir}")
file(MAKE_DIRECTORY "${_tmpdir}/pq")
file(MAKE_DIRECTORY "${_tmpdir}/port")
file(MAKE_DIRECTORY "${_tmpdir}/common")

# Extract each archive into its own subdirectory.
foreach(_pair "pq;${LIBPQ_A}" "port;${LIBPGPORT_A}" "common;${LIBPGCOMMON_A}")
    list(GET _pair 0 _dir)
    list(GET _pair 1 _arc)
    execute_process(
        COMMAND ${AR} -x "${_arc}"
        WORKING_DIRECTORY "${_tmpdir}/${_dir}"
        RESULT_VARIABLE _ret)
    if(NOT _ret EQUAL 0)
        message(FATAL_ERROR "merge_libpq.cmake: ar -x ${_arc} failed (code ${_ret})")
    endif()
endforeach()

# Gather all object files (explicitly from all three directories to preserve
# insertion order: libpq objects first, then port, then common).
file(GLOB _pq_objs     "${_tmpdir}/pq/*.o")
file(GLOB _port_objs   "${_tmpdir}/port/*.o")
file(GLOB _common_objs "${_tmpdir}/common/*.o")

set(_merged "/tmp/libpq_merged_new.a")
file(REMOVE "${_merged}")

execute_process(
    COMMAND ${AR} -crs "${_merged}" ${_pq_objs} ${_port_objs} ${_common_objs}
    RESULT_VARIABLE _ret)
if(NOT _ret EQUAL 0)
    message(FATAL_ERROR "merge_libpq.cmake: ar -crs failed (code ${_ret})")
endif()

execute_process(COMMAND ${RANLIB_EXE} "${_merged}")

file(RENAME "${_merged}" "${LIBPQ_A}")

# Clean up temp directory.
file(REMOVE_RECURSE "${_tmpdir}")

message(STATUS "merge_libpq.cmake: merged into ${LIBPQ_A}")
