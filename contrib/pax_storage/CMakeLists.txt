project(Pax)
cmake_minimum_required (VERSION 3.11.0)
set(CMAKE_CXX_STANDARD 14)

find_program(
  PG_CONFIG pg_config
  HINTS ${PG_PATH}
  PATH_SUFFIXES bin
  DOC "The path to the pg_config of the CBDB version to compile against")

if(NOT PG_CONFIG)
  message(FATAL_ERROR "Unable to find 'pg_config'")
endif()

# Function to call pg_config and extract values.
function(GET_PG_CONFIG var)
  set(_temp)

  # Only call pg_config if the variable didn't already have a value.
  if(NOT ${var})
    execute_process(
      COMMAND ${PG_CONFIG} ${ARGN}
      OUTPUT_VARIABLE _temp
      OUTPUT_STRIP_TRAILING_WHITESPACE)
  endif()

  set(${var}
    ${_temp}
    PARENT_SCOPE)
endfunction()

# Get CBDB configuration from pg_config
get_pg_config(PG_INCLUDEDIR --includedir)

# TODO check exists if this is needed
set(CBDB_INCLUDE_DIR ${PG_INCLUDEDIR}/postgresql/server)

# Debug options
option(ENBALE_DEBUG "Enable debug" ON)

# Build gtest options
option(BUILD_GTEST "Build with google test" ON)

# Build pax format lib
option(BUILD_PAX_FORMAT "Build pax format lib" OFF)


if (ENBALE_DEBUG)
  ADD_DEFINITIONS(-DENBALE_DEBUG)
	SET(CMAKE_BUILD_TYPE "Debug")
  SET(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb")
  SET(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall")
else()
  SET(CMAKE_BUILD_TYPE "Release")
  # no need build gtest in release mode
  SET(BUILD_GTEST OFF)
endif(ENBALE_DEBUG)

# Vec options
option(VEC_BUILD "Build pax vectorization version" OFF)
set(VEC_HOME "" CACHE STRING "Path to vectorization home")
if (VEC_BUILD)

if("${VEC_HOME}" STREQUAL "")
  message(FATAL_ERROR "No found vectorization home setting. Using -DVEC_HOME to spec vectorization home")
endif()

set(CBDB_ROOT_INCLUDE_DIR ${PG_INCLUDEDIR})
ADD_DEFINITIONS(-DVEC_BUILD)

endif(VEC_BUILD)
add_subdirectory(src/cpp)
