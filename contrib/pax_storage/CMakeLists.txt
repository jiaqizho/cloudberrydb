project(Pax)
cmake_minimum_required (VERSION 3.11.0)
set(CMAKE_CXX_STANDARD 14)

# TODO check exists if this is needed
set(CBDB_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../src/include)

# TODO: support build thirdparty library

# Debug options
option(ENBALE_DEBUG "Enable debug" ON)

# Build gtest options
option(BUILD_GTEST "Build with google test" ON)


if (ENBALE_DEBUG)
	SET(CMAKE_BUILD_TYPE "Debug")
	SET(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb")
	SET(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall")
endif(ENBALE_DEBUG)

add_subdirectory(src/cpp)
