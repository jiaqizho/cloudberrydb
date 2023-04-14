project(Pax)
cmake_minimum_required (VERSION 3.11.0)
set(CMAKE_CXX_STANDARD 14)

# TODO: support build thirdparty library

# Debug options
option(ENBALE_DEBUG "Enable debug" ON)

# Build gtest options
option(BUILD_GTEST "Build with google test" ON)

# Compile options
option(ENABLE_PAX_INDEPENDENT "Compile pax without CBDB context" ON)

if (ENABLE_PAX_INDEPENDENT)
    ADD_DEFINITIONS(-DPAX_INDEPENDENT_MODE)
endif(ENABLE_PAX_INDEPENDENT)


if (ENBALE_DEBUG)
	SET(CMAKE_BUILD_TYPE "Debug")
	SET(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb")
	SET(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall")
endif(ENBALE_DEBUG)

add_subdirectory(src/cpp)
