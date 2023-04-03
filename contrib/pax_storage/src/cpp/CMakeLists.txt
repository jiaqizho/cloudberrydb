cmake_minimum_required (VERSION 3.11.0)

if (BUILD_GTEST)

# Build gtest and gmock 
# A example which link gtest/gmock and include gtest/gmock headers:
#
# add_executable(test_target source...)
# add_dependencies(test_target gtest)
# include_directories(${gtest_SOURCE_DIR}/include)
# target_link_libraries(test_target gtest)
add_subdirectory(contrib/googletest)

# Defined tests inside `BUILD_GTEST`

endif(BUILD_GTEST)

set(pax_comm_src
	comm/local_wrappers.cc)

set(pax_exceptions_src
	exceptions/CException.cc)

set(pax_storage_src
	storage/micro_partition.cc
    storage/pax.cc)

add_library(pax_exceptions STATIC ${pax_exceptions_src})

add_library(pax_comm STATIC ${pax_comm_src})
target_include_directories(pax_comm PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(pax_comm PUBLIC pax_exceptions)

add_library(pax_storage STATIC ${pax_storage_src})
target_include_directories(pax_storage PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(pax_storage PUBLIC pax_comm pax_exceptions)