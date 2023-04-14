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

file(GLOB TEST_CASE_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/*/*_test.cc)

add_executable(test_main ${TEST_CASE_SOURCES})
add_dependencies(test_main gtest gtest_main)
include_directories(${gtest_SOURCE_DIR}/include)
target_link_libraries(test_main gtest gtest_main)

target_include_directories(test_main PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(test_main pax_storage)
    
endif(BUILD_GTEST)

set(pax_comm_src
    comm/cbdb_wrappers.cc
    comm/local_wrappers.cc)

set(pax_exceptions_src
    exceptions/CException.cc)

set(pax_storage_src
    storage/local_file_system.cc
    storage/micro_partition.cc
    storage/orc_file_stream.cc
    storage/orc_native_micro_partition.cc
    storage/pax.cc)

set(pax_access_src 
	access/pax_access.cc 
	access/pax_access_handle.cc
	access/pax_deleter.cc 
	access/pax_dml_state.cc
	access/pax_inserter.cc 
	access/pax_scanner.cc)  
set(pax_catalog_src
    catalog/pax_aux_table.cc
    catalog/table_metadata.cc)

add_library(pax_storage SHARED ${pax_storage_src} ${pax_exceptions_src} ${pax_access_src} ${pax_comm_src} ${pax_catalog_src})
target_include_directories(pax_storage PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} "/home/gpadmin/workspace/cbdb/src/include")
target_link_libraries(pax_storage PUBLIC uuid orc)
