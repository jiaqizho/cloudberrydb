cmake_minimum_required (VERSION 3.11.0)

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