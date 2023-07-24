cmake_minimum_required (VERSION 3.11.0)

# protobuf
include(ExternalProject)
option(ORC_PREFER_STATIC_PROTOBUF "Prefer static protobuf library, if available" ON)
set(THIRDPARTY_CONFIGURE_COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}")
set(THIRDPARTY_DIR "${CMAKE_BINARY_DIR}/src/cpp/contrib")
set(THIRDPARTY_LOG_OPTIONS LOG_CONFIGURE 1
                           LOG_BUILD 1
                           LOG_INSTALL 1
                           LOG_DOWNLOAD 1)

set(PROTOBUF_PREFIX "${THIRDPARTY_DIR}/protobuf_ep-install")
set(PROTOBUF_INCLUDE_DIR "${PROTOBUF_PREFIX}/include")
set(PROTOBUF_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${PROTOBUF_PREFIX}
                        -DCMAKE_INSTALL_LIBDIR=lib
                        -DBUILD_SHARED_LIBS=OFF
                        -Dprotobuf_BUILD_TESTS=OFF)

set(PROTOBUF_CMAKE_ARGS ${PROTOBUF_CMAKE_ARGS} -DCMAKE_POSITION_INDEPENDENT_CODE=ON)
set(PROTOBUF_STATIC_LIB_PREFIX ${CMAKE_STATIC_LIBRARY_PREFIX})
set(PROTOBUF_STATIC_LIB "${PROTOBUF_PREFIX}/lib/${PROTOBUF_STATIC_LIB_PREFIX}protobuf${CMAKE_STATIC_LIBRARY_SUFFIX}")
message(STATUS "${PROTOBUF_STATIC_LIB}")
set(PROTOC_STATIC_LIB "${PROTOBUF_PREFIX}/lib/${PROTOBUF_STATIC_LIB_PREFIX}protoc${CMAKE_STATIC_LIBRARY_SUFFIX}")
set(PROTOBUF_EXECUTABLE "${PROTOBUF_PREFIX}/bin/protoc${CMAKE_EXECUTABLE_SUFFIX}")

set(PROTOBUF_CONFIGURE CONFIGURE_COMMAND "${THIRDPARTY_CONFIGURE_COMMAND}" ${PROTOBUF_CMAKE_ARGS}
                                             "${CMAKE_CURRENT_BINARY_DIR}/protobuf_ep-prefix/src/protobuf_ep/cmake")

ExternalProject_Add(protobuf_ep
    URL "https://artifactory.hashdata.xyz/artifactory/utility/protobuf-3.6.1.tar.gz"
    ${PROTOBUF_CONFIGURE}
    ${THIRDPARTY_LOG_OPTIONS}
    BUILD_BYPRODUCTS "${PROTOBUF_STATIC_LIB}" "${PROTOC_STATIC_LIB}")

set(PROTOBUF_LIBRARY ${PROTOBUF_STATIC_LIB})
set(PROTOC_LIBRARY ${PROTOC_STATIC_LIB})
set(PROTOBUF_VENDORED ON)
set(INSTALL_VENDORED_LIBS OFF)

add_library (orc_protobuf INTERFACE)
add_library (orc::protobuf ALIAS orc_protobuf)
add_library (orc_protoc INTERFACE)
add_library (orc::protoc ALIAS orc_protoc)

if (ORC_PREFER_STATIC_PROTOBUF AND ${PROTOBUF_STATIC_LIB})
  target_link_libraries (orc_protobuf INTERFACE ${PROTOBUF_STATIC_LIB})
else ()
  target_link_libraries (orc_protobuf INTERFACE ${PROTOBUF_LIBRARY})
endif()

target_include_directories (orc_protobuf SYSTEM INTERFACE ${PROTOBUF_INCLUDE_DIR})

if (ORC_PREFER_STATIC_PROTOBUF AND ${PROTOC_STATIC_LIB})
  target_link_libraries (orc_protoc INTERFACE ${PROTOC_STATIC_LIB})
else ()
  target_link_libraries (orc_protoc INTERFACE ${PROTOC_LIBRARY})
endif()

target_include_directories (orc_protoc SYSTEM INTERFACE ${PROTOBUF_INCLUDE_DIR})

if (PROTOBUF_VENDORED)
  add_dependencies (orc_protoc protobuf_ep)
  add_dependencies (orc_protobuf protobuf_ep)
  if (INSTALL_VENDORED_LIBS)
    install(FILES "${PROTOBUF_STATIC_LIB}" "${PROTOC_STATIC_LIB}"
            DESTINATION "lib")
  endif ()
endif ()

set(pax_proto_file "${CMAKE_CURRENT_SOURCE_DIR}/storage/orc/orc_proto.proto")
set(pax_proto_src "${CMAKE_CURRENT_SOURCE_DIR}/storage/orc/orc_proto.pb.h" "${CMAKE_CURRENT_SOURCE_DIR}/storage/orc/orc_proto.pb.cc")

add_custom_command(OUTPUT ${pax_proto_src}
  COMMAND ${PROTOBUF_EXECUTABLE}
  -I ${CMAKE_CURRENT_SOURCE_DIR}/storage/orc/
  --cpp_out="${CMAKE_CURRENT_SOURCE_DIR}/storage/orc/"
  ${pax_proto_file})
add_custom_target(generate_protobuf DEPENDS ${pax_proto_src})

if (BUILD_GTEST)

add_subdirectory(contrib/googletest)
ADD_DEFINITIONS(-DRUN_GTEST)
file(GLOB TEST_CASE_SOURCES 
  ${CMAKE_CURRENT_SOURCE_DIR}/*/*_test.cc 
  ${CMAKE_CURRENT_SOURCE_DIR}/*/*/*_test.cc)

link_directories($ENV{GPHOME}/lib)
add_executable(test_main ${TEST_CASE_SOURCES})
add_dependencies(test_main gtest gmock gtest_main)
target_include_directories(test_main PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${gtest_SOURCE_DIR}/include)
target_link_libraries(test_main gtest gmock gtest_main postgres pax)
endif(BUILD_GTEST)

set(pax_comm_src
    comm/bitmap.cc
    comm/cbdb_wrappers.cc)

set(pax_exceptions_src
    exceptions/CException.cc)

set(pax_storage_src
    storage/local_file_system.cc
    storage/micro_partition.cc
    storage/micro_partition_file_factory.cc
    storage/pax.cc
    storage/pax_buffer.cc
    storage/pax_column.cc
    storage/paxc_block_map_manager.cc
    storage/orc/orc.cc
    storage/orc/protobuf_stream.cc
    storage/strategy.cc)

set(pax_access_src
    access/pax_access_handle.cc
    access/pax_deleter.cc
    access/pax_dml_state.cc
    access/pax_inserter.cc
    access/pax_updater.cc
    access/pax_scanner.cc)
set(pax_catalog_src
    catalog/micro_partition_metadata.cc
    catalog/pax_aux_table.cc
    catalog/table_metadata.cc)

link_directories($ENV{GPHOME}/lib)
add_library(pax SHARED ${pax_proto_src} ${pax_storage_src} ${pax_exceptions_src} ${pax_access_src} ${pax_comm_src} ${pax_catalog_src})
set_target_properties(pax PROPERTIES 
OUTPUT_NAME pax)
target_include_directories(pax PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} "${CBDB_INCLUDE_DIR}")
target_link_libraries(pax PUBLIC uuid orc_protobuf postgres)
add_dependencies(pax generate_protobuf)
add_custom_command(TARGET pax POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E
                       copy_if_different $<TARGET_FILE:pax> ${CMAKE_CURRENT_SOURCE_DIR}/../data/pax.so)
