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

set(orc_proto_file "${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/orc_proto.proto")
set(orc_proto_src "${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/orc_proto.pb.h" "${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/orc_proto.pb.cc")

set(pax_proto_file "${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/pax.proto")
set(pax_proto_src "${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/pax.pb.h" "${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/pax.pb.cc")

set(catalog_proto_file "${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/micro_partition_stats.proto")
set(stats_proto_src "${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/micro_partition_stats.pb.h" "${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/micro_partition_stats.pb.cc")

add_custom_command(OUTPUT ${orc_proto_src}
  COMMAND ${PROTOBUF_EXECUTABLE}
  -I ${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/
  --cpp_out="${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/"
  ${orc_proto_file})

add_custom_command(OUTPUT ${pax_proto_src}
  COMMAND ${PROTOBUF_EXECUTABLE}
  -I ${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/
  --cpp_out="${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/"
  ${pax_proto_file})

add_custom_command(OUTPUT ${stats_proto_src}
  COMMAND ${PROTOBUF_EXECUTABLE}
  -I ${CMAKE_CURRENT_SOURCE_DIR}/storage/proto/
  --cpp_out="${CMAKE_CURRENT_SOURCE_DIR}/storage/proto"
  ${catalog_proto_file})

add_custom_target(generate_protobuf DEPENDS ${orc_proto_src} ${pax_proto_src} ${stats_proto_src})

if (BUILD_GTEST AND NOT BUILD_PAX_FORMAT)
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
endif(BUILD_GTEST AND NOT BUILD_PAX_FORMAT)

# ztsd
set(ZSTD_BUILD_PROGRAMS OFF)
set(ZSTD_BUILD_TESTS OFF)
set(ZSTD_BUILD_CONTRIB)
add_subdirectory(contrib/zstd/build/cmake/)
set(ZTSD_HEADER contrib/zstd/lib)

set(pax_comm_src
    comm/bitmap.cc
    comm/paxc_wrappers.cc
    comm/cbdb_wrappers.cc)

set(pax_exceptions_src
    exceptions/CException.cc)

set(pax_storage_src
    storage/columns/pax_column.cc
    storage/columns/pax_column_int.cc
    storage/columns/pax_compress.cc
    storage/columns/pax_columns.cc
    storage/columns/pax_encoding_utils.cc
    storage/columns/pax_encoding_non_fixed_column.cc
    storage/columns/pax_encoding_column.cc
    storage/columns/pax_decoding.cc
    storage/columns/pax_encoding.cc
    storage/columns/pax_rlev2_decoding.cc
    storage/columns/pax_rlev2_encoding.cc
    storage/file_system.cc
    storage/pax_filter.cc
    storage/local_file_system.cc
    storage/micro_partition.cc
    storage/micro_partition_file_factory.cc
    storage/micro_partition_metadata.cc
    storage/micro_partition_stats.cc
    storage/pax_buffer.cc  
    storage/proto/protobuf_stream.cc
    storage/pax_filter.cc
    storage/strategy.cc
    storage/paxc_block_map_manager.cc
    storage/orc/orc.cc
    storage/strategy.cc)

if(NOT BUILD_PAX_FORMAT) 
  set(pax_storage_src ${pax_storage_src} storage/pax.cc)
  set(pax_storage_src ${pax_storage_src} storage/micro_partition_iterator.cc)
endif(NOT BUILD_PAX_FORMAT)

set(pax_access_src
    access/paxc_rel_options.cc
    access/pax_access_handle.cc
    access/pax_deleter.cc
    access/pax_dml_state.cc
    access/pax_inserter.cc
    access/pax_updater.cc
    access/pax_scanner.cc)

set(pax_catalog_src
    catalog/pax_aux_table.cc)

set(pax_vec_src
  storage/vec/pax_vec_adapter.cc
  storage/vec/pax_vec_reader.cc)

link_directories($ENV{GPHOME}/lib)

if(BUILD_PAX_FORMAT)
  # paxformat.so
  ADD_DEFINITIONS(-DBUILD_PAX_FORMAT)
  add_library(paxformat SHARED ${orc_proto_src} ${pax_proto_src} ${stats_proto_src} ${pax_storage_src} ${pax_exceptions_src} ${pax_comm_src} )
  target_include_directories(paxformat PUBLIC ${ZTSD_HEADER} ${CMAKE_CURRENT_SOURCE_DIR} ${CBDB_INCLUDE_DIR})
  target_link_libraries(paxformat PUBLIC uuid orc_protobuf zstd z)
  set_target_properties(paxformat PROPERTIES
   OUTPUT_NAME paxformat)
  add_dependencies(paxformat generate_protobuf)

  # export headers
  set(PAX_COMM_HEADERS
    comm/cbdb_api.h
    comm/cbdb_wrappers.h
  )

  # export headers
  set(PAX_EXCEPTION_HEADERS
    exceptions/CException.h
  )

  ## install dynamic libraray
  install(TARGETS paxformat
    LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX}/lib)

  # TODO(gongxun):
  # We should explicitly specify the headers
  # that need to be exported, and use the syntax of
  # install(FILES,...) to install the header files
  install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/storage
    DESTINATION ${CMAKE_INSTALL_PREFIX}/include/pax
    FILES_MATCHING
    PATTERN "*.h"
  )

  install(FILES ${PAX_COMM_HEADERS}
    DESTINATION ${CMAKE_INSTALL_PREFIX}/include/pax/comm
  )

  install(FILES ${PAX_EXCEPTION_HEADERS}
    DESTINATION ${CMAKE_INSTALL_PREFIX}/include/pax/exceptions
  )

else()

  add_library(pax SHARED ${orc_proto_src} ${pax_proto_src} ${pax_storage_src} ${stats_proto_src} ${pax_exceptions_src}
                  ${pax_access_src} ${pax_comm_src} ${pax_catalog_src} ${pax_vec_src})
  set_target_properties(pax PROPERTIES OUTPUT_NAME pax)
  target_include_directories(pax PUBLIC ${ZTSD_HEADER}  ${CMAKE_CURRENT_SOURCE_DIR} ${CBDB_INCLUDE_DIR})
  target_link_libraries(pax PUBLIC uuid orc_protobuf zstd z postgres)
  add_dependencies(pax generate_protobuf)
  add_custom_command(TARGET pax POST_BUILD
                  COMMAND ${CMAKE_COMMAND} -E
                  copy_if_different $<TARGET_FILE:pax> ${CMAKE_CURRENT_SOURCE_DIR}/../data/pax.so)
endif(BUILD_PAX_FORMAT)

# vec build
if (VEC_BUILD)
  set(VEC_HEADER ${VEC_HOME}/src/include/)

  find_package(PkgConfig REQUIRED)
  pkg_check_modules(GLIB REQUIRED glib-2.0)

  target_include_directories(pax PRIVATE 
    ${VEC_HEADER} # for utils/tuptable_vec.h
    ${CBDB_ROOT_INCLUDE_DIR}  # for arrow-glib/arrow-glib.h and otehr arrow interface
    ${GLIB_INCLUDE_DIRS} # for glib-object.h
  )

  if(BUILD_GTEST)
    target_include_directories(test_main PRIVATE ${VEC_HEADER} ${CBDB_ROOT_INCLUDE_DIR} ${GLIB_INCLUDE_DIRS})
  endif(BUILD_GTEST)

  target_link_libraries(pax PRIVATE arrow)
endif(VEC_BUILD)
