#!/bin/bash

#!/usr/bin/env bash
set -exo pipefail
GPDB_SRC_PATH="/code/gpdb_src"
GPDB_INSTALL_PATH="/usr/local/cloudberry-db-devel"
GPDB_PAX_SRC_PATH="$GPDB_SRC_PATH/contrib/pax_storage"
GPDB_PAX_GTEST_SRC_PATH="$GPDB_PAX_SRC_PATH/src/cpp/contrib/googletest"
GPDB_PAX_GTEST_SRC_BIN="$GPDB_PAX_SRC_PATH/build/src/cpp/test_main"

compile_pax() {
	git clone https://buildbot:Passw0rd@code.hashdata.xyz/cloudberry/googletest.git $GPDB_PAX_GTEST_SRC_PATH
	source $GPDB_INSTALL_PATH/greenplum_path.sh
	mkdir -p $GPDB_PAX_SRC_PATH/build
	pushd $GPDB_PAX_SRC_PATH/build
	cmake ..
	make
	popd
}

function compile_cmake() {
	wget -O /root/cmake-3.25.1-linux-x86_64.tar.gz https://artifactory.hashdata.xyz/artifactory/utility/cmake-3.25.1-linux-x86_64.tar.gz
	mkdir -p /root/cmake-3.25.1-linux-x86_64
	tar -xvf /root/cmake-3.25.1-linux-x86_64.tar.gz -C /root/cmake-3.25.1-linux-x86_64
	rm -rf /usr/bin/cmake
	rm -rf /opt/rh/llvm-toolset-13.0/root/usr/bin/cmake
	ln -s /root/cmake-3.25.1-linux-x86_64/cmake-3.25.1-linux-x86_64/bin/cmake /usr/bin/cmake
	ln -s /root/cmake-3.25.1-linux-x86_64/cmake-3.25.1-linux-x86_64/bin/cmake /opt/rh/llvm-toolset-13.0/root/usr/bin/cmake
}

function run_unit() {
	$GPDB_PAX_GTEST_SRC_BIN
}

main() {
	compile_cmake
	compile_pax
	run_unit
}

main
