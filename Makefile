.PHONY: all clean format debug release duckdb_debug duckdb_release pull update

all: release

MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJ_DIR := $(dir $(MKFILE_PATH))

ifneq (${OSX_BUILD_ARCH}, )
    CMAKE_VARS:=${CMAKE_VARS} -DOSX_BUILD_ARCH=${OSX_BUILD_ARCH}
endif

ifeq (${STATIC_LIBCPP}, 1)
	STATIC_LIBCPP=-DSTATIC_LIBCPP=TRUE
endif

ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif

# Enable VCPKG for this build
ifneq ("${VCPKG_TOOLCHAIN_PATH}", "")
	CMAKE_VARS:=${CMAKE_VARS} -DCMAKE_TOOLCHAIN_FILE='${VCPKG_TOOLCHAIN_PATH}' -DVCPKG_BUILD=1
endif
ifneq ("${VCPKG_TARGET_TRIPLET}", "")
	CMAKE_VARS:=${CMAKE_VARS} -DVCPKG_TARGET_TRIPLET='${VCPKG_TARGET_TRIPLET}'
endif
ifeq (${USE_MERGED_VCPKG_MANIFEST}, 1)
	CMAKE_VARS:=${CMAKE_VARS} -DVCPKG_MANIFEST_DIR='${PROJ_DIR}build/extension_configuration'
else
	CMAKE_VARS:=${CMAKE_VARS} -DVCPKG_MANIFEST_DIR='${PROJ_DIR}'
endif

BUILD_FLAGS=-DEXTENSION_STATIC_BUILD=1 ${OSX_BUILD_UNIVERSAL_FLAG} ${STATIC_LIBCPP} ${CMAKE_VARS}
ifeq (${BUILD_SHELL}, 0)
	BUILD_FLAGS += -DBUILD_SHELL=0
endif

CLIENT_FLAGS :=

# These flags will make DuckDB build the extension
EXTENSION_FLAGS=-DENABLE_SANITIZER=OFF -DDUCKDB_EXTENSION_NAMES="spatial" -DDUCKDB_EXTENSION_SPATIAL_PATH="$(PROJ_DIR)" -DDUCKDB_EXTENSION_SPATIAL_SHOULD_LINK="TRUE" -DDUCKDB_EXTENSION_SPATIAL_INCLUDE_PATH="$(PROJ_DIR)spatial/include"

pull:
	git submodule init
	git submodule update --recursive --remote

clean:
	rm -rf build
	rm -rf testext
	cd duckdb && make clean

# Main build
debug:
	mkdir -p  build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Debug ${BUILD_FLAGS} -S ./duckdb/ -B build/debug && \
	cmake --build build/debug --config Debug

release:
	mkdir -p build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Release ${BUILD_FLAGS} -S ./duckdb/ -B build/release && \
	cmake --build build/release --config Release

# Client build
debug_js: CLIENT_FLAGS=-DBUILD_NODE=1 -DBUILD_JSON_EXTENSION=1
debug_js: debug

debug_r: CLIENT_FLAGS=-DBUILD_R=1
debug_r: debug

debug_python: CLIENT_FLAGS=-DBUILD_PYTHON=1 -DBUILD_JSON_EXTENSION=1 -DBUILD_FTS_EXTENSION=1 -DBUILD_TPCH_EXTENSION=1 -DBUILD_VISUALIZER_EXTENSION=1 -DBUILD_TPCDS_EXTENSION=1
debug_python: debug

release_js: CLIENT_FLAGS=-DBUILD_NODE=1 -DBUILD_JSON_EXTENSION=1
release_js: release

release_r: CLIENT_FLAGS=-DBUILD_R=1
release_r: release

release_python: CLIENT_FLAGS=-DBUILD_PYTHON=1 -DBUILD_JSON_EXTENSION=1 -DBUILD_FTS_EXTENSION=1 -DBUILD_TPCH_EXTENSION=1 -DBUILD_VISUALIZER_EXTENSION=1 -DBUILD_TPCDS_EXTENSION=1
release_python: release

# Main tests
test: test_release

test_release: release
	./build/release/test/unittest --test-dir . "test/sql/*"

test_debug: debug
	./build/debug/test/unittest --test-dir . "test/sql/*"

test_debug_lldb: debug
	lldb ./build/debug/test/unittest -- --test-dir . "test/sql/*"

# Client tests
test_js: test_debug_js
test_debug_js: debug_js
	cd duckdb/tools/nodejs && npm run test-path -- "../../../test/nodejs/**/*.js"

test_release_js: release_js
	cd duckdb/tools/nodejs && npm run test-path -- "../../../test/nodejs/**/*.js"

test_python: test_debug_python
test_debug_python: debug_python
	cd test/python && python3 -m pytest

test_release_python: release_python
	cd test/python && python3 -m pytest

format:
	find spatial/src/ -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	find spatial/include -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	cmake-format -i spatial/src/CMakeLists.txt

update:
	git submodule update --remote --merge