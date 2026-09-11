PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=spatial
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

TEST_FLAGS:=--batch-size 1 --batch-timeout 300

# Stabilize all tests in CI
ifdef CI
TEST_FLAGS+= --stabilize-tests
endif

T ?= $(TEST_FLAGS) "test/*"

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

unittest_relassert:
	build/relassert/test/run $(T)

.PHONY: test_dbscan
test_dbscan:
	cmake -S test/unit -B build/dbscan-unit -DCMAKE_BUILD_TYPE=Release
	cmake --build build/dbscan-unit
	ctest --test-dir build/dbscan-unit --output-on-failure

test_release_internal test_debug_internal test_reldebug_internal: test_dbscan


#### Override the included format target because we have different source tree layout
format:
	find src/spatial -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	cmake-format -i CMakeLists.txt
