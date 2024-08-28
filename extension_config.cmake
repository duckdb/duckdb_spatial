# Extension from this repo
duckdb_extension_load(spatial
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
        INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/spatial/include
        TEST_DIR ${CMAKE_CURRENT_LIST_DIR}/test/sql
        LOAD_TESTS
)