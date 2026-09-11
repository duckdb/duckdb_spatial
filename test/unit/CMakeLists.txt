cmake_minimum_required(VERSION 3.5)
project(spatial_dbscan_tests LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

get_filename_component(SPATIAL_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../.." ABSOLUTE)
add_executable(spatial_dbscan_test
    test_dbscan.cpp
    "${SPATIAL_ROOT}/src/spatial/geometry/dbscan_engine.cpp")
target_include_directories(spatial_dbscan_test PRIVATE "${SPATIAL_ROOT}/src")
target_include_directories(spatial_dbscan_test SYSTEM PRIVATE
    "${SPATIAL_ROOT}/duckdb/src/include")

option(DBSCAN_SANITIZERS "Run DBSCAN engine tests with address and undefined behavior sanitizers" OFF)
if(DBSCAN_SANITIZERS AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(spatial_dbscan_test PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_libraries(spatial_dbscan_test PRIVATE -fsanitize=address,undefined)
endif()

enable_testing()
add_test(NAME spatial_dbscan_engine COMMAND spatial_dbscan_test)
