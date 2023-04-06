PROJECT(sqlite3)
cmake_minimum_required(VERSION 3.20)

include_directories(${CMAKE_SOURCE_DIR}/src)
add_library(sqlite3 STATIC src/sqlite3.c src/sqlite3.h src/sqlite3ext.h)
add_library(memvfs STATIC src/memvfs.c src/sqlite3ext.h)
add_executable(sqlite src/sqlite3.c src/shell.c src/sqlite3.h src/sqlite3ext.h)
set_target_properties(sqlite PROPERTIES OUTPUT_NAME sqlite3)

# Enable the SQLITE_USE_URI compile-time option
add_definitions(-DSQLITE_USE_URI=1)
# Enable column metadata
add_definitions(-DSQLITE_ENABLE_COLUMN_METADATA=1)
# Enable the RTREE module
add_definitions(-DSQLITE_ENABLE_RTREE=1)

# Compile the memvfs extension
target_compile_definitions(memvfs PUBLIC SQLITE_CORE)
target_compile_definitions(sqlite3 PUBLIC SQLITE_ENABLE_RTREE=1)
target_compile_definitions(sqlite3 PUBLIC SQLITE_ENABLE_COLUMN_METADATA=1)
target_compile_definitions(sqlite3 PUBLIC SQLITE_USE_URI=1)
target_compile_definitions(sqlite3 PUBLIC SQLITE_TEMP_STORE=3)

find_package(Threads REQUIRED)
target_link_libraries(sqlite PUBLIC Threads::Threads)
find_library(MATH_LIBRARY m)
if(MATH_LIBRARY)
    target_link_libraries(sqlite PUBLIC ${MATH_LIBRARY})
endif()
if(CMAKE_DL_LIBS)
  target_link_libraries(sqlite PUBLIC ${CMAKE_DL_LIBS})
endif()

install(TARGETS sqlite RUNTIME DESTINATION bin)
install(FILES src/sqlite3.h src/sqlite3ext.h DESTINATION include)
install(TARGETS sqlite3 LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)
install(TARGETS memvfs LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)