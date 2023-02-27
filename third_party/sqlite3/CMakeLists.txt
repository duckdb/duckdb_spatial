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


if(WIN32)
  add_custom_command(TARGET sqlite POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:sqlite> ${CMAKE_BINARY_DIR}/sqlite3.exe
    DEPENDS sqlite
  )
  install(FILES sqlite3.exe DESTINATION bin)
else()
  include(FindThreads)
  target_link_libraries(sqlite m ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS})
  install(TARGETS sqlite RUNTIME DESTINATION bin)
endif()

install(FILES src/sqlite3.h src/sqlite3ext.h DESTINATION include)
install(TARGETS sqlite3 LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)
install(TARGETS memvfs LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)