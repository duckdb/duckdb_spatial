cmake_minimum_required(VERSION 3.2)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Set extension name here
set(TARGET_NAME geo)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(CMAKE_CXX_STANDARD 11)

project(${TARGET_NAME})

include_directories(include)
add_subdirectory(src)
add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

# Add dependencies
find_library(SQLITE3 sqlite3)
find_library(SQLITE3_MEMVFS memvfs)
find_package(PROJ REQUIRED)
find_package(GEOS REQUIRED)
find_package(GDAL REQUIRED)
find_package(CURL REQUIRED)
find_package(Arrow REQUIRED)
find_package(EXPAT REQUIRED)

target_link_libraries(
  ${EXTENSION_NAME}
  PUBLIC PROJ::proj
         GEOS::geos_c
         GDAL::GDAL
         CURL::libcurl
         Arrow::arrow_static
         EXPAT::EXPAT
         ${SQLITE3}
         ${SQLITE3_MEMVFS})

# Build extensions
set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
