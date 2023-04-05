cmake_minimum_required(VERSION 3.2)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Set extension name here
set(TARGET_NAME spatial)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(CMAKE_CXX_STANDARD 11)

project(${TARGET_NAME})

include_directories(include)
add_subdirectory(src)

include_directories(third_party/ryu/include)
add_subdirectory(third_party/ryu)

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

# Build dependencies
# TODO: in the future we should allow users to dynamically link to their own dependencies
# if they are building locally. If they have a local (compatible) GDAL installation, 
# we should use that instead of building our own. (unless a static build is requested)
execute_process(
    # Generate project
    COMMAND ${CMAKE_COMMAND}
    -G ${CMAKE_GENERATOR}
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DOSX_BUILD_UNIVERSAL=${OSX_BUILD_UNIVERSAL}
    -S ${CMAKE_CURRENT_SOURCE_DIR}/dependencies 
    -B ${CMAKE_BINARY_DIR}/dependencies
    RESULT_VARIABLE DEPENDENCIES_GENERATE_RESULT
)
if(NOT DEPENDENCIES_GENERATE_RESULT EQUAL 0)
    message(FATAL_ERROR "Could not generate dependencies project")
endif()
execute_process(
    # Build project
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR}/dependencies --config ${CMAKE_BUILD_TYPE}
    RESULT_VARIABLE DEPENDENCIES_BUILD_RESULT
)
if(NOT DEPENDENCIES_BUILD_RESULT EQUAL 0)
    message(FATAL_ERROR "Could not build dependencies project")
endif()


# Add dependencies
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${CMAKE_BINARY_DIR}/dependencies/local)
set(CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES} MD.lib) # annoyingly for expat on windows
set(ZLIB_USE_STATIC_LIBS ON)
set(OPENSSL_USE_STATIC_LIBS ON)
find_library(SQLITE3_MEMVFS memvfs)
find_package(ZLIB REQUIRED)
find_package(PROJ REQUIRED)
find_package(SQLite3 REQUIRED)
find_package(GEOS REQUIRED)
find_package(GDAL REQUIRED)
find_package(CURL REQUIRED)
find_package(EXPAT REQUIRED)

# Important: The link order matters, its the reverse order of dependency
target_link_libraries(
  ${EXTENSION_NAME}
  PUBLIC
  GDAL::GDAL
  GEOS::geos_c
  PROJ::proj
  CURL::libcurl
  EXPAT::EXPAT
  OpenSSL::SSL
  OpenSSL::Crypto
  SQLite::SQLite3 
  ZLIB::ZLIB
  ${SQLITE3_MEMVFS})

if(WIN32)
  target_link_libraries(${EXTENSION_NAME} PUBLIC wbemuuid.lib)
endif()

if(APPLE)
  find_library(CoreFoundation_Library CoreFoundation)
  find_library(SystemConfiguration_Library SystemConfiguration)
  target_link_libraries(${EXTENSION_NAME} PUBLIC ${CoreFoundation_Library}
                                                 ${SystemConfiguration_Library})
endif()


# Build extensions
set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
