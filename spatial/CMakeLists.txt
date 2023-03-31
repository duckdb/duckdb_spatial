cmake_minimum_required(VERSION 3.2)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Set extension name here
set(TARGET_NAME spatial)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(CMAKE_CXX_STANDARD 11)

project(${TARGET_NAME})

add_subdirectory(dependencies)

include_directories(include)
add_subdirectory(src)
add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

# Add dependencies
target_link_libraries(${EXTENSION_NAME} PRIVATE dependencies)
target_include_directories(${EXTENSION_NAME} INTERFACE
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/local/include>
        $<INSTALL_INTERFACE:include>)

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
  TARGETS ${EXTENSION_NAME} dependencies
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
