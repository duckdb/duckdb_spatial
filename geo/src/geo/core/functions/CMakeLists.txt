add_subdirectory(scalar)
add_subdirectory(cast)

set(EXTENSION_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/common.cpp
    ${EXTENSION_SOURCES}
    PARENT_SCOPE
)