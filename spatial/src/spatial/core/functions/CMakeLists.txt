add_subdirectory(aggregate)
add_subdirectory(scalar)
add_subdirectory(cast)
add_subdirectory(table)

set(EXTENSION_SOURCES
    ${EXTENSION_SOURCES}
    ${CMAKE_CURRENT_SOURCE_DIR}/common.cpp
    PARENT_SCOPE
)