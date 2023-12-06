add_subdirectory(geometry)
add_subdirectory(functions)
add_subdirectory(io)

set(EXTENSION_SOURCES
        ${EXTENSION_SOURCES}
        ${CMAKE_CURRENT_SOURCE_DIR}/module.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/types.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/optimizer_rules.cpp
        PARENT_SCOPE
)