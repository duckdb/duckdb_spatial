add_subdirectory(functions)
set(EXTENSION_SOURCES
        ${EXTENSION_SOURCES}
        ${CMAKE_CURRENT_SOURCE_DIR}/module.cpp
        PARENT_SCOPE
        )