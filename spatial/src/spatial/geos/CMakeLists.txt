add_subdirectory(functions)
set(EXTENSION_SOURCES
        ${EXTENSION_SOURCES}
        ${CMAKE_CURRENT_SOURCE_DIR}/module.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/geos_wrappers.cpp
        PARENT_SCOPE
        )