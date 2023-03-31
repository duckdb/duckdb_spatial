add_subdirectory(spatial)

set(EXTENSION_SOURCES
        ${EXTENSION_SOURCES}
        ${CMAKE_CURRENT_SOURCE_DIR}/spatial_extension.cpp
        PARENT_SCOPE
)