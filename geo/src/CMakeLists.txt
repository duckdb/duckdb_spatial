add_subdirectory(geo)

set(EXTENSION_SOURCES
        ${EXTENSION_SOURCES}
        ${CMAKE_CURRENT_SOURCE_DIR}/geo_extension.cpp
        PARENT_SCOPE
)