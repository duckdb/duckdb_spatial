add_subdirectory(core)
add_subdirectory(gdal)
add_subdirectory(geos)

set(EXTENSION_SOURCES
        ${EXTENSION_SOURCES}
        PARENT_SCOPE
)