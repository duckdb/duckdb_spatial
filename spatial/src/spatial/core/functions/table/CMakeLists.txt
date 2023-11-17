set(EXTENSION_SOURCES
        ${EXTENSION_SOURCES}
        ${CMAKE_CURRENT_SOURCE_DIR}/st_read_osm.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/st_read_geoparquet.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/geoparquet_reader.cpp
        PARENT_SCOPE
)