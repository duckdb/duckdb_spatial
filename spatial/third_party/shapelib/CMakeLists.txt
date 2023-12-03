add_library(shapelib OBJECT
    ${CMAKE_CURRENT_SOURCE_DIR}/shpopen.c
    ${CMAKE_CURRENT_SOURCE_DIR}/dbfopen.c
    ${CMAKE_CURRENT_SOURCE_DIR}/shptree.c
    ${CMAKE_CURRENT_SOURCE_DIR}/sbnsearch.c
    ${CMAKE_CURRENT_SOURCE_DIR}/shapefil.h
)

set(EXTENSION_SOURCES
        ${EXTENSION_SOURCES}
        $<TARGET_OBJECTS:shapelib>
        PARENT_SCOPE
    )