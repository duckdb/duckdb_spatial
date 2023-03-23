add_subdirectory(scalar)

set(EXTENSION_SOURCES
    ${EXTENSION_SOURCES}
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregate.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/cast.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/scalar.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common.cpp
    PARENT_SCOPE
    )