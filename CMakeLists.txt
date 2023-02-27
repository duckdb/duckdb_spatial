cmake_minimum_required(VERSION 3.20)

project(duckdb-geo-build)

# Install and build dependencies locally
include(ExternalProject)
set(LOCAL_INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/local)
set_directory_properties(PROPERTIES EP_BASE ${CMAKE_CURRENT_BINARY_DIR}/base)

# SQLite3
ExternalProject_Add(
    SQLite3
    URL ${CMAKE_CURRENT_SOURCE_DIR}/third_party/sqlite3.zip
    CONFIGURE_HANDLED_BY_BUILD TRUE
    CMAKE_ARGS 
    -DCMAKE_INSTALL_PREFIX:PATH=${LOCAL_INSTALL_DIR}
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
)

# zlib
ExternalProject_Add(
    zlib
    URL ${CMAKE_CURRENT_SOURCE_DIR}/third_party/zlib1213.zip
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND ./configure --prefix=${LOCAL_INSTALL_DIR} --static
    BUILD_COMMAND make
    INSTALL_COMMAND make install
)

# OpenSSL
ExternalProject_Add(
    OpenSSL
    DEPENDS zlib
    URL ${CMAKE_CURRENT_SOURCE_DIR}/third_party/openssl-openssl-3.0.7.zip
    BUILD_IN_SOURCE 1    
    CONFIGURE_COMMAND ./Configure --prefix=${LOCAL_INSTALL_DIR} --openssldir=${LOCAL_INSTALL_DIR} no-shared
    BUILD_COMMAND make build_libs
    INSTALL_COMMAND make install_sw
)

# CURL
ExternalProject_Add(
    CURL
    DEPENDS OpenSSL
    URL ${CMAKE_CURRENT_SOURCE_DIR}/third_party/curl-7.87.0.zip
    CONFIGURE_HANDLED_BY_BUILD TRUE
    CMAKE_ARGS 
    -DCMAKE_INSTALL_PREFIX:PATH=${LOCAL_INSTALL_DIR}
    -DCMAKE_PREFIX_PATH=${LOCAL_INSTALL_DIR}
    -DCMAKE_DEBUG_POSTFIX=${} # Do not append "d" to the library name when building in debug mode
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DBUILD_SHARED_LIBS=OFF
    -DHTTP_ONLY=ON
    # Disable some defaults for now
    -DCURL_USE_LIBSSH2=OFF
    -DCURL_USE_LIBPSL=OFF
    -DCURL_USE_LIBSSH=OFF
    -DOPENSSL_USE_STATIC_LIBS=TRUE # Propagate to FindOpenSSL.cmake
    -DOPENSSL_ROOT_DIR=${LOCAL_INSTALL_DIR}
    -DOpenSSL_ROOT=${LOCAL_INSTALL_DIR}
)

#Expat
ExternalProject_Add(
    Expat
    DEPENDS CURL
    URL ${CMAKE_CURRENT_SOURCE_DIR}/third_party/expat-2.5.0.tar.bz2
    CONFIGURE_HANDLED_BY_BUILD TRUE
    CMAKE_ARGS 
    # CMake options
    -DCMAKE_INSTALL_PREFIX:PATH=${LOCAL_INSTALL_DIR}
    -DCMAKE_PREFIX_PATH=${LOCAL_INSTALL_DIR}
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    # EXPAT options
    -DEXPAT_SHARED_LIBS=OFF
    -DEXPAT_BUILD_TESTS=OFF
    -DEXPAT_BUILD_EXAMPLES=OFF
    -DEXPAT_BUILD_FUZZERS=OFF
    -DEXPAT_BUILD_DOCS=OFF
)

# Arrow
ExternalProject_Add(
    Arrow
    DEPENDS OpenSSL CURL
    URL ${CMAKE_CURRENT_SOURCE_DIR}/third_party/apache-arrow-10.0.1.zip
    SOURCE_SUBDIR cpp
    CMAKE_ARGS
    -DCMAKE_PREFIX_PATH=${LOCAL_INSTALL_DIR}
    -DCMAKE_INSTALL_PREFIX:PATH=${LOCAL_INSTALL_DIR}
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DARROW_BUILD_STATIC=ON
    -DARROW_BUILD_SHARED=OFF
    -DARROW_SIMD_LEVEL=NONE
    -DARROW_PARQUET=ON
    -DARROW_DATASET=ON
    -DARROW_WITH_RE2=OFF
    -DARROW_WITH_UTF8PROC=OFF
    -DARROW_BUILD_TESTS=OFF
)

# PROJ
ExternalProject_Add(
    PROJ
    DEPENDS SQLite3
    URL ${CMAKE_CURRENT_SOURCE_DIR}/third_party/proj-9.1.1.zip
    CONFIGURE_HANDLED_BY_BUILD TRUE
    CMAKE_ARGS
    # CMake options
    -DCMAKE_INSTALL_PREFIX:PATH=${LOCAL_INSTALL_DIR}
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_PREFIX_PATH=${LOCAL_INSTALL_DIR}
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    # PROJ options
    -DBUILD_SHARED_LIBS=OFF
    -DBUILD_APPS=OFF
    -DBUILD_TESTING=OFF
    -DENABLE_CURL=OFF
    -DENABLE_TIFF=OFF
)

# GEOS
ExternalProject_Add(
    GEOS
    DEPENDS CURL
    GIT_REPOSITORY "https://github.com/Maxxen/geos.git"
    # GEOS now requires c++14, but this commit fixes a build bug an and is still c++11
    # At some point we should fork..? Or go back and use the stable c api.
    GIT_TAG "312f771f997dd1023ad264c08773756bf76c1f57"
    GIT_SHALLOW TRUE
    UPDATE_DISCONNECTED TRUE
    CONFIGURE_HANDLED_BY_BUILD TRUE
    CMAKE_ARGS 
    # CMake options
    -DCMAKE_INSTALL_PREFIX:PATH=${LOCAL_INSTALL_DIR}
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    # GEOS options
    -DBUILD_SHARED_LIBS=OFF
    -DBUILD_TESTING=OFF
    -DBUILD_DOCUMENTATION=OFF
    -DBUILD_ASTYLE=OFF
    -DBUILD_GEOSOP=OFF
)

# GDAL
ExternalProject_Add(
    GDAL
    DEPENDS PROJ GEOS CURL Expat SQLite3 Arrow
    GIT_REPOSITORY "https://github.com/OSGeo/gdal.git"
    GIT_TAG "master" # TODO: Change this next release
    GIT_SHALLOW TRUE
    UPDATE_DISCONNECTED TRUE
    CONFIGURE_HANDLED_BY_BUILD TRUE
    # PATCH_COMMAND git apply ${CMAKE_CURRENT_SOURCE_DIR}/third_party/find_sqlite3.patch # Patch this until we can get it merged
    CMAKE_ARGS 
    # CMake options
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_INSTALL_PREFIX:PATH=${LOCAL_INSTALL_DIR}
    -DCMAKE_PREFIX_PATH=${LOCAL_INSTALL_DIR}
    -DCMAKE_MODULE_PATH=${LOCAL_INSTALL_DIR}/lib/cmake
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DCMAKE_FIND_ROOT_PATH=${LOCAL_INSTALL_DIR}
    # GDAL Options
    -DGDAL_OBJECT_LIBRARIES_POSITION_INDEPENDENT_CODE=ON # this is needed for GDAL to build with -fPIC
    -DBUILD_TESTING=OFF
    -DBUILD_APPS=OFF
    # Build static library
    -DBUILD_SHARED_LIBS=OFF
    # Disable apps
    -DBUILD_APPS=OFF
    
    # Arrow
    -DGDAL_USE_ARROW=ON
    -DARROW_USE_STATIC_LIBRARIES=ON

    # Disable all external drivers unless explicitly enabled
    -DGDAL_USE_EXTERNAL_LIBS=OFF
    -DGDAL_USE_INTERNAL_LIBS=ON

    # Supported drivers
    -DGDAL_USE_GEOS=ON
    -DGDAL_USE_SQLITE3=ON
    -DGDAL_USE_EXPAT=ON
    -DGDAL_USE_CURL=ON
    -DGDAL_USE_OPENSSL=ON
    -DOPENSSL_ROOT_DIR=${LOCAL_INSTALL_DIR}
    -DOPENSSL_USE_STATIC_LIBS=TRUE # Propagate to FindOpenSSL.cmake

    # remove optional gdal drivers
    -DGDAL_BUILD_OPTIONAL_DRIVERS=OFF
    -DOGR_BUILD_OPTIONAL_DRIVERS=ON

    # Remove bindings
    -DBUILD_PYTHON_BINDINGS=OFF
)

# DuckDB
ExternalProject_Add(
    DuckDB
    DEPENDS GDAL PROJ GEOS
    GIT_REPOSITORY "https://github.com/duckdb/duckdb.git"
    GIT_TAG "v0.7.1"
    GIT_SHALLOW TRUE
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/duckdb
    CONFIGURE_HANDLED_BY_BUILD TRUE
    UPDATE_DISCONNECTED 1 # Dont try to pull from git on every build
    BUILD_ALWAYS 1 # Since we technically change the source (when including the extension lib, ensure we always build)
    INSTALL_COMMAND ""
    CMAKE_ARGS
    # CMake options
    -DCMAKE_INSTALL_PREFIX:PATH=${LOCAL_INSTALL_DIR}
    -DCMAKE_PREFIX_PATH=${LOCAL_INSTALL_DIR}
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    # DuckDB options
    # Note: ExternalProject_Add is really anal when passing argument containing quotes,
    # so dont do that when passing the extension paths here.
    -DEXTENSION_STATIC_BUILD=1
    -DBUILD_TPCH_EXTENSION=OFF
    -DBUILD_PARQUET_EXTENSION=OFF
    #-DBUILD_UNITTESTS=OFF
    -DDUCKDB_OOT_EXTENSION_NAMES=geo
    -DDUCKDB_OOT_EXTENSION_GEO_PATH=${CMAKE_CURRENT_SOURCE_DIR}/geo
    -DDUCKDB_OOT_EXTENSION_GEO_INCLUDE_PATH=${CMAKE_CURRENT_SOURCE_DIR}/geo/include
    -DDUCKDB_OOT_EXTENSION_GEO_SHOULD_LINK=TRUE
    -DENABLE_SANITIZER=OFF
)