#pragma once
#include "geo/common.hpp"

namespace geo {

namespace gdal {

struct GeoTypes { 

    // A valid "well-known binary" blob of geometry data
    static LogicalType WKB_BLOB;

    static void Register(ClientContext &context);
};

} // namespace gdal

} // namespace geo