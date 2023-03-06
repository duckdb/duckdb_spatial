#pragma once
#include "geo/common.hpp"

namespace geo {

namespace core {

struct GeoTypes {
	static LogicalType POINT_2D;
	static LogicalType LINESTRING_2D;
	static LogicalType POLYGON_2D;
	static LogicalType BOX_2D;

	static LogicalType WKB_BLOB;

	static void Register(ClientContext &context);
};

} // namespace core

} // namespace geo