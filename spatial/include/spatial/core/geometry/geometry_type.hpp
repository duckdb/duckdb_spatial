#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

enum class GeometryType : uint8_t {
	POINT = 0,
	LINESTRING,
	POLYGON,
	MULTIPOINT,
	MULTILINESTRING,
	MULTIPOLYGON,
	GEOMETRYCOLLECTION
};

enum class SerializedGeometryType : uint32_t {
	POINT = 0,
	LINESTRING,
	POLYGON,
	MULTIPOINT,
	MULTILINESTRING,
	MULTIPOLYGON,
	GEOMETRYCOLLECTION
};

} // namespace core

} // namespace spatial