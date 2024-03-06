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

// A serialized geometry
class geometry_t {
private:
	string_t data;

public:
	geometry_t() = default;
	// NOLINTNEXTLINE
	explicit geometry_t(string_t data) : data(data) {
	}

	// NOLINTNEXTLINE
	operator string_t() const {
		return data;
	}

	GeometryType GetType() const {
		return Load<GeometryType>(const_data_ptr_cast(data.GetPrefix()));
	}
	GeometryProperties GetProperties() const {
		return Load<GeometryProperties>(const_data_ptr_cast(data.GetPrefix() + 1));
	}
	uint16_t GetHash() const {
		return Load<uint16_t>(const_data_ptr_cast(data.GetPrefix() + 2));
	}
};

static_assert(sizeof(geometry_t) == sizeof(string_t), "geometry_t should be the same size as string_t");

} // namespace core

} // namespace spatial