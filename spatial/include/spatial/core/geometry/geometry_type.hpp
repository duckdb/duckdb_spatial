#pragma once
#include "spatial/common.hpp"
#include "spatial/core/geometry/bbox.hpp"
#include "spatial/core/geometry/geometry_properties.hpp"
#include "spatial/core/util/cursor.hpp"

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

struct GeometryTypes {
	static bool IsSinglePart(GeometryType type) {
		return type == GeometryType::POINT || type == GeometryType::LINESTRING;
	}

	static bool IsMultiPart(GeometryType type) {
		return type == GeometryType::POLYGON || type == GeometryType::MULTIPOINT ||
		       type == GeometryType::MULTILINESTRING || type == GeometryType::MULTIPOLYGON ||
		       type == GeometryType::GEOMETRYCOLLECTION;
	}

	static bool IsCollection(GeometryType type) {
		return type == GeometryType::MULTIPOINT || type == GeometryType::MULTILINESTRING ||
		       type == GeometryType::MULTIPOLYGON || type == GeometryType::GEOMETRYCOLLECTION;
	}

	static string ToString(GeometryType type) {
		switch (type) {
		case GeometryType::POINT:
			return "POINT";
		case GeometryType::LINESTRING:
			return "LINESTRING";
		case GeometryType::POLYGON:
			return "POLYGON";
		case GeometryType::MULTIPOINT:
			return "MULTIPOINT";
		case GeometryType::MULTILINESTRING:
			return "MULTILINESTRING";
		case GeometryType::MULTIPOLYGON:
			return "MULTIPOLYGON";
		case GeometryType::GEOMETRYCOLLECTION:
			return "GEOMETRYCOLLECTION";
		default:
			return StringUtil::Format("UNKNOWN(%d)", static_cast<int>(type));
		}
	}
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
		// return the type
		const auto type = Load<GeometryType>(const_data_ptr_cast(data.GetPrefix()));
		const auto props = Load<GeometryProperties>(const_data_ptr_cast(data.GetPrefix() + 1));
		props.CheckVersion();
		return type;
	}

	GeometryProperties GetProperties() const {
		const auto props = Load<GeometryProperties>(const_data_ptr_cast(data.GetPrefix() + 1));
		// Check the version
		props.CheckVersion();
		return props;
	}

	bool TryGetCachedBounds(Box2D<double> &bbox) const {
		Cursor cursor(data);

		// Read the header
		auto header_type = cursor.Read<GeometryType>();
		auto properties = cursor.Read<GeometryProperties>();
		auto hash = cursor.Read<uint16_t>();
		(void)hash;

		// Check the version
		properties.CheckVersion();

		if (properties.HasBBox()) {
			cursor.Skip(4); // skip padding

			// Now set the bounding box
			bbox.min.x = cursor.Read<float>();
			bbox.min.y = cursor.Read<float>();
			bbox.max.x = cursor.Read<float>();
			bbox.max.y = cursor.Read<float>();
			return true;
		}

		if (header_type == GeometryType::POINT) {
			cursor.Skip(4); // skip padding

			// Read the point
			auto type = cursor.Read<SerializedGeometryType>();
			D_ASSERT(type == SerializedGeometryType::POINT);
			(void)type;

			auto count = cursor.Read<uint32_t>();
			if (count == 0) {
				// If the point is empty, there is no bounding box
				return false;
			}

			auto x = cursor.Read<double>();
			auto y = cursor.Read<double>();
			bbox.min.x = x;
			bbox.min.y = y;
			bbox.max.x = x;
			bbox.max.y = y;
			return true;
		}
		return false;
	}
};

static_assert(sizeof(geometry_t) == sizeof(string_t), "geometry_t should be the same size as string_t");

} // namespace core

} // namespace spatial