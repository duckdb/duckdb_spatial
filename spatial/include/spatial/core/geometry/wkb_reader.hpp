#pragma once
#include "spatial/common.hpp"
#include "spatial/core/geometry/vertex_vector.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"

namespace spatial {

namespace core {

enum class WKBByteOrder : uint8_t {
	XDR = 0, // Big endian
	NDR = 1, // Little endian
};

enum class WKBGeometryType : uint32_t {
	POINT = 1,
	LINESTRING = 2,
	POLYGON = 3,
	MULTIPOINT = 4,
	MULTILINESTRING = 5,
	MULTIPOLYGON = 6,
	GEOMETRYCOLLECTION = 7
};

struct WKBFlags {
	WKBGeometryType type;
	bool has_z;
	bool has_m;
	bool has_srid;
	uint32_t srid;

	explicit WKBFlags(WKBGeometryType type, bool has_z, bool has_m, bool has_srid, uint32_t srid)
	    : type(type), has_z(has_z), has_m(has_m), has_srid(has_srid), srid(srid) {
	}
};

struct WKBReader {
private:
	GeometryFactory &factory;
	const char *data;
	uint32_t length;
	uint32_t cursor;

public:
	template <WKBByteOrder ORDER>
	uint32_t ReadInt() = delete;

	template <WKBByteOrder ORDER>
	double ReadDouble() = delete;

	WKBReader(GeometryFactory &factory, const char *data, uint32_t length)
	    : factory(factory), data(data), length(length), cursor(0) {
	}

	void Reset() {
		cursor = 0;
	}

	Geometry ReadGeometry();
	Point ReadPoint();
	LineString ReadLineString();
	Polygon ReadPolygon();
	MultiPoint ReadMultiPoint();
	MultiLineString ReadMultiLineString();
	MultiPolygon ReadMultiPolygon();
	GeometryCollection ReadGeometryCollection();

private:
	template <WKBByteOrder ORDER>
	WKBFlags ReadFlags();
	template <WKBByteOrder ORDER>
	Geometry ReadGeometryBody();
	template <WKBByteOrder ORDER>
	Point ReadPointBody();
	template <WKBByteOrder ORDER>
	LineString ReadLineStringBody();
	template <WKBByteOrder ORDER>
	Polygon ReadPolygonBody();
	template <WKBByteOrder ORDER>
	MultiPoint ReadMultiPointBody();
	template <WKBByteOrder ORDER>
	MultiLineString ReadMultiLineStringBody();
	template <WKBByteOrder ORDER>
	MultiPolygon ReadMultiPolygonBody();
	template <WKBByteOrder ORDER>
	GeometryCollection ReadGeometryCollectionBody();
};

} // namespace core

} // namespace spatial
