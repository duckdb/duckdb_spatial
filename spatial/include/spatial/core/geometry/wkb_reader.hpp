#pragma once
#include "spatial/common.hpp"
#include "spatial/core/geometry/vertex_vector.hpp"
#include "spatial/core/geometry/geometry.hpp"

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
	Geometry ReadGeometryImpl();
	template <WKBByteOrder ORDER>
	Point ReadPointImpl();
	template <WKBByteOrder ORDER>
	LineString ReadLineStringImpl();
	template <WKBByteOrder ORDER>
	Polygon ReadPolygonImpl();
	template <WKBByteOrder ORDER>
	MultiPoint ReadMultiPointImpl();
	template <WKBByteOrder ORDER>
	MultiLineString ReadMultiLineStringImpl();
	template <WKBByteOrder ORDER>
	MultiPolygon ReadMultiPolygonImpl();
	template <WKBByteOrder ORDER>
	GeometryCollection ReadGeometryCollectionImpl();
};

} // namespace core

} // namespace spatial
