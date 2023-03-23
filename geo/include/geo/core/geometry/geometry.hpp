#pragma once

#include "geo/common.hpp"
#include "geo/core/geometry/vertex_vector.hpp"

namespace geo {

namespace core {

struct Geometry;
struct GeometryFactory;

enum class GeometryType : uint8_t {
	POINT,
	LINESTRING,
	POLYGON,
	MULTIPOINT,
	MULTILINESTRING,
	MULTIPOLYGON,
	GEOMETRYCOLLECTION
};

struct Point {
	friend GeometryFactory;
public:
	VertexVector data;
	explicit Point(VertexVector data) : data(data) {
	}
	string ToString() const;

	bool IsEmpty() const {
		return data.Count() == 0;
	}
	inline Vertex &GetVertex() {
		return data[0];
	}
	inline const Vertex &GetVertex() const {
		return data[0];
	}
};

struct LineString {
	friend GeometryFactory;
	VertexVector points;
	explicit LineString(VertexVector data) : points(data) {
	}
	// Common Methods
	string ToString() const;
	// Geometry Methods
	bool IsEmpty() const;
	double Length() const;
	Geometry Centroid() const;
	uint32_t Count() const;
};

struct Polygon {
	friend GeometryFactory;
	VertexVector *rings;
	uint32_t num_rings;
	explicit Polygon(VertexVector *rings, uint32_t num_rings) : rings(rings), num_rings(num_rings) {
	}
	// Common Methods
	string ToString() const;
	// Geometry Methods
	double Area() const;
	bool IsEmpty() const;
	double Perimiter() const;
	Geometry Centroid() const;
};

struct MultiPoint {
	friend GeometryFactory;
	Point *points;
	uint32_t num_points;
	explicit MultiPoint(Point *points, uint32_t num_points) : points(points), num_points(num_points) {
	}
	string ToString() const;
	bool IsEmpty() const;
};

struct MultiLineString {
	friend GeometryFactory;
	LineString *linestrings;
	uint32_t num_linestrings;
	explicit MultiLineString(LineString *linestrings, uint32_t num_linestrings)
	    : linestrings(linestrings), num_linestrings(num_linestrings) {
	}
	string ToString() const;
	// Geometry Methods
	bool IsEmpty() const;
	double Length() const;
};

struct MultiPolygon {
	friend GeometryFactory;
	Polygon *polygons;
	uint32_t num_polygons;
	explicit MultiPolygon(Polygon *polygons, uint32_t num_polygons) : polygons(polygons), num_polygons(num_polygons) {
	}
	bool IsEmpty() const;
	double Area() const;
	string ToString() const;
};

struct GeometryCollection {
	friend GeometryFactory;
	Geometry *geometries;
	uint32_t num_geometries;
	explicit GeometryCollection(Geometry *geometries, uint32_t num_geometries)
	    : geometries(geometries), num_geometries(num_geometries) {
	}
	string ToString() const;
	bool IsEmpty() const;
	template<class AGG, class RESULT_TYPE>
	RESULT_TYPE Aggregate(AGG agg, RESULT_TYPE zero) const;
};

struct Geometry {
	friend GeometryFactory;

private:
	GeometryType type;
	union {
		Point point;
		LineString linestring;
		Polygon polygon;
		MultiPoint multipoint;
		MultiLineString multilinestring;
		MultiPolygon multipolygon;
		GeometryCollection geometrycollection;
	};

public:
	explicit Geometry(Point point) : type(GeometryType::POINT), point(point) {
	}
	explicit Geometry(LineString linestring) : type(GeometryType::LINESTRING), linestring(linestring) {
	}
	explicit Geometry(Polygon polygon) : type(GeometryType::POLYGON), polygon(polygon) {
	}
	explicit Geometry(MultiPoint multipoint) : type(GeometryType::MULTIPOINT), multipoint(multipoint) {
	}
	explicit Geometry(MultiLineString multilinestring)
	    : type(GeometryType::MULTILINESTRING), multilinestring(multilinestring) {
	}
	explicit Geometry(MultiPolygon multipolygon) : type(GeometryType::MULTIPOLYGON), multipolygon(multipolygon) {
	}
	explicit Geometry(GeometryCollection geometrycollection)
	    : type(GeometryType::GEOMETRYCOLLECTION), geometrycollection(geometrycollection) {
	}

	// Accessor methods
	inline GeometryType Type() const {
		return type;
	}

	inline Point &GetPoint() {
		D_ASSERT(type == GeometryType::POINT);
		return point;
	}

	inline Point const &GetPoint() const {
		D_ASSERT(type == GeometryType::POINT);
		return point;
	}

	inline LineString &GetLineString() {
		D_ASSERT(type == GeometryType::LINESTRING);
		return linestring;
	}

	inline LineString const &GetLineString() const {
		D_ASSERT(type == GeometryType::LINESTRING);
		return linestring;
	}

	inline Polygon &GetPolygon() {
		D_ASSERT(type == GeometryType::POLYGON);
		return polygon;
	}

	inline Polygon const &GetPolygon() const {
		D_ASSERT(type == GeometryType::POLYGON);
		return polygon;
	}

	inline MultiPoint &GetMultiPoint() {
		D_ASSERT(type == GeometryType::MULTIPOINT);
		return multipoint;
	}

	inline MultiPoint const &GetMultiPoint() const {
		D_ASSERT(type == GeometryType::MULTIPOINT);
		return multipoint;
	}

	inline MultiLineString &GetMultiLineString() {
		D_ASSERT(type == GeometryType::MULTILINESTRING);
		return multilinestring;
	}

	inline MultiLineString const &GetMultiLineString() const {
		D_ASSERT(type == GeometryType::MULTILINESTRING);
		return multilinestring;
	}

	inline MultiPolygon &GetMultiPolygon() {
		D_ASSERT(type == GeometryType::MULTIPOLYGON);
		return multipolygon;
	}

	inline MultiPolygon const &GetMultiPolygon() const {
		D_ASSERT(type == GeometryType::MULTIPOLYGON);
		return multipolygon;
	}

	inline GeometryCollection &GetGeometryCollection() {
		D_ASSERT(type == GeometryType::GEOMETRYCOLLECTION);
		return geometrycollection;
	}

	inline GeometryCollection const &GetGeometryCollection() const {
		D_ASSERT(type == GeometryType::GEOMETRYCOLLECTION);
		return geometrycollection;
	}

	string ToString() const;
};

template<class AGG, class RESULT_TYPE>
RESULT_TYPE GeometryCollection::Aggregate(AGG agg, RESULT_TYPE zero) const {
	RESULT_TYPE result = zero;
	for (idx_t i = 0; i < num_geometries; i++) {
		auto &geometry = geometries[i];
		if(geometry.Type() == GeometryType::GEOMETRYCOLLECTION) {
			result = geometry.GetGeometryCollection().Aggregate(agg, result);
		} else {
			result = agg(geometry, result);
		}
	}
	return result;
}

struct GeometryPrefix {
	uint8_t flags;
	GeometryType type;
	uint8_t _pad1;
	uint8_t _pad2;

	GeometryPrefix(uint8_t flags, GeometryType type) : flags(flags), type(type), _pad1(0), _pad2(0) {
	}

	uint32_t SerializedSize() const {
		return sizeof(GeometryPrefix);
	}

	void Serialize(data_ptr_t &dst) const {
		Store<uint8_t>(flags, dst);
		dst += sizeof(uint8_t);
		Store<uint8_t>((uint8_t)type, dst);
		dst += sizeof(GeometryType);
		Store<uint8_t>(_pad1, dst);
		dst += sizeof(uint8_t);
		Store<uint8_t>(_pad2, dst);
		dst += sizeof(uint8_t);
	}
};
static_assert(sizeof(GeometryPrefix) == string_t::PREFIX_BYTES, "GeometryPrefix should fit in string_t prefix");
} // namespace core

} // namespace geo