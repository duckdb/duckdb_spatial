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
private:
	VertexVector data;
public:
	explicit Point(VertexVector data) : data(std::move(data)) { }
    string ToString() const;
	uint32_t SerializedSize() const;

	inline double& X() {
		return data[0].x;
	}
	inline double& Y() {
		return data[0].y;
	}
	inline Vertex& GetVertex() {
		return data[0];
	}
	inline Vertex const& GetVertex() const {
		return data[0];
	}
};

struct LineString {
	friend GeometryFactory;
	VertexVector points;
	explicit LineString(VertexVector data) : points(std::move(data)) { }
	// Common Methods
	string ToString() const;
	uint32_t SerializedSize() const;
 	// Geometry Methods
	double Length() const;
	Geometry Centroid() const;
};

struct Polygon {
	friend GeometryFactory;
    VertexVector *rings;
    uint32_t num_rings;
	explicit Polygon(VertexVector *rings, uint32_t num_rings) : rings(rings), num_rings(num_rings) { }
	// Common Methods
	string ToString() const;
	uint32_t SerializedSize() const;
	// Geometry Methods
    double Area() const;
    double Perimiter() const;
    Geometry Centroid() const;
};

struct MultiPoint {
	friend GeometryFactory;
	Point* points;
	uint32_t num_points;
	explicit MultiPoint(Point *points, uint32_t num_points) : points(points), num_points(num_points) { }
	string ToString() const;
	uint32_t SerializedSize() const;
};

struct MultiLineString {
	friend GeometryFactory;
	LineString* linestrings;
	uint32_t num_linestrings;
	explicit MultiLineString(LineString *linestrings, uint32_t num_linestrings) : linestrings(linestrings), num_linestrings(num_linestrings) { }
	string ToString() const;
	uint32_t SerializedSize() const;
};

struct MultiPolygon {
	friend GeometryFactory;
	Polygon* polygons;
	uint32_t num_polygons;
	explicit MultiPolygon(Polygon *polygons, uint32_t num_polygons) : polygons(polygons), num_polygons(num_polygons) { }
	string ToString() const;
	uint32_t SerializedSize() const;
};

struct GeometryCollection {
	friend GeometryFactory;
	Geometry* geometries;
	uint32_t num_geometries;
	explicit GeometryCollection(Geometry *geometries, uint32_t num_geometries) : geometries(geometries), num_geometries(num_geometries) { }
	string ToString() const;
	uint32_t SerializedSize() const;
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
	explicit Geometry(Point point) : type(GeometryType::POINT), point(std::move(point)) {}
	explicit Geometry(LineString linestring) : type(GeometryType::LINESTRING), linestring(std::move(linestring)) {}
	explicit Geometry(Polygon polygon) : type(GeometryType::POLYGON), polygon(polygon) {}
	explicit Geometry(MultiPoint multipoint) : type(GeometryType::MULTIPOINT), multipoint(multipoint) {}
	explicit Geometry(MultiLineString multilinestring) : type(GeometryType::MULTILINESTRING), multilinestring(multilinestring) {}
	explicit Geometry(MultiPolygon multipolygon) : type(GeometryType::MULTIPOLYGON), multipolygon(multipolygon) {}
	explicit Geometry(GeometryCollection geometrycollection) : type(GeometryType::GEOMETRYCOLLECTION), geometrycollection(geometrycollection) {}

	// Copy constructor (deleted)
	Geometry(const Geometry&) = delete;

	// Copy assignment (deleted)
	Geometry& operator=(const Geometry&) = delete;

	// Move constructor
	Geometry(Geometry &&other) noexcept : type(other.type) {
		switch(type) {
		case GeometryType::POINT:
			new (&point) struct Point(std::move(other.point));
			break;
		case GeometryType::LINESTRING:
			new (&linestring) struct LineString(std::move(other.linestring));
			break;
		case GeometryType::POLYGON:
			new (&polygon) struct Polygon(other.polygon);
			break;
		case GeometryType::MULTIPOINT:
			new (&multipoint) struct MultiPoint(other.multipoint);
			break;
		case GeometryType::MULTILINESTRING:
			new (&multilinestring) struct MultiLineString(other.multilinestring);
			break;
		case GeometryType::MULTIPOLYGON:
			new (&multipolygon) struct MultiPolygon(other.multipolygon);
			break;
		case GeometryType::GEOMETRYCOLLECTION:
			new (&geometrycollection) struct GeometryCollection(other.geometrycollection);
			break;
		default:
			throw NotImplementedException("Unimplemented geometry type");
		}
	}

	// Move assignment
	Geometry& operator=(Geometry &&other) noexcept {
		if (this != &other) {
			// Destroy the current object
			this->~Geometry();
			// Move the other object into this one
			new (this) Geometry(std::move(other));
		}
		return *this;
	}

	// Important: Call the destructor of the correct type
	~Geometry() {
		switch(type) {
		case GeometryType::POINT:
			point.~Point();
			break;
		case GeometryType::LINESTRING:
			linestring.~LineString();
			break;
		case GeometryType::POLYGON:
			polygon.~Polygon();
			break;
		case GeometryType::MULTIPOINT:
			multipoint.~MultiPoint();
			break;
		case GeometryType::MULTILINESTRING:
			multilinestring.~MultiLineString();
			break;
		case GeometryType::MULTIPOLYGON:
			multipolygon.~MultiPolygon();
			break;
		case GeometryType::GEOMETRYCOLLECTION:
			geometrycollection.~GeometryCollection();
			break;
		default:
			D_ASSERT(false);
		}
	}

    // Accessor methods
    inline GeometryType Type() const {
        return type;
    }

    inline Point& GetPoint() {
        D_ASSERT(type == GeometryType::POINT);
        return point;
    }

	inline Point const& GetPoint() const {
		D_ASSERT(type == GeometryType::POINT);
		return point;
	}

    inline LineString& GetLineString() {
        D_ASSERT(type == GeometryType::LINESTRING);
        return linestring;
    }

	inline LineString const& GetLineString() const {
		D_ASSERT(type == GeometryType::LINESTRING);
		return linestring;
	}

    inline Polygon& GetPolygon() {
        D_ASSERT(type == GeometryType::POLYGON);
        return polygon;
    }

	inline Polygon const& GetPolygon() const {
		D_ASSERT(type == GeometryType::POLYGON);
		return polygon;
	}

	inline MultiPoint& GetMultiPoint() {
		D_ASSERT(type == GeometryType::MULTIPOINT);
		return multipoint;
	}

	inline MultiPoint const& GetMultiPoint() const {
		D_ASSERT(type == GeometryType::MULTIPOINT);
		return multipoint;
	}

	inline MultiLineString& GetMultiLineString() {
		D_ASSERT(type == GeometryType::MULTILINESTRING);
		return multilinestring;
	}

	inline MultiLineString const& GetMultiLineString() const {
		D_ASSERT(type == GeometryType::MULTILINESTRING);
		return multilinestring;
	}

	inline MultiPolygon& GetMultiPolygon() {
		D_ASSERT(type == GeometryType::MULTIPOLYGON);
		return multipolygon;
	}

	inline MultiPolygon const& GetMultiPolygon() const {
		D_ASSERT(type == GeometryType::MULTIPOLYGON);
		return multipolygon;
	}

	inline GeometryCollection& GetGeometryCollection() {
		D_ASSERT(type == GeometryType::GEOMETRYCOLLECTION);
		return geometrycollection;
	}

	inline GeometryCollection const& GetGeometryCollection() const {
		D_ASSERT(type == GeometryType::GEOMETRYCOLLECTION);
		return geometrycollection;
	}

	string ToString() const;
	uint32_t SerializedSize() const;
};

struct GeometryPrefix {
	uint8_t flags;
	GeometryType type;
	uint8_t _pad1;
	uint8_t _pad2;

	GeometryPrefix(uint8_t flags, GeometryType type) : flags(flags), type(type), _pad1(0), _pad2(0) { }

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
} // core

} // geo