#pragma once

#include "geo/common.hpp"
#include "geo/core/geometry/vertex_vector.hpp"

namespace geo {

namespace core {

struct Geometry;
struct GeometryContext;

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
	friend GeometryContext;
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
	friend GeometryContext;
	VertexVector points;
	explicit LineString(VertexVector data) : points(std::move(data)) { }

    double Length() const;
    Geometry Centroid() const;
    string ToString() const;

	uint32_t SerializedSize() const;

	double Distance(const Geometry &other) const;
};

struct Polygon {
	friend GeometryContext;
    VertexVector *rings;
    uint32_t num_rings;
	explicit Polygon(VertexVector *rings, uint32_t num_rings) : rings(rings), num_rings(num_rings) { }

    double Area() const;
    double Perimiter() const;
    Geometry Centroid() const;
    string ToString() const;

	uint32_t SerializedSize() const;

	double Distance(const Geometry &other) const;
};

struct Geometry {
	friend GeometryContext;
private:
    GeometryType type;
    union {
        Point point;
        LineString linestring;
        Polygon polygon;
    };

public:
	explicit Geometry(Point point) : type(GeometryType::POINT), point(std::move(point)) {}
	explicit Geometry(LineString linestring) : type(GeometryType::LINESTRING), linestring(std::move(linestring)) {}
	explicit Geometry(Polygon polygon) : type(GeometryType::POLYGON), polygon(polygon) {}

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
			new (&polygon) struct Polygon(std::move(other.polygon));
			break;
		default:
			D_ASSERT(false);
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

	string ToString() const;
	uint32_t SerializedSize() const;

	// Forwarding Methods
	double Distance(const Geometry &other) const;
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

	uint32_t Serialize(data_ptr_t dst) const {
		*((GeometryPrefix*)dst) = *this;
		return sizeof(GeometryPrefix);
	}
};

static_assert(sizeof(GeometryPrefix) == string_t::PREFIX_BYTES, "GeometryPrefix should fit in string_t prefix");
} // core

} // geo