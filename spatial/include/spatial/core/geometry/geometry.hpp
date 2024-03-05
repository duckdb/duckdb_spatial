#pragma once

#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry_properties.hpp"
#include "spatial/core/geometry/vertex_vector.hpp"
#include "spatial/core/geometry/cursor.hpp"
#include "spatial/core/geometry/geometry_type.hpp"

namespace spatial {

namespace core {

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

//------------------------------------------------------------------------------
// Geometry Objects
//------------------------------------------------------------------------------

template <class T>
class IteratorPair {
	T *begin_ptr;
	T *end_ptr;

public:
	IteratorPair(T *begin_ptr, T *end_ptr) : begin_ptr(begin_ptr), end_ptr(end_ptr) {
	}

	T *begin() {
		return begin_ptr;
	}

	T *end() {
		return end_ptr;
	}
};

template <class T>
class ConstIteratorPair {
	const T *begin_ptr;
	const T *end_ptr;

public:
	ConstIteratorPair(const T *begin_ptr, const T *end_ptr) : begin_ptr(begin_ptr), end_ptr(end_ptr) {
	}

	const T *begin() {
		return begin_ptr;
	}

	const T *end() {
		return end_ptr;
	}
};

struct Utils {
	static string format_coord(double d);
	static string format_coord(double x, double y);
	static string format_coord(double x, double y, double z);
	static string format_coord(double x, double y, double z, double m);

	static inline float DoubleToFloatDown(double d) {
		if (d > static_cast<double>(std::numeric_limits<float>::max())) {
			return std::numeric_limits<float>::max();
		}
		if (d <= static_cast<double>(std::numeric_limits<float>::lowest())) {
			return std::numeric_limits<float>::lowest();
		}

		auto f = static_cast<float>(d);
		if (static_cast<double>(f) <= d) {
			return f;
		}
		return std::nextafter(f, std::numeric_limits<float>::lowest());
	}

	static inline float DoubleToFloatUp(double d) {
		if (d >= static_cast<double>(std::numeric_limits<float>::max())) {
			return std::numeric_limits<float>::max();
		}
		if (d < static_cast<double>(std::numeric_limits<float>::lowest())) {
			return std::numeric_limits<float>::lowest();
		}

		auto f = static_cast<float>(d);
		if (static_cast<double>(f) >= d) {
			return f;
		}
		return std::nextafter(f, std::numeric_limits<float>::max());
	}
};

struct BoundingBox {
	double minx = std::numeric_limits<double>::max();
	double miny = std::numeric_limits<double>::max();
	double maxx = std::numeric_limits<double>::lowest();
	double maxy = std::numeric_limits<double>::lowest();

	bool Intersects(const BoundingBox &other) const {
		return !(minx > other.maxx || maxx < other.minx || miny > other.maxy || maxy < other.miny);
	}
};


struct Geometry;
struct GeometryFactory;

class Point {
    VertexArray vertices;
public:
    explicit Point(Allocator &allocator, bool has_z, bool has_m) : vertices(allocator, has_z, has_m) { }
};

class LineString {
    VertexArray vertices;
public:
    explicit LineString(Allocator &allocator, bool has_z, bool has_m) : vertices(allocator, has_z, has_m) { }
};

class Polygon {
    reference<Allocator> alloc;
    VertexArray *rings;
    uint32_t num_rings;
public:
    explicit Polygon(Allocator &allocator, uint32_t num_rings, bool has_z, bool has_m) : alloc(allocator), num_rings(num_rings) {
        rings = reinterpret_cast<VertexArray*>(alloc.get().AllocateData(num_rings * sizeof(VertexArray)));
        for(uint32_t i = 0; i < num_rings; i++) {
            new (&rings[i]) VertexArray(allocator, has_z, has_m);
        }
    }

    VertexArray& operator[](uint32_t index) {
        D_ASSERT(index < num_rings);
        return rings[index];
    }

    const VertexArray& operator[](uint32_t index) const {
        D_ASSERT(index < num_rings);
        return rings[index];
    }

    VertexArray* begin() {
        return rings;
    }

    VertexArray* end() {
        return rings + num_rings;
    }

    const VertexArray* begin() const {
        return rings;
    }

    const VertexArray* end() const {
        return rings + num_rings;
    }

    // Copy Constructors and Assignment Operators
    Polygon(const Polygon &other) : alloc(other.alloc), num_rings(other.num_rings) {
        rings = reinterpret_cast<VertexArray*>(alloc.get().AllocateData(num_rings * sizeof(VertexArray)));
        for(uint32_t i = 0; i < num_rings; i++) {
            new (&rings[i]) VertexArray(other.rings[i]);
        }
    }

    Polygon &operator=(const Polygon &other) {
        if (this == &other) {
            return *this;
        }
        for(uint32_t i = 0; i < num_rings; i++) {
            rings[i].~VertexArray();
        }
        alloc.get().FreeData(reinterpret_cast<data_ptr_t>(rings), num_rings * sizeof(VertexArray));
        num_rings = other.num_rings;
        rings = reinterpret_cast<VertexArray*>(alloc.get().AllocateData(num_rings * sizeof(VertexArray)));
        for(uint32_t i = 0; i < num_rings; i++) {
            new (&rings[i]) VertexArray(other.rings[i]);
        }
        return *this;
    }

    // Move Constructors and Assignment Operators
    Polygon(Polygon &&other) noexcept : alloc(other.alloc), rings(other.rings), num_rings(other.num_rings) {
        other.rings = nullptr;
    }

    Polygon &operator=(Polygon &&other) noexcept {
        if (this == &other) {
            return *this;
        }
        for(uint32_t i = 0; i < num_rings; i++) {
            rings[i].~VertexArray();
        }
        alloc.get().FreeData(reinterpret_cast<data_ptr_t>(rings), num_rings * sizeof(VertexArray));
        num_rings = other.num_rings;
        rings = other.rings;
        other.rings = nullptr;
        return *this;
    }

    ~Polygon() {
        if(rings == nullptr) {
            return;
        }
        for(uint32_t i = 0; i < num_rings; i++) {
            rings[i].~VertexArray();
        }
        alloc.get().FreeData(reinterpret_cast<data_ptr_t>(rings), num_rings * sizeof(VertexArray));
    }
};

class GeomCollection {
    Geometry *items;
    uint32_t item_count;
};






// TODO: This is all crap, we need to re-implement this and add proper move semantics smh.

class Point {
	friend GeometryFactory;
	VertexArray vertices;

public:
	explicit Point(VertexArray vertices) : vertices(vertices) {
	}
	string ToString() const;
	bool IsEmpty() const;

	const VertexArray &Vertices() const {
		return vertices;
	}
    VertexArray &Vertices() {
		return vertices;
	}

	operator Geometry() const;
};

class LineString {
	friend GeometryFactory;
    VertexArray vertices;

public:
	explicit LineString(VertexArray vertices) : vertices(vertices) {
	}
    VertexArray &Vertices() {
		return vertices;
	}
	const VertexArray &Vertices() const {
		return vertices;
	}
	// Common Methods
	string ToString() const;
	// Geometry Methods
	bool IsEmpty() const;
	double Length() const;
	Geometry Centroid() const;
	uint32_t Count() const;
	operator Geometry() const;
};

class Polygon {
	friend GeometryFactory;
    VertexArray *rings;
	uint32_t num_rings;

public:
	explicit Polygon(VertexArray *rings, uint32_t num_rings) : rings(rings), num_rings(num_rings) {
	}

    ~Polygon() {
        for(uint32_t i = 0; i < num_rings; i++) {
            rings[i].~VertexArray();
        }
    }

    VertexArray &Ring(uint32_t index) {
		D_ASSERT(index < num_rings);
		return rings[index];
	}
	const VertexArray &Ring(uint32_t index) const {
		D_ASSERT(index < num_rings);
		return rings[index];
	}
	const VertexArray &Shell() const {
		return rings[0];
	}
    VertexArray &Shell() {
		return rings[0];
	}

	IteratorPair<VertexArray> Rings() {
		return IteratorPair<VertexArray>(rings, rings + num_rings);
	}

	ConstIteratorPair<VertexArray> Rings() const {
		return ConstIteratorPair<VertexArray>(rings, rings + num_rings);
	}

	// Common Methods
	string ToString() const;
	// Geometry Methods
	bool IsEmpty() const;
	uint32_t Count() const;

	operator Geometry() const;
};

class MultiPoint {
	friend GeometryFactory;
	Point *points;
	uint32_t num_points;

public:
	explicit MultiPoint(Point *points, uint32_t num_points) : points(points), num_points(num_points) {
	}
    ~MultiPoint() {
        for(uint32_t i = 0; i < num_points; i++) {
            points[i].~Point();
        }
    }

	string ToString() const;

	// Collection Methods
	uint32_t Count() const;
	bool IsEmpty() const;
	Point &operator[](uint32_t index);
	const Point &operator[](uint32_t index) const;

	// Iterator Methods
	const Point *begin() const;
	const Point *end() const;
	Point *begin();
	Point *end();

	operator Geometry() const;
};

class MultiLineString {
	friend GeometryFactory;
	LineString *lines;
	uint32_t count;

public:
	explicit MultiLineString(LineString *lines, uint32_t count) : lines(lines), count(count) {
	}

    ~MultiLineString() {
        for(uint32_t i = 0; i < count; i++) {
            lines[i].~LineString();
        }
    }

	string ToString() const;

	// Geometry Methods
	double Length() const;

	// Collection Methods
	uint32_t Count() const;
	bool IsEmpty() const;
	LineString &operator[](uint32_t index);
	const LineString &operator[](uint32_t index) const;

	// Iterator Methods
	const LineString *begin() const;
	const LineString *end() const;
	LineString *begin();
	LineString *end();

	operator Geometry() const;
};

class MultiPolygon {
	friend GeometryFactory;
	Polygon *polygons;
	uint32_t count;

public:
	explicit MultiPolygon(Polygon *polygons, uint32_t count) : polygons(polygons), count(count) {
	}

    ~MultiPolygon() {
        for(uint32_t i = 0; i < count; i++) {
            polygons[i].~Polygon();
        }
    }

	string ToString() const;

	// Collection Methods
	uint32_t Count() const;
	bool IsEmpty() const;
	Polygon &operator[](uint32_t index);
	const Polygon &operator[](uint32_t index) const;

	// Iterator Methods
	const Polygon *begin() const;
	const Polygon *end() const;
	Polygon *begin();
	Polygon *end();

	operator Geometry() const;
};

class GeometryCollection {
	friend GeometryFactory;
	Geometry *geometries;
	uint32_t count;

public:
	explicit GeometryCollection(Geometry *geometries, uint32_t count) : geometries(geometries), count(count) {
	}

    ~GeometryCollection();

	string ToString() const;

	// Collection Methods
	uint32_t Count() const;
	bool IsEmpty() const;
	Geometry &operator[](uint32_t index);
	const Geometry &operator[](uint32_t index) const;

	// Iterator Methods
	const Geometry *begin() const;
	const Geometry *end() const;
	Geometry *begin();
	Geometry *end();

	template <class AGG, class RESULT_TYPE>
	RESULT_TYPE Aggregate(AGG agg, RESULT_TYPE zero) const;

	operator Geometry() const;
};

struct Geometry {
	friend GeometryFactory;

private:
	GeometryType type;
	GeometryProperties properties;
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
	explicit Geometry(Point point) : type(GeometryType::POINT), point(std::move(point)) {
	}
	explicit Geometry(LineString linestring) : type(GeometryType::LINESTRING), linestring(std::move(linestring)) {
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

    //--------------------------------------------------------------------------------
    // Copy Constructors and Assignment Operators
    //--------------------------------------------------------------------------------
    Geometry(const Geometry &other) : type(other.type), properties(other.properties) {
        if (type == GeometryType::POINT) {
            new (&point) Point(other.point);
        } else if (type == GeometryType::LINESTRING) {
            new (&linestring) LineString(other.linestring);
        } else if (type == GeometryType::POLYGON) {
            new (&polygon) Polygon(other.polygon);
        } else if (type == GeometryType::MULTIPOINT) {
            new (&multipoint) MultiPoint(other.multipoint);
        } else if (type == GeometryType::MULTILINESTRING) {
            new (&multilinestring) MultiLineString(other.multilinestring);
        } else if (type == GeometryType::MULTIPOLYGON) {
            new (&multipolygon) MultiPolygon(other.multipolygon);
        } else if (type == GeometryType::GEOMETRYCOLLECTION) {
            new (&geometrycollection) GeometryCollection(other.geometrycollection);
        }
    }

    Geometry &operator=(const Geometry &other) {
        if (this == &other) {
            return *this;
        }
        if (type == GeometryType::POINT) {
            point.~Point();
        } else if (type == GeometryType::LINESTRING) {
            linestring.~LineString();
        } else if (type == GeometryType::POLYGON) {
            polygon.~Polygon();
        } else if (type == GeometryType::MULTIPOINT) {
            multipoint.~MultiPoint();
        } else if (type == GeometryType::MULTILINESTRING) {
            multilinestring.~MultiLineString();
        } else if (type == GeometryType::MULTIPOLYGON) {
            multipolygon.~MultiPolygon();
        } else if (type == GeometryType::GEOMETRYCOLLECTION) {
            geometrycollection.~GeometryCollection();
        }
        type = other.type;
        properties = other.properties;
        if (type == GeometryType::POINT) {
            new (&point) Point(other.point);
        } else if (type == GeometryType::LINESTRING) {
            new (&linestring) LineString(other.linestring);
        } else if (type == GeometryType::POLYGON) {
            new (&polygon) Polygon(other.polygon);
        } else if (type == GeometryType::MULTIPOINT) {
            new (&multipoint) MultiPoint(other.multipoint);
        } else if (type == GeometryType::MULTILINESTRING) {
            new (&multilinestring) MultiLineString(other.multilinestring);
        } else if (type == GeometryType::MULTIPOLYGON) {
            new (&multipolygon) MultiPolygon(other.multipolygon);
        } else if (type == GeometryType::GEOMETRYCOLLECTION) {
            new (&geometrycollection) GeometryCollection(other.geometrycollection);
        }
        return *this;
    }

    ~Geometry() {
        if (type == GeometryType::POINT) {
            point.~Point();
        } else if (type == GeometryType::LINESTRING) {
            linestring.~LineString();
        } else if (type == GeometryType::POLYGON) {
            polygon.~Polygon();
        } else if (type == GeometryType::MULTIPOINT) {
            multipoint.~MultiPoint();
        } else if (type == GeometryType::MULTILINESTRING) {
            multilinestring.~MultiLineString();
        } else if (type == GeometryType::MULTIPOLYGON) {
            multipolygon.~MultiPolygon();
        } else if (type == GeometryType::GEOMETRYCOLLECTION) {
            geometrycollection.~GeometryCollection();
        }
    }

	// Accessor methods
	inline GeometryType Type() const {
		return type;
	}

	inline GeometryProperties &Properties() {
		return properties;
	}

	inline const GeometryProperties &Properties() const {
		return properties;
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
	int32_t Dimension() const;
	bool IsEmpty() const;
	bool IsCollection() const;
};

template <class AGG, class RESULT_TYPE>
RESULT_TYPE GeometryCollection::Aggregate(AGG agg, RESULT_TYPE zero) const {
	RESULT_TYPE result = zero;
	for (idx_t i = 0; i < count; i++) {
		auto &geometry = geometries[i];
		if (geometry.Type() == GeometryType::GEOMETRYCOLLECTION) {
			result = geometry.GetGeometryCollection().Aggregate(agg, result);
		} else {
			result = agg(geometry, result);
		}
	}
	return result;
}

} // namespace core

} // namespace spatial