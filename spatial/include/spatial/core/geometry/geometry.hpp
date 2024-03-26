#pragma once

#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry_properties.hpp"
#include "spatial/core/geometry/cursor.hpp"
#include "spatial/core/geometry/geometry_type.hpp"
#include "spatial/core/geometry/vertex.hpp"

namespace spatial {

namespace core {

struct BoundingBox {
	double minx = std::numeric_limits<double>::max();
	double miny = std::numeric_limits<double>::max();
	double maxx = std::numeric_limits<double>::lowest();
	double maxy = std::numeric_limits<double>::lowest();
	double minz = std::numeric_limits<double>::max();
	double maxz = std::numeric_limits<double>::lowest();
	double minm = std::numeric_limits<double>::max();
	double maxm = std::numeric_limits<double>::lowest();

	bool Intersects(const BoundingBox &other) const {
		return !(minx > other.maxx || maxx < other.minx || miny > other.maxy || maxy < other.miny);
	}
};

//------------------------------------------------------------------------------
// Geometry Objects
//------------------------------------------------------------------------------

class Geometry;

//------------------------------------------------------------------------------
// Base Classes
//------------------------------------------------------------------------------

class GeometryBase {
    friend class Geometry;
protected:
    GeometryType type;
    GeometryProperties properties;
    bool is_readonly;
    uint32_t data_count;
    union {
        Geometry *part_data;
        data_ptr_t vertex_data;
    } data;

    GeometryBase(GeometryType type, Geometry *part_data)
        : type(type), properties(0), is_readonly(false), data_count(0), data({nullptr}){
        data.part_data = part_data;
    }

    GeometryBase(GeometryType type, data_ptr_t vert_data)
        : type(type), properties(0), is_readonly(false), data_count(0), data({nullptr}) {
        data.vertex_data = vert_data;
    }

    // Copy Constructor
    GeometryBase(const GeometryBase &other)
        : type(other.type), properties(other.properties), is_readonly(true), data_count(other.data_count), data({nullptr}) {
        data = other.data;
    }

    // Copy Assignment
    GeometryBase &operator=(const GeometryBase &other) {
        type = other.type;
        properties = other.properties;
        is_readonly = true;
        data_count = other.data_count;
        data = other.data;
        return *this;
    }

    // Move Constructor
    GeometryBase(GeometryBase &&other) noexcept
        : type(other.type), properties(other.properties), is_readonly(other.is_readonly), data_count(other.data_count), data({nullptr}) {
        data = other.data;
        if(!other.is_readonly) {
            // Take ownership of the data, and make the other object read-only
            other.is_readonly = true;
        }
    }

    // Move Assignment
    GeometryBase &operator=(GeometryBase &&other) noexcept {
        type = other.type;
        properties = other.properties;
        is_readonly = other.is_readonly;
        data_count = other.data_count;
        data = other.data;
        if(!other.is_readonly) {
            // Take ownership of the data, and make the other object read-only
            other.is_readonly = true;
        }
        return *this;
    }

public:
    GeometryProperties GetProperties() const { return properties; }
    GeometryProperties &GetProperties() { return properties; }
    bool IsReadOnly() const { return is_readonly; }
    uint32_t Count() const { return data_count; }
};

static_assert(sizeof(GeometryBase) == 16, "GeometryBase should be 16 bytes");

//------------------------------------------------------------------------------
// All of the following classes are just to provide a type-safe interface to the underlying geometry data, and to
// enable convenient matching of geometry types using function overloads.

// A single part geometry, contains a single array of vertices
class SinglePartGeometry : public GeometryBase {
    friend class Geometry;
protected:

    explicit SinglePartGeometry(GeometryType type) : GeometryBase(type, (Geometry*) nullptr) {}

public:
    uint32_t ByteSize() const { return data_count * properties.VertexSize(); }

    bool IsEmpty() const { return data_count == 0; }

    void Set(uint32_t index, const VertexXY &vertex) {
        D_ASSERT(index < data_count);
        Store(vertex, data.vertex_data + index * properties.VertexSize());
    }

    void Set(uint32_t index, double x, double y) {
        Set(index, VertexXY {x, y});
    }

    VertexXY Get(uint32_t index) const {
        D_ASSERT(index < data_count);
        return Load<VertexXY>(data.vertex_data + index * properties.VertexSize());
    }

    template<class V>
    void SetExact(uint32_t index, const V &vertex) {
        static_assert(V::IS_VERTEX, "V must be a vertex type");
        D_ASSERT(V::HAS_Z == properties.HasZ());
        D_ASSERT(V::HAS_M == properties.HasM());
        D_ASSERT(index < data_count);
        Store(vertex, data.vertex_data + index * sizeof(V));
    }

    template<class V>
    V GetExact(uint32_t index) const {
        static_assert(V::IS_VERTEX, "V must be a vertex type");
        D_ASSERT(V::HAS_Z == properties.HasZ());
        D_ASSERT(V::HAS_M == properties.HasM());
        D_ASSERT(index < data_count);
        return Load<V>(data.vertex_data + index * sizeof(V));
    }

    // Turn this geometry into a read-only reference to another geometry, starting at the specified index
    void Reference(const SinglePartGeometry &other, uint32_t offset, uint32_t count);

    // Turn this geometry into a read-only reference to raw data
    void ReferenceData(const_data_ptr_t data, uint32_t count, bool has_z, bool has_m);

    // Turn this geometry into a owning copy of another geometry, starting at the specified index
    void Copy(ArenaAllocator& alloc, const SinglePartGeometry &other, uint32_t offset, uint32_t count);

    // Turn this geometry into a owning copy of raw data
    void CopyData(ArenaAllocator& alloc, const_data_ptr_t data, uint32_t count, bool has_z, bool has_m);

    // Resize the geometry, truncating or extending with zeroed vertices as needed
    void Resize(ArenaAllocator &alloc, uint32_t new_count);

    // Append the data from another geometry
    void Append(ArenaAllocator &alloc, const SinglePartGeometry& other);

    // Append the data from multiple other geometries
    void Append(ArenaAllocator &alloc, const SinglePartGeometry* others, uint32_t others_count);

    // Force the geometry to have a specific vertex type, resizing or shrinking the data as needed
    void SetVertexType(ArenaAllocator &alloc, bool has_z, bool has_m, double default_z = 0, double default_m = 0);

    // If this geometry is read-only, make it mutable by copying the data
    void MakeMutable(ArenaAllocator &alloc);

    // Print this geometry as a string, starting at the specified index and printing the specified number of vertices
    // (useful for debugging)
    string ToString(uint32_t start = 0, uint32_t count = 0) const;
};

// A multi-part geometry, contains multiple parts
class MultiPartGeometry : public GeometryBase {
protected:
    explicit MultiPartGeometry(GeometryType type) : GeometryBase(type, (Geometry*) nullptr) {}
public:

    bool IsEmpty() const;

    Geometry &operator[](uint32_t index);
    Geometry *begin();
    Geometry *end();

    const Geometry &operator[](uint32_t index) const;
    const Geometry *begin() const;
    const Geometry *end() const;

    void Resize(ArenaAllocator &alloc, uint32_t new_count);
};

class CollectionGeometry : public MultiPartGeometry {
protected:
    explicit CollectionGeometry(GeometryType type) : MultiPartGeometry(type) {}
};

//------------------------------------------------------------------------------
// Concrete Classes
//------------------------------------------------------------------------------
// These are the actual Geometry types that are instantiated and used in practice

class Point : public SinglePartGeometry {
public:
    static constexpr GeometryType TYPE = GeometryType::POINT;

    Point() : SinglePartGeometry(TYPE) {}
};

class LineString : public SinglePartGeometry {
public:
    static constexpr GeometryType TYPE = GeometryType::LINESTRING;

    LineString() : SinglePartGeometry(TYPE) {}
};

class Polygon : public MultiPartGeometry {
public:
    static constexpr GeometryType TYPE = GeometryType::POLYGON;

    Polygon() : MultiPartGeometry(TYPE) {}

    LineString &operator[](uint32_t index);
    LineString* begin();
    LineString* end();

    const LineString &operator[](uint32_t index) const;
    const LineString* begin() const;
    const LineString* end() const;
};


class MultiPoint : public CollectionGeometry {
public:
    static constexpr GeometryType TYPE = GeometryType::MULTIPOINT;

    MultiPoint() : CollectionGeometry(TYPE) {}

    Point &operator[](uint32_t index);
    Point* begin();
    Point* end();

    const Point &operator[](uint32_t index) const;
    const Point *begin() const;
    const Point *end() const;
};

class MultiLineString : public CollectionGeometry {
public:
    static constexpr GeometryType TYPE = GeometryType::MULTILINESTRING;

    MultiLineString() : CollectionGeometry(TYPE) {}

    LineString &operator[](uint32_t index);
    LineString *begin();
    LineString *end();

    const LineString &operator[](uint32_t index) const;
    const LineString *begin() const;
    const LineString *end() const;
};

class MultiPolygon : public CollectionGeometry {
public:
    static constexpr GeometryType TYPE = GeometryType::MULTIPOLYGON;

    MultiPolygon() : CollectionGeometry(TYPE) {}

    Polygon &operator[](uint32_t index);
    Polygon *begin();
    Polygon *end();

    const Polygon &operator[](uint32_t index) const;
    const Polygon *begin() const;
    const Polygon *end() const;
};

class GeometryCollection : public CollectionGeometry {
public:
    static constexpr GeometryType TYPE = GeometryType::GEOMETRYCOLLECTION;

    GeometryCollection() : CollectionGeometry(TYPE) {}
};

class Geometry {
    union {
        Point point;
        LineString linestring;
        Polygon polygon;
        MultiPoint multipoint;
        MultiLineString multilinestring;
        MultiPolygon multipolygon;
        GeometryCollection collection;
    };
public:

    // This is legal because all members is standard layout and have the same common initial sequence
    // Additionally, a union is pointer-interconvertible with its first member
    GeometryType GetType() const { return point.type; }
    GeometryProperties GetProperties() const { return point.properties; }
    GeometryProperties &GetProperties() { return point.properties; }
    bool IsReadOnly() const { return reinterpret_cast<const GeometryBase&>(*this).IsReadOnly(); }

    // NOLINTBEGIN
    Geometry(Point point) : point(point) {}
    Geometry(LineString linestring) : linestring(linestring) {}
    Geometry(Polygon polygon) : polygon(polygon) {}
    Geometry(MultiPoint multipoint) : multipoint(multipoint) {}
    Geometry(MultiLineString multilinestring) : multilinestring(multilinestring) {}
    Geometry(MultiPolygon multipolygon) : multipolygon(multipolygon) {}
    Geometry(GeometryCollection collection) : collection(collection) {}
    // NOLINTEND

    template<class T>
    T &As() {
        D_ASSERT(GetType() == T::TYPE);
        return reinterpret_cast<T&>(*this);
    }

    // Apply a functor to the contained geometry
    template <class F, class... ARGS>
    auto Visit(ARGS &&...args) const
    -> decltype(F::Apply(std::declval<const Point &>(), std::forward<ARGS>(args)...)) {
        switch (GetType()) {
            case GeometryType::POINT:
                return F::Apply(const_cast<const Point &>(point), std::forward<ARGS>(args)...);
            case GeometryType::LINESTRING:
                return F::Apply(const_cast<const LineString &>(linestring), std::forward<ARGS>(args)...);
            case GeometryType::POLYGON:
                return F::Apply(const_cast<const Polygon &>(polygon), std::forward<ARGS>(args)...);
            case GeometryType::MULTIPOINT:
                return F::Apply(const_cast<const MultiPoint &>(multipoint), std::forward<ARGS>(args)...);
            case GeometryType::MULTILINESTRING:
                return F::Apply(const_cast<const MultiLineString &>(multilinestring), std::forward<ARGS>(args)...);
            case GeometryType::MULTIPOLYGON:
                return F::Apply(const_cast<const MultiPolygon &>(multipolygon), std::forward<ARGS>(args)...);
            case GeometryType::GEOMETRYCOLLECTION:
                return F::Apply(const_cast<const GeometryCollection &>(collection), std::forward<ARGS>(args)...);
            default:
                throw NotImplementedException("Geometry::Visit()");
        }
    }

    // Apply a functor to the contained geometry
    template <class F, class... ARGS>
    auto Visit(ARGS &&...args) -> decltype(F::Apply(std::declval<Point &>(), std::forward<ARGS>(args)...)) {
        switch (GetType()) {
            case GeometryType::POINT:
                return F::Apply(static_cast<Point &>(point), std::forward<ARGS>(args)...);
            case GeometryType::LINESTRING:
                return F::Apply(static_cast<LineString &>(linestring), std::forward<ARGS>(args)...);
            case GeometryType::POLYGON:
                return F::Apply(static_cast<Polygon &>(polygon), std::forward<ARGS>(args)...);
            case GeometryType::MULTIPOINT:
                return F::Apply(static_cast<MultiPoint &>(multipoint), std::forward<ARGS>(args)...);
            case GeometryType::MULTILINESTRING:
                return F::Apply(static_cast<MultiLineString &>(multilinestring), std::forward<ARGS>(args)...);
            case GeometryType::MULTIPOLYGON:
                return F::Apply(static_cast<MultiPolygon &>(multipolygon), std::forward<ARGS>(args)...);
            case GeometryType::GEOMETRYCOLLECTION:
                return F::Apply(static_cast<GeometryCollection &>(collection), std::forward<ARGS>(args)...);
            default:
                throw NotImplementedException("Geometry::Visit()");
        }
    }

    uint32_t GetDimension(bool skip_empty) const {
        if(IsEmpty()) {
            return 0;
        }
        struct op {
            static uint32_t Apply(const Point&, bool) { return 0; }
            static uint32_t Apply(const LineString&, bool) { return 1; }
            static uint32_t Apply(const Polygon&, bool) { return 2; }
            static uint32_t Apply(const MultiPoint&, bool) { return 0; }
            static uint32_t Apply(const MultiLineString&, bool) { return 1; }
            static uint32_t Apply(const MultiPolygon&, bool) { return 2; }
            static uint32_t Apply(const GeometryCollection &gc, bool skip_empty) {
                uint32_t max = 0;
                for (auto &item : gc) {
                    max = std::max(max, item.GetDimension(skip_empty));
                }
                return max;
            }
        };
        return Visit<op>(skip_empty);
    }

    bool IsEmpty() const {
        struct op {
            static bool Apply(const SinglePartGeometry &g) { return g.IsEmpty(); }
            static bool Apply(const MultiPartGeometry &g) { return g.IsEmpty(); }
        };
        return Visit<op>();
    }
};


//------------------------------------------------------------------------------
// Inlined Methods
//------------------------------------------------------------------------------

//-------------------
// MultiPartGeometry
//-------------------

bool MultiPartGeometry::IsEmpty() const {
    for (const auto &part : *this) {
        if (!part.IsEmpty()) {
            return false;
        }
    }
    return true;
}

inline Geometry& MultiPartGeometry::operator[](uint32_t index) { return data.part_data[index]; }
inline Geometry* MultiPartGeometry::begin() { return data.part_data; }
inline Geometry* MultiPartGeometry::end() { return data.part_data + data_count; }

inline const Geometry& MultiPartGeometry::operator[](uint32_t index) const { return data.part_data[index]; }
inline const Geometry* MultiPartGeometry::begin() const { return data.part_data; }
inline const Geometry* MultiPartGeometry::end() const { return data.part_data + data_count; }

//-----------------
// Polygon
//-----------------

inline LineString& Polygon::operator[](uint32_t index) { return reinterpret_cast<LineString&>(data.part_data[index]); }
inline LineString* Polygon::begin() { return reinterpret_cast<LineString*>(data.part_data); }
inline LineString* Polygon::end() { return reinterpret_cast<LineString*>(data.part_data + data_count); }

inline const LineString& Polygon::operator[](uint32_t index) const { return reinterpret_cast<const LineString&>(data.part_data[index]); }
inline const LineString* Polygon::begin() const { return reinterpret_cast<const LineString*>(data.part_data); }
inline const LineString* Polygon::end() const { return reinterpret_cast<const LineString*>(data.part_data + data_count); }

//-----------------
// MultiPoint
//-----------------

inline Point& MultiPoint::operator[](uint32_t index) { return reinterpret_cast<Point&>(data.part_data[index]); }
inline Point* MultiPoint::begin() { return reinterpret_cast<Point*>(data.part_data); }
inline Point* MultiPoint::end() { return reinterpret_cast<Point*>(data.part_data + data_count); }

inline const Point& MultiPoint::operator[](uint32_t index) const { return reinterpret_cast<const Point&>(data.part_data[index]); }
inline const Point* MultiPoint::begin() const { return reinterpret_cast<const Point*>(data.part_data); }
inline const Point* MultiPoint::end() const { return reinterpret_cast<const Point*>(data.part_data + data_count); }

//-----------------
// MultiLineString
//-----------------

inline LineString& MultiLineString::operator[](uint32_t index) { return reinterpret_cast<LineString&>(data.part_data[index]); }
inline LineString* MultiLineString::begin() { return reinterpret_cast<LineString*>(data.part_data); }
inline LineString* MultiLineString::end() { return reinterpret_cast<LineString*>(data.part_data + data_count); }

inline const LineString& MultiLineString::operator[](uint32_t index) const { return reinterpret_cast<const LineString&>(data.part_data[index]); }
inline const LineString* MultiLineString::begin() const { return reinterpret_cast<const LineString*>(data.part_data); }
inline const LineString* MultiLineString::end() const { return reinterpret_cast<const LineString*>(data.part_data + data_count); }

//-----------------
// MultiPolygon
//-----------------

inline Polygon& MultiPolygon::operator[](uint32_t index) { return reinterpret_cast<Polygon&>(data.part_data[index]); }
inline Polygon* MultiPolygon::begin() { return reinterpret_cast<Polygon*>(data.part_data); }
inline Polygon* MultiPolygon::end() { return reinterpret_cast<Polygon*>(data.part_data + data_count); }

inline const Polygon& MultiPolygon::operator[](uint32_t index) const { return reinterpret_cast<const Polygon&>(data.part_data[index]); }
inline const Polygon* MultiPolygon::begin() const { return reinterpret_cast<const Polygon*>(data.part_data); }
inline const Polygon* MultiPolygon::end() const { return reinterpret_cast<const Polygon*>(data.part_data + data_count); }


//------------------------------------------------------------------------------
// Assertions
//------------------------------------------------------------------------------

static_assert(std::is_standard_layout<Geometry>::value, "Geometry must be standard layout");
static_assert(std::is_standard_layout<GeometryBase>::value, "VertArray must be standard layout");
static_assert(std::is_standard_layout<GeometryBase>::value, "PartArray must be standard layout");
static_assert(std::is_standard_layout<SinglePartGeometry>::value, "SinglePartBase must be standard layout");
static_assert(std::is_standard_layout<MultiPartGeometry>::value, "Point must be standard layout");
static_assert(std::is_standard_layout<Point>::value, "Point must be standard layout");
static_assert(std::is_standard_layout<Polygon>::value, "Polygon must be standard layout");
static_assert(std::is_standard_layout<LineString>::value, "LineString must be standard layout");
static_assert(std::is_standard_layout<MultiPolygon>::value, "MultiPolygon must be standard layout");
static_assert(std::is_standard_layout<MultiLineString>::value, "MultiLineString must be standard layout");
static_assert(std::is_standard_layout<MultiPoint>::value, "MultiPoint must be standard layout");
static_assert(std::is_standard_layout<GeometryCollection>::value, "GeometryCollection must be standard layout");


//------------------------------------------------------------------------------
// Utils
//------------------------------------------------------------------------------

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

} // namespace core

} // namespace spatial