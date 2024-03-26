#pragma once

#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry_properties.hpp"
#include "spatial/core/geometry/cursor.hpp"
#include "spatial/core/geometry/geometry_type.hpp"
#include "spatial/core/geometry/vertex.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// Geometry Objects
//------------------------------------------------------------------------------

class Geometry;

//------------------------------------------------------------------------------
// Base Classes
//------------------------------------------------------------------------------

class BaseGeometry {
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

    BaseGeometry(GeometryType type, Geometry *part_data, bool is_readonly, bool has_z, bool has_m)
        : type(type), properties(has_z, has_m), is_readonly(is_readonly), data_count(0), data({nullptr}){
        data.part_data = part_data;
    }

    BaseGeometry(GeometryType type, data_ptr_t vert_data, bool is_readonly, bool has_z, bool has_m)
        : type(type), properties(has_z, has_m), is_readonly(is_readonly), data_count(0), data({nullptr}) {
        data.vertex_data = vert_data;
    }

    // Copy Constructor
    BaseGeometry(const BaseGeometry &other)
        : type(other.type), properties(other.properties), is_readonly(true), data_count(other.data_count), data({nullptr}) {
        data = other.data;
    }

    // Copy Assignment
    BaseGeometry &operator=(const BaseGeometry &other) {
        type = other.type;
        properties = other.properties;
        is_readonly = true;
        data_count = other.data_count;
        data = other.data;
        return *this;
    }

    // Move Constructor
    BaseGeometry(BaseGeometry &&other) noexcept
        : type(other.type), properties(other.properties), is_readonly(other.is_readonly), data_count(other.data_count), data({nullptr}) {
        data = other.data;
        if(!other.is_readonly) {
            // Take ownership of the data, and make the other object read-only
            other.is_readonly = true;
        }
    }

    // Move Assignment
    BaseGeometry &operator=(BaseGeometry &&other) noexcept {
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

static_assert(sizeof(BaseGeometry) == 16, "GeometryBase should be 16 bytes");

//------------------------------------------------------------------------------
// All of the following classes are just to provide a type-safe interface to the underlying geometry data, and to
// enable convenient matching of geometry types using function overloads.

// A single part geometry, contains a single array of vertices
class SinglePartGeometry : public BaseGeometry {
    friend class Geometry;
protected:

    SinglePartGeometry(GeometryType type, bool has_z, bool has_m)
        : BaseGeometry(type, (data_ptr_t) nullptr, true, has_z, has_m) {}

    SinglePartGeometry(GeometryType type, ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m)
        : BaseGeometry(type, (data_ptr_t) nullptr, false, has_z, has_m) {
        data_count = count;
        data.vertex_data = alloc.AllocateAligned(count * properties.VertexSize());
    }

public:
    uint32_t ByteSize() const { return data_count * properties.VertexSize(); }

    bool IsEmpty() const { return data_count == 0; }

    const_data_ptr_t GetData() const { return data.vertex_data; }

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

    void ReferenceData(const_data_ptr_t data, uint32_t count) {
        ReferenceData(data, count, properties.HasZ(), properties.HasM());
    }

    // Turn this geometry into a owning copy of another geometry, starting at the specified index
    void Copy(ArenaAllocator& alloc, const SinglePartGeometry &other, uint32_t offset, uint32_t count);

    // Turn this geometry into a owning copy of raw data
    void CopyData(ArenaAllocator& alloc, const_data_ptr_t data, uint32_t count, bool has_z, bool has_m);

    void CopyData(ArenaAllocator& alloc, const_data_ptr_t data, uint32_t count) {
        CopyData(alloc, data, count, properties.HasZ(), properties.HasM());
    }

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
class MultiPartGeometry : public BaseGeometry {
protected:

    MultiPartGeometry(GeometryType type, bool has_z, bool has_m)
        : BaseGeometry(type, (Geometry*) nullptr, true, has_z, has_m) {}

    MultiPartGeometry(GeometryType type, ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m)
        : BaseGeometry(type, (Geometry*) nullptr, false, has_z, has_m) {
            data_count = count;
            data.vertex_data = alloc.AllocateAligned(count * sizeof(BaseGeometry));
        }
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
    CollectionGeometry(GeometryType type, bool has_z, bool has_m)
        : MultiPartGeometry(type, has_z, has_m) {}
    CollectionGeometry(GeometryType type, ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m)
        : MultiPartGeometry(type, alloc, count, has_z, has_m) {}
};

template<class T>
class TypedCollectionGeometry : public CollectionGeometry {
protected:
    TypedCollectionGeometry(GeometryType type, bool has_z, bool has_m)
        : CollectionGeometry(type, has_z, has_m) {}
    TypedCollectionGeometry(GeometryType type, ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m)
        : CollectionGeometry(type, alloc, count, has_z, has_m) {
        auto ptr = data.part_data;
        for (uint32_t i = 0; i < count; i++) {
            new(ptr++) T(has_z, has_m);
        }
    }

public:
    T &operator[](uint32_t index);
    T* begin();
    T* end();

    const T &operator[](uint32_t index) const;
    const T *begin() const;
    const T *end() const;
};

//------------------------------------------------------------------------------
// Concrete Classes
//------------------------------------------------------------------------------
// These are the actual Geometry types that are instantiated and used in practice

class Point : public SinglePartGeometry {
public:
    static constexpr GeometryType TYPE = GeometryType::POINT;

    Point(bool has_z = false, bool has_m = false) : SinglePartGeometry(TYPE, has_z, has_m) {}
    Point(ArenaAllocator &alloc, bool has_z, bool has_m) : SinglePartGeometry(TYPE, alloc, 1, has_z, has_m) {}

    // Helpers
    template<class V>
    static Point FromVertex(ArenaAllocator &alloc, const V &vertex) {
        static_assert(V::IS_VERTEX, "V must be a vertex type");
        Point point(alloc, V::HAS_Z, V::HAS_M);
        point.SetExact(0, vertex);
        return point;
    }

    static Point CopyFromData(ArenaAllocator &alloc, const_data_ptr_t data, uint32_t count, bool has_z, bool has_m) {
        Point point(alloc, has_z, has_m);
        point.CopyData(alloc, data, 1);
        return point;
    }
};

class LineString : public SinglePartGeometry {
public:
    static constexpr GeometryType TYPE = GeometryType::LINESTRING;

    LineString(bool has_z = false, bool has_m = false) : SinglePartGeometry(TYPE, has_z, has_m) {}
    LineString(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m)
        : SinglePartGeometry(TYPE, alloc, count, has_z, has_m) {}

    static LineString CopyFromData(ArenaAllocator &alloc, const_data_ptr_t data, uint32_t count, bool has_z, bool has_m) {
        LineString line(alloc, count, has_z, has_m);
        line.CopyData(alloc, data, count);
        return line;
    }
};

class Polygon : public MultiPartGeometry {
public:
    static constexpr GeometryType TYPE = GeometryType::POLYGON;

    Polygon(bool has_z = false, bool has_m = false) : MultiPartGeometry(TYPE, has_z, has_m) {}
    Polygon(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m);

    LineString &operator[](uint32_t index);
    LineString* begin();
    LineString* end();

    const LineString &operator[](uint32_t index) const;
    const LineString* begin() const;
    const LineString* end() const;

    // TODO: Generalize this to take a min/max vertex instead
    static Polygon FromBox(ArenaAllocator &alloc, double minx, double miny, double maxx, double maxy) {
        Polygon box(alloc, 1, false, false);
        auto &ring = box[0];
        ring.Set(0, minx, miny);
        ring.Set(1, minx, maxy);
        ring.Set(2, maxx, maxy);
        ring.Set(3, maxx, miny);
        ring.Set(4, minx, miny);
        return box;
    }
};

class MultiPoint : public TypedCollectionGeometry<Point> {
public:
    static constexpr GeometryType TYPE = GeometryType::MULTIPOINT;

    MultiPoint(bool has_z = false, bool has_m = false) : TypedCollectionGeometry(TYPE, has_z, has_m) {}
    MultiPoint(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m)
        : TypedCollectionGeometry(TYPE, alloc, count, has_z, has_m) {}
};

class MultiLineString : public TypedCollectionGeometry<LineString> {
public:
    static constexpr GeometryType TYPE = GeometryType::MULTILINESTRING;

    MultiLineString(bool has_z = false, bool has_m = false) : TypedCollectionGeometry(TYPE, has_z, has_m) {}
    MultiLineString(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m)
        : TypedCollectionGeometry(TYPE, alloc, count, has_z, has_m) {}
};

class MultiPolygon : public TypedCollectionGeometry<Polygon> {
public:
    static constexpr GeometryType TYPE = GeometryType::MULTIPOLYGON;

    MultiPolygon(bool has_z = false, bool has_m = false) : TypedCollectionGeometry(TYPE, has_z, has_m) {}
    MultiPolygon(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m)
        : TypedCollectionGeometry(TYPE, alloc, count, has_z, has_m) {}
};

class GeometryCollection : public CollectionGeometry  {
public:
    static constexpr GeometryType TYPE = GeometryType::GEOMETRYCOLLECTION;

    GeometryCollection(bool has_z = false, bool has_m = false) : CollectionGeometry(TYPE, has_z, has_m) {}
    GeometryCollection(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m);
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
    bool IsReadOnly() const { return reinterpret_cast<const BaseGeometry&>(*this).IsReadOnly(); }
    bool IsCollection() const {
        auto type = GetType();
        return type == GeometryType::MULTIPOINT || type == GeometryType::MULTILINESTRING
            || type == GeometryType::MULTIPOLYGON || type == GeometryType::GEOMETRYCOLLECTION;
    }

    // NOLINTBEGIN
    Geometry(Point point) : point(point) {}
    Geometry(LineString linestring) : linestring(linestring) {}
    Geometry(Polygon polygon) : polygon(polygon) {}
    Geometry(MultiPoint multipoint) : multipoint(multipoint) {}
    Geometry(MultiLineString multilinestring) : multilinestring(multilinestring) {}
    Geometry(MultiPolygon multipolygon) : multipolygon(multipolygon) {}
    Geometry(GeometryCollection collection) : collection(collection) {}
    // NOLINTEND


    // Copy Constructor
    Geometry(const Geometry& other) {
        switch (other.GetType()) {
            case GeometryType::POINT:
                new(&point) Point(other.point);
                break;
            case GeometryType::LINESTRING:
                new(&linestring) LineString(other.linestring);
                break;
            case GeometryType::POLYGON:
                new(&polygon) Polygon(other.polygon);
                break;
            case GeometryType::MULTIPOINT:
                new(&multipoint) MultiPoint(other.multipoint);
                break;
            case GeometryType::MULTILINESTRING:
                new(&multilinestring) MultiLineString(other.multilinestring);
                break;
            case GeometryType::MULTIPOLYGON:
                new(&multipolygon) MultiPolygon(other.multipolygon);
                break;
            case GeometryType::GEOMETRYCOLLECTION:
                new(&collection) GeometryCollection(other.collection);
                break;
            default:
                throw NotImplementedException("Geometry::Geometry(const Geometry&)");
        }
    }

    // Copy Assignment
    Geometry &operator=(const Geometry &other) {
        if (this == &other) {
            return *this;
        }
        this->~Geometry();
        switch (other.GetType()) {
            case GeometryType::POINT:
                new(&point) Point(other.point);
                break;
            case GeometryType::LINESTRING:
                new(&linestring) LineString(other.linestring);
                break;
            case GeometryType::POLYGON:
                new(&polygon) Polygon(other.polygon);
                break;
            case GeometryType::MULTIPOINT:
                new(&multipoint) MultiPoint(other.multipoint);
                break;
            case GeometryType::MULTILINESTRING:
                new(&multilinestring) MultiLineString(other.multilinestring);
                break;
            case GeometryType::MULTIPOLYGON:
                new(&multipolygon) MultiPolygon(other.multipolygon);
                break;
            case GeometryType::GEOMETRYCOLLECTION:
                new(&collection) GeometryCollection(other.collection);
                break;
            default:
                throw NotImplementedException("Geometry::operator=(const Geometry&)");
        }
        return *this;
    }

    // Move Constructor
    Geometry(Geometry &&other) noexcept {
        switch (other.GetType()) {
            case GeometryType::POINT:
                new(&point) Point(std::move(other.point));
                break;
            case GeometryType::LINESTRING:
                new(&linestring) LineString(std::move(other.linestring));
                break;
            case GeometryType::POLYGON:
                new(&polygon) Polygon(std::move(other.polygon));
                break;
            case GeometryType::MULTIPOINT:
                new(&multipoint) MultiPoint(std::move(other.multipoint));
                break;
            case GeometryType::MULTILINESTRING:
                new(&multilinestring) MultiLineString(std::move(other.multilinestring));
                break;
            case GeometryType::MULTIPOLYGON:
                new(&multipolygon) MultiPolygon(std::move(other.multipolygon));
                break;
            case GeometryType::GEOMETRYCOLLECTION:
                new(&collection) GeometryCollection(std::move(other.collection));
                break;
            default:
                throw NotImplementedException("Geometry::Geometry(Geometry&&)");
        }
    }

    // Move Assignment
    Geometry &operator=(Geometry &&other) noexcept {
        if (this == &other) {
            return *this;
        }
        this->~Geometry();
        switch (other.GetType()) {
            case GeometryType::POINT:
                new(&point) Point(std::move(other.point));
                break;
            case GeometryType::LINESTRING:
                new(&linestring) LineString(std::move(other.linestring));
                break;
            case GeometryType::POLYGON:
                new(&polygon) Polygon(std::move(other.polygon));
                break;
            case GeometryType::MULTIPOINT:
                new(&multipoint) MultiPoint(std::move(other.multipoint));
                break;
            case GeometryType::MULTILINESTRING:
                new(&multilinestring) MultiLineString(std::move(other.multilinestring));
                break;
            case GeometryType::MULTIPOLYGON:
                new(&multipolygon) MultiPolygon(std::move(other.multipolygon));
                break;
            case GeometryType::GEOMETRYCOLLECTION:
                new(&collection) GeometryCollection(std::move(other.collection));
                break;
            default:
                throw NotImplementedException("Geometry::operator=(Geometry&&)");
        }
        return *this;
    }

    template<class T>
    T &As() {
        D_ASSERT(GetType() == T::TYPE);
        return reinterpret_cast<T&>(*this);
    }

    template<class T>
    const T &As() const {
        D_ASSERT(GetType() == T::TYPE);
        return reinterpret_cast<const T&>(*this);
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

                for (const auto &item : gc) {
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

    Geometry& SetVertexType(ArenaAllocator &arena, bool has_z, bool has_m) {
        struct op {
            static void Apply(SinglePartGeometry &g, ArenaAllocator &arena, bool has_z, bool has_m) {
                g.SetVertexType(arena, has_z, has_m);
            }
            static void Apply(MultiPartGeometry &g, ArenaAllocator &arena, bool has_z, bool has_m) {
                g.properties.SetZ(has_z);
                g.properties.SetM(has_m);
                for (auto &part : g) {
                    part.SetVertexType(arena, has_z, has_m);
                }
            }
        };
        Visit<op>(arena, has_z, has_m);
        return *this;
    }

    geometry_t Serialize(Vector &result);
    static Geometry Deserialize(ArenaAllocator &arena, const geometry_t &data);
};


//------------------------------------------------------------------------------
// Inlined Methods
//------------------------------------------------------------------------------

inline Polygon::Polygon(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m)
        : MultiPartGeometry(TYPE, alloc, count, has_z, has_m) {
        auto ptr = data.part_data;
        for (uint32_t i = 0; i < count; i++) {
        new(ptr++) LineString(alloc, 0, has_z, has_m);
    }
}

inline GeometryCollection::GeometryCollection(ArenaAllocator &alloc, uint32_t count, bool has_z, bool has_m)
        : CollectionGeometry(TYPE, alloc, count, has_z, has_m) {
        auto ptr = data.part_data;
        for (uint32_t i = 0; i < count; i++) {
        new(ptr++) Point(has_z, has_m);
    }
}

//-------------------
// MultiPartGeometry
//-------------------

inline bool MultiPartGeometry::IsEmpty() const {
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
// Collection
//-----------------

template<class T>
inline T& TypedCollectionGeometry<T>::operator[](uint32_t index) { return reinterpret_cast<T&>(data.part_data[index]); }
template<class T>
inline T* TypedCollectionGeometry<T>::begin() { return reinterpret_cast<T*>(data.part_data); }
template<class T>
inline T* TypedCollectionGeometry<T>::end() { return reinterpret_cast<T*>(data.part_data + data_count); }

template<class T>
inline const T& TypedCollectionGeometry<T>::operator[](uint32_t index) const { return reinterpret_cast<const T&>(data.part_data[index]); }

template<class T>
inline const T* TypedCollectionGeometry<T>::begin() const { return reinterpret_cast<const T*>(data.part_data); }

template<class T>
inline const T* TypedCollectionGeometry<T>::end() const { return reinterpret_cast<const T*>(data.part_data + data_count); }

//------------------------------------------------------------------------------
// Assertions
//------------------------------------------------------------------------------

static_assert(std::is_standard_layout<Geometry>::value, "Geometry must be standard layout");
static_assert(std::is_standard_layout<BaseGeometry>::value, "BaseGeometry must be standard layout");
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