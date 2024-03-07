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

/*
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
    bool geom_has_z;
    bool geom_has_m;
    bool geom_has_z_set;
    bool geom_has_m_set;

public:
	template <WKBByteOrder ORDER>
	uint32_t ReadInt() = delete;

	template <WKBByteOrder ORDER>
	double ReadDouble() = delete;

	WKBReader(GeometryFactory &factory, const char *data, uint32_t length)
	    : factory(factory), data(data), length(length), cursor(0), geom_has_z(false), geom_has_m(false), geom_has_z_set(false), geom_has_m_set(false) {
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

*/

template<bool SAFE>
class ReadCursor {
private:
    const_data_ptr_t pos;
    const_data_ptr_t end;
public:
    ReadCursor() : pos(nullptr), end(nullptr) { }
    explicit ReadCursor(const_data_ptr_t data, uint32_t length) : pos(data), end(data + length) {}
    explicit ReadCursor(const string_t &data) : pos(const_data_ptr_cast(data.GetData())), end(pos + data.GetSize()) { }

    template<class T>
    T Read() {
        static_assert(std::is_trivially_copyable<T>::value, "Type must be trivially copyable");
        if(SAFE && pos + sizeof(T) > end) {
            throw SerializationException("Read past end of buffer");
        }
        T result = Load<T>(pos);
        pos += sizeof(T);
        return result;
    }

    void Skip(uint32_t bytes) {
        if(SAFE && pos + bytes > end) {
            throw SerializationException("Read past end of buffer");
        }
        pos += bytes;
    }
};

template<bool SAFE>
class WKBReader {
private:
    ArenaAllocator &allocator;
    ReadCursor<SAFE> cursor;
    bool has_z;
    bool has_m;
    bool is_first;

    template<WKBByteOrder ORDER>
    uint32_t ReadInt() {
        if(ORDER == WKBByteOrder::NDR) {
            return cursor.template Read<uint32_t>();
        } else {
            auto data = cursor.template Read<uint32_t>();
            // swap bytes
            return (data >> 24) | ((data >> 8) & 0xFF00) | ((data << 8) & 0xFF0000) | (data << 24);
        }
    }
    template<WKBByteOrder ORDER>
    double ReadDouble() {
        if(ORDER == WKBByteOrder::NDR) {
            return cursor.template Read<double>();
        } else {
            auto data = cursor.template Read<uint64_t>();
            // swap bytes
            data = (data >> 56) | ((data >> 40) & 0xFF00) | ((data >> 24) & 0xFF0000) | ((data >> 8) & 0xFF000000) | ((data << 8) & 0xFF00000000) | ((data << 24) & 0xFF0000000000) | ((data << 40) & 0xFF000000000000) | (data << 56);
            double result;
            memcpy(&result, &data, sizeof(double));
            return result;
        }
    }

    template<WKBByteOrder ORDER>
    WKBGeometryType ReadType() {
        uint32_t type = ReadInt<ORDER>();

        // Check for extended WKB flags
        bool geom_has_z = (type & 0x80000000) == 0x80000000;
        if(geom_has_z) {
            type &= ~0x80000000;
        }
        bool geom_has_m = (type & 0x40000000) == 0x40000000;
        if(geom_has_m) {
            type &= ~0x40000000;
        }
        bool geom_has_srid = (type & 0x20000000) == 0x20000000;
        if (geom_has_srid) {
            // SRID present
            uint32_t srid = ReadInt<ORDER>();
            (void)srid;
            // Remove and ignore the srid flag for now
            type &= ~0x20000000;
        }

        // Check for ISO flags
        // TODO: We should just always check for ISO flags unless we are sure we are dealing with extended WKB
        if(type > 1000 && type < 2000) {
            geom_has_z = true;
            type -= 1000;
        } else if(type > 2000 && type < 3000) {
            geom_has_m = true;
            type -= 2000;
        } else if(type > 3000 && type < 4000) {
            geom_has_z = true;
            geom_has_m = true;
            type -= 3000;
        }

        if(is_first) {
            has_z = geom_has_z;
            has_m = geom_has_m;
            is_first = false;
        } else {
            // TODO: Is this even possible? If so, what should we do? Do we upcast?
            if(has_z != geom_has_z) {
                throw SerializationException(
                        "WKB Reader: Z value presence does not match top level geometry, DuckDB requires all geometries inside a WKB blob to have the same amount of coordinate dimensions");
            }
            if (has_m != geom_has_m) {
                throw SerializationException(
                        "WKB Reader: M value presence does not match top level geometry, DuckDB requires all geometries inside a WKB blob to have the same amount of coordinate dimensions");
            }
        }

        return static_cast<WKBGeometryType>(type);
    }

    template<WKBByteOrder ORDER>
    VertexArray ReadVertices(uint32_t count) {
        VertexArray vertices(allocator.GetAllocator(), count, has_z, has_m);
        vertices.Initialize(false);
        if(has_z && has_m) {
            for(uint32_t i = 0; i < count; i++) {
                auto x = ReadDouble<ORDER>();
                auto y = ReadDouble<ORDER>();
                auto z = ReadDouble<ORDER>();
                auto m = ReadDouble<ORDER>();
                vertices.SetUnsafe(i, x, y, z, m);
            }
            return vertices;
        } else if(has_z || has_m) {
            for(uint32_t i = 0; i < count; i++) {
                auto x = ReadDouble<ORDER>();
                auto y = ReadDouble<ORDER>();
                auto zm = ReadDouble<ORDER>();
                vertices.SetUnsafe(i, x, y, zm);
            }
            return vertices;
        } else {
            for(uint32_t i = 0; i < count; i++) {
                auto x = ReadDouble<ORDER>();
                auto y = ReadDouble<ORDER>();
                vertices.SetUnsafe(i, x, y);
            }
            return vertices;
        }
    }

    template<WKBByteOrder ORDER>
    Point ReadPoint() {
        Point point(allocator.GetAllocator(), has_z, has_m);
        if(has_z && has_m) {
            auto x = ReadDouble<ORDER>();
            auto y = ReadDouble<ORDER>();
            auto z = ReadDouble<ORDER>();
            auto m = ReadDouble<ORDER>();
            if(std::isnan(x) && std::isnan(y) && std::isnan(z) && std::isnan(m)) {
                return point;
            }
            point.Vertices().Reserve(1).Initialize(false).SetUnsafe(0, x, y, z, m);
        } else if(has_z || has_m) {
            auto x = ReadDouble<ORDER>();
            auto y = ReadDouble<ORDER>();
            auto zm = ReadDouble<ORDER>();
            if(std::isnan(x) && std::isnan(y) && std::isnan(zm)) {
                return point;
            }
            point.Vertices().Reserve(1).Initialize(false).SetUnsafe(0, x, y, zm);
        } else {
            auto x = ReadDouble<ORDER>();
            auto y = ReadDouble<ORDER>();
            if(std::isnan(x) && std::isnan(y)) {
                return point;
            }
            point.Vertices().Reserve(1).Initialize(false).SetUnsafe(0, x, y);
        }
        return point;
    }

    template<WKBByteOrder ORDER>
    LineString ReadLineString() {
        auto num_points = ReadInt<ORDER>();
        auto vertices = ReadVertices<ORDER>(num_points);
        return LineString(std::move(vertices));
    }

    template<WKBByteOrder ORDER>
    Polygon ReadPolygon() {
        auto num_rings = ReadInt<ORDER>();
        Polygon polygon(allocator.GetAllocator(), num_rings, has_z, has_m);
        for(uint32_t i = 0; i < num_rings; i++) {
            auto num_points = ReadInt<ORDER>();
            polygon[i] = ReadVertices<ORDER>(num_points);
        }
        return polygon;
    }

    template<WKBByteOrder ORDER>
    MultiPoint ReadMultiPoint() {
        auto num_points = ReadInt<ORDER>();
        MultiPoint multi_point(allocator.GetAllocator(), num_points);
        for(uint32_t i = 0; i < num_points; i++) {
            multi_point[i] = ReadPoint<ORDER>();
        }
        return multi_point;
    }

    template<WKBByteOrder ORDER>
    MultiLineString ReadMultiLineString() {
        auto num_lines = ReadInt<ORDER>();
        MultiLineString multi_line_string(allocator.GetAllocator(), num_lines);
        for(uint32_t i = 0; i < num_lines; i++) {
            multi_line_string[i] = ReadLineString<ORDER>();
        }
        return multi_line_string;
    }

    template<WKBByteOrder ORDER>
    MultiPolygon ReadMultiPolygon() {
        auto num_polygons = ReadInt<ORDER>();
        MultiPolygon multi_polygon(allocator.GetAllocator(), num_polygons);
        for(uint32_t i = 0; i < num_polygons; i++) {
            multi_polygon[i] = ReadPolygon<ORDER>();
        }
        return multi_polygon;
    }

    template<WKBByteOrder ORDER>
    GeometryCollection ReadGeometryCollection() {
        auto num_geometries = ReadInt<ORDER>();
        GeometryCollection geometry_collection(allocator.GetAllocator(), num_geometries);
        for(uint32_t i = 0; i < num_geometries; i++) {
            geometry_collection[i] = ReadGeometry();
        }
        return geometry_collection;
    }

    Geometry ReadGeometry() {
        auto byte_order = cursor.template Read<uint8_t>();
        if(SAFE && byte_order != 1 && byte_order != 0) {
            throw SerializationException("WKB Reader: invalid byte order");
        }
        if(byte_order == 1) {
            auto type = ReadType<WKBByteOrder::NDR>();
            switch(type) {
                case WKBGeometryType::POINT:
                    return ReadPoint<WKBByteOrder::NDR>();
                case WKBGeometryType::LINESTRING:
                    return ReadLineString<WKBByteOrder::NDR>();
                case WKBGeometryType::POLYGON:
                    return ReadPolygon<WKBByteOrder::NDR>();
                case WKBGeometryType::MULTIPOINT:
                    return ReadMultiPoint<WKBByteOrder::NDR>();
                case WKBGeometryType::MULTILINESTRING:
                    return ReadMultiLineString<WKBByteOrder::NDR>();
                case WKBGeometryType::MULTIPOLYGON:
                    return ReadMultiPolygon<WKBByteOrder::NDR>();
                case WKBGeometryType::GEOMETRYCOLLECTION:
                    return ReadGeometryCollection<WKBByteOrder::NDR>();
                default:
                    throw SerializationException("WKB Reader: Unsupported geometry type %u", type);
            }
        } else {
            auto type = ReadType<WKBByteOrder::XDR>();
            switch(type) {
                case WKBGeometryType::POINT:
                    return ReadPoint<WKBByteOrder::XDR>();
                case WKBGeometryType::LINESTRING:
                    return ReadLineString<WKBByteOrder::XDR>();
                case WKBGeometryType::POLYGON:
                    return ReadPolygon<WKBByteOrder::XDR>();
                case WKBGeometryType::MULTIPOINT:
                    return ReadMultiPoint<WKBByteOrder::XDR>();
                case WKBGeometryType::MULTILINESTRING:
                    return ReadMultiLineString<WKBByteOrder::XDR>();
                case WKBGeometryType::MULTIPOLYGON:
                    return ReadMultiPolygon<WKBByteOrder::XDR>();
                case WKBGeometryType::GEOMETRYCOLLECTION:
                    return ReadGeometryCollection<WKBByteOrder::XDR>();
                default:
                    throw SerializationException("WKB Reader: Unsupported geometry type %u", type);
            }
        }
    }

public:
    explicit WKBReader(ArenaAllocator &allocator) : allocator(allocator), has_z(false), has_m(false), is_first(false) { }

    Geometry Deserialize(const_data_ptr_t data, uint32_t length) {
        cursor = ReadCursor<SAFE>(data, length);
        is_first = true;
        has_z = false;
        has_m = false;
        return ReadGeometry();
    }

    Geometry Deserialize(const string_t &geom) {
        return Deserialize(const_data_ptr_cast(geom.GetData()), geom.GetSize());
    }

    bool GeomHasZ() const { return has_z; }
    bool GeomHasM() const { return has_m; }
};

} // namespace core

} // namespace spatial
