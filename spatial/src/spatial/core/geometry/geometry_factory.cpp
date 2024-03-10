#include "spatial/core/geometry/geometry_factory.hpp"

#include "spatial/common.hpp"
#include "spatial/core/geometry/cursor.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_processor.hpp"

namespace spatial {

namespace core {

VertexArray GeometryFactory::AllocateVertexArray(uint32_t capacity, bool has_z, bool has_m) {
	return VertexArray {allocator.GetAllocator(), capacity, has_z, has_m};
}

Point GeometryFactory::CreatePoint(double x, double y) {
	auto data = AllocateVertexArray(1, false, false);
	data.Append({x, y});
	return Point(std::move(data));
}

LineString GeometryFactory::CreateLineString(uint32_t num_points, bool has_z, bool has_m) {
	return LineString(AllocateVertexArray(num_points, has_z, has_m));
}

Polygon GeometryFactory::CreatePolygon(uint32_t num_rings, uint32_t *ring_capacities, bool has_z, bool has_m) {
	Polygon polygon(allocator.GetAllocator(), num_rings, has_z, has_m);
	for (uint32_t i = 0; i < num_rings; i++) {
		polygon[i].Reserve(ring_capacities[i]);
	}
	return polygon;
}

Polygon GeometryFactory::CreatePolygon(uint32_t num_rings) {
	Polygon polygon(allocator.GetAllocator(), num_rings, false, false);
	return polygon;
}

MultiPoint GeometryFactory::CreateMultiPoint(uint32_t num_points) {
	MultiPoint multipoint(allocator.GetAllocator(), num_points);
	return multipoint;
}

MultiLineString GeometryFactory::CreateMultiLineString(uint32_t num_linestrings) {
	MultiLineString multilinestring(allocator.GetAllocator(), num_linestrings);
	return multilinestring;
}

MultiPolygon GeometryFactory::CreateMultiPolygon(uint32_t num_polygons) {
	MultiPolygon multipolygon(allocator.GetAllocator(), num_polygons);
	return multipolygon;
}

GeometryCollection GeometryFactory::CreateGeometryCollection(uint32_t num_geometries) {
	GeometryCollection collection(allocator.GetAllocator(), num_geometries);
	return collection;
}

Polygon GeometryFactory::CreateBox(double xmin, double ymin, double xmax, double ymax) {
	Polygon polygon(allocator.GetAllocator(), 1, false, false);
	auto &shell = polygon[0];
	shell.Reserve(5);
	shell.Append({xmin, ymin});
	shell.Append({xmin, ymax});
	shell.Append({xmax, ymax});
	shell.Append({xmax, ymin});
	shell.Append({xmin, ymin}); // close the ring
	return polygon;
}

// Empty
Point GeometryFactory::CreateEmptyPoint() {
	return Point(VertexArray::CreateEmpty(allocator.GetAllocator(), false, false));
}

LineString GeometryFactory::CreateEmptyLineString() {
	return LineString(VertexArray::CreateEmpty(allocator.GetAllocator(), false, false));
}

Polygon GeometryFactory::CreateEmptyPolygon() {
	return Polygon(allocator.GetAllocator(), 0, false, false);
}

MultiPoint GeometryFactory::CreateEmptyMultiPoint() {
	return MultiPoint(allocator.GetAllocator(), 0);
}

MultiLineString GeometryFactory::CreateEmptyMultiLineString() {
	return MultiLineString(allocator.GetAllocator(), 0);
}

MultiPolygon GeometryFactory::CreateEmptyMultiPolygon() {
	return MultiPolygon(allocator.GetAllocator(), 0);
}

GeometryCollection GeometryFactory::CreateEmptyGeometryCollection() {
	return GeometryCollection(allocator.GetAllocator(), 0);
}

//----------------------------------------------------------------------
// Serialization
//----------------------------------------------------------------------
// We always want the coordinates to be double aligned (8 bytes)
// layout:
// GeometryHeader (4 bytes)
// Padding (4 bytes) (or SRID?)
// Data (variable length)
// -- Point
// 	  Type ( 4 bytes)
//    Count (4 bytes) (count == 0 if empty point, otherwise 1)
//    X (8 bytes)
//    Y (8 bytes)
// -- LineString
//    Type (4 bytes)
//    Length (4 bytes)
//    Points (variable length)
// -- Polygon
//    Type (4 bytes)
//    NumRings (4 bytes)
// 	  RingsLengths (variable length)
//    padding (4 bytes if num_rings is odd)
//    RingsData (variable length)
// --- Multi/Point/LineString/Polygon & GeometryCollection
//    Type (4 bytes)
//    NumGeometries (4 bytes)
//    Geometries (variable length)

geometry_t GeometryFactory::Serialize(Vector &result, const Geometry &geometry, bool has_z, bool has_m) {
	auto geom_size = GetSerializedSize(geometry);

	auto type = geometry.Type();
	bool has_bbox = type != GeometryType::POINT && !geometry.IsEmpty();

	// TODO: Keep around properties
	// auto properties = geometry.Properties();
	GeometryProperties properties;
	properties.SetBBox(has_bbox);
	properties.SetZ(has_z);
	properties.SetM(has_m);
	uint16_t hash = 0;

	auto header_size = 4;
    auto dims = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);
    auto bbox_size = has_bbox ? (sizeof(float) * dims) : 0;
	auto size = header_size + 4 + bbox_size + geom_size; // + 4 for padding, + 16 for bbox
	auto blob = StringVector::EmptyString(result, size);
	Cursor cursor(blob);

	// Write the header
	cursor.Write<GeometryType>(type);
	cursor.Write<GeometryProperties>(properties);
	cursor.Write<uint16_t>(hash);
	// Pad with 4 bytes (we might want to use this to store SRID in the future)
	cursor.Write<uint32_t>(0);

	// All geometries except points have a bounding box
	BoundingBox bbox;
	auto bbox_ptr = cursor.GetPtr();

    // skip the bounding box for now
    // we will come back and write it later
    cursor.Skip(bbox_size);

	switch (type) {
	case GeometryType::POINT: {
		auto &point = geometry.As<Point>();
		SerializePoint(cursor, point, bbox);
		break;
	}
	case GeometryType::LINESTRING: {
		auto &linestring = geometry.As<LineString>();
		SerializeLineString(cursor, linestring, bbox);
		break;
	}
	case GeometryType::POLYGON: {
		auto &polygon = geometry.As<Polygon>();
		SerializePolygon(cursor, polygon, bbox);
		break;
	}
	case GeometryType::MULTIPOINT: {
		auto &multipoint = geometry.As<MultiPoint>();
		SerializeMultiPoint(cursor, multipoint, bbox);
		break;
	}
	case GeometryType::MULTILINESTRING: {
		auto &multilinestring = geometry.As<MultiLineString>();
		SerializeMultiLineString(cursor, multilinestring, bbox);
		break;
	}
	case GeometryType::MULTIPOLYGON: {
		auto &multipolygon = geometry.As<MultiPolygon>();
		SerializeMultiPolygon(cursor, multipolygon, bbox);
		break;
	}
	case GeometryType::GEOMETRYCOLLECTION: {
		auto &collection = geometry.As<GeometryCollection>();
		SerializeGeometryCollection(cursor, collection, bbox);
		break;
	}
	default:
		auto msg = StringUtil::Format("Unimplemented geometry type for serialization: %d", type);
		throw SerializationException(msg);
	}

	// Now write the bounding box
	if (has_bbox) {
		cursor.SetPtr(bbox_ptr);
		// We serialize the bounding box as floats to save space, but ensure that the bounding box is
		// still large enough to contain the original double values by rounding up and down
		cursor.Write<float>(Utils::DoubleToFloatDown(bbox.minx));
		cursor.Write<float>(Utils::DoubleToFloatDown(bbox.miny));
		cursor.Write<float>(Utils::DoubleToFloatUp(bbox.maxx));
		cursor.Write<float>(Utils::DoubleToFloatUp(bbox.maxy));
        if(has_z) {
            cursor.Write<float>(Utils::DoubleToFloatDown(bbox.minz));
            cursor.Write<float>(Utils::DoubleToFloatUp(bbox.maxz));
        }
        if(has_m) {
            cursor.Write<float>(Utils::DoubleToFloatDown(bbox.minm));
            cursor.Write<float>(Utils::DoubleToFloatUp(bbox.maxm));
        }
	}
	blob.Finalize();
	return geometry_t(blob);
}

void GeometryFactory::SerializeVertexArray(Cursor &cursor, const VertexArray &array, bool update_bounds,
                                           BoundingBox &bbox) {
	// Write the vertex data
	auto byte_size = array.ByteSize();
	memcpy(cursor.GetPtr(), array.GetData(), byte_size);
	// Move the cursor forward
	cursor.Skip(byte_size);
	// Also update the bounds real quick
	if (update_bounds) {
        auto props = array.GetProperties();
        auto has_m = props.HasM();
        auto has_z = props.HasZ();
        if(has_m && has_z) {
            for (uint32_t i = 0; i < array.Count(); i++) {
                auto vertex = array.GetTemplated<VertexXYZM>(i);
                bbox.maxx = std::max(bbox.maxx, vertex.x);
                bbox.maxy = std::max(bbox.maxy, vertex.y);
                bbox.minx = std::min(bbox.minx, vertex.x);
                bbox.miny = std::min(bbox.miny, vertex.y);
                bbox.maxz = std::max(bbox.maxz, vertex.z);
                bbox.minz = std::min(bbox.minz, vertex.z);
                bbox.maxm = std::max(bbox.maxm, vertex.m);
                bbox.minm = std::min(bbox.minm, vertex.m);
            }
        } else if(has_z) {
            for (uint32_t i = 0; i < array.Count(); i++) {
                auto vertex = array.GetTemplated<VertexXYZ>(i);
                bbox.maxx = std::max(bbox.maxx, vertex.x);
                bbox.maxy = std::max(bbox.maxy, vertex.y);
                bbox.minx = std::min(bbox.minx, vertex.x);
                bbox.miny = std::min(bbox.miny, vertex.y);
                bbox.maxz = std::max(bbox.maxz, vertex.z);
                bbox.minz = std::min(bbox.minz, vertex.z);
            }
        } else if (has_m) {
            for (uint32_t i = 0; i < array.Count(); i++) {
                auto vertex = array.GetTemplated<VertexXYM>(i);
                bbox.maxx = std::max(bbox.maxx, vertex.x);
                bbox.maxy = std::max(bbox.maxy, vertex.y);
                bbox.minx = std::min(bbox.minx, vertex.x);
                bbox.miny = std::min(bbox.miny, vertex.y);
                bbox.maxm = std::max(bbox.maxm, vertex.m);
                bbox.minm = std::min(bbox.minm, vertex.m);
            }
        } else {
            for (uint32_t i = 0; i < array.Count(); i++) {
                auto vertex = array.GetTemplated<VertexXY>(i);
                bbox.maxx = std::max(bbox.maxx, vertex.x);
                bbox.maxy = std::max(bbox.maxy, vertex.y);
                bbox.minx = std::min(bbox.minx, vertex.x);
                bbox.miny = std::min(bbox.miny, vertex.y);
            }
        }
	}
}

void GeometryFactory::SerializePoint(Cursor &cursor, const Point &point, BoundingBox &bbox) {
	// Write type (4 bytes)
	cursor.Write(SerializedGeometryType::POINT);

	// Write point count (0 or 1) (4 bytes)
	cursor.Write<uint32_t>(point.Vertices().Count());

	// write data
	SerializeVertexArray(cursor, point.Vertices(), true, bbox);
}

void GeometryFactory::SerializeLineString(Cursor &cursor, const LineString &linestring, BoundingBox &bbox) {
	// Write type (4 bytes)
	cursor.Write(SerializedGeometryType::LINESTRING);

	// Write point count (4 bytes)
	cursor.Write<uint32_t>(linestring.Vertices().Count());

	// write data
	SerializeVertexArray(cursor, linestring.Vertices(), true, bbox);
}

void GeometryFactory::SerializePolygon(Cursor &cursor, const Polygon &polygon, BoundingBox &bbox) {
	// Write type (4 bytes)
	cursor.Write(SerializedGeometryType::POLYGON);

	// Write number of rings (4 bytes)
	cursor.Write<uint32_t>(polygon.RingCount());

	// Write ring lengths
	for (const auto &ring : polygon) {
		cursor.Write<uint32_t>(ring.Count());
	}

	if (polygon.RingCount() % 2 == 1) {
		// Write padding (4 bytes)
		cursor.Write<uint32_t>(0);
	}

	// Write ring data
	for (uint32_t i = 0; i < polygon.RingCount(); i++) {
		if (i == 0) {
			// The first ring is always the shell, and must be the only ring contributing to the bounding box
			// or the geometry is invalid.
			SerializeVertexArray(cursor, polygon[i], true, bbox);
		} else {
			SerializeVertexArray(cursor, polygon[i], false, bbox);
		}
	}
}

void GeometryFactory::SerializeMultiPoint(Cursor &cursor, const MultiPoint &multipoint, BoundingBox &bbox) {
	// Write type (4 bytes)
	cursor.Write(SerializedGeometryType::MULTIPOINT);

	// Write number of points (4 bytes)
	cursor.Write<uint32_t>(multipoint.ItemCount());

	// Write point data
	for (const auto &point : multipoint) {
		SerializePoint(cursor, point, bbox);
	}
}

void GeometryFactory::SerializeMultiLineString(Cursor &cursor, const MultiLineString &multilinestring,
                                               BoundingBox &bbox) {
	// Write type (4 bytes)
	cursor.Write(SerializedGeometryType::MULTILINESTRING);

	// Write number of linestrings (4 bytes)
	cursor.Write<uint32_t>(multilinestring.ItemCount());

	// Write linestring data
	for (const auto &linestring : multilinestring) {
		SerializeLineString(cursor, linestring, bbox);
	}
}

void GeometryFactory::SerializeMultiPolygon(Cursor &cursor, const MultiPolygon &multipolygon, BoundingBox &bbox) {
	// Write type (4 bytes)
	cursor.Write(SerializedGeometryType::MULTIPOLYGON);

	// Write number of polygons (4 bytes)
	cursor.Write<uint32_t>(multipolygon.ItemCount());

	// Write polygon data
	for (const auto &polygon : multipolygon) {
		SerializePolygon(cursor, polygon, bbox);
	}
}

void GeometryFactory::SerializeGeometryCollection(Cursor &cursor, const GeometryCollection &collection,
                                                  BoundingBox &bbox) {
	// Write type (4 bytes)
	cursor.Write(SerializedGeometryType::GEOMETRYCOLLECTION);

	// Write number of geometries (4 bytes)
	cursor.Write<uint32_t>(collection.ItemCount());

	// write geometry data
	for (const auto &geom : collection) {
		switch (geom.Type()) {
		case GeometryType::POINT:
			SerializePoint(cursor, geom.As<Point>(), bbox);
			break;
		case GeometryType::LINESTRING:
			SerializeLineString(cursor, geom.As<LineString>(), bbox);
			break;
		case GeometryType::POLYGON:
			SerializePolygon(cursor, geom.As<Polygon>(), bbox);
			break;
		case GeometryType::MULTIPOINT:
			SerializeMultiPoint(cursor, geom.As<MultiPoint>(), bbox);
			break;
		case GeometryType::MULTILINESTRING:
			SerializeMultiLineString(cursor, geom.As<MultiLineString>(), bbox);
			break;
		case GeometryType::MULTIPOLYGON:
			SerializeMultiPolygon(cursor, geom.As<MultiPolygon>(), bbox);
			break;
		case GeometryType::GEOMETRYCOLLECTION:
			SerializeGeometryCollection(cursor, geom.As<GeometryCollection>(), bbox);
			break;
		default:
			throw NotImplementedException("Unimplemented geometry type!");
		}
	}
}

bool GeometryFactory::TryGetSerializedBoundingBox(const geometry_t &data, BoundingBox &bbox) {
	Cursor cursor(data);

	// Read the header
	auto header_type = cursor.Read<GeometryType>();
	auto properties = cursor.Read<GeometryProperties>();
	auto hash = cursor.Read<uint16_t>();
	(void)hash;

	if (properties.HasBBox()) {
		cursor.Skip(4); // skip padding

		// Now set the bounding box
		bbox.minx = cursor.Read<float>();
		bbox.miny = cursor.Read<float>();
		bbox.maxx = cursor.Read<float>();
		bbox.maxy = cursor.Read<float>();
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
		bbox.minx = x;
		bbox.miny = y;
		bbox.maxx = x;
		bbox.maxy = y;
		return true;
	}
	return false;
}

//----------------------------------------------------------------------
// Serialized Size
//----------------------------------------------------------------------

uint32_t GeometryFactory::GetSerializedSize(const Point &point) {
	// 4 bytes for the type
	// 4 bytes for the length
	// sizeof(vertex) * count (either 0 or 16)
	return 4 + 4 + (point.Vertices().ByteSize());
}

uint32_t GeometryFactory::GetSerializedSize(const LineString &linestring) {
	// 4 bytes for the type
	// 4 bytes for the length
	// sizeof(vertex) * count)
	return 4 + 4 + (linestring.Vertices().ByteSize());
}

uint32_t GeometryFactory::GetSerializedSize(const Polygon &polygon) {
	// 4 bytes for the type
	// 4 bytes for the number of rings
	// 4 bytes for the number of vertices in each ring
	// sizeof(vertex) * count
	// 4 bytes for padding if num_rings is odd
	uint32_t size = 4 + 4;
	for (const auto &ring : polygon) {
		size += 4;
		size += ring.ByteSize();
	}
	if (polygon.RingCount() % 2 == 1) {
		size += 4;
	}
	return size;
}

uint32_t GeometryFactory::GetSerializedSize(const MultiPoint &multipoint) {
	// 4 bytes for the type
	// 4 bytes for the number of points
	// sizeof(point) * count
	uint32_t size = 4 + 4;
	for (const auto &point : multipoint) {
		size += GetSerializedSize(point);
	}
	return size;
}

uint32_t GeometryFactory::GetSerializedSize(const MultiLineString &multilinestring) {
	// 4 bytes for the type
	// 4 bytes for the number of linestrings
	// sizeof(linestring) * count
	uint32_t size = 4 + 4;
	for (const auto &linestring : multilinestring) {
		size += GetSerializedSize(linestring);
	}
	return size;
}

uint32_t GeometryFactory::GetSerializedSize(const MultiPolygon &multipolygon) {
	// 4 bytes for the type
	// 4 bytes for the number of polygons
	// sizeof(polygon) * count
	uint32_t size = 4 + 4;
	for (const auto &polygon : multipolygon) {
		size += GetSerializedSize(polygon);
	}
	return size;
}

uint32_t GeometryFactory::GetSerializedSize(const GeometryCollection &collection) {
	// 4 bytes for the type
	// 4 bytes for the number of geometries
	// sizeof(geometry) * count
	uint32_t size = 4 + 4;
	for (const auto &geom : collection) {
		size += GetSerializedSize(geom);
	}
	return size;
}

uint32_t GeometryFactory::GetSerializedSize(const Geometry &geometry) {
	switch (geometry.Type()) {
	case GeometryType::POINT:
		return GetSerializedSize(geometry.As<Point>());
	case GeometryType::LINESTRING:
		return GetSerializedSize(geometry.As<LineString>());
	case GeometryType::POLYGON:
		return GetSerializedSize(geometry.As<Polygon>());
	case GeometryType::MULTIPOINT:
		return GetSerializedSize(geometry.As<MultiPoint>());
	case GeometryType::MULTILINESTRING:
		return GetSerializedSize(geometry.As<MultiLineString>());
	case GeometryType::MULTIPOLYGON:
		return GetSerializedSize(geometry.As<MultiPolygon>());
	case GeometryType::GEOMETRYCOLLECTION:
		return GetSerializedSize(geometry.As<GeometryCollection>());
	default:
		throw NotImplementedException("Unimplemented geometry type!");
	}
}

//----------------------------------------------------------------------
// Deserialization
//----------------------------------------------------------------------
class GeometryDeserializer final : GeometryProcessor<Geometry> {
    Allocator &allocator;

    VertexArray ProcessVertices(const VertexData &vertices) {
        VertexArray array(allocator, const_cast<data_ptr_t>(vertices.data[0]), vertices.count, HasZ(), HasM());
        return array;
    }

    Geometry ProcessPoint(const VertexData &vertices) override {
        if (vertices.IsEmpty()) {
            return Point(allocator);
        }
        return Point(ProcessVertices(vertices));
    }

    Geometry ProcessLineString(const VertexData &vertices) override {
        return LineString(ProcessVertices(vertices));
    }

    Geometry ProcessPolygon(PolygonState &state) override {
        Polygon polygon(allocator, state.RingCount(), HasZ(), HasM());
        for (auto i = 0; i < state.RingCount(); i++) {
            polygon[i] = ProcessVertices(state.Next());
        }
        return polygon;
    }

    Geometry ProcessCollection(CollectionState &state) override {
        switch(CurrentType()) {
            case GeometryType::MULTIPOINT: {
                auto multi_point = MultiPoint(allocator, state.ItemCount());
                for (auto i = 0; i < state.ItemCount(); i++) {
                    multi_point[i] = state.Next().As<Point>();
                }
            }
            case GeometryType::MULTILINESTRING: {
                auto multi_line_string = MultiLineString(allocator, state.ItemCount());
                for (auto i = 0; i < state.ItemCount(); i++) {
                    multi_line_string[i] = state.Next().As<LineString>();
                }
            }
            case GeometryType::MULTIPOLYGON: {
                auto multi_polygon = MultiPolygon(allocator, state.ItemCount());
                for (auto i = 0; i < state.ItemCount(); i++) {
                    multi_polygon[i] = state.Next().As<Polygon>();
                }
            }
            case GeometryType::GEOMETRYCOLLECTION: {
                auto collection = GeometryCollection(allocator, state.ItemCount());
                for (auto i = 0; i < state.ItemCount(); i++) {
                    collection[i] = state.Next();
                }
            }
            default:
                throw NotImplementedException("GeometryDeserializer: Unimplemented geometry type: %d", CurrentType());
        }
    }

public:
    explicit GeometryDeserializer(Allocator &allocator) : allocator(allocator) { }
    Geometry Execute(const geometry_t &data) {
        return Process(data);
    }
};

Geometry GeometryFactory::Deserialize(const geometry_t &data) {
	GeometryDeserializer deserializer(allocator.GetAllocator());
    return deserializer.Execute(data);
}

} // namespace core

} // namespace spatial
