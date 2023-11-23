#include "spatial/core/geometry/geometry_factory.hpp"

#include "spatial/common.hpp"
#include "spatial/core/geometry/cursor.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/wkb_reader.hpp"
#include "spatial/core/geometry/wkb_writer.hpp"

namespace spatial {

namespace core {

Geometry GeometryFactory::FromWKT(const char *wkt, uint32_t length) {
	throw NotImplementedException("WKT not implemented yet");
}

string GeometryFactory::ToWKT(const Geometry &geometry) {
	throw NotImplementedException("WKT not implemented yet");
}
// Parse "standard" WKB format
Geometry GeometryFactory::FromWKB(const char *wkb, uint32_t length) {
	WKBReader reader(*this, wkb, length);
	return reader.ReadGeometry();
}

data_ptr_t GeometryFactory::ToWKB(const Geometry &geometry, uint32_t *size) {
	auto required_size = WKBWriter::GetRequiredSize(geometry);
	auto ptr = allocator.AllocateAligned(required_size);
	auto cursor = ptr;
	WKBWriter::Write(geometry, cursor);
	*size = required_size;
	return ptr;
}

VertexVector GeometryFactory::AllocateVertexVector(uint32_t capacity) {
	auto data = allocator.AllocateAligned(sizeof(Vertex) * capacity);
	return VertexVector(data, 0, capacity);
}

Point GeometryFactory::CreatePoint(double x, double y) {
	auto data = AllocateVertexVector(1);
	data.Add(Vertex(x, y));
	return Point(data);
}

LineString GeometryFactory::CreateLineString(uint32_t num_points) {
	return LineString(AllocateVertexVector(num_points));
}

Polygon GeometryFactory::CreatePolygon(uint32_t num_rings, uint32_t *ring_capacities) {
	auto rings = reinterpret_cast<VertexVector *>(allocator.AllocateAligned(sizeof(VertexVector) * num_rings));
	for (uint32_t i = 0; i < num_rings; i++) {
		rings[i] = AllocateVertexVector(ring_capacities[i]);
	}
	return Polygon(rings, num_rings);
}

Polygon GeometryFactory::CreatePolygon(uint32_t num_rings) {
	auto rings = reinterpret_cast<VertexVector *>(allocator.AllocateAligned(sizeof(VertexVector) * num_rings));
	return Polygon(rings, num_rings);
}

MultiPoint GeometryFactory::CreateMultiPoint(uint32_t num_points) {
	auto points = reinterpret_cast<Point *>(allocator.AllocateAligned(sizeof(Point) * num_points));
	return MultiPoint(points, num_points);
}

MultiLineString GeometryFactory::CreateMultiLineString(uint32_t num_linestrings) {
	auto line_strings = reinterpret_cast<LineString *>(allocator.AllocateAligned(sizeof(LineString) * num_linestrings));
	return MultiLineString(line_strings, num_linestrings);
}

MultiPolygon GeometryFactory::CreateMultiPolygon(uint32_t num_polygons) {
	auto polygons = reinterpret_cast<Polygon *>(allocator.AllocateAligned(sizeof(Polygon) * num_polygons));
	return MultiPolygon(polygons, num_polygons);
}

GeometryCollection GeometryFactory::CreateGeometryCollection(uint32_t num_geometries) {
	auto geometries = reinterpret_cast<Geometry *>(allocator.AllocateAligned(sizeof(Geometry) * num_geometries));
	return GeometryCollection(geometries, num_geometries);
}

Polygon GeometryFactory::CreateBox(double xmin, double ymin, double xmax, double ymax) {
	auto rings = reinterpret_cast<VertexVector *>(allocator.AllocateAligned(sizeof(VertexVector) * 1));
	auto &shell = rings[0];
	shell = AllocateVertexVector(5);
	shell.Add(Vertex(xmin, ymin));
	shell.Add(Vertex(xmin, ymax));
	shell.Add(Vertex(xmax, ymax));
	shell.Add(Vertex(xmax, ymin));
	shell.Add(Vertex(xmin, ymin)); // close the ring
	return Polygon(rings, 1);
}

// Empty
Point GeometryFactory::CreateEmptyPoint() {
	return Point(AllocateVertexVector(0));
}

LineString GeometryFactory::CreateEmptyLineString() {
	return LineString(AllocateVertexVector(0));
}

Polygon GeometryFactory::CreateEmptyPolygon() {
	return Polygon(nullptr, 0);
}

MultiPoint GeometryFactory::CreateEmptyMultiPoint() {
	return MultiPoint(nullptr, 0);
}

MultiLineString GeometryFactory::CreateEmptyMultiLineString() {
	return MultiLineString(nullptr, 0);
}

MultiPolygon GeometryFactory::CreateEmptyMultiPolygon() {
	return MultiPolygon(nullptr, 0);
}

GeometryCollection GeometryFactory::CreateEmptyGeometryCollection() {
	return GeometryCollection(nullptr, 0);
}

enum class SerializedGeometryType : uint32_t {
	POINT,
	LINESTRING,
	POLYGON,
	MULTIPOINT,
	MULTILINESTRING,
	MULTIPOLYGON,
	GEOMETRYCOLLECTION
};

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

string_t GeometryFactory::Serialize(VectorStringBuffer& buffer, const spatial::core::Geometry &geometry) {
	auto geom_size = GetSerializedSize(geometry);

	auto type = geometry.Type();
	bool has_bbox = type != GeometryType::POINT && !geometry.IsEmpty();

	auto properties = geometry.Properties();
	properties.SetBBox(has_bbox);
	uint16_t hash = 0;

	// Hash geom_size uint32_t to uint16_t
	for (uint32_t i = 0; i < sizeof(uint32_t); i++) {
		hash ^= (geom_size >> (i * 8)) & 0xFF;
	}
	GeometryHeader header(type, properties, hash);

	auto header_size = sizeof(GeometryHeader);
	auto size = header_size + 4 + (has_bbox ? 16 : 0) + geom_size; // + 4 for padding, + 16 for bbox
//	auto data_p = reinterpret_cast<char*>(this->allocator.Allocate(size));
	auto blob = buffer.EmptyString(size);
	Cursor cursor(blob);

	// Write the header
	cursor.Write(header);
	// Pad with 4 bytes (we might want to use this to store SRID in the future)
	cursor.Write<uint32_t>(0);

	// All geometries except points have a bounding box
	BoundingBox bbox;
	auto bbox_ptr = cursor.GetPtr();
	if (has_bbox) {
		// skip the bounding box for now
		// we will come back and write it later
		cursor.Skip(16);
	}

	switch (type) {
	case GeometryType::POINT: {
		auto &point = geometry.GetPoint();
		SerializePoint(cursor, point, bbox);
		break;
	}
	case GeometryType::LINESTRING: {
		auto &linestring = geometry.GetLineString();
		SerializeLineString(cursor, linestring, bbox);
		break;
	}
	case GeometryType::POLYGON: {
		auto &polygon = geometry.GetPolygon();
		SerializePolygon(cursor, polygon, bbox);
		break;
	}
	case GeometryType::MULTIPOINT: {
		auto &multipoint = geometry.GetMultiPoint();
		SerializeMultiPoint(cursor, multipoint, bbox);
		break;
	}
	case GeometryType::MULTILINESTRING: {
		auto &multilinestring = geometry.GetMultiLineString();
		SerializeMultiLineString(cursor, multilinestring, bbox);
		break;
	}
	case GeometryType::MULTIPOLYGON: {
		auto &multipolygon = geometry.GetMultiPolygon();
		SerializeMultiPolygon(cursor, multipolygon, bbox);
		break;
	}
	case GeometryType::GEOMETRYCOLLECTION: {
		auto &collection = geometry.GetGeometryCollection();
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
	}
	blob.Finalize();
	return blob;
}

string_t GeometryFactory::Serialize(Vector &result, const Geometry &geometry) {
	auto geom_size = GetSerializedSize(geometry);

	auto type = geometry.Type();
	bool has_bbox = type != GeometryType::POINT && !geometry.IsEmpty();

	auto properties = geometry.Properties();
	properties.SetBBox(has_bbox);
	uint16_t hash = 0;

	// Hash geom_size uint32_t to uint16_t
	for (uint32_t i = 0; i < sizeof(uint32_t); i++) {
		hash ^= (geom_size >> (i * 8)) & 0xFF;
	}
	GeometryHeader header(type, properties, hash);

	auto header_size = sizeof(GeometryHeader);
	auto size = header_size + 4 + (has_bbox ? 16 : 0) + geom_size; // + 4 for padding, + 16 for bbox
	auto blob = StringVector::EmptyString(result, size);
	Cursor cursor(blob);

	// Write the header
	cursor.Write(header);
	// Pad with 4 bytes (we might want to use this to store SRID in the future)
	cursor.Write<uint32_t>(0);

	// All geometries except points have a bounding box
	BoundingBox bbox;
	auto bbox_ptr = cursor.GetPtr();
	if (has_bbox) {
		// skip the bounding box for now
		// we will come back and write it later
		cursor.Skip(16);
	}

	switch (type) {
	case GeometryType::POINT: {
		auto &point = geometry.GetPoint();
		SerializePoint(cursor, point, bbox);
		break;
	}
	case GeometryType::LINESTRING: {
		auto &linestring = geometry.GetLineString();
		SerializeLineString(cursor, linestring, bbox);
		break;
	}
	case GeometryType::POLYGON: {
		auto &polygon = geometry.GetPolygon();
		SerializePolygon(cursor, polygon, bbox);
		break;
	}
	case GeometryType::MULTIPOINT: {
		auto &multipoint = geometry.GetMultiPoint();
		SerializeMultiPoint(cursor, multipoint, bbox);
		break;
	}
	case GeometryType::MULTILINESTRING: {
		auto &multilinestring = geometry.GetMultiLineString();
		SerializeMultiLineString(cursor, multilinestring, bbox);
		break;
	}
	case GeometryType::MULTIPOLYGON: {
		auto &multipolygon = geometry.GetMultiPolygon();
		SerializeMultiPolygon(cursor, multipolygon, bbox);
		break;
	}
	case GeometryType::GEOMETRYCOLLECTION: {
		auto &collection = geometry.GetGeometryCollection();
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
	}
	blob.Finalize();
	return blob;
}

void GeometryFactory::SerializePoint(Cursor &cursor, const Point &point, BoundingBox &bbox) {
	// Write type (4 bytes)
	cursor.Write(SerializedGeometryType::POINT);

	// Write point count (0 or 1) (4 bytes)
	cursor.Write<uint32_t>(point.vertices.Count());

	// write data
	point.vertices.SerializeAndUpdateBounds(cursor, bbox);
}

void GeometryFactory::SerializeLineString(Cursor &cursor, const LineString &linestring, BoundingBox &bbox) {
	// Write type (4 bytes)
	cursor.Write(SerializedGeometryType::LINESTRING);

	// Write point count (4 bytes)
	cursor.Write<uint32_t>(linestring.vertices.Count());

	// write data
	linestring.vertices.SerializeAndUpdateBounds(cursor, bbox);
}

void GeometryFactory::SerializePolygon(Cursor &cursor, const Polygon &polygon, BoundingBox &bbox) {
	// Write type (4 bytes)
	cursor.Write(SerializedGeometryType::POLYGON);

	// Write number of rings (4 bytes)
	cursor.Write<uint32_t>(polygon.num_rings);

	// Write ring lengths
	for (uint32_t i = 0; i < polygon.num_rings; i++) {
		cursor.Write<uint32_t>(polygon.rings[i].Count());
	}

	if (polygon.num_rings % 2 == 1) {
		// Write padding (4 bytes)
		cursor.Write<uint32_t>(0);
	}

	// Write ring data
	for (uint32_t i = 0; i < polygon.num_rings; i++) {
		if (i == 0) {
			// The first ring is always the shell, and must be the only ring contributing to the bounding box
			// or the geometry is invalid.
			polygon.rings[i].SerializeAndUpdateBounds(cursor, bbox);
		} else {
			polygon.rings[i].Serialize(cursor);
		}
	}
}

void GeometryFactory::SerializeMultiPoint(Cursor &cursor, const MultiPoint &multipoint, BoundingBox &bbox) {
	// Write type (4 bytes)
	cursor.Write(SerializedGeometryType::MULTIPOINT);

	// Write number of points (4 bytes)
	cursor.Write<uint32_t>(multipoint.num_points);

	// Write point data
	for (uint32_t i = 0; i < multipoint.num_points; i++) {
		SerializePoint(cursor, multipoint.points[i], bbox);
	}
}

void GeometryFactory::SerializeMultiLineString(Cursor &cursor, const MultiLineString &multilinestring,
                                               BoundingBox &bbox) {
	// Write type (4 bytes)
	cursor.Write(SerializedGeometryType::MULTILINESTRING);

	// Write number of linestrings (4 bytes)
	cursor.Write<uint32_t>(multilinestring.count);

	// Write linestring data
	for (uint32_t i = 0; i < multilinestring.count; i++) {
		SerializeLineString(cursor, multilinestring.lines[i], bbox);
	}
}

void GeometryFactory::SerializeMultiPolygon(Cursor &cursor, const MultiPolygon &multipolygon, BoundingBox &bbox) {
	// Write type (4 bytes)
	cursor.Write(SerializedGeometryType::MULTIPOLYGON);

	// Write number of polygons (4 bytes)
	cursor.Write<uint32_t>(multipolygon.count);

	// Write polygon data
	for (uint32_t i = 0; i < multipolygon.count; i++) {
		SerializePolygon(cursor, multipolygon.polygons[i], bbox);
	}
}

void GeometryFactory::SerializeGeometryCollection(Cursor &cursor, const GeometryCollection &collection,
                                                  BoundingBox &bbox) {
	// Write type (4 bytes)
	cursor.Write(SerializedGeometryType::GEOMETRYCOLLECTION);

	// Write number of geometries (4 bytes)
	cursor.Write<uint32_t>(collection.count);

	// write geometry data
	for (uint32_t i = 0; i < collection.count; i++) {
		auto &geom = collection.geometries[i];
		switch (geom.Type()) {
		case GeometryType::POINT:
			SerializePoint(cursor, geom.GetPoint(), bbox);
			break;
		case GeometryType::LINESTRING:
			SerializeLineString(cursor, geom.GetLineString(), bbox);
			break;
		case GeometryType::POLYGON:
			SerializePolygon(cursor, geom.GetPolygon(), bbox);
			break;
		case GeometryType::MULTIPOINT:
			SerializeMultiPoint(cursor, geom.GetMultiPoint(), bbox);
			break;
		case GeometryType::MULTILINESTRING:
			SerializeMultiLineString(cursor, geom.GetMultiLineString(), bbox);
			break;
		case GeometryType::MULTIPOLYGON:
			SerializeMultiPolygon(cursor, geom.GetMultiPolygon(), bbox);
			break;
		case GeometryType::GEOMETRYCOLLECTION:
			SerializeGeometryCollection(cursor, geom.GetGeometryCollection(), bbox);
			break;
		default:
			throw NotImplementedException("Unimplemented geometry type!");
		}
	}
}

bool GeometryFactory::TryGetSerializedBoundingBox(const string_t &data, BoundingBox &bbox) {
	Cursor cursor(data);

	// Read the header
	auto header = cursor.Read<GeometryHeader>();
	if (header.properties.HasBBox()) {
		cursor.Skip(4); // skip padding

		// Now set the bounding box
		bbox.minx = cursor.Read<float>();
		bbox.miny = cursor.Read<float>();
		bbox.maxx = cursor.Read<float>();
		bbox.maxy = cursor.Read<float>();
		return true;
	}

	if (header.type == GeometryType::POINT) {
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
	return 4 + 4 + (point.vertices.count * sizeof(Vertex));
}

uint32_t GeometryFactory::GetSerializedSize(const LineString &linestring) {
	// 4 bytes for the type
	// 4 bytes for the length
	// sizeof(vertex) * count)
	return 4 + 4 + (linestring.vertices.count * sizeof(Vertex));
}

uint32_t GeometryFactory::GetSerializedSize(const Polygon &polygon) {
	// 4 bytes for the type
	// 4 bytes for the number of rings
	// 4 bytes for the number of vertices in each ring
	// sizeof(vertex) * count
	// 4 bytes for padding if num_rings is odd
	uint32_t size = 4 + 4;
	for (uint32_t i = 0; i < polygon.num_rings; i++) {
		size += 4;
		size += polygon.rings[i].count * sizeof(Vertex);
	}
	if (polygon.num_rings % 2 == 1) {
		size += 4;
	}
	return size;
}

uint32_t GeometryFactory::GetSerializedSize(const MultiPoint &multipoint) {
	// 4 bytes for the type
	// 4 bytes for the number of points
	// sizeof(point) * count
	uint32_t size = 4 + 4;
	for (uint32_t i = 0; i < multipoint.num_points; i++) {
		size += GetSerializedSize(multipoint.points[i]);
	}
	return size;
}

uint32_t GeometryFactory::GetSerializedSize(const MultiLineString &multilinestring) {
	// 4 bytes for the type
	// 4 bytes for the number of linestrings
	// sizeof(linestring) * count
	uint32_t size = 4 + 4;
	for (uint32_t i = 0; i < multilinestring.count; i++) {
		size += GetSerializedSize(multilinestring.lines[i]);
	}
	return size;
}

uint32_t GeometryFactory::GetSerializedSize(const MultiPolygon &multipolygon) {
	// 4 bytes for the type
	// 4 bytes for the number of polygons
	// sizeof(polygon) * count
	uint32_t size = 4 + 4;
	for (uint32_t i = 0; i < multipolygon.count; i++) {
		size += GetSerializedSize(multipolygon.polygons[i]);
	}
	return size;
}

uint32_t GeometryFactory::GetSerializedSize(const GeometryCollection &collection) {
	// 4 bytes for the type
	// 4 bytes for the number of geometries
	// sizeof(geometry) * count
	uint32_t size = 4 + 4;
	for (uint32_t i = 0; i < collection.count; i++) {
		size += GetSerializedSize(collection.geometries[i]);
	}
	return size;
}

uint32_t GeometryFactory::GetSerializedSize(const Geometry &geometry) {
	switch (geometry.Type()) {
	case GeometryType::POINT:
		return GetSerializedSize(geometry.GetPoint());
	case GeometryType::LINESTRING:
		return GetSerializedSize(geometry.GetLineString());
	case GeometryType::POLYGON:
		return GetSerializedSize(geometry.GetPolygon());
	case GeometryType::MULTIPOINT:
		return GetSerializedSize(geometry.GetMultiPoint());
	case GeometryType::MULTILINESTRING:
		return GetSerializedSize(geometry.GetMultiLineString());
	case GeometryType::MULTIPOLYGON:
		return GetSerializedSize(geometry.GetMultiPolygon());
	case GeometryType::GEOMETRYCOLLECTION:
		return GetSerializedSize(geometry.GetGeometryCollection());
	default:
		throw NotImplementedException("Unimplemented geometry type!");
	}
}

//----------------------------------------------------------------------
// Deserialization
//----------------------------------------------------------------------
Geometry GeometryFactory::Deserialize(const string_t &data) {
	Cursor cursor(data);
	GeometryHeader header = cursor.Read<GeometryHeader>();
	cursor.Skip(4); // Skip padding

	if (header.properties.HasBBox()) {
		cursor.Skip(16); // Skip bounding box
	}

	// peek the type
	auto type = cursor.Peek<SerializedGeometryType>();
	switch (type) {
	case SerializedGeometryType::POINT:
		return Geometry(DeserializePoint(cursor));
	case SerializedGeometryType::LINESTRING:
		return Geometry(DeserializeLineString(cursor));
	case SerializedGeometryType::POLYGON:
		return Geometry(DeserializePolygon(cursor));
	case SerializedGeometryType::MULTIPOINT:
		return Geometry(DeserializeMultiPoint(cursor));
	case SerializedGeometryType::MULTILINESTRING:
		return Geometry(DeserializeMultiLineString(cursor));
	case SerializedGeometryType::MULTIPOLYGON:
		return Geometry(DeserializeMultiPolygon(cursor));
	case SerializedGeometryType::GEOMETRYCOLLECTION:
		return Geometry(DeserializeGeometryCollection(cursor));
	default:
		throw NotImplementedException(
		    StringUtil::Format("Deserialize: Geometry type %d not supported", static_cast<int>(type)));
	}
}

Point GeometryFactory::DeserializePoint(Cursor &reader) {
	auto type = reader.Read<SerializedGeometryType>();
	D_ASSERT(type == SerializedGeometryType::POINT);
	(void)type;

	// Points can be empty too, in which case the count is 0
	auto count = reader.Read<uint32_t>();
	if (count == 0) {
		VertexVector vertex_data(reader.GetPtr(), 0, 0);
		return Point(vertex_data);
	} else {
		D_ASSERT(count == 1);
		VertexVector vertex_data(reader.GetPtr(), 1, 1);
		// Move the pointer forward (in case we are reading from a collection type)
		reader.Skip(sizeof(Vertex));
		return Point(vertex_data);
	}
}

LineString GeometryFactory::DeserializeLineString(Cursor &reader) {
	auto type = reader.Read<SerializedGeometryType>();
	D_ASSERT(type == SerializedGeometryType::LINESTRING);
	(void)type;
	// 0 if the linestring is empty
	auto count = reader.Read<uint32_t>();
	// read data
	VertexVector vertex_data(reader.GetPtr(), count, count);

	reader.Skip(count * sizeof(Vertex));

	return LineString(vertex_data);
}

Polygon GeometryFactory::DeserializePolygon(Cursor &reader) {
	auto type = reader.Read<SerializedGeometryType>();
	D_ASSERT(type == SerializedGeometryType::POLYGON);
	(void)type;
	// read num rings
	auto num_rings = reader.Read<uint32_t>();

	auto rings = reinterpret_cast<VertexVector *>(allocator.AllocateAligned(sizeof(VertexVector) * num_rings));

	// Read the count and corresponding ring in parallel
	auto data_ptr = reader.GetPtr() + sizeof(uint32_t) * num_rings + ((num_rings % 2) * sizeof(uint32_t));
	for (uint32_t i = 0; i < num_rings; i++) {
		auto count = reader.Read<uint32_t>();
		rings[i] = VertexVector(data_ptr, count, count);
		data_ptr += count * sizeof(Vertex);
	}
	reader.SetPtr(data_ptr);
	return Polygon(rings, num_rings);
}

MultiPoint GeometryFactory::DeserializeMultiPoint(Cursor &reader) {
	auto type = reader.Read<SerializedGeometryType>();
	D_ASSERT(type == SerializedGeometryType::MULTIPOINT);
	(void)type;
	// read num points
	auto num_points = reader.Read<uint32_t>();

	auto points = reinterpret_cast<Point *>(allocator.AllocateAligned(sizeof(Point) * num_points));
	for (uint32_t i = 0; i < num_points; i++) {
		points[i] = DeserializePoint(reader);
	}
	return MultiPoint(points, num_points);
}

MultiLineString GeometryFactory::DeserializeMultiLineString(Cursor &reader) {
	auto type = reader.Read<SerializedGeometryType>();
	D_ASSERT(type == SerializedGeometryType::MULTILINESTRING);
	(void)type;
	// read num linestrings
	auto num_linestrings = reader.Read<uint32_t>();

	auto linestrings = reinterpret_cast<LineString *>(allocator.AllocateAligned(sizeof(LineString) * num_linestrings));
	for (uint32_t i = 0; i < num_linestrings; i++) {
		linestrings[i] = DeserializeLineString(reader);
	}
	return MultiLineString(linestrings, num_linestrings);
}

MultiPolygon GeometryFactory::DeserializeMultiPolygon(Cursor &reader) {
	auto type = reader.Read<SerializedGeometryType>();
	D_ASSERT(type == SerializedGeometryType::MULTIPOLYGON);
	(void)type;
	// read num polygons
	auto num_polygons = reader.Read<uint32_t>();

	auto polygons = reinterpret_cast<Polygon *>(allocator.AllocateAligned(sizeof(Polygon) * num_polygons));
	for (uint32_t i = 0; i < num_polygons; i++) {
		polygons[i] = DeserializePolygon(reader);
	}
	return MultiPolygon(polygons, num_polygons);
}

GeometryCollection GeometryFactory::DeserializeGeometryCollection(Cursor &reader) {
	auto type = reader.Read<SerializedGeometryType>();
	D_ASSERT(type == SerializedGeometryType::GEOMETRYCOLLECTION);
	(void)type;
	// read num geometries
	auto num_geometries = reader.Read<uint32_t>();
	auto geometries = reinterpret_cast<Geometry *>(allocator.AllocateAligned(sizeof(Geometry) * num_geometries));
	for (uint32_t i = 0; i < num_geometries; i++) {
		// peek at the type
		auto geometry_type = reader.Peek<SerializedGeometryType>();
		switch (geometry_type) {
		case SerializedGeometryType::POINT:
			geometries[i] = Geometry(DeserializePoint(reader));
			break;
		case SerializedGeometryType::LINESTRING:
			geometries[i] = Geometry(DeserializeLineString(reader));
			break;
		case SerializedGeometryType::POLYGON:
			geometries[i] = Geometry(DeserializePolygon(reader));
			break;
		case SerializedGeometryType::MULTIPOINT:
			geometries[i] = Geometry(DeserializeMultiPoint(reader));
			break;
		case SerializedGeometryType::MULTILINESTRING:
			geometries[i] = Geometry(DeserializeMultiLineString(reader));
			break;
		case SerializedGeometryType::MULTIPOLYGON:
			geometries[i] = Geometry(DeserializeMultiPolygon(reader));
			break;
		case SerializedGeometryType::GEOMETRYCOLLECTION:
			geometries[i] = Geometry(DeserializeGeometryCollection(reader));
			break;
		default:
			auto msg = StringUtil::Format("Unimplemented geometry type for deserialization: %d", geometry_type);
			throw SerializationException(msg);
		}
	}
	return GeometryCollection(geometries, num_geometries);
}

//----------------------------------------------------------------------
// Copy
//----------------------------------------------------------------------

VertexVector GeometryFactory::CopyVertexVector(const VertexVector &vector) {
	auto result = VertexVector(vector);
	result.data = allocator.AllocateAligned(vector.capacity * sizeof(Vertex));
	memcpy(result.data, vector.data, vector.capacity * sizeof(Vertex));
	return result;
}

Point GeometryFactory::CopyPoint(const Point &point) {
	auto result = Point(point);
	result.vertices = CopyVertexVector(point.vertices);
	return result;
}

LineString GeometryFactory::CopyLineString(const LineString &linestring) {
	auto result = LineString(linestring);
	result.vertices = CopyVertexVector(linestring.vertices);
	return result;
}

Polygon GeometryFactory::CopyPolygon(const Polygon &polygon) {
	auto result = Polygon(polygon);
	result.rings = (VertexVector *)allocator.AllocateAligned(sizeof(VertexVector) * polygon.num_rings);
	for (idx_t i = 0; i < polygon.num_rings; i++) {
		result.rings[i] = CopyVertexVector(polygon.rings[i]);
	}
	return result;
}

MultiPoint GeometryFactory::CopyMultiPoint(const MultiPoint &multipoint) {
	auto result = MultiPoint(multipoint);
	result.points = (Point *)allocator.AllocateAligned(sizeof(Point) * multipoint.num_points);
	for (idx_t i = 0; i < multipoint.num_points; i++) {
		result.points[i] = CopyPoint(multipoint.points[i]);
	}
	return result;
}

MultiLineString GeometryFactory::CopyMultiLineString(const MultiLineString &multilinestring) {
	auto result = MultiLineString(multilinestring);
	result.lines = (LineString *)allocator.AllocateAligned(sizeof(LineString) * multilinestring.Count());
	for (idx_t i = 0; i < multilinestring.Count(); i++) {
		result.lines[i] = CopyLineString(multilinestring.lines[i]);
	}
	return result;
}

MultiPolygon GeometryFactory::CopyMultiPolygon(const MultiPolygon &multipolygon) {
	auto result = MultiPolygon(multipolygon);
	result.polygons = (Polygon *)allocator.AllocateAligned(sizeof(Polygon) * multipolygon.Count());
	for (idx_t i = 0; i < multipolygon.Count(); i++) {
		result.polygons[i] = CopyPolygon(multipolygon.polygons[i]);
	}
	return result;
}

GeometryCollection GeometryFactory::CopyGeometryCollection(const GeometryCollection &collection) {
	auto result = GeometryCollection(collection);
	result.geometries = (Geometry *)allocator.AllocateAligned(sizeof(Geometry) * collection.Count());
	for (idx_t i = 0; i < collection.Count(); i++) {
		result.geometries[i] = CopyGeometry(collection.geometries[i]);
	}
	return result;
}

Geometry GeometryFactory::CopyGeometry(const Geometry &geometry) {
	switch (geometry.type) {
	case GeometryType::POINT:
		return Geometry(CopyPoint(geometry.GetPoint()));
	case GeometryType::LINESTRING:
		return Geometry(CopyLineString(geometry.GetLineString()));
	case GeometryType::POLYGON:
		return Geometry(CopyPolygon(geometry.GetPolygon()));
	case GeometryType::MULTIPOINT:
		return Geometry(CopyMultiPoint(geometry.GetMultiPoint()));
	case GeometryType::MULTILINESTRING:
		return Geometry(CopyMultiLineString(geometry.GetMultiLineString()));
	case GeometryType::MULTIPOLYGON:
		return Geometry(CopyMultiPolygon(geometry.GetMultiPolygon()));
	case GeometryType::GEOMETRYCOLLECTION:
		return Geometry(CopyGeometryCollection(geometry.GetGeometryCollection()));
	default:
		throw NotImplementedException("Unimplemented geometry type for copy");
	}
}

} // namespace core

} // namespace spatial
