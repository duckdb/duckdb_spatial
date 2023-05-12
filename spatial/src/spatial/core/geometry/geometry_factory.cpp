#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
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
	auto data = reinterpret_cast<Vertex *>(allocator.AllocateAligned(sizeof(Vertex) * capacity));
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

//----------------------------------------------------------------------
// Serialization
//----------------------------------------------------------------------
// We always want the coordinates to be double aligned (8 bytes)
// layout:
// GeometryPrefix (4 bytes)
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

string_t GeometryFactory::Serialize(Vector &result, const Geometry &geometry) {
	auto geom_size = GetSerializedSize(geometry);

	auto type = geometry.Type();

	// Hash geom_size uint32_t to uint16_t
	uint16_t hash = 0;
	for (uint32_t i = 0; i < sizeof(uint32_t); i++) {
		hash ^= (geom_size >> (i * 8)) & 0xFF;
	}

	GeometryPrefix prefix(0, type, hash);
	auto header_size = prefix.SerializedSize() + 4; // + 4 for padding

	auto size = header_size + geom_size;
	auto str = StringVector::EmptyString(result, size);
	auto ptr = (uint8_t *)str.GetDataUnsafe();
	prefix.Serialize(ptr);
	ptr += 4; // skip padding

	switch (type) {
	case GeometryType::POINT: {
		auto &point = geometry.GetPoint();
		SerializePoint(ptr, point);
		return str;
	}
	case GeometryType::LINESTRING: {
		auto &linestring = geometry.GetLineString();
		SerializeLineString(ptr, linestring);
		return str;
	}
	case GeometryType::POLYGON: {
		auto &polygon = geometry.GetPolygon();
		SerializePolygon(ptr, polygon);
		return str;
	}
	case GeometryType::MULTIPOINT: {
		auto &multipoint = geometry.GetMultiPoint();
		SerializeMultiPoint(ptr, multipoint);
		return str;
	}
	case GeometryType::MULTILINESTRING: {
		auto &multilinestring = geometry.GetMultiLineString();
		SerializeMultiLineString(ptr, multilinestring);
		return str;
	}
	case GeometryType::MULTIPOLYGON: {
		auto &multipolygon = geometry.GetMultiPolygon();
		SerializeMultiPolygon(ptr, multipolygon);
		return str;
	}
	case GeometryType::GEOMETRYCOLLECTION: {
		auto &collection = geometry.GetGeometryCollection();
		SerializeGeometryCollection(ptr, collection);
		return str;
	}
	default:
		throw NotImplementedException("Unimplemented geometry type for serialization!");
	}
}

void GeometryFactory::SerializePoint(data_ptr_t &ptr, const Point &point) {
	Store<uint32_t>((uint32_t)GeometryType::POINT, ptr);
	ptr += sizeof(uint32_t);

	// write point count
	Store<uint32_t>(point.vertices.Count(), ptr);
	ptr += sizeof(uint32_t);

	// write data
	point.vertices.Serialize(ptr);
}

void GeometryFactory::SerializeLineString(data_ptr_t &ptr, const LineString &linestring) {
	Store<uint32_t>((uint32_t)GeometryType::LINESTRING, ptr);
	ptr += sizeof(uint32_t);

	// write point count
	Store<uint32_t>(linestring.vertices.Count(), ptr);
	ptr += sizeof(uint32_t);

	// write data
	linestring.vertices.Serialize(ptr);
}

void GeometryFactory::SerializePolygon(data_ptr_t &ptr, const Polygon &polygon) {
	Store<uint32_t>((uint32_t)GeometryType::POLYGON, ptr);
	ptr += sizeof(uint32_t);

	// write number of rings
	Store<uint32_t>(polygon.num_rings, ptr);
	ptr += sizeof(uint32_t);

	// write ring counts
	for (uint32_t i = 0; i < polygon.num_rings; i++) {
		Store<uint32_t>(polygon.rings[i].Count(), ptr);
		ptr += sizeof(uint32_t);
	}

	if (polygon.num_rings % 2 == 1) {
		// padding
		Store<uint32_t>(0, ptr);
		ptr += sizeof(uint32_t);
	}

	// write ring data
	for (uint32_t i = 0; i < polygon.num_rings; i++) {
		polygon.rings[i].Serialize(ptr);
	}
}

void GeometryFactory::SerializeMultiPoint(data_ptr_t &ptr, const MultiPoint &multipoint) {
	Store<uint32_t>((uint32_t)GeometryType::MULTIPOINT, ptr);
	ptr += sizeof(uint32_t);

	// write point count
	Store<uint32_t>(multipoint.num_points, ptr);
	ptr += sizeof(uint32_t);

	// write data
	for (uint32_t i = 0; i < multipoint.num_points; i++) {
		SerializePoint(ptr, multipoint.points[i]);
	}
}

void GeometryFactory::SerializeMultiLineString(data_ptr_t &ptr, const MultiLineString &multilinestring) {
	Store<uint32_t>((uint32_t)GeometryType::MULTILINESTRING, ptr);
	ptr += sizeof(uint32_t);

	// write number of linestrings
	Store<uint32_t>(multilinestring.count, ptr);
	ptr += sizeof(uint32_t);

	// write linestring data
	for (uint32_t i = 0; i < multilinestring.count; i++) {
		SerializeLineString(ptr, multilinestring.lines[i]);
	}
}

void GeometryFactory::SerializeMultiPolygon(data_ptr_t &ptr, const MultiPolygon &multipolygon) {
	Store<uint32_t>((uint32_t)GeometryType::MULTIPOLYGON, ptr);
	ptr += sizeof(uint32_t);

	// write number of polygons
	Store<uint32_t>(multipolygon.count, ptr);
	ptr += sizeof(uint32_t);

	// write polygon data
	for (uint32_t i = 0; i < multipolygon.count; i++) {
		SerializePolygon(ptr, multipolygon.polygons[i]);
	}
}

void GeometryFactory::SerializeGeometryCollection(data_ptr_t &ptr, const GeometryCollection &collection) {
	Store<uint32_t>((uint32_t)GeometryType::GEOMETRYCOLLECTION, ptr);
	ptr += sizeof(uint32_t);

	// write number of geometries
	Store<uint32_t>(collection.count, ptr);
	ptr += sizeof(uint32_t);

	// write geometry data
	for (uint32_t i = 0; i < collection.count; i++) {
		auto &geom = collection.geometries[i];
		switch (geom.Type()) {
		case GeometryType::POINT:
			SerializePoint(ptr, geom.GetPoint());
			break;
		case GeometryType::LINESTRING:
			SerializeLineString(ptr, geom.GetLineString());
			break;
		case GeometryType::POLYGON:
			SerializePolygon(ptr, geom.GetPolygon());
			break;
		case GeometryType::MULTIPOINT:
			SerializeMultiPoint(ptr, geom.GetMultiPoint());
			break;
		case GeometryType::MULTILINESTRING:
			SerializeMultiLineString(ptr, geom.GetMultiLineString());
			break;
		case GeometryType::MULTIPOLYGON:
			SerializeMultiPolygon(ptr, geom.GetMultiPolygon());
			break;
		case GeometryType::GEOMETRYCOLLECTION:
			SerializeGeometryCollection(ptr, geom.GetGeometryCollection());
			break;
		default:
			throw NotImplementedException("Unimplemented geometry type!");
		}
	}
}

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
	auto ptr = (const_data_ptr_t)data.GetDataUnsafe();
	// read prefix
	auto type = (GeometryType)Load<uint8_t>(ptr++);
	auto flags = (uint8_t)Load<uint8_t>(ptr++);
	auto hash = Load<uint16_t>(ptr);
	ptr += sizeof(uint16_t);

	GeometryPrefix prefix(flags, type, hash);

	// skip padding
	ptr += sizeof(uint32_t);

	// peek the type
	auto ty = (GeometryType)Load<uint32_t>(ptr);
	switch (ty) {
	case GeometryType::POINT:
		return Geometry(DeserializePoint(ptr));
	case GeometryType::LINESTRING:
		return Geometry(DeserializeLineString(ptr));
	case GeometryType::POLYGON:
		return Geometry(DeserializePolygon(ptr));
	case GeometryType::MULTIPOINT:
		return Geometry(DeserializeMultiPoint(ptr));
	case GeometryType::MULTILINESTRING:
		return Geometry(DeserializeMultiLineString(ptr));
	case GeometryType::MULTIPOLYGON:
		return Geometry(DeserializeMultiPolygon(ptr));
	case GeometryType::GEOMETRYCOLLECTION:
		return Geometry(DeserializeGeometryCollection(ptr));
	default:
		throw NotImplementedException("Deserialize::Geometry type not implemented yet!");
	}
}

Point GeometryFactory::DeserializePoint(const_data_ptr_t &ptr) {
	auto type = Load<uint32_t>(ptr);
	ptr += sizeof(uint32_t);
	D_ASSERT(type == (uint32_t)GeometryType::POINT);

	// Points can be empty too, in which case the count is 0
	auto count = Load<uint32_t>(ptr);
	ptr += sizeof(uint32_t);

	if (count == 0) {
		VertexVector vertex_data((Vertex *)ptr, 0, 0);
		return Point(vertex_data);
	} else {
		D_ASSERT(count == 1);
		VertexVector vertex_data((Vertex *)ptr, 1, 1);
		// Move the pointer forward (in case we are reading from a collection type)
		ptr += sizeof(Vertex);
		return Point(vertex_data);
	}
}

LineString GeometryFactory::DeserializeLineString(const_data_ptr_t &ptr) {
	auto type = Load<uint32_t>(ptr);
	ptr += sizeof(uint32_t);
	D_ASSERT(type == (uint32_t)GeometryType::LINESTRING);

	// 0 if the linestring is empty
	auto length = Load<uint32_t>(ptr);
	ptr += sizeof(uint32_t);

	// read data
	VertexVector vertex_data((Vertex *)ptr, length, length);

	ptr += length * sizeof(Vertex);

	return LineString(vertex_data);
}

Polygon GeometryFactory::DeserializePolygon(const_data_ptr_t &ptr) {
	auto type = Load<uint32_t>(ptr);
	ptr += sizeof(uint32_t);
	D_ASSERT(type == (uint32_t)GeometryType::POLYGON);

	// read num rings
	auto num_rings = Load<uint32_t>(ptr);
	ptr += sizeof(uint32_t);

	auto rings = reinterpret_cast<VertexVector *>(allocator.AllocateAligned(sizeof(VertexVector) * num_rings));
	auto data_offset = ptr + sizeof(uint32_t) * num_rings + ((num_rings % 2) * sizeof(uint32_t));
	for (uint32_t i = 0; i < num_rings; i++) {
		// read length
		auto length = Load<uint32_t>(ptr);
		ptr += sizeof(uint32_t);
		rings[i] = VertexVector((Vertex *)data_offset, length, length);
		data_offset += length * sizeof(Vertex);
	}
	ptr = data_offset;

	return Polygon(rings, num_rings);
}

MultiPoint GeometryFactory::DeserializeMultiPoint(const_data_ptr_t &ptr) {
	auto type = Load<uint32_t>(ptr);
	ptr += sizeof(uint32_t);
	D_ASSERT(type == (uint32_t)GeometryType::MULTIPOINT);

	// read num points
	auto num_points = Load<uint32_t>(ptr);
	ptr += sizeof(uint32_t);

	auto points = reinterpret_cast<Point *>(allocator.AllocateAligned(sizeof(Point) * num_points));
	for (uint32_t i = 0; i < num_points; i++) {
		points[i] = DeserializePoint(ptr);
	}
	return MultiPoint(points, num_points);
}

MultiLineString GeometryFactory::DeserializeMultiLineString(const_data_ptr_t &ptr) {
	auto type = Load<uint32_t>(ptr);
	ptr += sizeof(uint32_t);
	D_ASSERT(type == (uint32_t)GeometryType::MULTILINESTRING);

	// read num linestrings
	auto num_linestrings = Load<uint32_t>(ptr);
	ptr += sizeof(uint32_t);

	auto linestrings = reinterpret_cast<LineString *>(allocator.AllocateAligned(sizeof(LineString) * num_linestrings));
	for (uint32_t i = 0; i < num_linestrings; i++) {
		linestrings[i] = DeserializeLineString(ptr);
	}
	return MultiLineString(linestrings, num_linestrings);
}

MultiPolygon GeometryFactory::DeserializeMultiPolygon(const_data_ptr_t &ptr) {
	auto type = Load<uint32_t>(ptr);
	ptr += sizeof(uint32_t);
	D_ASSERT(type == (uint32_t)GeometryType::MULTIPOLYGON);

	// read num polygons
	auto num_polygons = Load<uint32_t>(ptr);
	ptr += sizeof(uint32_t);

	auto polygons = reinterpret_cast<Polygon *>(allocator.AllocateAligned(sizeof(Polygon) * num_polygons));
	for (uint32_t i = 0; i < num_polygons; i++) {
		polygons[i] = DeserializePolygon(ptr);
	}
	return MultiPolygon(polygons, num_polygons);
}

GeometryCollection GeometryFactory::DeserializeGeometryCollection(const_data_ptr_t &ptr) {
	auto type = Load<uint32_t>(ptr);
	ptr += sizeof(uint32_t);
	D_ASSERT(type == (uint32_t)GeometryType::GEOMETRYCOLLECTION);

	// read num geometries
	auto num_geometries = Load<uint32_t>(ptr);
	ptr += sizeof(uint32_t);

	auto geometries = reinterpret_cast<Geometry *>(allocator.AllocateAligned(sizeof(Geometry) * num_geometries));
	for (uint32_t i = 0; i < num_geometries; i++) {
		// peek at the type
		auto geometry_type = (GeometryType)Load<uint32_t>(ptr);
		switch (geometry_type) {
		case GeometryType::POINT:
			geometries[i] = Geometry(DeserializePoint(ptr));
			break;
		case GeometryType::LINESTRING:
			geometries[i] = Geometry(DeserializeLineString(ptr));
			break;
		case GeometryType::POLYGON:
			geometries[i] = Geometry(DeserializePolygon(ptr));
			break;
		case GeometryType::MULTIPOINT:
			geometries[i] = Geometry(DeserializeMultiPoint(ptr));
			break;
		case GeometryType::MULTILINESTRING:
			geometries[i] = Geometry(DeserializeMultiLineString(ptr));
			break;
		case GeometryType::MULTIPOLYGON:
			geometries[i] = Geometry(DeserializeMultiPolygon(ptr));
			break;
		case GeometryType::GEOMETRYCOLLECTION:
			geometries[i] = Geometry(DeserializeGeometryCollection(ptr));
			break;
		}
	}
	return GeometryCollection(geometries, num_geometries);
}

//----------------------------------------------------------------------
// Copy
//----------------------------------------------------------------------

VertexVector GeometryFactory::CopyVertexVector(const VertexVector &vector) {
	auto result = VertexVector(vector);
	result.data = (Vertex *)allocator.AllocateAligned(vector.capacity * sizeof(Vertex));
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
