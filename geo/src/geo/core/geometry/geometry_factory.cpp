#include "geo/common.hpp"
#include "geo/core/geometry/geometry.hpp"
#include "geo/core/geometry/geometry_factory.hpp"
#include "geo/core/geometry/wkb_reader.hpp"

namespace geo {

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

string GeometryFactory::ToWKB(const Geometry &geometry) {
	throw NotImplementedException("WKB not implemented yet");
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

LineString GeometryFactory::CreateLineString(uint32_t num_points)  {
	return LineString(AllocateVertexVector(num_points));
}

Polygon GeometryFactory::CreatePolygon(uint32_t num_rings, uint32_t *ring_capacities)  {
	auto rings = reinterpret_cast<VertexVector *>(allocator.AllocateAligned(sizeof(VertexVector) * num_rings));
	for (uint32_t i = 0; i < num_rings; i++) {
		rings[i] = AllocateVertexVector(ring_capacities[i]);
	}
	return Polygon(rings, num_rings);
}

Polygon GeometryFactory::CreatePolygon(uint32_t num_rings)  {
	auto rings = reinterpret_cast<VertexVector *>(allocator.AllocateAligned(sizeof(VertexVector) * num_rings));
	return Polygon(rings, num_rings);
}

MultiPoint GeometryFactory::CreateMultiPoint(uint32_t num_points)  {
	auto points = reinterpret_cast<Point *>(allocator.AllocateAligned(sizeof(Point) * num_points));
	return MultiPoint(points, num_points);
}

MultiLineString GeometryFactory::CreateMultiLineString(uint32_t num_linestrings)  {
	auto linestrings = reinterpret_cast<LineString *>(allocator.AllocateAligned(sizeof(LineString) * num_linestrings));
	return MultiLineString(linestrings, num_linestrings);
}

MultiPolygon GeometryFactory::CreateMultiPolygon(uint32_t num_polygons)  {
	auto polygons = reinterpret_cast<Polygon *>(allocator.AllocateAligned(sizeof(Polygon) * num_polygons));
	return MultiPolygon(polygons, num_polygons);
}

GeometryCollection GeometryFactory::CreateGeometryCollection(uint32_t num_geometries)  {
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
	auto type = geometry.Type();
	GeometryPrefix prefix(0, type);
	auto header_size = prefix.SerializedSize() + 4; // + 4 for padding

	switch (type) {
	case GeometryType::POINT: {
		auto &point = geometry.GetPoint();
		auto size = header_size + GetSerializedSize(point);
		auto start = allocator.AllocateAligned(size);
		auto ptr = start;
		prefix.Serialize(ptr);
		ptr += 4; // skip padding
		SerializePoint(ptr, point);
		return StringVector::AddStringOrBlob(result, (const char *)start, size);
	}
	case GeometryType::LINESTRING: {
		auto &linestring = geometry.GetLineString();
		auto size = header_size + GetSerializedSize(linestring);
		auto start = allocator.AllocateAligned(size);
		auto ptr = start;

		prefix.Serialize(ptr);
		ptr += 4; // skip padding
		SerializeLineString(ptr, linestring);
		return StringVector::AddStringOrBlob(result, (const char *)start, size);
	}
	case GeometryType::POLYGON: {
		auto &polygon = geometry.GetPolygon();
		auto size = header_size + GetSerializedSize(polygon);
		auto start = allocator.AllocateAligned(size);
		auto ptr = start;
		prefix.Serialize(ptr);
		ptr += 4; // skip padding
		SerializePolygon(ptr, polygon);
		return StringVector::AddStringOrBlob(result, (const char *)start, size);
	}
	case GeometryType::MULTIPOINT: {
		auto &multipoint = geometry.GetMultiPoint();
		auto size = header_size + GetSerializedSize(multipoint);
		auto start = allocator.AllocateAligned(size);
		auto ptr = start;
		prefix.Serialize(ptr);
		ptr += 4; // skip padding
		SerializeMultiPoint(ptr, multipoint);
		return StringVector::AddStringOrBlob(result, (const char *)start, size);
	}
	case GeometryType::MULTILINESTRING: {
		auto &multilinestring = geometry.GetMultiLineString();
		auto size = header_size + GetSerializedSize(multilinestring);
		auto start = allocator.AllocateAligned(size);
		auto ptr = start;
		prefix.Serialize(ptr);
		ptr += 4; // skip padding
		SerializeMultiLineString(ptr, multilinestring);
		return StringVector::AddStringOrBlob(result, (const char *)start, size);
	}
	case GeometryType::MULTIPOLYGON: {
		auto &multipolygon = geometry.GetMultiPolygon();
		auto size = header_size + GetSerializedSize(multipolygon);
		auto start = allocator.AllocateAligned(size);
		auto ptr = start;
		prefix.Serialize(ptr);
		ptr += 4; // skip padding
		SerializeMultiPolygon(ptr, multipolygon);
		return StringVector::AddStringOrBlob(result, (const char *)start, size);
	}
	case GeometryType::GEOMETRYCOLLECTION: {
		auto &collection = geometry.GetGeometryCollection();
		auto size = header_size + GetSerializedSize(collection);
		auto start = allocator.AllocateAligned(size);
		auto ptr = start;
		prefix.Serialize(ptr);
		ptr += 4; // skip padding
		SerializeGeometryCollection(ptr, collection);
		return StringVector::AddStringOrBlob(result, (const char *)start, size);
	}
	default:
		throw NotImplementedException("Unimplemented geometry type for serialization!");
	}
}

void GeometryFactory::SerializePoint(data_ptr_t &ptr, const Point &point) {
	Store<uint32_t>((uint32_t)GeometryType::POINT, ptr);
	ptr += sizeof(uint32_t);

	// write point count
	Store<uint32_t>(point.data.Count(), ptr);
	ptr += sizeof(uint32_t);

	// write data
	point.data.Serialize(ptr);
}

void GeometryFactory::SerializeLineString(data_ptr_t &ptr, const LineString &linestring) {
	Store<uint32_t>((uint32_t)GeometryType::LINESTRING, ptr);
	ptr += sizeof(uint32_t);

	// write point count
	Store<uint32_t>(linestring.points.Count(), ptr);
	ptr += sizeof(uint32_t);

	// write data
	linestring.points.Serialize(ptr);
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
	Store<uint32_t>(multilinestring.num_linestrings, ptr);
	ptr += sizeof(uint32_t);

	// write linestring data
	for (uint32_t i = 0; i < multilinestring.num_linestrings; i++) {
		SerializeLineString(ptr, multilinestring.linestrings[i]);
	}
}

void GeometryFactory::SerializeMultiPolygon(data_ptr_t &ptr, const MultiPolygon &multipolygon) {
	Store<uint32_t>((uint32_t)GeometryType::MULTIPOLYGON, ptr);
	ptr += sizeof(uint32_t);

	// write number of polygons
	Store<uint32_t>(multipolygon.num_polygons, ptr);
	ptr += sizeof(uint32_t);

	// write polygon data
	for (uint32_t i = 0; i < multipolygon.num_polygons; i++) {
		SerializePolygon(ptr, multipolygon.polygons[i]);
	}
}

void GeometryFactory::SerializeGeometryCollection(data_ptr_t &ptr, const GeometryCollection &collection) {
	Store<uint32_t>((uint32_t)GeometryType::GEOMETRYCOLLECTION, ptr);
	ptr += sizeof(uint32_t);

	// write number of geometries
	Store<uint32_t>(collection.num_geometries, ptr);
	ptr += sizeof(uint32_t);

	// write geometry data
	for (uint32_t i = 0; i < collection.num_geometries; i++) {
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
	return 4 + 4 + (point.data.count * sizeof(Vertex));
}

uint32_t GeometryFactory::GetSerializedSize(const LineString &linestring) {
	// 4 bytes for the type
	// 4 bytes for the length
	// sizeof(vertex) * count)
	return 4 + 4 + (linestring.points.count * sizeof(Vertex));
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
	for (uint32_t i = 0; i < multilinestring.num_linestrings; i++) {
		size += GetSerializedSize(multilinestring.linestrings[i]);
	}
	return size;
}

uint32_t GeometryFactory::GetSerializedSize(const MultiPolygon &multipolygon) {
	// 4 bytes for the type
	// 4 bytes for the number of polygons
	// sizeof(polygon) * count
	uint32_t size = 4 + 4;
	for (uint32_t i = 0; i < multipolygon.num_polygons; i++) {
		size += GetSerializedSize(multipolygon.polygons[i]);
	}
	return size;
}

uint32_t GeometryFactory::GetSerializedSize(const GeometryCollection &collection) {
	// 4 bytes for the type
	// 4 bytes for the number of geometries
	// sizeof(geometry) * count
	uint32_t size = 4 + 4;
	for (uint32_t i = 0; i < collection.num_geometries; i++) {
		auto &geom = collection.geometries[i];
		switch (geom.Type()) {
		case GeometryType::POINT:
			size += GetSerializedSize(geom.GetPoint());
			break;
		case GeometryType::LINESTRING:
			size += GetSerializedSize(geom.GetLineString());
			break;
		case GeometryType::POLYGON:
			size += GetSerializedSize(geom.GetPolygon());
			break;
		case GeometryType::MULTIPOINT:
			size += GetSerializedSize(geom.GetMultiPoint());
			break;
		case GeometryType::MULTILINESTRING:
			size += GetSerializedSize(geom.GetMultiLineString());
			break;
		case GeometryType::MULTIPOLYGON:
			size += GetSerializedSize(geom.GetMultiPolygon());
			break;
		case GeometryType::GEOMETRYCOLLECTION:
			size += GetSerializedSize(geom.GetGeometryCollection());
			break;
		default:
			throw NotImplementedException("Unimplemented geometry type for serialization!");
		}
	}
	return size;
}

//----------------------------------------------------------------------
// Deserialization
//----------------------------------------------------------------------
Geometry GeometryFactory::Deserialize(const string_t &data) {
	auto ptr = (const_data_ptr_t)data.GetDataUnsafe();
	// read prefix
	auto type = (GeometryType)Load<uint8_t>(ptr++);
	auto flags = (uint8_t)Load<uint8_t>(ptr++);
	auto _pad1 = (uint8_t)Load<uint8_t>(ptr++);
	auto _pad2 = (uint8_t)Load<uint8_t>(ptr++);

	GeometryPrefix prefix(flags, type);

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

} // namespace core

} // namespace geo
