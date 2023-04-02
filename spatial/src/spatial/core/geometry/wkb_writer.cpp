#include "spatial/common.hpp"
#include "spatial/core/geometry/vertex_vector.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/wkb_writer.hpp"

namespace spatial {

namespace core {

enum class WKBGeometryType : uint32_t {
	POINT = 1,
	LINESTRING = 2,
	POLYGON = 3,
	MULTIPOINT = 4,
	MULTILINESTRING = 5,
	MULTIPOLYGON = 6,
	GEOMETRYCOLLECTION = 7
};

uint32_t WKBWriter::GetRequiredSize(const Geometry &geom) {
	switch (geom.Type()) {
	case GeometryType::POINT: return GetRequiredSize(geom.GetPoint());
		case GeometryType::LINESTRING: return GetRequiredSize(geom.GetLineString());
		case GeometryType::POLYGON: return GetRequiredSize(geom.GetPolygon());
		case GeometryType::MULTIPOINT: return GetRequiredSize(geom.GetMultiPoint());
		case GeometryType::MULTILINESTRING: return GetRequiredSize(geom.GetMultiLineString());
		case GeometryType::MULTIPOLYGON: return GetRequiredSize(geom.GetMultiPolygon());
		case GeometryType::GEOMETRYCOLLECTION: return GetRequiredSize(geom.GetGeometryCollection());
		default:
			throw NotImplementedException("Geometry type not supported");
	}
}

uint32_t WKBWriter::GetRequiredSize(const Point &point) {
	// Byte order + type + x + y
	return sizeof(uint8_t) + sizeof(uint32_t) + sizeof(double) + sizeof(double);
}

uint32_t WKBWriter::GetRequiredSize(const LineString &line) {
	// Byte order + type + count + (count * (x + y))
	return sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t) + line.Count() * sizeof(double) * 2;
}

uint32_t WKBWriter::GetRequiredSize(const Polygon &poly) {
	// Byte order + type + count + (count * (ring_count[i] * (x + y)))
	uint32_t size = sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
	for (uint32_t i = 0; i < poly.Count(); i++) {
		size += sizeof(uint32_t) + poly.rings[i].Count() * sizeof(double) * 2;
	}
	return size;
}

uint32_t WKBWriter::GetRequiredSize(const MultiPoint &multi_point) {
	uint32_t size = sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
	for (uint32_t i = 0; i < multi_point.Count(); i++) {
		size += GetRequiredSize(multi_point.points[i]);
	}
	return size;
}

uint32_t WKBWriter::GetRequiredSize(const MultiLineString &multi_line) {
	uint32_t size = sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
	for (uint32_t i = 0; i < multi_line.Count(); i++) {
		size += GetRequiredSize(multi_line.linestrings[i]);
	}
	return size;
}

uint32_t WKBWriter::GetRequiredSize(const MultiPolygon &multi_poly) {
	uint32_t size = sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
	for (uint32_t i = 0; i < multi_poly.Count(); i++) {
		size += GetRequiredSize(multi_poly.polygons[i]);
	}
	return size;
}

uint32_t WKBWriter::GetRequiredSize(const GeometryCollection &collection) {
	uint32_t size = sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
	for (uint32_t i = 0; i < collection.Count(); i++) {
		size += GetRequiredSize(collection.geometries[i]);
	}
	return size;
}

static void WriteByte(uint8_t value, data_ptr_t &ptr) {
	Store<uint8_t>(value, ptr);
	ptr += sizeof(uint8_t);
}

static void WriteInt(uint32_t value, data_ptr_t &ptr) {
	Store<uint32_t>(value, ptr);
	ptr += sizeof(uint32_t);
}

static void WriteDouble(double value, data_ptr_t &ptr) {
	Store<double>(value, ptr);
	ptr += sizeof(double);
}

// Public API
void WKBWriter::Write(const Geometry &geom, data_ptr_t &ptr) {
	switch (geom.Type()) {
	case GeometryType::POINT: Write(geom.GetPoint(), ptr); break;
		case GeometryType::LINESTRING: Write(geom.GetLineString(), ptr); break;
		case GeometryType::POLYGON: Write(geom.GetPolygon(), ptr); break;
		case GeometryType::MULTIPOINT: Write(geom.GetMultiPoint(), ptr); break;
		case GeometryType::MULTILINESTRING: Write(geom.GetMultiLineString(), ptr); break;
		case GeometryType::MULTIPOLYGON: Write(geom.GetMultiPolygon(), ptr); break;
		case GeometryType::GEOMETRYCOLLECTION: Write(geom.GetGeometryCollection(), ptr); break;
		default:
			throw NotImplementedException("Geometry type not supported");
	}
}

void WKBWriter::Write(const Point &point, data_ptr_t &ptr) {
	WriteByte(1, ptr); // byte order
	WriteInt(static_cast<uint32_t>(WKBGeometryType::POINT), ptr); // geometry type

	if(point.IsEmpty()) {
		auto x = std::numeric_limits<double>::quiet_NaN();
		auto y = std::numeric_limits<double>::quiet_NaN();
		WriteDouble(x, ptr);
		WriteDouble(y, ptr);
	} else {
		auto &vertex = point.GetVertex();
		WriteDouble(vertex.x, ptr);
		WriteDouble(vertex.y, ptr);
	}
}

void WKBWriter::Write(const LineString &line, data_ptr_t &ptr) {
	WriteByte(1, ptr); // byte order
	WriteInt(static_cast<uint32_t>(WKBGeometryType::LINESTRING), ptr); // geometry type

	auto num_points = line.Count();
	WriteInt(num_points, ptr);
	for(uint32_t i = 0; i < num_points; i++) {
		auto &vertex = line.points[i];
		WriteDouble(vertex.x, ptr);
		WriteDouble(vertex.y, ptr);
	}
}

void WKBWriter::Write(const Polygon &polygon, data_ptr_t &ptr) {
	WriteByte(1, ptr); // byte order
	WriteInt(static_cast<uint32_t>(WKBGeometryType::POLYGON), ptr); // geometry type

	auto num_rings = polygon.Count();
	WriteInt(num_rings, ptr);
	for(uint32_t i = 0; i < num_rings; i++) {
		auto &ring = polygon.rings[i];
		auto num_points = ring.Count();
		WriteInt(num_points, ptr);
		for(uint32_t j = 0; j < num_points; j++) {
			auto &vertex = ring.data[j];
			WriteDouble(vertex.x, ptr);
			WriteDouble(vertex.y, ptr);
		}
	}
}

void WKBWriter::Write(const MultiPoint &multi_point, data_ptr_t &ptr) {
	WriteByte(1, ptr); // byte order
	WriteInt(static_cast<uint32_t>(WKBGeometryType::MULTIPOINT), ptr); // geometry type

	auto num_points = multi_point.Count();
	WriteInt(num_points, ptr);
	for(uint32_t i = 0; i < num_points; i++) {
		auto &point = multi_point.points[i];
		Write(point, ptr);
	}
}

void WKBWriter::Write(const MultiLineString &multi_line, data_ptr_t &ptr) {
	WriteByte(1, ptr); // byte order
	WriteInt(static_cast<uint32_t>(WKBGeometryType::MULTILINESTRING), ptr); // geometry type

	auto num_lines = multi_line.Count();
	WriteInt(num_lines, ptr);
	for(uint32_t i = 0; i < num_lines; i++) {
		auto &line = multi_line.linestrings[i];
		Write(line, ptr);
	}
}

void WKBWriter::Write(const MultiPolygon &multi_polygon, data_ptr_t &ptr) {
	WriteByte(1, ptr); // byte order
	WriteInt(static_cast<uint32_t>(WKBGeometryType::MULTIPOLYGON), ptr); // geometry type

	auto num_polygons = multi_polygon.Count();
	WriteInt(num_polygons, ptr);
	for(uint32_t i = 0; i < num_polygons; i++) {
		auto &polygon = multi_polygon.polygons[i];
		Write(polygon, ptr);
	}
}

void WKBWriter::Write(const GeometryCollection &collection, data_ptr_t &ptr) {
	WriteByte(1, ptr); // byte order
	WriteInt(static_cast<uint32_t>(WKBGeometryType::GEOMETRYCOLLECTION), ptr); // geometry type

	auto num_geometries = collection.Count();
	WriteInt(num_geometries, ptr);
	for(uint32_t i = 0; i < num_geometries; i++) {
		auto &geom = collection.geometries[i];
		Write(geom, ptr);
	}
}

} // namespace core

} // namespace spatial



