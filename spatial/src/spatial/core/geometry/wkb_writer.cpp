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
	case GeometryType::POINT:
		return GetRequiredSize(geom.GetPoint());
	case GeometryType::LINESTRING:
		return GetRequiredSize(geom.GetLineString());
	case GeometryType::POLYGON:
		return GetRequiredSize(geom.GetPolygon());
	case GeometryType::MULTIPOINT:
		return GetRequiredSize(geom.GetMultiPoint());
	case GeometryType::MULTILINESTRING:
		return GetRequiredSize(geom.GetMultiLineString());
	case GeometryType::MULTIPOLYGON:
		return GetRequiredSize(geom.GetMultiPolygon());
	case GeometryType::GEOMETRYCOLLECTION:
		return GetRequiredSize(geom.GetGeometryCollection());
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
	for (auto &ring : poly.Rings()) {
		size += sizeof(uint32_t) + ring.Count() * sizeof(double) * 2;
	}
	return size;
}

uint32_t WKBWriter::GetRequiredSize(const MultiPoint &multi_point) {
	uint32_t size = sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
	for (auto &point : multi_point) {
		size += GetRequiredSize(point);
	}
	return size;
}

uint32_t WKBWriter::GetRequiredSize(const MultiLineString &multi_line) {
	uint32_t size = sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
	for (auto &line : multi_line) {
		size += GetRequiredSize(line);
	}
	return size;
}

uint32_t WKBWriter::GetRequiredSize(const MultiPolygon &multi_poly) {
	uint32_t size = sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
	for (auto &poly : multi_poly) {
		size += GetRequiredSize(poly);
	}
	return size;
}

uint32_t WKBWriter::GetRequiredSize(const GeometryCollection &collection) {
	uint32_t size = sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint32_t);
	for (auto &geom : collection) {
		size += GetRequiredSize(geom);
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
	case GeometryType::POINT:
		Write(geom.GetPoint(), ptr);
		break;
	case GeometryType::LINESTRING:
		Write(geom.GetLineString(), ptr);
		break;
	case GeometryType::POLYGON:
		Write(geom.GetPolygon(), ptr);
		break;
	case GeometryType::MULTIPOINT:
		Write(geom.GetMultiPoint(), ptr);
		break;
	case GeometryType::MULTILINESTRING:
		Write(geom.GetMultiLineString(), ptr);
		break;
	case GeometryType::MULTIPOLYGON:
		Write(geom.GetMultiPolygon(), ptr);
		break;
	case GeometryType::GEOMETRYCOLLECTION:
		Write(geom.GetGeometryCollection(), ptr);
		break;
	default:
		throw NotImplementedException("Geometry type not supported");
	}
}

void WKBWriter::Write(const Point &point, data_ptr_t &ptr) {
	WriteByte(1, ptr);                                            // byte order
	WriteInt(static_cast<uint32_t>(WKBGeometryType::POINT), ptr); // geometry type

	if (point.IsEmpty()) {
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
	WriteByte(1, ptr);                                                 // byte order
	WriteInt(static_cast<uint32_t>(WKBGeometryType::LINESTRING), ptr); // geometry type

	auto num_points = line.Count();
	WriteInt(num_points, ptr);
	for (auto &vertex : line.Vertices()) {
		WriteDouble(vertex.x, ptr);
		WriteDouble(vertex.y, ptr);
	}
}

void WKBWriter::Write(const Polygon &polygon, data_ptr_t &ptr) {
	WriteByte(1, ptr);                                              // byte order
	WriteInt(static_cast<uint32_t>(WKBGeometryType::POLYGON), ptr); // geometry type

	WriteInt(polygon.Count(), ptr);
	for (auto &ring : polygon.Rings()) {
		WriteInt(ring.Count(), ptr);
		for (auto &vertex : ring) {
			WriteDouble(vertex.x, ptr);
			WriteDouble(vertex.y, ptr);
		}
	}
}

void WKBWriter::Write(const MultiPoint &multi_point, data_ptr_t &ptr) {
	WriteByte(1, ptr);                                                 // byte order
	WriteInt(static_cast<uint32_t>(WKBGeometryType::MULTIPOINT), ptr); // geometry type
	WriteInt(multi_point.Count(), ptr);
	for (auto &point : multi_point) {
		Write(point, ptr);
	}
}

void WKBWriter::Write(const MultiLineString &multi_line, data_ptr_t &ptr) {
	WriteByte(1, ptr);                                                      // byte order
	WriteInt(static_cast<uint32_t>(WKBGeometryType::MULTILINESTRING), ptr); // geometry type
	WriteInt(multi_line.Count(), ptr);
	for (auto &line : multi_line) {
		Write(line, ptr);
	}
}

void WKBWriter::Write(const MultiPolygon &multi_polygon, data_ptr_t &ptr) {
	WriteByte(1, ptr);                                                   // byte order
	WriteInt(static_cast<uint32_t>(WKBGeometryType::MULTIPOLYGON), ptr); // geometry type
	WriteInt(multi_polygon.Count(), ptr);
	for (auto &polygon : multi_polygon) {
		Write(polygon, ptr);
	}
}

void WKBWriter::Write(const GeometryCollection &collection, data_ptr_t &ptr) {
	WriteByte(1, ptr);                                                         // byte order
	WriteInt(static_cast<uint32_t>(WKBGeometryType::GEOMETRYCOLLECTION), ptr); // geometry type
	WriteInt(collection.Count(), ptr);
	for (auto &geom : collection) {
		Write(geom, ptr);
	}
}

} // namespace core

} // namespace spatial
