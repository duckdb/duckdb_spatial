#include "spatial/common.hpp"
#include "spatial/core/geometry/vertex_vector.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/wkb_reader.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"

namespace spatial {

namespace core {




template <>
uint32_t WKBReader::ReadInt<WKBByteOrder::NDR>() {
	if (cursor + sizeof(uint32_t) > length) {
		throw SerializationException("WKBReader: ReadInt: not enough data");
	}
	// Read uint32_t in little endian
	auto result = Load<uint32_t>((const_data_ptr_t)data + cursor);
	cursor += sizeof(uint32_t);
	return result;
}

template <>
double WKBReader::ReadDouble<WKBByteOrder::NDR>() {
	if (cursor + sizeof(double) > length) {
		throw SerializationException("WKBReader: ReadDouble: not enough data");
	}
	// Read double in little endian
	auto result = Load<double>((const_data_ptr_t)data + cursor);
	cursor += sizeof(double);
	return result;
}

template <>
uint32_t WKBReader::ReadInt<WKBByteOrder::XDR>() {
	if (cursor + sizeof(uint32_t) > length) {
		throw SerializationException("WKBReader: ReadInt: not enough data");
	}
	// Read uint32_t in big endian
	uint32_t result = 0;
	result |= (uint32_t)data[cursor + 0] << 24 & 0xFF000000;
	result |= (uint32_t)data[cursor + 1] << 16 & 0x00FF0000;
	result |= (uint32_t)data[cursor + 2] << 8 & 0x0000FF00;
	result |= (uint32_t)data[cursor + 3] << 0 & 0x000000FF;
	cursor += sizeof(uint32_t);
	return result;
}

template <>
double WKBReader::ReadDouble<WKBByteOrder::XDR>() {
	if (cursor + sizeof(double) > length) {
		throw SerializationException("WKBReader: ReadDouble: not enough data");
	}
	// Read double in big endian
	uint64_t result = 0;
	result |= (uint64_t)data[cursor + 0] << 56 & 0xFF00000000000000;
	result |= (uint64_t)data[cursor + 1] << 48 & 0x00FF000000000000;
	result |= (uint64_t)data[cursor + 2] << 40 & 0x0000FF0000000000;
	result |= (uint64_t)data[cursor + 3] << 32 & 0x000000FF00000000;
	result |= (uint64_t)data[cursor + 4] << 24 & 0x00000000FF000000;
	result |= (uint64_t)data[cursor + 5] << 16 & 0x0000000000FF0000;
	result |= (uint64_t)data[cursor + 6] << 8 & 0x000000000000FF00;
	result |= (uint64_t)data[cursor + 7] << 0 & 0x00000000000000FF;
	cursor += sizeof(uint64_t);
	double d;
	memcpy(&d, &result, sizeof(double));
	return d;
}

template <WKBByteOrder ORDER>
WKBFlags WKBReader::ReadFlags() {
	auto type = ReadInt<ORDER>();
	bool has_z = (type & 0x80000000) == 0x80000000;
	bool has_m = (type & 0x40000000) == 0x40000000;
	bool has_srid = (type & 0x20000000) == 0x20000000;
	uint32_t srid = 0;

	if (has_z) {
		// Z present, throw
		throw NotImplementedException(
		    "Z value present in WKB, DuckDB spatial does not support geometries with Z coordinates yet");
	}

	if (has_m) {
		// M present, throw
		throw NotImplementedException(
		    "M value present in WKB, DuckDB spatial does not support geometries with M coordinates yet");
	}

	if (has_srid) {
		// SRID present
		srid = ReadInt<ORDER>();
		// Remove and ignore the srid flag for now
		type &= ~0x20000000;
	}

	return WKBFlags((WKBGeometryType)type, has_z, has_m, has_srid, srid);
}

Geometry WKBReader::ReadGeometry() {
	auto order = static_cast<WKBByteOrder>(data[cursor++]);
	if (order == WKBByteOrder::XDR) {
		return ReadGeometryBody<WKBByteOrder::XDR>();
	} else {
		return ReadGeometryBody<WKBByteOrder::NDR>();
	}
}

Point WKBReader::ReadPoint() {
	auto order = static_cast<WKBByteOrder>(data[cursor++]);
	if (order == WKBByteOrder::XDR) {
		return ReadPointBody<WKBByteOrder::XDR>();
	} else {
		return ReadPointBody<WKBByteOrder::NDR>();
	}
}

LineString WKBReader::ReadLineString() {
	auto order = static_cast<WKBByteOrder>(data[cursor++]);
	if (order == WKBByteOrder::XDR) {
		return ReadLineStringBody<WKBByteOrder::XDR>();
	} else {
		return ReadLineStringBody<WKBByteOrder::NDR>();
	}
}

Polygon WKBReader::ReadPolygon() {
	auto order = static_cast<WKBByteOrder>(data[cursor++]);
	if (order == WKBByteOrder::XDR) {
		return ReadPolygonBody<WKBByteOrder::XDR>();
	} else {
		return ReadPolygonBody<WKBByteOrder::NDR>();
	}
}

MultiPoint WKBReader::ReadMultiPoint() {
	auto order = static_cast<WKBByteOrder>(data[cursor++]);
	if (order == WKBByteOrder::XDR) {
		return ReadMultiPointBody<WKBByteOrder::XDR>();
	} else {
		return ReadMultiPointBody<WKBByteOrder::NDR>();
	}
}

MultiLineString WKBReader::ReadMultiLineString() {
	auto order = static_cast<WKBByteOrder>(data[cursor++]);
	if (order == WKBByteOrder::XDR) {
		return ReadMultiLineStringBody<WKBByteOrder::XDR>();
	} else {
		return ReadMultiLineStringBody<WKBByteOrder::NDR>();
	}
}

MultiPolygon WKBReader::ReadMultiPolygon() {
	auto order = static_cast<WKBByteOrder>(data[cursor++]);
	if (order == WKBByteOrder::XDR) {
		return ReadMultiPolygonBody<WKBByteOrder::XDR>();
	} else {
		return ReadMultiPolygonBody<WKBByteOrder::NDR>();
	}
}

GeometryCollection WKBReader::ReadGeometryCollection() {
	auto order = static_cast<WKBByteOrder>(data[cursor++]);
	if (order == WKBByteOrder::XDR) {
		return ReadGeometryCollectionBody<WKBByteOrder::XDR>();
	} else {
		return ReadGeometryCollectionBody<WKBByteOrder::NDR>();
	}
}

template <WKBByteOrder ORDER>
Geometry WKBReader::ReadGeometryBody() {
	// Only peek the flags, don't advance the cursor
	auto flags = ReadFlags<ORDER>();
	cursor -= sizeof(uint32_t);
	if (flags.has_srid) {
		cursor -= sizeof(uint32_t);
	}

	switch (flags.type) {
	case WKBGeometryType::POINT:
		return Geometry(ReadPointBody<ORDER>());
	case WKBGeometryType::LINESTRING:
		return Geometry(ReadLineStringBody<ORDER>());
	case WKBGeometryType::POLYGON:
		return Geometry(ReadPolygonBody<ORDER>());
	case WKBGeometryType::MULTIPOINT:
		return Geometry(ReadMultiPointBody<ORDER>());
	case WKBGeometryType::MULTILINESTRING:
		return Geometry(ReadMultiLineStringBody<ORDER>());
	case WKBGeometryType::MULTIPOLYGON:
		return Geometry(ReadMultiPolygonBody<ORDER>());
	case WKBGeometryType::GEOMETRYCOLLECTION:
		return Geometry(ReadGeometryCollectionBody<ORDER>());
	default:
		throw NotImplementedException("Geometry type '%u' not supported", flags.type);
	}
}

template <WKBByteOrder ORDER>
Point WKBReader::ReadPointBody() {
	auto flags = ReadFlags<ORDER>();
	if (flags.type != WKBGeometryType::POINT) {
		throw InvalidInputException("Expected POINT, got %u", flags.type);
	}
	auto x = ReadDouble<ORDER>();
	auto y = ReadDouble<ORDER>();
	if (std::isnan(x) && std::isnan(y)) {
		auto point_data = factory.AllocateVertexVector(0);
		return Point(point_data);
	}
	auto point_data = factory.AllocateVertexVector(1);
	point_data.Add(Vertex(x, y));
	return Point(point_data);
}

template <WKBByteOrder ORDER>
LineString WKBReader::ReadLineStringBody() {
	auto flags = ReadFlags<ORDER>();
	if (flags.type != WKBGeometryType::LINESTRING) {
		throw InvalidInputException("Expected LINESTRING, got %u", flags.type);
	}
	auto num_points = ReadInt<ORDER>();
	auto line_data = factory.AllocateVertexVector(num_points);
	for (uint32_t i = 0; i < num_points; i++) {
		auto x = ReadDouble<ORDER>();
		auto y = ReadDouble<ORDER>();
		line_data.Add(Vertex(x, y));
	}
	return LineString(line_data);
}

template <WKBByteOrder ORDER>
Polygon WKBReader::ReadPolygonBody() {
	auto flags = ReadFlags<ORDER>();
	if (flags.type != WKBGeometryType::POLYGON) {
		throw InvalidInputException("Expected POLYGON, got %u", flags.type);
	}
	auto num_rings = ReadInt<ORDER>();
	auto rings = reinterpret_cast<VertexVector *>(factory.allocator.Allocate(sizeof(VertexVector) * num_rings));

	for (uint32_t i = 0; i < num_rings; i++) {
		auto num_points = ReadInt<ORDER>();
		rings[i] = factory.AllocateVertexVector(num_points);
		auto &ring = rings[i];

		for (uint32_t j = 0; j < num_points; j++) {
			auto x = ReadDouble<ORDER>();
			auto y = ReadDouble<ORDER>();
			ring.Add(Vertex(x, y));
		}
	}
	return Polygon(rings, num_rings);
}

// TODO: Break after reading order instead. Peek type for GEOMETRY and GEOMETRYCOLLECTION

template <WKBByteOrder ORDER>
MultiPoint WKBReader::ReadMultiPointBody() {
	auto flags = ReadFlags<ORDER>();
	if (flags.type != WKBGeometryType::MULTIPOINT) {
		throw InvalidInputException("Expected MULTIPOINT, got %u", flags.type);
	}
	auto num_points = ReadInt<ORDER>();
	auto points = reinterpret_cast<Point *>(factory.allocator.Allocate(sizeof(Point) * num_points));
	for (uint32_t i = 0; i < num_points; i++) {
		points[i] = ReadPoint();
	}
	return MultiPoint(points, num_points);
}

template <WKBByteOrder ORDER>
MultiLineString WKBReader::ReadMultiLineStringBody() {
	auto flags = ReadFlags<ORDER>();
	if (flags.type != WKBGeometryType::MULTILINESTRING) {
		throw InvalidInputException("Expected MULTILINESTRING, got %u", flags.type);
	}
	auto num_lines = ReadInt<ORDER>();
	auto lines = reinterpret_cast<LineString *>(factory.allocator.Allocate(sizeof(LineString) * num_lines));
	for (uint32_t i = 0; i < num_lines; i++) {
		lines[i] = ReadLineString();
	}
	return MultiLineString(lines, num_lines);
}

template <WKBByteOrder ORDER>
MultiPolygon WKBReader::ReadMultiPolygonBody() {
	auto flags = ReadFlags<ORDER>();
	if (flags.type != WKBGeometryType::MULTIPOLYGON) {
		throw InvalidInputException("Expected MULTIPOLYGON, got %u", flags.type);
	}
	auto num_polygons = ReadInt<ORDER>();
	auto polygons = reinterpret_cast<Polygon *>(factory.allocator.Allocate(sizeof(Polygon) * num_polygons));
	for (uint32_t i = 0; i < num_polygons; i++) {
		polygons[i] = ReadPolygon();
	}
	return MultiPolygon(polygons, num_polygons);
}

template <WKBByteOrder ORDER>
GeometryCollection WKBReader::ReadGeometryCollectionBody() {
	auto flags = ReadFlags<ORDER>();
	if (flags.type != WKBGeometryType::GEOMETRYCOLLECTION) {
		throw InvalidInputException("Expected GEOMETRYCOLLECTION, got %u", flags.type);
	}
	auto num_geometries = ReadInt<ORDER>();
	auto geometries = reinterpret_cast<Geometry *>(factory.allocator.Allocate(sizeof(Geometry) * num_geometries));
	for (uint32_t i = 0; i < num_geometries; i++) {
		geometries[i] = ReadGeometry();
	}
	return GeometryCollection(geometries, num_geometries);
}

} // namespace core

} // namespace spatial
