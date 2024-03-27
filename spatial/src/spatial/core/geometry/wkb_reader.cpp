#include "spatial/common.hpp"
#include "spatial/core/geometry/wkb_reader.hpp"
#include "spatial/core/geometry/geometry.hpp"

namespace spatial {

namespace core {

Geometry WKBReader::Deserialize(const string_t &wkb) {
	return Deserialize(const_data_ptr_cast(wkb.GetDataUnsafe()), wkb.GetSize());
}

Geometry WKBReader::Deserialize(const_data_ptr_t wkb, uint32_t size) {
	Cursor cursor(const_cast<data_ptr_t>(wkb), const_cast<data_ptr_t>(wkb + size));

	has_any_m = false;
	has_any_z = false;

	auto geom = ReadGeometry(cursor);

	// Make sure the geometry has unified vertex type, in case we got some funky nested WKB with mixed dimensions
	geom.SetVertexType(arena, has_any_z, has_any_m);

	return geom;
}

uint32_t WKBReader::ReadInt(Cursor &cursor, bool little_endian) {
	if (little_endian) {
		return cursor.Read<uint32_t>();
	} else {
		auto data = cursor.template Read<uint32_t>();
		// swap bytes
		return (data >> 24) | ((data >> 8) & 0xFF00) | ((data << 8) & 0xFF0000) | (data << 24);
	}
}

double WKBReader::ReadDouble(Cursor &cursor, bool little_endian) {
	if (little_endian) {
		return cursor.Read<double>();
	} else {
		auto data = cursor.template Read<uint64_t>();
		// swap bytes
		data = (data & 0x00000000FF000000) << 24 | (data & 0x000000FF00000000) << 8 | (data & 0x0000FF0000000000) >> 8 |
		       (data & 0xFF00000000000000) >> 24;
		double result;
		memcpy(&result, &data, sizeof(double));
		return result;
	}
}

WKBReader::WKBType WKBReader::ReadType(Cursor &cursor, bool little_endian) {
	auto wkb_type = ReadInt(cursor, little_endian);
	// Subtract 1 since the WKB type is 1-indexed
	auto geometry_type = static_cast<GeometryType>(((wkb_type & 0xffff) % 1000) - 1);
	bool has_z = false;
	bool has_m = false;
	bool has_srid = false;
	// Check for ISO WKB Z and M flags
	uint32_t iso_wkb_props = (wkb_type & 0xffff) / 1000;
	has_z = (iso_wkb_props == 1) || (iso_wkb_props == 3);
	has_m = (iso_wkb_props == 2) || (iso_wkb_props == 3);

	// Check for EWKB Z and M flags
	has_z = has_z | ((wkb_type & 0x80000000) != 0);
	has_m = has_m | ((wkb_type & 0x40000000) != 0);
	has_srid = (wkb_type & 0x20000000) != 0;

	if (has_srid) {
		// We don't support SRID yet, so just skip it if we encounter it
		cursor.Skip(sizeof(uint32_t));
	}

	has_any_z |= has_z;
	has_any_m |= has_m;

	return {geometry_type, has_z, has_m};
}

Point WKBReader::ReadPoint(Cursor &cursor, bool little_endian, bool has_z, bool has_m) {
	uint32_t dims = 2 + has_z + has_m;
	bool all_nan = true;
	double coords[4];
	for (uint32_t i = 0; i < dims; i++) {
		coords[i] = ReadDouble(cursor, little_endian);
		if (!std::isnan(coords[i])) {
			all_nan = false;
		}
	}
	if (all_nan) {
		return Point(has_z, has_m);
	} else {
        return Point::CopyFromData(arena, data_ptr_cast(coords), 1, has_z, has_m);
	}
}

LineString WKBReader::ReadLineString(Cursor &cursor, bool little_endian, bool has_z, bool has_m) {
	auto count = ReadInt(cursor, little_endian);
	auto ptr = cursor.GetPtr();
	auto vertices = LineString::CopyFromData(arena, ptr, count, has_z, has_m);
	// Move the cursor forwards
	cursor.Skip(vertices.ByteSize());
	return vertices;
}

Polygon WKBReader::ReadPolygon(Cursor &cursor, bool little_endian, bool has_z, bool has_m) {
	auto ring_count = ReadInt(cursor, little_endian);
	Polygon polygon(arena, ring_count, has_z, has_m);
	for (uint32_t i = 0; i < ring_count; i++) {
		auto point_count = ReadInt(cursor, little_endian);
		auto vertices = LineString::CopyFromData(arena, cursor.GetPtr(), point_count, has_z, has_m);
		cursor.Skip(vertices.ByteSize());
		polygon[i] = vertices;
	}
	return polygon;
}

MultiPoint WKBReader::ReadMultiPoint(Cursor &cursor, bool little_endian) {
	uint32_t count = ReadInt(cursor, little_endian);
	MultiPoint multi_point(arena, count, false, false);
	for (uint32_t i = 0; i < count; i++) {
		bool point_order = cursor.Read<uint8_t>();
		auto point_type = ReadType(cursor, point_order);
		multi_point[i] = ReadPoint(cursor, point_order, point_type.has_z, point_type.has_m);
	}
	return multi_point;
}

MultiLineString WKBReader::ReadMultiLineString(Cursor &cursor, bool little_endian) {
	uint32_t count = ReadInt(cursor, little_endian);
	MultiLineString multi_line_string(arena, count, false, false);
	for (uint32_t i = 0; i < count; i++) {
		bool line_order = cursor.Read<uint8_t>();
		auto line_type = ReadType(cursor, line_order);
		multi_line_string[i] = ReadLineString(cursor, line_order, line_type.has_z, line_type.has_m);
	}
	return multi_line_string;
}

MultiPolygon WKBReader::ReadMultiPolygon(Cursor &cursor, bool little_endian) {
	uint32_t count = ReadInt(cursor, little_endian);
	MultiPolygon multi_polygon(arena, count, false, false);
	for (uint32_t i = 0; i < count; i++) {
		bool polygon_order = cursor.Read<uint8_t>();
		auto polygon_type = ReadType(cursor, polygon_order);
		multi_polygon[i] = ReadPolygon(cursor, polygon_order, polygon_type.has_z, polygon_type.has_m);
	}
	return multi_polygon;
}

GeometryCollection WKBReader::ReadGeometryCollection(Cursor &cursor, bool byte_order) {
	uint32_t count = ReadInt(cursor, byte_order);
	GeometryCollection geometry_collection(arena, count, false, false);
	for (uint32_t i = 0; i < count; i++) {
		geometry_collection[i] = ReadGeometry(cursor);
	}
	return geometry_collection;
}

Geometry WKBReader::ReadGeometry(Cursor &cursor) {
	bool little_endian = cursor.Read<uint8_t>();
	auto type = ReadType(cursor, little_endian);
	switch (type.type) {
	case GeometryType::POINT:
		return ReadPoint(cursor, little_endian, type.has_z, type.has_m);
	case GeometryType::LINESTRING:
		return ReadLineString(cursor, little_endian, type.has_z, type.has_m);
	case GeometryType::POLYGON:
		return ReadPolygon(cursor, little_endian, type.has_z, type.has_m);
	case GeometryType::MULTIPOINT:
		return ReadMultiPoint(cursor, little_endian);
	case GeometryType::MULTILINESTRING:
		return ReadMultiLineString(cursor, little_endian);
	case GeometryType::MULTIPOLYGON:
		return ReadMultiPolygon(cursor, little_endian);
	case GeometryType::GEOMETRYCOLLECTION:
		return ReadGeometryCollection(cursor, little_endian);
	default:
		throw NotImplementedException("WKB Reader: Geometry type %u not supported", type.type);
	}
}

} // namespace core

} // namespace spatial
