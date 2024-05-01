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
		return cursor.ReadBigEndian<uint32_t>();
	}
}

double WKBReader::ReadDouble(Cursor &cursor, bool little_endian) {
	if (little_endian) {
		return cursor.Read<double>();
	} else {
		return cursor.ReadBigEndian<double>();
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

Geometry WKBReader::ReadPoint(Cursor &cursor, bool little_endian, bool has_z, bool has_m) {
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
		return Point::CreateEmpty(has_z, has_m);
	} else {
		return Point::CreateFromCopy(arena, data_ptr_cast(coords), 1, has_z, has_m);
	}
}

void WKBReader::ReadVertices(Cursor &cursor, bool little_endian, bool has_z, bool has_m, Geometry &geometry) {
	for (uint32_t i = 0; i < geometry.Count(); i++) {
		if (has_z && has_m) {
			auto x = ReadDouble(cursor, little_endian);
			auto y = ReadDouble(cursor, little_endian);
			auto z = ReadDouble(cursor, little_endian);
			auto m = ReadDouble(cursor, little_endian);
			SinglePartGeometry::SetVertex(geometry, i, VertexXYZM {x, y, z, m});
		} else if (has_z) {
			auto x = ReadDouble(cursor, little_endian);
			auto y = ReadDouble(cursor, little_endian);
			auto z = ReadDouble(cursor, little_endian);
			SinglePartGeometry::SetVertex(geometry, i, VertexXYZ {x, y, z});
		} else if (has_m) {
			auto x = ReadDouble(cursor, little_endian);
			auto y = ReadDouble(cursor, little_endian);
			auto m = ReadDouble(cursor, little_endian);
			SinglePartGeometry::SetVertex(geometry, i, VertexXYM {x, y, m});
		} else {
			auto x = ReadDouble(cursor, little_endian);
			auto y = ReadDouble(cursor, little_endian);
			SinglePartGeometry::SetVertex(geometry, i, VertexXY {x, y});
		}
	}
}

Geometry WKBReader::ReadLineString(Cursor &cursor, bool little_endian, bool has_z, bool has_m) {
	auto count = ReadInt(cursor, little_endian);
	auto vertices = LineString::Create(arena, count, has_z, has_m);
	ReadVertices(cursor, little_endian, has_z, has_m, vertices);
	return vertices;
}

Geometry WKBReader::ReadPolygon(Cursor &cursor, bool little_endian, bool has_z, bool has_m) {
	auto ring_count = ReadInt(cursor, little_endian);
	auto polygon = Polygon::Create(arena, ring_count, has_z, has_m);
	for (uint32_t i = 0; i < ring_count; i++) {
		auto point_count = ReadInt(cursor, little_endian);
		Polygon::Part(polygon, i) = LineString::Create(arena, point_count, has_z, has_m);
		ReadVertices(cursor, little_endian, has_z, has_m, Polygon::Part(polygon, i));
	}
	return polygon;
}

Geometry WKBReader::ReadMultiPoint(Cursor &cursor, bool little_endian, bool has_z, bool has_m) {
	uint32_t count = ReadInt(cursor, little_endian);
	auto multi_point = MultiPoint::Create(arena, count, has_z, has_m);
	for (uint32_t i = 0; i < count; i++) {
		bool point_order = cursor.Read<uint8_t>();
		auto point_type = ReadType(cursor, point_order);
		MultiPoint::Part(multi_point, i) = ReadPoint(cursor, point_order, point_type.has_z, point_type.has_m);
	}
	return multi_point;
}

Geometry WKBReader::ReadMultiLineString(Cursor &cursor, bool little_endian, bool has_z, bool has_m) {
	uint32_t count = ReadInt(cursor, little_endian);
	auto multi_line_string = MultiLineString::Create(arena, count, has_z, has_m);
	for (uint32_t i = 0; i < count; i++) {
		bool line_order = cursor.Read<uint8_t>();
		auto line_type = ReadType(cursor, line_order);
		MultiLineString::Part(multi_line_string, i) =
		    ReadLineString(cursor, line_order, line_type.has_z, line_type.has_m);
	}
	return multi_line_string;
}

Geometry WKBReader::ReadMultiPolygon(Cursor &cursor, bool little_endian, bool has_z, bool has_m) {
	uint32_t count = ReadInt(cursor, little_endian);
	auto multi_polygon = MultiPolygon::Create(arena, count, has_z, has_m);
	for (uint32_t i = 0; i < count; i++) {
		bool polygon_order = cursor.Read<uint8_t>();
		auto polygon_type = ReadType(cursor, polygon_order);
		MultiPolygon::Part(multi_polygon, i) =
		    ReadPolygon(cursor, polygon_order, polygon_type.has_z, polygon_type.has_m);
	}
	return multi_polygon;
}

Geometry WKBReader::ReadGeometryCollection(Cursor &cursor, bool little_endian, bool has_z, bool has_m) {
	uint32_t count = ReadInt(cursor, little_endian);
	auto geometry_collection = GeometryCollection::Create(arena, count, has_z, has_m);
	for (uint32_t i = 0; i < count; i++) {
		GeometryCollection::Part(geometry_collection, i) = ReadGeometry(cursor);
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
		return ReadMultiPoint(cursor, little_endian, type.has_z, type.has_m);
	case GeometryType::MULTILINESTRING:
		return ReadMultiLineString(cursor, little_endian, type.has_z, type.has_m);
	case GeometryType::MULTIPOLYGON:
		return ReadMultiPolygon(cursor, little_endian, type.has_z, type.has_m);
	case GeometryType::GEOMETRYCOLLECTION:
		return ReadGeometryCollection(cursor, little_endian, type.has_z, type.has_m);
	default:
		throw NotImplementedException("WKB Reader: Geometry type %u not supported", type.type);
	}
}

} // namespace core

} // namespace spatial
