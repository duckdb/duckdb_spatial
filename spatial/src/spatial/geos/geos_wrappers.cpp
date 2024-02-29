#include "spatial/common.hpp"
#include "spatial/geos/geos_wrappers.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/cursor.hpp"

namespace spatial {

namespace geos {

using namespace core;

//------------------------------------------------------------------------------
// Deserialize
//------------------------------------------------------------------------------
// Note: We dont use GEOSCoordSeq_CopyFromBuffer here because we cant actually guarantee that the
// double* is aligned to 8 bytes when duckdb loads the blob from storage, and GEOS only performs a
// memcpy for 3d geometry. In the future this may change on our end though.
static GEOSGeometry *DeserializeGeometry(Cursor &reader, GEOSContextHandle_t ctx);
static GEOSGeometry *DeserializePoint(Cursor &reader, GEOSContextHandle_t ctx) {
	reader.Skip(4); // skip type
	auto count = reader.Read<uint32_t>();
	if (count == 0) {
		return GEOSGeom_createEmptyPoint_r(ctx);
	} else {
		auto seq = GEOSCoordSeq_create_r(ctx, count, 2);
		for (uint32_t i = 0; i < count; i++) {
			auto x = reader.Read<double>();
			auto y = reader.Read<double>();
			GEOSCoordSeq_setX_r(ctx, seq, i, x);
			GEOSCoordSeq_setY_r(ctx, seq, i, y);
		}
		return GEOSGeom_createPoint_r(ctx, seq);
	}
}

static GEOSGeometry *DeserializeLineString(Cursor &reader, GEOSContextHandle_t ctx) {
	reader.Skip(4); // skip type
	auto count = reader.Read<uint32_t>();
	if (count == 0) {
		return GEOSGeom_createEmptyLineString_r(ctx);
	} else {
		auto seq = GEOSCoordSeq_create_r(ctx, count, 2);
		for (uint32_t i = 0; i < count; i++) {
			auto x = reader.Read<double>();
			auto y = reader.Read<double>();
			GEOSCoordSeq_setX_r(ctx, seq, i, x);
			GEOSCoordSeq_setY_r(ctx, seq, i, y);
		}
		return GEOSGeom_createLineString_r(ctx, seq);
	}
}

static GEOSGeometry *DeserializePolygon(Cursor &reader, GEOSContextHandle_t ctx) {
	reader.Skip(4); // skip type
	auto num_rings = reader.Read<uint32_t>();
	if (num_rings == 0) {
		return GEOSGeom_createEmptyPolygon_r(ctx);
	} else {
		// TODO: This doesnt handle the offset properly
		auto rings = new GEOSGeometry *[num_rings];
		auto data_ptr = reader.GetPtr() + sizeof(uint32_t) * num_rings + ((num_rings % 2) * sizeof(uint32_t));
		for (uint32_t i = 0; i < num_rings; i++) {
			auto count = reader.Read<uint32_t>();
			auto seq = GEOSCoordSeq_create_r(ctx, count, 2);
			for (uint32_t j = 0; j < count; j++) {
				auto x = Load<double>(data_ptr);
				data_ptr += sizeof(double);
				auto y = Load<double>(data_ptr);
				data_ptr += sizeof(double);
				GEOSCoordSeq_setX_r(ctx, seq, j, x);
				GEOSCoordSeq_setY_r(ctx, seq, j, y);
			}
			rings[i] = GEOSGeom_createLinearRing_r(ctx, seq);
		}
		reader.SetPtr(data_ptr);
		auto poly = GEOSGeom_createPolygon_r(ctx, rings[0], rings + 1, num_rings - 1);
		delete[] rings;
		return poly;
	}
}

static GEOSGeometry *DeserializeMultiPoint(Cursor &reader, GEOSContextHandle_t ctx) {
	reader.Skip(4); // skip type
	auto num_points = reader.Read<uint32_t>();
	if (num_points == 0) {
		return GEOSGeom_createEmptyCollection_r(ctx, GEOS_MULTIPOINT);
	} else {
		auto points = new GEOSGeometry *[num_points];
		for (uint32_t i = 0; i < num_points; i++) {
			points[i] = DeserializePoint(reader, ctx);
		}
		auto mp = GEOSGeom_createCollection_r(ctx, GEOS_MULTIPOINT, points, num_points);
		delete[] points;
		return mp;
	}
}

static GEOSGeometry *DeserializeMultiLineString(Cursor &reader, GEOSContextHandle_t ctx) {
	reader.Skip(4); // skip type
	auto num_lines = reader.Read<uint32_t>();
	if (num_lines == 0) {
		return GEOSGeom_createEmptyCollection_r(ctx, GEOS_MULTILINESTRING);
	} else {
		auto lines = new GEOSGeometry *[num_lines];
		for (uint32_t i = 0; i < num_lines; i++) {
			lines[i] = DeserializeLineString(reader, ctx);
		}
		auto mls = GEOSGeom_createCollection_r(ctx, GEOS_MULTILINESTRING, lines, num_lines);
		delete[] lines;
		return mls;
	}
}

static GEOSGeometry *DeserializeMultiPolygon(Cursor &reader, GEOSContextHandle_t ctx) {
	reader.Skip(4); // skip type
	auto num_polygons = reader.Read<uint32_t>();
	if (num_polygons == 0) {
		return GEOSGeom_createEmptyCollection_r(ctx, GEOS_MULTIPOLYGON);
	} else {
		auto polygons = new GEOSGeometry *[num_polygons];
		for (uint32_t i = 0; i < num_polygons; i++) {
			polygons[i] = DeserializePolygon(reader, ctx);
		}
		auto mp = GEOSGeom_createCollection_r(ctx, GEOS_MULTIPOLYGON, polygons, num_polygons);
		delete[] polygons;
		return mp;
	}
}

static GEOSGeometry *DeserializeGeometryCollection(Cursor &reader, GEOSContextHandle_t ctx) {
	reader.Skip(4); // skip type
	auto num_geoms = reader.Read<uint32_t>();
	if (num_geoms == 0) {
		return GEOSGeom_createEmptyCollection_r(ctx, GEOS_GEOMETRYCOLLECTION);
	} else {
		auto geoms = new GEOSGeometry *[num_geoms];
		for (uint32_t i = 0; i < num_geoms; i++) {
			geoms[i] = DeserializeGeometry(reader, ctx);
		}
		auto gc = GEOSGeom_createCollection_r(ctx, GEOS_GEOMETRYCOLLECTION, geoms, num_geoms);
		delete[] geoms;
		return gc;
	}
}

GEOSGeometry *DeserializeGeometry(Cursor &reader, GEOSContextHandle_t ctx) {
	auto type = reader.Peek<GeometryType>();
	switch (type) {
	case GeometryType::POINT: {
		return DeserializePoint(reader, ctx);
	}
	case GeometryType::LINESTRING: {
		return DeserializeLineString(reader, ctx);
	}
	case GeometryType::POLYGON: {
		return DeserializePolygon(reader, ctx);
	}
	case GeometryType::MULTIPOINT: {
		return DeserializeMultiPoint(reader, ctx);
	}
	case GeometryType::MULTILINESTRING: {
		return DeserializeMultiLineString(reader, ctx);
	}
	case GeometryType::MULTIPOLYGON: {
		return DeserializeMultiPolygon(reader, ctx);
	}
	case GeometryType::GEOMETRYCOLLECTION: {
		return DeserializeGeometryCollection(reader, ctx);
	}
	default: {
		throw NotImplementedException(
		    StringUtil::Format("GEOS Deserialize: Geometry type %d not supported", static_cast<int>(type)));
	}
	}
}

GEOSGeometry *DeserializeGEOSGeometry(const geometry_t &blob, GEOSContextHandle_t ctx) {
	Cursor reader(blob);
    auto type = reader.Read<GeometryType>();
    (void)type;
    auto properties = reader.Read<GeometryProperties>();
    auto hash = reader.Read<uint16_t>();
    (void)hash;
	reader.Skip(4); // Skip padding
	if (properties.HasBBox()) {
		reader.Skip(16); // Skip bbox
	}

	return DeserializeGeometry(reader, ctx);
}

GeometryPtr GeosContextWrapper::Deserialize(const geometry_t &blob) {
	return GeometryPtr(DeserializeGEOSGeometry(blob, ctx));
}

//-------------------------------------------------------------------
// Serialize
//-------------------------------------------------------------------
static uint32_t GetSerializedSize(const GEOSGeometry *geom, const GEOSContextHandle_t ctx) {
	auto type = GEOSGeomTypeId_r(ctx, geom);
	switch (type) {
	case GEOS_POINT: {
		// 4 bytes for type,
		// 4 bytes for num points,
		// 16 bytes for data if not empty
		bool empty = GEOSisEmpty_r(ctx, geom);
		return 4 + 4 + (empty ? 0 : 16);
	}
	case GEOS_LINESTRING: {
		// 4 bytes for type,
		// 4 bytes for num points,
		// 16 bytes per point
		auto seq = GEOSGeom_getCoordSeq_r(ctx, geom);
		uint32_t count;
		GEOSCoordSeq_getSize_r(ctx, seq, &count);
		return 4 + 4 + count * 16;
	}
	case GEOS_POLYGON: {
		// 4 bytes for type,
		// 4 bytes for num rings
		//   4 bytes for num points in shell,
		//   16 bytes per point in shell,
		// 4 bytes for num holes,
		//   4 bytes for num points in hole,
		// 	 16 bytes per point in hole
		// 4 bytes padding if (shell + holes) % 2 == 1
		uint32_t size = 4 + 4;
		auto shell = GEOSGetExteriorRing_r(ctx, geom);
		auto seq = GEOSGeom_getCoordSeq_r(ctx, shell);
		uint32_t count;
		GEOSCoordSeq_getSize_r(ctx, seq, &count);
		size += 4 + count * 16;
		auto num_holes = GEOSGetNumInteriorRings_r(ctx, geom);
		for (uint32_t i = 0; i < num_holes; i++) {
			auto hole = GEOSGetInteriorRingN_r(ctx, geom, i);
			auto seq = GEOSGeom_getCoordSeq_r(ctx, hole);
			uint32_t count;
			GEOSCoordSeq_getSize_r(ctx, seq, &count);
			size += 4 + count * 16;
		}

		if ((num_holes + 1) % 2 == 1) {
			size += 4;
		}

		return size;
	}
	case GEOS_MULTIPOINT: {
		// 4 bytes for type,
		// 4 bytes for num points,
		// x bytes per point
		auto size = 4 + 4;
		auto num_points = GEOSGetNumGeometries_r(ctx, geom);
		for (uint32_t i = 0; i < num_points; i++) {
			auto point = GEOSGetGeometryN_r(ctx, geom, i);
			size += GetSerializedSize(point, ctx);
		}
		return size;
	}
	case GEOS_MULTILINESTRING: {
		// 4 bytes for type,
		// 4 bytes for num lines,
		// x bytes per line
		auto size = 4 + 4;
		auto num_lines = GEOSGetNumGeometries_r(ctx, geom);
		for (uint32_t i = 0; i < num_lines; i++) {
			auto line = GEOSGetGeometryN_r(ctx, geom, i);
			size += GetSerializedSize(line, ctx);
		}
		return size;
	}
	case GEOS_MULTIPOLYGON: {
		// 4 bytes for type,
		// 4 bytes for num polygons,
		// x bytes per polygon
		auto size = 4 + 4;
		auto num_polygons = GEOSGetNumGeometries_r(ctx, geom);
		for (uint32_t i = 0; i < num_polygons; i++) {
			auto polygon = GEOSGetGeometryN_r(ctx, geom, i);
			size += GetSerializedSize(polygon, ctx);
		}
		return size;
	}
	case GEOS_GEOMETRYCOLLECTION: {
		// 4 bytes for type,
		// 4 bytes for num geoms,
		// x bytes per geom
		auto size = 4 + 4;
		auto num_geoms = GEOSGetNumGeometries_r(ctx, geom);
		for (uint32_t i = 0; i < num_geoms; i++) {
			auto subgeom = GEOSGetGeometryN_r(ctx, geom, i);
			size += GetSerializedSize(subgeom, ctx);
		}
		return size;
	}
	default: {
		throw NotImplementedException(StringUtil::Format("GEOS SerializedSize: Geometry type %d not supported", type));
	}
	}
}
static void SerializeGeometry(Cursor &writer, const GEOSGeometry *geom, const GEOSContextHandle_t ctx);

static void SerializePoint(Cursor &writer, const GEOSGeometry *geom, const GEOSContextHandle_t ctx) {
	writer.Write<uint32_t>((uint32_t)GeometryType::POINT);

	if (GEOSisEmpty_r(ctx, geom)) {
		writer.Write<uint32_t>(0);
		return;
	}
	writer.Write<uint32_t>(1);
	auto seq = GEOSGeom_getCoordSeq_r(ctx, geom);
	double x, y;
	GEOSCoordSeq_getX_r(ctx, seq, 0, &x);
	GEOSCoordSeq_getY_r(ctx, seq, 0, &y);
	writer.Write<double>(x);
	writer.Write<double>(y);
}

static void SerializeLineString(Cursor &writer, const GEOSGeometry *geom, const GEOSContextHandle_t ctx) {
	writer.Write<uint32_t>((uint32_t)GeometryType::LINESTRING);
	auto seq = GEOSGeom_getCoordSeq_r(ctx, geom);
	uint32_t count;
	GEOSCoordSeq_getSize_r(ctx, seq, &count);
	writer.Write<uint32_t>(count);
	for (uint32_t i = 0; i < count; i++) {
		double x, y;
		GEOSCoordSeq_getX_r(ctx, seq, i, &x);
		GEOSCoordSeq_getY_r(ctx, seq, i, &y);
		writer.Write<double>(x);
		writer.Write<double>(y);
	}
}

static void SerializePolygon(Cursor &writer, const GEOSGeometry *geom, const GEOSContextHandle_t ctx) {
	// TODO: check this
	writer.Write<uint32_t>((uint32_t)GeometryType::POLYGON);

	// Write number of rings
	if (GEOSisEmpty_r(ctx, geom)) {
		writer.Write<uint32_t>(0);
		return;
	}

	uint32_t num_holes = GEOSGetNumInteriorRings_r(ctx, geom);
	writer.Write<uint32_t>(num_holes + 1); // +1 for the shell

	// Get shell
	auto shell = GEOSGetExteriorRing_r(ctx, geom);
	auto shell_seq = GEOSGeom_getCoordSeq_r(ctx, shell);

	// First pass, write all ring counts (including shell)
	// Start with shell
	uint32_t shell_count;
	GEOSCoordSeq_getSize_r(ctx, shell_seq, &shell_count);
	writer.Write<uint32_t>(shell_count);

	// Then write all holes
	for (uint32_t i = 0; i < num_holes; i++) {
		auto ring = GEOSGetInteriorRingN_r(ctx, geom, i);
		auto ring_seq = GEOSGeom_getCoordSeq_r(ctx, ring);
		uint32_t ring_count;
		GEOSCoordSeq_getSize_r(ctx, ring_seq, &ring_count);
		writer.Write<uint32_t>(ring_count);
	}

	// If rings are odd, add padding
	if ((num_holes + 1) % 2 == 1) {
		writer.Write<uint32_t>(0);
	}

	// Second pass, write data for each ring
	// Start with shell
	for (uint32_t i = 0; i < shell_count; i++) {
		double x, y;
		GEOSCoordSeq_getX_r(ctx, shell_seq, i, &x);
		GEOSCoordSeq_getY_r(ctx, shell_seq, i, &y);
		writer.Write<double>(x);
		writer.Write<double>(y);
	}

	// Then write each hole
	for (uint32_t i = 0; i < num_holes; i++) {
		auto ring = GEOSGetInteriorRingN_r(ctx, geom, i);
		auto ring_seq = GEOSGeom_getCoordSeq_r(ctx, ring);
		uint32_t ring_count;
		GEOSCoordSeq_getSize_r(ctx, ring_seq, &ring_count);
		for (uint32_t j = 0; j < ring_count; j++) {
			double x, y;
			GEOSCoordSeq_getX_r(ctx, ring_seq, j, &x);
			GEOSCoordSeq_getY_r(ctx, ring_seq, j, &y);
			writer.Write<double>(x);
			writer.Write<double>(y);
		}
	}
}

static void SerializeMultiPoint(Cursor &writer, const GEOSGeometry *geom, const GEOSContextHandle_t ctx) {
	writer.Write<uint32_t>((uint32_t)GeometryType::MULTIPOINT);
	uint32_t num_points = GEOSGetNumGeometries_r(ctx, geom);
	writer.Write<uint32_t>(num_points);
	for (uint32_t i = 0; i < num_points; i++) {
		auto point = GEOSGetGeometryN_r(ctx, geom, i);
		SerializePoint(writer, point, ctx);
	}
}

static void SerializeMultiLineString(Cursor &writer, const GEOSGeometry *geom, const GEOSContextHandle_t ctx) {
	writer.Write<uint32_t>((uint32_t)GeometryType::MULTILINESTRING);
	uint32_t num_linestrings = GEOSGetNumGeometries_r(ctx, geom);
	writer.Write<uint32_t>(num_linestrings);
	for (uint32_t i = 0; i < num_linestrings; i++) {
		auto linestring = GEOSGetGeometryN_r(ctx, geom, i);
		SerializeLineString(writer, linestring, ctx);
	}
}

static void SerializeMultiPolygon(Cursor &writer, const GEOSGeometry *geom, const GEOSContextHandle_t ctx) {
	writer.Write<uint32_t>((uint32_t)GeometryType::MULTIPOLYGON);
	uint32_t num_polygons = GEOSGetNumGeometries_r(ctx, geom);
	writer.Write<uint32_t>(num_polygons);
	for (uint32_t i = 0; i < num_polygons; i++) {
		auto polygon = GEOSGetGeometryN_r(ctx, geom, i);
		SerializePolygon(writer, polygon, ctx);
	}
}

static void SerializeGeometryCollection(Cursor &writer, const GEOSGeometry *geom, const GEOSContextHandle_t ctx) {
	writer.Write<uint32_t>((uint32_t)GeometryType::GEOMETRYCOLLECTION);
	uint32_t num_geometries = GEOSGetNumGeometries_r(ctx, geom);
	writer.Write<uint32_t>(num_geometries);
	for (uint32_t i = 0; i < num_geometries; i++) {
		auto geometry = GEOSGetGeometryN_r(ctx, geom, i);
		SerializeGeometry(writer, geometry, ctx);
	}
}

static void SerializeGeometry(Cursor &writer, const GEOSGeometry *geom, const GEOSContextHandle_t ctx) {
	auto type = GEOSGeomTypeId_r(ctx, geom);
	switch (type) {
	case GEOS_POINT:
		SerializePoint(writer, geom, ctx);
		break;
	case GEOS_LINESTRING:
		SerializeLineString(writer, geom, ctx);
		break;
	case GEOS_POLYGON:
		SerializePolygon(writer, geom, ctx);
		break;
	case GEOS_MULTIPOINT:
		SerializeMultiPoint(writer, geom, ctx);
		break;
	case GEOS_MULTILINESTRING:
		SerializeMultiLineString(writer, geom, ctx);
		break;
	case GEOS_MULTIPOLYGON:
		SerializeMultiPolygon(writer, geom, ctx);
		break;
	case GEOS_GEOMETRYCOLLECTION:
		SerializeGeometryCollection(writer, geom, ctx);
		break;
	default:
		throw NotImplementedException(StringUtil::Format("GEOS Serialize: Geometry type %d not supported", type));
	}
}

geometry_t SerializeGEOSGeometry(Vector &result, const GEOSGeometry *geom, GEOSContextHandle_t ctx) {

	GeometryType type;
	auto geos_type = GEOSGeomTypeId_r(ctx, geom);
	switch (geos_type) {
	case GEOS_POINT:
		type = GeometryType::POINT;
		break;
	case GEOS_LINESTRING:
		type = GeometryType::LINESTRING;
		break;
	case GEOS_POLYGON:
		type = GeometryType::POLYGON;
		break;
	case GEOS_MULTIPOINT:
		type = GeometryType::MULTIPOINT;
		break;
	case GEOS_MULTILINESTRING:
		type = GeometryType::MULTILINESTRING;
		break;
	case GEOS_MULTIPOLYGON:
		type = GeometryType::MULTIPOLYGON;
		break;
	case GEOS_GEOMETRYCOLLECTION:
		type = GeometryType::GEOMETRYCOLLECTION;
		break;
	default:
		throw NotImplementedException(
		    StringUtil::Format("GEOS Wrapper Serialize: Geometry type %d not supported", geos_type));
	}

	bool has_bbox = type != GeometryType::POINT && GEOSisEmpty_r(ctx, geom) == 0;

	auto size = GetSerializedSize(geom, ctx);
	size += 4; // Header
	size += sizeof(uint32_t);       // Padding
	size += has_bbox ? 16 : 0;      // BBox

	auto blob = StringVector::EmptyString(result, size);
	Cursor writer(blob);

	uint16_t hash = 0;
	for (uint32_t i = 0; i < sizeof(uint32_t); i++) {
		hash ^= (size >> (i * 8)) & 0xFF;
	}


    GeometryProperties properties;
	properties.SetBBox(has_bbox);
    writer.Write<GeometryType>(type);    // Type
    writer.Write<GeometryProperties>(properties); // Properties
    writer.Write<uint16_t>(hash);         // Hash
	writer.Write<uint32_t>(0);            // Padding

	// If the geom is not a point, write the bounding box
	if (has_bbox) {
		double minx, maxx, miny, maxy;
		GEOSGeom_getExtent_r(ctx, geom, &minx, &miny, &maxx, &maxy);
		writer.Write<float>(Utils::DoubleToFloatDown(minx));
		writer.Write<float>(Utils::DoubleToFloatDown(miny));
		writer.Write<float>(Utils::DoubleToFloatUp(maxx));
		writer.Write<float>(Utils::DoubleToFloatUp(maxy));
	}

	SerializeGeometry(writer, geom, ctx);

	blob.Finalize();

	return geometry_t(blob);
}

geometry_t GeosContextWrapper::Serialize(Vector &result, const GeometryPtr &geom) {
	return SerializeGEOSGeometry(result, geom.get(), ctx);
}

} // namespace geos

} // namespace spatial