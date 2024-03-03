#include "spatial/common.hpp"
#include "spatial/geos/geos_wrappers.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/cursor.hpp"
#include "spatial/core/geometry/geometry_processor.hpp"


namespace spatial {

namespace geos {

using namespace core;

//------------------------------------------------------------------------------
// Deserialize
//------------------------------------------------------------------------------
// TODO: Replace this with a GeometryProcessor instead of a recursive function

template<class T>
bool IsPointerAligned(const void *ptr) {
    auto uintptr = reinterpret_cast<uintptr_t>(ptr);
    return (uintptr % alignof(T)) == 0;
}

template<bool HAS_Z, bool HAS_M>
static GEOSGeometry *DeserializeGeometry(Cursor &reader, GEOSContextHandle_t ctx);

template<bool HAS_Z, bool HAS_M>
static GEOSCoordSeq_t *DeserializeCoordSeq(Cursor &reader, uint32_t count, GEOSContextHandle_t ctx) {
    auto dims = 2 + (HAS_Z ? 1 : 0) + (HAS_M ? 1 : 0);
    auto vertex_size = sizeof(double) * dims;
    if(HAS_Z) {
        // GEOS does a memcpy in this case, so we can pass the buffer directly even if it's not aligned
        auto data = reader.GetPtr();
        auto seq = GEOSCoordSeq_copyFromBuffer_r(ctx, reinterpret_cast<const double *>(data), count, HAS_Z, HAS_M);
        reader.Skip(count * vertex_size);
        D_ASSERT(seq != nullptr);
        return seq;
    }
    else {
        auto data_ptr = reader.GetPtr();
        auto vertex_data = reinterpret_cast<const double *>(data_ptr);
        unsafe_unique_array<double> aligned_buffer;
        if (!IsPointerAligned<double>(data_ptr)) {
            // If the pointer is not aligned we need to copy the data to an aligned buffer before passing it to GEOS
            aligned_buffer = make_unsafe_uniq_array<double>(count * dims);
            memcpy(aligned_buffer.get(), data_ptr, count * vertex_size);
            vertex_data = aligned_buffer.get();
        }

        auto seq = GEOSCoordSeq_copyFromBuffer_r(ctx, vertex_data, count, HAS_Z, HAS_M);
        reader.Skip(count * vertex_size);
        D_ASSERT(seq != nullptr);
        return seq;
    }
}

template<bool HAS_Z, bool HAS_M>
static GEOSGeometry *DeserializePoint(Cursor &reader, GEOSContextHandle_t ctx) {
	reader.Skip(4); // skip type
	auto count = reader.Read<uint32_t>();
	if (count == 0) {
		return GEOSGeom_createEmptyPoint_r(ctx);
	} else {
        auto seq = DeserializeCoordSeq<HAS_Z, HAS_M>(reader, count, ctx);
		return GEOSGeom_createPoint_r(ctx, seq);
	}
}


template<bool HAS_Z, bool HAS_M>
static GEOSGeometry *DeserializeLineString(Cursor &reader, GEOSContextHandle_t ctx) {
	reader.Skip(4); // skip type
	auto count = reader.Read<uint32_t>();
	if (count == 0) {
		return GEOSGeom_createEmptyLineString_r(ctx);
	} else {
        auto seq = DeserializeCoordSeq<HAS_Z, HAS_M>(reader, count, ctx);
		return GEOSGeom_createLineString_r(ctx, seq);
	}
}


template<bool HAS_Z, bool HAS_M>
static GEOSGeometry *DeserializePolygon(Cursor &reader, GEOSContextHandle_t ctx) {
	reader.Skip(4); // skip type
	auto num_rings = reader.Read<uint32_t>();
	if (num_rings == 0) {
		return GEOSGeom_createEmptyPolygon_r(ctx);
	} else {
		// TODO: This doesnt handle the offset properly
		auto rings = new GEOSGeometry *[num_rings];
        auto data_reader = reader;
        data_reader.Skip(sizeof(uint32_t) * num_rings + ((num_rings % 2) * sizeof(uint32_t)));
		for (uint32_t i = 0; i < num_rings; i++) {
			auto count = reader.Read<uint32_t>();
			auto seq = DeserializeCoordSeq<HAS_Z, HAS_M>(data_reader, count, ctx);
			rings[i] = GEOSGeom_createLinearRing_r(ctx, seq);
		}
        reader.SetPtr(data_reader.GetPtr());
		auto poly = GEOSGeom_createPolygon_r(ctx, rings[0], rings + 1, num_rings - 1);
		delete[] rings;
		return poly;
	}
}

template<bool HAS_Z, bool HAS_M>
static GEOSGeometry *DeserializeMultiPoint(Cursor &reader, GEOSContextHandle_t ctx) {
	reader.Skip(4); // skip type
	auto num_points = reader.Read<uint32_t>();
	if (num_points == 0) {
		return GEOSGeom_createEmptyCollection_r(ctx, GEOS_MULTIPOINT);
	} else {
		auto points = new GEOSGeometry *[num_points];
		for (uint32_t i = 0; i < num_points; i++) {
			points[i] = DeserializePoint<HAS_Z, HAS_M>(reader, ctx);
		}
		auto mp = GEOSGeom_createCollection_r(ctx, GEOS_MULTIPOINT, points, num_points);
		delete[] points;
		return mp;
	}
}


template<bool HAS_Z, bool HAS_M>
static GEOSGeometry *DeserializeMultiLineString(Cursor &reader, GEOSContextHandle_t ctx) {
	reader.Skip(4); // skip type
	auto num_lines = reader.Read<uint32_t>();
	if (num_lines == 0) {
		return GEOSGeom_createEmptyCollection_r(ctx, GEOS_MULTILINESTRING);
	} else {
		auto lines = new GEOSGeometry *[num_lines];
		for (uint32_t i = 0; i < num_lines; i++) {
			lines[i] = DeserializeLineString<HAS_Z, HAS_M>(reader, ctx);
		}
		auto mls = GEOSGeom_createCollection_r(ctx, GEOS_MULTILINESTRING, lines, num_lines);
		delete[] lines;
		return mls;
	}
}


template<bool HAS_Z, bool HAS_M>
static GEOSGeometry *DeserializeMultiPolygon(Cursor &reader, GEOSContextHandle_t ctx) {
	reader.Skip(4); // skip type
	auto num_polygons = reader.Read<uint32_t>();
	if (num_polygons == 0) {
		return GEOSGeom_createEmptyCollection_r(ctx, GEOS_MULTIPOLYGON);
	} else {
		auto polygons = new GEOSGeometry *[num_polygons];
		for (uint32_t i = 0; i < num_polygons; i++) {
			polygons[i] = DeserializePolygon<HAS_Z, HAS_M>(reader, ctx);
		}
		auto mp = GEOSGeom_createCollection_r(ctx, GEOS_MULTIPOLYGON, polygons, num_polygons);
		delete[] polygons;
		return mp;
	}
}


template<bool HAS_Z, bool HAS_M>
static GEOSGeometry *DeserializeGeometryCollection(Cursor &reader, GEOSContextHandle_t ctx) {
	reader.Skip(4); // skip type
	auto num_geoms = reader.Read<uint32_t>();
	if (num_geoms == 0) {
        // TODO: Set Z and M?
		return GEOSGeom_createEmptyCollection_r(ctx, GEOS_GEOMETRYCOLLECTION);
	} else {
		auto geoms = new GEOSGeometry *[num_geoms];
		for (uint32_t i = 0; i < num_geoms; i++) {
			geoms[i] = DeserializeGeometry<HAS_Z, HAS_M>(reader, ctx);
		}
		auto gc = GEOSGeom_createCollection_r(ctx, GEOS_GEOMETRYCOLLECTION, geoms, num_geoms);
		delete[] geoms;
		return gc;
	}
}


template<bool HAS_Z, bool HAS_M>
GEOSGeometry *DeserializeGeometry(Cursor &reader, GEOSContextHandle_t ctx) {
	auto type = reader.Peek<GeometryType>();
	switch (type) {
	case GeometryType::POINT: {
		return DeserializePoint<HAS_Z, HAS_M>(reader, ctx);
	}
	case GeometryType::LINESTRING: {
		return DeserializeLineString<HAS_Z, HAS_M>(reader, ctx);
	}
	case GeometryType::POLYGON: {
		return DeserializePolygon<HAS_Z, HAS_M>(reader, ctx);
	}
	case GeometryType::MULTIPOINT: {
		return DeserializeMultiPoint<HAS_Z, HAS_M>(reader, ctx);
	}
	case GeometryType::MULTILINESTRING: {
		return DeserializeMultiLineString<HAS_Z, HAS_M>(reader, ctx);
	}
	case GeometryType::MULTIPOLYGON: {
		return DeserializeMultiPolygon<HAS_Z, HAS_M>(reader, ctx);
	}
	case GeometryType::GEOMETRYCOLLECTION: {
		return DeserializeGeometryCollection<HAS_Z, HAS_M>(reader, ctx);
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

    auto has_z = properties.HasZ();
    auto has_m = properties.HasM();
    if (has_z && has_m) {
        return DeserializeGeometry<true, true>(reader, ctx);
    } else if (has_z) {
        return DeserializeGeometry<true, false>(reader, ctx);
    } else if (has_m) {
        return DeserializeGeometry<false, true>(reader, ctx);
    } else {
        return DeserializeGeometry<false, false>(reader, ctx);
    }
}

GeometryPtr GeosContextWrapper::Deserialize(const geometry_t &blob) {
	return GeometryPtr(DeserializeGEOSGeometry(blob, ctx));
}

//-------------------------------------------------------------------
// Serialize
//-------------------------------------------------------------------
static uint32_t GetSerializedSize(const GEOSGeometry *geom, const GEOSContextHandle_t ctx) {
	auto type = GEOSGeomTypeId_r(ctx, geom);
    bool has_z = GEOSHasZ_r(ctx, geom);
    bool has_m = GEOSHasM_r(ctx, geom);

    auto vertex_size = sizeof(double) * (2 + (has_z ? 1 : 0) + (has_m ? 1 : 0));

	switch (type) {
	case GEOS_POINT: {
		// 4 bytes for type,
		// 4 bytes for num points,
		// vertex_size bytes for data if not empty
		bool empty = GEOSisEmpty_r(ctx, geom);
		return 4 + 4 + (empty ? 0 : vertex_size);
	}
	case GEOS_LINESTRING: {
		// 4 bytes for type,
		// 4 bytes for num points,
		// vertex_size bytes per point
		auto seq = GEOSGeom_getCoordSeq_r(ctx, geom);
		uint32_t count;
		GEOSCoordSeq_getSize_r(ctx, seq, &count);
		return 4 + 4 + count * vertex_size;
	}
	case GEOS_POLYGON: {
		// 4 bytes for type,
		// 4 bytes for num rings
		//   4 bytes for num points in shell,
		//   vertex_size bytes per point in shell,
		// 4 bytes for num holes,
		//   4 bytes for num points in hole,
		// 	 vertex_size bytes per point in hole
		// 4 bytes padding if (shell + holes) % 2 == 1
		uint32_t size = 4 + 4;
		auto shell = GEOSGetExteriorRing_r(ctx, geom);
		auto seq = GEOSGeom_getCoordSeq_r(ctx, shell);
		uint32_t count;
		GEOSCoordSeq_getSize_r(ctx, seq, &count);
		size += 4 + (count * vertex_size);
		auto num_holes = GEOSGetNumInteriorRings_r(ctx, geom);
		for (uint32_t i = 0; i < num_holes; i++) {
			auto hole = GEOSGetInteriorRingN_r(ctx, geom, i);
			auto seq = GEOSGeom_getCoordSeq_r(ctx, hole);
			uint32_t count;
			GEOSCoordSeq_getSize_r(ctx, seq, &count);
			size += 4 + (count * vertex_size);
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


static void SerializeCoordSeq(Cursor &writer, const GEOSCoordSequence *seq, bool has_z, bool has_m, uint32_t count, const GEOSContextHandle_t ctx) {
    GEOSCoordSeq_copyToBuffer_r(ctx, seq, reinterpret_cast<double *>(writer.GetPtr()), has_z, has_m);
    auto vertex_size = sizeof(double) * (2 + (has_z ? 1 : 0) + (has_m ? 1 : 0));
    writer.Skip(count * vertex_size);
}

static void SerializePoint(Cursor &writer, const GEOSGeometry *geom, const GEOSContextHandle_t ctx) {
	writer.Write<uint32_t>((uint32_t)GeometryType::POINT);

	if (GEOSisEmpty_r(ctx, geom)) {
		writer.Write<uint32_t>(0);
		return;
	}
    auto has_z = GEOSHasZ_r(ctx, geom);
    auto has_m = GEOSHasM_r(ctx, geom);
    auto seq = GEOSGeom_getCoordSeq_r(ctx, geom);
    uint32_t count;
    GEOSCoordSeq_getSize_r(ctx, seq, &count);
    writer.Write<uint32_t>(count);
    SerializeCoordSeq(writer, seq, has_z, has_m, count, ctx);
}

static void SerializeLineString(Cursor &writer, const GEOSGeometry *geom, const GEOSContextHandle_t ctx) {
	writer.Write<uint32_t>((uint32_t)GeometryType::LINESTRING);
    if(GEOSisEmpty_r(ctx, geom)) {
        writer.Write<uint32_t>(0);
        return;
    }
    auto has_z = GEOSHasZ_r(ctx, geom);
    auto has_m = GEOSHasM_r(ctx, geom);
    auto seq = GEOSGeom_getCoordSeq_r(ctx, geom);
    uint32_t count;
    GEOSCoordSeq_getSize_r(ctx, seq, &count);
    writer.Write<uint32_t>(count);
    SerializeCoordSeq(writer, seq, has_z, has_m, count, ctx);
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
    bool has_z = GEOSHasZ_r(ctx, geom);
    bool has_m = GEOSHasM_r(ctx, geom);
	// Start with shell
    SerializeCoordSeq(writer, shell_seq, has_z, has_m, shell_count, ctx);

	// Then write each hole
	for (uint32_t i = 0; i < num_holes; i++) {
		auto ring = GEOSGetInteriorRingN_r(ctx, geom, i);
		auto ring_seq = GEOSGeom_getCoordSeq_r(ctx, ring);
		uint32_t ring_count;

        GEOSCoordSeq_getSize_r(ctx, ring_seq, &ring_count);
        SerializeCoordSeq(writer, ring_seq, has_z, has_m, ring_count, ctx);
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
    properties.SetZ(GEOSHasZ_r(ctx, geom));
    properties.SetM(GEOSHasM_r(ctx, geom));
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