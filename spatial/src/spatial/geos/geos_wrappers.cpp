#include "spatial/common.hpp"
#include "spatial/geos/geos_wrappers.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/util/cursor.hpp"
#include "spatial/core/util/math.hpp"
#include "spatial/core/geometry/geometry_processor.hpp"

namespace spatial {

namespace geos {

using namespace core;

//------------------------------------------------------------------------------
// Deserialize
//------------------------------------------------------------------------------

template <class T>
bool IsPointerAligned(const void *ptr) {
	auto uintptr = reinterpret_cast<uintptr_t>(ptr);
	return (uintptr % alignof(T)) == 0;
}

class GEOSDeserializer final : GeometryProcessor<GEOSGeometry *> {
private:
	GEOSContextHandle_t ctx;
	vector<double> aligned_buffer;

private:
	GEOSCoordSeq_t *HandleVertexData(const VertexData &vertices) {
		auto n_dims = 2 + (HasZ() ? 1 : 0) + (HasM() ? 1 : 0);
		auto vertex_size = sizeof(double) * n_dims;

		// We know that the data is interleaved :^)
		auto data = vertices.data[0];
		auto count = vertices.count;

		if (HasZ()) {
			// GEOS does a memcpy in this case, so we can pass the buffer directly even if it's not aligned
			return GEOSCoordSeq_copyFromBuffer_r(ctx, reinterpret_cast<const double *>(data), count, HasZ(), HasM());
		} else {
			auto data_ptr = data;
			auto vertex_data = reinterpret_cast<const double *>(data_ptr);
			if (!IsPointerAligned<double>(data_ptr)) {
				// If the pointer is not aligned we need to copy the data to an aligned buffer before passing it to GEOS
				aligned_buffer.clear();
				aligned_buffer.resize(count * n_dims);
				memcpy(aligned_buffer.data(), data_ptr, count * vertex_size);
				vertex_data = aligned_buffer.data();
			}

			return GEOSCoordSeq_copyFromBuffer_r(ctx, vertex_data, count, HasZ(), HasM());
		}
	}

	GEOSGeometry *ProcessPoint(const VertexData &data) override {
		if (data.IsEmpty()) {
			return GEOSGeom_createEmptyPoint_r(ctx);
		} else {
			auto seq = HandleVertexData(data);
			return GEOSGeom_createPoint_r(ctx, seq);
		}
	}

	GEOSGeometry *ProcessLineString(const VertexData &data) override {
		if (data.IsEmpty()) {
			return GEOSGeom_createEmptyLineString_r(ctx);
		} else {
			auto seq = HandleVertexData(data);
			return GEOSGeom_createLineString_r(ctx, seq);
		}
	}

	GEOSGeometry *ProcessPolygon(PolygonState &state) override {
		auto num_rings = state.RingCount();
		if (num_rings == 0) {
			return GEOSGeom_createEmptyPolygon_r(ctx);
		} else {
			// TODO: Make a vector here instead of using new
			auto geoms = new GEOSGeometry *[num_rings];
			for (uint32_t i = 0; i < num_rings; i++) {
				auto vertices = state.Next();
				auto seq = HandleVertexData(vertices);
				geoms[i] = GEOSGeom_createLinearRing_r(ctx, seq);
			}
			auto result = GEOSGeom_createPolygon_r(ctx, geoms[0], geoms + 1, num_rings - 1);
			delete[] geoms;
			return result;
		}
	}

	GEOSGeometry *ProcessCollection(CollectionState &state) override {
		GEOSGeomTypes collection_type = GEOS_GEOMETRYCOLLECTION;
		switch (CurrentType()) {
		case GeometryType::MULTIPOINT:
			collection_type = GEOS_MULTIPOINT;
			break;
		case GeometryType::MULTILINESTRING:
			collection_type = GEOS_MULTILINESTRING;
			break;
		case GeometryType::MULTIPOLYGON:
			collection_type = GEOS_MULTIPOLYGON;
			break;
		default:
			break;
		}
		auto item_count = state.ItemCount();
		if (item_count == 0) {
			return GEOSGeom_createEmptyCollection_r(ctx, collection_type);
		} else {
			auto geoms = new GEOSGeometry *[item_count];
			for (uint32_t i = 0; i < item_count; i++) {
				geoms[i] = state.Next();
			}
			auto result = GEOSGeom_createCollection_r(ctx, collection_type, geoms, item_count);
			delete[] geoms;
			return result;
		}
	}

public:
	explicit GEOSDeserializer(GEOSContextHandle_t ctx) : ctx(ctx) {
	}

	GeometryPtr Execute(const geometry_t &geom) {
		return GeometryPtr {Process(geom)};
	}
};

GEOSGeometry *DeserializeGEOSGeometry(const geometry_t &blob, GEOSContextHandle_t ctx) {
	GEOSDeserializer deserializer(ctx);
	return deserializer.Execute(blob).release();
}

GeometryPtr GeosContextWrapper::Deserialize(const geometry_t &blob) {
	GEOSDeserializer deserializer(ctx);
	return deserializer.Execute(blob);
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

static void SerializeCoordSeq(Cursor &writer, const GEOSCoordSequence *seq, bool has_z, bool has_m, uint32_t count,
                              const GEOSContextHandle_t ctx) {
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

template <bool HAS_Z, bool HAS_M>
static void GetExtendedExtent(const GEOSCoordSeq_t *seq, double *zmin, double *zmax, double *mmin, double *mmax,
                              GEOSContextHandle_t ctx) {
	uint32_t size;
	GEOSCoordSeq_getSize_r(ctx, seq, &size);
	if (size > 2) {
		if (HAS_Z && HAS_M) {
			double z, m;
			GEOSCoordSeq_getOrdinate_r(ctx, seq, 0, 2, &z);
			GEOSCoordSeq_getOrdinate_r(ctx, seq, 0, 3, &m);
			*zmin = std::min(z, *zmin);
			*zmax = std::max(z, *zmax);
			*mmin = std::min(m, *mmin);
			*mmax = std::max(m, *mmax);
		} else if (HAS_Z) {
			double z;
			GEOSCoordSeq_getOrdinate_r(ctx, seq, 0, 2, &z);
			*zmin = std::min(z, *zmin);
			*zmax = std::max(z, *zmax);
		} else if (HAS_M) {
			double m;
			GEOSCoordSeq_getOrdinate_r(ctx, seq, 0, 2, &m);
			*mmin = std::min(m, *mmin);
			*mmax = std::max(m, *mmax);
		}
	}
}

template <bool HAS_Z, bool HAS_M>
static void GetExtendedExtent(const GEOSGeometry *geom, double *zmin, double *zmax, double *mmin, double *mmax,
                              GEOSContextHandle_t ctx) {
	auto geos_type = GEOSGeomTypeId_r(ctx, geom);
	switch (geos_type) {
	case GEOS_POINT:
	case GEOS_LINESTRING: {
		auto seq = GEOSGeom_getCoordSeq_r(ctx, geom);
		GetExtendedExtent<HAS_Z, HAS_M>(seq, zmin, zmax, mmin, mmax, ctx);
		break;
	}
	case GEOS_POLYGON: {
		auto shell = GEOSGetExteriorRing_r(ctx, geom);
		auto seq = GEOSGeom_getCoordSeq_r(ctx, shell);
		GetExtendedExtent<HAS_Z, HAS_M>(seq, zmin, zmax, mmin, mmax, ctx);
		auto num_holes = GEOSGetNumInteriorRings_r(ctx, geom);
		for (auto i = 0; i < num_holes; i++) {
			auto hole = GEOSGetInteriorRingN_r(ctx, geom, i);
			auto rseq = GEOSGeom_getCoordSeq_r(ctx, hole);
			GetExtendedExtent<HAS_Z, HAS_M>(rseq, zmin, zmax, mmin, mmax, ctx);
		}
		break;
	}
	case GEOS_MULTIPOINT:
	case GEOS_MULTILINESTRING:
	case GEOS_MULTIPOLYGON:
	case GEOS_GEOMETRYCOLLECTION: {
		auto num_polygons = GEOSGetNumGeometries_r(ctx, geom);
		for (auto i = 0; i < num_polygons; i++) {
			auto polygon = GEOSGetGeometryN_r(ctx, geom, i);
			GetExtendedExtent<HAS_Z, HAS_M>(polygon, zmin, zmax, mmin, mmax, ctx);
		}
		break;
	}
	default:
		throw NotImplementedException(StringUtil::Format("GEOS Serialize: Geometry type %d not supported", geos_type));
	}
}

static void GetExtendedExtent(const GEOSGeometry *geom, double *zmin, double *zmax, double *mmin, double *mmax,
                              GEOSContextHandle_t ctx) {
	*zmin = std::numeric_limits<double>::max();
	*zmax = std::numeric_limits<double>::lowest();
	*mmin = std::numeric_limits<double>::max();
	*mmax = std::numeric_limits<double>::lowest();
	auto has_z = GEOSHasZ_r(ctx, geom);
	auto has_m = GEOSHasM_r(ctx, geom);
	if (has_z && has_m) {
		GetExtendedExtent<true, true>(geom, zmin, zmax, mmin, mmax, ctx);
	} else if (has_z) {
		GetExtendedExtent<true, false>(geom, zmin, zmax, mmin, mmax, ctx);
	} else if (has_m) {
		GetExtendedExtent<false, true>(geom, zmin, zmax, mmin, mmax, ctx);
	} else {
		GetExtendedExtent<false, false>(geom, zmin, zmax, mmin, mmax, ctx);
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
	bool has_z = GEOSHasZ_r(ctx, geom);
	bool has_m = GEOSHasM_r(ctx, geom);

	auto bbox_size = has_bbox ? (sizeof(float) * 2 * (2 + (has_z ? 1 : 0) + (has_m ? 1 : 0))) : 0;

	auto size = GetSerializedSize(geom, ctx);
	size += 4;                // Header
	size += sizeof(uint32_t); // Padding
	size += bbox_size;        // BBox

	auto blob = StringVector::EmptyString(result, size);
	Cursor writer(blob);

	uint16_t hash = 0;

	GeometryProperties properties;
	properties.SetBBox(has_bbox);
	properties.SetZ(GEOSHasZ_r(ctx, geom));
	properties.SetM(GEOSHasM_r(ctx, geom));
	writer.Write<GeometryType>(type);             // Type
	writer.Write<GeometryProperties>(properties); // Properties
	writer.Write<uint16_t>(hash);                 // Hash
	writer.Write<uint32_t>(0);                    // Padding

	// If the geom is not a point, write the bounding box
	if (has_bbox) {
		double minx, maxx, miny, maxy;
		GEOSGeom_getExtent_r(ctx, geom, &minx, &miny, &maxx, &maxy);
		writer.Write<float>(MathUtil::DoubleToFloatDown(minx));
		writer.Write<float>(MathUtil::DoubleToFloatDown(miny));
		writer.Write<float>(MathUtil::DoubleToFloatUp(maxx));
		writer.Write<float>(MathUtil::DoubleToFloatUp(maxy));

		// well, this sucks. GEOS doesnt have a native way to get the Z and M value extents.
		if (has_z || has_m) {
			double minz, maxz, minm, maxm;
			GetExtendedExtent(geom, &minz, &maxz, &minm, &maxm, ctx);
			if (has_z) {
				writer.Write<float>(MathUtil::DoubleToFloatDown(minz));
				writer.Write<float>(MathUtil::DoubleToFloatUp(maxz));
			}
			if (has_m) {
				writer.Write<float>(MathUtil::DoubleToFloatDown(minm));
				writer.Write<float>(MathUtil::DoubleToFloatUp(maxm));
			}
		}
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