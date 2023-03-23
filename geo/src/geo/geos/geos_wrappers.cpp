#include "geo/common.hpp"
#include "geo/geos/geos_wrappers.hpp"

namespace geo {

namespace geos {

// Accessors
double GeometryPtr::Area() const {
	double area;
	return GEOSArea_r(ctx, ptr, &area) ? area : 0;
}

double GeometryPtr::Length() const {
	double len = 0;
	return GEOSLength_r(ctx, ptr, &len) ? len : 0;
}

double GeometryPtr::GetX() {
	double x = 0;
	return GEOSGeomGetX_r(ctx, ptr, &x) ? x : 0;
}

double GeometryPtr::GetY() {
	double y = 0;
	return GEOSGeomGetY_r(ctx, ptr, &y) ? y : 0;
}

bool GeometryPtr::IsEmpty() const {
	return GEOSisEmpty_r(ctx, ptr);
}

bool GeometryPtr::IsSimple() const {
	return GEOSisSimple_r(ctx, ptr);
}

bool GeometryPtr::IsValid() const {
	return GEOSisValid_r(ctx, ptr);
}

bool GeometryPtr::IsRing() const {
	return GEOSisRing_r(ctx, ptr);
}

bool GeometryPtr::IsClosed() const {
	return GEOSisClosed_r(ctx, ptr);
}

// Constructive ops
GeometryPtr GeometryPtr::Simplify(double tolerance) const {
	auto result = GEOSSimplify_r(ctx, ptr, tolerance);
	if (!result) {
		throw Exception("Could not simplify geometry");
	}
	return GeometryPtr(ctx, result);
}

GeometryPtr GeometryPtr::SimplifyPreserveTopology(double tolerance) const {
	auto result = GEOSTopologyPreserveSimplify_r(ctx, ptr, tolerance);
	if (!result) {
		throw Exception("Could not simplify geometry");
	}
	return GeometryPtr(ctx, result);
}

GeometryPtr GeometryPtr::Buffer(double distance, int n_quadrant_segments) const {
	auto result = GEOSBuffer_r(ctx, ptr, distance, n_quadrant_segments);
	if (!result) {
		throw Exception("Could not buffer geometry");
	}
	return GeometryPtr(ctx, result);
}

GeometryPtr GeometryPtr::Boundary() const {
	auto result = GEOSBoundary_r(ctx, ptr);
	if (!result) {
		throw Exception("Could not compute boundary");
	}
	return GeometryPtr(ctx, result);
}

GeometryPtr GeometryPtr::ConvexHull() const {
	auto result = GEOSConvexHull_r(ctx, ptr);
	if (!result) {
		throw Exception("Could not compute convex hull");
	}
	return GeometryPtr(ctx, result);
}

GeometryPtr GeometryPtr::Centroid() const {
	auto result = GEOSGetCentroid_r(ctx, ptr);
	if (!result) {
		throw Exception("Could not compute centroid");
	}
	return GeometryPtr(ctx, result);
}

GeometryPtr GeometryPtr::Envelope() const {
	auto result = GEOSEnvelope_r(ctx, ptr);
	if (!result) {
		throw Exception("Could not compute envelope");
	}
	return GeometryPtr(ctx, result);
}

GeometryPtr GeometryPtr::Intersection(const GeometryPtr &other) const {
	auto result = GEOSIntersection_r(ctx, ptr, other.ptr);
	if (!result) {
		throw Exception("Could not compute intersection");
	}
	return GeometryPtr(ctx, result);
}

void GeometryPtr::Normalize() const {
	auto result = GEOSNormalize_r(ctx, ptr);
	if (result == -1) {
		throw Exception("Could not normalize geometry");
	}
}

// Predicates
bool GeometryPtr::Covers(const GeometryPtr &other) const {
	return GEOSCovers_r(ctx, ptr, other.ptr);
}

bool GeometryPtr::CoveredBy(const geo::geos::GeometryPtr &other) const {
	return GEOSCoveredBy_r(ctx, ptr, other.ptr);
}

bool GeometryPtr::Crosses(const GeometryPtr &other) const {
	return GEOSCrosses_r(ctx, ptr, other.ptr);
}

bool GeometryPtr::Disjoint(const GeometryPtr &other) const {
	return GEOSDisjoint_r(ctx, ptr, other.ptr);
}

bool GeometryPtr::Equals(const GeometryPtr &other) const {
	return GEOSEquals_r(ctx, ptr, other.ptr);
}

bool GeometryPtr::Intersects(const GeometryPtr &other) const {
	return GEOSIntersects_r(ctx, ptr, other.ptr);
}

bool GeometryPtr::Overlaps(const GeometryPtr &other) const {
	return GEOSOverlaps_r(ctx, ptr, other.ptr);
}

bool GeometryPtr::Touches(const GeometryPtr &other) const {
	return GEOSTouches_r(ctx, ptr, other.ptr);
}

bool GeometryPtr::Within(const GeometryPtr &other) const {
	return GEOSWithin_r(ctx, ptr, other.ptr);
}

bool GeometryPtr::Contains(const GeometryPtr &other) const {
	return GEOSContains_r(ctx, ptr, other.ptr);
}


//----------------------------------------------------------------------
// From Geometry
//----------------------------------------------------------------------
GEOSCoordSeq GeosContextWrapper::FromVertexVector(const core::VertexVector &vec) const {
	return GEOSCoordSeq_copyFromBuffer_r(ctx, (double*)vec.data, vec.count, false, false);
}

GeometryPtr GeosContextWrapper::FromPoint(const core::Point &point) const {
	auto seq = FromVertexVector(point.data);
	auto ptr = GEOSGeom_createPoint_r(ctx, seq);
	return GeometryPtr(ctx, ptr);
}

GeometryPtr GeosContextWrapper::FromLineString(const core::LineString &line) const {
	auto seq = FromVertexVector(line.points);
	auto ptr = GEOSGeom_createLineString_r(ctx, seq);
	return GeometryPtr(ctx, ptr);
}

GeometryPtr GeosContextWrapper::FromPolygon(const core::Polygon &poly) const {
	auto shell_ptr = GEOSGeom_createLinearRing_r(ctx, FromVertexVector(poly.rings[0]));
	auto poly_ptr = malloc(sizeof(GEOSGeometry*) * poly.num_rings);
	for (size_t i = 0; i < poly.num_rings; i++) {
		auto ring_ptr = GEOSGeom_createLinearRing_r(ctx, FromVertexVector(poly.rings[i]));
		((GEOSGeometry**)poly_ptr)[i] = ring_ptr;
	}
	auto ptr = GEOSGeom_createPolygon_r(ctx, shell_ptr, (GEOSGeometry**)poly_ptr, poly.num_rings);
	return GeometryPtr(ctx, ptr);
}

GeometryPtr GeosContextWrapper::FromMultiPoint(const core::MultiPoint &mpoint) const {
	auto ptr = malloc(sizeof(GEOSGeometry*) * mpoint.num_points);
	for (size_t i = 0; i < mpoint.num_points; i++) {
		auto point_ptr = FromPoint(mpoint.points[i]);
		((GEOSGeometry**)ptr)[i] = point_ptr.release();
	}
	auto result = GEOSGeom_createCollection_r(ctx, GEOS_MULTIPOINT, (GEOSGeometry**)ptr, mpoint.num_points);
	return GeometryPtr(ctx, result);
}

GeometryPtr GeosContextWrapper::FromMultiLineString(const core::MultiLineString &mline) const {
	auto ptr = malloc(sizeof(GEOSGeometry*) * mline.num_linestrings);
	for (size_t i = 0; i < mline.num_linestrings; i++) {
		auto line_ptr = FromLineString(mline.linestrings[i]);
		((GEOSGeometry**)ptr)[i] = line_ptr.release();
	}
	auto result = GEOSGeom_createCollection_r(ctx, GEOS_MULTILINESTRING, (GEOSGeometry**)ptr, mline.num_linestrings);
	return GeometryPtr(ctx, result);
}

GeometryPtr GeosContextWrapper::FromMultiPolygon(const core::MultiPolygon &mpoly) const {
	auto ptr = malloc(sizeof(GEOSGeometry*) * mpoly.num_polygons);
	for (size_t i = 0; i < mpoly.num_polygons; i++) {
		auto poly_ptr = FromPolygon(mpoly.polygons[i]);
		((GEOSGeometry**)ptr)[i] = poly_ptr.release();
	}
	auto result = GEOSGeom_createCollection_r(ctx, GEOS_MULTIPOLYGON, (GEOSGeometry**)ptr, mpoly.num_polygons);
	return GeometryPtr(ctx, result);
}

GeometryPtr GeosContextWrapper::FromGeometryCollection(const core::GeometryCollection &collection) const {
	auto ptr = malloc(sizeof(GEOSGeometry*) * collection.num_geometries);
	for (size_t i = 0; i < collection.num_geometries; i++) {
		auto geom_ptr = FromGeometry(collection.geometries[i]);
		((GEOSGeometry**)ptr)[i] = geom_ptr.release();
	}
	auto result = GEOSGeom_createCollection_r(ctx, GEOS_GEOMETRYCOLLECTION, (GEOSGeometry**)ptr, collection.num_geometries);
	return GeometryPtr(ctx, result);
}

GeometryPtr GeosContextWrapper::FromGeometry(core::Geometry &geom) const {
	switch(geom.Type()) {
	case core::GeometryType::POINT: {
		return FromPoint(geom.GetPoint());
	}
	case core::GeometryType::LINESTRING: {
		return FromLineString(geom.GetLineString());
	}
	case core::GeometryType::POLYGON: {
		return FromPolygon(geom.GetPolygon());
	}
	case core::GeometryType::MULTIPOINT: {
		return FromMultiPoint(geom.GetMultiPoint());
	}
	case core::GeometryType::MULTILINESTRING: {
		return FromMultiLineString(geom.GetMultiLineString());
	}
	case core::GeometryType::MULTIPOLYGON: {
		return FromMultiPolygon(geom.GetMultiPolygon());
	}
	case core::GeometryType::GEOMETRYCOLLECTION: {
		return FromGeometryCollection(geom.GetGeometryCollection());
	}
	default:
		throw InvalidInputException("Unsupported geometry type");
	}
}

//----------------------------------------------------------------------
// To Geometry
//----------------------------------------------------------------------
core::VertexVector GeosContextWrapper::ToVertexVector(const core::GeometryFactory &factory, const GEOSCoordSequence *seq) const {
	unsigned int size;
	GEOSCoordSeq_getSize_r(ctx, seq, &size);
	auto vec = factory.AllocateVertexVector(size);
	vec.count = size;
	auto ok = 1 == GEOSCoordSeq_copyToBuffer_r(ctx, seq, (double*)vec.data, false, false);
	if(!ok) {
		throw InvalidInputException("Failed to copy coordinates to buffer");
	}
	return vec;
}

core::Point GeosContextWrapper::ToPoint(const core::GeometryFactory &factory, const GEOSGeometry *geom) const {
	D_ASSERT(GEOSGeomTypeId_r(ctx, geom) == GEOS_POINT);
	auto seq = GEOSGeom_getCoordSeq_r(ctx, geom);
	auto vec = ToVertexVector(factory, seq);
	return core::Point(vec);
}

core::LineString GeosContextWrapper::ToLineString(const core::GeometryFactory &factory, const GEOSGeometry *geom) const {
	D_ASSERT(GEOSGeomTypeId_r(ctx, geom) == GEOS_LINESTRING);
	auto seq = GEOSGeom_getCoordSeq_r(ctx, geom);
	auto vec = ToVertexVector(factory, seq);
	return core::LineString(vec);
}

core::Polygon GeosContextWrapper::ToPolygon(const core::GeometryFactory &factory, const GEOSGeometry *geom) const {
	D_ASSERT(GEOSGeomTypeId_r(ctx, geom) == GEOS_POLYGON);
	auto shell_ptr = GEOSGetExteriorRing_r(ctx, geom);
	auto shell_seq = GEOSGeom_getCoordSeq_r(ctx, shell_ptr);
	auto shell_vec = ToVertexVector(factory, shell_seq);
	auto num_holes = GEOSGetNumInteriorRings_r(ctx, geom);
	auto poly = factory.CreatePolygon(num_holes + 1);

	poly.rings[0] = shell_vec;

	for (int i = 1; i < num_holes; i++) {
		auto hole_ptr = GEOSGetInteriorRingN_r(ctx, geom, i);
		auto hole_seq = GEOSGeom_getCoordSeq_r(ctx, hole_ptr);
		auto hole_vec = ToVertexVector(factory, hole_seq);
		poly.rings[i] = hole_vec;
	}

	return poly;
}

core::MultiPoint GeosContextWrapper::ToMultiPoint(const core::GeometryFactory &factory, const GEOSGeometry *geom) const {
	D_ASSERT(GEOSGeomTypeId_r(ctx, geom) == GEOS_MULTIPOINT);
	auto num_points = GEOSGetNumGeometries_r(ctx, geom);
	auto mpoint = factory.CreateMultiPoint(num_points);
	for (int i = 0; i < num_points; i++) {
		auto point_ptr = GEOSGetGeometryN_r(ctx, geom, i);
		auto point = ToPoint(factory, point_ptr);
		mpoint.points[i] = point;
	}
	return mpoint;
}

core::MultiLineString GeosContextWrapper::ToMultiLineString(const core::GeometryFactory &factory, const GEOSGeometry *geom) const {
    D_ASSERT(GEOSGeomTypeId_r(ctx, geom) == GEOS_MULTILINESTRING);
	auto num_lines = GEOSGetNumGeometries_r(ctx, geom);
	auto mline = factory.CreateMultiLineString(num_lines);
	for (int i = 0; i < num_lines; i++) {
		auto line_ptr = GEOSGetGeometryN_r(ctx, geom, i);
		auto line = ToLineString(factory, line_ptr);
		mline.linestrings[i] = line;
	}
	return mline;
}

core::MultiPolygon GeosContextWrapper::ToMultiPolygon(const core::GeometryFactory &factory, const GEOSGeometry *geom) const {
	D_ASSERT(GEOSGeomTypeId_r(ctx, geom) == GEOS_MULTIPOLYGON);
	auto num_polys = GEOSGetNumGeometries_r(ctx, geom);
	auto mpoly = factory.CreateMultiPolygon(num_polys);
	for (int i = 0; i < num_polys; i++) {
		auto poly_ptr = GEOSGetGeometryN_r(ctx, geom, i);
		auto poly = ToPolygon(factory, poly_ptr);
		mpoly.polygons[i] = poly;
	}
	return mpoly;
}

core::GeometryCollection GeosContextWrapper::ToGeometryCollection(const core::GeometryFactory &factory, const GEOSGeometry *geom) const {
	D_ASSERT(GEOSGeomTypeId_r(ctx, geom) == GEOS_GEOMETRYCOLLECTION);
	auto num_geoms = GEOSGetNumGeometries_r(ctx, geom);
	auto collection = factory.CreateGeometryCollection(num_geoms);
	for (int i = 0; i < num_geoms; i++) {
		auto geom_ptr = GEOSGetGeometryN_r(ctx, geom, i);
		auto item = ToGeometry(factory, geom_ptr);
		collection.geometries[i] = item;
	}
	return collection;
}

core::Geometry GeosContextWrapper::ToGeometry(const core::GeometryFactory &factory, const GEOSGeometry *geom) const {
	switch (GEOSGeomTypeId_r(ctx, geom)) {
	case GEOS_POINT: {
		return core::Geometry(ToPoint(factory, geom));
	}
	case GEOS_LINESTRING: {
		return core::Geometry(ToLineString(factory, geom));
	}
	case GEOS_POLYGON: {
		return core::Geometry(ToPolygon(factory, geom));
	}
	case GEOS_MULTIPOINT: {
		return core::Geometry(ToMultiPoint(factory, geom));
	}
	case GEOS_MULTILINESTRING: {
		return core::Geometry(ToMultiLineString(factory, geom));
	}
	case GEOS_MULTIPOLYGON: {
		return core::Geometry(ToMultiPolygon(factory, geom));
	}
	case GEOS_GEOMETRYCOLLECTION: {
		return core::Geometry(ToGeometryCollection(factory, geom));
	}
	default:
		throw InvalidInputException(StringUtil::Format("Unsupported geometry type %d", GEOSGeomTypeId_r(ctx, geom)));
	}
}

} // namespace geos

} // namespace geo