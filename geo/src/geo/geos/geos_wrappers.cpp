#include "geo/common.hpp"
#include "geo/geos/geos_wrappers.hpp"

namespace geo {

namespace geos {

// Accessors
double GeometryPtr::Area() const {
	return GEOSArea(ptr, nullptr);
}

double GeometryPtr::Length() const {
	return GEOSLength(ptr, nullptr);
}

double GeometryPtr::GetX() {
	double x = 0;
	return GEOSGeomGetX(ptr, &x) ? x : 0;
}

double GeometryPtr::GetY() {
	double y = 0;
	return GEOSGeomGetY(ptr, &y) ? y : 0;
}

bool GeometryPtr::IsEmpty() const {
	return GEOSisEmpty(ptr);
}

bool GeometryPtr::IsSimple() const {
	return GEOSisSimple(ptr);
}

bool GeometryPtr::IsValid() const {
	return GEOSisValid(ptr);
}

bool GeometryPtr::IsRing() const {
	return GEOSisRing(ptr);
}

bool GeometryPtr::IsClosed() const {
	return GEOSisClosed(ptr);
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

}

}