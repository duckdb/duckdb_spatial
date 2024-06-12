#pragma once

#include "spatial/common.hpp"
#include "spatial/core/geometry/vertex.hpp"

namespace spatial {

namespace core {

struct BoundingBox {
	double minx = std::numeric_limits<double>::max();
	double miny = std::numeric_limits<double>::max();
	double maxx = std::numeric_limits<double>::lowest();
	double maxy = std::numeric_limits<double>::lowest();
	double minz = std::numeric_limits<double>::max();
	double maxz = std::numeric_limits<double>::lowest();
	double minm = std::numeric_limits<double>::max();
	double maxm = std::numeric_limits<double>::lowest();

	bool Intersects(const BoundingBox &other) const {
		return !(minx > other.maxx || maxx < other.minx || miny > other.maxy || maxy < other.miny);
	}

	// Update the bounding box to include the specified vertex
	void Stretch(const double x, const double y) {
		minx = std::min(minx, x);
		miny = std::min(miny, y);
		maxx = std::max(maxx, x);
		maxy = std::max(maxy, y);
	}

	void Stretch(const VertexXY &vertex) {
		minx = std::min(minx, vertex.x);
		miny = std::min(miny, vertex.y);
		maxx = std::max(maxx, vertex.x);
		maxy = std::max(maxy, vertex.y);
	}

	void Stretch(const VertexXYZ &vertex) {
		minx = std::min(minx, vertex.x);
		miny = std::min(miny, vertex.y);
		maxx = std::max(maxx, vertex.x);
		maxy = std::max(maxy, vertex.y);
		minz = std::min(minz, vertex.z);
		maxz = std::max(maxz, vertex.z);
	}

	void Stretch(const VertexXYM &vertex) {
		minx = std::min(minx, vertex.x);
		miny = std::min(miny, vertex.y);
		maxx = std::max(maxx, vertex.x);
		maxy = std::max(maxy, vertex.y);
		minm = std::min(minm, vertex.m);
		maxm = std::max(maxm, vertex.m);
	}

	void Stretch(const VertexXYZM &vertex) {
		minx = std::min(minx, vertex.x);
		miny = std::min(miny, vertex.y);
		maxx = std::max(maxx, vertex.x);
		maxy = std::max(maxy, vertex.y);
		minz = std::min(minz, vertex.z);
		maxz = std::max(maxz, vertex.z);
		minm = std::min(minm, vertex.m);
		maxm = std::max(maxm, vertex.m);
	}
};

} // namespace core

} // namespace spatial