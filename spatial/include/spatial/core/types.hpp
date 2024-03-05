#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct GeoTypes {
	static LogicalType POINT_2D();
	static LogicalType POINT_3D();
	static LogicalType POINT_4D();
	static LogicalType LINESTRING_2D();
	static LogicalType POLYGON_2D();
	static LogicalType BOX_2D();
	static LogicalType GEOMETRY();
	static LogicalType WKB_BLOB();

	static void Register(DatabaseInstance &db);

	static LogicalType CreateEnumType(const string &name, const vector<string> &members);
};

enum class Side { LEFT, RIGHT, ON };

struct PointXY {
	double x;
	double y;
	explicit PointXY(double x, double y) : x(x), y(y) {
	}

	// Approximate equality
	bool operator==(const PointXY &other) const {
		return std::abs(x - other.x) < 1e-6 && std::abs(y - other.y) < 1e-6;
	}
};

} // namespace core

} // namespace spatial