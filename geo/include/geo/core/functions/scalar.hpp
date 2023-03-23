#pragma once
#include "geo/common.hpp"
#include "geo/core/geometry/geometry_factory.hpp"

namespace geo {

namespace core {

struct CoreScalarFunctions {
public:
	static void Register(ClientContext &context) {
		RegisterStArea(context);
		RegisterStAsText(context);
		RegisterStAsWKB(context);
		RegisterStContains(context);
		RegisterStDistance(context);
		RegisterStGeometryType(context);
		RegisterStGeomFromWKB(context);
		RegisterStIsEmpty(context);
		RegisterStLength(context);
		RegisterStPoint(context);
		RegisterStX(context);
		RegisterStY(context);
	}

private:
	// ST_Area
	static void RegisterStArea(ClientContext &context);

	// ST_AsText
	static void RegisterStAsText(ClientContext &context);

	// ST_AsWKB
	static void RegisterStAsWKB(ClientContext &context);

	// ST_Contains
	static void RegisterStContains(ClientContext &context);

	// ST_Distance
	static void RegisterStDistance(ClientContext &context);

	// ST_GeometryType
	static void RegisterStGeometryType(ClientContext &context);

	// ST_GeomFromWKB
	static void RegisterStGeomFromWKB(ClientContext &context);

	// ST_IsEmpty
	static void RegisterStIsEmpty(ClientContext &context);

	// ST_Length
	static void RegisterStLength(ClientContext &context);

	// ST_Point
	static void RegisterStPoint(ClientContext &context);

	// ST_X
	static void RegisterStX(ClientContext &context);

	// ST_Y
	static void RegisterStY(ClientContext &context);
};

} // namespace core

} // namespace geo