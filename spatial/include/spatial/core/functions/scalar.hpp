#pragma once
#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"

namespace spatial {

namespace core {

struct CoreScalarFunctions {
public:
	static void Register(ClientContext &context) {
		RegisterStArea(context);
		RegisterStAsText(context);
		RegisterStAsWKB(context);
		RegisterStCentroid(context);
		RegisterStCollect(context);
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

	// ST_Centroid
	static void RegisterStCentroid(ClientContext &context);

	// ST_Collect
	static void RegisterStCollect(ClientContext &context);

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

} // namespace spatial