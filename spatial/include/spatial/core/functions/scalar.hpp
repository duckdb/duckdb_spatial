#pragma once
#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"

namespace spatial {

namespace core {

struct CoreScalarFunctions {
public:
	static void Register(ClientContext &context) {
		RegisterStArea(context);
		RegisterStAsGeoJSON(context);
		RegisterStAsText(context);
		RegisterStAsWKB(context);
		RegisterStAsHEXWKB(context);
		RegisterStCentroid(context);
		RegisterStCollect(context);
		RegisterStCollectionExtract(context);
		RegisterStContains(context);
		RegisterStDimension(context);
		RegisterStDistance(context);
		RegisterStFlipCoordinates(context);
		RegisterStGeometryType(context);
		RegisterStGeomFromWKB(context);
		RegisterStIsEmpty(context);
		RegisterStLength(context);
		RegisterStMakeLine(context);
		RegisterStNPoints(context);
		RegisterStPerimeter(context);
		RegisterStPoint(context);
		RegisterStX(context);
		RegisterStY(context);
	}

private:
	// ST_Area
	static void RegisterStArea(ClientContext &context);

	// ST_AsGeoJSON
	static void RegisterStAsGeoJSON(ClientContext &context);

	// ST_AsText
	static void RegisterStAsText(ClientContext &context);

	// ST_AsHextWKB
	static void RegisterStAsHEXWKB(ClientContext &context);

	// ST_AsWKB
	static void RegisterStAsWKB(ClientContext &context);

	// ST_Centroid
	static void RegisterStCentroid(ClientContext &context);

	// ST_Collect
	static void RegisterStCollect(ClientContext &context);

	// ST_CollectionExtract
	static void RegisterStCollectionExtract(ClientContext &context);

	// ST_Contains
	static void RegisterStContains(ClientContext &context);

	// ST_Dimension
	static void RegisterStDimension(ClientContext &context);

	// ST_Distance
	static void RegisterStDistance(ClientContext &context);

	// ST_FlipCoordinates
	static void RegisterStFlipCoordinates(ClientContext &context);

	// ST_GeometryType
	static void RegisterStGeometryType(ClientContext &context);

	// ST_GeomFromWKB
	static void RegisterStGeomFromWKB(ClientContext &context);

	// ST_IsEmpty
	static void RegisterStIsEmpty(ClientContext &context);

	// ST_Length
	static void RegisterStLength(ClientContext &context);

	// ST_MakeLine
	static void RegisterStMakeLine(ClientContext &context);

	// ST_NPoints
	static void RegisterStNPoints(ClientContext &context);

	// ST_Perimeter
	static void RegisterStPerimeter(ClientContext &context);

	// ST_Point
	static void RegisterStPoint(ClientContext &context);

	// ST_X
	static void RegisterStX(ClientContext &context);

	// ST_Y
	static void RegisterStY(ClientContext &context);
};

} // namespace core

} // namespace spatial