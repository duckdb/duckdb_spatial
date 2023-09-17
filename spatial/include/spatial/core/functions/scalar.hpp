#pragma once
#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"

namespace spatial {

namespace core {

struct CoreScalarFunctions {
public:
	static void Register(DatabaseInstance &instance) {
		RegisterStArea(instance);
		RegisterStAsGeoJSON(instance);
		RegisterStAsText(instance);
		RegisterStAsWKB(instance);
		RegisterStAsHEXWKB(instance);
		RegisterStCentroid(instance);
		RegisterStCollect(instance);
		RegisterStCollectionExtract(instance);
		RegisterStContains(instance);
		RegisterStDimension(instance);
		RegisterStDistance(instance);
		RegisterStExtent(instance);
		RegisterStFlipCoordinates(instance);
		RegisterStGeometryType(instance);
		RegisterStGeomFromHEXWKB(instance);
		RegisterStGeomFromWKB(instance);
		RegisterStIsEmpty(instance);
		RegisterStLength(instance);
		RegisterStMakeLine(instance);
		RegisterStNPoints(instance);
		RegisterStPerimeter(instance);
		RegisterStPoint(instance);
		RegisterStRemoveRepeatedPoints(instance);
		RegisterStX(instance);
		RegisterStXMax(instance);
		RegisterStXMin(instance);
		RegisterStY(instance);
		RegisterStYMax(instance);
		RegisterStYMin(instance);
	}

private:
	// ST_Area
	static void RegisterStArea(DatabaseInstance &instance);

	// ST_AsGeoJSON
	static void RegisterStAsGeoJSON(DatabaseInstance &instance);

	// ST_AsText
	static void RegisterStAsText(DatabaseInstance &instance);

	// ST_AsHextWKB
	static void RegisterStAsHEXWKB(DatabaseInstance &instance);

	// ST_AsWKB
	static void RegisterStAsWKB(DatabaseInstance &instance);

	// ST_Centroid
	static void RegisterStCentroid(DatabaseInstance &instance);

	// ST_Collect
	static void RegisterStCollect(DatabaseInstance &instance);

	// ST_CollectionExtract
	static void RegisterStCollectionExtract(DatabaseInstance &instance);

	// ST_Contains
	static void RegisterStContains(DatabaseInstance &instance);

	// ST_Dimension
	static void RegisterStDimension(DatabaseInstance &instance);

	// ST_Distance
	static void RegisterStDistance(DatabaseInstance &instance);

	// ST_Extent
	static void RegisterStExtent(DatabaseInstance &instance);

	// ST_FlipCoordinates
	static void RegisterStFlipCoordinates(DatabaseInstance &instance);

	// ST_GeometryType
	static void RegisterStGeometryType(DatabaseInstance &instance);

	// ST_GeomFromHEXWKB
	static void RegisterStGeomFromHEXWKB(DatabaseInstance &instance);

	// ST_GeomFromWKB
	static void RegisterStGeomFromWKB(DatabaseInstance &instance);

	// ST_IsEmpty
	static void RegisterStIsEmpty(DatabaseInstance &instance);

	// ST_Length
	static void RegisterStLength(DatabaseInstance &instance);

	// ST_MakeLine
	static void RegisterStMakeLine(DatabaseInstance &instance);

	// ST_NPoints
	static void RegisterStNPoints(DatabaseInstance &instance);

	// ST_Perimeter
	static void RegisterStPerimeter(DatabaseInstance &instance);

	// ST_Point
	static void RegisterStPoint(DatabaseInstance &instance);

	// ST_RemoveRepeatedPoints
	static void RegisterStRemoveRepeatedPoints(DatabaseInstance &instance);

	// ST_X
	static void RegisterStX(DatabaseInstance &instance);

	// ST_XMax
	static void RegisterStXMax(DatabaseInstance &instance);

	// ST_XMin
	static void RegisterStXMin(DatabaseInstance &instance);

	// ST_Y
	static void RegisterStY(DatabaseInstance &instance);

	// ST_YMax
	static void RegisterStYMax(DatabaseInstance &instance);

	// ST_YMin
	static void RegisterStYMin(DatabaseInstance &instance);
};

} // namespace core

} // namespace spatial