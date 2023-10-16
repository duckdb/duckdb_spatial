#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct CoreScalarFunctions {
public:
	static void Register(DatabaseInstance &db) {
		RegisterStArea(db);
		RegisterStAsGeoJSON(db);
		RegisterStAsText(db);
		RegisterStAsWKB(db);
		RegisterStAsHEXWKB(db);
		RegisterStCentroid(db);
		RegisterStCollect(db);
		RegisterStCollectionExtract(db);
		RegisterStContains(db);
		RegisterStDimension(db);
		RegisterStDistance(db);
		RegisterStEndPoint(db);
		RegisterStExtent(db);
		RegisterStExteriorRing(db);
		RegisterStFlipCoordinates(db);
		RegisterStGeometryType(db);
		RegisterStGeomFromHEXWKB(db);
		RegisterStGeomFromWKB(db);
		RegisterStIntersects(db);
		RegisterStIntersectsExtent(db);
		RegisterStIsEmpty(db);
		RegisterStLength(db);
		RegisterStMakeEnvelope(db);
		RegisterStMakeLine(db);
		RegisterStMakePolygon(db);
		RegisterStNGeometries(db);
		RegisterStNInteriorRings(db);
		RegisterStNPoints(db);
		RegisterStPerimeter(db);
		RegisterStPoint(db);
		RegisterStPointN(db);
		RegisterStRemoveRepeatedPoints(db);
		RegisterStStartPoint(db);
		RegisterStX(db);
		RegisterStXMax(db);
		RegisterStXMin(db);
		RegisterStY(db);
		RegisterStYMax(db);
		RegisterStYMin(db);
	}

private:
	// ST_Area
	static void RegisterStArea(DatabaseInstance &db);

	// ST_AsGeoJSON
	static void RegisterStAsGeoJSON(DatabaseInstance &db);

	// ST_AsText
	static void RegisterStAsText(DatabaseInstance &db);

	// ST_AsHextWKB
	static void RegisterStAsHEXWKB(DatabaseInstance &db);

	// ST_AsWKB
	static void RegisterStAsWKB(DatabaseInstance &db);

	// ST_Centroid
	static void RegisterStCentroid(DatabaseInstance &db);

	// ST_Collect
	static void RegisterStCollect(DatabaseInstance &db);

	// ST_CollectionExtract
	static void RegisterStCollectionExtract(DatabaseInstance &db);

	// ST_Contains
	static void RegisterStContains(DatabaseInstance &db);

	// ST_Dimension
	static void RegisterStDimension(DatabaseInstance &db);

	// ST_Distance
	static void RegisterStDistance(DatabaseInstance &db);

	// ST_EndPoint
	static void RegisterStEndPoint(DatabaseInstance &db);

	// ST_Extent
	static void RegisterStExtent(DatabaseInstance &db);

	// ST_ExteriorRing
	static void RegisterStExteriorRing(DatabaseInstance &db);

	// ST_FlipCoordinates
	static void RegisterStFlipCoordinates(DatabaseInstance &db);

	// ST_GeometryType
	static void RegisterStGeometryType(DatabaseInstance &db);

	// ST_GeomFromHEXWKB
	static void RegisterStGeomFromHEXWKB(DatabaseInstance &db);

	// ST_GeomFromWKB
	static void RegisterStGeomFromWKB(DatabaseInstance &db);

	// ST_Intersects
	static void RegisterStIntersects(DatabaseInstance &db);

	// ST_IntersectsExtent (&&)
	static void RegisterStIntersectsExtent(DatabaseInstance &db);

	// ST_IsEmpty
	static void RegisterStIsEmpty(DatabaseInstance &db);

	// ST_Length
	static void RegisterStLength(DatabaseInstance &db);

	// ST_MakeEnvelope
	static void RegisterStMakeEnvelope(DatabaseInstance &db);

	// ST_MakeLine
	static void RegisterStMakeLine(DatabaseInstance &db);

	// ST_MakePolygon
	static void RegisterStMakePolygon(DatabaseInstance &db);

	// ST_NGeometries
	static void RegisterStNGeometries(DatabaseInstance &db);

	// ST_NInteriorRings
	static void RegisterStNInteriorRings(DatabaseInstance &db);

	// ST_NPoints
	static void RegisterStNPoints(DatabaseInstance &db);

	// ST_Perimeter
	static void RegisterStPerimeter(DatabaseInstance &db);

	// ST_Point
	static void RegisterStPoint(DatabaseInstance &db);

	// ST_PointN
	static void RegisterStPointN(DatabaseInstance &db);

	// ST_RemoveRepeatedPoints
	static void RegisterStRemoveRepeatedPoints(DatabaseInstance &db);

	// ST_StartPoint
	static void RegisterStStartPoint(DatabaseInstance &db);

	// ST_X
	static void RegisterStX(DatabaseInstance &db);

	// ST_XMax
	static void RegisterStXMax(DatabaseInstance &db);

	// ST_XMin
	static void RegisterStXMin(DatabaseInstance &db);

	// ST_Y
	static void RegisterStY(DatabaseInstance &db);

	// ST_YMax
	static void RegisterStYMax(DatabaseInstance &db);

	// ST_YMin
	static void RegisterStYMin(DatabaseInstance &db);
};

} // namespace core

} // namespace spatial