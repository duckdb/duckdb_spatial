#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace geos {

struct GEOSScalarFunctions {
public:
	static void Register(DatabaseInstance &db) {
		RegisterStBoundary(db);
		RegisterStBuffer(db);
		RegisterStCentroid(db);
		RegisterStContains(db);
		RegisterStContainsProperly(db);
		RegisterStConvexHull(db);
		RegisterStCoveredBy(db);
		RegisterStCovers(db);
		RegisterStCrosses(db);
		RegisterStDifference(db);
		RegisterStDisjoint(db);
		RegisterStDistance(db);
		RegisterStDistanceWithin(db);
		RegisterStEquals(db);
		RegisterStEnvelope(db);
		RegisterStIntersection(db);
		RegisterStIntersects(db);
		RegisterStIsRing(db);
		RegisterStIsSimple(db);
		RegisterStIsValid(db);
		RegisterStLineMerge(db);
		RegisterStMakeValid(db);
		RegisterStNormalize(db);
		RegisterStOverlaps(db);
		RegisterStPointOnSurface(db);
		RegisterStReducePrecision(db);
		RegisterStRemoveRepeatedPoints(db);
		RegisterStReverse(db);
		RegisterStShortestLine(db);
		RegisterStSimplifyPreserveTopology(db);
		RegisterStSimplify(db);
		RegisterStTouches(db);
		RegisterStUnion(db);
		RegisterStWithin(db);
	}

private:
	static void RegisterStBoundary(DatabaseInstance &db);
	static void RegisterStBuffer(DatabaseInstance &db);
	static void RegisterStCentroid(DatabaseInstance &db);
	static void RegisterStContains(DatabaseInstance &db);
	static void RegisterStContainsProperly(DatabaseInstance &db);
	static void RegisterStConvexHull(DatabaseInstance &db);
	static void RegisterStCoveredBy(DatabaseInstance &db);
	static void RegisterStCovers(DatabaseInstance &db);
	static void RegisterStCrosses(DatabaseInstance &db);
	static void RegisterStDifference(DatabaseInstance &db);
	static void RegisterStDisjoint(DatabaseInstance &db);
	static void RegisterStDistance(DatabaseInstance &db);
	static void RegisterStDistanceWithin(DatabaseInstance &db);
	static void RegisterStEquals(DatabaseInstance &db);
	static void RegisterStEnvelope(DatabaseInstance &db);
	static void RegisterStIntersection(DatabaseInstance &db);
	static void RegisterStIntersects(DatabaseInstance &db);
	static void RegisterStIsRing(DatabaseInstance &db);
	static void RegisterStIsSimple(DatabaseInstance &db);
	static void RegisterStIsValid(DatabaseInstance &db);
	static void RegisterStNormalize(DatabaseInstance &db);
	static void RegisterStOverlaps(DatabaseInstance &db);
	static void RegisterStPointOnSurface(DatabaseInstance &db);
	static void RegisterStReducePrecision(DatabaseInstance &db);
	static void RegisterStRemoveRepeatedPoints(DatabaseInstance &db);
	static void RegisterStReverse(DatabaseInstance &db);
	static void RegisterStLineMerge(DatabaseInstance &db);
	static void RegisterStMakeValid(DatabaseInstance &db);
	static void RegisterStShortestLine(DatabaseInstance &db);
	static void RegisterStSimplifyPreserveTopology(DatabaseInstance &db);
	static void RegisterStSimplify(DatabaseInstance &db);
	static void RegisterStTouches(DatabaseInstance &db);
	static void RegisterStUnion(DatabaseInstance &db);
	static void RegisterStWithin(DatabaseInstance &db);
};

} // namespace geos

} // namespace spatial