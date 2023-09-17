#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace geos {

struct GEOSScalarFunctions {
public:
	static void Register(DatabaseInstance &instance) {
		RegisterStBoundary(instance);
		RegisterStBuffer(instance);
		RegisterStCentroid(instance);
		RegisterStContains(instance);
		RegisterStContainsProperly(instance);
		RegisterStConvexHull(instance);
		RegisterStCoveredBy(instance);
		RegisterStCovers(instance);
		RegisterStCrosses(instance);
		RegisterStDifference(instance);
		RegisterStDisjoint(instance);
		RegisterStDistance(instance);
		RegisterStDistanceWithin(instance);
		RegisterStEquals(instance);
		RegisterStGeomFromText(instance);
		RegisterStEnvelope(instance);
		RegisterStIntersection(instance);
		RegisterStIntersects(instance);
		RegisterStIsClosed(instance);
		RegisterStIsRing(instance);
		RegisterStIsSimple(instance);
		RegisterStIsValid(instance);
		RegisterStNormalize(instance);
		RegisterStOverlaps(instance);
		RegisterStPointOnSurface(instance);
		RegisterStReducePrecision(instance);
		RegisterStRemoveRepeatedPoints(instance);
		RegisterStSimplifyPreserveTopology(instance);
		RegisterStSimplify(instance);
		RegisterStTouches(instance);
		RegisterStUnion(instance);
		RegisterStWithin(instance);
	}

private:
	static void RegisterStBoundary(DatabaseInstance &instance);
	static void RegisterStBuffer(DatabaseInstance &instance);
	static void RegisterStCentroid(DatabaseInstance &instance);
	static void RegisterStContains(DatabaseInstance &instance);
	static void RegisterStContainsProperly(DatabaseInstance &instance);
	static void RegisterStConvexHull(DatabaseInstance &instance);
	static void RegisterStCoveredBy(DatabaseInstance &instance);
	static void RegisterStCovers(DatabaseInstance &instance);
	static void RegisterStCrosses(DatabaseInstance &instance);
	static void RegisterStDifference(DatabaseInstance &instance);
	static void RegisterStDisjoint(DatabaseInstance &instance);
	static void RegisterStDistance(DatabaseInstance &instance);
	static void RegisterStDistanceWithin(DatabaseInstance &instance);
	static void RegisterStEquals(DatabaseInstance &instance);
	static void RegisterStGeomFromText(DatabaseInstance &instance);
	static void RegisterStEnvelope(DatabaseInstance &instance);
	static void RegisterStIntersection(DatabaseInstance &instance);
	static void RegisterStIntersects(DatabaseInstance &instance);
	static void RegisterStIsClosed(DatabaseInstance &instance);
	static void RegisterStIsRing(DatabaseInstance &instance);
	static void RegisterStIsSimple(DatabaseInstance &instance);
	static void RegisterStIsValid(DatabaseInstance &instance);
	static void RegisterStNormalize(DatabaseInstance &instance);
	static void RegisterStOverlaps(DatabaseInstance &instance);
	static void RegisterStPointOnSurface(DatabaseInstance &instance);
	static void RegisterStReducePrecision(DatabaseInstance &instance);
	static void RegisterStRemoveRepeatedPoints(DatabaseInstance &instance);
	static void RegisterStSimplifyPreserveTopology(DatabaseInstance &instance);
	static void RegisterStSimplify(DatabaseInstance &instance);
	static void RegisterStTouches(DatabaseInstance &instance);
	static void RegisterStUnion(DatabaseInstance &instance);
	static void RegisterStWithin(DatabaseInstance &instance);
};

} // namespace geos

} // namespace spatial