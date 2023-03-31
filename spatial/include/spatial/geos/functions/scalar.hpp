#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace geos {

struct GEOSScalarFunctions {
public:
	static void Register(ClientContext &context) {
		RegisterStAsText(context);
		RegisterStBoundary(context);
		RegisterStBuffer(context);
		RegisterStCentroid(context);
		RegisterStContains(context);
		RegisterStConvexHull(context);
		RegisterStCoveredBy(context);
		RegisterStCovers(context);
		RegisterStCrosses(context);
		RegisterStDifference(context);
		RegisterStDisjoint(context);
		RegisterStDistance(context);
		RegisterStDistanceWithin(context);
		RegisterStEquals(context);
		RegisterStGeomFromText(context);
		RegisterStEnvelope(context);
		RegisterStIntersection(context);
		RegisterStIntersects(context);
		RegisterStIsClosed(context);
		RegisterStIsRing(context);
		RegisterStIsSimple(context);
		RegisterStIsValid(context);
		RegisterStNormalize(context);
		RegisterStOverlaps(context);
		RegisterStSimplifyPreserveTopology(context);
		RegisterStSimplify(context);
		RegisterStTouches(context);
		RegisterStUnion(context);
		RegisterStWithin(context);
	}
private:
	static void RegisterStAsText(ClientContext &context);
	static void RegisterStBoundary(ClientContext &context);
	static void RegisterStBuffer(ClientContext &context);
	static void RegisterStCentroid(ClientContext &context);
	static void RegisterStContains(ClientContext &context);
	static void RegisterStConvexHull(ClientContext &context);
	static void RegisterStCoveredBy(ClientContext &context);
	static void RegisterStCovers(ClientContext &context);
	static void RegisterStCrosses(ClientContext &context);
	static void RegisterStDifference(ClientContext &context);
	static void RegisterStDisjoint(ClientContext &context);
	static void RegisterStDistance(ClientContext &context);
	static void RegisterStDistanceWithin(ClientContext &context);
	static void RegisterStEquals(ClientContext &context);
	static void RegisterStGeomFromText(ClientContext &context);
	static void RegisterStEnvelope(ClientContext &context);
	static void RegisterStIntersection(ClientContext &context);
	static void RegisterStIntersects(ClientContext &context);
	static void RegisterStIsClosed(ClientContext &context);
	static void RegisterStIsRing(ClientContext &context);
	static void RegisterStIsSimple(ClientContext &context);
	static void RegisterStIsValid(ClientContext &context);
	static void RegisterStNormalize(ClientContext &context);
	static void RegisterStOverlaps(ClientContext &context);
	static void RegisterStSimplifyPreserveTopology(ClientContext &context);
	static void RegisterStSimplify(ClientContext &context);
	static void RegisterStTouches(ClientContext &context);
	static void RegisterStUnion(ClientContext &context);
	static void RegisterStWithin(ClientContext &context);

};

} // namespace spatials

} // namespace spatial