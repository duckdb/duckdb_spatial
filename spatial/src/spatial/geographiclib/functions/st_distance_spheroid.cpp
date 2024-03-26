#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/geographiclib/functions.hpp"
#include "spatial/geographiclib/module.hpp"

#include "GeographicLib/Geodesic.hpp"

namespace spatial {

namespace geographiclib {

//------------------------------------------------------------------------------
// POINT_2D
//------------------------------------------------------------------------------
static void GeodesicPoint2DFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	using POINT_TYPE = StructTypeBinary<double, double>;
	using DISTANCE_TYPE = PrimitiveType<double>;
	auto count = args.size();
	auto &p1 = args.data[0];
	auto &p2 = args.data[1];

	const GeographicLib::Geodesic &geod = GeographicLib::Geodesic::WGS84();

	GenericExecutor::ExecuteBinary<POINT_TYPE, POINT_TYPE, DISTANCE_TYPE>(
	    p1, p2, result, count, [&](POINT_TYPE p1, POINT_TYPE p2) {
		    double distance;
		    geod.Inverse(p1.a_val, p1.b_val, p2.a_val, p2.b_val, distance);
		    return distance;
	    });
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
Returns the distance between two geometries in meters using a ellipsoidal model of the earths surface

The input geometry is assumed to be in the [EPSG:4326](https://en.wikipedia.org/wiki/World_Geodetic_System) coordinate system (WGS84), with [latitude, longitude] axis order and the distance is returned in meters. This function uses the [GeographicLib](https://geographiclib.sourceforge.io/) library to solve the [inverse geodesic problem](https://en.wikipedia.org/wiki/Geodesics_on_an_ellipsoid#Solution_of_the_direct_and_inverse_problems), calculating the distance between two points using an ellipsoidal model of the earth. This is a highly accurate method for calculating the distance between two arbitrary points taking the curvature of the earths surface into account, but is also the slowest.
)";

static constexpr const char *DOC_EXAMPLE = R"(
-- Note: the coordinates are in WGS84 and [latitude, longitude] axis order
-- Whats the distance between New York and Amsterdam (JFK and AMS airport)?
SELECT st_distance_spheroid(
st_point(40.6446, 73.7797),
st_point(52.3130, 4.7725)
);
----
5243187.666873225
-- Roughly 5243km!
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "relation"}, {"category", "spheroid"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void GeographicLibFunctions::RegisterDistance(DatabaseInstance &db) {

	// Distance
	ScalarFunctionSet set("ST_Distance_Spheroid");
	set.AddFunction(ScalarFunction({spatial::core::GeoTypes::POINT_2D(), spatial::core::GeoTypes::POINT_2D()},
	                               LogicalType::DOUBLE, GeodesicPoint2DFunction));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_Distance_Spheroid", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace geographiclib

} // namespace spatial