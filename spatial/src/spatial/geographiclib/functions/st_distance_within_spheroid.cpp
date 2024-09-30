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
	using BOOL_TYPE = PrimitiveType<bool>;
	auto count = args.size();
	auto &p1_vec = args.data[0];
	auto &p2_vec = args.data[1];
	auto &limit_vec = args.data[2];

	const GeographicLib::Geodesic &geod = GeographicLib::Geodesic::WGS84();

	GenericExecutor::ExecuteTernary<POINT_TYPE, POINT_TYPE, DISTANCE_TYPE, BOOL_TYPE>(
	    p1_vec, p2_vec, limit_vec, result, count, [&](POINT_TYPE p1, POINT_TYPE p2, DISTANCE_TYPE limit) {
		    double distance;
		    geod.Inverse(p1.a_val, p1.b_val, p2.a_val, p2.b_val, distance);
		    return distance <= limit.val;
	    });
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
    Returns if two POINT_2D's are within a target distance in meters, using an ellipsoidal model of the earths surface

	The input geometry is assumed to be in the [EPSG:4326](https://en.wikipedia.org/wiki/World_Geodetic_System) coordinate system (WGS84), with [latitude, longitude] axis order and the distance is returned in meters. This function uses the [GeographicLib](https://geographiclib.sourceforge.io/) library to solve the [inverse geodesic problem](https://en.wikipedia.org/wiki/Geodesics_on_an_ellipsoid#Solution_of_the_direct_and_inverse_problems), calculating the distance between two points using an ellipsoidal model of the earth. This is a highly accurate method for calculating the distance between two arbitrary points taking the curvature of the earths surface into account, but is also the slowest.
)";

static constexpr const char *DOC_EXAMPLE = R"(

)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "relation"}, {"category", "spheroid"}};

//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------
void GeographicLibFunctions::RegisterDistanceWithin(DatabaseInstance &db) {

	// Distance
	ScalarFunctionSet set("ST_DWithin_Spheroid");
	set.AddFunction(
	    ScalarFunction({spatial::core::GeoTypes::POINT_2D(), spatial::core::GeoTypes::POINT_2D(), LogicalType::DOUBLE},
	                   LogicalType::BOOLEAN, GeodesicPoint2DFunction));

	ExtensionUtil::RegisterFunction(db, set);
	DocUtil::AddDocumentation(db, "ST_DWithin_Spheroid", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace geographiclib

} // namespace spatial