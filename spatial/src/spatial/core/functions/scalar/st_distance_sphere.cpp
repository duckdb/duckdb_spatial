#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/types.hpp"

#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/constants.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// Helper
//------------------------------------------------------------------------------
static inline double HaversineFunction(double lat1_p, double lon1_p, double lat2_p, double lon2_p) {
	// Radius of the earth in km
	auto R = 6371000.0;

	// Convert to radians
	auto lat1 = lat1_p * PI / 180.0;
	auto lon1 = lon1_p * PI / 180.0;
	auto lat2 = lat2_p * PI / 180.0;
	auto lon2 = lon2_p * PI / 180.0;

	auto dlat = lat2 - lat1;
	auto dlon = lon2 - lon1;

	auto a =
	    std::pow(std::sin(dlat / 2.0), 2.0) + std::cos(lat1) * std::cos(lat2) * std::pow(std::sin(dlon / 2.0), 2.0);
	auto c = 2.0 * std::atan2(std::sqrt(a), std::sqrt(1.0 - a));
	return R * c;
}

//------------------------------------------------------------------------------
// POINT_2D - POINT_2D
//------------------------------------------------------------------------------
static void PointHaversineFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	auto &left = args.data[0];
	auto &right = args.data[1];
	auto count = args.size();

	using POINT_TYPE = StructTypeBinary<double, double>;
	using DISTANCE_TYPE = PrimitiveType<double>;

	GenericExecutor::ExecuteBinary<POINT_TYPE, POINT_TYPE, DISTANCE_TYPE>(
	    left, right, result, count, [&](POINT_TYPE left, POINT_TYPE right) {
		    return HaversineFunction(left.a_val, left.b_val, right.a_val, right.b_val);
	    });
}

//------------------------------------------------------------------------------
// GEOMETRY - GEOMETRY
//------------------------------------------------------------------------------
static void GeometryHaversineFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	auto &left = args.data[0];
	auto &right = args.data[1];
	auto count = args.size();

	BinaryExecutor::Execute<geometry_t, geometry_t, double>(
	    left, right, result, count, [&](geometry_t left, geometry_t right) {
		    if (left.GetType() != GeometryType::POINT || right.GetType() != GeometryType::POINT) {
			    throw InvalidInputException("ST_Distance_Sphere only supports POINT geometries (for now!)");
		    }
		    auto left_geom = Geometry::Deserialize(lstate.arena, left);
		    auto right_geom = Geometry::Deserialize(lstate.arena, right);
		    if (Point::IsEmpty(left_geom) || Point::IsEmpty(right_geom)) {
			    throw InvalidInputException("ST_Distance_Sphere does not support EMPTY geometries");
		    }
		    auto v1 = Point::GetVertex(left_geom);
		    auto v2 = Point::GetVertex(right_geom);
		    return HaversineFunction(v1.x, v1.y, v2.x, v2.y);
	    });
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
    Returns the haversine distance between two geometries.

    - Only supports POINT geometries.
    - Returns the distance in meters.
    - The input is expected to be in WGS84 (EPSG:4326) coordinates, using a [latitude, longitude] axis order.
)";

static constexpr const char *DOC_EXAMPLE = R"(
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStHaversine(DatabaseInstance &db) {
	ScalarFunctionSet distance_function_set("ST_Distance_Sphere");

	distance_function_set.AddFunction(
	    ScalarFunction({GeoTypes::POINT_2D(), GeoTypes::POINT_2D()}, LogicalType::DOUBLE, PointHaversineFunction));

	distance_function_set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY()}, LogicalType::DOUBLE,
	                                                 GeometryHaversineFunction, nullptr, nullptr, nullptr,
	                                                 GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, distance_function_set);
	DocUtil::AddDocumentation(db, "ST_Distance_Sphere", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial