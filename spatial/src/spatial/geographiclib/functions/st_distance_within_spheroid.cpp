#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
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

void GeographicLibFunctions::RegisterDistanceWithin(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	// Distance
	ScalarFunctionSet set("ST_DWithin_Spheroid");
	set.AddFunction(
	    ScalarFunction({spatial::core::GeoTypes::POINT_2D(), spatial::core::GeoTypes::POINT_2D(), LogicalType::DOUBLE},
	                   LogicalType::DOUBLE, GeodesicPoint2DFunction));
	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, info);
}

} // namespace geographiclib

} // namespace spatial