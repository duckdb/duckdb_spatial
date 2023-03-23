#include "geo/common.hpp"
#include "geo/core/types.hpp"
#include "geo/geos/functions/scalar.hpp"
#include "geo/geos/functions/common.hpp"
#include "geo/geos/geos_wrappers.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"


namespace geo {

namespace geos {

using namespace geo::core;

static void DistanceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);

	BinaryExecutor::Execute<string_t, string_t, bool>(args.data[0], args.data[1], result, args.size(), [&](string_t &left_blob, string_t &right_blob) {
		auto left_geometry = lstate.factory.Deserialize(left_blob);
		auto right_geometry = lstate.factory.Deserialize(right_blob);
		auto geos_left = lstate.ctx.FromGeometry(left_geometry);
		auto geos_right = lstate.ctx.FromGeometry(right_geometry);
		double distance;
		GEOSDistance_r(lstate.ctx.GetCtx(), geos_left.get(), geos_right.get(), &distance);
		return distance;
	});
}

void GEOSScalarFunctions::RegisterStDistance(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("ST_Distance");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, DistanceFunction, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info);
}

} // namespace geos

} // namespace geo
