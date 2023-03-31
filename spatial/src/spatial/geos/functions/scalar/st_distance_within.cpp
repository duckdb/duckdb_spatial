#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/geos/functions/scalar.hpp"
#include "spatial/geos/functions/common.hpp"
#include "spatial/geos/geos_wrappers.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"


namespace spatial {

namespace geos {

using namespace spatial::core;

static void DistanceWithinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);

	TernaryExecutor::Execute<string_t, string_t, double, bool>(args.data[0], args.data[1], args.data[2], result, args.size(),
	                                                           [&](string_t &left_blob, string_t &right_blob, double distance) {
		auto left_geometry = lstate.factory.Deserialize(left_blob);
		auto right_geometry = lstate.factory.Deserialize(right_blob);
		auto geos_left = lstate.ctx.FromGeometry(left_geometry);
		auto geos_right = lstate.ctx.FromGeometry(right_geometry);
		auto ok = 1 == GEOSDistanceWithin_r(lstate.ctx.GetCtx(), geos_left.get(), geos_right.get(), distance);
		return ok;
	});
}

void GEOSScalarFunctions::RegisterStDistanceWithin(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("ST_DWithin");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY(), LogicalType::DOUBLE}, LogicalType::BOOLEAN, DistanceWithinFunction, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info);
}

} // namespace spatials

} // namespace spatial
