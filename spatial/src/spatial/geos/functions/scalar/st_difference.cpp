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

static void DifferenceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &ctx = lstate.ctx.GetCtx();
	BinaryExecutor::Execute<geometry_t, geometry_t, geometry_t>(
	    args.data[0], args.data[1], result, args.size(), [&](geometry_t left, geometry_t right) {
		    auto left_geos_geom = lstate.ctx.Deserialize(left);
		    auto right_geos_geom = lstate.ctx.Deserialize(right);
		    auto geos_result = make_uniq_geos(ctx, GEOSDifference_r(ctx, left_geos_geom.get(), right_geos_geom.get()));
		    return lstate.ctx.Serialize(result, geos_result);
	    });
}

void GEOSScalarFunctions::RegisterStDifference(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_Difference");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(),
	                               DifferenceFunction, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace geos

} // namespace spatial
