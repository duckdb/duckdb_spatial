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

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void GeometryReverseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &input = args.data[0];
	auto count = args.size();

	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto ctx = lstate.ctx.GetCtx();
	UnaryExecutor::Execute<geometry_t, geometry_t>(input, result, count, [&](geometry_t input) {
		auto geom = lstate.ctx.Deserialize(input);
		auto result_geom = make_uniq_geos(ctx, GEOSReverse_r(ctx, geom.get()));
		return lstate.ctx.Serialize(result, result_geom);
	});
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void GEOSScalarFunctions::RegisterStReverse(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_Reverse");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), GeometryReverseFunction, nullptr,
	                               nullptr, nullptr, GEOSFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace geos

} // namespace spatial
