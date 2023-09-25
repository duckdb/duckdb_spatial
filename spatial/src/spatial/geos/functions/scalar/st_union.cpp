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

static void UnionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &ctx = lstate.ctx.GetCtx();
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t left, string_t right) {
		    auto left_geom = lstate.ctx.Deserialize(left);
		    auto right_geom = lstate.ctx.Deserialize(right);
		    auto result_geom = make_uniq_geos(ctx, GEOSUnion_r(ctx, left_geom.get(), right_geom.get()));
		    return lstate.ctx.Serialize(result, result_geom);
	    });
}

void GEOSScalarFunctions::RegisterStUnion(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_Union");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), UnionFunction,
	                               nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace geos

} // namespace spatial
