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

static void MakeValidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &ctx = lstate.ctx.GetCtx();
	UnaryExecutor::Execute<geometry_t, geometry_t>(args.data[0], result, args.size(), [&](geometry_t input) {
		auto geom = lstate.ctx.Deserialize(input);
		auto valid = make_uniq_geos(ctx, GEOSMakeValid_r(ctx, geom.get()));
		return lstate.ctx.Serialize(result, valid);
	});
}

void GEOSScalarFunctions::RegisterStMakeValid(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_MakeValid");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), MakeValidFunction, nullptr, nullptr,
	                               nullptr, GEOSFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace geos

} // namespace spatial
