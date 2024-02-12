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

static void NormalizeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &ctx = lstate.ctx.GetCtx();
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
		auto geom = lstate.ctx.Deserialize(input);
		auto res = GEOSNormalize_r(ctx, geom.get());
		if (res == -1) {
			throw InvalidInputException("Could not normalize geometry");
		}
		return lstate.ctx.Serialize(result, geom);
	});
}

void GEOSScalarFunctions::RegisterStNormalize(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_Normalize");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), NormalizeFunction, nullptr, nullptr,
	                               nullptr, GEOSFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace geos

} // namespace spatial
