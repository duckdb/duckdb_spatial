#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/geos/functions/scalar.hpp"
#include "spatial/geos/functions/common.hpp"

#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace spatial {

namespace geos {

using namespace spatial::core;

static void CentroidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &ctx = lstate.ctx.GetCtx();
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t &geometry_blob) {
		auto geometry = lstate.ctx.Deserialize(geometry_blob);
		auto centroid = make_uniq_geos(ctx, GEOSGetCentroid_r(ctx, geometry.get()));
		return lstate.ctx.Serialize(result, centroid);
	});
}

void GEOSScalarFunctions::RegisterStCentroid(ScalarFunctionSet& set) {
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), CentroidFunction, nullptr, nullptr,
	                               nullptr, GEOSFunctionLocalState::Init));
}

} // namespace geos

} // namespace spatial
