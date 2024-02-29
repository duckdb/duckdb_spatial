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

static void LineMergeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &ctx = lstate.ctx.GetCtx();
	UnaryExecutor::Execute<geometry_t, geometry_t>(args.data[0], result, args.size(), [&](geometry_t &geometry_blob) {
		auto geometry = lstate.ctx.Deserialize(geometry_blob);
		auto convex_hull_geometry = make_uniq_geos(ctx, GEOSLineMerge_r(ctx, geometry.get()));
		return lstate.ctx.Serialize(result, convex_hull_geometry);
	});
}

static void LineMergeFunctionWithDirected(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &ctx = lstate.ctx.GetCtx();
	BinaryExecutor::Execute<geometry_t, bool, geometry_t>(
	    args.data[0], args.data[1], result, args.size(), [&](geometry_t &geometry_blob, bool directed) {
		    auto geometry = lstate.ctx.Deserialize(geometry_blob);
		    auto convex_hull_geometry = directed ? make_uniq_geos(ctx, GEOSLineMergeDirected_r(ctx, geometry.get()))
		                                         : make_uniq_geos(ctx, GEOSLineMerge_r(ctx, geometry.get()));

		    return lstate.ctx.Serialize(result, convex_hull_geometry);
	    });
}

void GEOSScalarFunctions::RegisterStLineMerge(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_LineMerge");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), LineMergeFunction, nullptr, nullptr,
	                               nullptr, GEOSFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::BOOLEAN}, GeoTypes::GEOMETRY(),
	                               LineMergeFunctionWithDirected, nullptr, nullptr, nullptr,
	                               GEOSFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace geos

} // namespace spatial
