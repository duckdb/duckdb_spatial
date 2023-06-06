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

static void RemoveRepeatedPointsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    D_ASSERT(args.data.size() == 1);
	auto &input = args.data[0];
	auto count = args.size();

	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
    auto ctx = lstate.ctx.GetCtx();
    UnaryExecutor::Execute<string_t, string_t>(
	    input, result, count, [&](string_t input) {
		    auto geom = lstate.factory.Deserialize(input);
		    auto geos_geom = lstate.ctx.FromGeometry(geom);
		    auto geos_result = make_uniq_geos(ctx, GEOSRemoveRepeatedPoints_r(ctx, geos_geom.get(), 0));
            auto simplified_geom = lstate.ctx.ToGeometry(lstate.factory, geos_result.get());
		    return lstate.factory.Serialize(result, simplified_geom);
	    });
}

static void RemoveRepeatedPointsFunctionWithTolerance(DataChunk &args, ExpressionState &state, Vector &result) {
    D_ASSERT(args.data.size() == 2);
	auto &input = args.data[0];
    auto &tolerance = args.data[1];
	auto count = args.size();
    
    auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
    auto ctx = lstate.ctx.GetCtx();

    BinaryExecutor::Execute<string_t, double, string_t>(
	    input, tolerance, result, count, [&](string_t input, double tolerance) {
		    auto geom = lstate.factory.Deserialize(input);
		    auto geos_geom = lstate.ctx.FromGeometry(geom);
		    auto geos_result = make_uniq_geos(ctx, GEOSRemoveRepeatedPoints_r(ctx, geos_geom.get(), tolerance));
            auto simplified_geom = lstate.ctx.ToGeometry(lstate.factory, geos_result.get());
		    return lstate.factory.Serialize(result, simplified_geom);
	    });
}

void GEOSScalarFunctions::RegisterStRemoveRepeatedPoints(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("ST_RemoveRepeatedPoints");

    set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), 
        RemoveRepeatedPointsFunction, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::DOUBLE}, GeoTypes::GEOMETRY(), 
        RemoveRepeatedPointsFunctionWithTolerance, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, info);
}

} // namespace geos

} // namespace spatial
