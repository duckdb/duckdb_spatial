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

static void BufferFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &left = args.data[0];
	auto &right = args.data[1];

	BinaryExecutor::Execute<string_t, double, string_t>(
	    left, right, result, args.size(), [&](string_t &geometry_blob, double radius) {
		    auto geos_geom = lstate.ctx.Deserialize(geometry_blob);
		    auto boundary =
		        make_uniq_geos(lstate.ctx.GetCtx(), GEOSBuffer_r(lstate.ctx.GetCtx(), geos_geom.get(), radius, 8));
		    return lstate.ctx.Serialize(result, boundary);
	    });
}

static void BufferFunctionWithSegments(DataChunk &args, ExpressionState &state, Vector &result) {

	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &left = args.data[0];
	auto &right = args.data[1];
	auto &segments = args.data[2];

	TernaryExecutor::Execute<string_t, double, int32_t, string_t>(
	    left, right, segments, result, args.size(), [&](string_t &geometry_blob, double radius, int32_t segments) {
		    auto geos_geom = lstate.ctx.Deserialize(geometry_blob);
		    auto boundary = make_uniq_geos(lstate.ctx.GetCtx(),
		                                   GEOSBuffer_r(lstate.ctx.GetCtx(), geos_geom.get(), radius, segments));
		    return lstate.ctx.Serialize(result, boundary);
	    });
}

void GEOSScalarFunctions::RegisterStBuffer(DatabaseInstance &db) {
	ScalarFunctionSet set("ST_Buffer");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::DOUBLE}, GeoTypes::GEOMETRY(), BufferFunction,
	                               nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::DOUBLE, LogicalType::INTEGER},
	                               GeoTypes::GEOMETRY(), BufferFunctionWithSegments, nullptr, nullptr, nullptr,
	                               GEOSFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace geos

} // namespace spatial
