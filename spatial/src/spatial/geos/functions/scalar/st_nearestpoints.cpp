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

static void NearestPointsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &ctx = lstate.ctx.GetCtx();
	BinaryExecutor::Execute<geometry_t, geometry_t, geometry_t>(
	    args.data[0], args.data[1], result, args.size(), [&](geometry_t left, geometry_t right) {
		    auto left_geom = lstate.ctx.Deserialize(left);
		    auto right_geom = lstate.ctx.Deserialize(right);

            // Returned as a line string given the C++ API doesn't provide create_MultiPoint
            auto coord_seq =  GEOSNearestPoints_r(ctx, left_geom.get(), right_geom.get());
		    auto result_geom = make_uniq_geos(ctx, GEOSGeom_createLineString_r(ctx, coord_seq));

		    return lstate.ctx.Serialize(result, result_geom);
	    });
}

void GEOSScalarFunctions::RegisterStNearestPoints(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_NearestPoints");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(),
	                               NearestPointsFunction, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace geos

} // namespace spatial
