#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/geos/functions/scalar.hpp"
#include "spatial/geos/functions/common.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace spatial {

namespace geos {

using namespace spatial::core;

static void ReducePrecisionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
    auto ctx = lstate.ctx.GetCtx();
    BinaryExecutor::Execute<string_t, double, string_t>(args.data[0], args.data[1], result, args.size(), 
        [&](string_t &geometry_blob, double precision) {
		auto geometry = lstate.ctx.Deserialize(geometry_blob);
        // Follow PostGIS behavior and dont set any special flags
		auto result_geom = make_uniq_geos(ctx, GEOSGeom_setPrecision_r(ctx, geometry.get(), precision, 0));
		return lstate.ctx.Serialize(result, result_geom);
	});
}

void GEOSScalarFunctions::RegisterStReducePrecision(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("ST_ReducePrecision");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::DOUBLE}, GeoTypes::GEOMETRY(), ReducePrecisionFunction, nullptr, nullptr,
	                               nullptr, GEOSFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, info);
}

} // namespace geos

} // namespace spatial
