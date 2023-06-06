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

static void PointOnSurfaceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
    auto ctx = lstate.ctx.GetCtx();
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t &geometry_blob) {
		auto geometry = lstate.factory.Deserialize(geometry_blob);
		auto geos_geom = lstate.ctx.FromGeometry(geometry);
		auto geos_centroid = make_uniq_geos(ctx, GEOSPointOnSurface_r(ctx, geos_geom.get()));
		auto centroid_geometry = lstate.ctx.ToGeometry(lstate.factory, geos_centroid.get());

		return lstate.factory.Serialize(result, centroid_geometry);
	});
}

void GEOSScalarFunctions::RegisterStPointOnSurface(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("ST_PointOnSurface");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), PointOnSurfaceFunction, nullptr, nullptr,
	                               nullptr, GEOSFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, info);
}

} // namespace geos

} // namespace spatial
