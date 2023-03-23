#include "geo/common.hpp"
#include "geo/core/types.hpp"
#include "geo/geos/functions/scalar.hpp"
#include "geo/geos/functions/common.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace geo {

namespace geos {

using namespace geo::core;

static void CentroidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);

	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t &geometry_blob) {
		auto geometry = lstate.factory.Deserialize(geometry_blob);
		auto geos_geom = lstate.ctx.FromGeometry(geometry);
		auto geos_centroid = geos_geom.Centroid();
		auto centroid_geometry = lstate.ctx.ToGeometry(lstate.factory, geos_centroid.get());

		return lstate.factory.Serialize(result, centroid_geometry);
	});
}

void GEOSScalarFunctions::RegisterStCentroid(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("ST_Centroid");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), CentroidFunction, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info);
}

} // namespace geos

} // namespace geo
