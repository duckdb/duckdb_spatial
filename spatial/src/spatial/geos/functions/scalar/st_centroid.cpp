#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/geos/functions/scalar.hpp"
#include "spatial/geos/functions/common.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/catalog/catalog_entry/function_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"

namespace spatial {

namespace geos {

using namespace spatial::core;

static void CentroidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	auto &ctx = lstate.ctx.GetCtx();
	UnaryExecutor::Execute<geometry_t, geometry_t>(args.data[0], result, args.size(), [&](geometry_t &geometry_blob) {
		auto geometry = lstate.ctx.Deserialize(geometry_blob);
		auto centroid = make_uniq_geos(ctx, GEOSGetCentroid_r(ctx, geometry.get()));
		return lstate.ctx.Serialize(result, centroid);
	});
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
Calculates the centroid of a geometry
)";

static constexpr const char *DOC_EXAMPLE = R"(
select st_centroid('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
----
 POINT(0.5 0.5)
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void GEOSScalarFunctions::RegisterStCentroid(DatabaseInstance &db) {
	ScalarFunctionSet set("ST_Centroid");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), CentroidFunction, nullptr, nullptr,
	                               nullptr, GEOSFunctionLocalState::Init));

	ExtensionUtil::AddFunctionOverload(db, set);
	DocUtil::AddDocumentation(db, "ST_Centroid", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace geos

} // namespace spatial
