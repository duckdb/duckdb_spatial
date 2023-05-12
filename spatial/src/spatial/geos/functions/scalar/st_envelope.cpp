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

static void EnvelopeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);

	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(args.data[0], result, args.size(), [&](string_t &geometry_blob, ValidityMask& mask, idx_t i) {
		auto geometry = lstate.factory.Deserialize(geometry_blob);
		auto geos = lstate.ctx.FromGeometry(geometry);
		auto boundary = geos.Envelope();
		auto boundary_geometry = lstate.ctx.ToGeometry(lstate.factory, boundary.get());
		return lstate.factory.Serialize(result, boundary_geometry);
	});
}

void GEOSScalarFunctions::RegisterStEnvelope(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("ST_Envelope");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), EnvelopeFunction, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, info);
}

} // namespace spatials

} // namespace spatial
