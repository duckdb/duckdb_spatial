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

static void SimplifyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);
	BinaryExecutor::Execute<string_t, double, string_t>(args.data[0], args.data[1], result, args.size(), [&](string_t input, double distance) {
		auto geom = lstate.factory.Deserialize(input);
		auto geos_geom = lstate.ctx.FromGeometry(geom);
		auto geos_result = geos_geom.Simplify(distance);
		auto simplified_geom = lstate.ctx.ToGeometry(lstate.factory, geos_result.get());
		return lstate.factory.Serialize(result, simplified_geom);
	});
}

void GEOSScalarFunctions::RegisterStSimplify(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("ST_Simplify");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), LogicalType::DOUBLE}, GeoTypes::GEOMETRY(), SimplifyFunction, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info);
}

} // namespace spatials

} // namespace spatial
