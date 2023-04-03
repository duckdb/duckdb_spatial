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

static void DistanceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);

	BinaryExecutor::Execute<string_t, string_t, double>(args.data[0], args.data[1], result, args.size(), [&](string_t &left_blob, string_t &right_blob) {
		auto left_geometry = lstate.factory.Deserialize(left_blob);
		auto right_geometry = lstate.factory.Deserialize(right_blob);
		auto geos_left = lstate.ctx.FromGeometry(left_geometry);
		auto geos_right = lstate.ctx.FromGeometry(right_geometry);
		double distance;
		auto ok = GEOSDistance_r(lstate.ctx.GetCtx(), geos_left.get(), geos_right.get(), &distance);
		if (!ok) {
			throw Exception("GEOSDistance_r failed");
		}
		return distance;
	});
}

void GEOSScalarFunctions::RegisterStDistance(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("ST_Distance");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, DistanceFunction, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info);
}

} // namespace spatials

} // namespace spatial
