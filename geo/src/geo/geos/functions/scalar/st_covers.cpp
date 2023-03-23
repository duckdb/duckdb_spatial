#include "geo/common.hpp"
#include "geo/core/types.hpp"
#include "geo/geos/functions/scalar.hpp"
#include "geo/geos/functions/common.hpp"
#include "geo/geos/geos_wrappers.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace geo {

namespace geos {

using namespace geo::core;

static void CoversFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &local_state = GEOSFunctionLocalState::ResetAndGet(state);

	BinaryExecutor::Execute<string_t, string_t, bool>(args.data[0], args.data[1], result, args.size(), [&](string_t &left_blob, string_t &right_blob) {
		auto left_geometry = local_state.factory.Deserialize(left_blob);
		auto right_geometry = local_state.factory.Deserialize(right_blob);
		auto geos_left = local_state.ctx.FromGeometry(left_geometry);
		auto geos_right = local_state.ctx.FromGeometry(right_geometry);
		return geos_left.Covers(geos_right);
	});
}

void GEOSScalarFunctions::RegisterStCovers(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("ST_Covers");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY(), GeoTypes::GEOMETRY()}, LogicalType::BOOLEAN, CoversFunction, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info);
}

} // namespace geos

} // namespace geo
