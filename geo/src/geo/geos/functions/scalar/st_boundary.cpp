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

static void BoundaryFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);

	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(args.data[0], result, args.size(), [&](string_t &geometry_blob, ValidityMask& mask, idx_t i) {
		auto geometry = lstate.factory.Deserialize(geometry_blob);
		if(geometry.Type() == GeometryType::GEOMETRYCOLLECTION) {
			mask.SetInvalid(i);
			return string_t();
		}

		auto geos = lstate.ctx.FromGeometry(geometry);
		auto boundary = geos.Boundary();
		auto boundary_geometry = lstate.ctx.ToGeometry(lstate.factory, boundary.get());

		return lstate.factory.Serialize(result, boundary_geometry);
	});
}

void GEOSScalarFunctions::RegisterStBoundary(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet set("ST_Boundary");

	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(), BoundaryFunction, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info);
}

} // namespace geos

} // namespace geo
