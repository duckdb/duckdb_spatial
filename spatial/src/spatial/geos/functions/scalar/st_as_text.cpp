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

// TODO: we should implement our own WKT writer asap. This is a temporary and really inefficient solution.
// We could use the existing Geometry.ToString() but they do a bunch of allocations and doesnt format/trim the output.
static void GeometryToTextFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];
	auto ctx = GeosContextWrapper();

	auto writer = ctx.CreateWKTWriter();
	writer.SetTrim(true);

	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t &wkt) {
		auto geom = lstate.factory.Deserialize(wkt);
		auto geos_geom = ctx.FromGeometry(geom);
		return writer.Write(geos_geom, result);
	});
}

void GEOSScalarFunctions::RegisterStAsText(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	CreateScalarFunctionInfo info(
	    ScalarFunction("ST_AsText", {core::GeoTypes::GEOMETRY()}, LogicalType::VARCHAR, GeometryToTextFunction, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &info);
}

} // namespace spatials

} // namespace spatial
