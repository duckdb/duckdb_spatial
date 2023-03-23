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

// TODO: we should implement our own WKT parser asap. This is a temporary and really inefficient solution.
static void GeometryFromWKTFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto input = args.data[0];
	auto ctx = GeosContextWrapper();

	auto reader = ctx.CreateWKTReader();

	auto &lstate = GEOSFunctionLocalState::ResetAndGet(state);

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t &wkt) {
		auto geos_geom = reader.Read(wkt);
		if(geos_geom.get() == nullptr) {
			throw InvalidInputException("Invalid WKT string");
		}
		auto multidimensional = (GEOSHasZ_r(lstate.ctx.GetCtx(), geos_geom.get()) == 1);
		if(multidimensional) {
			throw InvalidInputException("3D/4D geometries are not supported");
		}

		auto geometry = ctx.ToGeometry(lstate.factory, geos_geom.get());
		return lstate.factory.Serialize(result, geometry);
	});
}

void GEOSScalarFunctions::RegisterStGeomFromText(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	CreateScalarFunctionInfo geometry_from_wkt_info(
	    ScalarFunction("ST_GeomFromText", {LogicalType::VARCHAR}, core::GeoTypes::GEOMETRY(), GeometryFromWKTFunction, nullptr, nullptr, nullptr, GEOSFunctionLocalState::Init));
	geometry_from_wkt_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.AddFunction(context, &geometry_from_wkt_info);

}

} // namespace geos

} // namespace geo
