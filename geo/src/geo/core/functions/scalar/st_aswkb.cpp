#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "geo/common.hpp"
#include "geo/core/functions/scalar.hpp"
#include "geo/core/functions/common.hpp"
#include "geo/core/geometry/geometry_factory.hpp"
#include "geo/core/types.hpp"
namespace geo {

namespace core {

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
void GeometryAsWBKFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &input = args.data[0];
	auto count = args.size();

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t input) {
		auto geometry = lstate.factory.Deserialize(input);
		switch (geometry.Type()) {
		case GeometryType::POINT:
			return StringVector::AddString(result, geometry.GetPoint().ToString());
		case GeometryType::LINESTRING:
			return StringVector::AddString(result, geometry.GetLineString().ToString());
		case GeometryType::POLYGON:
			return StringVector::AddString(result, geometry.GetPolygon().ToString());
		default:
			throw NotImplementedException("Geometry type not implemented");
		}
	});
}

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStAsWKB(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet as_wkb_function_set("st_aswkb");

	as_wkb_function_set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY}, LogicalType::VARCHAR, GeometryAsWBKFunction, nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(as_wkb_function_set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info);
}

} // namespace core

} // namespace geo