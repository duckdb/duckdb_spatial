#include "geo/common.hpp"
#include "geo/core/types.hpp"
#include "geo/core/functions/scalar.hpp"
#include "geo/core/geometry/geometry.hpp"
#include "geo/core/geometry/geometry_context.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
namespace geo {

namespace core {

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
void GeometryAsWBKFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &input = args.data[0];
	auto count = args.size();

	auto &default_alloc = Allocator::DefaultAllocator();
	ArenaAllocator allocator(default_alloc, 1024);
	GeometryContext ctx(allocator);

	UnaryExecutor::Execute<string_t, string_t>(input, result, count, [&](string_t input) {
		auto geometry = ctx.Deserialize(input);
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

	as_wkb_function_set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY}, LogicalType::VARCHAR, GeometryAsWBKFunction));

	CreateScalarFunctionInfo info(std::move(as_wkb_function_set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info);
}

} // namespace core

} // namespace geo