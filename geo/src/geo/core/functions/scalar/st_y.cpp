#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "geo/common.hpp"
#include "geo/core/functions/scalar.hpp"
#include "geo/core/geometry/geometry.hpp"
#include "geo/core/geometry/geometry_factory.hpp"
#include "geo/core/types.hpp"

namespace geo {

namespace core {

//------------------------------------------------------------------------------
// POINT_2D
//------------------------------------------------------------------------------
static void Point2DFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &point = args.data[0];
	auto &point_children = StructVector::GetEntries(point);
	auto &y_child = point_children[1];
	result.Reference(*y_child);
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void GeometryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto &default_alloc = Allocator::DefaultAllocator();
	ArenaAllocator allocator(default_alloc);
	GeometryFactory ctx(allocator);

	auto &input = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<string_t, double>(input, result, count, [&](string_t input) {
		allocator.Reset();
		auto geometry = ctx.Deserialize(input);
		if (geometry.Type() != GeometryType::POINT) {
			throw InvalidInputException("ST_Y only implemented for POINT geometries");
		}
		return geometry.GetPoint().Y();
	});
}

//------------------------------------------------------------------------------
// Register Functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStY(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet st_y("st_y");
	st_y.AddFunction(ScalarFunction({GeoTypes::POINT_2D}, LogicalType::DOUBLE, Point2DFunction));
	st_y.AddFunction(ScalarFunction({GeoTypes::GEOMETRY}, LogicalType::DOUBLE, GeometryFunction));

	CreateScalarFunctionInfo info(std::move(st_y));
	catalog.AddFunction(context, &info);
}

} // namespace core

} // namespace geo
