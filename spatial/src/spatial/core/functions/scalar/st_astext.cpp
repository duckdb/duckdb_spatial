#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/types.hpp"

#include "spatial/core/functions/cast.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// POINT_2D
//------------------------------------------------------------------------------

static void Point2DAsTextFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &input = args.data[0];
	auto count = args.size();
	CoreVectorOperations::Point2DToVarchar(input, result, count);
}
 
//------------------------------------------------------------------------------
// LINESTRING_2D
//------------------------------------------------------------------------------

// TODO: We want to format these to trim trailing zeros
static void LineString2DAsTextFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto &input = args.data[0];
	auto count = args.size();
	CoreVectorOperations::LineString2DToVarchar(input, result, count);
}

//------------------------------------------------------------------------------
// POLYGON_2D
//------------------------------------------------------------------------------

// TODO: We want to format these to trim trailing zeros
static void Polygon2DAsTextFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto count = args.size();
	auto &input = args.data[0];
	CoreVectorOperations::Polygon2DToVarchar(input, result, count);
}

//------------------------------------------------------------------------------
// BOX_2D
//------------------------------------------------------------------------------
static void Box2DAsTextFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto count = args.size();
	auto &input = args.data[0];
	CoreVectorOperations::Box2DToVarchar(input, result, count);
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void GeometryAsTextFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto count = args.size();
	auto &input = args.data[0];
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);
	CoreVectorOperations::GeometryToVarchar(input, result, count, lstate.factory);
}

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStAsText(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet as_text_function_set("ST_AsText");

	as_text_function_set.AddFunction(
	    ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::VARCHAR, Point2DAsTextFunction));
	as_text_function_set.AddFunction(
	    ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::VARCHAR, LineString2DAsTextFunction));
	as_text_function_set.AddFunction(
	    ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::VARCHAR, Polygon2DAsTextFunction));
	as_text_function_set.AddFunction(
	    ScalarFunction({GeoTypes::BOX_2D()}, LogicalType::VARCHAR, Box2DAsTextFunction));
	as_text_function_set.AddFunction(
	    ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::VARCHAR, GeometryAsTextFunction, nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(as_text_function_set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, info);
}

} // namespace core

} // namespace spatial