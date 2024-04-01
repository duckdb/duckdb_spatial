#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
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
	CoreVectorOperations::GeometryToVarchar(input, result, count);
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
    Returns the geometry as a WKT string
)";

static constexpr const char *DOC_EXAMPLE = R"(
SELECT ST_AsText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry);
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "conversion"}};

//------------------------------------------------------------------------------
//  Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStAsText(DatabaseInstance &db) {
	ScalarFunctionSet as_text_function_set("ST_AsText");

	as_text_function_set.AddFunction(
	    ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::VARCHAR, Point2DAsTextFunction));
	as_text_function_set.AddFunction(
	    ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::VARCHAR, LineString2DAsTextFunction));
	as_text_function_set.AddFunction(
	    ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::VARCHAR, Polygon2DAsTextFunction));
	as_text_function_set.AddFunction(ScalarFunction({GeoTypes::BOX_2D()}, LogicalType::VARCHAR, Box2DAsTextFunction));
	as_text_function_set.AddFunction(
	    ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::VARCHAR, GeometryAsTextFunction));

	ExtensionUtil::RegisterFunction(db, as_text_function_set);
	DocUtil::AddDocumentation(db, "ST_AsText", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial