#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

static unique_ptr<FunctionData> GeometryTypeFunctionBind(ClientContext &context, ScalarFunction &bound_function,
                                                         vector<unique_ptr<Expression>> &arguments) {
	// Create an enum type for all geometry types
	// Ensure that these are in the same order as the GeometryType enum
	vector<string> enum_values = {"POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON",
	                              "GEOMETRYCOLLECTION",
	                              // or...
	                              "UNKNOWN"};

	bound_function.return_type = GeoTypes::CreateEnumType("GEOMETRY_TYPE", enum_values);

	return nullptr;
}

//------------------------------------------------------------------------------
// Point2D
//------------------------------------------------------------------------------
static void Point2DTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	*ConstantVector::GetData<uint8_t>(result) = static_cast<uint8_t>(GeometryType::POINT);
}

//------------------------------------------------------------------------------
// LineString2D
//------------------------------------------------------------------------------
static void Linestring2DTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	*ConstantVector::GetData<uint8_t>(result) = static_cast<uint8_t>(GeometryType::LINESTRING);
}

//------------------------------------------------------------------------------
// Polygon2D
//------------------------------------------------------------------------------
static void Polygon2DTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	*ConstantVector::GetData<uint8_t>(result) = static_cast<uint8_t>(GeometryType::POLYGON);
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void GeometryTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();
	auto &input = args.data[0];

	UnaryExecutor::Execute<geometry_t, uint8_t>(
	    input, result, count, [&](const geometry_t &geom) {
		    return static_cast<uint8_t>(geom.GetType());
	    });
}

//------------------------------------------------------------------------------
// WKB
//------------------------------------------------------------------------------
static void WKBTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();
	auto &input = args.data[0];

	UnaryExecutor::Execute<string_t, uint8_t>(input, result, count, [&](const string_t &blob) {
		Cursor cursor(blob);
		const auto le = cursor.Read<uint8_t>();
		const auto type = le ? cursor.Read<uint32_t>() : cursor.ReadBigEndian<uint32_t>();
		const auto normalized_type = (type & 0xffff) % 1000;
		if(normalized_type == 0 || normalized_type > 7) {
			throw InvalidInputException("WKB type '%d' is not a supported geometry type", type);
		}

		// Return the geometry type
		// Subtract 1 since the WKB type is 1-indexed
		return static_cast<uint8_t>(normalized_type - 1);
	});
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char *DOC_DESCRIPTION = R"(
    Returns a 'GEOMETRY_TYPE' enum identifying the input geometry type.
)";

static constexpr const char *DOC_EXAMPLE = R"()";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};
//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStGeometryType(DatabaseInstance &db) {

	ScalarFunctionSet geometry_type_set("ST_GeometryType");
	geometry_type_set.AddFunction(
	    ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::ANY, Point2DTypeFunction, GeometryTypeFunctionBind));
	geometry_type_set.AddFunction(ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::ANY,
	                                             Linestring2DTypeFunction, GeometryTypeFunctionBind));
	geometry_type_set.AddFunction(
	    ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::ANY, Polygon2DTypeFunction, GeometryTypeFunctionBind));
	geometry_type_set.AddFunction(
	    ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::ANY, GeometryTypeFunction, GeometryTypeFunctionBind));

	geometry_type_set.AddFunction(
		ScalarFunction({GeoTypes::WKB_BLOB()}, LogicalType::ANY, WKBTypeFunction, GeometryTypeFunctionBind));

	ExtensionUtil::RegisterFunction(db, geometry_type_set);
	DocUtil::AddDocumentation(db, "ST_GeometryType", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial