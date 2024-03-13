#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// LineString2D
//------------------------------------------------------------------------------
static void LineIsEmptyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto &line_vec = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<list_entry_t, bool>(line_vec, result, count,
	                                           [&](list_entry_t line) { return line.length == 0; });

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// POLYGON_2D
//------------------------------------------------------------------------------
static void PolygonIsEmptyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto &polygon_vec = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<list_entry_t, bool>(polygon_vec, result, count,
	                                           [&](list_entry_t poly) { return poly.length == 0; });

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void GeometryIsEmptyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

	auto &input = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<geometry_t, bool>(input, result, count, [&](geometry_t input) {
		auto geometry = lstate.factory.Deserialize(input);
		return geometry.IsEmpty();
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStIsEmpty(DatabaseInstance &db) {

	ScalarFunctionSet is_empty_function_set("ST_IsEmpty");

	is_empty_function_set.AddFunction(
	    ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::BOOLEAN, LineIsEmptyFunction));
	is_empty_function_set.AddFunction(
	    ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::BOOLEAN, PolygonIsEmptyFunction));
	is_empty_function_set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::BOOLEAN,
	                                                 GeometryIsEmptyFunction, nullptr, nullptr, nullptr,
	                                                 GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, is_empty_function_set);
}

} // namespace core

} // namespace spatial