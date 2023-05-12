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

	UnaryExecutor::Execute<list_entry_t, bool>(line_vec, result, count, [&](list_entry_t line) {
		return line.length == 0;
	});

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

	UnaryExecutor::Execute<list_entry_t, bool>(polygon_vec, result, count, [&](list_entry_t poly) {
		return poly.length == 0;
	});

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

	UnaryExecutor::Execute<string_t, bool>(input, result, count, [&](string_t input) {
		auto geometry = lstate.factory.Deserialize(input);
		switch (geometry.Type()) {
		case GeometryType::POINT:
			return geometry.GetPoint().IsEmpty();
		case GeometryType::LINESTRING:
			return geometry.GetLineString().IsEmpty();
		case GeometryType::POLYGON:
			return geometry.GetPolygon().IsEmpty();
		case GeometryType::MULTIPOINT:
			return geometry.GetMultiPoint().IsEmpty();
		case GeometryType::MULTILINESTRING:
			return geometry.GetMultiLineString().IsEmpty();
		case GeometryType::MULTIPOLYGON:
			return geometry.GetMultiPolygon().IsEmpty();
		case GeometryType::GEOMETRYCOLLECTION:
			return geometry.GetGeometryCollection().IsEmpty();
		default:
			throw NotImplementedException("Unimplemented geometry type for ST_IsEmpty");
		}
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStIsEmpty(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet is_empty_function_set("ST_IsEmpty");

	is_empty_function_set.AddFunction(ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::BOOLEAN, LineIsEmptyFunction));
	is_empty_function_set.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::BOOLEAN, PolygonIsEmptyFunction));
	is_empty_function_set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::BOOLEAN, GeometryIsEmptyFunction, nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(is_empty_function_set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, info);
}

} // namespace core

} // namespace spatial