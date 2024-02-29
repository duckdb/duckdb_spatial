#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void GeometryNGeometriesFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto &ctx = GeometryFunctionLocalState::ResetAndGet(state);

	auto &input = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<geometry_t, int32_t>(input, result, count, [&](geometry_t input) {
		switch (input.GetType()) {
		case GeometryType::MULTIPOINT: {
			auto mpoint = ctx.factory.Deserialize(input).GetMultiPoint();
			return static_cast<int32_t>(mpoint.Count());
		}
		case GeometryType::MULTILINESTRING: {
			auto mline = ctx.factory.Deserialize(input).GetMultiLineString();
			return static_cast<int32_t>(mline.Count());
		}
		case GeometryType::MULTIPOLYGON: {
			auto mpoly = ctx.factory.Deserialize(input).GetMultiPolygon();
			return static_cast<int32_t>(mpoly.Count());
		}
		case GeometryType::GEOMETRYCOLLECTION: {
			auto collection = ctx.factory.Deserialize(input).GetGeometryCollection();
			return static_cast<int32_t>(collection.Count());
		}
		default:
			auto geom = ctx.factory.Deserialize(input);
			return geom.IsEmpty() ? 0 : 1;
		}
	});
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStNGeometries(DatabaseInstance &db) {

	const char *aliases[] = {"ST_NGeometries", "ST_NumGeometries"};
	for (auto alias : aliases) {
		ScalarFunctionSet set(alias);
		set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::INTEGER, GeometryNGeometriesFunction,
		                               nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));

		ExtensionUtil::RegisterFunction(db, set);
	}
}

} // namespace core

} // namespace spatial