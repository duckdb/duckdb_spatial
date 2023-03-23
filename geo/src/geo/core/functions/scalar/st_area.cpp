#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "geo/common.hpp"
#include "geo/core/functions/scalar.hpp"
#include "geo/core/functions/common.hpp"
#include "geo/core/geometry/geometry.hpp"
#include "geo/core/types.hpp"

namespace geo {

namespace core {

//------------------------------------------------------------------------------
// POLYGON_2D
//------------------------------------------------------------------------------
static void PolygonAreaFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto &input = args.data[0];
	auto count = args.size();

	auto &ring_vec = ListVector::GetEntry(input);
	auto ring_entries = ListVector::GetData(ring_vec);
	auto &coord_vec = ListVector::GetEntry(ring_vec);
	auto &coord_vec_children = StructVector::GetEntries(coord_vec);
	auto x_data = FlatVector::GetData<double>(*coord_vec_children[0]);
	auto y_data = FlatVector::GetData<double>(*coord_vec_children[1]);

	UnaryExecutor::Execute<list_entry_t, double>(input, result, count, [&](list_entry_t polygon) {
		auto polygon_offset = polygon.offset;
		auto polygon_length = polygon.length;

		bool first = true;
		double area = 0;
		for (idx_t ring_idx = polygon_offset; ring_idx < polygon_offset + polygon_length; ring_idx++) {
			auto ring = ring_entries[ring_idx];
			auto ring_offset = ring.offset;
			auto ring_length = ring.length;

			double sum = 0;
			for (idx_t coord_idx = ring_offset; coord_idx < ring_offset + ring_length - 1; coord_idx++) {
				sum += (x_data[coord_idx] * y_data[coord_idx + 1]) - (x_data[coord_idx + 1] * y_data[coord_idx]);
			}
			if (first) {
				// Add outer ring
				area = sum * 0.5;
				first = false;
			} else {
				// Subtract holes
				area -= sum * 0.5;
			}
		}
		return area;
	});

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------
static void GeometryAreaFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto &ctx = GeometryFunctionLocalState::ResetAndGet(state);

	auto &input = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<string_t, double>(input, result, count, [&](string_t input) {
		auto geometry = ctx.factory.Deserialize(input);
		switch (geometry.Type()) {
		case GeometryType::POLYGON:
			return geometry.GetPolygon().Area();
		case GeometryType::MULTIPOLYGON:
			return geometry.GetMultiPolygon().Area();
		case GeometryType::GEOMETRYCOLLECTION:
			return geometry.GetGeometryCollection().Aggregate([](Geometry &geom, double state){
				if(geom.Type() == GeometryType::POLYGON) {
					return state + geom.GetPolygon().Area();
				} else if(geom.Type() == GeometryType::MULTIPOLYGON) {
					return state + geom.GetMultiPolygon().Area();
				} else {
					return state;
				}
			}, 0.0);
		default:
			return 0.0;
		}
	});
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStArea(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet area_function_set("ST_Area");

	area_function_set.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::DOUBLE, PolygonAreaFunction, nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));
	area_function_set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryAreaFunction, nullptr, nullptr, nullptr, GeometryFunctionLocalState::Init));

	CreateScalarFunctionInfo info(std::move(area_function_set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, &info);
}

} // namespace core

} // namespace geo