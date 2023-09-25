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
// LINESTRING_2D
//------------------------------------------------------------------------------
static void LineStringAreaFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	UnaryExecutor::Execute<list_entry_t, double>(input, result, args.size(), [](list_entry_t) { return 0; });
}

//------------------------------------------------------------------------------
// POINT_2D
//------------------------------------------------------------------------------
static void PointAreaFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	using POINT_TYPE = StructTypeBinary<double, double>;
	using AREA_TYPE = PrimitiveType<double>;
	GenericExecutor::ExecuteUnary<POINT_TYPE, AREA_TYPE>(args.data[0], result, args.size(),
	                                                     [](POINT_TYPE) { return 0; });
}

//------------------------------------------------------------------------------
// BOX_2D
//------------------------------------------------------------------------------
static void BoxAreaFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
	using AREA_TYPE = PrimitiveType<double>;

	GenericExecutor::ExecuteUnary<BOX_TYPE, AREA_TYPE>(args.data[0], result, args.size(), [&](BOX_TYPE &box) {
		auto minx = box.a_val;
		auto miny = box.b_val;
		auto maxx = box.c_val;
		auto maxy = box.d_val;
		return AREA_TYPE {(maxx - minx) * (maxy - miny)};
	});
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
			return geometry.GetGeometryCollection().Aggregate(
			    [](Geometry &geom, double state) {
				    if (geom.Type() == GeometryType::POLYGON) {
					    return state + geom.GetPolygon().Area();
				    } else if (geom.Type() == GeometryType::MULTIPOLYGON) {
					    return state + geom.GetMultiPolygon().Area();
				    } else {
					    return state;
				    }
			    },
			    0.0);
		default:
			return 0.0;
		}
	});
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStArea(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_Area");
	set.AddFunction(ScalarFunction({GeoTypes::POINT_2D()}, LogicalType::DOUBLE, PointAreaFunction));
	set.AddFunction(ScalarFunction({GeoTypes::LINESTRING_2D()}, LogicalType::DOUBLE, LineStringAreaFunction));
	set.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D()}, LogicalType::DOUBLE, PolygonAreaFunction));
	set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, LogicalType::DOUBLE, GeometryAreaFunction, nullptr, nullptr,
	                               nullptr, GeometryFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({GeoTypes::BOX_2D()}, LogicalType::DOUBLE, BoxAreaFunction));

	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace core

} // namespace spatial