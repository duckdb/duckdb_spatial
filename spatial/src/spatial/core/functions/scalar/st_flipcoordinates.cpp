#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

// TODO: We should be able to optimize these and avoid the flatten

//------------------------------------------------------------------------------
// POINT_2D
//------------------------------------------------------------------------------
static void PointFlipCoordinatesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	auto count = args.size();

	// TODO: Avoid flatten
	input.Flatten(count);

	auto &coords_in = StructVector::GetEntries(input);
	auto x_data_in = FlatVector::GetData<double>(*coords_in[0]);
	auto y_data_in = FlatVector::GetData<double>(*coords_in[1]);

	auto &coords_out = StructVector::GetEntries(result);
	auto x_data_out = FlatVector::GetData<double>(*coords_out[0]);
	auto y_data_out = FlatVector::GetData<double>(*coords_out[1]);

	memcpy(x_data_out, y_data_in, count * sizeof(double));
	memcpy(y_data_out, x_data_in, count * sizeof(double));

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// LINESTRING_2D
//------------------------------------------------------------------------------
static void LineStringFlipCoordinatesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	auto count = args.size();

	// TODO: Avoid flatten
	input.Flatten(count);

	auto coord_vec_in = ListVector::GetEntry(input);
	auto &coords_in = StructVector::GetEntries(coord_vec_in);
	auto x_data_in = FlatVector::GetData<double>(*coords_in[0]);
	auto y_data_in = FlatVector::GetData<double>(*coords_in[1]);

	auto coord_count = ListVector::GetListSize(input);
	ListVector::Reserve(result, coord_count);
	ListVector::SetListSize(result, coord_count);

	auto line_entries_in = ListVector::GetData(input);
	auto line_entries_out = ListVector::GetData(result);
	memcpy(line_entries_out, line_entries_in, count * sizeof(list_entry_t));

	auto coord_vec_out = ListVector::GetEntry(result);
	auto &coords_out = StructVector::GetEntries(coord_vec_out);
	auto x_data_out = FlatVector::GetData<double>(*coords_out[0]);
	auto y_data_out = FlatVector::GetData<double>(*coords_out[1]);

	memcpy(x_data_out, y_data_in, coord_count * sizeof(double));
	memcpy(y_data_out, x_data_in, coord_count * sizeof(double));

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// POLYGON_2D
//------------------------------------------------------------------------------
static void PolygonFlipCoordinatesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	auto count = args.size();

	// TODO: Avoid flatten
	input.Flatten(count);

	auto ring_vec_in = ListVector::GetEntry(input);
	auto ring_count = ListVector::GetListSize(input);

	auto coord_vec_in = ListVector::GetEntry(ring_vec_in);
	auto &coords_in = StructVector::GetEntries(coord_vec_in);
	auto x_data_in = FlatVector::GetData<double>(*coords_in[0]);
	auto y_data_in = FlatVector::GetData<double>(*coords_in[1]);

	auto coord_count = ListVector::GetListSize(ring_vec_in);

	ListVector::Reserve(result, ring_count);
	ListVector::SetListSize(result, ring_count);
	auto ring_vec_out = ListVector::GetEntry(result);
	ListVector::Reserve(ring_vec_out, coord_count);
	ListVector::SetListSize(ring_vec_out, coord_count);

	auto ring_entries_in = ListVector::GetData(input);
	auto ring_entries_out = ListVector::GetData(result);
	memcpy(ring_entries_out, ring_entries_in, count * sizeof(list_entry_t));

	auto coord_entries_in = ListVector::GetData(ring_vec_in);
	auto coord_entries_out = ListVector::GetData(ring_vec_out);
	memcpy(coord_entries_out, coord_entries_in, ring_count * sizeof(list_entry_t));

	auto coord_vec_out = ListVector::GetEntry(ring_vec_out);
	auto &coords_out = StructVector::GetEntries(coord_vec_out);
	auto x_data_out = FlatVector::GetData<double>(*coords_out[0]);
	auto y_data_out = FlatVector::GetData<double>(*coords_out[1]);

	memcpy(x_data_out, y_data_in, coord_count * sizeof(double));
	memcpy(y_data_out, x_data_in, coord_count * sizeof(double));

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// BOX_2D
//------------------------------------------------------------------------------
static void BoxFlipCoordinatesFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto input = args.data[0];
	auto count = args.size();

	// TODO: Avoid flatten
	input.Flatten(count);

	auto &children_in = StructVector::GetEntries(input);
	auto min_x_in = FlatVector::GetData<double>(*children_in[0]);
	auto min_y_in = FlatVector::GetData<double>(*children_in[1]);
	auto max_x_in = FlatVector::GetData<double>(*children_in[2]);
	auto max_y_in = FlatVector::GetData<double>(*children_in[3]);

	auto &children_out = StructVector::GetEntries(result);
	auto min_x_out = FlatVector::GetData<double>(*children_out[0]);
	auto min_y_out = FlatVector::GetData<double>(*children_out[1]);
	auto max_x_out = FlatVector::GetData<double>(*children_out[2]);
	auto max_y_out = FlatVector::GetData<double>(*children_out[3]);

	memcpy(min_x_out, min_y_in, count * sizeof(double));
	memcpy(min_y_out, min_x_in, count * sizeof(double));
	memcpy(max_x_out, max_y_in, count * sizeof(double));
	memcpy(max_y_out, max_x_in, count * sizeof(double));
}

//------------------------------------------------------------------------------
// GEOMETRY
//------------------------------------------------------------------------------

struct FlipOp {
	static void FlipVertexArray(VertexArray &vertices, ArenaAllocator &arena) {
		vertices.MakeOwning(arena);
		for (idx_t i = 0; i < vertices.Count(); i++) {
			auto vertex = vertices.Get(i);
			std::swap(vertex.x, vertex.y);
			vertices.Set(i, vertex);
		}
	}

	static void Apply(Point &point, ArenaAllocator &alloc) {
		FlipVertexArray(point.Vertices(), alloc);
	}

	static void Apply(LineString &line, ArenaAllocator &alloc) {
		FlipVertexArray(line.Vertices(), alloc);
	}

	static void Apply(Polygon &poly, ArenaAllocator &alloc) {
		for (auto &ring : poly) {
			FlipVertexArray(ring, alloc);
		}
	}

	static void Apply(MultiPoint &multi_point, ArenaAllocator &alloc) {
		for (auto &point : multi_point) {
			Apply(point, alloc);
		}
	}

	static void Apply(MultiLineString &multi_line, ArenaAllocator &alloc) {
		for (auto &line : multi_line) {
			Apply(line, alloc);
		}
	}

	static void Apply(MultiPolygon &multi_poly, ArenaAllocator &alloc) {
		for (auto &poly : multi_poly) {
			Apply(poly, alloc);
		}
	}

	static void Apply(GeometryCollection &geom, ArenaAllocator &alloc) {
		for (auto &child : geom) {
			child.Dispatch<FlipOp>(alloc);
		}
	}
};

static void GeometryFlipCoordinatesFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

	auto input = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<geometry_t, geometry_t>(input, result, count, [&](geometry_t input) {
		auto props = input.GetProperties();
		auto geom = lstate.factory.Deserialize(input);
		geom.Dispatch<FlipOp>(lstate.factory.allocator);
		return lstate.factory.Serialize(result, geom, props.HasZ(), props.HasM());
	});
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr const char* DOC_DESCRIPTION = R"(
    Returns a new geometry with the coordinates of the input geometry "flipped" so that x = y and y = x.
)";

static constexpr const char* DOC_EXAMPLE = R"()";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "construction"}};
//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStFlipCoordinates(DatabaseInstance &db) {
	ScalarFunctionSet flip_function_set("ST_FlipCoordinates");
	flip_function_set.AddFunction(
	    ScalarFunction({GeoTypes::POINT_2D()}, GeoTypes::POINT_2D(), PointFlipCoordinatesFunction));
	flip_function_set.AddFunction(
	    ScalarFunction({GeoTypes::LINESTRING_2D()}, GeoTypes::LINESTRING_2D(), LineStringFlipCoordinatesFunction));
	flip_function_set.AddFunction(
	    ScalarFunction({GeoTypes::POLYGON_2D()}, GeoTypes::POLYGON_2D(), PolygonFlipCoordinatesFunction));
	flip_function_set.AddFunction(ScalarFunction({GeoTypes::BOX_2D()}, GeoTypes::BOX_2D(), BoxFlipCoordinatesFunction));
	flip_function_set.AddFunction(ScalarFunction({GeoTypes::GEOMETRY()}, GeoTypes::GEOMETRY(),
	                                             GeometryFlipCoordinatesFunction, nullptr, nullptr, nullptr,
	                                             GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, flip_function_set);
    DocUtil::AddDocumentation(db, "ST_FlipCoordinates", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace core

} // namespace spatial