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
static void FlipVertexVector(VertexVector &vertices) {
	for (idx_t i = 0; i < vertices.count; i++) {
		auto vertex = vertices.Get(i);
		std::swap(vertex.x, vertex.y);
		vertices.Set(i, vertex);
	}
}
static void FlipGeometry(Point &point) {
	FlipVertexVector(point.Vertices());
}

static void FlipGeometry(LineString &line) {
	FlipVertexVector(line.Vertices());
}

static void FlipGeometry(Polygon &poly) {
	for (auto &ring : poly.Rings()) {
		FlipVertexVector(ring);
	}
}

static void FlipGeometry(MultiPoint &multi_point) {
	for (auto &point : multi_point) {
		FlipGeometry(point);
	}
}

static void FlipGeometry(MultiLineString &multi_line) {
	for (auto &line : multi_line) {
		FlipGeometry(line);
	}
}

static void FlipGeometry(MultiPolygon &multi_poly) {
	for (auto &poly : multi_poly) {
		FlipGeometry(poly);
	}
}

static void FlipGeometry(Geometry &geom);
static void FlipGeometry(GeometryCollection &geom) {
	for (auto &child : geom) {
		FlipGeometry(child);
	}
}

static void FlipGeometry(Geometry &geom) {
	switch (geom.Type()) {
	case GeometryType::POINT:
		FlipGeometry(geom.GetPoint());
		break;
	case GeometryType::LINESTRING:
		FlipGeometry(geom.GetLineString());
		break;
	case GeometryType::POLYGON:
		FlipGeometry(geom.GetPolygon());
		break;
	case GeometryType::MULTIPOINT:
		FlipGeometry(geom.GetMultiPoint());
		break;
	case GeometryType::MULTILINESTRING:
		FlipGeometry(geom.GetMultiLineString());
		break;
	case GeometryType::MULTIPOLYGON:
		FlipGeometry(geom.GetMultiPolygon());
		break;
	case GeometryType::GEOMETRYCOLLECTION:
		FlipGeometry(geom.GetGeometryCollection());
		break;
	default:
		throw NotImplementedException("Unimplemented geometry type!");
	}
}

static void GeometryFlipCoordinatesFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

	auto input = args.data[0];
	auto count = args.size();

	UnaryExecutor::Execute<geometry_t, geometry_t>(input, result, count, [&](geometry_t input) {
		auto geom = lstate.factory.Deserialize(input);
		auto copy = lstate.factory.CopyGeometry(geom);
		FlipGeometry(copy);
		return lstate.factory.Serialize(result, copy);
	});
}

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
}

} // namespace core

} // namespace spatial