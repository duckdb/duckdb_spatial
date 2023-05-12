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
// POINT_2D
//------------------------------------------------------------------------------
static void PointCentroidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// The centroid of a point is the point itself
	auto input = args.data[0];
	result.Reference(input);
}

//------------------------------------------------------------------------------
// LINESTRING_2D
//------------------------------------------------------------------------------
static void LineStringCentroidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	auto count = args.size();
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(count, format);

	auto line_vertex_entries = ListVector::GetData(input);
	auto &line_vertex_vec = ListVector::GetEntry(input);
	auto &line_vertex_vec_children = StructVector::GetEntries(line_vertex_vec);
	auto line_x_data = FlatVector::GetData<double>(*line_vertex_vec_children[0]);
	auto line_y_vec = FlatVector::GetData<double>(*line_vertex_vec_children[1]);

	auto &point_vertex_children = StructVector::GetEntries(result);
	auto point_x_data = FlatVector::GetData<double>(*point_vertex_children[0]);
	auto point_y_data = FlatVector::GetData<double>(*point_vertex_children[1]);
	for (idx_t out_row_idx = 0; out_row_idx < count; out_row_idx++) {

		auto in_row_idx = format.sel->get_index(out_row_idx);
		if (format.validity.RowIsValid(in_row_idx)) {
			auto line = line_vertex_entries[in_row_idx];
			auto line_offset = line.offset;
			auto line_length = line.length;

			double total_x = 0;
			double total_y = 0;
			double total_length = 0;

			// To calculate the centroid of a line, we calculate the centroid of each segment
			// and then weight the segment centroids by the length of the segment.
			// The final centroid is the sum of the weighted segment centroids divided by the total length.
			for (idx_t coord_idx = line_offset; coord_idx < line_offset + line_length - 1; coord_idx++) {
				auto x1 = line_x_data[coord_idx];
				auto y1 = line_y_vec[coord_idx];
				auto x2 = line_x_data[coord_idx + 1];
				auto y2 = line_y_vec[coord_idx + 1];

				auto segment_length = sqrt((x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1));
				total_length += segment_length;
				total_x += (x1 + x2) * 0.5 * segment_length;
				total_y += (y1 + y2) * 0.5 * segment_length;
			}

			point_x_data[out_row_idx] = total_x / total_length;
			point_y_data[out_row_idx] = total_y / total_length;

		} else {
			FlatVector::SetNull(result, out_row_idx, true);
		}
	}
	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// POLYGON_2D
//------------------------------------------------------------------------------
static void PolygonCentroidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto input = args.data[0];
	auto count = args.size();
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(count, format);

	auto poly_entries = ListVector::GetData(input);
	auto &ring_vec = ListVector::GetEntry(input);
	auto ring_entries = ListVector::GetData(ring_vec);
	auto &vertex_vec = ListVector::GetEntry(ring_vec);
	auto &vertex_vec_children = StructVector::GetEntries(vertex_vec);
	auto x_data = FlatVector::GetData<double>(*vertex_vec_children[0]);
	auto y_data = FlatVector::GetData<double>(*vertex_vec_children[1]);

	auto &centroid_children = StructVector::GetEntries(result);
	auto centroid_x_data = FlatVector::GetData<double>(*centroid_children[0]);
	auto centroid_y_data = FlatVector::GetData<double>(*centroid_children[1]);

	for (idx_t in_row_idx = 0; in_row_idx < count; in_row_idx++) {
		if (format.validity.RowIsValid(in_row_idx)) {
			auto poly = poly_entries[in_row_idx];
			auto poly_offset = poly.offset;
			auto poly_length = poly.length;

			double poly_centroid_x = 0;
			double poly_centroid_y = 0;
			double poly_area = 0;

			// To calculate the centroid of a polygon, we calculate the centroid of each ring
			// and then weight the ring centroids by the area of the ring.
			// The final centroid is the sum of the weighted ring centroids divided by the total area.
			for (idx_t ring_idx = poly_offset; ring_idx < poly_offset + poly_length; ring_idx++) {
				auto ring = ring_entries[ring_idx];
				auto ring_offset = ring.offset;
				auto ring_length = ring.length;

				double ring_centroid_x = 0;
				double ring_centroid_y = 0;
				double ring_area = 0;

				// To calculate the centroid of a ring, we calculate the centroid of each triangle
				// and then weight the triangle centroids by the area of the triangle.
				// The final centroid is the sum of the weighted triangle centroids divided by the ring area.
				for (idx_t coord_idx = ring_offset; coord_idx < ring_offset + ring_length - 1; coord_idx++) {
					auto x1 = x_data[coord_idx];
					auto y1 = y_data[coord_idx];
					auto x2 = x_data[coord_idx + 1];
					auto y2 = y_data[coord_idx + 1];

					auto tri_area = (x1 * y2) - (x2 * y1);
					ring_centroid_x += (x1 + x2) * tri_area;
					ring_centroid_y += (y1 + y2) * tri_area;
					ring_area += tri_area;
				}
				ring_area *= 0.5;

				ring_centroid_x /= (ring_area * 6);
				ring_centroid_y /= (ring_area * 6);

				if (ring_idx == poly_offset) {
					// The first ring is the outer ring, and the remaining rings are holes.
					// For the outer ring, we add the area and centroid to the total area and centroid.
					poly_area += ring_area;
					poly_centroid_x += ring_centroid_x * ring_area;
					poly_centroid_y += ring_centroid_y * ring_area;
				} else {
					// For holes, we subtract the area and centroid from the total area and centroid.
					poly_area -= ring_area;
					poly_centroid_x -= ring_centroid_x * ring_area;
					poly_centroid_y -= ring_centroid_y * ring_area;
				}
			}
			centroid_x_data[in_row_idx] = poly_centroid_x / poly_area;
			centroid_y_data[in_row_idx] = poly_centroid_y / poly_area;
		} else {
			FlatVector::SetNull(result, in_row_idx, true);
		}
	}
	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// BOX_2D
//------------------------------------------------------------------------------
static void BoxCentroidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// using BOX_TYPE = StructTypeQuaternary<double, double, double, double>;
	// using POINT_TYPE = StructTypeBinary<double, double>;

	auto input = args.data[0];
	auto count = args.size();
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(count, format);
	auto &box_children = StructVector::GetEntries(input);
	auto minx_data = FlatVector::GetData<double>(*box_children[0]);
	auto miny_data = FlatVector::GetData<double>(*box_children[1]);
	auto maxx_data = FlatVector::GetData<double>(*box_children[2]);
	auto maxy_data = FlatVector::GetData<double>(*box_children[3]);

	auto &centroid_children = StructVector::GetEntries(result);
	auto centroid_x_data = FlatVector::GetData<double>(*centroid_children[0]);
	auto centroid_y_data = FlatVector::GetData<double>(*centroid_children[1]);

	for (idx_t out_row_idx = 0; out_row_idx < count; out_row_idx++) {
		auto in_row_idx = format.sel->get_index(out_row_idx);
		if (format.validity.RowIsValid(in_row_idx)) {
			centroid_x_data[out_row_idx] = (minx_data[in_row_idx] + maxx_data[in_row_idx]) * 0.5;
			centroid_y_data[out_row_idx] = (miny_data[in_row_idx] + maxy_data[in_row_idx]) * 0.5;
		} else {
			FlatVector::SetNull(result, out_row_idx, true);
		}
	}
	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> BoxCentroidBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {

	bound_function.return_type = GeoTypes::POINT_2D();
	return nullptr;
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStCentroid(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	ScalarFunctionSet area_function_set("ST_Centroid");
	area_function_set.AddFunction(ScalarFunction({GeoTypes::POINT_2D()}, GeoTypes::POINT_2D(), PointCentroidFunction));
	area_function_set.AddFunction(
	    ScalarFunction({GeoTypes::LINESTRING_2D()}, GeoTypes::POINT_2D(), LineStringCentroidFunction));
	area_function_set.AddFunction(
	    ScalarFunction({GeoTypes::POLYGON_2D()}, GeoTypes::POINT_2D(), PolygonCentroidFunction));
	area_function_set.AddFunction(
	    ScalarFunction({GeoTypes::BOX_2D()}, GeoTypes::POINT_2D(), BoxCentroidFunction, BoxCentroidBind));

	CreateScalarFunctionInfo info(std::move(area_function_set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	catalog.CreateFunction(context, info);
}

} // namespace core

} // namespace spatial