#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/scalar.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// POLYGON_2D - POINT_2D
//------------------------------------------------------------------------------
static void PointInPolygonOperation(Vector &in_point, Vector &in_polygon, Vector &result, idx_t count) {

	in_polygon.Flatten(count);
	in_point.Flatten(count);

	// Setup point vectors
	auto &p_children = StructVector::GetEntries(in_point);
	auto p_x_data = FlatVector::GetData<double>(*p_children[0]);
	auto p_y_data = FlatVector::GetData<double>(*p_children[1]);

	// Setup polygon vectors
	auto polygon_entries = ListVector::GetData(in_polygon);
	auto &ring_vec = ListVector::GetEntry(in_polygon);
	auto ring_entries = ListVector::GetData(ring_vec);
	auto &coord_vec = ListVector::GetEntry(ring_vec);
	auto &coord_children = StructVector::GetEntries(coord_vec);
	auto x_data = FlatVector::GetData<double>(*coord_children[0]);
	auto y_data = FlatVector::GetData<double>(*coord_children[1]);

	auto result_data = FlatVector::GetData<bool>(result);

	for (idx_t polygon_idx = 0; polygon_idx < count; polygon_idx++) {
		auto polygon = polygon_entries[polygon_idx];
		auto polygon_offset = polygon.offset;
		auto polygon_length = polygon.length;
		bool first = true;

		// does the point lie inside the polygon?
		bool contains = false;

		auto x = p_x_data[polygon_idx];
		auto y = p_y_data[polygon_idx];

		for (idx_t ring_idx = polygon_offset; ring_idx < polygon_offset + polygon_length; ring_idx++) {
			auto ring = ring_entries[ring_idx];
			auto ring_offset = ring.offset;
			auto ring_length = ring.length;

			auto x1 = x_data[ring_offset];
			auto y1 = y_data[ring_offset];
			int winding_number = 0;

			for (idx_t coord_idx = ring_offset + 1; coord_idx < ring_offset + ring_length; coord_idx++) {
				// foo foo foo
				auto x2 = x_data[coord_idx];
				auto y2 = y_data[coord_idx];

				if (x1 == x2 && y1 == y2) {
					x1 = x2;
					y1 = y2;
					continue;
				}

				auto y_min = std::min(y1, y2);
				auto y_max = std::max(y1, y2);

				if (y > y_max || y < y_min) {
					x1 = x2;
					y1 = y2;
					continue;
				}

				auto side = Side::ON;
				double side_v = ((x - x1) * (y2 - y1) - (x2 - x1) * (y - y1));
				if (side_v == 0) {
					side = Side::ON;
				} else if (side_v < 0) {
					side = Side::LEFT;
				} else {
					side = Side::RIGHT;
				}

				if (side == Side::ON &&
				    (((x1 <= x && x < x2) || (x1 >= x && x > x2)) || ((y1 <= y && y < y2) || (y1 >= y && y > y2)))) {

					// return Contains::ON_EDGE;
					contains = false;
					break;
				} else if (side == Side::LEFT && (y1 < y && y <= y2)) {
					winding_number++;
				} else if (side == Side::RIGHT && (y2 <= y && y < y1)) {
					winding_number--;
				}

				x1 = x2;
				y1 = y2;
			}
			bool in_ring = winding_number != 0;
			if (first) {
				if (!in_ring) {
					// if the first ring is not inside, then the point is not inside the polygon
					contains = false;
					break;
				} else {
					// if the first ring is inside, then the point is inside the polygon
					// but might be inside a hole, so we continue
					contains = true;
				}
			} else {
				if (in_ring) {
					// if the hole is inside, then the point is not inside the polygon
					contains = false;
					break;
				} // else continue
			}
			first = false;
		}
		result_data[polygon_idx] = contains;
	}
	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void PolygonContainsPointFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	auto count = args.size();
	auto &in_polygon = args.data[0];
	auto &in_point = args.data[1];
	PointInPolygonOperation(in_point, in_polygon, result, count);
}

static void PointWithinPolygonFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	auto count = args.size();
	auto &in_point = args.data[0];
	auto &in_polygon = args.data[1];
	PointInPolygonOperation(in_point, in_polygon, result, count);
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStContains(DatabaseInstance &db) {

	// ST_Within is the inverse of ST_Contains
	ScalarFunctionSet contains_function_set("st_contains");
	ScalarFunctionSet within_function_set("st_within");

	// POLYGON_2D - POINT_2D
	contains_function_set.AddFunction(ScalarFunction({GeoTypes::POLYGON_2D(), GeoTypes::POINT_2D()},
	                                                 LogicalType::BOOLEAN, PolygonContainsPointFunction));
	within_function_set.AddFunction(ScalarFunction({GeoTypes::POINT_2D(), GeoTypes::POLYGON_2D()}, LogicalType::BOOLEAN,
	                                               PointWithinPolygonFunction));

	ExtensionUtil::RegisterFunction(db, contains_function_set);
	ExtensionUtil::RegisterFunction(db, within_function_set);
}

} // namespace core

} // namespace spatial