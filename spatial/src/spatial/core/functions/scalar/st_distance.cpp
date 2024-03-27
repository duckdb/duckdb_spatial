#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "spatial/common.hpp"
#include "spatial/core/functions/scalar.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

//-----------------------------------------------------------------------------
// Helpers
//-----------------------------------------------------------------------------
static PointXY ClosestPointOnSegment(const PointXY &p, const PointXY &p1, const PointXY &p2) {
	// If the segment is a Vertex, then return that Vertex
	if (p1 == p2) {
		return p1;
	}
	auto n1 = ((p.x - p1.x) * (p2.x - p1.x) + (p.y - p1.y) * (p2.y - p1.y));
	auto n2 = ((p2.x - p1.x) * (p2.x - p1.x) + (p2.y - p1.y) * (p2.y - p1.y));
	auto r = n1 / n2;
	// If r is less than 0, then the Point is outside the segment in the p1 direction
	if (r <= 0) {
		return p1;
	}
	// If r is greater than 1, then the Point is outside the segment in the p2 direction
	if (r >= 1) {
		return p2;
	}
	// Interpolate between p1 and p2
	return PointXY(p1.x + r * (p2.x - p1.x), p1.y + r * (p2.y - p1.y));
}

static double DistanceToSegmentSquared(const PointXY &px, const PointXY &ax, const PointXY &bx) {
	auto point = ClosestPointOnSegment(px, ax, bx);
	auto dx = px.x - point.x;
	auto dy = px.y - point.y;
	return dx * dx + dy * dy;
}

//------------------------------------------------------------------------------
// POINT_2D - POINT_2D
//------------------------------------------------------------------------------
static void PointToPointDistanceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	auto &left = args.data[0];
	auto &right = args.data[1];
	auto count = args.size();

	left.Flatten(count);
	right.Flatten(count);

	auto &left_entries = StructVector::GetEntries(left);
	auto &right_entries = StructVector::GetEntries(right);

	auto left_x = FlatVector::GetData<double>(*left_entries[0]);
	auto left_y = FlatVector::GetData<double>(*left_entries[1]);
	auto right_x = FlatVector::GetData<double>(*right_entries[0]);
	auto right_y = FlatVector::GetData<double>(*right_entries[1]);

	auto out_data = FlatVector::GetData<double>(result);
	for (idx_t i = 0; i < count; i++) {
		out_data[i] = std::sqrt(std::pow(left_x[i] - right_x[i], 2) + std::pow(left_y[i] - right_y[i], 2));
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//------------------------------------------------------------------------------
// POINT_2D - LINESTRING_2D
//------------------------------------------------------------------------------

static void PointToLineStringDistanceOperation(Vector &in_point, Vector &in_line, Vector &result, idx_t count) {

	// Set up the point vectors
	in_point.Flatten(count);
	auto &p_children = StructVector::GetEntries(in_point);
	auto &p_x = p_children[0];
	auto &p_y = p_children[1];
	auto p_x_data = FlatVector::GetData<double>(*p_x);
	auto p_y_data = FlatVector::GetData<double>(*p_y);

	// Set up the line vectors
	in_line.Flatten(count);

	auto &inner = ListVector::GetEntry(in_line);
	auto &children = StructVector::GetEntries(inner);
	auto &x = children[0];
	auto &y = children[1];
	auto x_data = FlatVector::GetData<double>(*x);
	auto y_data = FlatVector::GetData<double>(*y);
	auto lines = ListVector::GetData(in_line);

	auto result_data = FlatVector::GetData<double>(result);
	for (idx_t i = 0; i < count; i++) {
		auto offset = lines[i].offset;
		auto length = lines[i].length;

		double min_distance = std::numeric_limits<double>::max();
		auto p = PointXY(p_x_data[i], p_y_data[i]);

		// Loop over the segments and find the closes one to the point
		for (idx_t j = 0; j < length - 1; j++) {
			auto a = PointXY(x_data[offset + j], y_data[offset + j]);
			auto b = PointXY(x_data[offset + j + 1], y_data[offset + j + 1]);

			auto distance = DistanceToSegmentSquared(p, a, b);
			if (distance < min_distance) {
				min_distance = distance;

				if (min_distance == 0) {
					break;
				}
			}
		}
		result_data[i] = std::sqrt(min_distance);
	}
	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void PointToLineStringDistanceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	auto &in_point = args.data[0];
	auto &in_line = args.data[1];
	auto count = args.size();
	PointToLineStringDistanceOperation(in_point, in_line, result, count);
}

static void LineStringToPointDistanceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	auto &in_line = args.data[0];
	auto &in_point = args.data[1];
	auto count = args.size();
	PointToLineStringDistanceOperation(in_point, in_line, result, count);
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterStDistance(DatabaseInstance &db) {
	ScalarFunctionSet distance_function_set("ST_Distance");

	distance_function_set.AddFunction(ScalarFunction({GeoTypes::POINT_2D(), GeoTypes::POINT_2D()}, LogicalType::DOUBLE,
	                                                 PointToPointDistanceFunction));
	distance_function_set.AddFunction(ScalarFunction({GeoTypes::POINT_2D(), GeoTypes::LINESTRING_2D()},
	                                                 LogicalType::DOUBLE, PointToLineStringDistanceFunction));
	distance_function_set.AddFunction(ScalarFunction({GeoTypes::LINESTRING_2D(), GeoTypes::POINT_2D()},
	                                                 LogicalType::DOUBLE, LineStringToPointDistanceFunction));

	ExtensionUtil::RegisterFunction(db, distance_function_set);
}

} // namespace core

} // namespace spatial