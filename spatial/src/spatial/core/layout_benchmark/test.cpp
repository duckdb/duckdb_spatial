#pragma once

#include "spatial/common.hpp"
#include "spatial/core/layout_benchmark/test.hpp"
#include "spatial/core/types.hpp"

#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

namespace geo {

namespace core {

static Point ClosestPointOnSegment(const Point &p, const Point &p1, const Point &p2) {
	// If the segment is a Vertex, then return that Vertex
	if (p1 == p2) {
		return p1;
	}
	double r = ((p.x - p1.x) * (p2.x - p1.x) + (p.y - p1.y) * (p2.y - p1.y)) /
	           ((p2.x - p1.x) * (p2.x - p1.x) + (p2.y - p1.y) * (p2.y - p1.y));
	// If r is less than 0, then the Vertex is outside the segment in the p1 direction
	if (r <= 0) {
		return p1;
	}
	// If r is greater than 1, then the Vertex is outside the segment in the p2 direction
	if (r >= 1) {
		return p2;
	}
	// Interpolate between p1 and p2
	return Point(p1.x + r * (p2.x - p1.x), p1.y + r * (p2.y - p1.y));
}
static double DistanceToSegmentSquared(const Point &px, const Point &ax, const Point &bx) {
	auto point = ClosestPointOnSegment(px, ax, bx);
	auto dx = px.x - point.x;
	auto dy = px.y - point.y;
	return dx * dx + dy * dy;
}

//----------------------------------------------------------------------
// WKB (WELL KNOWN BINARY) READER
//----------------------------------------------------------------------

// Super simple WKB reader that only supports reading known types in little endian

struct SimpleWKBReader {
	const char *data = nullptr;
	uint32_t cursor = 0;
	uint32_t length = 0;

	SimpleWKBReader(const char *data, uint32_t length) : data(data), length(length) {
	}

	vector<Point> ReadLine() {
		auto byte_order = ReadByte();
		D_ASSERT(byte_order == 1); // Little endian
		auto type = ReadInt();
		D_ASSERT(type == 2); // LineString
		auto num_points = ReadInt();
		D_ASSERT(num_points > 0);
		D_ASSERT(cursor + num_points * 2 * sizeof(double) <= length);
		vector<Point> result;
		for (uint32_t i = 0; i < num_points; i++) {
			auto x = ReadDouble();
			auto y = ReadDouble();
			result.emplace_back(x, y);
		}
		return result;
	}

	Point ReadPoint() {
		auto byte_order = ReadByte();
		D_ASSERT(byte_order == 1); // Little endian
		auto type = ReadInt();
		D_ASSERT(type == 1); // Point
		auto x = ReadDouble();
		auto y = ReadDouble();
		return Point(x, y);
	}

	vector<vector<Point>> ReadPolygon() {
		auto byte_order = ReadByte();
		D_ASSERT(byte_order == 1); // Little endian
		auto type = ReadInt();
		D_ASSERT(type == 3); // Polygon
		auto num_rings = ReadInt();
		D_ASSERT(num_rings > 0);
		vector<vector<Point>> result;
		for (uint32_t i = 0; i < num_rings; i++) {
			auto num_points = ReadInt();
			D_ASSERT(num_points > 0);
			D_ASSERT(cursor + num_points * 2 * sizeof(double) <= length);
			vector<Point> ring;
			for (uint32_t j = 0; j < num_points; j++) {
				auto x = ReadDouble();
				auto y = ReadDouble();
				ring.emplace_back(x, y);
			}
			result.push_back(ring);
		}
		return result;
	}

	uint8_t ReadByte() {
		D_ASSERT(cursor + sizeof(uint8_t) <= length);
		uint8_t result = data[cursor];
		cursor += sizeof(uint8_t);
		return result;
	}

	uint32_t ReadInt() {
		D_ASSERT(cursor + sizeof(uint32_t) <= length);
		// Read uint32_t in little endian
		uint32_t result = 0;
		result |= (uint32_t)data[cursor + 0] << 0 & 0x000000FF;
		result |= (uint32_t)data[cursor + 1] << 8 & 0x0000FF00;
		result |= (uint32_t)data[cursor + 2] << 16 & 0x00FF0000;
		result |= (uint32_t)data[cursor + 3] << 24 & 0xFF000000;
		cursor += sizeof(uint32_t);
		return result;
	}

	double ReadDouble() {
		D_ASSERT(cursor + sizeof(double) <= length);
		// Read double in little endian
		uint64_t result = 0;
		result |= (uint64_t)data[cursor + 0] << 0 & 0x00000000000000FF;
		result |= (uint64_t)data[cursor + 1] << 8 & 0x000000000000FF00;
		result |= (uint64_t)data[cursor + 2] << 16 & 0x0000000000FF0000;
		result |= (uint64_t)data[cursor + 3] << 24 & 0x00000000FF000000;
		result |= (uint64_t)data[cursor + 4] << 32 & 0x000000FF00000000;
		result |= (uint64_t)data[cursor + 5] << 40 & 0x0000FF0000000000;
		result |= (uint64_t)data[cursor + 6] << 48 & 0x00FF000000000000;
		result |= (uint64_t)data[cursor + 7] << 56 & 0xFF00000000000000;
		cursor += sizeof(double);
		return *reinterpret_cast<double *>(&result);
	}
};

//----------------------------------------------------------------------
// TYPES (COLUMNAR)
//----------------------------------------------------------------------

static LogicalType GEO_POINT_2D_C = LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}});

static LogicalType GEO_POINT_3D_C =
    LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}, {"z", LogicalType::DOUBLE}});

static LogicalType GEO_POINT_4D_C = LogicalType::STRUCT(
    {{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}, {"z", LogicalType::DOUBLE}, {"m", LogicalType::DOUBLE}});

static LogicalType GEO_LINE_2D_C = LogicalType::LIST(GEO_POINT_2D_C);

static LogicalType GEO_POLYGON_2D_C = LogicalType::LIST(GEO_LINE_2D_C);

static void CreatePoint2D_C(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	auto count = args.size();

	auto &x = args.data[0];
	auto &y = args.data[1];

	x.Flatten(count);
	y.Flatten(count);

	auto &children = StructVector::GetEntries(result);
	auto &x_child = children[0];
	auto &y_child = children[1];

	x_child->Reference(x);
	y_child->Reference(y);

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void CreatePoint3D_C(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 3);
	auto count = args.size();

	auto &x = args.data[0];
	auto &y = args.data[1];
	auto &z = args.data[2];

	x.Flatten(count);
	y.Flatten(count);
	z.Flatten(count);

	auto &children = StructVector::GetEntries(result);
	auto &x_child = children[0];
	auto &y_child = children[1];
	auto &z_child = children[2];

	x_child->Reference(x);
	y_child->Reference(y);
	z_child->Reference(z);

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void CreatePoint4D_C(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 4);
	auto count = args.size();

	auto &x = args.data[0];
	auto &y = args.data[1];
	auto &z = args.data[2];
	auto &m = args.data[3];

	x.Flatten(count);
	y.Flatten(count);
	z.Flatten(count);
	m.Flatten(count);

	auto &children = StructVector::GetEntries(result);
	auto &x_child = children[0];
	auto &y_child = children[1];
	auto &z_child = children[2];
	auto &m_child = children[3];

	x_child->Reference(x);
	y_child->Reference(y);
	z_child->Reference(z);
	m_child->Reference(m);

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

/// point casts
static bool CastPointTo2D_C(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &children = StructVector::GetEntries(source);
	auto &x_child = children[0];
	auto &y_child = children[1];

	auto &result_children = StructVector::GetEntries(result);
	auto &result_x_child = result_children[0];
	auto &result_y_child = result_children[1];
	result_x_child->Reference(*x_child);
	result_y_child->Reference(*y_child);

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return true;
}

static void CreateLine_2D_C(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto count = args.size();
	auto &wkb_blobs = args.data[0];
	wkb_blobs.Flatten(count);

	auto &inner = ListVector::GetEntry(result);
	auto lines = ListVector::GetData(result);

	auto wkb_data = FlatVector::GetData<string_t>(wkb_blobs);

	idx_t total_size = 0;
	for (idx_t i = 0; i < count; i++) {
		auto wkb = wkb_data[i];
		auto wkb_ptr = wkb.GetDataUnsafe();
		auto wkb_size = wkb.GetSize();

		SimpleWKBReader reader(wkb_ptr, wkb_size);
		auto line = reader.ReadLine();
		auto line_size = line.size();

		lines[i].offset = total_size;
		lines[i].length = line_size;

		ListVector::Reserve(result, total_size + line_size);

		// Since ListVector::Reserve potentially reallocates, we need to re-fetch the inner vector pointers
		auto &children = StructVector::GetEntries(inner);
		auto &x_child = children[0];
		auto &y_child = children[1];
		auto x_data = FlatVector::GetData<double>(*x_child);
		auto y_data = FlatVector::GetData<double>(*y_child);

		for (idx_t j = 0; j < line_size; j++) {
			x_data[total_size + j] = line[j].x;
			y_data[total_size + j] = line[j].y;
		}

		total_size += line_size;
	}

	ListVector::SetListSize(result, total_size);

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void CreatePolygon_2D_C(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto count = args.size();

	// Set up input data
	auto &wkb_blobs = args.data[0];
	wkb_blobs.Flatten(count);
	auto wkb_data = FlatVector::GetData<string_t>(wkb_blobs);

	// Set up output data
	auto &ring_vec = ListVector::GetEntry(result);
	auto polygons = ListVector::GetData(result);

	idx_t total_ring_count = 0;
	idx_t total_point_count = 0;

	for (idx_t i = 0; i < count; i++) {
		auto wkb = wkb_data[i];
		auto wkb_ptr = wkb.GetDataUnsafe();
		auto wkb_size = wkb.GetSize();

		SimpleWKBReader reader(wkb_ptr, wkb_size);
		auto polygon = reader.ReadPolygon();
		auto ring_count = polygon.size();

		polygons[i].offset = total_ring_count;
		polygons[i].length = ring_count;

		ListVector::Reserve(result, total_ring_count + ring_count);
		// Since ListVector::Reserve potentially reallocates, we need to re-fetch the inner vector pointers

		for (idx_t j = 0; j < ring_count; j++) {
			auto ring = polygon[j];
			auto point_count = ring.size();

			ListVector::Reserve(ring_vec, total_point_count + point_count);
			auto ring_entries = ListVector::GetData(ring_vec);
			auto &inner = ListVector::GetEntry(ring_vec);

			auto &children = StructVector::GetEntries(inner);
			auto &x_child = children[0];
			auto &y_child = children[1];
			auto x_data = FlatVector::GetData<double>(*x_child);
			auto y_data = FlatVector::GetData<double>(*y_child);

			for (idx_t k = 0; k < point_count; k++) {
				x_data[total_point_count + k] = ring[k].x;
				y_data[total_point_count + k] = ring[k].y;
			}

			ring_entries[total_ring_count + j].offset = total_point_count;
			ring_entries[total_ring_count + j].length = point_count;

			total_point_count += point_count;
		}
		total_ring_count += ring_count;
	}

	ListVector::SetListSize(result, total_ring_count);
	ListVector::SetListSize(ring_vec, total_point_count);

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//----------------------------------------------------------------------
// TYPES (ROW)
//----------------------------------------------------------------------

static LogicalType GEO_POINT_2D_R = LogicalType::LIST(LogicalType::DOUBLE);

static LogicalType GEO_POINT_3D_R = LogicalType::LIST(LogicalType::DOUBLE);

static LogicalType GEO_POINT_4D_R = LogicalType::LIST(LogicalType::DOUBLE);

static LogicalType GEO_LINE_2D_R = LogicalType::LIST(GEO_POINT_2D_R);

static LogicalType GEO_POLYGON_2D_R = LogicalType::LIST(GEO_LINE_2D_R);

static void CreatePoint2D_R(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	auto count = args.size();

	auto &x = args.data[0];
	auto &y = args.data[1];

	x.Flatten(count);
	y.Flatten(count);

	auto x_data = FlatVector::GetData<double>(x);
	auto y_data = FlatVector::GetData<double>(y);

	ListVector::Reserve(result, count * 2);

	auto entries = ListVector::GetData(result);
	auto &inner = ListVector::GetEntry(result);
	auto inner_Data = FlatVector::GetData<double>(inner);

	for (idx_t i = 0; i < count; i++) {
		entries[i].offset = i * 2;
		entries[i].length = 2;
		inner_Data[i * 2] = x_data[i];
		inner_Data[i * 2 + 1] = y_data[i];
	}
	ListVector::SetListSize(result, count * 2);

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void CreatePoint3D_R(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 3);
	auto count = args.size();

	auto &x = args.data[0];
	auto &y = args.data[1];
	auto &z = args.data[2];

	x.Flatten(count);
	y.Flatten(count);
	z.Flatten(count);

	auto x_data = FlatVector::GetData<double>(x);
	auto y_data = FlatVector::GetData<double>(y);
	auto z_data = FlatVector::GetData<double>(z);

	ListVector::Reserve(result, count * 3);
	auto entries = ListVector::GetData(result);
	auto &inner = ListVector::GetEntry(result);
	auto inner_Data = FlatVector::GetData<double>(inner);
	for (idx_t i = 0; i < count; i++) {
		entries[i].offset = i * 3;
		entries[i].length = 3;
		inner_Data[i * 3] = x_data[i];
		inner_Data[i * 3 + 1] = y_data[i];
		inner_Data[i * 3 + 2] = z_data[i];
	}

	ListVector::SetListSize(result, count * 3);

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void CreatePoint4D_R(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 4);
	auto count = args.size();

	auto &x = args.data[0];
	auto &y = args.data[1];
	auto &z = args.data[2];
	auto &m = args.data[3];

	x.Flatten(count);
	y.Flatten(count);
	z.Flatten(count);
	m.Flatten(count);

	auto x_data = FlatVector::GetData<double>(x);
	auto y_data = FlatVector::GetData<double>(y);
	auto z_data = FlatVector::GetData<double>(z);
	auto m_data = FlatVector::GetData<double>(m);

	ListVector::Reserve(result, count * 4);
	auto entries = ListVector::GetData(result);
	auto &inner = ListVector::GetEntry(result);
	auto inner_Data = FlatVector::GetData<double>(inner);
	for (idx_t i = 0; i < count; i++) {
		entries[i].offset = i * 4;
		entries[i].length = 4;
		inner_Data[i * 4] = x_data[i];
		inner_Data[i * 4 + 1] = y_data[i];
		inner_Data[i * 4 + 2] = z_data[i];
		inner_Data[i * 4 + 3] = m_data[i];
	}

	ListVector::SetListSize(result, count * 4);

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void CreateLine_2D_R(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto count = args.size();
	auto &wkb_blobs = args.data[0];
	wkb_blobs.Flatten(count);

	auto line_entries = ListVector::GetData(result);
	auto &coord_vec = ListVector::GetEntry(result);

	auto wkb_data = FlatVector::GetData<string_t>(wkb_blobs);

	idx_t total_coords_size = 0;
	idx_t total_coords_data_size = 0;

	for (idx_t i = 0; i < count; i++) {
		auto wkb = wkb_data[i];
		auto wkb_ptr = wkb.GetDataUnsafe();
		auto wkb_size = wkb.GetSize();
		SimpleWKBReader reader(wkb_ptr, wkb_size);
		auto line_geom = reader.ReadLine();

		auto offset = total_coords_size;
		auto length = line_geom.size();

		line_entries[i].offset = offset;
		line_entries[i].length = length;

		total_coords_size += length;

		ListVector::Reserve(result, total_coords_size);
		ListVector::Reserve(coord_vec, total_coords_data_size + length * 2);

		auto coord_entries = ListVector::GetData(coord_vec);
		auto &coord_inner = ListVector::GetEntry(coord_vec);
		auto coord_inner_data = FlatVector::GetData<double>(coord_inner);

		for (idx_t j = 0; j < length; j++) {
			auto coord = line_geom[j];
			coord_entries[offset + j].offset = total_coords_data_size;
			coord_entries[offset + j].length = 2;
			coord_inner_data[total_coords_data_size] = coord.x;
			coord_inner_data[total_coords_data_size + 1] = coord.y;
			total_coords_data_size += 2;
		}
	}

	ListVector::SetListSize(result, total_coords_size);
	ListVector::SetListSize(coord_vec, total_coords_data_size);

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void CreatePolygon_2D_R(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);
	auto count = args.size();

	// Set up input data
	auto &wkb_blobs = args.data[0];
	wkb_blobs.Flatten(count);
	auto wkb_data = FlatVector::GetData<string_t>(wkb_blobs);

	// Set up output data
	auto &ring_vec = ListVector::GetEntry(result);
	auto &coords_vec = ListVector::GetEntry(ring_vec);
	auto polygons = ListVector::GetData(result);

	idx_t total_ring_count = 0;
	idx_t total_point_count = 0;
	idx_t total_coord_count = 0;

	for (idx_t i = 0; i < count; i++) {
		auto wkb = wkb_data[i];
		auto wkb_ptr = wkb.GetDataUnsafe();
		auto wkb_size = wkb.GetSize();

		SimpleWKBReader reader(wkb_ptr, wkb_size);
		auto polygon = reader.ReadPolygon();
		auto ring_count = polygon.size();

		polygons[i].offset = total_ring_count;
		polygons[i].length = ring_count;

		ListVector::Reserve(result, total_ring_count + ring_count);
		// Since ListVector::Reserve potentially reallocates, we need to re-fetch the inner vector pointers

		for (idx_t j = 0; j < ring_count; j++) {
			auto ring = polygon[j];
			auto point_count = ring.size();

			ListVector::Reserve(ring_vec, total_point_count + point_count);
			auto ring_entries = ListVector::GetData(ring_vec);
			ListVector::Reserve(coords_vec, total_coord_count + point_count * 2);
			auto coords_entries = ListVector::GetData(coords_vec);
			auto &coord_inner = ListVector::GetEntry(coords_vec);
			auto coord_inner_data = FlatVector::GetData<double>(coord_inner);

			for (idx_t k = 0; k < point_count; k++) {
				auto point = ring[k];
				coords_entries[total_point_count + k].offset = total_coord_count;
				coords_entries[total_point_count + k].length = 2;
				coord_inner_data[total_coord_count] = point.x;
				coord_inner_data[total_coord_count + 1] = point.y;
				total_coord_count += 2;
			}

			ring_entries[total_ring_count + j].offset = total_point_count;
			ring_entries[total_ring_count + j].length = point_count;

			total_point_count += point_count;
		}
		total_ring_count += ring_count;
	}
	ListVector::SetListSize(result, total_ring_count);
	ListVector::SetListSize(ring_vec, total_point_count);
	ListVector::SetListSize(coords_vec, total_coord_count);

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//----------------------------------------------------------------------
// Distance2D COLUMNAR
//----------------------------------------------------------------------
static void Distance2D_C(Vector &left, Vector &right, Vector &out, idx_t count) {
	/*
	using POINT_2D_TYPE = StructTypeBinary<double, double>;
	GenericExecutor::ExecuteBinary<POINT_2D_TYPE, POINT_2D_TYPE, PrimitiveType<double>>(
	    left, right, out, count, [](POINT_2D_TYPE &left, POINT_2D_TYPE &right) {
	    return sqrt(pow(left.a_val - right.a_val, 2) + pow(left.b_val - right.b_val, 2));
	});
	 */
	left.Flatten(count);
	right.Flatten(count);

	auto &left_entries = StructVector::GetEntries(left);
	auto &right_entries = StructVector::GetEntries(right);

	auto left_x = FlatVector::GetData<double>(*left_entries[0]);
	auto left_y = FlatVector::GetData<double>(*left_entries[1]);
	auto right_x = FlatVector::GetData<double>(*right_entries[0]);
	auto right_y = FlatVector::GetData<double>(*right_entries[1]);

	auto out_data = FlatVector::GetData<double>(out);
	for (idx_t i = 0; i < count; i++) {
		out_data[i] = sqrt(pow(left_x[i] - right_x[i], 2) + pow(left_y[i] - right_y[i], 2));
	}
}

static void Distance2DFunction_C(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	Distance2D_C(args.data[0], args.data[1], result, args.size());
}

//----------------------------------------------------------------------
// Distance2D ROW
//----------------------------------------------------------------------
static void Distance2D_R(Vector &left, Vector &right, Vector &out, idx_t count) {

	// TODO:
	right.Flatten(count);
	left.Flatten(count);

	auto &l_inner = ListVector::GetEntry(left);
	auto &r_inner = ListVector::GetEntry(right);

	auto l_data = FlatVector::GetData<double>(l_inner);
	auto r_data = FlatVector::GetData<double>(r_inner);

	auto l_entries = ListVector::GetData(left);
	auto r_entries = ListVector::GetData(right);

	auto result_data = FlatVector::GetData<double>(out);

	for (idx_t i = 0; i < count; i++) {
		auto l_offset = l_entries[i].offset;
		auto r_offset = r_entries[i].offset;

		auto lx = l_data[l_offset];
		auto ly = l_data[l_offset + 1];
		auto rx = r_data[r_offset];
		auto ry = r_data[r_offset + 1];

		result_data[i] = sqrt(pow(lx - rx, 2) + pow(ly - ry, 2));
	}

	if (count == 1) {
		out.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void Distance2DFunction_R(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	Distance2D_R(args.data[0], args.data[1], result, args.size());
}

static unique_ptr<FunctionData> BindDistance2D_R(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	bool left_ok = false;
	if (arguments[0]->return_type == GEO_POINT_2D_R || arguments[0]->return_type == GEO_POINT_3D_R ||
	    arguments[0]->return_type == GEO_POINT_4D_R) {
		left_ok = true;
	}

	bool right_ok = false;
	if (arguments[1]->return_type == GEO_POINT_2D_R || arguments[1]->return_type == GEO_POINT_3D_R ||
	    arguments[1]->return_type == GEO_POINT_4D_R) {
		right_ok = true;
	}

	if (!left_ok || !right_ok) {
		throw BinderException("Invalid arguments for Distance2D_R: %s, %s", arguments[0]->return_type.ToString(),
		                      arguments[1]->return_type.ToString());
	}
	return nullptr;
}

//----------------------------------------------------------------------
// Length2D COLUMN
//----------------------------------------------------------------------
static void LengthFunction_2D_C(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto &input = args.data[0];
	auto count = args.size();
	input.Flatten(count);

	auto &inner = ListVector::GetEntry(input);
	auto &children = StructVector::GetEntries(inner);
	auto &x = children[0];
	auto &y = children[1];
	auto x_data = FlatVector::GetData<double>(*x);
	auto y_data = FlatVector::GetData<double>(*y);
	auto lines = ListVector::GetData(input);

	auto result_data = FlatVector::GetData<double>(result);
	for (idx_t i = 0; i < count; i++) {
		auto offset = lines[i].offset;
		auto length = lines[i].length;
		double sum = 0;

		// Loop over the segments
		for (idx_t j = 0; j < length - 1; j++) {
			auto x1 = x_data[offset + j];
			auto y1 = y_data[offset + j];
			auto x2 = x_data[offset + j + 1];
			auto y2 = y_data[offset + j + 1];
			sum += sqrt(pow(x1 - x2, 2) + pow(y1 - y2, 2));
		}

		result_data[i] = sum;
	}
	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//----------------------------------------------------------------------
// Line To Point Distance COLUMN
//----------------------------------------------------------------------

static void LinePointDistance_2D_C(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	auto count = args.size();

	// Set up the point vectors
	auto &point_input = args.data[0];
	point_input.Flatten(count);
	auto &p_children = StructVector::GetEntries(point_input);
	auto &p_x = p_children[0];
	auto &p_y = p_children[1];
	auto p_x_data = FlatVector::GetData<double>(*p_x);
	auto p_y_data = FlatVector::GetData<double>(*p_y);

	// Set up the line vectors
	auto &line_input = args.data[1];
	line_input.Flatten(count);

	auto &inner = ListVector::GetEntry(line_input);
	auto &children = StructVector::GetEntries(inner);
	auto &x = children[0];
	auto &y = children[1];
	auto x_data = FlatVector::GetData<double>(*x);
	auto y_data = FlatVector::GetData<double>(*y);
	auto lines = ListVector::GetData(line_input);

	auto result_data = FlatVector::GetData<double>(result);
	for (idx_t i = 0; i < count; i++) {
		auto offset = lines[i].offset;
		auto length = lines[i].length;

		double min_distance = std::numeric_limits<double>::max();
		auto p = Point(p_x_data[i], p_y_data[i]);

		// Loop over the segments and find the closes one to the point
		for (idx_t j = 0; j < length - 1; j++) {
			auto a = Point(x_data[offset + j], y_data[offset + j]);
			auto b = Point(x_data[offset + j + 1], y_data[offset + j + 1]);

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

//----------------------------------------------------------------------
// Line To Point Distance ROW
//----------------------------------------------------------------------
static void LinePointDistance_2D_R(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	auto count = args.size();

	// Set up the point vectors
	auto &point_input = args.data[0];
	point_input.Flatten(count);
	auto point_entries = ListVector::GetData(point_input);
	auto point_data_vec = ListVector::GetEntry(point_input);
	auto point_data = FlatVector::GetData<double>(point_data_vec);

	// Set up line vectors
	auto &line_input = args.data[1];
	line_input.Flatten(count);

	auto &coord_vec = ListVector::GetEntry(line_input);
	auto line_entries = ListVector::GetData(line_input);
	auto coord_entries = ListVector::GetData(coord_vec);
	auto coord_data_vec = ListVector::GetEntry(coord_vec);
	auto coord_data = FlatVector::GetData<double>(coord_data_vec);

	auto result_data = FlatVector::GetData<double>(result);
	for (idx_t i = 0; i < count; i++) {
		auto offset = line_entries[i].offset;
		auto length = line_entries[i].length;
		double sum = 0;

		auto p = Point(point_data[point_entries[i].offset], point_data[point_entries[i].offset + 1]);

		double min_distance = std::numeric_limits<double>::max();
		// Loop over the segments
		for (idx_t j = 0; j < length - 1; j++) {
			auto x1 = coord_data[coord_entries[offset + j].offset];
			auto y1 = coord_data[coord_entries[offset + j].offset + 1];
			auto x2 = coord_data[coord_entries[offset + j + 1].offset];
			auto y2 = coord_data[coord_entries[offset + j + 1].offset + 1];

			auto a = Point(x1, y1);
			auto b = Point(x2, y2);

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

//----------------------------------------------------------------------
// Length2D ROW
//----------------------------------------------------------------------
static void LengthFunction_2D_R(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto &input = args.data[0];
	auto count = args.size();
	input.Flatten(count);

	auto &coord_vec = ListVector::GetEntry(input);
	auto line_entries = ListVector::GetData(input);
	auto coord_entries = ListVector::GetData(coord_vec);
	auto coord_data_vec = ListVector::GetEntry(coord_vec);
	auto coord_data = FlatVector::GetData<double>(coord_data_vec);

	auto result_data = FlatVector::GetData<double>(result);
	for (idx_t i = 0; i < count; i++) {
		auto offset = line_entries[i].offset;
		auto length = line_entries[i].length;
		double sum = 0;

		// Loop over the segments
		for (idx_t j = 0; j < length - 1; j++) {
			auto x1 = coord_data[coord_entries[offset + j].offset];
			auto y1 = coord_data[coord_entries[offset + j].offset + 1];
			auto x2 = coord_data[coord_entries[offset + j + 1].offset];
			auto y2 = coord_data[coord_entries[offset + j + 1].offset + 1];
			sum += sqrt(pow(x1 - x2, 2) + pow(y1 - y2, 2));
		}

		result_data[i] = sum;
	}
	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//----------------------------------------------------------------------
// POINT IN POLYGON (COLUMN)
//----------------------------------------------------------------------

//----------------------------------------------------------------------
// POLYGON AREA (ROW)
//----------------------------------------------------------------------
static void PointInPolygon_R(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);

	auto count = args.size();
	auto &in_point = args.data[0];
	auto &in_polygon = args.data[1];

	in_polygon.Flatten(count);
	in_point.Flatten(count);

	// Setup point vectors
	auto point_entries = ListVector::GetData(in_point);
	auto point_data = FlatVector::GetData<double>(ListVector::GetEntry(in_point));

	// Setup polygon vectors
	auto polygon_entries = ListVector::GetData(in_polygon);
	auto &ring_vec = ListVector::GetEntry(in_polygon);
	auto ring_entries = ListVector::GetData(ring_vec);
	auto &coord_vec = ListVector::GetEntry(ring_vec);
	auto coord_entries = ListVector::GetData(coord_vec);
	auto coord_data = FlatVector::GetData<double>(ListVector::GetEntry(coord_vec));

	auto result_data = FlatVector::GetData<bool>(result);

	for (idx_t polygon_idx = 0; polygon_idx < count; polygon_idx++) {
		auto polygon = polygon_entries[polygon_idx];
		auto polygon_offset = polygon.offset;
		auto polygon_length = polygon.length;
		bool first = true;

		// does the point lie inside the polygon?
		bool contains = false;

		auto x = point_data[point_entries[polygon_idx].offset];
		auto y = point_data[point_entries[polygon_idx].offset + 1];

		for (idx_t ring_idx = polygon_offset; ring_idx < polygon_offset + polygon_length; ring_idx++) {
			auto ring = ring_entries[ring_idx];
			auto ring_offset = ring.offset;
			auto ring_length = ring.length;

			auto x1 = coord_data[coord_entries[ring_offset].offset];
			auto y1 = coord_data[coord_entries[ring_offset].offset + 1];

			int winding_number = 0;

			for (idx_t coord_idx = ring_offset + 1; coord_idx < ring_offset + ring_length; coord_idx++) {
				// foo foo foo
				auto x2 = coord_data[coord_entries[coord_idx].offset];
				auto y2 = coord_data[coord_entries[coord_idx].offset + 1];

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

//----------------------------------------------------------------------
// POINT IN POLYGON (ROW)
//----------------------------------------------------------------------
static void AreaFunction_2D_R(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 1);

	auto count = args.size();
	auto &in_polygon = args.data[0];

	in_polygon.Flatten(count);

	// Setup polygon vectors
	auto polygon_entries = ListVector::GetData(in_polygon);
	auto &ring_vec = ListVector::GetEntry(in_polygon);
	auto ring_entries = ListVector::GetData(ring_vec);
	auto &coord_vec = ListVector::GetEntry(ring_vec);
	auto coord_entries = ListVector::GetData(coord_vec);
	auto coord_data = FlatVector::GetData<double>(ListVector::GetEntry(coord_vec));

	auto result_data = FlatVector::GetData<double>(result);

	for (idx_t polygon_idx = 0; polygon_idx < count; polygon_idx++) {
		auto polygon = polygon_entries[polygon_idx];
		auto polygon_offset = polygon.offset;
		auto polygon_length = polygon.length;
		bool first = true;
		auto area = 0.0;

		for (idx_t ring_idx = polygon_offset; ring_idx < polygon_offset + polygon_length; ring_idx++) {
			auto ring = ring_entries[ring_idx];
			auto ring_offset = ring.offset;
			auto ring_length = ring.length;

			auto sum = 0.0;
			for (idx_t coord_idx = ring_offset; coord_idx < ring_offset + ring_length - 1; coord_idx++) {
				auto x1 = coord_data[coord_entries[ring_offset].offset];
				auto y1 = coord_data[coord_entries[ring_offset].offset + 1];
				auto x2 = coord_data[coord_entries[ring_offset + 1].offset];
				auto y2 = coord_data[coord_entries[ring_offset + 1].offset + 1];
				sum += (x2 - x1) * (y2 + y1);
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
		result_data[polygon_idx] = area;
	}
	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//----------------------------------------------------------------------
// REGISTER
//----------------------------------------------------------------------
void LayoutBenchmark::Register(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &config = DBConfig::GetConfig(context);
	auto &casts = config.GetCastFunctions();

	// GEO_POINT_2D_C
	auto geo_point_2d_c = CreateTypeInfo("GEO_POINT_2D_C", GEO_POINT_2D_C);
	geo_point_2d_c.temporary = true;
	geo_point_2d_c.internal = true;
	GEO_POINT_2D_C.SetAlias("GEO_POINT_2D_C");
	auto geo_point_2d_c_entry = (TypeCatalogEntry *)catalog.CreateType(context, &geo_point_2d_c);
	LogicalType::SetCatalog(GEO_POINT_2D_C, geo_point_2d_c_entry);

	// GEO_POINT_3D_C
	auto geo_point_3d_c = CreateTypeInfo("GEO_POINT_3D_C", GEO_POINT_3D_C);
	geo_point_3d_c.temporary = true;
	geo_point_3d_c.internal = true;
	GEO_POINT_3D_C.SetAlias("GEO_POINT_3D_C");
	auto geo_point_3d_c_entry = (TypeCatalogEntry *)catalog.CreateType(context, &geo_point_3d_c);
	LogicalType::SetCatalog(GEO_POINT_3D_C, geo_point_3d_c_entry);

	// GEO_POINT_4D_C
	auto geo_point_4d_c = CreateTypeInfo("GEO_POINT_4D_C", GEO_POINT_4D_C);
	geo_point_4d_c.temporary = true;
	geo_point_4d_c.internal = true;
	GEO_POINT_4D_C.SetAlias("GEO_POINT_4D_C");
	auto geo_point_4d_c_entry = (TypeCatalogEntry *)catalog.CreateType(context, &geo_point_4d_c);
	LogicalType::SetCatalog(GEO_POINT_4D_C, geo_point_4d_c_entry);

	auto geo_line_2d_c = CreateTypeInfo("GEO_LINE_2D_C", GEO_LINE_2D_C);
	geo_line_2d_c.temporary = true;
	geo_line_2d_c.internal = true;
	GEO_LINE_2D_C.SetAlias("GEO_LINE_2D_C");
	auto geo_line_2d_c_entry = (TypeCatalogEntry *)catalog.CreateType(context, &geo_line_2d_c);
	LogicalType::SetCatalog(GEO_LINE_2D_C, geo_line_2d_c_entry);

	// GEO_POINT_2D_R
	auto geo_point_2d_r = CreateTypeInfo("GEO_POINT_2D_R", GEO_POINT_2D_R);
	geo_point_2d_r.temporary = true;
	geo_point_2d_r.internal = true;
	GEO_POINT_2D_R.SetAlias("GEO_POINT_2D_R");
	auto geo_point_2d_r_entry = (TypeCatalogEntry *)catalog.CreateType(context, &geo_point_2d_r);
	LogicalType::SetCatalog(GEO_POINT_2D_R, geo_point_2d_r_entry);

	// GEO_POINT_3D_R
	auto geo_point_3d_r = CreateTypeInfo("GEO_POINT_3D_R", GEO_POINT_3D_R);
	geo_point_3d_r.temporary = true;
	geo_point_3d_r.internal = true;
	GEO_POINT_3D_R.SetAlias("GEO_POINT_3D_R");
	auto geo_point_3d_r_entry = (TypeCatalogEntry *)catalog.CreateType(context, &geo_point_3d_r);
	LogicalType::SetCatalog(GEO_POINT_3D_R, geo_point_3d_r_entry);

	// GEO_POINT_4D_R
	auto geo_point_4d_r = CreateTypeInfo("GEO_POINT_4D_R", GEO_POINT_4D_R);
	geo_point_4d_r.temporary = true;
	geo_point_4d_r.internal = true;
	GEO_POINT_4D_R.SetAlias("GEO_POINT_4D_R");
	auto geo_point_4d_r_entry = (TypeCatalogEntry *)catalog.CreateType(context, &geo_point_4d_r);
	LogicalType::SetCatalog(GEO_POINT_4D_R, geo_point_4d_r_entry);

	// GEO_POLYGON_2D_C
	auto geo_polygon_2d_c = CreateTypeInfo("GEO_POLYGON_2D_C", GEO_POLYGON_2D_C);
	geo_polygon_2d_c.temporary = true;
	geo_polygon_2d_c.internal = true;
	GEO_POLYGON_2D_C.SetAlias("GEO_POLYGON_2D_C");
	auto geo_polygon_2d_c_entry = (TypeCatalogEntry *)catalog.CreateType(context, &geo_polygon_2d_c);
	LogicalType::SetCatalog(GEO_POLYGON_2D_C, geo_polygon_2d_c_entry);

	// GEO_POLYGON_2D_R
	auto geo_polygon_2d_r = CreateTypeInfo("GEO_POLYGON_2D_R", GEO_POLYGON_2D_R);
	geo_polygon_2d_r.temporary = true;
	geo_polygon_2d_r.internal = true;
	GEO_POLYGON_2D_R.SetAlias("GEO_POLYGON_2D_R");
	auto geo_polygon_2d_r_entry = (TypeCatalogEntry *)catalog.CreateType(context, &geo_polygon_2d_r);
	LogicalType::SetCatalog(GEO_POLYGON_2D_R, geo_polygon_2d_r_entry);

	// functions

	/// POINTS (COLUMNS)

	// Create
	CreateScalarFunctionInfo create_point_2d_c_info(ScalarFunction(
	    "geo_create_point2d_c", {LogicalType::DOUBLE, LogicalType::DOUBLE}, GEO_POINT_2D_C, CreatePoint2D_C));
	catalog.CreateFunction(context, &create_point_2d_c_info);

	CreateScalarFunctionInfo create_point_3d_c_info(
	    ScalarFunction("geo_create_point3d_c", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	                   GEO_POINT_3D_C, CreatePoint3D_C));
	catalog.CreateFunction(context, &create_point_3d_c_info);

	CreateScalarFunctionInfo create_point_4d_c_info(ScalarFunction(
	    "geo_create_point4d_c", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	    GEO_POINT_4D_C, CreatePoint4D_C));
	catalog.CreateFunction(context, &create_point_4d_c_info);

	CreateScalarFunctionInfo create_polygon_2d_c_info(
	    ScalarFunction("geo_create_polygon2d_c", {GeoTypes::WKB_BLOB}, GEO_POLYGON_2D_C, CreatePolygon_2D_C));
	catalog.CreateFunction(context, &create_polygon_2d_c_info);

	// this is the nice thing about columnar data, we can cast effortlessly
	casts.RegisterCastFunction(GEO_POINT_3D_C, GEO_POINT_2D_C, CastPointTo2D_C, 0);
	casts.RegisterCastFunction(GEO_POINT_4D_C, GEO_POINT_2D_C, CastPointTo2D_C, 0);

	// Distance 2D
	CreateScalarFunctionInfo distance_2d_c_info(ScalarFunction("geo_distance2d_c", {GEO_POINT_2D_C, GEO_POINT_2D_C},
	                                                           LogicalType::DOUBLE, Distance2DFunction_C));
	catalog.CreateFunction(context, &distance_2d_c_info);

	CreateScalarFunctionInfo line_point_distance_2d_c_info(ScalarFunction(
	    "geo_line_point_distance2d_c", {GEO_POINT_2D_C, GEO_LINE_2D_C}, LogicalType::DOUBLE, LinePointDistance_2D_C));
	catalog.CreateFunction(context, &line_point_distance_2d_c_info);

	CreateScalarFunctionInfo polygon_area_2d_c_info(
	    ScalarFunction("geo_polygon_area2d_c", {GEO_POLYGON_2D_C}, LogicalType::DOUBLE, AreaFunction_2D_C));
	catalog.CreateFunction(context, &polygon_area_2d_c_info);

	CreateScalarFunctionInfo point_in_polygon_2d_c_info(ScalarFunction(
	    "geo_point_in_polygon2d_c", {GEO_POINT_2D_C, GEO_POLYGON_2D_C}, LogicalType::BOOLEAN, PointInPolygon_C));
	catalog.CreateFunction(context, &point_in_polygon_2d_c_info);

	/// LINES (COLUMNS)
	CreateScalarFunctionInfo create_line_2d_c_info(
	    ScalarFunction("geo_create_line2d_c", {GeoTypes::WKB_BLOB}, GEO_LINE_2D_C, CreateLine_2D_C));
	catalog.CreateFunction(context, &create_line_2d_c_info);

	/// Length2d
	CreateScalarFunctionInfo length_2d_c_info(
	    ScalarFunction("geo_length2d_c", {LogicalType::ANY}, LogicalType::DOUBLE, LengthFunction_2D_C));
	catalog.CreateFunction(context, &length_2d_c_info);

	/// POINT (ROWS)

	// Create
	CreateScalarFunctionInfo create_point_2d_r_info(ScalarFunction(
	    "geo_create_point2d_r", {LogicalType::DOUBLE, LogicalType::DOUBLE}, GEO_POINT_2D_R, CreatePoint2D_R));
	catalog.CreateFunction(context, &create_point_2d_r_info);

	CreateScalarFunctionInfo create_point_3d_r_info(
	    ScalarFunction("geo_create_point3d_r", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	                   GEO_POINT_3D_R, CreatePoint3D_R));
	catalog.CreateFunction(context, &create_point_3d_r_info);

	CreateScalarFunctionInfo create_point_4d_r_info(ScalarFunction(
	    "geo_create_point4d_r", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	    GEO_POINT_4D_R, CreatePoint4D_R));
	catalog.CreateFunction(context, &create_point_4d_r_info);

	// Distance2d
	CreateScalarFunctionInfo distance_2d_r_info(ScalarFunction("geo_distance2d_r", {LogicalType::ANY, LogicalType::ANY},
	                                                           LogicalType::DOUBLE, Distance2DFunction_R,
	                                                           BindDistance2D_R));
	catalog.CreateFunction(context, &distance_2d_r_info);

	CreateScalarFunctionInfo line_point_distance_2d_r_info(ScalarFunction(
	    "geo_line_point_distance2d_r", {GEO_POINT_2D_R, GEO_LINE_2D_R}, LogicalType::DOUBLE, LinePointDistance_2D_R));
	catalog.CreateFunction(context, &line_point_distance_2d_r_info);

	CreateScalarFunctionInfo create_polygon_2d_r_info(
	    ScalarFunction("geo_create_polygon2d_r", {GeoTypes::WKB_BLOB}, GEO_POLYGON_2D_R, CreatePolygon_2D_R));
	catalog.CreateFunction(context, &create_polygon_2d_r_info);

	/// LINES (ROWS)
	CreateScalarFunctionInfo create_line_2d_r_info(
	    ScalarFunction("geo_create_line2d_r", {GeoTypes::WKB_BLOB}, GEO_LINE_2D_R, CreateLine_2D_R));
	catalog.CreateFunction(context, &create_line_2d_r_info);

	/// Length2d
	CreateScalarFunctionInfo length_2d_r_info(
	    ScalarFunction("geo_length2d_r", {LogicalType::ANY}, LogicalType::DOUBLE, LengthFunction_2D_R));
	catalog.CreateFunction(context, &length_2d_r_info);

	/// POLYGONS (ROWS)
	CreateScalarFunctionInfo polygon_area_2d_r_info(
	    ScalarFunction("geo_polygon_area2d_r", {GEO_POLYGON_2D_R}, LogicalType::DOUBLE, AreaFunction_2D_R));
	catalog.CreateFunction(context, &polygon_area_2d_r_info);

	CreateScalarFunctionInfo point_in_polygon_2d_r_info(ScalarFunction(
	    "geo_point_in_polygon2d_r", {GEO_POINT_2D_R, GEO_POLYGON_2D_R}, LogicalType::BOOLEAN, PointInPolygon_R));
	catalog.CreateFunction(context, &point_in_polygon_2d_r_info);

	/*
	    GeoTypes::WKB_BLOB.SetAlias("WKB_BLOB");
	    auto wkb = CreateTypeInfo("WKB_BLOB", GeoTypes::WKB_BLOB);
	    wkb.internal = true;
	    wkb.temporary = true;
	    catalog.CreateType(context, &wkb);
	 */
}

} // namespace core

} // namespace spatial

// D CREATE TABLE t1 as SELECT geo_create_polygon2d_c(wkb_geometry) as geom,
// geo_create_point2d_c(st_x(st_centroid(wkb_geometry)), st_y(st_centroid(wkb_geometry))) as centroid FROM
// st_read('./geo/test/data/germany/forest/forest.fgb') LIMIT 1000;