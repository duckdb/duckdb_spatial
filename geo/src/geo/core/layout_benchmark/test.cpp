#pragma once

#include "geo/common.hpp"
#include "geo/core/layout_benchmark/test.hpp"
#include "geo/core/types.hpp"

#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

namespace geo {

namespace core {

//----------------------------------------------------------------------
// WKB (WELL KNOWN BINARY) READER
//----------------------------------------------------------------------

// Super simple WKB reader that only supports reading known types in little endian
struct SimpleWKBReader {
	const char *data = nullptr;
	uint32_t cursor = 0;
	uint32_t length = 0;

	struct Point {
		double x;
		double y;
		explicit Point(double x, double y) : x(x), y(y) { }
	};

	SimpleWKBReader(const char *data, uint32_t length) : data(data), length(length) { }

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

static LogicalType GEO_POINT_2D_C = LogicalType::STRUCT({
		{"x", LogicalType::DOUBLE},
		{"y", LogicalType::DOUBLE}
	});

static LogicalType GEO_POINT_3D_C = LogicalType::STRUCT({
		{"x", LogicalType::DOUBLE},
		{"y", LogicalType::DOUBLE},
		{"z", LogicalType::DOUBLE}
	});

static LogicalType GEO_POINT_4D_C = LogicalType::STRUCT({
		{"x", LogicalType::DOUBLE},
		{"y", LogicalType::DOUBLE},
		{"z", LogicalType::DOUBLE},
		{"m", LogicalType::DOUBLE}
	});

static LogicalType GEO_LINE_2D_C = LogicalType::LIST(GEO_POINT_2D_C);

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

	if(count == 1) {
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

	if(count == 1) {
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

	if(count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
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
	for(idx_t i = 0; i < count; i++) {
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

	if(count == 1) {
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

	if(count == 1) {
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

	if(count == 1) {
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

	if(count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void CreateLine_2D_R(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	// We read the line from WKB format
	auto count = args.size();

	auto input = args.data[0];
	input.Flatten(count);
	auto wkb_blobs = FlatVector::GetData<string_t>(input);
	idx_t data_total_size = 0;

	auto line_vec = result;
	auto line_entries = ListVector::GetData(line_vec);
	auto point_vec = ListVector::GetEntry(line_vec);
	auto point_entries = ListVector::GetData(point_vec);
	auto data_vec = ListVector::GetEntry(point_vec);
	auto data = FlatVector::GetData<double>(data_vec);

	idx_t total_data_size = 0;
	idx_t total_point_size = 0;
	for (idx_t i = 0; i < count; i++) {
		auto wkb = wkb_blobs[i];
		auto wkb_data = wkb.GetDataUnsafe();
		auto wkb_size = wkb.GetSize();

		auto wkb_reader = SimpleWKBReader(wkb_data, wkb_size);
		auto line = wkb_reader.ReadLine();
		auto line_size = line.size();

		line_entries[i].offset = total_point_size;
		line_entries[i].length = line_size;
		total_point_size += line_size;

		ListVector::Reserve(line_vec, total_point_size);

		for (idx_t j = 0; j < line_size; j++) {
			auto point = line[j];
			point_entries[total_point_size + j].offset = total_data_size;
			point_entries[total_point_size + j].length = 2;
			total_data_size += 2;

			data[total_data_size] = point.x;
			data[total_data_size + 1] = point.y;
		}

		data_total_size += total_data_size;
		ListVector::Reserve(point_vec, data_total_size);
	}

	ListVector::SetListSize(line_vec, total_point_size);
	ListVector::SetListSize(point_vec, data_total_size);
}

//----------------------------------------------------------------------
// Distance2D COLUMNAR
//----------------------------------------------------------------------
static void Distance2D_C(Vector &left, Vector &right, Vector& out, idx_t count) {
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
	for(idx_t i = 0; i < count; i++) {
		out_data[i] = sqrt(pow(left_x[i] - right_x[i], 2) + pow(left_y[i] - right_y[i], 2));
	}
}

static void Distance2DFunction_C(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	Distance2D_C(args.data[0], args.data[1], result, args.size());
}

static unique_ptr<FunctionData> BindDistance2D_C(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	// TODO: check that they are called x and y. (and store offset in bind data?)
	auto left_ok = false;
	if (arguments[0]->return_type.id() == LogicalTypeId::STRUCT) {
		auto &child_types = StructType::GetChildTypes(arguments[0]->return_type);
		if (child_types.size() >= 2) {
			if (child_types[0].second.id() == LogicalTypeId::DOUBLE &&
			    child_types[1].second.id() == LogicalTypeId::DOUBLE) {
				left_ok = true;
			}
		}
	}
	// TODO: check that they are called x and y. (and store offset in bind data?)
	auto right_ok = false;
	if (arguments[1]->return_type.id() == LogicalTypeId::STRUCT) {
		auto &child_types = StructType::GetChildTypes(arguments[1]->return_type);
		if (child_types.size() >= 2) {
			if (child_types[0].second.id() == LogicalTypeId::DOUBLE &&
			    child_types[1].second.id() == LogicalTypeId::DOUBLE) {
				right_ok = true;
			}
		}
	}
	if (!left_ok || !right_ok) {
		throw BinderException("Invalid arguments for Distance2D_C: %s, %s", arguments[0]->return_type.ToString(),
		                      arguments[1]->return_type.ToString());
	}
	return nullptr;
}

//----------------------------------------------------------------------
// Distance2D ROW
//----------------------------------------------------------------------

static void Distance2D_R(Vector &left, Vector &right, Vector& out, idx_t count) {

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

	if(count == 1) {
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
	if(arguments[0]->return_type == GEO_POINT_2D_R ||
	    arguments[0]->return_type == GEO_POINT_3D_R ||
	    arguments[0]->return_type == GEO_POINT_4D_R) {
		left_ok = true;
	}

	bool right_ok = false;
	if(arguments[1]->return_type == GEO_POINT_2D_R ||
	    arguments[1]->return_type == GEO_POINT_3D_R ||
	    arguments[1]->return_type == GEO_POINT_4D_R) {
		right_ok = true;
	}

	if(!left_ok || !right_ok) {
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
	for(idx_t i = 0; i < count; i++) {
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
	if(count == 1) {
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


	// functions

	/// POINTS (COLUMNS)

	// Create
	CreateScalarFunctionInfo create_point_2d_c_info(ScalarFunction(
	    "geo_create_point2d_c", {LogicalType::DOUBLE, LogicalType::DOUBLE}, GEO_POINT_2D_C, CreatePoint2D_C));
	catalog.CreateFunction(context, &create_point_2d_c_info);

	CreateScalarFunctionInfo create_point_3d_c_info(ScalarFunction(
	    "geo_create_point3d_c", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE}, GEO_POINT_3D_C, CreatePoint3D_C));
	catalog.CreateFunction(context, &create_point_3d_c_info);

	CreateScalarFunctionInfo create_point_4d_c_info(ScalarFunction(
	    "geo_create_point4d_c", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE}, GEO_POINT_4D_C, CreatePoint4D_C));
	catalog.CreateFunction(context, &create_point_4d_c_info);

	// Distance 2D
	CreateScalarFunctionInfo distance_2d_c_info(ScalarFunction(
	    "geo_distance2d_c", {LogicalType::ANY, LogicalType::ANY}, LogicalType::DOUBLE, Distance2DFunction_C, BindDistance2D_C));
	catalog.CreateFunction(context, &distance_2d_c_info);

	/// LINES (COLUMNS)
	CreateScalarFunctionInfo create_line_2d_c_info(ScalarFunction(
	    "geo_create_line2d_c", {GeoTypes::WKB_BLOB}, GEO_LINE_2D_C, CreateLine_2D_C));
	catalog.CreateFunction(context, &create_line_2d_c_info);

	/// Length2d
	CreateScalarFunctionInfo length_2d_c_info(ScalarFunction(
	    "geo_length2d_c", {LogicalType::ANY}, LogicalType::DOUBLE, LengthFunction_2D_C));
	catalog.CreateFunction(context, &length_2d_c_info);

	/// POINT (ROWS)

	// Create
	CreateScalarFunctionInfo create_point_2d_r_info(ScalarFunction(
	    "geo_create_point2d_r", {LogicalType::DOUBLE, LogicalType::DOUBLE}, GEO_POINT_2D_R, CreatePoint2D_R));
	catalog.CreateFunction(context, &create_point_2d_r_info);

	CreateScalarFunctionInfo create_point_3d_r_info(ScalarFunction(
	    "geo_create_point3d_r", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE}, GEO_POINT_3D_R, CreatePoint3D_R));
	catalog.CreateFunction(context, &create_point_3d_r_info);

	CreateScalarFunctionInfo create_point_4d_r_info(ScalarFunction(
	    "geo_create_point4d_r", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE}, GEO_POINT_4D_R, CreatePoint4D_R));
	catalog.CreateFunction(context, &create_point_4d_r_info);

	// Distance2d
	CreateScalarFunctionInfo distance_2d_r_info(ScalarFunction(
	    "geo_distance2d_r", {LogicalType::ANY, LogicalType::ANY}, LogicalType::DOUBLE, Distance2DFunction_R, BindDistance2D_R));
	catalog.CreateFunction(context, &distance_2d_r_info);

	/*
	 	GeoTypes::WKB_BLOB.SetAlias("WKB_BLOB");
		auto wkb = CreateTypeInfo("WKB_BLOB", GeoTypes::WKB_BLOB);
		wkb.internal = true;
		wkb.temporary = true;
		catalog.CreateType(context, &wkb);
	 */
}


} // namespace core

} // namespace geo