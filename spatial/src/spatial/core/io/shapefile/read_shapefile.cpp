#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/multi_file_reader.hpp"

#include "duckdb/function/copy_function.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/io/shapefile.hpp"
#include "spatial/core/functions/table.hpp"
#include "spatial/core/types.hpp"

#include "shapefil.h"
#include "utf8proc_wrapper.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// Bind
//------------------------------------------------------------------------------

struct ShapefileBindData : TableFunctionData {
	string file_name;
	int shape_count;
	int shape_type;
	double min_bound[4];
	double max_bound[4];
	AttributeEncoding attribute_encoding;
	vector<LogicalType> attribute_types;

	explicit ShapefileBindData(string file_name_p)
	    : file_name(std::move(file_name_p)), shape_count(0), shape_type(0), min_bound {0, 0, 0, 0},
	      max_bound {0, 0, 0, 0}, attribute_encoding(AttributeEncoding::LATIN1) {
	}
};

static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {

	auto file_name = StringValue::Get(input.inputs[0]);
	auto result = make_uniq<ShapefileBindData>(file_name);

	auto &fs = FileSystem::GetFileSystem(context);
	auto shp_handle = OpenSHPFile(fs, file_name);

	// Get info about the geometry
	SHPGetInfo(shp_handle.get(), &result->shape_count, &result->shape_type, result->min_bound, result->max_bound);

	// Ensure we have a supported shape type
	auto valid_types = {SHPT_NULL, SHPT_POINT, SHPT_ARC, SHPT_POLYGON, SHPT_MULTIPOINT};
	bool is_valid_type = false;
	for (auto type : valid_types) {
		if (result->shape_type == type) {
			is_valid_type = true;
			break;
		}
	}
	if (!is_valid_type) {
		throw InvalidInputException("Invalid shape type %d", result->shape_type);
	}

	auto base_name = file_name.substr(0, file_name.find_last_of('.'));

	// A standards compliant shapefile should use ISO-8859-1 encoding for attributes, but it can be overridden
	// by a .cpg file. So check if there is a .cpg file, if so use that to determine the encoding
	auto cpg_file = base_name + ".cpg";
	if (fs.FileExists(cpg_file)) {
		auto cpg_handle = fs.OpenFile(cpg_file, FileFlags::FILE_FLAGS_READ);
		auto cpg_type = StringUtil::Lower(cpg_handle->ReadLine());
		if (cpg_type == "utf-8") {
			result->attribute_encoding = AttributeEncoding::UTF8;
		} else if (cpg_type == "iso-8859-1") {
			result->attribute_encoding = AttributeEncoding::LATIN1;
		} else {
			// Otherwise, parse as blob
			result->attribute_encoding = AttributeEncoding::BLOB;
		}
	}

	for (auto &kv : input.named_parameters) {
		if (kv.first == "encoding") {
			auto encoding = StringUtil::Lower(StringValue::Get(kv.second));
			if (encoding == "utf-8") {
				result->attribute_encoding = AttributeEncoding::UTF8;
			} else if (encoding == "iso-8859-1") {
				result->attribute_encoding = AttributeEncoding::LATIN1;
			} else if (encoding == "blob") {
				// Otherwise, parse as blob
				result->attribute_encoding = AttributeEncoding::BLOB;
			} else {
				vector<string> candidates = {"utf-8", "iso-8859-1", "blob"};
				auto msg = StringUtil::CandidatesErrorMessage(candidates, encoding, "encoding");
				throw InvalidInputException("Invalid encoding %s", encoding.c_str());
			}
		}
		if (kv.first == "spatial_filter_box") {
			auto filter_box = StructValue::GetChildren(kv.second);
		}
	}

	// Get info about the attributes
	// Remove file extension and replace with .dbf
	auto dbf_handle = OpenDBFFile(fs, base_name + ".dbf");

	// TODO: Try to get the encoding from the dbf if there is no .cpg file
	// auto code_page = DBFGetCodePage(dbf_handle.get());
	// if(!has_cpg_file && code_page != 0) { }

	// Then return the attributes
	auto field_count = DBFGetFieldCount(dbf_handle.get());
	char field_name[12]; // Max field name length is 11 + null terminator
	int field_width = 0;
	int field_precision = 0;
	memset(field_name, 0, sizeof(field_name));

	for (int i = 0; i < field_count; i++) {
		auto field_type = DBFGetFieldInfo(dbf_handle.get(), i, field_name, &field_width, &field_precision);

		LogicalType type;
		switch (field_type) {
		case FTString:
			type = result->attribute_encoding == AttributeEncoding::BLOB ? LogicalType::BLOB : LogicalType::VARCHAR;
			break;
		case FTInteger:
			type = LogicalType::INTEGER;
			break;
		case FTDouble:
			if (field_precision == 0 && field_width < 19) {
				type = LogicalType::BIGINT;
			} else {
				type = LogicalType::DOUBLE;
			}
			break;
		case FTDate:
			// Dates are stored as 8-char strings
			// YYYYMMDD
			type = LogicalType::DATE;
			break;
		case FTLogical:
			type = LogicalType::BOOLEAN;
			break;
		default:
			throw InvalidInputException("DBF field type %d not supported", field_type);
		}
		names.emplace_back(field_name);
		return_types.push_back(type);
		result->attribute_types.push_back(type);
	}

	// Always return geometry last
	return_types.push_back(GeoTypes::GEOMETRY());
	names.push_back("geom");

	// Deduplicate field names if necessary
	for (size_t i = 0; i < names.size(); i++) {
		idx_t count = 1;
		for (size_t j = i + 1; j < names.size(); j++) {
			if (names[i] == names[j]) {
				names[j] += "_" + std::to_string(count++);
			}
		}
	}

	return std::move(result);
}

//------------------------------------------------------------------------------
// Init Global
//------------------------------------------------------------------------------

struct ShapefileGlobalState : public GlobalTableFunctionState {
	int shape_idx;
	SHPHandlePtr shp_handle;
	DBFHandlePtr dbf_handle;
	ArenaAllocator arena;
	vector<idx_t> column_ids;

	explicit ShapefileGlobalState(ClientContext &context, const string &file_name, vector<idx_t> column_ids_p)
	    : shape_idx(0), arena(BufferAllocator::Get(context)), column_ids(std::move(column_ids_p)) {
		auto &fs = FileSystem::GetFileSystem(context);

		shp_handle = OpenSHPFile(fs, file_name);

		// Remove file extension and replace with .dbf
		auto dot_idx = file_name.find_last_of('.');
		auto base_name = file_name.substr(0, dot_idx);
		dbf_handle = OpenDBFFile(fs, base_name + ".dbf");
	}
};

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ShapefileBindData>();
	auto result = make_uniq<ShapefileGlobalState>(context, bind_data.file_name, input.column_ids);
	return std::move(result);
}

//------------------------------------------------------------------------------
// Geometry Conversion
//------------------------------------------------------------------------------

struct ConvertPoint {
	static Geometry Convert(SHPObjectPtr &shape, ArenaAllocator &arena) {
		return Point::CreateFromVertex(arena, VertexXY {shape->padfX[0], shape->padfY[0]});
	}
};

struct ConvertLineString {
	static Geometry Convert(SHPObjectPtr &shape, ArenaAllocator &arena) {
		if (shape->nParts == 1) {
			// Single LineString
			auto line = LineString::Create(arena, shape->nVertices, false, false);
			for (int i = 0; i < shape->nVertices; i++) {
				LineString::SetVertex<VertexXY>(line, i, {shape->padfX[i], shape->padfY[i]});
			}
			return line;
		} else {
			// MultiLineString
			auto multi_line_string = MultiLineString::Create(arena, shape->nParts, false, false);
			auto start = shape->panPartStart[0];
			for (int i = 0; i < shape->nParts; i++) {
				auto end = i == shape->nParts - 1 ? shape->nVertices : shape->panPartStart[i + 1];
				auto line_size = end - start;
				auto &line = MultiLineString::Part(multi_line_string, i);
				LineString::Resize(line, arena, line_size);
				for (int j = 0; j < line_size; j++) {
					auto offset = start + j;
					LineString::SetVertex<VertexXY>(line, j, {shape->padfX[offset], shape->padfY[offset]});
				}
				start = end;
			}
			return multi_line_string;
		}
	}
};

struct ConvertPolygon {
	static Geometry Convert(SHPObjectPtr &shape, ArenaAllocator &arena) {
		// First off, check if there are more than one polygon.
		// Each polygon is identified by a part with clockwise winding order
		// we calculate the winding order by checking the sign of the area
		vector<int> polygon_part_starts;
		for (int i = 0; i < shape->nParts; i++) {
			auto start = shape->panPartStart[i];
			auto end = i == shape->nParts - 1 ? shape->nVertices : shape->panPartStart[i + 1];
			double area = 0;
			for (int j = start; j < end - 1; j++) {
				area += (shape->padfX[j] * shape->padfY[j + 1]) - (shape->padfX[j + 1] * shape->padfY[j]);
			}
			if (area < 0) {
				polygon_part_starts.push_back(i);
			}
		}
		if (polygon_part_starts.size() < 2) {
			// Single polygon, every part is an interior ring
			// Even if the polygon is counter-clockwise (which should not happen for shapefiles).
			// we still fall back and convert it to a single polygon.
			auto polygon = Polygon::Create(arena, shape->nParts, false, false);
			auto start = shape->panPartStart[0];
			for (int i = 0; i < shape->nParts; i++) {
				auto end = i == shape->nParts - 1 ? shape->nVertices : shape->panPartStart[i + 1];
				auto &ring = Polygon::Part(polygon, i);
				auto ring_size = end - start;
				LineString::Resize(ring, arena, ring_size);
				for (int j = 0; j < ring_size; j++) {
					auto offset = start + j;
					LineString::SetVertex<VertexXY>(ring, j, {shape->padfX[offset], shape->padfY[offset]});
				}
				start = end;
			}
			return polygon;
		} else {
			// MultiPolygon
			auto multi_polygon = MultiPolygon::Create(arena, polygon_part_starts.size(), false, false);
			for (size_t polygon_idx = 0; polygon_idx < polygon_part_starts.size(); polygon_idx++) {
				auto part_start = polygon_part_starts[polygon_idx];
				auto part_end = polygon_idx == polygon_part_starts.size() - 1 ? shape->nParts
				                                                              : polygon_part_starts[polygon_idx + 1];

				auto polygon = Polygon::Create(arena, part_end - part_start, false, false);

				for (auto ring_idx = part_start; ring_idx < part_end; ring_idx++) {
					auto start = shape->panPartStart[ring_idx];
					auto end = ring_idx == shape->nParts - 1 ? shape->nVertices : shape->panPartStart[ring_idx + 1];
					auto &ring = Polygon::Part(polygon, ring_idx - part_start);
					auto ring_size = end - start;
					LineString::Resize(ring, arena, ring_size);
					for (int j = 0; j < ring_size; j++) {
						auto offset = start + j;
						LineString::SetVertex<VertexXY>(ring, j, {shape->padfX[offset], shape->padfY[offset]});
					}
				}
				MultiPolygon::Part(multi_polygon, polygon_idx) = std::move(polygon);
			}
			return multi_polygon;
		}
	}
};

struct ConvertMultiPoint {
	static Geometry Convert(SHPObjectPtr &shape, ArenaAllocator &arena) {
		auto multi_point = MultiPoint::Create(arena, shape->nVertices, false, false);
		for (int i = 0; i < shape->nVertices; i++) {
			auto point = Point::CreateFromVertex(arena, VertexXY {shape->padfX[i], shape->padfY[i]});
			MultiPoint::Part(multi_point, i) = std::move(point);
		}
		return multi_point;
	}
};

template <class OP>
static void ConvertGeomLoop(Vector &result, int record_start, idx_t count, SHPHandle &shp_handle,
                            ArenaAllocator &arena) {
	for (idx_t result_idx = 0; result_idx < count; result_idx++) {
		auto shape = SHPObjectPtr(SHPReadObject(shp_handle, record_start++));
		if (shape->nSHPType == SHPT_NULL) {
			FlatVector::SetNull(result, result_idx, true);
		} else {
			// TODO: Handle Z and M
			FlatVector::GetData<string_t>(result)[result_idx] = Geometry::Serialize(OP::Convert(shape, arena), result);
		}
	}
}

static void ConvertGeometryVector(Vector &result, int record_start, idx_t count, SHPHandle shp_handle,
                                  ArenaAllocator &arena, int geom_type) {
	switch (geom_type) {
	case SHPT_NULL:
		FlatVector::Validity(result).SetAllInvalid(count);
		break;
	case SHPT_POINT:
		ConvertGeomLoop<ConvertPoint>(result, record_start, count, shp_handle, arena);
		break;
	case SHPT_ARC:
		ConvertGeomLoop<ConvertLineString>(result, record_start, count, shp_handle, arena);
		break;
	case SHPT_POLYGON:
		ConvertGeomLoop<ConvertPolygon>(result, record_start, count, shp_handle, arena);
		break;
	case SHPT_MULTIPOINT:
		ConvertGeomLoop<ConvertMultiPoint>(result, record_start, count, shp_handle, arena);
		break;
	default:
		throw InvalidInputException("Shape type %d not supported", geom_type);
	}
}

//------------------------------------------------------------------------------
// Attribute Conversion
//------------------------------------------------------------------------------

struct ConvertBlobAttribute {
	using TYPE = string_t;
	static string_t Convert(Vector &result, DBFHandle dbf_handle, int record_idx, int field_idx) {
		auto value = DBFReadStringAttribute(dbf_handle, record_idx, field_idx);
		return StringVector::AddString(result, const_char_ptr_cast(value));
	}
};

struct ConvertIntegerAttribute {
	using TYPE = int32_t;
	static int32_t Convert(Vector &, DBFHandle dbf_handle, int record_idx, int field_idx) {
		return DBFReadIntegerAttribute(dbf_handle, record_idx, field_idx);
	}
};

struct ConvertBigIntAttribute {
	using TYPE = int64_t;
	static int64_t Convert(Vector &, DBFHandle dbf_handle, int record_idx, int field_idx) {
		return static_cast<int64_t>(DBFReadDoubleAttribute(dbf_handle, record_idx, field_idx));
	}
};

struct ConvertDoubleAttribute {
	using TYPE = double;
	static double Convert(Vector &, DBFHandle dbf_handle, int record_idx, int field_idx) {
		return DBFReadDoubleAttribute(dbf_handle, record_idx, field_idx);
	}
};

struct ConvertDateAttribute {
	using TYPE = date_t;
	static date_t Convert(Vector &, DBFHandle dbf_handle, int record_idx, int field_idx) {
		// XBase stores dates as 8-char strings (without separators)
		// but DuckDB expects a date string with separators.
		auto value = DBFReadStringAttribute(dbf_handle, record_idx, field_idx);
		char date_with_separator[11];
		memcpy(date_with_separator, value, 4);
		date_with_separator[4] = '-';
		memcpy(date_with_separator + 5, value + 4, 2);
		date_with_separator[7] = '-';
		memcpy(date_with_separator + 8, value + 6, 2);
		date_with_separator[10] = '\0';
		return Date::FromString(date_with_separator);
	}
};

struct ConvertBooleanAttribute {
	using TYPE = bool;
	static bool Convert(Vector &result, DBFHandle dbf_handle, int record_idx, int field_idx) {
		return *DBFReadLogicalAttribute(dbf_handle, record_idx, field_idx) == 'T';
	}
};

template <class OP>
static void ConvertAttributeLoop(Vector &result, int record_start, idx_t count, DBFHandle dbf_handle, int field_idx) {
	int record_idx = record_start;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (DBFIsAttributeNULL(dbf_handle, record_idx, field_idx)) {
			FlatVector::SetNull(result, row_idx, true);
		} else {
			FlatVector::GetData<typename OP::TYPE>(result)[row_idx] =
			    OP::Convert(result, dbf_handle, record_idx, field_idx);
		}
		record_idx++;
	}
}

static void ConvertStringAttributeLoop(Vector &result, int record_start, idx_t count, DBFHandle dbf_handle,
                                       int field_idx, AttributeEncoding attribute_encoding) {
	int record_idx = record_start;
	vector<data_t> conversion_buffer;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (DBFIsAttributeNULL(dbf_handle, record_idx, field_idx)) {
			FlatVector::SetNull(result, row_idx, true);
		} else {
			auto string_bytes = DBFReadStringAttribute(dbf_handle, record_idx, field_idx);
			string_t result_str;
			if (attribute_encoding == AttributeEncoding::LATIN1) {
				conversion_buffer.reserve(strlen(string_bytes) * 2 + 1); // worst case (all non-ascii chars)
				auto out_len =
				    EncodingUtil::LatinToUTF8Buffer(const_data_ptr_cast(string_bytes), conversion_buffer.data());
				result_str = StringVector::AddString(result, const_char_ptr_cast(conversion_buffer.data()), out_len);
			} else {
				result_str = StringVector::AddString(result, const_char_ptr_cast(string_bytes));
			}
			if (!Utf8Proc::IsValid(result_str.GetDataUnsafe(), result_str.GetSize())) {
				throw InvalidInputException("Could not decode VARCHAR field as valid UTF-8, try passing "
				                            "encoding='blob' to skip decoding of string attributes");
			}
			FlatVector::GetData<string_t>(result)[row_idx] = result_str;
		}
		record_idx++;
	}
}

static void ConvertAttributeVector(Vector &result, int record_start, idx_t count, DBFHandle dbf_handle, int field_idx,
                                   AttributeEncoding attribute_encoding) {
	switch (result.GetType().id()) {
	case LogicalTypeId::BLOB:
		ConvertAttributeLoop<ConvertBlobAttribute>(result, record_start, count, dbf_handle, field_idx);
		break;
	case LogicalTypeId::VARCHAR:
		ConvertStringAttributeLoop(result, record_start, count, dbf_handle, field_idx, attribute_encoding);
		break;
	case LogicalTypeId::INTEGER:
		ConvertAttributeLoop<ConvertIntegerAttribute>(result, record_start, count, dbf_handle, field_idx);
		break;
	case LogicalTypeId::BIGINT:
		ConvertAttributeLoop<ConvertBigIntAttribute>(result, record_start, count, dbf_handle, field_idx);
		break;
	case LogicalTypeId::DOUBLE:
		ConvertAttributeLoop<ConvertDoubleAttribute>(result, record_start, count, dbf_handle, field_idx);
		break;
	case LogicalTypeId::DATE:
		ConvertAttributeLoop<ConvertDateAttribute>(result, record_start, count, dbf_handle, field_idx);
		break;
	case LogicalTypeId::BOOLEAN:
		ConvertAttributeLoop<ConvertBooleanAttribute>(result, record_start, count, dbf_handle, field_idx);
		break;
	default:
		throw InvalidInputException("Attribute type %s not supported", result.GetType().ToString());
	}
}

//------------------------------------------------------------------------------
// Execute
//------------------------------------------------------------------------------

static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<ShapefileBindData>();
	auto &gstate = input.global_state->Cast<ShapefileGlobalState>();

	// Reset the buffer allocator
	gstate.arena.Reset();

	// Calculate how many record we can fit in the output
	auto output_size = std::min<int>(STANDARD_VECTOR_SIZE, bind_data.shape_count - gstate.shape_idx);
	int record_start = gstate.shape_idx;
	for (auto col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {

		// Projected column indices
		auto projected_col_idx = gstate.column_ids[col_idx];

		auto &col_vec = output.data[col_idx];
		if (col_vec.GetType() == GeoTypes::GEOMETRY()) {
			ConvertGeometryVector(col_vec, record_start, output_size, gstate.shp_handle.get(), gstate.arena,
			                      bind_data.shape_type);
		} else {
			// The geometry is always last, so we can use the projected column index directly
			auto field_idx = projected_col_idx;
			ConvertAttributeVector(col_vec, record_start, output_size, gstate.dbf_handle.get(), (int)field_idx,
			                       bind_data.attribute_encoding);
		}
	}
	// Update the shape index
	gstate.shape_idx += output_size;

	// Set the cardinality of the output
	output.SetCardinality(output_size);
}

//------------------------------------------------------------------------------
// Progress, Cardinality and Replacement Scans
//------------------------------------------------------------------------------

static double GetProgress(ClientContext &context, const FunctionData *bind_data_p,
                          const GlobalTableFunctionState *global_state) {

	auto &gstate = global_state->Cast<ShapefileGlobalState>();
	auto &bind_data = bind_data_p->Cast<ShapefileBindData>();

	return (double)gstate.shape_idx / (double)bind_data.shape_count;
}

static unique_ptr<NodeStatistics> GetCardinality(ClientContext &context, const FunctionData *data) {
	auto &bind_data = data->Cast<ShapefileBindData>();
	auto result = make_uniq<NodeStatistics>();

	// This is the maximum number of shapes in a single file
	result->has_max_cardinality = true;
	result->max_cardinality = bind_data.shape_count;

	return result;
}

static unique_ptr<TableRef> GetReplacementScan(ClientContext &context, ReplacementScanInput &input,
                                               optional_ptr<ReplacementScanData> data) {
	auto &table_name = input.table_name;
	// Check if the table name ends with .shp
	if (!StringUtil::EndsWith(StringUtil::Lower(table_name), ".shp")) {
		return nullptr;
	}

	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	table_function->function = make_uniq<FunctionExpression>("ST_ReadSHP", std::move(children));
	return std::move(table_function);
}

//------------------------------------------------------------------------------
// Register table function
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterShapefileTableFunction(DatabaseInstance &db) {
	TableFunction read_func("ST_ReadSHP", {LogicalType::VARCHAR}, Execute, Bind, InitGlobal);

	read_func.named_parameters["encoding"] = LogicalType::VARCHAR;
	read_func.table_scan_progress = GetProgress;
	read_func.cardinality = GetCardinality;
	read_func.projection_pushdown = true;
	ExtensionUtil::RegisterFunction(db, read_func);

	// Replacement scan
	auto &config = DBConfig::GetConfig(db);
	config.replacement_scans.emplace_back(GetReplacementScan);
}

} // namespace core

} // namespace spatial
