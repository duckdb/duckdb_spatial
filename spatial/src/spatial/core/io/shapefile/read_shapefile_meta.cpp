#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/multi_file_reader.hpp"

#include "spatial/common.hpp"
#include "spatial/core/io/shapefile.hpp"
#include "spatial/core/functions/table.hpp"
#include "spatial/core/types.hpp"

#include "shapefil.h"
#include "utf8proc_wrapper.hpp"

namespace spatial {

namespace core {

struct ShapeFileMetaBindData : public TableFunctionData {
	vector<string> files;
};

struct ShapeTypeEntry {
	int shp_type;
	const char *shp_name;
};

static ShapeTypeEntry shape_type_map[] = {
    {SHPT_NULL, "NULL"},
    {SHPT_POINT, "POINT"},
    {SHPT_ARC, "LINESTRING"},
    {SHPT_POLYGON, "POLYGON"},
    {SHPT_MULTIPOINT, "MULTIPOINT"},
    {SHPT_POINTZ, "POINTZ"},
    {SHPT_ARCZ, "LINESTRINGZ"},
    {SHPT_POLYGONZ, "POLYGONZ"},
    {SHPT_MULTIPOINTZ, "MULTIPOINTZ"},
    {SHPT_POINTM, "POINTM"},
    {SHPT_ARCM, "LINESTRINGM"},
    {SHPT_POLYGONM, "POLYGONM"},
    {SHPT_MULTIPOINTM, "MULTIPOINTM"},
    {SHPT_MULTIPATCH, "MULTIPATCH"},
};

static unique_ptr<FunctionData> ShapeFileMetaBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ShapeFileMetaBindData>();

	auto multi_file_reader = MultiFileReader::Create(input.table_function);
	auto file_list = multi_file_reader->CreateFileList(context, input.inputs[0], FileGlobOptions::ALLOW_EMPTY);

	for (auto &file : file_list->Files()) {
		if (StringUtil::EndsWith(StringUtil::Lower(file), ".shp")) {
			result->files.push_back(file);
		}
	}

	auto shape_type_count = sizeof(shape_type_map) / sizeof(ShapeTypeEntry);
	auto varchar_vector = Vector(LogicalType::VARCHAR, shape_type_count);
	auto varchar_data = FlatVector::GetData<string_t>(varchar_vector);
	for (idx_t i = 0; i < shape_type_count; i++) {
		auto str = string_t(shape_type_map[i].shp_name);
		varchar_data[i] = str.IsInlined() ? str : StringVector::AddString(varchar_vector, str);
	}
	auto shape_type_enum = LogicalType::ENUM("SHAPE_TYPE", varchar_vector, shape_type_count);
	shape_type_enum.SetAlias("SHAPE_TYPE");

	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(shape_type_enum);
	return_types.push_back(GeoTypes::BOX_2D());
	return_types.push_back(LogicalType::INTEGER);
	names.push_back("name");
	names.push_back("shape_type");
	names.push_back("bounds");
	names.push_back("count");
	return std::move(result);
}

struct ShapeFileMetaGlobalState : public GlobalTableFunctionState {
	ShapeFileMetaGlobalState() : current_file_idx(0) {
	}
	idx_t current_file_idx;
	vector<string> files;
};

static unique_ptr<GlobalTableFunctionState> ShapeFileMetaInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ShapeFileMetaBindData>();
	auto result = make_uniq<ShapeFileMetaGlobalState>();

	result->files = bind_data.files;
	result->current_file_idx = 0;

	return std::move(result);
}

static void ShapeFileMetaExecute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<ShapeFileMetaBindData>();
	auto &state = input.global_state->Cast<ShapeFileMetaGlobalState>();
	auto &fs = FileSystem::GetFileSystem(context);

	auto &file_name_vector = output.data[0];
	auto file_name_data = FlatVector::GetData<string_t>(file_name_vector);
	auto &shape_type_vector = output.data[1];
	auto shape_type_data = FlatVector::GetData<uint8_t>(shape_type_vector);
	auto &bounds_vector = output.data[2];
	auto &bounds_vector_children = StructVector::GetEntries(bounds_vector);
	auto minx_data = FlatVector::GetData<double>(*bounds_vector_children[0]);
	auto miny_data = FlatVector::GetData<double>(*bounds_vector_children[1]);
	auto maxx_data = FlatVector::GetData<double>(*bounds_vector_children[2]);
	auto maxy_data = FlatVector::GetData<double>(*bounds_vector_children[3]);
	auto record_count_vector = output.data[3];
	auto record_count_data = FlatVector::GetData<int32_t>(record_count_vector);

	auto output_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind_data.files.size() - state.current_file_idx);

	for (idx_t out_idx = 0; out_idx < output_count; out_idx++) {
		auto &file_name = bind_data.files[state.current_file_idx + out_idx];

		auto file_handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ);
		auto shp_handle = OpenSHPFile(fs, file_name.c_str());

		double min_bound[4];
		double max_bound[4];
		int shape_type;
		int record_count;
		SHPGetInfo(shp_handle.get(), &record_count, &shape_type, min_bound, max_bound);
		file_name_data[out_idx] = StringVector::AddString(file_name_vector, file_name);
		shape_type_data[out_idx] = 0;
		for (auto shape_type_idx = 0; shape_type_idx < sizeof(shape_type_map) / sizeof(ShapeTypeEntry);
		     shape_type_idx++) {
			if (shape_type_map[shape_type_idx].shp_type == shape_type) {
				shape_type_data[out_idx] = shape_type_idx;
				break;
			}
		}
		minx_data[out_idx] = min_bound[0];
		miny_data[out_idx] = min_bound[1];
		maxx_data[out_idx] = max_bound[0];
		maxy_data[out_idx] = max_bound[1];
		record_count_data[out_idx] = record_count;
	}

	state.current_file_idx += output_count;
	output.SetCardinality(output_count);
}

static double ShapeFileMetaProgress(ClientContext &context, const FunctionData *bind_data,
                                    const GlobalTableFunctionState *gstate) {
	auto &state = gstate->Cast<ShapeFileMetaGlobalState>();
	return static_cast<double>(state.current_file_idx) / static_cast<double>(state.files.size());
}

static unique_ptr<NodeStatistics> ShapeFileMetaCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ShapeFileMetaBindData>();
	auto result = make_uniq<NodeStatistics>();
	result->has_max_cardinality = true;
	result->max_cardinality = bind_data.files.size();
	result->has_estimated_cardinality = true;
	result->estimated_cardinality = bind_data.files.size();
	return result;
}

void CoreTableFunctions::RegisterShapefileMetaTableFunction(DatabaseInstance &db) {

	TableFunction meta_func("shapefile_meta", {LogicalType::VARCHAR}, ShapeFileMetaExecute, ShapeFileMetaBind,
	                        ShapeFileMetaInitGlobal);
	meta_func.table_scan_progress = ShapeFileMetaProgress;
	meta_func.cardinality = ShapeFileMetaCardinality;
	ExtensionUtil::RegisterFunction(db, MultiFileReader::CreateFunctionSet(meta_func));
}

} // namespace core

} // namespace spatial