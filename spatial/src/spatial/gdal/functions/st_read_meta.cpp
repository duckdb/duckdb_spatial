#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/multi_file_reader.hpp"

#include "spatial/common.hpp"
#include "spatial/gdal/functions.hpp"
#include "spatial/gdal/file_handler.hpp"

#include "ogrsf_frmts.h"
#include <cstring>

namespace spatial {

namespace gdal {

//------------------------------------------------------------------------------
// Bind
//------------------------------------------------------------------------------

struct GDALMetadataBindData : public TableFunctionData {
	vector<string> file_names;
};

static LogicalType GEOMETRY_FIELD_TYPE = LogicalType::STRUCT({
    {"name", LogicalType::VARCHAR},
    {"type", LogicalType::VARCHAR},
    {"nullable", LogicalType::BOOLEAN},
    {"crs", LogicalType::STRUCT({
                {"name", LogicalType::VARCHAR},
                {"auth_name", LogicalType::VARCHAR},
                {"auth_code", LogicalType::VARCHAR},
                {"wkt", LogicalType::VARCHAR},
                {"proj4", LogicalType::VARCHAR},
                {"projjson", LogicalType::VARCHAR},
            })},
});

static LogicalType STANDARD_FIELD_TYPE = LogicalType::STRUCT({
    {"name", LogicalType::VARCHAR},
    {"type", LogicalType::VARCHAR},
    {"subtype", LogicalType::VARCHAR},
    {"nullable", LogicalType::BOOLEAN},
    {"unique", LogicalType::BOOLEAN},
    {"width", LogicalType::BIGINT},
    {"precision", LogicalType::BIGINT},
});

static LogicalType LAYER_TYPE = LogicalType::STRUCT({
    {"name", LogicalType::VARCHAR},
    {"feature_count", LogicalType::BIGINT},
    {"geometry_fields", LogicalType::LIST(GEOMETRY_FIELD_TYPE)},
    {"fields", LogicalType::LIST(STANDARD_FIELD_TYPE)},
});

static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<GDALMetadataBindData>();

	auto multi_file_reader = MultiFileReader::Create(input.table_function);
	result->file_names =
	    multi_file_reader->CreateFileList(context, input.inputs[0], FileGlobOptions::ALLOW_EMPTY)->GetAllFiles();

	names.push_back("file_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("driver_short_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("driver_long_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("layers");
	return_types.push_back(LogicalType::LIST(LAYER_TYPE));

	// TODO: Add metadata, domains, relationships
	/*
	names.push_back("metadata");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("domains");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("relationships");
	return_types.push_back(LogicalType::VARCHAR);
	*/

	return std::move(result);
}

//------------------------------------------------------------------------------
// Init
//------------------------------------------------------------------------------
struct GDALMetadataState : public GlobalTableFunctionState {
	idx_t current_file_idx = 0;
};

static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<GDALMetadataState>();
	return std::move(result);
}

//------------------------------------------------------------------------------
// Scan
//------------------------------------------------------------------------------

static Value GetLayerData(GDALDatasetUniquePtr &dataset) {
	vector<Value> layer_values;

	for (const auto &layer : dataset->GetLayers()) {
		child_list_t<Value> layer_value_fields;

		layer_value_fields.emplace_back("name", Value(layer->GetName()));
		layer_value_fields.emplace_back("feature_count", Value(static_cast<int64_t>(layer->GetFeatureCount())));

		vector<Value> geometry_fields;
		for (const auto &field : layer->GetLayerDefn()->GetGeomFields()) {
			child_list_t<Value> geometry_field_value_fields;
			auto field_name = field->GetNameRef();
			if (std::strlen(field_name) == 0) {
				field_name = "geom";
			}
			geometry_field_value_fields.emplace_back("name", Value(field_name));
			geometry_field_value_fields.emplace_back("type", Value(OGRGeometryTypeToName(field->GetType())));
			geometry_field_value_fields.emplace_back("nullable", Value(static_cast<bool>(field->IsNullable())));

			auto crs = field->GetSpatialRef();
			if (crs != nullptr) {
				child_list_t<Value> crs_value_fields;
				crs_value_fields.emplace_back("name", Value(crs->GetName()));
				crs_value_fields.emplace_back("auth_name", Value(crs->GetAuthorityName(nullptr)));
				crs_value_fields.emplace_back("auth_code", Value(crs->GetAuthorityCode(nullptr)));

				char *wkt_ptr = nullptr;
				crs->exportToWkt(&wkt_ptr);
				crs_value_fields.emplace_back("wkt", wkt_ptr ? Value(wkt_ptr) : Value());
				CPLFree(wkt_ptr);

				char *proj4_ptr = nullptr;
				crs->exportToProj4(&proj4_ptr);
				crs_value_fields.emplace_back("proj4", proj4_ptr ? Value(proj4_ptr) : Value());
				CPLFree(proj4_ptr);

				char *projjson_ptr = nullptr;
				crs->exportToPROJJSON(&projjson_ptr, nullptr);
				crs_value_fields.emplace_back("projjson", projjson_ptr ? Value(projjson_ptr) : Value());
				CPLFree(projjson_ptr);

				geometry_field_value_fields.emplace_back("crs", Value::STRUCT(crs_value_fields));
			}

			geometry_fields.push_back(Value::STRUCT(geometry_field_value_fields));
		}
		layer_value_fields.emplace_back("geometry_fields",
		                                Value::LIST(GEOMETRY_FIELD_TYPE, std::move(geometry_fields)));

		vector<Value> standard_fields;
		for (const auto &field : layer->GetLayerDefn()->GetFields()) {
			child_list_t<Value> standard_field_value_fields;
			standard_field_value_fields.emplace_back("name", Value(field->GetNameRef()));
			standard_field_value_fields.emplace_back("type", Value(OGR_GetFieldTypeName(field->GetType())));
			standard_field_value_fields.emplace_back("subtype", Value(OGR_GetFieldSubTypeName(field->GetSubType())));
			standard_field_value_fields.emplace_back("nullable", Value(field->IsNullable()));
			standard_field_value_fields.emplace_back("unique", Value(field->IsUnique()));
			standard_field_value_fields.emplace_back("width", Value(field->GetWidth()));
			standard_field_value_fields.emplace_back("precision", Value(field->GetPrecision()));
			standard_fields.push_back(Value::STRUCT(standard_field_value_fields));
		}
		layer_value_fields.emplace_back("fields", Value::LIST(STANDARD_FIELD_TYPE, std::move(standard_fields)));

		layer_values.push_back(Value::STRUCT(layer_value_fields));
	}

	return Value::LIST(LAYER_TYPE, std::move(layer_values));
}

static void Scan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<GDALMetadataBindData>();
	auto &state = input.global_state->Cast<GDALMetadataState>();

	auto out_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind_data.file_names.size() - state.current_file_idx);

	for (idx_t out_idx = 0; out_idx < out_size; out_idx++, state.current_file_idx++) {
		auto file_name = bind_data.file_names[state.current_file_idx];
		auto prefixed_file_name = GDALClientContextState::GetOrCreate(context).GetPrefix(file_name);

		GDALDatasetUniquePtr dataset;
		try {
			dataset = GDALDatasetUniquePtr(
			    GDALDataset::Open(prefixed_file_name.c_str(), GDAL_OF_VECTOR | GDAL_OF_VERBOSE_ERROR));
		} catch (...) {
			// Just skip anything we cant open
			out_idx--;
			out_size--;
			continue;
		}

		output.data[0].SetValue(out_idx, file_name);
		output.data[1].SetValue(out_idx, dataset->GetDriver()->GetDescription());
		output.data[2].SetValue(out_idx, dataset->GetDriver()->GetMetadataItem(GDAL_DMD_LONGNAME));
		output.data[3].SetValue(out_idx, GetLayerData(dataset));
	}

	output.SetCardinality(out_size);
}

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}};

static constexpr const char *DOC_DESCRIPTION = R"(
    Read and the metadata from a variety of geospatial file formats using the GDAL library.

    The `ST_Read_Meta` table function accompanies the `ST_Read` table function, but instead of reading the contents of a file, this function scans the metadata instead.
    Since the data model of the underlying GDAL library is quite flexible, most of the interesting metadata is within the returned `layers` column, which is a somewhat complex nested structure of DuckDB `STRUCT` and `LIST` types.
)";

static constexpr const char *DOC_EXAMPLE = R"(
    -- Find the coordinate reference system authority name and code for the first layers first geometry column in the file
    SELECT
        layers[1].geometry_fields[1].crs.auth_name as name,
        layers[1].geometry_fields[1].crs.auth_code as code
    FROM st_read_meta('../../tmp/data/amsterdam_roads.fgb');
)";

//------------------------------------------------------------------------------
// Register
//------------------------------------------------------------------------------
void GdalMetadataFunction::Register(DatabaseInstance &db) {
	TableFunction func("ST_Read_Meta", {LogicalType::VARCHAR}, Scan, Bind, Init);
	ExtensionUtil::RegisterFunction(db, MultiFileReader::CreateFunctionSet(func));
	DocUtil::AddDocumentation(db, "ST_Read_Meta", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace gdal

} // namespace spatial