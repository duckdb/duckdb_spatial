#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/replacement_scan.hpp"

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/gdal/functions.hpp"
#include "spatial/gdal/file_handler.hpp"
#include "spatial/gdal/raster/raster.hpp"
#include "spatial/gdal/raster/raster_factory.hpp"
#include "spatial/gdal/raster/raster_registry.hpp"
#include "spatial/gdal/raster/raster_value.hpp"

#include "gdal_priv.h"

using namespace spatial::core;

namespace spatial {

namespace gdal {

struct GdalRasterTableFunctionData : public TableFunctionData {
	string file_name;
	named_parameter_map_t parameters;
	RasterRegistry raster_registry;
	bool loaded;
};

//------------------------------------------------------------------------------
// Bind
//------------------------------------------------------------------------------

unique_ptr<FunctionData> GdalRasterTableFunction::Bind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(GeoTypes::RASTER());
	names.emplace_back("path");
	names.emplace_back("raster");

	auto raw_file_name = input.inputs[0].GetValue<string>();
	auto parameters = input.named_parameters;

	auto result = make_uniq<GdalRasterTableFunctionData>();
	result->file_name = raw_file_name;
	result->parameters = parameters;
	result->loaded = false;
	return std::move(result);
}

//------------------------------------------------------------------------------
// Execute
//------------------------------------------------------------------------------

void GdalRasterTableFunction::Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = (GdalRasterTableFunctionData &)*input.bind_data;

	if (bind_data.loaded) {
		output.SetCardinality(0);
		return;
	}

	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("Scanning GDAL files is disabled through configuration");
	}

	// First scan for "options" parameter
	auto gdal_open_options = RasterFactory::FromNamedParameters(bind_data.parameters, "open_options");

	auto gdal_allowed_drivers = RasterFactory::FromNamedParameters(bind_data.parameters, "allowed_drivers");

	auto gdal_sibling_files = RasterFactory::FromNamedParameters(bind_data.parameters, "sibling_files");

	// Now we can open the dataset
	auto raw_file_name = bind_data.file_name;
	auto &ctx_state = GDALClientContextState::GetOrCreate(context);
	auto prefixed_file_name = ctx_state.GetPrefix(raw_file_name);
	auto dataset = GDALDataset::Open(prefixed_file_name.c_str(), GDAL_OF_RASTER | GDAL_OF_VERBOSE_ERROR,
	                                 gdal_allowed_drivers.empty() ? nullptr : gdal_allowed_drivers.data(),
	                                 gdal_open_options.empty() ? nullptr : gdal_open_options.data(),
	                                 gdal_sibling_files.empty() ? nullptr : gdal_sibling_files.data());

	if (dataset == nullptr) {
		auto error = Raster::GetLastErrorMsg();
		throw IOException("Could not open file: " + raw_file_name + " (" + error + ")");
	}

	// Now we can bind the dataset
	bind_data.raster_registry.RegisterRaster(dataset);
	bind_data.loaded = true;

	// And fill the output
	output.data[0].SetValue(0, Value::CreateValue(raw_file_name));
	output.data[1].SetValue(0, RasterValue::CreateValue(dataset));
	output.SetCardinality(1);
};

//------------------------------------------------------------------------------
// Cardinality
//------------------------------------------------------------------------------

unique_ptr<NodeStatistics> GdalRasterTableFunction::Cardinality(ClientContext &context, const FunctionData *data) {
	auto result = make_uniq<NodeStatistics>();
	result->has_estimated_cardinality = true;
	result->estimated_cardinality = 1;
	result->has_max_cardinality = true;
	result->max_cardinality = 1;
	return result;
}

//------------------------------------------------------------------------------
// ReplacementScan
//------------------------------------------------------------------------------

unique_ptr<TableRef> GdalRasterTableFunction::ReplacementScan(ClientContext &, ReplacementScanInput &input,
                                                              optional_ptr<ReplacementScanData>) {

	auto &table_name = input.table_name;
	auto lower_name = StringUtil::Lower(table_name);

	// Check if the file name ends with some common raster file extensions
	if (StringUtil::EndsWith(lower_name, ".img") || StringUtil::EndsWith(lower_name, ".tiff") ||
	    StringUtil::EndsWith(lower_name, ".tif") || StringUtil::EndsWith(lower_name, ".vrt")) {

		auto table_function = make_uniq<TableFunctionRef>();
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
		table_function->function = make_uniq<FunctionExpression>("ST_ReadRaster", std::move(children));
		return std::move(table_function);
	}
	// else not something we can replace
	return nullptr;
}

//------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
	The `ST_ReadRaster` table function is based on the [GDAL](https://gdal.org/index.html) translator library and enables reading spatial data from a variety of geospatial raster file formats as if they were DuckDB tables.
	See `ST_Drivers` for a list of supported file formats and drivers.

	Except for the `path` parameter, all parameters are optional.

	| Parameter | Type | Description |
	| --------- | -----| ----------- |
	| `path` | VARCHAR | The path to the file to read. Mandatory |
	| `open_options` | VARCHAR[] | A list of key-value pairs that are passed to the GDAL driver to control the opening of the file. |
	| `allowed_drivers` | VARCHAR[] | A list of GDAL driver names that are allowed to be used to open the file. If empty, all drivers are allowed. |
	| `sibling_files` | VARCHAR[] | A list of sibling files that are required to open the file. E.g., the ESRI Shapefile driver requires a .shx file to be present. Although most of the time these can be discovered automatically. |

	Note that GDAL is single-threaded, so this table function will not be able to make full use of parallelism.

	By using `ST_ReadRaster`, the spatial extension also provides “replacement scans” for common raster file formats, allowing you to query files of these formats as if they were tables directly.
	In practice this is just syntax-sugar for calling ST_ReadRaster, so there is no difference in performance. If you want to pass additional options, you should use the ST_ReadRaster table function directly.

	The following formats are currently recognized by their file extension:

	| Format | Extension |
	| ------ | --------- |
	| GeoTiff COG | .tif, .tiff |
	| Erdas Imagine | .img |
	| GDAL Virtual | .vrt |
)";

static constexpr const char *DOC_EXAMPLE = R"(
	SELECT * FROM ST_ReadRaster('some/file/path/filename.tiff');

	SELECT * FROM './path/to/some/shapefile/dataset.tiff';
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}};

//------------------------------------------------------------------------------
// Register
//------------------------------------------------------------------------------

void GdalRasterTableFunction::Register(DatabaseInstance &db) {

	TableFunctionSet set("ST_ReadRaster");

	TableFunction func({LogicalType::VARCHAR}, Execute, Bind);
	func.cardinality = GdalRasterTableFunction::Cardinality;
	func.named_parameters["open_options"] = LogicalType::LIST(LogicalType::VARCHAR);
	func.named_parameters["allowed_drivers"] = LogicalType::LIST(LogicalType::VARCHAR);
	func.named_parameters["sibling_files"] = LogicalType::LIST(LogicalType::VARCHAR);
	set.AddFunction(func);

	ExtensionUtil::RegisterFunction(db, set);

	DocUtil::AddDocumentation(db, "ST_ReadRaster", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);

	// Replacement scan
	auto &config = DBConfig::GetConfig(db);
	config.replacement_scans.emplace_back(GdalRasterTableFunction::ReplacementScan);
}

} // namespace gdal

} // namespace spatial
