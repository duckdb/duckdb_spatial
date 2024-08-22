#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/multi_file_reader.hpp"

#include "spatial/common.hpp"
#include "spatial/gdal/functions.hpp"
#include "spatial/gdal/file_handler.hpp"
#include "spatial/gdal/raster/raster.hpp"

#include "gdal_priv.h"
#include <cstring>

namespace spatial {

namespace gdal {

//------------------------------------------------------------------------------
// Bind
//------------------------------------------------------------------------------

struct GDALMetadataBindData : public TableFunctionData {
	vector<string> file_names;
};

static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {

	auto file_name = input.inputs[0].GetValue<string>();
	auto result = make_uniq<GDALMetadataBindData>();

	auto multi_file_reader = MultiFileReader::Create(input.table_function);
	result->file_names =
	    multi_file_reader->CreateFileList(context, input.inputs[0], FileGlobOptions::ALLOW_EMPTY)->GetAllFiles();

	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::DOUBLE);
	return_types.push_back(LogicalType::DOUBLE);
	return_types.push_back(LogicalType::INTEGER);
	return_types.push_back(LogicalType::INTEGER);
	return_types.push_back(LogicalType::DOUBLE);
	return_types.push_back(LogicalType::DOUBLE);
	return_types.push_back(LogicalType::DOUBLE);
	return_types.push_back(LogicalType::DOUBLE);
	return_types.push_back(LogicalType::INTEGER);
	return_types.push_back(LogicalType::INTEGER);

	names.emplace_back("file_name");
	names.emplace_back("driver_short_name");
	names.emplace_back("driver_long_name");
	names.emplace_back("upper_left_x");
	names.emplace_back("upper_left_y");
	names.emplace_back("width");
	names.emplace_back("height");
	names.emplace_back("scale_x");
	names.emplace_back("scale_y");
	names.emplace_back("skew_x");
	names.emplace_back("skew_y");
	names.emplace_back("srid");
	names.emplace_back("num_bands");

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
			    GDALDataset::Open(prefixed_file_name.c_str(), GDAL_OF_RASTER | GDAL_OF_VERBOSE_ERROR));
		} catch (...) {
			// Just skip anything we cant open
			out_idx--;
			out_size--;
			continue;
		}

		Raster raster(dataset.get());
		double gt[6] = {0};
		raster.GetGeoTransform(gt);

		output.data[0].SetValue(out_idx, file_name);
		output.data[1].SetValue(out_idx, dataset->GetDriver()->GetDescription());
		output.data[2].SetValue(out_idx, dataset->GetDriver()->GetMetadataItem(GDAL_DMD_LONGNAME));
		output.data[3].SetValue(out_idx, gt[0]);
		output.data[4].SetValue(out_idx, gt[3]);
		output.data[5].SetValue(out_idx, raster.GetRasterXSize());
		output.data[6].SetValue(out_idx, raster.GetRasterYSize());
		output.data[7].SetValue(out_idx, gt[1]);
		output.data[8].SetValue(out_idx, gt[5]);
		output.data[9].SetValue(out_idx, gt[2]);
		output.data[10].SetValue(out_idx, gt[4]);
		output.data[11].SetValue(out_idx, raster.GetSrid());
		output.data[12].SetValue(out_idx, raster.GetRasterCount());
	}

	output.SetCardinality(out_size);
}

//------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
	The `ST_Read_Meta` table function accompanies the [ST_ReadRaster](#st_readraster) table function, but instead of reading the contents of a file, this function scans the metadata instead.
)";

static constexpr const char *DOC_EXAMPLE = R"(
	SELECT
		driver_short_name,
		driver_long_name,
		upper_left_x,
		upper_left_y,
		width,
		height,
		scale_x,
		scale_y,
		skew_x,
		skew_y,
		srid,
		num_bands
	FROM
		ST_ReadRaster_Meta('./test/data/mosaic/SCL.tif-land-clip00.tiff')
	;

	┌───────────────────┬──────────────────┬──────────────┬──────────────┬───────┬────────┬─────────┬─────────┬────────┬────────┬───────┬───────────┐
	│ driver_short_name │ driver_long_name │ upper_left_x │ upper_left_y │ width │ height │ scale_x │ scale_y │ skew_x │ skew_y │ srid  │ num_bands │
	│      varchar      │     varchar      │    double    │    double    │ int32 │ int32  │ double  │ double  │ double │ double │ int32 │   int32   │
	├───────────────────┼──────────────────┼──────────────┼──────────────┼───────┼────────┼─────────┼─────────┼────────┼────────┼───────┼───────────┤
	│ GTiff             │ GeoTIFF          │     541020.0 │    4796640.0 │  3438 │   5322 │    20.0 │   -20.0 │    0.0 │    0.0 │ 32630 │         1 │
	└───────────────────┴──────────────────┴──────────────┴──────────────┴───────┴────────┴─────────┴─────────┴────────┴────────┴───────┴───────────┘
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}};

//------------------------------------------------------------------------------
// Register
//------------------------------------------------------------------------------

void GdalRasterMetadataFunction::Register(DatabaseInstance &db) {
	TableFunction func("ST_ReadRaster_Meta", {LogicalType::VARCHAR}, Scan, Bind, Init);
	ExtensionUtil::RegisterFunction(db, MultiFileReader::CreateFunctionSet(func));

	DocUtil::AddDocumentation(db, "ST_ReadRaster_Meta", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace gdal

} // namespace spatial
