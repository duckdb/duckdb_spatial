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

	result->file_names =
		MultiFileReader::GetFileList(context, input.inputs[0], "gdal metadata", FileGlobOptions::ALLOW_EMPTY);

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

		output.data[0] .SetValue(out_idx, file_name);
		output.data[1] .SetValue(out_idx, dataset->GetDriver()->GetDescription());
		output.data[2] .SetValue(out_idx, dataset->GetDriver()->GetMetadataItem(GDAL_DMD_LONGNAME));
		output.data[3] .SetValue(out_idx, gt[0]);
		output.data[4] .SetValue(out_idx, gt[3]);
		output.data[5] .SetValue(out_idx, raster.GetRasterXSize());
		output.data[6] .SetValue(out_idx, raster.GetRasterYSize());
		output.data[7] .SetValue(out_idx, gt[1]);
		output.data[8] .SetValue(out_idx, gt[5]);
		output.data[9] .SetValue(out_idx, gt[2]);
		output.data[10].SetValue(out_idx, gt[4]);
		output.data[11].SetValue(out_idx, raster.GetSrid());
		output.data[12].SetValue(out_idx, raster.GetRasterCount());
	}

	output.SetCardinality(out_size);
}

//------------------------------------------------------------------------------
// Register
//------------------------------------------------------------------------------

void GdalRasterMetadataFunction::Register(DatabaseInstance &db) {
	TableFunction func("ST_ReadRaster_Meta", {LogicalType::VARCHAR}, Scan, Bind, Init);
	ExtensionUtil::RegisterFunction(db, MultiFileReader::CreateFunctionSet(func));
}

} // namespace gdal

} // namespace spatial
