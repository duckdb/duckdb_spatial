#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "spatial/core/types.hpp"
#include "spatial/gdal/functions.hpp"
#include "spatial/gdal/file_handler.hpp"
#include "spatial/gdal/raster/raster.hpp"
#include "spatial/gdal/raster/raster_factory.hpp"

#include "gdal_priv.h"

namespace spatial {

namespace gdal {

struct BindRasterData : public TableFunctionData {

	string file_path;
	vector<LogicalType> field_sql_types;
	vector<string> field_names;
	string driver_name;
	vector<string> creation_options;

	BindRasterData(string file_path, vector<LogicalType> field_sql_types, vector<string> field_names)
	    : file_path(std::move(file_path)), field_sql_types(std::move(field_sql_types)),
	      field_names(std::move(field_names)) {
	}
};

struct LocalRasterState : public LocalFunctionData {

	explicit LocalRasterState(ClientContext &context) {
	}
};

struct GlobalRasterState : public GlobalFunctionData {

	explicit GlobalRasterState(ClientContext &context) {
	}
};

//===--------------------------------------------------------------------===//
// Bind
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> Bind(ClientContext &context, CopyFunctionBindInput &input, const vector<string> &names,
                                     const vector<LogicalType> &sql_types) {

	auto bind_data = make_uniq<BindRasterData>(input.info.file_path, sql_types, names);

	// check all the options in the copy info and set
	for (auto &option : input.info.options) {
		if (StringUtil::Upper(option.first) == "DRIVER") {
			auto set = option.second.front();
			if (set.type().id() == LogicalTypeId::VARCHAR) {
				bind_data->driver_name = set.GetValue<string>();
			} else {
				throw BinderException("Driver name must be a string");
			}
		} else if (StringUtil::Upper(option.first) == "CREATION_OPTIONS") {
			auto set = option.second;
			for (auto &s : set) {
				if (s.type().id() != LogicalTypeId::VARCHAR) {
					throw BinderException("Creation options must be strings");
				}
				bind_data->creation_options.push_back(s.GetValue<string>());
			}
		} else {
			throw BinderException("Unknown option '%s'", option.first);
		}
	}

	if (bind_data->driver_name.empty()) {
		throw BinderException("Driver name must be specified");
	}

	auto driver = GetGDALDriverManager()->GetDriverByName(bind_data->driver_name.c_str());
	if (!driver) {
		throw BinderException("Unknown driver '%s'", bind_data->driver_name);
	}

	// Try get the file extension from the driver
	auto file_ext = driver->GetMetadataItem(GDAL_DMD_EXTENSION);
	if (file_ext) {
		input.file_extension = file_ext;
	} else {
		// Space separated list of file extensions
		auto file_exts = driver->GetMetadataItem(GDAL_DMD_EXTENSIONS);
		if (file_exts) {
			auto exts = StringUtil::Split(file_exts, ' ');
			if (!exts.empty()) {
				input.file_extension = exts[0];
			}
		}
	}

	return std::move(bind_data);
}

//===--------------------------------------------------------------------===//
// Init Local
//===--------------------------------------------------------------------===//

static unique_ptr<LocalFunctionData> InitLocal(ExecutionContext &context, FunctionData &bind_data) {

	auto local_data = make_uniq<LocalRasterState>(context.client);
	return std::move(local_data);
}

//===--------------------------------------------------------------------===//
// Init Global
//===--------------------------------------------------------------------===//

static unique_ptr<GlobalFunctionData> InitGlobal(ClientContext &context, FunctionData &bind_data,
                                                 const string &file_path) {

	auto global_data = make_uniq<GlobalRasterState>(context);
	return std::move(global_data);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

static void Sink(ExecutionContext &context, FunctionData &bdata, GlobalFunctionData &gstate, LocalFunctionData &lstate,
                 DataChunk &input) {
	auto &bind_data = bdata.Cast<BindRasterData>();

	// Create the raster
	input.Flatten();
	for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {

		for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
			auto &type = bind_data.field_sql_types[col_idx];

			if (type == core::GeoTypes::RASTER()) {
				auto value = input.GetValue(col_idx, row_idx);
				GDALDataset *dataset = reinterpret_cast<GDALDataset *>(value.GetValueUnsafe<uint64_t>());

				auto raw_file_name = bind_data.file_path;
				auto &client_ctx = GDALClientContextState::GetOrCreate(context.client);
				auto prefixed_file_name = client_ctx.GetPrefix(raw_file_name);
				auto driver_name = bind_data.driver_name;
				auto creation_options = bind_data.creation_options;

				if (!RasterFactory::WriteFile(dataset, prefixed_file_name, driver_name, creation_options)) {
					auto error = Raster::GetLastErrorMsg();
					throw IOException("Could not save file: " + raw_file_name + " (" + error + ")");
				}
				break;
			}
		}
	}
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//

static void Combine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                    LocalFunctionData &lstate) {
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//

static void Finalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
}

void GdalRasterCopyFunction::Register(DatabaseInstance &db) {
	// register the copy function
	CopyFunction info("RASTER");
	info.copy_to_bind = Bind;
	info.copy_to_initialize_local = InitLocal;
	info.copy_to_initialize_global = InitGlobal;
	info.copy_to_sink = Sink;
	info.copy_to_combine = Combine;
	info.copy_to_finalize = Finalize;
	info.extension = "raster";

	ExtensionUtil::RegisterFunction(db, info);
}

} // namespace gdal

} // namespace spatial
