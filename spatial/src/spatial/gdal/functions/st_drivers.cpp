#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/gdal/functions.hpp"

#include "ogrsf_frmts.h"

namespace spatial {

namespace gdal {

// Simple table function to list all the drivers available
unique_ptr<FunctionData> GdalDriversTableFunction::Bind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::BOOLEAN);
	return_types.emplace_back(LogicalType::BOOLEAN);
	return_types.emplace_back(LogicalType::BOOLEAN);
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("short_name");
	names.emplace_back("long_name");
	names.emplace_back("can_create");
	names.emplace_back("can_copy");
	names.emplace_back("can_open");
	names.emplace_back("help_url");

	auto driver_count = GDALGetDriverCount();
	return make_unique<BindData>(driver_count);
}

unique_ptr<GlobalTableFunctionState> GdalDriversTableFunction::Init(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	return make_unique<State>();
}

void GdalDriversTableFunction::Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = (State &)*input.global_state;
	auto &bind_data = (BindData &)*input.bind_data;

	idx_t count = 0;
	auto next_idx = MinValue<idx_t>(state.current_idx + STANDARD_VECTOR_SIZE, bind_data.driver_count);

	for (; state.current_idx < next_idx; state.current_idx++) {
		auto driver = GDALGetDriver((int)state.current_idx);

		// Check if the driver is a vector driver
		if (GDALGetMetadataItem(driver, GDAL_DCAP_VECTOR, nullptr) == nullptr) {
			continue;
		}

		auto short_name = Value::CreateValue(GDALGetDriverShortName(driver));
		auto long_name = Value::CreateValue(GDALGetDriverLongName(driver));

		const char *create_flag = GDALGetMetadataItem(driver, GDAL_DCAP_CREATE, nullptr);
		auto create_value = Value::CreateValue(create_flag != nullptr);

		const char *copy_flag = GDALGetMetadataItem(driver, GDAL_DCAP_CREATECOPY, nullptr);
		auto copy_value = Value::CreateValue(copy_flag != nullptr);
		const char *open_flag = GDALGetMetadataItem(driver, GDAL_DCAP_OPEN, nullptr);
		auto open_value = Value::CreateValue(open_flag != nullptr);
		
		auto help_topic_flag = GDALGetDriverHelpTopic(driver);
		auto help_topic_value = help_topic_flag == nullptr ? Value(LogicalType::VARCHAR) : Value(StringUtil::Format("https://gdal.org/%s", help_topic_flag));

		output.data[0].SetValue(count, short_name);
		output.data[1].SetValue(count, long_name);
		output.data[2].SetValue(count, create_value);
		output.data[3].SetValue(count, copy_value);
		output.data[4].SetValue(count, open_value);
		output.data[5].SetValue(count, help_topic_value);
		count++;
	}
	output.SetCardinality(count);
}

void GdalDriversTableFunction::Register(ClientContext &context) {
	TableFunction func("st_drivers", {}, Execute, Bind, Init);

	auto &catalog = Catalog::GetSystemCatalog(context);
	CreateTableFunctionInfo info(func);
	catalog.CreateTableFunction(context, &info);
}

} // namespace gdal

} // namespace spatial