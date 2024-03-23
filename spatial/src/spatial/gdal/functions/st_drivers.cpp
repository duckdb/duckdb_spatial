#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "spatial/common.hpp"
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
	auto result = make_uniq<BindData>(driver_count);
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> GdalDriversTableFunction::Init(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	auto result = make_uniq<State>();
	return std::move(result);
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
		auto help_topic_value = help_topic_flag == nullptr
		                            ? Value(LogicalType::VARCHAR)
		                            : Value(StringUtil::Format("https://gdal.org/%s", help_topic_flag));

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

//------------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------------
static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}};

static constexpr const char *DOC_DESCRIPTION = R"(
    Returns the list of supported GDAL drivers and file formats

    Note that far from all of these drivers have been tested properly, and some may require additional options to be passed to work as expected. If you run into any issues please first consult the [consult the GDAL docs](https://gdal.org/drivers/vector/index.html).
)";

static constexpr const char *DOC_EXAMPLE = R"(
    SELECT * FROM ST_Drivers();
)";

//------------------------------------------------------------------------------
// Register
//------------------------------------------------------------------------------
void GdalDriversTableFunction::Register(DatabaseInstance &db) {
	TableFunction func("ST_Drivers", {}, Execute, Bind, Init);

	ExtensionUtil::RegisterFunction(db, func);
	DocUtil::AddDocumentation(db, "ST_Drivers", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace gdal

} // namespace spatial