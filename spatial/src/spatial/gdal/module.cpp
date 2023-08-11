#include "spatial/gdal/module.hpp"
#include "spatial/gdal/functions.hpp"

#include "spatial/common.hpp"

#include "ogrsf_frmts.h"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/client_config.hpp"

namespace spatial {

namespace gdal {

void GdalModule::Register(ClientContext &context) {

	// Load GDAL
	OGRRegisterAll();


	// Config options
	auto &config = DBConfig::GetConfig(*context.db);
	
	// TODO: Remove this once GDAL 3.8 is released
	CPLSetConfigOption("OGR_XLSX_HEADERS", "AUTO");
	config.AddExtensionOption(
		"OGR_XLSX_HEADERS", 
		"Whether or not to read headers in xlsx through st_read. One of [FORCE/DISABLE/AUTO]", 
		LogicalType::VARCHAR,
		Value("AUTO"),
		[](ClientContext &context, SetScope scope, Value &val) {
			auto str = StringValue::Get(val);
			if(str == "FORCE") {
				CPLSetConfigOption("OGR_XLSX_HEADERS", "FORCE");
			}
			else if(str == "DISABLE") {
				CPLSetConfigOption("OGR_XLSX_HEADERS", "DISABLE");
			}
			else if(str == "AUTO") {
				CPLSetConfigOption("OGR_XLSX_HEADERS", "AUTO");
			}
			else {
				throw InvalidInputException(StringUtil::Format("Invalid value '%s' for OGR_XLSX_HEADERS, must be one of [FORCE/DISABLE/AUTO]", str));
			}
		});

	// Register functions
	GdalTableFunction::Register(context);
	GdalDriversTableFunction::Register(context);
	GdalCopyFunction::Register(context);
}

} // namespace gdal

} // namespace spatial
