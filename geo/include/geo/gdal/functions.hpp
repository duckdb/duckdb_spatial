#pragma once

#include "duckdb/function/table/arrow.hpp"

#include "geo/common.hpp"

namespace geo {

namespace gdal {

struct GdalTableFunction : ArrowTableFunction {
private:
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);

	static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input);

	static void Scan(ClientContext &context, TableFunctionInput &input, DataChunk &output);

	static idx_t MaxThreads(ClientContext &context, const FunctionData *bind_data_p);

public:
	static void Register(ClientContext &context);
};

} // namespace gdal

} // namespace geo