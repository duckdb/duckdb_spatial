#pragma once

#include "duckdb/function/table/arrow.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/replacement_scan.hpp"

#include "spatial/common.hpp"

namespace spatial {

namespace gdal {

struct GdalTableFunction : ArrowTableFunction {
private:
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);
	static void RenameColumns(vector<string> &names);

	static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input);
	static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
	                                                     GlobalTableFunctionState *global_state_p);

	static void Scan(ClientContext &context, TableFunctionInput &input, DataChunk &output);

	static idx_t MaxThreads(ClientContext &context, const FunctionData *bind_data_p);

	static unique_ptr<NodeStatistics> Cardinality(ClientContext &context, const FunctionData *data);

	static unique_ptr<TableRef> ReplacementScan(ClientContext &context, ReplacementScanInput &input,
	                                            optional_ptr<ReplacementScanData> data);

public:
	static void Register(DatabaseInstance &db);
};

struct GdalDriversTableFunction {

	struct BindData : public TableFunctionData {
		idx_t driver_count;
		BindData(idx_t driver_count) : driver_count(driver_count) {
		}
	};

	struct State : public GlobalTableFunctionState {
		idx_t current_idx;
		State() : current_idx(0) {
		}
	};
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);
	static void Execute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
	static void Register(DatabaseInstance &db);
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);
};
struct GdalCopyFunction {
	static void Register(DatabaseInstance &db);
};

struct GdalMetadataFunction {
	static void Register(DatabaseInstance &db);
};

} // namespace gdal

} // namespace spatial