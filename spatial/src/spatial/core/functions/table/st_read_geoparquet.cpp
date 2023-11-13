#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "spatial/common.hpp"
#include "spatial/core/functions/table.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

struct BindData : TableFunctionData {
	string file_name;
	explicit BindData(string file_name) : file_name(file_name) {

	}
};

static unique_ptr<FunctionData> Bind(ClientContext& context,
                                     TableFunctionBindInput& input,
                                     vector<LogicalType>& return_types,
                                     vector<string>& names) = delete;

struct GlobalState : GlobalTableFunctionState { };

static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input) = delete;

struct LocalState : LocalTableFunctionState { };

static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global_state) = delete;

static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) = delete;

static double Progress(ClientContext &context, const FunctionData *bind_data,
                       const GlobalTableFunctionState *global_state) = delete;

static idx_t GetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                           LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state) = delete;

static unique_ptr<TableRef> ReadGeoparquetReplacementScan(ClientContext &context, const string &table_name,
                                                          ReplacementScanData *data) = delete;


void CoreTableFunctions::RegisterGeoparquetTableFunction(duckdb::DatabaseInstance &db) {
	TableFunction read("ST_ReadGeoparquet", {LogicalType::VARCHAR}, Execute, Bind, InitGlobal, InitLocal);
	read.get_batch_index = GetBatchIndex;
	read.table_scan_progress = Progress;

	ExtensionUtil::RegisterFunction(db, read);

	auto& config = DBConfig::GetConfig(db);
	config.replacement_scans.emplace_back(ReadGeoparquetReplacementScan);
}

}
}