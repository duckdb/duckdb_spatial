#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/storage/data_table.hpp"

#include "spatial/core/index/rtree/rtree_module.hpp"
#include "spatial/core/index/rtree/rtree_index.hpp"
#include "spatial/core/index/rtree/rtree_index_scan.hpp"

namespace spatial {

namespace core {

BindInfo RTreeIndexScanBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<RTreeIndexScanBindData>();
	return BindInfo(bind_data.table);
}

//-------------------------------------------------------------------------
// Global State
//-------------------------------------------------------------------------
struct RTreeIndexScanGlobalState : public GlobalTableFunctionState {
	ColumnFetchState fetch_state;
	TableScanState local_storage_state;
	vector<storage_t> column_ids;

	// Index scan state
	unique_ptr<IndexScanState> index_state;
	Vector row_ids = Vector(LogicalType::ROW_TYPE);
};

static unique_ptr<GlobalTableFunctionState> RTreeIndexScanInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<RTreeIndexScanBindData>();

	auto result = make_uniq<RTreeIndexScanGlobalState>();

	// Setup the scan state for the local storage
	auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);
	result->column_ids.reserve(input.column_ids.size());

	// Figure out the storage column ids
	for (auto &id : input.column_ids) {
		storage_t col_id = id;
		if (id != DConstants::INVALID_INDEX) {
			col_id = bind_data.table.GetColumn(LogicalIndex(id)).StorageOid();
		}
		result->column_ids.push_back(col_id);
	}

	// Initialize the storage scan state
	result->local_storage_state.Initialize(result->column_ids, input.filters.get());
	local_storage.InitializeScan(bind_data.table.GetStorage(), result->local_storage_state.local_state, input.filters);

	// Initialize the scan state for the index
	result->index_state = bind_data.index.Cast<RTreeIndex>().InitializeScan(bind_data.bbox);

	return std::move(result);
}

//-------------------------------------------------------------------------
// Execute
//-------------------------------------------------------------------------
static void RTreeIndexScanExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {

	auto &bind_data = data_p.bind_data->Cast<RTreeIndexScanBindData>();
	auto &state = data_p.global_state->Cast<RTreeIndexScanGlobalState>();
	auto &transaction = DuckTransaction::Get(context, bind_data.table.catalog);

	// Scan the index for row id's
	auto row_count = bind_data.index.Cast<RTreeIndex>().Scan(*state.index_state, state.row_ids);
	if (row_count == 0) {
		// Short-circuit if the index had no more rows
		output.SetCardinality(0);
		return;
	}

	// Fetch the data from the local storage given the row ids
	bind_data.table.GetStorage().Fetch(transaction, output, state.column_ids, state.row_ids, row_count,
	                                   state.fetch_state);
}

//-------------------------------------------------------------------------
// Statistics
//-------------------------------------------------------------------------
static unique_ptr<BaseStatistics> RTreeIndexScanStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                                           column_t column_id) {
	auto &bind_data = bind_data_p->Cast<RTreeIndexScanBindData>();
	auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);
	if (local_storage.Find(bind_data.table.GetStorage())) {
		// we don't emit any statistics for tables that have outstanding transaction-local data
		return nullptr;
	}
	return bind_data.table.GetStatistics(context, column_id);
}

//-------------------------------------------------------------------------
// Dependency
//-------------------------------------------------------------------------
void RTreeIndexScanDependency(LogicalDependencyList &entries, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<RTreeIndexScanBindData>();
	entries.AddDependency(bind_data.table);

	// TODO: Add dependency to index here?
}

//-------------------------------------------------------------------------
// Cardinality
//-------------------------------------------------------------------------
unique_ptr<NodeStatistics> RTreeIndexScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<RTreeIndexScanBindData>();
	auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);
	const auto &storage = bind_data.table.GetStorage();
	idx_t table_rows = storage.GetTotalRows();
	idx_t estimated_cardinality = table_rows + local_storage.AddedRows(bind_data.table.GetStorage());
	return make_uniq<NodeStatistics>(table_rows, estimated_cardinality);
}

//-------------------------------------------------------------------------
// ToString
//-------------------------------------------------------------------------
static string RTreeIndexScanToString(const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<RTreeIndexScanBindData>();
	return bind_data.table.name + " (RTREE INDEX SCAN : " + bind_data.index.GetIndexName() + ")";
}

//-------------------------------------------------------------------------
// De/Serialize
//-------------------------------------------------------------------------
static void RTreeScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
							   const TableFunction &function) {
	auto &bind_data = bind_data_p->Cast<RTreeIndexScanBindData>();
	serializer.WriteProperty(100, "catalog", bind_data.table.schema.catalog.GetName());
	serializer.WriteProperty(101, "schema", bind_data.table.schema.name);
	serializer.WriteProperty(102, "table", bind_data.table.name);
	serializer.WriteProperty(103, "index_name", bind_data.index.GetIndexName());

	serializer.WriteObject(104, "bbox", [&](Serializer &ser){
		ser.WriteProperty<float>(10, "min_x", bind_data.bbox.min.x);
		ser.WriteProperty<float>(11, "min_y", bind_data.bbox.min.y);
		ser.WriteProperty<float>(20, "max_x", bind_data.bbox.max.x);
		ser.WriteProperty<float>(21, "max_y", bind_data.bbox.max.y);
	});
}

static unique_ptr<FunctionData> RTreeScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	auto &context = deserializer.Get<ClientContext &>();

	const auto catalog = deserializer.ReadProperty<string>(100, "catalog");
	const auto schema = deserializer.ReadProperty<string>(101, "schema");
	const auto table = deserializer.ReadProperty<string>(102, "table");
	auto &catalog_entry =
		Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, table);
	if (catalog_entry.type != CatalogType::TABLE_ENTRY) {
		throw SerializationException("Cant find table for %s.%s", schema, table);
	}

	// Now also lookup the index by name
	const auto index_name = deserializer.ReadProperty<string>(103, "index_name");
	RTreeBounds bbox;
	deserializer.ReadObject(104, "bbox", [&](Deserializer &ser){
		bbox.min.x = ser.ReadProperty<float>(10, "min_x");
		bbox.min.y = ser.ReadProperty<float>(11, "min_y");
		bbox.max.x = ser.ReadProperty<float>(20, "max_x");
		bbox.max.y = ser.ReadProperty<float>(21, "max_y");
	});

	auto &duck_table = catalog_entry.Cast<DuckTableEntry>();
	auto &table_info = *catalog_entry.GetStorage().GetDataTableInfo();

	unique_ptr<RTreeIndexScanBindData> result = nullptr;

	table_info.GetIndexes().BindAndScan<RTreeIndex>(context, table_info, [&](RTreeIndex &index_entry) {
		if (index_entry.GetIndexName() == index_name) {
			result = make_uniq<RTreeIndexScanBindData>(duck_table, index_entry, bbox);
			return true;
		}
		return false;
	});

	if(!result) {
		throw SerializationException("Could not find index %s on table %s.%s", index_name, schema, table);
	}
	return std::move(result);
}



//-------------------------------------------------------------------------
// Get Function
//-------------------------------------------------------------------------
TableFunction RTreeIndexScanFunction::GetFunction() {
	TableFunction func("rtree_index_scan", {}, RTreeIndexScanExecute);
	func.init_local = nullptr;
	func.init_global = RTreeIndexScanInitGlobal;
	func.statistics = RTreeIndexScanStatistics;
	func.dependency = RTreeIndexScanDependency;
	func.cardinality = RTreeIndexScanCardinality;
	func.pushdown_complex_filter = nullptr;
	func.to_string = RTreeIndexScanToString;
	func.table_scan_progress = nullptr;
	func.get_batch_index = nullptr;
	func.projection_pushdown = true;
	func.filter_pushdown = false;
	func.get_bind_info = RTreeIndexScanBindInfo;
	func.serialize = RTreeScanSerialize;
	func.deserialize = RTreeScanDeserialize;

	return func;
}

//-------------------------------------------------------------------------
// Register
//-------------------------------------------------------------------------
void RTreeModule::RegisterIndexScan(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db, RTreeIndexScanFunction::GetFunction());
}

} // namespace core

} // namespace spatial