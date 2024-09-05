#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"

#include "spatial/core/types.hpp"
#include "spatial/core/index/rtree/rtree_index.hpp"
#include "spatial/core/index/rtree/rtree_module.hpp"
#include "spatial/core/index/rtree/rtree_node.hpp"
#include "spatial/core/index/rtree/rtree_scanner.hpp"

namespace spatial {

namespace core {

// BIND
static unique_ptr<FunctionData> RTreeindexInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("catalog_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("index_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

// INIT GLOBAL
struct RTreeIndexInfoState final : public GlobalTableFunctionState {
	idx_t offset = 0;
	vector<reference<IndexCatalogEntry>> entries;
};

static unique_ptr<GlobalTableFunctionState> RTreeIndexInfoInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<RTreeIndexInfoState>();

	// scan all the schemas for indexes and collect them
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		schema.get().Scan(context, CatalogType::INDEX_ENTRY, [&](CatalogEntry &entry) {
			auto &index_entry = entry.Cast<IndexCatalogEntry>();
			if (index_entry.index_type == RTreeIndex::TYPE_NAME) {
				result->entries.push_back(index_entry);
			}
		});
	};
	return std::move(result);
}

// EXECUTE
static void RTreeIndexInfoExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<RTreeIndexInfoState>();
	if (data.offset >= data.entries.size()) {
		return;
	}

	idx_t row = 0;
	while (data.offset < data.entries.size() && row < STANDARD_VECTOR_SIZE) {
		auto &index_entry = data.entries[data.offset++].get();
		auto &table_entry = index_entry.schema.catalog.GetEntry<TableCatalogEntry>(context, index_entry.GetSchemaName(),
		                                                                           index_entry.GetTableName());
		auto &storage = table_entry.GetStorage();
		RTreeIndex *rtree_index = nullptr;

		auto &table_info = *storage.GetDataTableInfo();
		table_info.GetIndexes().BindAndScan<RTreeIndex>(context, table_info, [&](RTreeIndex &index) {
			if (index.name == index_entry.name) {
				rtree_index = &index;
				return true;
			}
			return false;
		});

		if (!rtree_index) {
			throw BinderException("Index %s not found", index_entry.name);
		}

		idx_t col = 0;

		output.data[col++].SetValue(row, Value(index_entry.catalog.GetName()));
		output.data[col++].SetValue(row, Value(index_entry.schema.name));
		output.data[col++].SetValue(row, Value(index_entry.name));
		output.data[col++].SetValue(row, Value(table_entry.name));

		row++;
	}
	output.SetCardinality(row);
}

static optional_ptr<RTreeIndex> TryGetIndex(ClientContext &context, const string &index_name) {
	auto qname = QualifiedName::Parse(index_name);

	// look up the index name in the catalog
	Binder::BindSchemaOrCatalog(context, qname.catalog, qname.schema);
	auto &index_entry = Catalog::GetEntry(context, CatalogType::INDEX_ENTRY, qname.catalog, qname.schema, qname.name)
	                        .Cast<IndexCatalogEntry>();
	auto &table_entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, qname.catalog, index_entry.GetSchemaName(),
	                                      index_entry.GetTableName())
	                        .Cast<TableCatalogEntry>();

	auto &storage = table_entry.GetStorage();
	RTreeIndex *rtree_index = nullptr;

	auto &table_info = *storage.GetDataTableInfo();
	table_info.GetIndexes().BindAndScan<RTreeIndex>(context, table_info, [&](RTreeIndex &index) {
		if (index_entry.name == index_name) {
			rtree_index = &index;
			return true;
		}
		return false;
	});

	return rtree_index;
}

//-------------------------------------------------------------------------
// RTree Index Dump
//-------------------------------------------------------------------------
// BIND
struct RTreeIndexDumpBindData final : public TableFunctionData {
	string index_name;
};

static unique_ptr<FunctionData> RTreeIndexDumpBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RTreeIndexDumpBindData>();

	result->index_name = input.inputs[0].GetValue<string>();

	// Return types
	names.emplace_back("level");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("bounds");
	return_types.emplace_back(GeoTypes::BOX_2DF());

	names.emplace_back("row_id");
	return_types.emplace_back(LogicalType::ROW_TYPE);

	return std::move(result);
}

// INIT
struct RTreeIndexDumpStackFrame {
	RTreePointer pointer;
	idx_t entry_idx = 0;
	RTreeIndexDumpStackFrame(const RTreePointer &pointer_p, idx_t entry_idx_p)
	    : pointer(pointer_p), entry_idx(entry_idx_p) {
	}
};

struct RTreeIndexDumpState final : public GlobalTableFunctionState {
	const RTreeIndex &index;
	RTreeScanner scanner;

public:
	explicit RTreeIndexDumpState(const RTreeIndex &index) : index(index) {
	}
};

static unique_ptr<GlobalTableFunctionState> RTreeIndexDumpInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<RTreeIndexDumpBindData>();

	auto rtree_index = TryGetIndex(context, bind_data.index_name);
	if (!rtree_index) {
		throw BinderException("Index %s not found", bind_data.index_name);
	}

	auto result = make_uniq<RTreeIndexDumpState>(*rtree_index);
	const auto &root_entry = rtree_index->tree->GetRoot();

	if (root_entry.pointer.IsSet()) {
		result->scanner.Init(root_entry);
	}
	return std::move(result);
}

// EXECUTE
static void RTreeIndexDumpExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<RTreeIndexDumpState>();

	idx_t output_idx = 0;

	const auto level_data = FlatVector::GetData<int32_t>(output.data[0]);
	const auto &bounds_vectors = StructVector::GetEntries(output.data[1]);
	const auto xmin_data = FlatVector::GetData<float>(*bounds_vectors[0]);
	const auto ymin_data = FlatVector::GetData<float>(*bounds_vectors[1]);
	const auto xmax_data = FlatVector::GetData<float>(*bounds_vectors[2]);
	const auto ymax_data = FlatVector::GetData<float>(*bounds_vectors[3]);
	const auto rowid_data = FlatVector::GetData<row_t>(output.data[2]);

	const auto &tree = *state.index.tree;

	state.scanner.Scan(tree, [&](const RTreeEntry &entry, const idx_t &level) {
		level_data[output_idx] = UnsafeNumericCast<int32_t>(level);
		xmin_data[output_idx] = entry.bounds.min.x;
		ymin_data[output_idx] = entry.bounds.min.y;
		xmax_data[output_idx] = entry.bounds.max.x;
		ymax_data[output_idx] = entry.bounds.max.y;
		rowid_data[output_idx] = entry.pointer.GetRowId();
		output_idx++;
		if (output_idx == STANDARD_VECTOR_SIZE) {
			// We've filled the result vector, yield!
			return RTreeScanResult::YIELD;
		}
		return RTreeScanResult::CONTINUE;
	});

	output.SetCardinality(output_idx);
}

//-------------------------------------------------------------------------
// Register
//-------------------------------------------------------------------------
void RTreeModule::RegisterIndexPragmas(DatabaseInstance &db) {

	TableFunction info_function("pragma_rtree_index_info", {}, RTreeIndexInfoExecute, RTreeindexInfoBind,
	                            RTreeIndexInfoInit);

	ExtensionUtil::RegisterFunction(db, info_function);

	TableFunction dump_function("rtree_index_dump", {LogicalType::VARCHAR}, RTreeIndexDumpExecute, RTreeIndexDumpBind,
	                            RTreeIndexDumpInit);

	ExtensionUtil::RegisterFunction(db, dump_function);
}

} // namespace core

} // namespace spatial