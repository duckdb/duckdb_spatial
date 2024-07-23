#include "spatial/core/index/rtree/rtree_index_create_physical.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"

#include "spatial/core/index/rtree/rtree_index.hpp"
#include "spatial/core/index/rtree/rtree_node.hpp"
#include "spatial/core/util/managed_collection.hpp"

namespace spatial {

namespace core {

//-------------------------------------------------------------
// Physical Create RTree Index
//-------------------------------------------------------------
PhysicalCreateRTreeIndex::PhysicalCreateRTreeIndex(LogicalOperator &op, TableCatalogEntry &table,
                                                 const vector<column_t> &column_ids, unique_ptr<CreateIndexInfo> info,
                                                 vector<unique_ptr<Expression>> unbound_expressions,
                                                 idx_t estimated_cardinality)
    // Declare this operators as a EXTENSION operator
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, estimated_cardinality),
      table(table.Cast<DuckTableEntry>()), info(std::move(info)), unbound_expressions(std::move(unbound_expressions)) {

	// convert virtual column ids to storage column ids
	for (auto &column_id : column_ids) {
		storage_ids.push_back(table.GetColumns().LogicalToPhysical(LogicalIndex(column_id)).index);
	}
}

//-------------------------------------------------------------
// Global State
//-------------------------------------------------------------
class CreateRTreeIndexGlobalState final : public GlobalSinkState {
public:
	//! Global index to be added to the table
	unique_ptr<RTreeIndex> rtree;

	//! The bottom most layer of the RTree
	ManagedCollection<RTreePointer> bottom_layer;
	ManagedCollectionAppendState append_state;
	RTreePointer current_pointer;

	idx_t entry_idx = RTreeNode::CAPACITY;

	explicit CreateRTreeIndexGlobalState(ClientContext &context) : bottom_layer(BufferManager::GetBufferManager(context)), append_state() {
		bottom_layer.InitializeAppend(append_state);
	}
};

unique_ptr<GlobalSinkState> PhysicalCreateRTreeIndex::GetGlobalSinkState(ClientContext &context) const {
	auto gstate = make_uniq<CreateRTreeIndexGlobalState>(context);

	// Create the index
	auto &storage = table.GetStorage();
	auto &table_manager = TableIOManager::Get(storage);
	auto &constraint_type = info->constraint_type;
	auto &db = storage.db;
	gstate->rtree =
	    make_uniq<RTreeIndex>(info->index_name, constraint_type, storage_ids, table_manager, unbound_expressions, db,
	                         info->options, IndexStorageInfo(), estimated_cardinality);

	return std::move(gstate);
}

//-------------------------------------------------------------
// Sink
//-------------------------------------------------------------
SinkResultType PhysicalCreateRTreeIndex::Sink(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<CreateRTreeIndexGlobalState>();
	auto &rtree = *gstate.rtree;

	if(chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	// TODO: Dont flatten chunk
	chunk.Flatten();

	const auto &bbox_vecs = StructVector::GetEntries(chunk.data[0]);
	const auto &rowid_data = FlatVector::GetData<row_t>(chunk.data[1]);
	const auto min_x_data = FlatVector::GetData<float>(*bbox_vecs[0]);
	const auto min_y_data = FlatVector::GetData<float>(*bbox_vecs[1]);
	const auto max_x_data = FlatVector::GetData<float>(*bbox_vecs[2]);
	const auto max_y_data = FlatVector::GetData<float>(*bbox_vecs[3]);

	idx_t elem_idx = 0;
	while(elem_idx < chunk.size()){

		// Initialize a new node, if needed
		if(gstate.entry_idx == RTreeNode::CAPACITY) {
			RTreePointer::NewBranch(rtree, gstate.current_pointer);
			gstate.entry_idx = 0;

			// Append the current node to the layer
			gstate.bottom_layer.Append(gstate.append_state, gstate.current_pointer);
		}

		// Pick the last node in the layer
		auto &node = RTreePointer::RefMutable(rtree, gstate.current_pointer);

		const auto remaining_capacity = RTreeNode::CAPACITY - gstate.entry_idx;
		const auto remaining_elements = chunk.size() - elem_idx;

		for(idx_t j = 0; j < MinValue<idx_t>(remaining_capacity, remaining_elements); j++) {
			const auto &rowid = rowid_data[elem_idx];

			Box2D<float> bbox;
			bbox.min.x = min_x_data[elem_idx];
			bbox.min.y = min_y_data[elem_idx];
			bbox.max.x = max_x_data[elem_idx];
			bbox.max.y = max_y_data[elem_idx];

			auto child_ptr = RTreePointer::NewLeaf(rowid);
			node.entries[gstate.entry_idx++] = { child_ptr, bbox };
			elem_idx++;
		}
	}

	return SinkResultType::NEED_MORE_INPUT;
}

//-------------------------------------------------------------
// Finalize
//-------------------------------------------------------------

static void BuildRTreeBottomUp(ClientContext &context, CreateRTreeIndexGlobalState &gstate) {
	auto &rtree = *gstate.rtree;
	auto &bottom_layer = gstate.bottom_layer;

	// Now, we have our base layer with all the leaves, we need to build the rest of the tree layer by layer
	ManagedCollection<RTreePointer> next_layer(BufferManager::GetBufferManager(context));

	ManagedCollection<RTreePointer> *current_layer_ptr = &bottom_layer;
	ManagedCollection<RTreePointer> *next_layer_ptr = &next_layer;

	// TODO: Do this in a task so we can yield
	while(current_layer_ptr->Count() != 1) {

		// Recurse
		next_layer_ptr->Clear();

		const auto next_layer_size = (current_layer_ptr->Count() + RTreeNode::CAPACITY - 1) / RTreeNode::CAPACITY;
		ManagedCollectionAppendState append_state;
		next_layer_ptr->InitializeAppend(append_state, next_layer_size);

		ManagedCollectionScanState scan_state;
		current_layer_ptr->InitializeScan(scan_state, true);

		RTreePointer scan_buffer[RTreeNode::CAPACITY];

		// Scan up to RTreeNode::CAPACITY nodes at a time
		const auto buffer_beg = scan_buffer;
		const auto buffer_end = scan_buffer + RTreeNode::CAPACITY;

		auto scan_end = buffer_end;
		while(scan_end == buffer_end) {
			scan_end = current_layer_ptr->Scan(scan_state, buffer_beg, buffer_end);

			RTreePointer new_ptr;
			auto &node = RTreePointer::NewBranch(rtree, new_ptr);
			idx_t i = 0;
			for(auto scan_beg = buffer_beg; scan_beg != scan_end; scan_beg++) {
				auto &child_ptr = *scan_beg;
				D_ASSERT(child_ptr.IsBranch());
				auto &child = RTreePointer::Ref(rtree, child_ptr);
				node.entries[i++] = { child_ptr, child.GetBounds() };
			}
			next_layer_ptr->Append(append_state, new_ptr);
		}

		// Swap the layers
		std::swap(current_layer_ptr, next_layer_ptr);
	}

	// Set the root node!
	rtree.root_block_ptr = current_layer_ptr->Fetch(0);
}

SinkFinalizeType PhysicalCreateRTreeIndex::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                   OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<CreateRTreeIndexGlobalState>();

	BuildRTreeBottomUp(context, gstate);

	// Now actually add the index to the storage
	auto &storage = table.GetStorage();

	if (!storage.IsRoot()) {
		throw TransactionException("Cannot create index on non-root transaction");
	}

	// Create the index entry in the catalog
	auto &schema = table.schema;
	info->column_ids = storage_ids;
	const auto index_entry = schema.CreateIndex(context, *info, table).get();
	if (!index_entry) {
		D_ASSERT(info->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT);
		// index already exists, but error ignored because of IF NOT EXISTS
		return SinkFinalizeType::READY;
	}

	// Get the entry as a DuckIndexEntry
	auto &duck_index = index_entry->Cast<DuckIndexEntry>();
	duck_index.initial_index_size = gstate.rtree->Cast<BoundIndex>().GetInMemorySize();
	duck_index.info = make_uniq<IndexDataTableInfo>(storage.GetDataTableInfo(), duck_index.name);
	for (auto &parsed_expr : info->parsed_expressions) {
		duck_index.parsed_expressions.push_back(parsed_expr->Copy());
	}

	// Finally add it to storage
	storage.AddIndex(std::move(gstate.rtree));

	return SinkFinalizeType::READY;
}

} // namespace core

} // namespace spatial