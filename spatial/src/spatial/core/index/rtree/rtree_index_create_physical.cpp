#include "spatial/core/index/rtree/rtree_index_create_physical.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "spatial/core/index/rtree/rtree_index.hpp"
#include "spatial/core/index/rtree/rtree_node.hpp"
#include "spatial/core/util/managed_collection.hpp"

#include "duckdb/common/sort/sort.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"

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

	//! The total number of leaf nodes in the RTree
	idx_t rtree_size = 0;
	idx_t slice_size = 0;

	//! The bottom most layer of the RTree
	ManagedCollection<RTreePointer> curr_layer;
	ManagedCollection<RTreePointer> next_layer;

	ManagedCollectionAppendState append_state;
	ManagedCollectionScanState scan_state;

	ManagedCollection<RTreePointer>* curr_layer_ptr;
	ManagedCollection<RTreePointer>* next_layer_ptr;

	RTreePointer current_pointer;

	idx_t entry_idx = RTreeNode::CAPACITY;

	explicit CreateRTreeIndexGlobalState(ClientContext &context) :
		curr_layer(BufferManager::GetBufferManager(context)),
		next_layer(BufferManager::GetBufferManager(context)),
		curr_layer_ptr(&next_layer), // We swap the order here so that it initializes properly later
		next_layer_ptr(&curr_layer) {
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

	gstate->curr_layer.InitializeAppend(gstate->append_state);

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

	// Count the number of entries
	gstate.rtree_size += chunk.size();

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
			gstate.curr_layer.Append(gstate.append_state, gstate.current_pointer);
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
// RTree Construction
//-------------------------------------------------------------
static TaskExecutionResult BuildRTreeBottomUp(CreateRTreeIndexGlobalState &state, TaskExecutionMode mode, Event &event) {
	auto &rtree = *state.rtree;

	unsafe_vector<RTreePointer> slice(state.slice_size);
	unsafe_vector<idx_t> slice_idx(state.slice_size);
	unsafe_vector<Box2D<float>> slice_bounds(state.slice_size);

	// Now, we have our base layer with all the leaves, we need to build the rest of the tree layer by layer
	while(state.curr_layer_ptr->Count() != 1) {
		if(state.scan_state.IsDone()) {

			// Swap the layers and initialize the next layer
			std::swap(state.curr_layer_ptr, state.next_layer_ptr);

			if(state.curr_layer_ptr->Count() == 1) {
				// We are done!
				break;
			}

			const auto next_layer_size = (state.curr_layer_ptr->Count() + RTreeNode::CAPACITY - 1) / RTreeNode::CAPACITY;
			state.next_layer_ptr->Clear();
			state.next_layer_ptr->InitializeAppend(state.append_state, next_layer_size);
			state.curr_layer_ptr->InitializeScan(state.scan_state, true);
		}

		idx_t child_idx = RTreeNode::CAPACITY;
		RTreePointer current_ptr;

		auto scan_count = state.curr_layer_ptr->Scan(state.scan_state, slice.begin(), slice.end());
		while(scan_count != 0) {

			// Sort the slice by the bounding box y-min value
			for(idx_t i = 0; i < scan_count; i++) {
				const auto &node = RTreePointer::Ref(rtree, slice[i]);
				slice_idx[i] = i;
				slice_bounds[i] = node.GetBounds();
			}
			std::stable_sort(slice_idx.begin(), slice_idx.begin() + scan_count, [&](const idx_t &a, const idx_t &b) {
				return slice_bounds[a].min.y < slice_bounds[b].min.y;
			});

			// Now we can build the next layer
			idx_t scan_idx = 0;
			while(scan_idx < scan_count) {

				// Initialize a new node if we have to
				if(child_idx == RTreeNode::CAPACITY) {
					RTreePointer::NewBranch(rtree, current_ptr);
					child_idx = 0;

					// Append the current node to the layer
					state.next_layer_ptr->Append(state.append_state, current_ptr);
				}

				// Pick the last node in the layer
				auto &node = RTreePointer::RefMutable(rtree, current_ptr);

				const auto remaining_capacity = RTreeNode::CAPACITY - child_idx;
				const auto remaining_elements = scan_count - scan_idx;

				for(idx_t j = 0; j < MinValue<idx_t>(remaining_capacity, remaining_elements); j++) {
					const auto &child_ptr = slice[slice_idx[scan_idx]];
					const auto &child_bounds = slice_bounds[slice_idx[scan_idx]];

					node.entries[child_idx] = { child_ptr, child_bounds };

					child_idx++;
					scan_idx++;
				}
			}

			// Scan the next batch
			scan_count = state.curr_layer_ptr->Scan(state.scan_state, slice.begin(), slice.end());
		}

		// Yield if we are in partial mode and the scan is exhausted
		if(mode == TaskExecutionMode::PROCESS_PARTIAL) {
			return TaskExecutionResult::TASK_NOT_FINISHED;
		}
	}

	// Set the root node!
	rtree.root_block_ptr = state.curr_layer_ptr->Fetch(0);
	event.FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

class RTreeIndexConstructionTask final : public ExecutorTask {
public:
	RTreeIndexConstructionTask(shared_ptr<Event> event_p, ClientContext &context, CreateRTreeIndexGlobalState &gstate)
		: ExecutorTask(context, std::move(event_p)), state(gstate) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		return BuildRTreeBottomUp(state, mode, *event);
	}

private:
	CreateRTreeIndexGlobalState &state;
};

class RTreeIndexConstructionEvent final : public BasePipelineEvent {
public:
	RTreeIndexConstructionEvent(CreateRTreeIndexGlobalState &gstate_p, Pipeline &pipeline_p, CreateIndexInfo &info_p, DuckTableEntry &table_p)
		: BasePipelineEvent(pipeline_p), gstate(gstate_p), info(info_p), table(table_p) {
	}

	void Schedule() override {
		auto &context = pipeline->GetClientContext();

		if(gstate.rtree_size == 0) {
			// No entries to build the RTree from, just create an empty root node
			RTreePointer root_ptr;
			RTreePointer::NewBranch(*gstate.rtree, root_ptr);
			gstate.rtree->root_block_ptr = root_ptr;
			return;
		}

		// We only schedule 1 task, as the bottom-up construction is single-threaded.
		vector<shared_ptr<Task>> tasks;
		tasks.push_back(make_uniq<RTreeIndexConstructionTask>(shared_from_this(), context, gstate));
		SetTasks(std::move(tasks));
	}

	void FinishEvent() override {
		auto &context = pipeline->GetClientContext();

		// Now actually add the index to the storage
		auto &storage = table.GetStorage();

		if (!storage.IsRoot()) {
			throw TransactionException("Cannot create index on non-root transaction");
		}

		// Create the index entry in the catalog
		auto &schema = table.schema;
		const auto index_entry = schema.CreateIndex(context, info, table).get();
		if (!index_entry) {
			D_ASSERT(info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT);
			// index already exists, but error ignored because of IF NOT EXISTS
			return;
		}

		// Get the entry as a DuckIndexEntry
		auto &duck_index = index_entry->Cast<DuckIndexEntry>();
		duck_index.initial_index_size = gstate.rtree->Cast<BoundIndex>().GetInMemorySize();
		duck_index.info = make_uniq<IndexDataTableInfo>(storage.GetDataTableInfo(), duck_index.name);
		for (auto &parsed_expr : info.parsed_expressions) {
			duck_index.parsed_expressions.push_back(parsed_expr->Copy());
		}

		// Finally add it to storage
		storage.AddIndex(std::move(gstate.rtree));
	}
private:
	CreateRTreeIndexGlobalState &gstate;
	CreateIndexInfo &info;
	DuckTableEntry &table;
};

//-------------------------------------------------------------
// Finalize
//-------------------------------------------------------------
SinkFinalizeType PhysicalCreateRTreeIndex::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                   OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<CreateRTreeIndexGlobalState>();
	info->column_ids = storage_ids;

	// Calculate the vertical slice size
	// square root of the total number of entries divide by the capacity of a node, rounded up
	gstate.slice_size = NumericCast<idx_t>(std::ceil(std::sqrt((gstate.rtree_size + RTreeNode::CAPACITY - 1) / RTreeNode::CAPACITY)));

	// Schedule the construction of the RTree
	auto construction_event = make_uniq<RTreeIndexConstructionEvent>(gstate, pipeline, *info, table);
	event.InsertEvent(std::move(construction_event));

	return SinkFinalizeType::READY;
}

} // namespace core

} // namespace spatial