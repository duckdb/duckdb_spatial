#include "spatial/core/index/rtree/rtree_index_create_physical.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"

#include "spatial/core/index/rtree/rtree_index.hpp"
#include "spatial/core/index/rtree/rtree_node.hpp"
#include "spatial/core/geometry/geometry_type.hpp"

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
	vector<RTreePointer> bottom_layer;

	idx_t entry_idx = RTreeNode::CAPACITY;
};

unique_ptr<GlobalSinkState> PhysicalCreateRTreeIndex::GetGlobalSinkState(ClientContext &context) const {
	auto gstate = make_uniq<CreateRTreeIndexGlobalState>();

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
	auto &layer = gstate.bottom_layer;

	if(chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	// TODO: Dont flatten chunk
	chunk.Flatten();

	const auto &geom_data = FlatVector::GetData<geometry_t>(chunk.data[0]);
	const auto &rowid_data = FlatVector::GetData<row_t>(chunk.data[1]);

	idx_t elem_idx = 0;
	while(elem_idx < chunk.size()){

		// Initialize a new node, if needed
		if(gstate.entry_idx == RTreeNode::CAPACITY) {
			layer.emplace_back();
			RTreePointer::NewBranch(rtree, layer.back());
			gstate.entry_idx = 0;
		}

		// Pick the last node in the layer
		auto &node = RTreePointer::RefMutable(rtree, layer.back());

		const auto remaining_capacity = RTreeNode::CAPACITY - gstate.entry_idx;
		const auto remaining_elements = chunk.size() - elem_idx;

		for(idx_t j = 0; j < MinValue<idx_t>(remaining_capacity, remaining_elements); j++) {
			const auto &geom = geom_data[elem_idx];
			const auto &rowid = rowid_data[elem_idx];

			Box2D<double> bbox;
			if(!geom.TryGetCachedBounds(bbox)) {
				throw NotImplementedException("Geometry does not have cached bounds");
			}

			Box2D<float> bbox_f;
			bbox_f.min.x = MathUtil::DoubleToFloatDown(bbox.min.x);
			bbox_f.min.y = MathUtil::DoubleToFloatDown(bbox.min.y);
			bbox_f.max.x = MathUtil::DoubleToFloatUp(bbox.max.x);
			bbox_f.max.y = MathUtil::DoubleToFloatUp(bbox.max.y);

			auto child_ptr = RTreePointer::NewLeaf(rowid);
			node.entries[gstate.entry_idx++] = { child_ptr, bbox_f };
			elem_idx++;
		}
	}

	return SinkResultType::NEED_MORE_INPUT;
}

//-------------------------------------------------------------
// Finalize
//-------------------------------------------------------------
SinkFinalizeType PhysicalCreateRTreeIndex::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                   OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<CreateRTreeIndexGlobalState>();
	auto &rtree = *gstate.rtree;
	auto &bottom_layer = gstate.bottom_layer;

	// Now, we have our base layer with all the leaves, we need to build the rest of the tree layer by layer
	vector<RTreePointer> next_layer;

	vector<RTreePointer> *current_layer_ptr = &bottom_layer;
	vector<RTreePointer> *next_layer_ptr = &next_layer;

	// TODO: Do this in a task so we can yield
	while(current_layer_ptr->size() != 1) {
		// Recurse
		next_layer_ptr->clear();

		const auto next_layer_size = (current_layer_ptr->size() + RTreeNode::CAPACITY - 1) / RTreeNode::CAPACITY;
		next_layer_ptr->reserve(next_layer_size);

		// Add the new node to the next layer
		for(idx_t i = 0; i < next_layer_size; i++) {
			next_layer_ptr->emplace_back();
			auto &node = RTreePointer::NewBranch(rtree, next_layer_ptr->back());

			// Add the children
			for(idx_t j = 0; j < RTreeNode::CAPACITY; j++) {
				const idx_t child_idx = i * RTreeNode::CAPACITY + j;
				if(child_idx >= current_layer_ptr->size()) {
					break;
				}

				auto &child_ptr = (*current_layer_ptr)[child_idx];
				auto &child = RTreePointer::Ref(rtree, child_ptr);
				D_ASSERT(child_ptr.IsBranch());
				node.entries[j] = { child_ptr, child.GetBounds() };
			}
		}

		// Swap the layers
		std::swap(current_layer_ptr, next_layer_ptr);
	}

	// Set the root node!
	rtree.root_block_ptr = current_layer_ptr->back();

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