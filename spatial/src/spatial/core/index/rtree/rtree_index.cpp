#include "spatial/core/index/rtree/rtree_index.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "spatial/core/index/rtree/rtree_module.hpp"
#include "spatial/core/index/rtree/rtree_node.hpp"

#include <spatial/core/geometry/geometry_type.hpp>
#include <spatial/core/util/math.hpp>

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// RTree Index Scan State
//------------------------------------------------------------------------------
class RTreeIndexScanState final : public IndexScanState {
public:
	RTreeBounds query_bounds;
	vector<RTreePointer> stack;
};

//------------------------------------------------------------------------------
// RTreeIndex Methods
//------------------------------------------------------------------------------

// Constructor
RTreeIndex::RTreeIndex(const string &name, IndexConstraintType index_constraint_type, const vector<column_t> &column_ids,
                     TableIOManager &table_io_manager, const vector<unique_ptr<Expression>> &unbound_expressions,
                     AttachedDatabase &db, const case_insensitive_map_t<Value> &options, const IndexStorageInfo &info,
                     idx_t estimated_cardinality)
    : BoundIndex(name, TYPE_NAME, index_constraint_type, column_ids, table_io_manager, unbound_expressions, db) {

	if (index_constraint_type != IndexConstraintType::NONE) {
		throw NotImplementedException("RTree indexes do not support unique or primary key constraints");
	}

	// Create a allocator for the linked blocks
	auto &block_manager = table_io_manager.GetIndexBlockManager();
	node_allocator = make_uniq<FixedSizeAllocator>(sizeof(RTreeNode), block_manager);

	if (info.IsValid()) {
		// This is an old index that needs to be loaded
		// Set the root node
		root_block_ptr.Set(info.root);
		node_allocator->Init(info.allocator_infos[0]);
	} else {
		// TODO: Create a root node.
	}
}

unique_ptr<IndexScanState> RTreeIndex::InitializeScan(const RTreeBounds &query) const {
	auto state = make_uniq<RTreeIndexScanState>();
	state->query_bounds = query;
	state->stack.emplace_back(root_block_ptr);
	return std::move(state);
}

idx_t RTreeIndex::Scan(IndexScanState &state_p, Vector &result) const {
	auto &state = state_p.Cast<RTreeIndexScanState>();

	idx_t total_scanned = 0;
	const auto row_ids = FlatVector::GetData<row_t>(result);

	while(!state.stack.empty()) {
		auto &ptr = state.stack.back();
		if(ptr.IsRowId()) {
			// Its a leaf! Collect the row id
			row_ids[total_scanned++] = ptr.GetRowId();
			state.stack.pop_back();
			if(total_scanned == STANDARD_VECTOR_SIZE) {
				// We've filled the result vector, yield!
				return total_scanned;
			}
		} else {
			// Its a page! Add the valid intersecting children to the stack and continue
			auto &node = RTreePointer::Ref(*this, ptr);
			// Even though we modify the stack, we've already dereferenced the current node
			// so its ok if the ptr gets invalidated when we pop_back();
			state.stack.pop_back();
			for(auto &entry : node.entries) {
				if(entry.IsSet() && entry.bounds.Intersects(state.query_bounds)) {
					state.stack.emplace_back(entry.pointer);
				}
			}
		}
	}
	return total_scanned;
}

void RTreeIndex::CommitDrop(IndexLock &index_lock) {
	// TODO: Maybe we can drop these much earlier?
	node_allocator->Reset();
	root_block_ptr.Clear();
}

static auto NOT_IMPLEMENTED_MESSAGE = "RTreeIndex does not support modifications to the base table yet. Drop the index, perform the modifications and recreate the index to work around this.";



// TODO: maybe add a NO_CHANGE to signal that the rowid was deleted but the bounds did not change
enum class RTreeNodeDeleteResult {
	NOT_FOUND = 0,
	MODIFIED,
	DELETED,
};

static RTreeNodeDeleteResult DeleteRecursive(RTreeIndex &rtree, const Box2D<float> &bounds, const row_t& rowid, RTreeEntry &entry) {
	D_ASSERT(entry.pointer.IsSet());

	if(entry.pointer.IsRowId() && entry.pointer.GetRowId() == rowid) {
		entry.Clear();
		return RTreeNodeDeleteResult::DELETED;
	}
	if(entry.pointer.IsPage() && entry.bounds.Intersects(bounds)) {
		auto &node = RTreePointer::RefMutable(rtree, entry.pointer);
		auto result = RTreeNodeDeleteResult::NOT_FOUND;

		idx_t last_idx = RTreeNode::CAPACITY;
		idx_t found_idx = 0;

		for(idx_t i = 0; i < last_idx; i++) {
			if(!node.entries[i].IsSet()) {
				// This is the end of the valid entries.
				last_idx = i;
				break;
			}
			if(result == RTreeNodeDeleteResult::NOT_FOUND) {
				result = DeleteRecursive(rtree, bounds, rowid, node.entries[i]);
				found_idx = i;
			}
		}

		// Was the rowid found?
		if(result == RTreeNodeDeleteResult::NOT_FOUND) {
			node.Verify();
			return RTreeNodeDeleteResult::NOT_FOUND;
		}

		if(result == RTreeNodeDeleteResult::DELETED) {
			// Did we delete the last child?
			if(found_idx == 0) {
				// We did, free the node and clear the entry
				RTreePointer::Free(rtree, entry.pointer);
				entry.Clear();

				node.Verify();
				return RTreeNodeDeleteResult::DELETED;
			}

			// Otherwise, swap the last entry with the deleted entry
			// (it has already been zeroed out in the recursive call)
			std::swap(node.entries[found_idx], node.entries[--last_idx]);
		}

		// We've modified the bounding box, update it
		entry.bounds = node.GetBounds();
		node.Verify();
		return RTreeNodeDeleteResult::MODIFIED;
	}

	return RTreeNodeDeleteResult::NOT_FOUND;
}

void RTreeIndex::Delete(IndexLock &lock, DataChunk &input, Vector &rowid_vec) {
	const auto count = input.size();

	UnifiedVectorFormat geom_format;
	UnifiedVectorFormat rowid_format;

	input.data[0].ToUnifiedFormat(count, geom_format);
	rowid_vec.ToUnifiedFormat(count, rowid_format);

	for(idx_t i = 0; i < count; i++) {
		const auto geom_idx = geom_format.sel->get_index(i);
		const auto rowid_idx = rowid_format.sel->get_index(i);

		if(!geom_format.validity.RowIsValid(geom_idx) || !rowid_format.validity.RowIsValid(rowid_idx)) {
			continue;
		}

		auto &geom = UnifiedVectorFormat::GetData<geometry_t>(geom_format)[geom_idx];
		auto &rowid = UnifiedVectorFormat::GetData<row_t>(rowid_format)[rowid_idx];

		Box2D<double> raw_bounds;
		if(!geom.TryGetCachedBounds(raw_bounds)) {
			continue;
		}

		Box2D<float> approx_bounds;
		approx_bounds.min.x = MathUtil::DoubleToFloatDown(raw_bounds.min.x);
		approx_bounds.min.y = MathUtil::DoubleToFloatDown(raw_bounds.min.y);
		approx_bounds.max.x = MathUtil::DoubleToFloatUp(raw_bounds.max.x);
		approx_bounds.max.y = MathUtil::DoubleToFloatUp(raw_bounds.max.y);

		auto &root = RTreePointer::RefMutable(*this, RTreePointer(root_block_ptr));
		bool found = false;
		idx_t found_idx = 0;
		idx_t last_idx = RTreeNode::CAPACITY;
		for(idx_t entry_idx = 0; entry_idx < last_idx; entry_idx++) {
			auto &entry = root.entries[entry_idx];
			if(!entry.IsSet()) {
				last_idx = entry_idx;
				break;
			}
			if(!found) {
				if(DeleteRecursive(*this, approx_bounds, rowid, entry) != RTreeNodeDeleteResult::NOT_FOUND) {
					found = true;
					found_idx = entry_idx;
				}
			}
		}
		// Swap	the last entry with the deleted entry (to keep the entries contiguous)
		if(found) {
			std::swap(root.entries[found_idx], root.entries[--last_idx]);
		}

		root.Verify();

		D_ASSERT(found);
	}
}

void RTreePointer::Free(RTreeIndex &index, RTreePointer &ptr) {
	if(ptr.IsRowId()) {
		// Nothing to do here
		return;
	}
	auto &node = RTreePointer::RefMutable(index, ptr);
	for(auto &entry : node.entries) {
		if(entry.IsSet()) {
			Free(index, entry.pointer);
		}
	}
	index.node_allocator->Free(ptr);
}

ErrorData RTreeIndex::Insert(IndexLock &lock, DataChunk &input, Vector &rowid_vec) {
	const NotImplementedException ex(NOT_IMPLEMENTED_MESSAGE);
	return ErrorData {ex};
}

ErrorData RTreeIndex::Append(IndexLock &lock, DataChunk &appended_data, Vector &row_identifiers) {
	return Insert(lock, appended_data, row_identifiers);
}

void RTreeIndex::VerifyAppend(DataChunk &chunk) {
	// There is nothing to verify here as we dont support constraints anyway
}

void RTreeIndex::VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) {
	// There is nothing to verify here as we dont support constraints anyway
}

IndexStorageInfo RTreeIndex::GetStorageInfo(const bool get_buffers) {

	IndexStorageInfo info;
	info.name = name;
	info.root = root_block_ptr.Get();

	if (!get_buffers) {
		// use the partial block manager to serialize all allocator data
		auto &block_manager = table_io_manager.GetIndexBlockManager();
		PartialBlockManager partial_block_manager(block_manager, PartialBlockType::FULL_CHECKPOINT);
		node_allocator->SerializeBuffers(partial_block_manager);
		partial_block_manager.FlushPartialBlocks();
	} else {
		info.buffers.push_back(node_allocator->InitSerializationToWAL());
	}

	info.allocator_infos.push_back(node_allocator->GetInfo());
	return info;
}

idx_t RTreeIndex::GetInMemorySize(IndexLock &state) {
	return node_allocator->GetInMemorySize();
}

bool RTreeIndex::MergeIndexes(IndexLock &state, BoundIndex &other_index) {
	throw NotImplementedException("RTreeIndex::MergeIndexes() not implemented");
}

void RTreeIndex::Vacuum(IndexLock &state) {
}

void RTreeIndex::CheckConstraintsForChunk(DataChunk &input, ConflictManager &conflict_manager) {
	throw NotImplementedException("RTreeIndex::CheckConstraintsForChunk() not implemented");
}

string RTreeIndex::VerifyAndToString(IndexLock &state, const bool only_verify) {
	throw NotImplementedException("RTreeIndex::VerifyAndToString() not implemented");
}

//------------------------------------------------------------------------------
// Register Index Type
//------------------------------------------------------------------------------
void RTreeModule::RegisterIndex(DatabaseInstance &db) {

	IndexType index_type;

	index_type.name = RTreeIndex::TYPE_NAME;
	index_type.create_instance = [](CreateIndexInput &input) -> unique_ptr<BoundIndex> {
		auto res = make_uniq<RTreeIndex>(input.name, input.constraint_type, input.column_ids, input.table_io_manager,
		                                input.unbound_expressions, input.db, input.options, input.storage_info);
		return std::move(res);
	};

	// Register the index type
	db.config.GetIndexTypes().RegisterIndexType(index_type);
}

} // namespace core

} // namespace spatial