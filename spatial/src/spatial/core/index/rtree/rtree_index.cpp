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


// TODO: maybe add a NO_CHANGE to signal that the rowid was deleted but the bounds did not change
enum class RTreeNodeDeleteResult {
	NOT_FOUND = 0,
	MODIFIED,
	DELETED,
};

static RTreeNodeDeleteResult DeleteRecursive(RTreeIndex &rtree, const Box2D<float> &bounds, const row_t& rowid, RTreeEntry &entry, vector<RTreePointer> &orphans) {
	D_ASSERT(entry.pointer.IsSet());
	if(entry.pointer.IsRowId() && entry.pointer.GetRowId() == rowid) {
		entry.Clear();
		return RTreeNodeDeleteResult::DELETED;
	}
	D_ASSERT(entry.pointer.IsPage());
	if(entry.bounds.Intersects(bounds)) {
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
				result = DeleteRecursive(rtree, bounds, rowid, node.entries[i], orphans);
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
			if(found_idx == 0 && last_idx == 1) {
				// We did, free the node and clear the entry
				RTreePointer::Free(rtree, entry.pointer);
				entry.Clear();

				node.Verify();
				return RTreeNodeDeleteResult::DELETED;
			}

			// Otherwise, swap the last entry with the deleted entry
			// (it has already been zeroed out in the recursive call)
			std::swap(node.entries[found_idx], node.entries[--last_idx]);

			// Is this branch now an orphan?
			if(last_idx < RTreeNode::CAPACITY / 2) {
				// We have too few children, add this node to the orphans list

				orphans.push_back(entry.pointer);
				// Clear the entry
				entry.Clear();

				// We mark this as deleted, as we want to reinsert it into the root.
				return RTreeNodeDeleteResult::DELETED;
			}
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

		// Orphans
		vector<RTreePointer> orphans;

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
				if(DeleteRecursive(*this, approx_bounds, rowid, entry, orphans) != RTreeNodeDeleteResult::NOT_FOUND) {
					found = true;
					found_idx = entry_idx;
				}
			}
		}
		// Swap	the last entry with the deleted entry (to keep the entries contiguous)
		if(found) {
			std::swap(root.entries[found_idx], root.entries[--last_idx]);
		}

		for(auto &orphan : orphans) {
			// Reinsert the orphan into the root
			// TODO: Handle deletes
			// Insert(*this, root, orphan);
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


//------------------------------------------------------------------------------
// Insert
//------------------------------------------------------------------------------
static idx_t GetSmallestEnlargment(const RTreeEntry entries[], const RTreeEntry &new_entry) {
	idx_t best_match = 0;
	auto best_diff = NumericLimits<float>::Maximum();

	for(idx_t i = 0; i < RTreeNode::CAPACITY; i++) {
		auto &entry = entries[i];
		if(entry.IsSet()) {
			const auto old_area = entry.bounds.Area();
			auto new_bounds = entry.bounds;
			new_bounds.Union(new_entry.bounds);
			const auto new_area = new_bounds.Area() - entry.bounds.Area();

			const auto diff = new_area - old_area;
			if(diff < best_diff) {
				best_diff = diff;
				best_match = i;
			}
		} else {
			break;
		}
	}

	return best_match;
}


static RTreeEntry NodeSplit(RTreeIndex &rtree, RTreeEntry &entry) {
	auto &page = RTreePointer::RefMutable(rtree, entry.pointer);

	// How many valid entries the page has
	idx_t entry_count = 0;

	// The two entries that are the farthest apart
	idx_t left_idx = 0;
	idx_t right_idx = 1;

	auto diff = NumericLimits<float>::Minimum();
	for (auto dim = 0; dim < 2; dim++)
	{
		entry_count = 0;

		idx_t min_child_idx = 0;
		idx_t max_child_idx = 0;

		auto min_value = page.entries[min_child_idx].bounds.min[dim];
		auto max_value = page.entries[max_child_idx].bounds.max[dim];

		for (idx_t child_idx = 1; child_idx < RTreeNode::CAPACITY; child_idx++)
		{
			auto &child = page.entries[child_idx];
			if(child.IsSet()) {
				if (child.bounds.min[dim] > page.entries[min_child_idx].bounds.min[dim]) {
					min_child_idx = child_idx;
				}
				if (child.bounds.max[dim] < page.entries[max_child_idx].bounds.max[dim]) {
					max_child_idx = child_idx;
				}
				min_value = std::min(child.bounds.min[dim], min_value);
				max_value = std::max(child.bounds.max[dim], max_value);
			} else {
				break;
			}
			entry_count++;
		}

		// Normalize the distance between the two children by the length of the bounding box
		const auto dist = page.entries[min_child_idx].bounds.min[dim] - page.entries[max_child_idx].bounds.max[dim];
		auto len = max_value - min_value;
		if (len <= 0) {
			len = 1;
		}
		const auto norm = dist / len;
		if (norm > diff) {
			left_idx = max_child_idx;
			right_idx = min_child_idx;
			diff = norm;
		}
	}

	if (left_idx == right_idx)
	{
		if (right_idx == 0) {
			right_idx++;
		} else {
			right_idx--;
		}
	}

	// We now have the two entries that are the farthest apart
	auto &left_entries = page.entries;

	// Create a new page for the right entries
	RTreePointer new_page_ptr;
	auto &right_page = RTreePointer::NewPage(rtree, new_page_ptr, entry.pointer.GetType());
	auto &right_entries = right_page.entries;

	// Move the two farthest entries to each page
	right_entries[0] = left_entries[right_idx];
	left_entries[right_idx] = left_entries[entry_count--];

	left_entries[0] = left_entries[left_idx];
	left_entries[left_idx] = left_entries[entry_count--];

	// How many entries to move
	const auto half = entry_count / 2;

	// Move the second half of the entries to the new right page
	memcpy(right_entries + 1, left_entries + half, half * sizeof(RTreeEntry));

	// Clear the second half of the left page
	memset(left_entries + half, 0, half * sizeof(RTreeEntry));

	// Recalculate the bounding box for the left page
	entry.bounds = page.GetBounds();

	// Return the new right page
	return { new_page_ptr, right_page.GetBounds() };
}

struct InsertResult {
	bool split;
	bool grown;
};

InsertResult NodeInsert(RTreeIndex &rtree, RTreeEntry &n, const RTreeEntry &new_entry) {
	auto &node = RTreePointer::RefMutable(rtree, n.pointer);
	auto count = node.GetCount();

	if(n.pointer.IsLeafPage()) {
		// Is this leaf full?
		if(count == RTreeNode::CAPACITY) {
			return InsertResult { true, false };
		}
		// Otherwise, insert at the end
		node.entries[count++] = new_entry;

		// Do we need to grow the bounding box?
		const auto grown = !n.bounds.Contains(new_entry.bounds);

		return InsertResult { false, grown };
	}

	// choose a subtree
	// TODO: Optimize: if any child contains the new bbox completely, pick that immediately
	const auto idx = GetSmallestEnlargment(node.entries, new_entry);

	// Insert into the selected child
	const auto result = NodeInsert(rtree, node.entries[idx], new_entry);
	if(result.split) {

		if(count == RTreeNode::CAPACITY) {
			// This node is also full!, we need to split it first.
			return InsertResult { true, false };
		}

		// Otherwise, split the selected child
		auto &left = node.entries[idx];
		auto right = NodeSplit(rtree, left);

		// Insert the new right node into the current node
		node.entries[count++] = right;

		// Now insert again
		return NodeInsert(rtree, n, new_entry);
	}

	if(result.grown) {
		// The child rectangle must expand to accomadate the new item.
		n.bounds.Union(new_entry.bounds);

		// Do we need to grow the bounding box?
		const auto grown = !n.bounds.Contains(new_entry.bounds);

		return InsertResult { false, grown };
	}

	// Otherwise, this was a clean insert.
	return InsertResult { false, false };
}

static void RootInsert(RTreeIndex &rtree, RTreeEntry &root_entry, const RTreeEntry &new_entry) {
	// Insert the new entry into the root node
	const auto result = NodeInsert(rtree, root_entry, new_entry);
	if(result.split) {
		// The root node was split, we need to create a new root node
		RTreePointer new_root_ptr;
		auto &new_root = RTreePointer::NewPage(rtree, new_root_ptr, RTreeNodeType::BRANCH_PAGE);

		// Insert the old root into the new root
		new_root.entries[0] = root_entry;

		// Split the old root
		auto right = NodeSplit(rtree, new_root.entries[0]);

		// Insert the new right node into the new root
		new_root.entries[1] = right;

		// Update the root pointer
		root_entry.pointer = new_root_ptr;

		// Insert the new entry into the new root now that we have space
		RootInsert(rtree, root_entry, new_entry);
	}

	if(result.grown) {
		// Update the root bounding box
		root_entry.bounds.Union(new_entry.bounds);
	}
}

ErrorData RTreeIndex::Insert(IndexLock &lock, DataChunk &input, Vector &rowid_vec) {
	// TODO: Dont flatten chunk
	input.Flatten();

	const auto &geom_vec = FlatVector::GetData<geometry_t>(input.data[0]);
	const auto &rowid_data = FlatVector::GetData<row_t>(rowid_vec);
	const auto &root = RTreePointer::RefMutable(*this, RTreePointer(root_block_ptr));

	RTreeEntry root_entry = { RTreePointer(root_block_ptr), root.GetBounds() };

	for(idx_t i = 0; i < input.size(); i++) {
		const auto rowid = rowid_data[i];

		Box2D<double> box_2d;
		if(!geom_vec[i].TryGetCachedBounds(box_2d)) {
			continue;
		}

		Box2D<float> bbox;
		bbox.min.x = MathUtil::DoubleToFloatDown(box_2d.min.x);
		bbox.min.y = MathUtil::DoubleToFloatDown(box_2d.min.y);
		bbox.max.x = MathUtil::DoubleToFloatUp(box_2d.max.x);
		bbox.max.y = MathUtil::DoubleToFloatUp(box_2d.max.y);

		RTreeEntry new_entry = { RTreePointer::NewRowId(rowid), bbox };
		RootInsert(*this, root_entry, new_entry);
	}

	// If a new root was created, update the root block pointer
	root_block_ptr = root_entry.pointer;

	return ErrorData{};
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