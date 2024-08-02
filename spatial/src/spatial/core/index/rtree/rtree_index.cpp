#include "spatial/core/index/rtree/rtree_index.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "spatial/core/index/rtree/rtree_module.hpp"
#include "spatial/core/index/rtree/rtree_node.hpp"
#include "spatial/core/geometry/geometry_type.hpp"
#include "spatial/core/util/math.hpp"

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
RTreeIndex::RTreeIndex(const string &name, IndexConstraintType index_constraint_type,
                       const vector<column_t> &column_ids, TableIOManager &table_io_manager,
                       const vector<unique_ptr<Expression>> &unbound_expressions, AttachedDatabase &db,
                       const case_insensitive_map_t<Value> &options, const IndexStorageInfo &info,
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
		// Initialize the allocators
		node_allocator->Init(info.allocator_infos[0]);
		// Set the root node
		root_entry.pointer.Set(info.root);
		// Recalculate the bounds
		root_entry.bounds = RTreePointer::Ref(*this, root_entry.pointer).GetBounds();
	} else {
		// For now, we just create an empty root node
		// TODO: Create a root node.
	}
}

unique_ptr<IndexScanState> RTreeIndex::InitializeScan(const RTreeBounds &query) const {
	auto state = make_uniq<RTreeIndexScanState>();
	state->query_bounds = query;
	if(root_entry.IsSet() && state->query_bounds.Intersects(root_entry.bounds)) {
		state->stack.emplace_back(root_entry.pointer);
	}
	return std::move(state);
}

idx_t RTreeIndex::Scan(IndexScanState &state_p, Vector &result) const {
	auto &state = state_p.Cast<RTreeIndexScanState>();

	idx_t total_scanned = 0;
	const auto row_ids = FlatVector::GetData<row_t>(result);

	while (!state.stack.empty()) {
		auto &ptr = state.stack.back();
		if (ptr.IsRowId()) {
			// Its a leaf! Collect the row id
			row_ids[total_scanned++] = ptr.GetRowId();
			state.stack.pop_back();
			if (total_scanned == STANDARD_VECTOR_SIZE) {
				// We've filled the result vector, yield!
				return total_scanned;
			}
		} else {
			// Its a page! Add the valid intersecting children to the stack and continue
			auto &node = RTreePointer::Ref(*this, ptr);
			// Even though we modify the stack, we've already dereferenced the current node
			// so its ok if the ptr gets invalidated when we pop_back();
			state.stack.pop_back();
			for (auto &entry : node.entries) {
				if (entry.IsSet()) {
					if (entry.bounds.Intersects(state.query_bounds)) {
						state.stack.emplace_back(entry.pointer);
					}
				} else {
					break;
				}
			}
		}
	}
	return total_scanned;
}

void RTreeIndex::CommitDrop(IndexLock &index_lock) {
	// TODO: Maybe we can drop these much earlier?
	node_allocator->Reset();
	root_entry.Clear();
	root_entry.bounds = RTreeBounds();
}

//------------------------------------------------------------------------------
// Insert
//------------------------------------------------------------------------------
static RTreeEntry& PickSubtree(RTreeEntry entries[], const RTreeEntry &new_entry) {
	idx_t best_match = 0;
	float best_area = NumericLimits<float>::Maximum();
	float best_diff = NumericLimits<float>::Maximum();

	for(idx_t i = 0; i < RTreeNode::CAPACITY; i++) {
		auto &entry = entries[i];
		if(!entry.IsSet()) {
			break;
		}

		// Optimization: if the new entry is completely contained in a subtree, return that immediately
		if(entry.bounds.Contains(new_entry.bounds)) {
			return entry;
		}

		auto &old_bounds = entry.bounds;
		auto new_bounds = old_bounds;
		new_bounds.Union(new_entry.bounds);

		const auto old_area = old_bounds.Area();
		const auto new_area = new_bounds.Area();
		const auto diff = new_area - old_area;
		if(diff < best_diff || (diff <= best_diff && old_area < best_area)) {
			best_diff = diff;
			best_area = old_area;
			best_match = i;
		}
	}
	return entries[best_match];
}

static RTreeEntry SplitNode(RTreeIndex &rtree, RTreeEntry &entry) {
	auto &left_page = RTreePointer::RefMutable(rtree, entry.pointer);

	D_ASSERT(left_page.GetCount() == RTreeNode::CAPACITY);

	auto &left_entries = left_page.entries;

	// The two entries that are the farthest apart
	idx_t left_idx = 0;
	idx_t right_idx = 1;

	auto diff = NumericLimits<float>::Minimum();
	for (auto dim = 0; dim < 2; dim++) {
		idx_t min_child_idx = 0;
		idx_t max_child_idx = 0;

		auto min_value = left_page.entries[min_child_idx].bounds.min[dim];
		auto max_value = left_page.entries[max_child_idx].bounds.max[dim];

		for (idx_t child_idx = 1; child_idx < RTreeNode::CAPACITY; child_idx++) {
			auto &child = left_page.entries[child_idx];
			if (child.IsSet()) {
				if (child.bounds.min[dim] > left_page.entries[min_child_idx].bounds.min[dim]) {
					min_child_idx = child_idx;
				}
				if (child.bounds.max[dim] < left_page.entries[max_child_idx].bounds.max[dim]) {
					max_child_idx = child_idx;
				}
				min_value = std::min(child.bounds.min[dim], min_value);
				max_value = std::max(child.bounds.max[dim], max_value);
			} else {
				break;
			}
		}

		// Normalize the distance between the two children by the length of the bounding box
		const auto dist =
		    left_page.entries[min_child_idx].bounds.min[dim] - left_page.entries[max_child_idx].bounds.max[dim];
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

	if (left_idx == right_idx) {
		if (right_idx == 0) {
			right_idx++;
		} else {
			right_idx--;
		}
	}

	// We now have the two entries that are the farthest apart
	// Create a new page for the right entries
	RTreePointer new_page_ptr;
	auto &right_page = RTreePointer::NewPage(rtree, new_page_ptr, entry.pointer.GetType());
	auto &right_entries = right_page.entries;

	// Move the right seed entry to the new right page
	right_entries[0] = left_entries[right_idx];

	// Move the left idx so that it is in the first half of the entries
	const auto left_swap_idx = right_idx == 0 ? 1 : 0;
	std::swap(left_entries[left_idx], left_entries[left_swap_idx]);

	// Compact the left page by swapping the right seed entry with the last entry
	left_entries[right_idx] = left_entries[RTreeNode::CAPACITY - 1];

	// How many entries to move
	constexpr auto half = RTreeNode::CAPACITY / 2;

	// Copy the second half of the entries to the new right page
	// (except the last entry, which was already moved during compaction)
	memcpy(right_entries + 1, left_entries + half, (half - 1) * sizeof(RTreeEntry));

	// Clear the second half of the entries in the left page
	memset(left_entries + half, 0, half * sizeof(RTreeEntry));

	// Recalculate the bounding box for the left page
	entry.bounds = left_page.GetBounds();

	// Asserts
	D_ASSERT(left_page.GetCount() == right_page.GetCount());
	left_page.Verify();
	right_page.Verify();

	// Return the new right page
	return {new_page_ptr, right_page.GetBounds()};
}

struct InsertResult {
	bool split;
	bool grown;
};

// Insert a rowid into a node
InsertResult NodeInsert(RTreeIndex &rtree, RTreeEntry &entry, const RTreeEntry &new_entry) {
	D_ASSERT(new_entry.pointer.IsRowId());

	auto &node = RTreePointer::RefMutable(rtree, entry.pointer);

	// Is this a leaf?
	if (entry.pointer.IsLeafPage()) {
		auto count = node.GetCount();
		// Is this leaf full?
		if (count == RTreeNode::CAPACITY) {
			return InsertResult {true, false};
		}
		// Otherwise, insert at the end
		node.entries[count++] = new_entry;

		// Do we need to grow the bounding box?
		const auto grown = !entry.bounds.Contains(new_entry.bounds);

		return InsertResult {false, grown};
	}

	// Otherwise: this is a branch node
	D_ASSERT(entry.pointer.IsBranchPage());

	// Choose a subtree
	auto &target = PickSubtree(node.entries, new_entry);

	// Insert into the selected child
	const auto result = NodeInsert(rtree, target, new_entry);
	if (result.split) {
		auto count = node.GetCount();
		if (count == RTreeNode::CAPACITY) {
			// This node is also full!, we need to split it first.
			return InsertResult {true, false};
		}

		// Otherwise, split the selected child
		auto &left = target;
		auto right = SplitNode(rtree, left);

		// Insert the new right node into the current node
		node.entries[count++] = right;

		// Sort all entries by the x-coordinate
		std::sort(node.entries, node.entries + count, [&](const RTreeEntry &a, const RTreeEntry &b) {
			D_ASSERT(a.IsSet());
			D_ASSERT(b.IsSet());
			return a.bounds.min.x < b.bounds.min.x;
		});

		// Now insert again
		return NodeInsert(rtree, entry, new_entry);
	}

	if (result.grown) {
		// Update the bounding box of the child
		target.bounds.Union(new_entry.bounds);

		// Do we need to grow the bounding box?
		const auto grown = !entry.bounds.Contains(new_entry.bounds);

		return InsertResult {false, grown};
	}

	// Otherwise, this was a clean insert.
	return InsertResult {false, false};
}

static void RootInsert(RTreeIndex &rtree, RTreeEntry &root_entry, const RTreeEntry &new_entry) {
	// If there is no root node, create one and insert the new entry immediately
	if (!root_entry.IsSet()) {
		auto &node = RTreePointer::NewPage(rtree, root_entry.pointer, RTreeNodeType::LEAF_PAGE);
		root_entry.bounds = new_entry.bounds;
		node.entries[0] = new_entry;
		return;
	}

	// Insert the new entry into the root node
	const auto result = NodeInsert(rtree, root_entry, new_entry);
	if (result.split) {
		// The root node was split, we need to create a new root node
		RTreePointer new_root_ptr;
		auto &new_root = RTreePointer::NewPage(rtree, new_root_ptr, RTreeNodeType::BRANCH_PAGE);

		// Insert the old root into the new root
		new_root.entries[0] = root_entry;

		// Split the old root
		auto right = SplitNode(rtree, new_root.entries[0]);

		// Insert the new right node into the new root
		new_root.entries[1] = right;

		// Update the root pointer
		root_entry.pointer = new_root_ptr;

		// Insert the new entry into the new root now that we have space
		RootInsert(rtree, root_entry, new_entry);
	}

	if (result.grown) {
		// Update the root bounding box
		root_entry.bounds.Union(new_entry.bounds);
	}
}

ErrorData RTreeIndex::Insert(IndexLock &lock, DataChunk &input, Vector &rowid_vec) {
	// TODO: Dont flatten chunk
	input.Flatten();

	const auto &geom_vec = FlatVector::GetData<geometry_t>(input.data[0]);
	const auto &rowid_data = FlatVector::GetData<row_t>(rowid_vec);

	RTreeEntry entry_buffer[STANDARD_VECTOR_SIZE];

	for (idx_t i = 0; i < input.size(); i++) {
		const auto rowid = rowid_data[i];

		Box2D<double> box_2d;
		if (!geom_vec[i].TryGetCachedBounds(box_2d)) {
			continue;
		}

		Box2D<float> bbox;
		bbox.min.x = MathUtil::DoubleToFloatDown(box_2d.min.x);
		bbox.min.y = MathUtil::DoubleToFloatDown(box_2d.min.y);
		bbox.max.x = MathUtil::DoubleToFloatUp(box_2d.max.x);
		bbox.max.y = MathUtil::DoubleToFloatUp(box_2d.max.y);

		entry_buffer[i] = {RTreePointer::NewRowId(rowid), bbox};
	}

	// TODO: Investigate this more, is there a better way to insert multiple entries
	// so that they produce a better tree?
	// E.g. sort by x coordinate, or hilbert sort? or STR packing?
	// Or insert by smallest first? or largest first?
	// Or even create a separate subtree entirely, and then insert that into the root?
	for(idx_t i = 0; i < input.size(); i++) {
		RootInsert(*this, root_entry, entry_buffer[i]);
	}

	return ErrorData {};
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

//------------------------------------------------------------------------------
// Delete
//------------------------------------------------------------------------------
struct DeleteResult {

	// Whether or not the rowid was found and cleared
	bool found;
	// Whether or not the node shrunk
	bool shrunk;
	// Whether or not the node is now empty or underfull and should be removed
	bool remove;

	DeleteResult(const bool found_p, const bool shrunk_p, const bool remove_p )
		: found(found_p), shrunk(shrunk_p), remove(remove_p) {
	}
};

static DeleteResult NodeDelete(RTreeIndex &rtree, RTreeEntry &entry, const RTreeEntry &target, vector<RTreeEntry> &orphans) {
	if(entry.pointer.IsLeafPage()) {
		auto &node = RTreePointer::RefMutable(rtree, entry.pointer);

		auto child_idx_opt = optional_idx::Invalid();
		idx_t child_count = 0;
		for(; child_count < RTreeNode::CAPACITY; child_count++) {
			auto &child = node.entries[child_count];
			if(!child.IsSet()) {
				break;
			}
			D_ASSERT(child.pointer.IsRowId());
			if(child.pointer.GetRowId() == target.pointer.GetRowId()) {
				D_ASSERT(!child_idx_opt.IsValid());
				child_idx_opt = child_count;
			}
		}

		// Found the target entry
		if(child_idx_opt.IsValid()) {
			// Found the target entry
			const auto child_idx = child_idx_opt.GetIndex();
			if(child_idx == 0 && child_count == 1) {
				// This is the only entry in the leaf, we can remove the entire leaf
				node.entries[0].Clear();
				return DeleteResult {true, true, true};
			}

			// Overwrite the target entry with the last entry and clear the last entry
			// to compact the leaf
			node.entries[child_idx] = node.entries[child_count - 1];
			node.entries[child_count - 1].Clear();

			// Does this node now have too few children?
			if(child_count < RTreeNode::CAPACITY / 2) {
				// Yes, orphan all children and signal that this node should be removed
				for(idx_t i = 0; i < child_count; i++) {
					orphans.push_back(node.entries[i]);
				}
				return {true, true, true};
			}

			// TODO: Check if we actually shrunk and need to update the bounds
			// We can do this more efficiently by checking if the deleted entry bordered the bounds
			// Recalculate the bounds
			auto shrunk = true;
			if(shrunk) {
				const auto old_bounds = entry.bounds;
				entry.bounds = node.GetBounds();
				shrunk = entry.bounds != old_bounds;

				// Assert that the bounds shrunk
				D_ASSERT(entry.bounds.Area() <= old_bounds.Area());
			}
			return DeleteResult {true, shrunk, false};
		}

		// Not found in this leaf
		return { false, false, false};
	}
	// Otherwise: this is a branch node
	D_ASSERT(entry.pointer.IsBranchPage());
	auto &node = RTreePointer::RefMutable(rtree, entry.pointer);

	DeleteResult result = { false, false, false };

	idx_t child_count = 0;
	idx_t child_idx = 0;
	for(; child_count < RTreeNode::CAPACITY; child_count++) {
		auto &child = node.entries[child_count];
		if(!child.IsSet()) {
			break;
		}
		if(!result.found) {
			result = NodeDelete(rtree, child, target, orphans);
			if(result.found) {
				child_idx = child_count;
			}
		}
	}

	// Did we find the target entry?
	if(!result.found) {
		return result;
	}

	// Should we delete the child entirely?
	if(result.remove) {
		// If this is the only child in the branch, we can remove the entire branch
		if(child_idx == 0 && child_count == 1) {
			RTreePointer::Free(rtree, node.entries[0].pointer);
			return {true, true, true};
		}

		// Swap the removed entry with the last entry and clear it
		std::swap(node.entries[child_idx], node.entries[child_count - 1]);
		RTreePointer::Free(rtree, node.entries[child_count - 1].pointer);;
		child_count--;

		// Does this node now have too few children?
		if(child_count < RTreeNode::CAPACITY / 2) {
			// Yes, orphan all children and signal that this node should be removed
			for(idx_t i = 0; i < child_count; i++) {
				orphans.push_back(node.entries[i]);
			}
			return {true, true, true};
		}

		// Recalculate the bounding box
		// We most definitely shrunk if we removed a child
		// TODO: Do we always need to do this?
		auto shrunk = true;
		if(shrunk) {
			const auto old_bounds = entry.bounds;
			entry.bounds = node.GetBounds();
			shrunk = entry.bounds != old_bounds;

			// Assert that the bounds shrunk
			D_ASSERT(entry.bounds.Area() <= old_bounds.Area());
		}
		return {true, shrunk, false};
	}

	// Check if we shrunk
	// TODO: Compare the bounds before and after the delete?
	auto shrunk = result.shrunk;
	if(shrunk) {
		const auto old_bounds  = entry.bounds;
		entry.bounds = node.GetBounds();
		shrunk = entry.bounds != old_bounds;

		D_ASSERT(entry.bounds.Area() <= old_bounds.Area());
	}

	return { true, shrunk, false };
}

static void ReInsertNode(RTreeIndex &rtree, RTreeEntry &root, const RTreeEntry &target) {
	D_ASSERT(target.IsSet());
	auto &node = RTreePointer::Ref(rtree, target.pointer);
	if(target.pointer.IsLeafPage()) {
		// Insert all the entries
		for(const auto & entry : node.entries) {
			if(!entry.IsSet()) {
				break;
			}
			D_ASSERT(entry.pointer.IsRowId());
			RootInsert(rtree, root, entry);
		}
	}
	else {
		D_ASSERT(target.pointer.IsBranchPage());
		for(const auto & entry : node.entries) {
			if(!entry.IsSet()) {
				break;
			}
			ReInsertNode(rtree, root, entry);
		}
	}
}

static void RootDelete(RTreeIndex &rtree, RTreeEntry &root, const RTreeEntry &target) {
	D_ASSERT(root.pointer.IsSet());

	vector<RTreeEntry> orphans;
	const auto result = NodeDelete(rtree, root, target, orphans);

	// We at least found the target entry
	D_ASSERT(result.found);

	// If the node was removed, we need to clear the root
	// but if there are orphans we keep the root around.
	if(result.remove) {
		root.bounds = RTreeBounds();
		if(orphans.empty()) {
			// The entire tree was removed, clear the root node
			RTreePointer::Free(rtree, root.pointer);
		} else {
			for(auto &orphan : orphans) {
				ReInsertNode(rtree, root, orphan);
			}
		}
	} else {
		if(result.shrunk) {
			// Update the root bounding box
			root.bounds = RTreePointer::Ref(rtree, root.pointer).GetBounds();
		}
		// Reinsert orphans
		for (auto &orphan : orphans) {
			ReInsertNode(rtree, root, orphan);
		}
	}
}

void RTreeIndex::Delete(IndexLock &lock, DataChunk &input, Vector &rowid_vec) {
	const auto count = input.size();

	UnifiedVectorFormat geom_format;
	UnifiedVectorFormat rowid_format;

	input.data[0].ToUnifiedFormat(count, geom_format);
	rowid_vec.ToUnifiedFormat(count, rowid_format);

	for (idx_t i = 0; i < count; i++) {
		const auto geom_idx = geom_format.sel->get_index(i);
		const auto rowid_idx = rowid_format.sel->get_index(i);

		if (!geom_format.validity.RowIsValid(geom_idx) || !rowid_format.validity.RowIsValid(rowid_idx)) {
			continue;
		}

		auto &geom = UnifiedVectorFormat::GetData<geometry_t>(geom_format)[geom_idx];
		auto &rowid = UnifiedVectorFormat::GetData<row_t>(rowid_format)[rowid_idx];

		Box2D<double> raw_bounds;
		if (!geom.TryGetCachedBounds(raw_bounds)) {
			continue;
		}

		Box2D<float> approx_bounds;
		approx_bounds.min.x = MathUtil::DoubleToFloatDown(raw_bounds.min.x);
		approx_bounds.min.y = MathUtil::DoubleToFloatDown(raw_bounds.min.y);
		approx_bounds.max.x = MathUtil::DoubleToFloatUp(raw_bounds.max.x);
		approx_bounds.max.y = MathUtil::DoubleToFloatUp(raw_bounds.max.y);

		RTreeEntry new_entry = {RTreePointer::NewRowId(rowid), approx_bounds};
		RootDelete(*this, root_entry, new_entry);
	}
}

IndexStorageInfo RTreeIndex::GetStorageInfo(const bool get_buffers) {

	IndexStorageInfo info;
	info.name = name;
	info.root = root_entry.pointer.Get();

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