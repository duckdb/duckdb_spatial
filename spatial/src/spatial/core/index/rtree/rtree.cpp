#include "spatial/core/index/rtree/rtree.hpp"
#include "duckdb/common/printer.hpp"

namespace spatial {

namespace core {

struct InsertResult {
	// Whether or not the node was split
	bool split;

	// Whether or not the node bounds grew
	bool grown;
};

struct DeleteResult {

	// Whether or not the rowid was found and cleared
	bool found;
	// Whether or not the node shrunk
	bool shrunk;
	// Whether or not the node is now empty or underfull and should be removed
	bool remove;

	DeleteResult(const bool found_p, const bool shrunk_p, const bool remove_p)
	    : found(found_p), shrunk(shrunk_p), remove(remove_p) {
	}
};

RTreePointer RTree::MakePage(RTreeNodeType type) const {
	D_ASSERT(type == RTreeNodeType::BRANCH_PAGE || type == RTreeNodeType::LEAF_PAGE);
	RTreePointer pointer;
	auto &alloc = type == RTreeNodeType::LEAF_PAGE ? *leaf_allocator : *node_allocator;
	pointer = alloc.New();
	pointer.SetMetadata(static_cast<uint8_t>(type));
	return pointer;
}

RTreePointer RTree::MakeRowId(row_t row_id) {
	RTreePointer pointer;
	pointer.SetMetadata(static_cast<uint8_t>(RTreeNodeType::ROW_ID));
	pointer.SetRowId(row_id);
	return pointer;
}

RTreeNode &RTree::RefMutable(const RTreePointer &pointer) const {
	auto &alloc = pointer.IsLeafPage() ? *leaf_allocator : *node_allocator;
	return *alloc.Get<RTreeNode>(pointer, true);
}

const RTreeNode &RTree::Ref(const RTreePointer &pointer) const {
	auto &alloc = pointer.IsLeafPage() ? *leaf_allocator : *node_allocator;
	return *alloc.Get<const RTreeNode>(pointer, false);
}

void RTree::Free(RTreePointer &pointer) {
	if (pointer.IsRowId()) {
		pointer.Clear();
		// Nothing to do here
		return;
	}
	auto &node = RefMutable(pointer);
	for (auto &entry : node) {
		Free(entry.pointer);
	}
	node.Clear();
	auto &alloc = pointer.IsLeafPage() ? *leaf_allocator : *node_allocator;
	alloc.Free(pointer);
	pointer.Clear();
}

//------------------------------------------------------------------------------
// Split
//------------------------------------------------------------------------------
void RTree::RebalanceSplitNodes(RTreeNode &src, RTreeNode &dst, bool split_axis, PointXY<float> &split_point) const {
	D_ASSERT(src.GetCount() > dst.GetCount());

	// How many entries to we need to move until we have the minimum capacity?
	const auto remaining = config.min_node_capacity - dst.GetCount();

	// Setup a min heap to keep track of the entries that are closest to the split point
	vector<pair<float, idx_t>> diff_heap;
	diff_heap.reserve(remaining);

	auto cmp = [&](const pair<float, idx_t> &a, const pair<float, idx_t> &b) {
		return a.first > b.first;
	};

	// Find the entries that are closest to the split point
	for (idx_t i = 0; i < src.GetCount(); i++) {
		auto center = src[i].bounds.Center();
		auto diff = std::abs(center[split_axis] - split_point[split_axis]);
		if (diff_heap.size() < remaining) {
			diff_heap.emplace_back(diff, i);
			if (diff_heap.size() == remaining) {
				std::make_heap(diff_heap.begin(), diff_heap.end(), cmp);
			}
		} else if (diff < diff_heap.front().first) {
			std::pop_heap(diff_heap.begin(), diff_heap.end(), cmp);
			diff_heap.back() = {diff, i};
			std::push_heap(diff_heap.begin(), diff_heap.end(), cmp);
		}
	}

	// Sort the heap by entry idx so that we move the entries from the largest idx to the smallest
	// Which allows us to SwapRemove all the ways
	std::sort(diff_heap.begin(), diff_heap.end(),
	          [&](const pair<float, idx_t> &a, const pair<float, idx_t> &b) { return a.second > b.second; });

	// Now move over the selected entries to the destination node
	for (const auto &pair : diff_heap) {
		dst.PushEntry(src.SwapRemove(pair.second));
	}
}

RTreeEntry RTree::SplitNode(RTreeEntry &entry) const {

	auto &left_node = RefMutable(entry.pointer);
	D_ASSERT(left_node.GetCount() == config.max_node_capacity);

	/*
	 *  C1 | C2
	 *  -------
	 *  C0 | C3
	 */

	// Split the entry bounding box into four quadrants
	RTreeBounds quadrants[4];
	auto center = entry.bounds.Center();
	quadrants[0] = {entry.bounds.min, center};
	quadrants[1] = {{entry.bounds.min.x, center.y}, {center.x, entry.bounds.max.y}};
	quadrants[2] = {center, entry.bounds.max};
	quadrants[3] = {{center.x, entry.bounds.min.y}, {entry.bounds.max.x, center.y}};

	idx_t q_counts[4] = {0, 0, 0, 0};
	RTreeBounds q_bounds[4];
	const auto q_assign = make_unsafe_uniq_array<uint8_t>(config.max_node_capacity);
	uint8_t q_node[4] = {0, 0, 0, 0};

	// Figure out which quadrant each entry in the node belongs to
	for (idx_t i = 0; i < config.max_node_capacity; i++) {
		auto child_center = left_node[i].bounds.Center();
		auto found = false;
		for (idx_t q_idx = 0; q_idx < 4; q_idx++) {
			if (quadrants[q_idx].Contains(child_center)) {
				q_counts[q_idx]++;
				q_assign[i] = q_idx;
				q_bounds[q_idx].Union(left_node[i].bounds);
				found = true;
				break;
			}
		}
		D_ASSERT(found);
		(void)found;
	}

	// Create a temporary node for the first split
	// Create a buffer to hold all the entries we are going to move
	const auto entry_buffer = make_unsafe_uniq_array<RTreeEntry>(config.max_node_capacity);
	for (idx_t i = 0; i < config.max_node_capacity; i++) {
		entry_buffer[i] = left_node[i];
	}
	left_node.Clear();

	auto right_ptr = MakePage(entry.pointer.GetType());
	auto &right_node = RefMutable(right_ptr);

	const reference<RTreeNode> node_ref[2] = {left_node, right_node};

	// Setup the first two nodes
	if (q_counts[0] > q_counts[2]) {
		q_node[0] = 0;
		q_node[2] = 1;
	} else if (q_counts[0] < q_counts[2]) {
		q_node[0] = 1;
		q_node[2] = 0;
	}

	// Setup the last two nodes
	if (q_counts[1] > q_counts[3]) {
		q_node[1] = 0;
		q_node[3] = 1;
	} else if (q_counts[1] < q_counts[3]) {
		q_node[1] = 1;
		q_node[3] = 0;
	} else {
		// Tie break! Select based on least overlap

		// The two bounds if we were to split from top to bottom
		const auto bounds_v_l = RTreeBounds::Union(q_bounds[0], q_bounds[1]);
		const auto bounds_v_r = RTreeBounds::Union(q_bounds[2], q_bounds[3]);

		// The two bounds if we were to split from left to right
		const auto bounds_h_t = RTreeBounds::Union(q_bounds[0], q_bounds[3]);
		const auto bounds_h_b = RTreeBounds::Union(q_bounds[1], q_bounds[2]);

		// How much overlap would each half have?
		const auto overlap_v = bounds_v_l.OverlapArea(bounds_v_r);
		const auto overlap_h = bounds_h_t.OverlapArea(bounds_h_b);

		if (overlap_h < overlap_v) {
			q_node[1] = q_node[0];
			q_node[3] = q_node[2];
		} else if (overlap_h > overlap_v) {
			q_node[1] = q_node[2];
			q_node[3] = q_node[0];
		} else {
			// Still a tie!, theres no overlap between the two splits!
			// Select based on what split would increase the area the least
			const auto area_v = bounds_v_l.Area() + bounds_v_r.Area();
			const auto area_h = bounds_h_t.Area() + bounds_h_b.Area();

			if (area_v < area_h) {
				q_node[1] = q_node[0];
				q_node[3] = q_node[2];
			} else {
				q_node[1] = q_node[2];
				q_node[3] = q_node[0];
			}
		}
	}

	// Distribute the entries to the two nodes
	for (idx_t i = 0; i < config.max_node_capacity; i++) {
		const auto q_idx = q_assign[i];
		const auto n_idx = q_node[q_idx];
		auto &dst = node_ref[n_idx];
		dst.get().PushEntry(entry_buffer[i]);
	}

	// IF we join c0 with c1, we split along the y-axis
	// If c0 has more entries than c2
	// - Join c0 with c1 or c3 depending on which has the least entries
	// else
	// - Join c0 with c1 or c3 depending on which has the most entries

	// If we join c0 with c1, we split the node top to bottom, so when we rebalance we want to move entries that are
	// closest to the split line, i.e. calculate the distance on the axis perpendicular to the split line
	const auto perp_split_axis = q_node[0] != q_node[1];

	// If one of the nodes have less than the minimum capacity, we need to move entries from the other node
	// but do so by moving the entries that are closest to the splitting line
	if (left_node.GetCount() < config.min_node_capacity) {
		RebalanceSplitNodes(right_node, left_node, perp_split_axis, center);
	} else if (right_node.GetCount() < config.min_node_capacity) {
		RebalanceSplitNodes(left_node, right_node, perp_split_axis, center);
	}

	D_ASSERT(left_node.GetCount() >= config.min_node_capacity);
	D_ASSERT(right_node.GetCount() >= config.min_node_capacity);

	// TODO: Reuse q_bounds if we didnt have to rebalance the nodes
	entry.bounds = left_node.GetBounds();

	// Sort both node entries on the min x-coordinate
	// This is not really necessary, but produces a slightly nicer tree
	if (entry.pointer.IsBranchPage()) {
		left_node.SortEntriesByXMin();
		right_node.SortEntriesByXMin();
	} else {
		// Or sort by rowid to maintain the order
		left_node.SortEntriesByRowId();
		right_node.SortEntriesByRowId();
	}

	left_node.Verify(config.max_node_capacity);
	right_node.Verify(config.max_node_capacity);

	// Return a new entry for the second node
	return RTreeEntry {right_ptr, right_node.GetBounds()};
}

//------------------------------------------------------------------------------
// Insert
//------------------------------------------------------------------------------
RTreeEntry &RTree::PickSubtree(RTreeNode &node, const RTreeEntry &new_entry) const {
	idx_t best_match = 0;
	float best_area = NumericLimits<float>::Maximum();
	float best_diff = NumericLimits<float>::Maximum();

	for (idx_t i = 0; i < node.GetCount(); i++) {
		auto &entry = node[i];

		auto &old_bounds = entry.bounds;
		auto new_bounds = RTreeBounds::Union(new_entry.bounds, old_bounds);

		const auto old_area = old_bounds.Perimeter();
		const auto new_area = new_bounds.Perimeter();
		const auto diff = new_area - old_area;
		if (diff < best_diff || (diff <= best_diff && old_area < best_area)) {
			best_diff = diff;
			best_area = old_area;
			best_match = i;
		}
	}
	return node[best_match];
}

// Insert a rowid into a node
InsertResult RTree::NodeInsert(RTreeEntry &entry, const RTreeEntry &new_entry) {
	D_ASSERT(new_entry.pointer.IsRowId());
	D_ASSERT(entry.pointer.Get() != 0);

	const auto is_leaf = entry.pointer.IsLeafPage();
	return is_leaf ? LeafInsert(entry, new_entry) : BranchInsert(entry, new_entry);
}

InsertResult RTree::LeafInsert(RTreeEntry &entry, const RTreeEntry &new_entry) {
	D_ASSERT(entry.pointer.IsLeafPage());

	// Dereference the node
	auto &node = RefMutable(entry.pointer);

	// Is this leaf full?
	if (node.GetCount() == config.max_node_capacity) {
		return InsertResult {true, false};
	}
	// Otherwise, insert at the end
	node.PushEntry(new_entry);

	// Sort by rowid
	node.SortEntriesByRowId();

	// Do we need to grow the bounding box?
	const auto grown = !entry.bounds.Contains(new_entry.bounds);

	return InsertResult {false, grown};
}

InsertResult RTree::BranchInsert(RTreeEntry &entry, const RTreeEntry &new_entry) {
	D_ASSERT(entry.pointer.IsBranchPage());

	// Dereference the node
	auto &node = RefMutable(entry.pointer);

	// Choose a subtree
	auto &target = PickSubtree(node, new_entry);

	D_ASSERT(target.pointer.Get() != 0);

	// Insert into the selected child
	const auto result = NodeInsert(target, new_entry);
	if (result.split) {
		if (node.GetCount() == config.max_node_capacity) {
			// This node is also full!, we need to split it first.
			return InsertResult {true, false};
		}

		// Otherwise, split the selected child
		auto &left = target;
		auto right = SplitNode(left);

		// Insert the new right node into the current node
		node.PushEntry(right);

		node.SortEntriesByXMin();

		// Now insert again
		return NodeInsert(entry, new_entry);
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

void RTree::RootInsert(RTreeEntry &root_entry, const RTreeEntry &new_entry) {
	// If there is no root node, create one and insert the new entry immediately
	if (root_entry.pointer.Get() == 0) {
		root_entry.pointer = MakePage(RTreeNodeType::LEAF_PAGE);
		root_entry.bounds = new_entry.bounds;

		RefMutable(root_entry.pointer).PushEntry(new_entry);
		return;
	}

	// Insert the new entry into the root node
	const auto result = NodeInsert(root_entry, new_entry);
	if (result.split) {
		// The root node was split, we need to create a new root node
		auto new_root_ptr = MakePage(RTreeNodeType::BRANCH_PAGE);
		auto &new_root = RefMutable(new_root_ptr);

		// Insert the old root into the new root
		new_root.PushEntry(root_entry);

		// Split the old root
		auto right = SplitNode(new_root[0]);

		// Insert the new right node into the new root
		new_root.PushEntry(right);

		// Update the root pointer
		root_entry.pointer = new_root_ptr;

		// Insert the new entry into the new root now that we have space
		RootInsert(root_entry, new_entry);
	}

	if (result.grown) {
		// Update the root bounding box
		root_entry.bounds.Union(new_entry.bounds);
	}
}

//------------------------------------------------------------------------------
// Delete
//------------------------------------------------------------------------------
DeleteResult RTree::NodeDelete(RTreeEntry &entry, const RTreeEntry &target, vector<RTreeEntry> &orphans) {
	if(!entry.bounds.Intersects(target.bounds)) {
		return {false, false, false};
	}
	const auto is_leaf = entry.pointer.IsLeafPage();
	return is_leaf ? LeafDelete(entry, target, orphans) : BranchDelete(entry, target, orphans);
}

DeleteResult RTree::BranchDelete(RTreeEntry &entry, const RTreeEntry &target, vector<RTreeEntry> &orphans) {
	D_ASSERT(entry.pointer.IsBranchPage());

	auto &node = RefMutable(entry.pointer);

	DeleteResult result = {false, false, false};
	idx_t child_idx;
	for (child_idx = 0; child_idx < node.GetCount(); child_idx++) {
		result = NodeDelete(node[child_idx], target, orphans);
		if (result.found) {
			break;
		}
	}

	// Did we find the target entry?
	if (!result.found) {
		return result;
	}

	// Should we delete the child entirely?
	if (result.remove) {
		// Swap the removed entry with the last entry and clear it
		node.SwapRemove(child_idx);

		// Does this node now have too few children?
		if (node.GetCount() < config.min_node_capacity) {
			// Yes, orphan all children and signal that this node should be removed
			orphans.insert(orphans.end(), node.begin(), node.end());
			node.Clear();
			node.Verify(config.max_node_capacity);
			return {true, true, true};
		}

		// Recalculate the bounding box
		// We most definitely shrunk if we removed a child
		// TODO: Do we always need to do this?
		auto shrunk = true;
		if (shrunk) {
			const auto old_bounds = entry.bounds;
			entry.bounds = node.GetBounds();
			shrunk = entry.bounds != old_bounds;

			// If the min capacity is zero, the bounds can grow when a node becomes empty
			D_ASSERT(entry.bounds.IsUnbounded() || entry.bounds.Area() <= old_bounds.Area());
		}
		return {true, shrunk, false};
	}

	// Check if we shrunk
	// TODO: Compare the bounds before and after the delete?
	auto shrunk = result.shrunk;
	if (shrunk) {
		const auto old_bounds = entry.bounds;
		entry.bounds = node.GetBounds();
		shrunk = entry.bounds != old_bounds;

		// If the min capacity is zero, the bounds can grow when a node becomes empty
		D_ASSERT(entry.bounds.IsUnbounded() || (entry.bounds.Area() <= old_bounds.Area()));
	}

	return {true, shrunk, false};
}

DeleteResult RTree::LeafDelete(RTreeEntry &entry, const RTreeEntry &target, vector<RTreeEntry> &orphans) {
	D_ASSERT(entry.pointer.IsLeafPage());

	auto &node = RefMutable(entry.pointer);

	// Do a binary search with std::lower_bound to find the matching rowid
	// This is faster than a linear search
	const auto it = std::lower_bound(node.begin(), node.end(), target.pointer.GetRowId(),
	                           [](const RTreeEntry &item, const row_t &row) { return item.pointer.GetRowId() < row; });
	if (it == node.end()) {
		// Not found in this leaf
		return {false, false, false};
	}

	const auto &child = *it;

	// Ok, did the binary search actually find the rowid?
	if(child.pointer.GetRowId() != target.pointer.GetRowId()) {
		return {false, false, false};
	}

	const auto child_idx = it - node.begin();

	D_ASSERT(child.pointer.IsRowId());

	// If we remove the entry, will this node now have too few children?
	if (node.GetCount() - 1 < config.min_node_capacity) {
		// Yes, orphan all children and signal that this node should be removed

		// But first, remove the actual entry. We dont care about preserving the order here
		// because we will orphan all children anyway
		node.SwapRemove(child_idx);

		orphans.insert(orphans.end(), node.begin(), node.end());
		node.Clear();
		node.Verify(config.max_node_capacity);
		return {true, true, true};
	}

	// Remove the entry and compact the node, taking care to preserve the order
	node.CompactRemove(child_idx);

	// TODO: Check if we actually shrunk and need to update the bounds
	// We can do this more efficiently by checking if the deleted entry bordered the bounds
	// Recalculate the bounds
	auto shrunk = true;
	if (shrunk) {
		const auto old_bounds = entry.bounds;
		entry.bounds = node.GetBounds();
		shrunk = entry.bounds != old_bounds;

		// Assert that the bounds shrunk
		// TODO: Cant call area blindly here if count == 0 as we get +inf back
		D_ASSERT(node.GetCount() == 0 || entry.bounds.Area() <= old_bounds.Area());
	}
	return DeleteResult {true, shrunk, false};
}

void RTree::ReInsertNode(RTreeEntry &root, RTreeEntry &target) {
	if (target.pointer.IsRowId()) {
		RootInsert(root, target);
	} else {
		D_ASSERT(target.pointer.IsPage());
		auto &node = RefMutable(target.pointer);
		for (auto &entry : node) {
			ReInsertNode(root, entry);
		}
		// Also free the page after we've reinserted all the rowids
		Free(target.pointer);
	}
}

void RTree::RootDelete(RTreeEntry &root, const RTreeEntry &target) {
	D_ASSERT(root.pointer.IsSet());

	vector<RTreeEntry> orphans;
	const auto result = NodeDelete(root, target, orphans);

	// We at least found the target entry
	D_ASSERT(result.found);

	// If the node was removed, we need to clear the root
	if (result.remove) {
		root.bounds = RTreeBounds();
		Free(root.pointer);

		// Are there orphans?
		if (!orphans.empty()) {
			// Allocate a new root
			root.pointer = MakePage(RTreeNodeType::LEAF_PAGE);
			for (auto &orphan : orphans) {
				ReInsertNode(root, orphan);
			}
		}
	} else {
		if (result.shrunk) {
			// Update the root bounding box
			root.bounds = Ref(root.pointer).GetBounds();
		}
		// Reinsert orphans
		for (auto &orphan : orphans) {
			ReInsertNode(root, orphan);
		}
	}
}

// Print as ascii tree
string RTree::ToString() const {
	string result;

	struct PrintState {
		RTreePointer pointer;
		idx_t entry_idx;
		explicit PrintState(const RTreePointer &pointer_p) : pointer(pointer_p), entry_idx(0) {
		}
	};

	vector<PrintState> stack;
	idx_t level = 0;

	stack.emplace_back(root.pointer);

	while(!stack.empty()) {
		auto &frame = stack.back();
		const auto &node = Ref(frame.pointer);
		const auto count = node.GetCount();

		if(frame.pointer.IsLeafPage()) {
			while(frame.entry_idx < count) {
				auto &entry = node[frame.entry_idx];
				// TODO: Print entry
				for(idx_t i = 0; i < level; i++) {
					result += "  ";
				}

				result += "Leaf: " + std::to_string(entry.pointer.GetRowId()) + "\n";

				frame.entry_idx++;
			}
			stack.pop_back();
			level--;
		}
		else {
			D_ASSERT(frame.pointer.IsBranchPage());
			if(frame.entry_idx < count) {
				auto &entry = node[frame.entry_idx];

				// TODO: Print entry
				for(idx_t i = 0; i < level; i++) {
					result += "  ";
				}

				result += "Branch: " + std::to_string(frame.entry_idx) + "\n";

				frame.entry_idx++;
				level++;
				stack.emplace_back(entry.pointer);
			} else {
				stack.pop_back();
				level--;
			}
		}
	}


	return result;
}

void RTree::Print() const {
	Printer::Print(ToString());
}


} // namespace core

} // namespace spatial