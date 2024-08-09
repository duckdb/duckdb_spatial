#include "spatial/core/index/rtree/rtree.hpp"

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
	pointer = allocator->New();
	pointer.SetMetadata(static_cast<uint8_t>(type));
	return pointer;
}

RTreePointer RTree::MakeRowId(row_t row_id) {
	RTreePointer pointer;
	pointer.SetMetadata(static_cast<uint8_t>(RTreeNodeType::ROW_ID));
	pointer.SetRowId(row_id);
	return pointer;
}

MutableRTreeNodeRef RTree::RefMutable(const RTreePointer &pointer) const {
	auto ptr = allocator->Get<data_t>(pointer, true);
	return MutableRTreeNodeRef(ptr);
}

ConstRTreeNodeRef RTree::Ref(const RTreePointer &pointer) const {
	auto ptr = allocator->Get<const data_t>(pointer, false);
	return ConstRTreeNodeRef(ptr);
}

void RTree::Free(RTreePointer &pointer) {
	if (pointer.IsRowId()) {
		pointer.Clear();
		// Nothing to do here
		return;
	}
	const auto node = RefMutable(pointer);
	for (idx_t i = 0; i < max_node_capacity; i++) {
		auto &entry = node.entries[i];
		if (!entry.IsSet()) {
			break;
		}
		Free(entry.pointer);
	}
	allocator->Free(pointer);
	pointer.Clear();
}

//------------------------------------------------------------------------------
// Split
//------------------------------------------------------------------------------
void RTree::RebalanceSplitNodes(const MutableRTreeNodeRef &src, const MutableRTreeNodeRef &dst, idx_t &src_cnt,
                                idx_t &dst_cnt, bool split_axis, PointXY<float> &split_point) const {
	D_ASSERT(src_cnt > dst_cnt);

	// How many entries to we need to move until we have the minimum capacity?
	const auto remaining = min_node_capacity - dst_cnt;

	// Setup a min heap to keep track of the entries that are closest to the split point
	vector<pair<float, idx_t>> diff_heap;
	diff_heap.reserve(remaining);

	auto cmp = [&](const pair<float, idx_t> &a, const pair<float, idx_t> &b) {
		return a.first > b.first;
	};

	// Find the entries that are closest to the split point
	for (idx_t i = 0; i < src_cnt; i++) {
		D_ASSERT(src.entries[i].IsSet());
		auto center = src.entries[i].bounds.Center();
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
	std::sort(diff_heap.begin(), diff_heap.end(),
	          [&](const pair<float, idx_t> &a, const pair<float, idx_t> &b) { return a.second > b.second; });

	// Now move over the selected entries to the destination node
	for (const auto &entry : diff_heap) {
		dst.entries[dst_cnt++] = src.entries[entry.second];
		// Compact the source node
		src.entries[entry.second] = src.entries[src_cnt - 1];
		src.entries[src_cnt - 1].Clear();
		src_cnt--;
	}
}

RTreeEntry RTree::SplitNode(RTreeEntry &entry) {
	// Split the entry bounding box into four quadrants
	auto node = RefMutable(entry.pointer);
	D_ASSERT(node.GetCount(max_node_capacity) == max_node_capacity);

	/*
	 *  C1 | C2
	 *  -------
	 *  C0 | C3
	 */

	RTreeBounds quadrants[4];
	auto center = entry.bounds.Center();
	quadrants[0] = {entry.bounds.min, center};
	quadrants[1] = {{entry.bounds.min.x, center.y}, {center.x, entry.bounds.max.y}};
	quadrants[2] = {center, entry.bounds.max};
	quadrants[3] = {{center.x, entry.bounds.min.y}, {entry.bounds.max.x, center.y}};

	idx_t q_counts[4] = {0, 0, 0, 0};
	RTreeBounds q_bounds[4];
	auto q_assign = make_unsafe_uniq_array<uint8_t>(max_node_capacity);
	uint8_t q_node[4] = {0, 0, 0, 0};

	for (idx_t i = 0; i < max_node_capacity; i++) {
		D_ASSERT(node.entries[i].IsSet());
		auto child_center = node.entries[i].bounds.Center();
		auto found = false;
		for (idx_t q_idx = 0; q_idx < 4; q_idx++) {
			if (quadrants[q_idx].Contains(child_center)) {
				q_counts[q_idx]++;
				q_assign[i] = q_idx;
				q_bounds[q_idx].Union(node.entries[i].bounds);
				found = true;
				break;
			}
		}
		D_ASSERT(found);
		(void)found;
	}

	// Create a temporary node for the first split
	auto n1_ptr = make_uniq_array<RTreeEntry>(max_node_capacity);
	MutableRTreeNodeRef n1(data_ptr_cast(n1_ptr.get()));

	RTreePointer n2_ptr = MakePage(entry.pointer.GetType());
	auto n2 = RefMutable(n2_ptr);
	// auto &n2 = RTreePointer::NewPage(rtree, n2_ptr, entry.pointer.GetType());

	idx_t node_count[2] = {0, 0};
	MutableRTreeNodeRef node_ref[2] = {n1, n2};

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
	for (idx_t i = 0; i < max_node_capacity; i++) {
		const auto q_idx = q_assign[i];
		const auto n_idx = q_node[q_idx];
		const auto &dst = node_ref[n_idx];
		auto &cnt = node_count[n_idx];
		dst.entries[cnt++] = node.entries[i];
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
	if (node_count[0] < min_node_capacity) {
		RebalanceSplitNodes(n2, n1, node_count[1], node_count[0], perp_split_axis, center);
	} else if (node_count[1] < min_node_capacity) {
		RebalanceSplitNodes(n1, n2, node_count[0], node_count[1], perp_split_axis, center);
	}

	D_ASSERT(node_count[0] >= min_node_capacity);
	D_ASSERT(node_count[1] >= min_node_capacity);

	// Replace the current node with n1
	memset(node.entries, 0, sizeof(RTreeEntry) * max_node_capacity);
	memcpy(node.entries, n1.entries, sizeof(RTreeEntry) * node_count[0]);

	// TODO: Reuse q_bounds if we didnt have to rebalance the nodes
	entry.bounds = n1.GetBounds(node_count[0]);

	// Sort both node entries on the min x-coordinate
	// This is not really necessary, but produces a slightly nicer tree
	node.SortEntriesByXMin(node_count[0]);
	n2.SortEntriesByXMin(node_count[1]);

	node.Verify(max_node_capacity);
	n2.Verify(max_node_capacity);

	// Return a new entry for the second node
	return {n2_ptr, n2.GetBounds(max_node_capacity)};
}

//------------------------------------------------------------------------------
// Insert
//------------------------------------------------------------------------------
RTreeEntry &RTree::PickSubtree(const MutableRTreeNodeRef &node, const RTreeEntry &new_entry) const {
	idx_t best_match = 0;
	float best_area = NumericLimits<float>::Maximum();
	float best_diff = NumericLimits<float>::Maximum();

	for (idx_t i = 0; i < max_node_capacity; i++) {
		auto &entry = node.entries[i];
		if (!entry.IsSet()) {
			break;
		}

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
	return node.entries[best_match];
}

// Insert a rowid into a node
InsertResult RTree::NodeInsert(RTreeEntry &entry, const RTreeEntry &new_entry) {
	D_ASSERT(new_entry.pointer.IsRowId());
	D_ASSERT(entry.IsSet());

	auto node = RefMutable(entry.pointer);

	// Is this a leaf?
	if (entry.pointer.IsLeafPage()) {
		auto count = node.GetCount(max_node_capacity);
		// Is this leaf full?
		if (count == max_node_capacity) {
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
	auto &target = PickSubtree(node, new_entry);

	if (!target.IsSet()) {
		throw InternalException("RTreeIndex::NodeInsert: target is not set");
	}

	// Insert into the selected child
	const auto result = NodeInsert(target, new_entry);
	if (result.split) {
		auto count = node.GetCount(max_node_capacity);
		if (count == max_node_capacity) {
			// This node is also full!, we need to split it first.
			return InsertResult {true, false};
		}

		// Otherwise, split the selected child
		auto &left = target;
		auto right = SplitNode(left);

		// Insert the new right node into the current node
		node.entries[count++] = right;

		// Sort all entries by the x-coordinate
		std::sort(node.entries, node.entries + count, [&](const RTreeEntry &a, const RTreeEntry &b) {
			D_ASSERT(a.IsSet());
			D_ASSERT(b.IsSet());
			return a.bounds.min.x < b.bounds.min.x;
		});

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
	if (!root_entry.IsSet()) {
		root_entry.pointer = MakePage(RTreeNodeType::LEAF_PAGE);
		root_entry.bounds = new_entry.bounds;
		auto node = RefMutable(root_entry.pointer);
		node.entries[0] = new_entry;
		return;
	}

	// Insert the new entry into the root node
	const auto result = NodeInsert(root_entry, new_entry);
	if (result.split) {
		// The root node was split, we need to create a new root node
		auto new_root_ptr = MakePage(RTreeNodeType::BRANCH_PAGE);
		auto new_root = RefMutable(new_root_ptr);

		// Insert the old root into the new root
		new_root.entries[0] = root_entry;

		// Split the old root
		auto right = SplitNode(new_root.entries[0]);

		// Insert the new right node into the new root
		new_root.entries[1] = right;

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
	if (entry.pointer.IsLeafPage()) {
		auto node = RefMutable(entry.pointer);

		auto child_idx_opt = optional_idx::Invalid();
		idx_t child_count = 0;
		for (; child_count < max_node_capacity; child_count++) {
			auto &child = node.entries[child_count];
			if (!child.IsSet()) {
				break;
			}
			D_ASSERT(child.pointer.IsRowId());
			if (child.pointer.GetRowId() == target.pointer.GetRowId()) {
				D_ASSERT(!child_idx_opt.IsValid());
				child_idx_opt = child_count;
			}
		}

		// Found the target entry
		if (child_idx_opt.IsValid()) {
			// Found the target entry
			const auto child_idx = child_idx_opt.GetIndex();

			if (child_idx == 0 && child_count == 1) {
				// This is the only entry in the leaf, we can remove the entire leaf
				node.entries[0].Clear();
				node.Verify(max_node_capacity);
				return DeleteResult {true, true, true};
			}

			// Overwrite the target entry with the last entry and clear the last entry
			// to compact the leaf
			node.entries[child_idx] = node.entries[child_count - 1];
			node.entries[child_count - 1].Clear();
			child_count--;

			// Does this node now have too few children?
			if (child_count < min_node_capacity) {
				// Yes, orphan all children and signal that this node should be removed
				for (idx_t i = 0; i < child_count; i++) {
					orphans.push_back(node.entries[i]);
					// Clear the child pointer so we dont free it if we end up freeing the node
					node.entries[i].Clear();
				}
				node.Verify(max_node_capacity);
				return {true, true, true};
			}

			// TODO: Check if we actually shrunk and need to update the bounds
			// We can do this more efficiently by checking if the deleted entry bordered the bounds
			// Recalculate the bounds
			auto shrunk = true;
			if (shrunk) {
				const auto old_bounds = entry.bounds;
				entry.bounds = node.GetBounds(max_node_capacity);
				shrunk = entry.bounds != old_bounds;

				// Assert that the bounds shrunk
				D_ASSERT(entry.bounds.Area() <= old_bounds.Area());
			}
			return DeleteResult {true, shrunk, false};
		}

		// Not found in this leaf
		return {false, false, false};
	}
	// Otherwise: this is a branch node
	D_ASSERT(entry.pointer.IsBranchPage());
	auto node = RefMutable(entry.pointer);

	DeleteResult result = {false, false, false};

	idx_t child_count = 0;
	idx_t child_idx = 0;
	for (; child_count < max_node_capacity; child_count++) {
		auto &child = node.entries[child_count];
		if (!child.IsSet()) {
			break;
		}
		if (!result.found) {
			result = NodeDelete(child, target, orphans);
			if (result.found) {
				child_idx = child_count;
			}
		}
	}

	// Did we find the target entry?
	if (!result.found) {
		return result;
	}

	// Should we delete the child entirely?
	if (result.remove) {
		// If this is the only child in the branch, we can remove the entire branch

		if (child_idx == 0 && child_count == 1) {
			Free(node.entries[0].pointer);
			return {true, true, true};
		}

		// Swap the removed entry with the last entry and clear it
		std::swap(node.entries[child_idx], node.entries[child_count - 1]);
		Free(node.entries[child_count - 1].pointer);
		child_count--;

		// Does this node now have too few children?
		if (child_count < min_node_capacity) {
			// Yes, orphan all children and signal that this node should be removed
			for (idx_t i = 0; i < child_count; i++) {
				orphans.push_back(node.entries[i]);
				// Clear the child pointer so we dont free them if we end up freeing the node
				node.entries[i].Clear();
			}
			return {true, true, true};
		}

		// Recalculate the bounding box
		// We most definitely shrunk if we removed a child
		// TODO: Do we always need to do this?
		auto shrunk = true;
		if (shrunk) {
			const auto old_bounds = entry.bounds;
			entry.bounds = node.GetBounds(max_node_capacity);
			shrunk = entry.bounds != old_bounds;

			// Assert that the bounds shrunk
			D_ASSERT(entry.bounds.Area() <= old_bounds.Area());
		}
		return {true, shrunk, false};
	}

	// Check if we shrunk
	// TODO: Compare the bounds before and after the delete?
	auto shrunk = result.shrunk;
	if (shrunk) {
		const auto old_bounds = entry.bounds;
		entry.bounds = node.GetBounds(max_node_capacity);
		shrunk = entry.bounds != old_bounds;

		D_ASSERT(entry.bounds.Area() <= old_bounds.Area());
	}

	return {true, shrunk, false};
}

void RTree::ReInsertNode(RTreeEntry &root, RTreeEntry &target) {
	D_ASSERT(target.IsSet());
	if (target.pointer.IsRowId()) {
		RootInsert(root, target);
	} else {
		D_ASSERT(target.pointer.IsPage());
		const auto node = RefMutable(target.pointer);
		for (idx_t i = 0; i < max_node_capacity; i++) {
			auto &entry = node.entries[i];
			if (!entry.IsSet()) {
				break;
			}
			D_ASSERT(entry.pointer.IsRowId());
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
			auto node = Ref(root.pointer);
			root.bounds = node.GetBounds(max_node_capacity);
		}
		// Reinsert orphans
		for (auto &orphan : orphans) {
			ReInsertNode(root, orphan);
		}
	}
}

} // namespace core

} // namespace spatial