#pragma once

#include "spatial/core/index/rtree/rtree_node.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/storage/block_manager.hpp"

namespace spatial {

namespace core {

struct InsertResult;
struct DeleteResult;

struct RTree {
public:
	RTree(BlockManager &block_manager, idx_t max_node_capacity_p, idx_t min_node_capacity_p)
	    : max_node_capacity(max_node_capacity_p), min_node_capacity(min_node_capacity_p) {
		auto node_size = sizeof(RTreeEntry) * max_node_capacity;

		allocator = make_uniq<FixedSizeAllocator>(node_size, block_manager);
	}

	void Insert(const RTreeEntry &entry) {
		RootInsert(root, entry);
	}

	void Delete(const RTreeEntry &entry) {
		RootDelete(root, entry);
	}

	FixedSizeAllocator &GetAllocator() {
		return *allocator;
	}

	const RTreeEntry &GetRoot() const {
		return root;
	}

	idx_t GetNodeCapacity() const {
		return max_node_capacity;
	}

	void SetRoot(idx_t root_ptr) {
		// Set the pointer
		root.pointer.Set(root_ptr);
		// Compute the bounds
		const auto node = Ref(root.pointer);
		root.bounds = node.GetBounds(max_node_capacity);
	}

	void SetRoot(const RTreeEntry &entry) {
		root = entry;
	}

	void Reset() {
		allocator->Reset();
		root.Clear();
		root.bounds = RTreeBounds();
	}

	ConstRTreeNodeRef Ref(const RTreePointer &pointer) const;
	MutableRTreeNodeRef RefMutable(const RTreePointer &pointer) const;

	RTreePointer MakePage(RTreeNodeType type) const;
	static RTreePointer MakeRowId(row_t row_id);

private:
	void Free(RTreePointer &pointer);

	void RootInsert(RTreeEntry &root_entry, const RTreeEntry &new_entry);
	InsertResult NodeInsert(RTreeEntry &entry, const RTreeEntry &new_entry);
	RTreeEntry &PickSubtree(const MutableRTreeNodeRef &node, const RTreeEntry &new_entry) const;

	RTreeEntry SplitNode(RTreeEntry &entry);
	void RebalanceSplitNodes(const MutableRTreeNodeRef &src, const MutableRTreeNodeRef &dst, idx_t &src_cnt,
	                         idx_t &dst_cnt, bool split_axis, PointXY<float> &split_point) const;

	void RootDelete(RTreeEntry &root, const RTreeEntry &target);
	DeleteResult NodeDelete(RTreeEntry &entry, const RTreeEntry &target, vector<RTreeEntry> &orphans);
	void ReInsertNode(RTreeEntry &root, RTreeEntry &target);

private:
	unique_ptr<FixedSizeAllocator> allocator;
	RTreeEntry root;

	const idx_t max_node_capacity;
	const idx_t min_node_capacity;
};

} // namespace core

} // namespace spatial