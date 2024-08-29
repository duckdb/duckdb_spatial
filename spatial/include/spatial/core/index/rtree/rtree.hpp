#pragma once

#include "spatial/core/index/rtree/rtree_node.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/storage/block_manager.hpp"

namespace spatial {

namespace core {

struct InsertResult;
struct DeleteResult;

struct RTreeConfig {
	idx_t max_node_capacity;
	idx_t min_node_capacity;

	// TODO: Allow setting leaf capacity separately
	// idx_t max_leaf_capacity;
	// idx_t min_leaf_capacity;

	idx_t GetNodeByteSize() const {
		return sizeof(RTreeNode) + (sizeof(RTreeEntry) * max_node_capacity);
	}
	idx_t GetLeafByteSize() const {
		return sizeof(RTreeNode) + (sizeof(RTreeEntry) * max_node_capacity);
	}
};

struct RTree {
public:
	RTree(BlockManager &block_manager, const RTreeConfig &config_p) : config(config_p) {

		// Create the allocators
		node_allocator = make_uniq<FixedSizeAllocator>(config.GetNodeByteSize(), block_manager);
		leaf_allocator = make_uniq<FixedSizeAllocator>(config.GetLeafByteSize(), block_manager);
	}

	void Insert(const RTreeEntry &entry) {
		RootInsert(root, entry);
	}

	void Delete(const RTreeEntry &entry) {
		RootDelete(root, entry);
	}

	FixedSizeAllocator &GetNodeAllocator() {
		return *node_allocator;
	}

	FixedSizeAllocator &GetLeafAllocator() {
		return *leaf_allocator;
	}

	const RTreeEntry &GetRoot() const {
		return root;
	}

	const RTreeConfig& GetConfig() const {
		return config;
	}

	void SetRoot(idx_t root_ptr) {
		// Set the pointer
		root.pointer.Set(root_ptr);
		// Compute the bounds
		if(root.pointer.Get() != 0) {
			root.bounds = Ref(root.pointer).GetBounds();
		}
	}

	void SetRoot(const RTreeEntry &entry) {
		root = entry;
	}

	void Reset() {
		node_allocator->Reset();
		leaf_allocator->Reset();
		root.Clear();
		root.bounds = RTreeBounds();
	}

	const RTreeNode &Ref(const RTreePointer &pointer) const;
	RTreeNode &RefMutable(const RTreePointer &pointer) const;

	RTreePointer MakePage(RTreeNodeType type) const;
	static RTreePointer MakeRowId(row_t row_id);

private:
	void Free(RTreePointer &pointer);

	void RootInsert(RTreeEntry &root_entry, const RTreeEntry &new_entry);
	InsertResult NodeInsert(RTreeEntry &entry, const RTreeEntry &new_entry);
	InsertResult LeafInsert(RTreeEntry &entry, const RTreeEntry &new_entry);
	InsertResult BranchInsert(RTreeEntry &entry, const RTreeEntry &new_entry);
	RTreeEntry &PickSubtree(RTreeNode &node, const RTreeEntry &new_entry) const;

	RTreeEntry SplitNode(RTreeEntry &entry) const;
	void RebalanceSplitNodes(RTreeNode &src, RTreeNode &dst, bool split_axis, PointXY<float> &split_point) const;

	void RootDelete(RTreeEntry &root, const RTreeEntry &target);
	DeleteResult NodeDelete(RTreeEntry &entry, const RTreeEntry &target, vector<RTreeEntry> &orphans);
	DeleteResult LeafDelete(RTreeEntry &entry, const RTreeEntry &target, vector<RTreeEntry> &orphans);
	DeleteResult BranchDelete(RTreeEntry &entry, const RTreeEntry &target, vector<RTreeEntry> &orphans);
	void ReInsertNode(RTreeEntry &root, RTreeEntry &target);

private:
	unique_ptr<FixedSizeAllocator> node_allocator;
	unique_ptr<FixedSizeAllocator> leaf_allocator;

	RTreeEntry root;

	const RTreeConfig config;
};

} // namespace core

} // namespace spatial