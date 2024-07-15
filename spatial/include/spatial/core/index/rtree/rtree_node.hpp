#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/index_pointer.hpp"
#include "spatial/common.hpp"
#include "spatial/core/geometry/bbox.hpp"
#include "spatial/core/index/rtree/rtree_index.hpp"

#include <spatial/core/util/math.hpp>

namespace spatial {

namespace core {

//-------------------------------------------------------------
// RTree Pointer
//-------------------------------------------------------------
class RTreeNode;

enum class RTreeNodeType : uint8_t {
	LEAF = 1,
	BRANCH = 2,
};

class RTreePointer final : public IndexPointer {
	static constexpr idx_t AND_ROW_ID = 0x00FFFFFFFFFFFFFF;
public:
	RTreePointer() = default;
	explicit RTreePointer(const IndexPointer &ptr) : IndexPointer(ptr) {}
	//! Get a new pointer to a node, might cause a new buffer allocation, and initialize it
	static RTreeNode& NewBranch(RTreeIndex &index, RTreePointer &pointer);

	static RTreePointer NewLeaf(row_t row_id) {
		RTreePointer pointer;
		pointer.SetMetadata(static_cast<uint8_t>(RTreeNodeType::LEAF));
		pointer.SetRowId(row_id);
		return pointer;
	}

	//! Free the node (and the subtree)
	// static void Free(RTreeIndex &index, RTreePointer &pointer);

	//! Get a reference to the allocator
	static FixedSizeAllocator &GetAllocator(const RTreeIndex &index) {
		return *index.node_allocator;
	}

	//! Get reference to the node (immutable). If dirty is false, then T should be a const class
	static const RTreeNode &Ref(const RTreeIndex &index, const RTreePointer ptr) {
		return *(GetAllocator(index).Get<const RTreeNode>(ptr, false));
	}

	//! Get a reference to the node (mutable). If dirty is false, then T should be a const class
	static RTreeNode &RefMutable(const RTreeIndex &index, const RTreePointer ptr) {
		return *(GetAllocator(index).Get<RTreeNode>(ptr, false));
	}

	//! Get the RowID from this pointer
	row_t GetRowId() const {
		return static_cast<row_t>(Get() & AND_ROW_ID);
	}

	void SetRowId(const row_t row_id) {
		Set((Get() & AND_METADATA) | UnsafeNumericCast<idx_t>(row_id));
	}

	RTreeNodeType GetType() const {
		return static_cast<RTreeNodeType>(GetMetadata());
	}

	bool IsLeaf() const { return GetType() == RTreeNodeType::LEAF; }
	bool IsBranch() const { return GetType() == RTreeNodeType::BRANCH; }

	//! Assign operator
	RTreePointer& operator=(const IndexPointer &ptr) {
		Set(ptr.Get());
		return *this;
	}
};

// The bounding box for a entry
struct RTreeBounds {
	float minx = NumericLimits<float>::Maximum();
	float miny = NumericLimits<float>::Maximum();
	float maxx = NumericLimits<float>::Minimum();
	float maxy = NumericLimits<float>::Minimum();

	RTreeBounds() = default;

	explicit RTreeBounds(const BoundingBox &box) {
		minx = MathUtil::DoubleToFloatDown(box.minx);
		miny = MathUtil::DoubleToFloatDown(box.miny);
		maxx = MathUtil::DoubleToFloatUp(box.maxx);
		maxy = MathUtil::DoubleToFloatUp(box.maxy);
	}

	bool Intersects(const RTreeBounds &other) const {
		return !(minx > other.maxx || maxx < other.minx || miny > other.maxy || maxy < other.miny);
	}
};

// The RTreeNode contains up to CAPACITY entries
// each represented by a bounds and a pointer
// Node is kind of a bad name, maybe a "RTreePage" would be better
struct RTreeEntry {
	RTreePointer pointer;
	RTreeBounds bounds;
	RTreeEntry() = default;
	RTreeEntry(const RTreePointer &pointer_p, const RTreeBounds &bounds_p) : pointer(pointer_p), bounds(bounds_p) {}
	bool IsSet() const { return pointer.Get() != 0; }
};

class RTreeNode {
public:
	friend class RTreePointer;
	static constexpr idx_t CAPACITY = 16;
public:
	RTreeEntry entries[CAPACITY];

	// Compute the bounds of this node by summing up all the child entries bounds
	RTreeBounds GetBounds() const {
		RTreeBounds result;
		for(const auto & entry : entries) {
			if(entry.IsSet()) {
				auto &bounds = entry.bounds;
				result.minx = std::min(result.minx, bounds.minx);
				result.miny = std::min(result.miny, bounds.miny);
				result.maxx = std::max(result.maxx, bounds.maxx);
				result.maxy = std::max(result.maxy, bounds.maxy);
			}
		}
		return result;
	}
};

inline RTreeNode& RTreePointer::NewBranch(RTreeIndex &index, RTreePointer &pointer) {
	// Allocate a new node. This will also memset the entries to 0
	pointer = index.node_allocator->New();

	// Set the pointer to the type
	pointer.SetMetadata(static_cast<uint8_t>(RTreeNodeType::BRANCH));
	auto &ref = RTreePointer::RefMutable(index, pointer);

	return ref;
}

}

}

// -74.0891647, -0.0194350015, 0.0184550006, 40.8297081)