#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/index_pointer.hpp"
#include "spatial/common.hpp"
#include "spatial/core/geometry/bbox.hpp"

namespace spatial {

namespace core {

//-------------------------------------------------------------
// RTree Pointer
//-------------------------------------------------------------
class RTreeNode;
class RTreeIndex;

enum class RTreeNodeType : uint8_t {
	UNSET = 0,
	ROW_ID = 1,
	LEAF_PAGE = 2,
	BRANCH_PAGE = 3,
};

class RTreePointer final : public IndexPointer {
	static constexpr idx_t AND_ROW_ID = 0x00FFFFFFFFFFFFFF;

public:
	RTreePointer() = default;
	explicit RTreePointer(const IndexPointer &ptr) : IndexPointer(ptr) {
	}
	//! Get a new pointer to a node, might cause a new buffer allocation, and initialize it
	static RTreeNode &NewPage(RTreeIndex &index, RTreePointer &pointer,
	                          RTreeNodeType type = RTreeNodeType::BRANCH_PAGE);

	static RTreePointer NewRowId(row_t row_id) {
		RTreePointer pointer;
		pointer.SetMetadata(static_cast<uint8_t>(RTreeNodeType::ROW_ID));
		pointer.SetRowId(row_id);
		return pointer;
	}

	//! Free the node (and the subtree)
	static void Free(RTreeIndex &index, RTreePointer &pointer);

	//! Get a reference to the allocator
	static FixedSizeAllocator &GetAllocator(const RTreeIndex &index);

	//! Get reference to the node (immutable).
	static const RTreeNode &Ref(const RTreeIndex &index, const RTreePointer ptr) {
		return *(GetAllocator(index).Get<const RTreeNode>(ptr, false));
	}

	//! Get a reference to the node (mutable), marking the node as dirty.
	static RTreeNode &RefMutable(const RTreeIndex &index, const RTreePointer ptr) {
		return *(GetAllocator(index).Get<RTreeNode>(ptr, true));
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

	bool IsRowId() const {
		return GetType() == RTreeNodeType::ROW_ID;
	}
	bool IsLeafPage() const {
		return GetType() == RTreeNodeType::LEAF_PAGE;
	}
	bool IsBranchPage() const {
		return GetType() == RTreeNodeType::BRANCH_PAGE;
	}
	bool IsPage() const {
		return IsLeafPage() || IsBranchPage();
	}
	bool IsSet() const {
		return Get() != 0;
	}

	//! Assign operator
	RTreePointer &operator=(const IndexPointer &ptr) {
		Set(ptr.Get());
		return *this;
	}
};

// The RTreeNode contains up to CAPACITY entries
// each represented by a bounds and a pointer
// Node is kind of a bad name, maybe a "RTreePage" would be better

using RTreeBounds = Box2D<float>;

struct RTreeEntry {
	RTreePointer pointer;
	RTreeBounds bounds;
	RTreeEntry() = default;
	RTreeEntry(const RTreePointer &pointer_p, const RTreeBounds &bounds_p) : pointer(pointer_p), bounds(bounds_p) {
	}
	bool IsSet() const {
		return pointer.Get() != 0;
	}
	void Clear() {
		pointer.Set(0);
	}
};

class RTreeNode {
public:
	friend class RTreePointer;
	static constexpr idx_t CAPACITY = 128;
	static constexpr idx_t MIN_CAPACITY = CAPACITY / 2;

public:
	RTreeNode() = default;

	RTreeEntry entries[CAPACITY];

	// Compute the bounds of this node by summing up all the child entries bounds
	RTreeBounds GetBounds() const {
		RTreeBounds result;
		for (const auto &entry : entries) {
			if (entry.IsSet()) {
				result.Union(entry.bounds);
			} else {
				break;
			}
		}
		return result;
	}

	// Returns the index of the first non-set entry
	idx_t GetCount() const {
		idx_t i;
		for (i = 0; i < CAPACITY; i++) {
			if (!entries[i].IsSet()) {
				break;
			}
		}
		return i;
	}

	// Swap the entry at the given index with the last entry in the node
	// and clear the last entry
	RTreeEntry RemoveAndSwap(idx_t index, idx_t count) {
		D_ASSERT(index < count);
		const auto entry = entries[index];
		entries[index] = entries[count - 1];
		entries[count - 1].Clear();
		return entry;
	}

	void Verify() const {
#ifdef DEBUG
		for (idx_t i = 0; i < CAPACITY; i++) {
			// If the entry is not set
			if (!entries[i].IsSet()) {
				// Then all the following entries should be unset
				for (idx_t j = i; j < CAPACITY; j++) {
					D_ASSERT(!entries[j].IsSet());
				}
				break;
			}
		}
#endif
	}
};

} // namespace core

} // namespace spatial

// -74.0891647, -0.0194350015, 0.0184550006, 40.8297081)