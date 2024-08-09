#pragma once

#include "duckdb/execution/index/index_pointer.hpp"
#include "spatial/common.hpp"
#include "spatial/core/geometry/bbox.hpp"

namespace spatial {

namespace core {

//-------------------------------------------------------------
// RTree Pointer
//-------------------------------------------------------------

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

template <bool CONST>
struct RTreeNodeRefBase {
	using element_type = typename std::conditional<CONST, const RTreeEntry, RTreeEntry>::type;
	element_type *entries;
	explicit RTreeNodeRefBase(element_type *entries_p) : entries(entries_p) {
	}

	RTreeBounds GetBounds(idx_t capacity) const {
		RTreeBounds result;
		for (idx_t i = 0; i < capacity; i++) {
			auto &entry = entries[i];
			if (!entry.IsSet()) {
				break;
			}
			result.Union(entry.bounds);
		}
		return result;
	}

	idx_t GetCount(idx_t capacity) const {
		idx_t i;
		for (i = 0; i < capacity; i++) {
			if (!entries[i].IsSet()) {
				break;
			}
		}
		return i;
	}

	void Verify(idx_t capacity) const {
#ifdef DEBUG
		for (idx_t i = 0; i < capacity; i++) {
			// If the entry is not set
			if (!entries[i].IsSet()) {
				// Then all the following entries should be unset
				for (idx_t j = i; j < capacity; j++) {
					D_ASSERT(!entries[j].IsSet());
				}
				break;
			}
		}
#endif
	}
};

struct MutableRTreeNodeRef : public RTreeNodeRefBase<false> {
	explicit MutableRTreeNodeRef(const data_ptr_t &ptr) : RTreeNodeRefBase(reinterpret_cast<element_type *>(ptr)) {
	}

	void SortEntriesByXMin(idx_t count) {
		std::sort(entries, entries + count, [&](const RTreeEntry &a, const RTreeEntry &b) {
			D_ASSERT(a.IsSet());
			D_ASSERT(b.IsSet());
			return a.bounds.min.x < b.bounds.min.x;
		});
	}
};

struct ConstRTreeNodeRef : public RTreeNodeRefBase<true> {
	explicit ConstRTreeNodeRef(const const_data_ptr_t &ptr) : RTreeNodeRefBase(reinterpret_cast<element_type *>(ptr)) {
	}
};

} // namespace core

} // namespace spatial
