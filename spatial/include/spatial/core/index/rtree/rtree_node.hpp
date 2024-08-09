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

} // namespace core

} // namespace spatial
