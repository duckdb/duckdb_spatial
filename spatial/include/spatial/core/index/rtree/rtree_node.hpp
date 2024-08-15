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

struct alignas(RTreeEntry) RTreeNode {

	// Delete copy
	RTreeNode(const RTreeNode &) = delete;
	RTreeNode &operator=(const RTreeNode &) = delete;
	// Delete move
	RTreeNode(RTreeNode &&) = delete;
	RTreeNode &operator=(RTreeNode &&) = delete;

public:
	idx_t GetCount() const {
		return count;
	}

	RTreeBounds GetBounds() const {
		RTreeBounds result;
		for (idx_t i = 0; i < count; i++) {
			auto &entry = begin()[i];
			D_ASSERT(entry.IsSet());
			result.Union(entry.bounds);
		}
		return result;
	}

	void Clear() {
		count = 0;
	}

	void PushEntry(const RTreeEntry &entry) {
		begin()[count++] = entry;
	}

	RTreeEntry SwapRemove(const idx_t idx) {
		D_ASSERT(idx < count);
		const auto result = begin()[idx];
		begin()[idx] = begin()[--count];
		return result;
	}

	void Verify(const idx_t capacity) const {
#ifdef DEBUG
		D_ASSERT(count <= capacity);
		if (count != 0) {
			if (begin()[0].pointer.GetType() == RTreeNodeType::ROW_ID) {
				// This is a leaf node, rowids should be sorted
				const auto ok = std::is_sorted(begin(), end(), [](const RTreeEntry &a, const RTreeEntry &b) {
					D_ASSERT(a.IsSet());
					D_ASSERT(b.IsSet());
					return a.pointer.GetRowId() < b.pointer.GetRowId();
				});
				D_ASSERT(ok);
			}
		}
#endif
	}

	void SortEntriesByXMin() {
		std::sort(begin(), end(), [&](const RTreeEntry &a, const RTreeEntry &b) {
			D_ASSERT(a.IsSet());
			D_ASSERT(b.IsSet());
			return a.bounds.min.x < b.bounds.min.x;
		});
	}

	void SortEntriesByRowId() {
		std::sort(begin(), end(), [&](const RTreeEntry &a, const RTreeEntry &b) {
			D_ASSERT(a.IsSet());
			D_ASSERT(b.IsSet());
			return a.pointer.GetRowId() < b.pointer.GetRowId();
		});
	}

public: // Collection interface
	RTreeEntry *begin() {
		return reinterpret_cast<RTreeEntry *>(this + 1);
	}
	RTreeEntry *end() {
		return begin() + count;
	}
	const RTreeEntry *begin() const {
		return reinterpret_cast<const RTreeEntry *>(this + 1);
	}
	const RTreeEntry *end() const {
		return begin() + count;
	}

	RTreeEntry &operator[](const idx_t idx) {
		D_ASSERT(idx < count);
		return begin()[idx];
	}

	const RTreeEntry &operator[](const idx_t idx) const {
		D_ASSERT(idx < count);
		return begin()[idx];
	}

private:
	uint32_t count;

	// We got 12 bytes for the future
	uint32_t _unused1;
	uint64_t _unused2;
	uint64_t _unused3;
};

} // namespace core

} // namespace spatial
