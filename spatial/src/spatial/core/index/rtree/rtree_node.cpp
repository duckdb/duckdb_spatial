#include "spatial/core/index/rtree/rtree_node.hpp"
#include "spatial/core/index/rtree/rtree_index.hpp"

namespace spatial {

namespace core {

RTreeNode &RTreePointer::NewPage(RTreeIndex &index, RTreePointer &pointer, RTreeNodeType type) {
	D_ASSERT(type == RTreeNodeType::BRANCH_PAGE || type == RTreeNodeType::LEAF_PAGE);

	// Allocate a new node. This will also memset the entries to 0
	pointer = index.node_allocator->New();

	// Set the pointer to the type
	pointer.SetMetadata(static_cast<uint8_t>(type));
	auto &ref = RTreePointer::RefMutable(index, pointer);

	return ref;
}

FixedSizeAllocator &RTreePointer::GetAllocator(const RTreeIndex &index) {
	return *index.node_allocator;
}

void RTreePointer::Free(RTreeIndex &index, RTreePointer &ptr) {
	if (ptr.IsRowId()) {
		ptr.Clear();
		// Nothing to do here
		return;
	}

	auto &node = RTreePointer::RefMutable(index, ptr);

	for (auto &entry : node.entries) {
		if (!entry.IsSet()) {
			break;
		}
		Free(index, entry.pointer);
	}
	index.node_allocator->Free(ptr);
	ptr.Clear();
}

} // namespace core

} // namespace spatial
