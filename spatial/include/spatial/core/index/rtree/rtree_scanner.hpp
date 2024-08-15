#pragma once

#include "spatial/core/index/rtree/rtree.hpp"

namespace spatial {

namespace core {

class RTreeScanner {
public:
	void Init(const RTreeEntry &root);
	template <class FUNC>
	void Scan(const RTree &tree, FUNC &&handler);
	void Reset();

private:
	struct NodeScanState {
		RTreePointer pointer;
		idx_t entry_idx;
		explicit NodeScanState(const RTreePointer &pointer_p) : pointer(pointer_p), entry_idx(0) {
		}
	};
	vector<NodeScanState> stack;
	idx_t level = 0;
};

inline void RTreeScanner::Init(const RTreeEntry &root) {
	stack.clear();
	stack.emplace_back(root.pointer);
	level = 0;
}

enum class RTreeScanResult : uint8_t {
	CONTINUE,
	SKIP,
	YIELD,
};

template <class FUNC>
inline void RTreeScanner::Scan(const RTree &tree, FUNC &&handler) {
	// Depth-first scan of all nodes in the RTree with an explicit stack
	while (!stack.empty()) {
		auto &frame = stack.back();
		const auto &node = tree.Ref(frame.pointer);

		if (frame.pointer.IsLeafPage()) {
			while (frame.entry_idx < node.GetCount()) {
				auto &entry = node[frame.entry_idx];
				auto result = handler(entry, level);
				frame.entry_idx++;
				if (result == RTreeScanResult::YIELD) {
					// Yield!
					return;
				}
			}
			stack.pop_back();
			level--;
		} else {
			D_ASSERT(frame.pointer.IsBranchPage());
			if (frame.entry_idx < node.GetCount()) {
				auto &entry = node[frame.entry_idx];

				auto result = handler(entry, level);
				frame.entry_idx++;

				if (result == RTreeScanResult::SKIP) {
					// Dont recurse into this node
					continue;
				}

				level++;
				stack.emplace_back(entry.pointer);

				if (result == RTreeScanResult::YIELD) {
					// Yield!
					return;
				}
			} else {
				// We've exhausted the branch, pop it from the stack
				stack.pop_back();
				level--;
			}
		}
	}
}

} // namespace core

} // namespace spatial