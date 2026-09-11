#pragma once

#include "spatial/geometry/bbox.hpp"
#include <stdexcept>
#include <vector>
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <functional>
#include <utility>

namespace duckdb {
namespace spatial {

// Hilbert curve encoding (16-bit coordinates, fast integer bit twiddling)
inline uint32_t HilbertEncode(uint32_t x, uint32_t y) {
	uint32_t d = 0;
	for (uint32_t s = 1 << 15; s > 0; s /= 2) {
		uint32_t rx = (x & s) > 0 ? 1 : 0;
		uint32_t ry = (y & s) > 0 ? 1 : 0;
		d += s * s * ((3 * rx) ^ ry);
		if (ry == 0) {
			if (rx == 1) {
				x = (1 << 16) - 1 - x;
				y = (1 << 16) - 1 - y;
			}
			std::swap(x, y);
		}
	}
	return d;
}

using Point2D = PointXY<double>;
using BBox2D = Box2D<double>;

// In-Memory Packed Static R-Tree (Hilbert-curve sorted bulk load)
class FlatRTree2D {
public:
	explicit FlatRTree2D(size_t node_size = 32, std::function<void()> interrupt = {})
	    : node_size_(node_size), item_count_(0), points_(nullptr), interrupt_(std::move(interrupt)) {
		if (node_size < 2) {
			throw std::invalid_argument("R-tree node size must be at least 2");
		}
	}

	void CheckInterrupt() const {
		if (interrupt_) {
			interrupt_();
		}
	}

	void Build(const std::vector<Point2D> &points) {
		CheckInterrupt();
		points_ = &points;
		item_count_ = points.size();

		if (item_count_ == 0) {
			boxes_.clear();
			indices_.clear();
			layer_bounds_.clear();
			return;
		}

		ComputeLayerBounds();
		const size_t total_nodes = layer_bounds_.back();

		boxes_.resize(total_nodes);
		indices_.resize(total_nodes);

		// Compute data bounds
		tree_box_ = BBox2D();
		for (size_t i = 0; i < item_count_; ++i) {
			if (i % 1024 == 0) {
				CheckInterrupt();
			}
			const double x = points[i].x;
			const double y = points[i].y;
			if (!std::isfinite(x) || !std::isfinite(y)) {
				throw std::invalid_argument("R-tree coordinates must be finite");
			}
			boxes_[i] = BBox2D(points[i], points[i]);
			indices_[i] = i;
			tree_box_.Union(boxes_[i]);
		}

		if (item_count_ <= node_size_) {
			// Multiple leaves still have a root, even when they fit in one node.
			// RadiusSearch starts there, so initialize its bounds and first child.
			if (item_count_ > 1) {
				boxes_[item_count_] = tree_box_;
				indices_[item_count_] = 0;
			}
			return;
		}

		// Calculate 16-bit Hilbert curve projection
		constexpr double max_hilbert = 65535.0;
		// Halving before subtraction avoids overflow for opposite finite extremes.
		const double width = std::max(tree_box_.max.x * 0.5 - tree_box_.min.x * 0.5, 1e-9);
		const double height = std::max(tree_box_.max.y * 0.5 - tree_box_.min.y * 0.5, 1e-9);

		std::vector<uint32_t> curve(item_count_);
		for (size_t i = 0; i < item_count_; ++i) {
			if (i % 1024 == 0) {
				CheckInterrupt();
			}
			const double norm_x = (points[i].x * 0.5 - tree_box_.min.x * 0.5) / width;
			const double norm_y = (points[i].y * 0.5 - tree_box_.min.y * 0.5) / height;
			const uint32_t hx = static_cast<uint32_t>(std::max(0.0, std::min(max_hilbert, norm_x * max_hilbert)));
			const uint32_t hy = static_cast<uint32_t>(std::max(0.0, std::min(max_hilbert, norm_y * max_hilbert)));
			curve[i] = HilbertEncode(hx, hy);
		}

		// Sort leaf indices, breaking Hilbert ties by input position. std::sort
		// bounds the worst-case work and avoids a custom recursive quicksort.
		size_t comparisons = 0;
		std::sort(indices_.begin(), indices_.begin() + item_count_, [&](size_t lhs, size_t rhs) {
			if (++comparisons % 1024 == 0) {
				CheckInterrupt();
			}
			return curve[lhs] < curve[rhs] || (curve[lhs] == curve[rhs] && lhs < rhs);
		});
		for (size_t i = 0; i < item_count_; i++) {
			if (i % 1024 == 0) {
				CheckInterrupt();
			}
			boxes_[i] = BBox2D(points[indices_[i]], points[indices_[i]]);
		}

		// Build internal R-Tree layers bottom-up
		size_t current_pos = item_count_;
		size_t layer_idx = 0;
		size_t entry_idx = 0;

		while (layer_idx < layer_bounds_.size() - 1) {
			const size_t entry_end = layer_bounds_[layer_idx];

			while (entry_idx < entry_end) {
				const size_t node_start = entry_idx;
				BBox2D node_box = boxes_[entry_idx];

				size_t child_count = 0;
				while (child_count < node_size_ && entry_idx < entry_end) {
					if (entry_idx % 1024 == 0) {
						CheckInterrupt();
					}
					node_box.Union(boxes_[entry_idx]);
					child_count++;
					entry_idx++;
				}

				indices_[current_pos] = static_cast<size_t>(node_start);
				boxes_[current_pos] = node_box;
				current_pos++;
			}

			layer_idx++;
		}
	}

	void RadiusSearch(const Point2D &center, double eps, std::vector<size_t> &matches) const {
		CheckInterrupt();
		matches.clear();
		if (item_count_ == 0) {
			return;
		}

		const double search_min_x = center.x - eps;
		const double search_min_y = center.y - eps;
		const double search_max_x = center.x + eps;
		const double search_max_y = center.y + eps;
		const BBox2D search_box(Point2D(search_min_x, search_min_y), Point2D(search_max_x, search_max_y));

		// Stack-based depth first search through tree layers
		std::vector<size_t> stack;
		stack.reserve(64);

		// Root is at the end of upper layer
		const size_t root_pos = layer_bounds_.back() - 1;
		stack.push_back(root_pos);

		size_t visited = 0;
		while (!stack.empty()) {
			if (++visited % 1024 == 0) {
				CheckInterrupt();
			}
			const size_t node_pos = stack.back();
			stack.pop_back();

			if (!search_box.Intersects(boxes_[node_pos])) {
				continue;
			}

			if (node_pos < item_count_) {
				// Leaf node: perform exact double-precision Euclidean distance check
				const size_t orig_idx = indices_[node_pos];
				if (std::hypot(center.x - (*points_)[orig_idx].x, center.y - (*points_)[orig_idx].y) <= eps) {
					matches.push_back(orig_idx);
				}
			} else {
				// Internal node: push children
				const size_t child_start = indices_[node_pos];
				const size_t child_end = std::min(child_start + node_size_, UpperBound(child_start));

				for (size_t c = child_start; c < child_end; ++c) {
					if (c % 1024 == 0) {
						CheckInterrupt();
					}
					if (search_box.Intersects(boxes_[c])) {
						stack.push_back(c);
					}
				}
			}
		}
	}

	const Point2D &GetPoint(size_t i) const {
		return (*points_)[i];
	}

	size_t Count() const {
		return item_count_;
	}

private:
	void ComputeLayerBounds() {
		layer_bounds_.clear();
		size_t count = item_count_;
		size_t total = item_count_;
		layer_bounds_.push_back(total);

		while (count > 1) {
			count = count / node_size_ + (count % node_size_ != 0);
			total += count;
			layer_bounds_.push_back(total);
		}
	}

	size_t UpperBound(size_t node_idx) const {
		for (size_t bound : layer_bounds_) {
			if (node_idx < bound) {
				return bound;
			}
		}
		return layer_bounds_.back();
	}

	size_t node_size_;
	size_t item_count_;
	BBox2D tree_box_;
	// The caller owns the points and must keep them alive and unchanged.
	const std::vector<Point2D> *points_;
	std::vector<size_t> layer_bounds_;
	std::vector<BBox2D> boxes_;
	std::vector<size_t> indices_;
	std::function<void()> interrupt_;
};

} // namespace spatial
} // namespace duckdb
