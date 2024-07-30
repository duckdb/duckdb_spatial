#pragma once

#include "spatial/common.hpp"
#include "spatial/core/geometry/vertex.hpp"

namespace spatial {

namespace core {

template <class V>
struct Box {
	using VERTEX_TYPE = V;
	using VALUE_TYPE = typename V::VALUE_TYPE;

	V min;
	V max;

	Box() : min(NumericLimits<VALUE_TYPE>::Maximum()), max(NumericLimits<VALUE_TYPE>::Minimum()) {
	}

	Box(const V &min_p, const V &max_p) : min(min_p), max(max_p) {
	}

	// Only does a 2D intersection check
	bool Intersects(const Box &other) const {
		return !(min.x > other.max.x || max.x < other.min.x || min.y > other.max.y || max.y < other.min.y);
	}

	// Only does a 2D containment check
	bool Contains(const Box &other) const {
		return min.x <= other.min.x && min.y <= other.min.y && max.x >= other.max.x && max.y >= other.max.y;
	}

	void Stretch(const V &vertex) {
		for (idx_t i = 0; i < V::SIZE; i++) {
			min[i] = MinValue(min[i], vertex[i]);
			max[i] = MaxValue(max[i], vertex[i]);
		}
	}

	void Union(const Box &other) {
		for (idx_t i = 0; i < V::SIZE; i++) {
			min[i] = MinValue(min[i], other.min[i]);
			max[i] = MaxValue(max[i], other.max[i]);
		}
	}

	// 2D area
	VALUE_TYPE Area() const {
		return (max.x - min.x) * (max.y - min.y);
	}

	// 2D Perimeter
	VALUE_TYPE Perimeter() const {
		return 2 * (max.x - min.x + max.y - min.y);
	}

	bool operator==(const Box &other) const {
		return min == other.min && max == other.max;
	}

	bool operator!=(const Box &other) const {
		return !(*this == other);
	}
};

template <class T>
using Box2D = Box<PointXY<T>>;

} // namespace core

} // namespace spatial