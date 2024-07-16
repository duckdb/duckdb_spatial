#pragma once

#include "spatial/common.hpp"
#include "spatial/core/geometry/vertex.hpp"

namespace spatial {

namespace core {

template<class V>
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

	void Stretch(const V &vertex) {
		for(idx_t i = 0; i < V::SIZE; i++) {
			min[i] = MinValue(min[i], vertex[i]);
			max[i] = MaxValue(max[i], vertex[i]);
		}
    }
};

template<class T>
using Box2D = Box<PointXY<T>>;

} // namespace core

} // namespace spatial