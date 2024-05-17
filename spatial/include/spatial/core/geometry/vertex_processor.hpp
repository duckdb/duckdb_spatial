#pragma once

#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/core/util/cursor.hpp"
#include "spatial/core/geometry/geometry_type.hpp"

namespace spatial {

namespace core {

// TODO: Implement this
struct VertexProcessor {
	static geometry_t Process(const geometry_t &geom, const VertexVector &vertices) {
		throw NotImplementedException("VertexProcessor::Process");
	}
};

} // namespace core

} // namespace spatial