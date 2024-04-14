#pragma once
#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry.hpp"

namespace spatial {

namespace core {

struct WKBWriter {
	// Write a geometry to a WKB blob attached to a vector
	static string_t Write(const geometry_t &geometry, Vector &result);

	// Write a geometry to a WKB blob into a buffer
	static void Write(const geometry_t &geometry, vector<data_t> &buffer);

	// Write a geometry to a WKB blob into an arena allocator
	static const_data_ptr_t Write(const geometry_t &geometry, uint32_t *size, ArenaAllocator &allocator);
};

} // namespace core

} // namespace spatial