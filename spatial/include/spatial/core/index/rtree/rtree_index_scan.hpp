#pragma once

#include "duckdb/function/table_function.hpp"
#include "spatial/core/index/rtree/rtree_node.hpp"

namespace duckdb {
class DuckTableEntry;
class Index;
} // namespace duckdb

namespace spatial {

namespace core {

// This is created by the optimizer rule
struct RTreeIndexScanBindData final : public TableFunctionData {
	explicit RTreeIndexScanBindData(DuckTableEntry &table, Index &index, const RTreeBounds &bbox)
	    : table(table), index(index), bbox(bbox) {
	}

	//! The table to scan
	DuckTableEntry &table;

	//! The index to use
	Index &index;

	//! The bounds to scan
	RTreeBounds bbox;

public:
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<RTreeIndexScanBindData>();
		return &other.table == &table;
	}
};

struct RTreeIndexScanFunction {
	static TableFunction GetFunction();
};

} // namespace core

} // namespace spatial