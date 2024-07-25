#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "spatial/core/types.hpp"
#include "spatial/core/index/rtree/rtree_module.hpp"
#include "spatial/core/index/rtree/rtree_index.hpp"
#include "spatial/core/index/rtree/rtree_index_create_logical.hpp"

namespace spatial {

namespace core {

//-----------------------------------------------------------------------------
// Plan rewriter
//-----------------------------------------------------------------------------
class RTreeIndexInsertionRewriter : public OptimizerExtension {
public:
	RTreeIndexInsertionRewriter() {
		optimize_function = RTreeIndexInsertionRewriter::Optimize;
	}

	static void TryOptimize(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
		auto &op = *plan;

		// Look for a CREATE INDEX operator
		if (op.type != LogicalOperatorType::LOGICAL_CREATE_INDEX) {
			return;
		}
		auto &create_index = op.Cast<LogicalCreateIndex>();

		if (create_index.info->index_type != RTreeIndex::TYPE_NAME) {
			// Not the index type we are looking for
			return;
		}

		// Verify the number of expressions
		if (create_index.expressions.size() != 1) {
			throw BinderException("RTree indexes can only be created over a single column of keys.");
		}

		// Verify the expression type
		if (create_index.expressions[0]->return_type != GeoTypes::GEOMETRY()) {
			throw BinderException("RTree indexes can only be created over GEOMETRY columns.");
		}

		// We have a create index operator for our index
		// We can replace this with a operator that creates the index
		// The "LogicalCreateRTreeIndex" operator is a custom operator that we defined in the extension
		auto create_rtree_index = make_uniq<LogicalCreateRTreeIndex>(
			std::move(create_index.info), std::move(create_index.expressions), create_index.table);

		// Move the children
		create_rtree_index->children = std::move(create_index.children);

		// Replace the operator
		plan = std::move(create_rtree_index);
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {

		TryOptimize(input.context, plan);

		// Recursively traverse the children
		for (auto &child : plan->children) {
			Optimize(input, child);
		}
	}
};

//-------------------------------------------------------------
// Register
//-------------------------------------------------------------
void RTreeModule::RegisterIndexPlanCreate(DatabaseInstance &db) {
	// Register the optimizer extension
	db.config.optimizer_extensions.push_back(RTreeIndexInsertionRewriter());
}

} // namespace core

} // namespace spatial
