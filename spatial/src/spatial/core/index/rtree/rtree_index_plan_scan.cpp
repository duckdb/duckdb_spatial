#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/optimizer/column_lifetime_analyzer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/storage/data_table.hpp"
#include "spatial/core/index/rtree/rtree_index.hpp"
#include "spatial/core/index/rtree/rtree_index_scan.hpp"
#include "spatial/core/index/rtree/rtree_module.hpp"

#include <spatial/core/geometry/bbox.hpp>
#include <spatial/core/geometry/geometry_type.hpp>
#include <spatial/core/types.hpp>

namespace spatial {

namespace core {
//-----------------------------------------------------------------------------
// Plan rewriter
//-----------------------------------------------------------------------------
class RTreeIndexScanOptimizer : public OptimizerExtension {
public:
	RTreeIndexScanOptimizer() {
		optimize_function = RTreeIndexScanOptimizer::Optimize;
	}

	static bool TryOptimize(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
		// Look for a FILTER with a spatial predicate followed by a LOGICAL_GET table scan
		auto &op = *plan;

		if (op.type != LogicalOperatorType::LOGICAL_FILTER) {
			return false;
		}

		// Look for a spatial predicate
		auto &filter = op.Cast<LogicalFilter>();

		if (filter.expressions.size() != 1) {
			// We can only optimize if there is a single expression right now
			return false;
		}
		auto &expr = filter.expressions[0];
		if (expr->GetExpressionType() != ExpressionType::BOUND_FUNCTION) {
			// The expression has to be a function
			return false;
		}
		auto &expr_func = expr->Cast<BoundFunctionExpression>();
		if(expr_func.function.name != "ST_Within") {
			return false;
		}

		// Figure out the query vector
		Value target_value;
		if (expr_func.children[0]->IsFoldable()) {
			target_value = ExpressionExecutor::EvaluateScalar(context, *expr_func.children[0]);
		}
		else if (expr_func.children[1]->IsFoldable()) {
			target_value = ExpressionExecutor::EvaluateScalar(context, *expr_func.children[1]);
		} else {
			// We can only optimize if one of the children is a constant
			return false;
		}
		if (target_value.type() != GeoTypes::GEOMETRY()) {
			// We can only optimize if the constant is a GEOMETRY
			return false;
		}

		// Look for a table scan
		if(filter.children.front()->type != LogicalOperatorType::LOGICAL_GET) {
			return false;
		}
		auto &get = filter.children.front()->Cast<LogicalGet>();
		if(get.function.name != "seq_scan") {
			return false;
		}

		// We can replace the scan function with a rtree index scan (if the table has a rtree index)
		// Get the table
		auto &table = *get.GetTable();
		if (!table.IsDuckTable()) {
			// We can only replace the scan if the table is a duck table
			return false;
		}

		auto &duck_table = table.Cast<DuckTableEntry>();
		auto &table_info = *table.GetStorage().GetDataTableInfo();

		// Find the index
		unique_ptr<RTreeIndexScanBindData> bind_data = nullptr;
		table_info.GetIndexes().BindAndScan<RTreeIndex>(context, table_info, [&](RTreeIndex &index_entry) {

			// TODO: Verify
			// auto &rtree_index = index_entry.Cast<RTreeIndex>();

			// Create a query vector from the constant value
			const auto str = target_value.GetValueUnsafe<string_t>();
			const geometry_t blob(str);

			BoundingBox bbox;
			if(!blob.TryGetCachedBounds(bbox)) {
				return false;
			}

			// Create the bind data for this index given the bounding box
			bind_data = make_uniq<RTreeIndexScanBindData>(duck_table, index_entry, bbox);
			return true;
		});

		if (!bind_data) {
			// No index found
			return false;
		}

		// Replace the scan with our custom index scan function

		get.function = RTreeIndexScanFunction::GetFunction();
		auto cardinality = get.function.cardinality(context, bind_data.get());
		get.has_estimated_cardinality = cardinality->has_estimated_cardinality;
		get.estimated_cardinality = cardinality->estimated_cardinality;
		get.bind_data = std::move(bind_data);

		return true;
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
		if(!TryOptimize(input.context, plan)) {
			// No match: continue with the children
			for(auto &child : plan->children) {
				Optimize(input, child);
			}
		}
	}
};

//-----------------------------------------------------------------------------
// Register
//-----------------------------------------------------------------------------
void RTreeModule::RegisterIndexPlanScan(DatabaseInstance &db) {
	// Register the optimizer extension
	db.config.optimizer_extensions.push_back(RTreeIndexScanOptimizer());
}

} // namespace core

} // namespace spatial