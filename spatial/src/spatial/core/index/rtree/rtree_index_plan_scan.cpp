#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/optimizer/column_lifetime_analyzer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator_extension.hpp"
#include "duckdb/storage/data_table.hpp"
#include "spatial/core/geometry/bbox.hpp"
#include "spatial/core/geometry/geometry_type.hpp"
#include "spatial/core/index/rtree/rtree_index.hpp"
#include "spatial/core/index/rtree/rtree_index_scan.hpp"
#include "spatial/core/index/rtree/rtree_module.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/util/math.hpp"

#include <spatial/core/index/rtree/rtree_index_create_logical.hpp>

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

	static bool IsSpatialPredicate(const ScalarFunction &function) {
		case_insensitive_set_t predicates = {"st_equals", "st_intersects", "st_touches", "st_crosses",
									 "st_within", "st_contains", "st_overlaps", "st_covers",
									 "st_coveredby", "st_containsproperly"};

		if(predicates.find(function.name) == predicates.end()) {
			return false;
		}
		if (function.arguments.size() < 2) {
			// We can only optimize if there are two children
			return false;
		}
		if (function.arguments[0] != GeoTypes::GEOMETRY()) {
			// We can only optimize if the first child is a GEOMETRY
			return false;
		}
		if(function.arguments[1] != GeoTypes::GEOMETRY()) {
			// We can only optimize if the second child is a GEOMETRY
			return false;
		}
		if (function.return_type != LogicalType::BOOLEAN) {
			// We can only optimize if the return type is a BOOLEAN
			return false;
		}
		return true;
	}

	static Box2D<float> GetBoundingBox(const Value &value) {
		const auto str = value.GetValueUnsafe<string_t>();
		const geometry_t blob(str);

		Box2D<double> bbox;
		if(!blob.TryGetCachedBounds(bbox)) {
			throw InternalException("Could not get bounding box from geometry");
		}

		Box2D<float> bbox_f;
		bbox_f.min.x = MathUtil::DoubleToFloatDown(bbox.min.x);
		bbox_f.min.y = MathUtil::DoubleToFloatDown(bbox.min.y);
		bbox_f.max.x = MathUtil::DoubleToFloatUp(bbox.max.x);
		bbox_f.max.y = MathUtil::DoubleToFloatUp(bbox.max.y);

		return bbox_f;
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

		// Check that the function is a spatial predicate
		if(!IsSpatialPredicate(expr_func.function)) {
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

		// Get the query geometry
		Value target_value;
		if (expr_func.children[0]->return_type == GeoTypes::GEOMETRY() && expr_func.children[0]->IsFoldable()) {
			target_value = ExpressionExecutor::EvaluateScalar(context, *expr_func.children[0]);
		}
		else if (expr_func.children[1]->return_type == GeoTypes::GEOMETRY() && expr_func.children[1]->IsFoldable()) {
			target_value = ExpressionExecutor::EvaluateScalar(context, *expr_func.children[1]);
		} else {
			// We can only optimize if one of the children is a constant
			return false;
		}

		// Compute the bounding box
		auto bbox = GetBoundingBox(target_value);

		// Find the index
		auto &duck_table = table.Cast<DuckTableEntry>();
		auto &table_info = *table.GetStorage().GetDataTableInfo();
		unique_ptr<RTreeIndexScanBindData> bind_data = nullptr;

		table_info.GetIndexes().BindAndScan<RTreeIndex>(context, table_info, [&](RTreeIndex &index_entry) {
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