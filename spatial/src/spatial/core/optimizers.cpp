#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/optimizers.hpp"

namespace spatial {

namespace core {

class RangeJoinSpatialPredicateRewriter : public OptimizerExtension {
public:
	RangeJoinSpatialPredicateRewriter() {
		optimize_function = RangeJoinSpatialPredicateRewriter::Optimize;
	}

	static void Optimize(ClientContext &context, OptimizerExtensionInfo *info, unique_ptr<LogicalOperator> &plan) {

		auto &op = *plan;

		// Look for ANY_JOIN operators
		if (op.type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
			auto &any_join = op.Cast<LogicalAnyJoin>();

			// Check if the join condition is a spatial predicate and the join type is INNER
			if (any_join.condition->type == ExpressionType::BOUND_FUNCTION && any_join.join_type == JoinType::INNER) {
				auto &bound_function = any_join.condition->Cast<BoundFunctionExpression>();

				// Found a spatial predicate we can optimize
				auto lower_name = StringUtil::Lower(bound_function.function.name);
				if (lower_name == "st_intersects") {

					// Convert this into a comparison join on st_xmin, st_xmax, st_ymin, st_ymax of the two input
					// geometries

					auto &left_pred_expr = bound_function.children[0];
					auto &right_pred_expr = bound_function.children[1];

					// Lookup the st_xmin, st_xmax, st_ymin, st_ymax functions in the catalog
					auto &catalog = Catalog::GetSystemCatalog(context);
					auto &xmin_func_set = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, "", "st_xmin")
					                          .Cast<ScalarFunctionCatalogEntry>();
					auto &xmax_func_set = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, "", "st_xmax")
					                          .Cast<ScalarFunctionCatalogEntry>();
					auto &ymin_func_set = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, "", "st_ymin")
					                          .Cast<ScalarFunctionCatalogEntry>();
					auto &ymax_func_set = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, "", "st_ymax")
					                          .Cast<ScalarFunctionCatalogEntry>();

					auto xmin_func_left =
					    xmin_func_set.functions.GetFunctionByArguments(context, {left_pred_expr->return_type});
					auto xmax_func_left =
					    xmax_func_set.functions.GetFunctionByArguments(context, {left_pred_expr->return_type});
					auto ymin_func_left =
					    ymin_func_set.functions.GetFunctionByArguments(context, {left_pred_expr->return_type});
					auto ymax_func_left =
					    ymax_func_set.functions.GetFunctionByArguments(context, {left_pred_expr->return_type});

					auto xmin_func_right =
					    xmin_func_set.functions.GetFunctionByArguments(context, {right_pred_expr->return_type});
					auto xmax_func_right =
					    xmax_func_set.functions.GetFunctionByArguments(context, {right_pred_expr->return_type});
					auto ymin_func_right =
					    ymin_func_set.functions.GetFunctionByArguments(context, {right_pred_expr->return_type});
					auto ymax_func_right =
					    ymax_func_set.functions.GetFunctionByArguments(context, {right_pred_expr->return_type});

					// Create the new join condition

					// Left
					vector<unique_ptr<Expression>> left_xmin_args;
					left_xmin_args.push_back(left_pred_expr->Copy());
					auto xmin_left_expr = make_uniq<BoundFunctionExpression>(
					    LogicalType::DOUBLE, std::move(xmin_func_left), std::move(left_xmin_args), nullptr);

					vector<unique_ptr<Expression>> left_xmax_args;
					left_xmax_args.push_back(left_pred_expr->Copy());
					auto xmax_left_expr = make_uniq<BoundFunctionExpression>(
					    LogicalType::DOUBLE, std::move(xmax_func_left), std::move(left_xmax_args), nullptr);

					vector<unique_ptr<Expression>> left_ymin_args;
					left_ymin_args.push_back(left_pred_expr->Copy());
					auto ymin_left_expr = make_uniq<BoundFunctionExpression>(
					    LogicalType::DOUBLE, std::move(ymin_func_left), std::move(left_ymin_args), nullptr);

					vector<unique_ptr<Expression>> left_ymax_args;
					left_ymax_args.push_back(left_pred_expr->Copy());
					auto ymax_left_expr = make_uniq<BoundFunctionExpression>(
					    LogicalType::DOUBLE, std::move(ymax_func_left), std::move(left_ymax_args), nullptr);

					// Right
					vector<unique_ptr<Expression>> right_xmin_args;
					right_xmin_args.push_back(right_pred_expr->Copy());
					auto xmin_right_expr = make_uniq<BoundFunctionExpression>(
					    LogicalType::DOUBLE, std::move(xmin_func_right), std::move(right_xmin_args), nullptr);

					vector<unique_ptr<Expression>> right_xmax_args;
					right_xmax_args.push_back(right_pred_expr->Copy());
					auto xmax_right_expr = make_uniq<BoundFunctionExpression>(
					    LogicalType::DOUBLE, std::move(xmax_func_right), std::move(right_xmax_args), nullptr);

					vector<unique_ptr<Expression>> right_ymin_args;
					right_ymin_args.push_back(right_pred_expr->Copy());
					auto ymin_right_expr = make_uniq<BoundFunctionExpression>(
					    LogicalType::DOUBLE, std::move(ymin_func_right), std::move(right_ymin_args), nullptr);

					vector<unique_ptr<Expression>> right_ymax_args;
					right_ymax_args.push_back(right_pred_expr->Copy());
					auto ymax_right_expr = make_uniq<BoundFunctionExpression>(
					    LogicalType::DOUBLE, std::move(ymax_func_right), std::move(right_ymax_args), nullptr);

					// Now create the new join condition

					JoinCondition cmp1, cmp2, cmp3, cmp4;
					cmp1.comparison = ExpressionType::COMPARE_GREATERTHAN;
					cmp1.left = std::move(ymax_left_expr);
					cmp1.right = std::move(ymin_right_expr);

					cmp2.comparison = ExpressionType::COMPARE_LESSTHAN;
					cmp2.left = std::move(ymin_left_expr);
					cmp2.right = std::move(ymax_right_expr);

					cmp3.comparison = ExpressionType::COMPARE_GREATERTHAN;
					cmp3.left = std::move(xmax_left_expr);
					cmp3.right = std::move(xmin_right_expr);

					cmp4.comparison = ExpressionType::COMPARE_LESSTHAN;
					cmp4.left = std::move(xmin_left_expr);
					cmp4.right = std::move(xmax_right_expr);

					// Now create the new join operator
					auto new_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
					new_join->conditions.push_back(std::move(cmp1));
					new_join->conditions.push_back(std::move(cmp2));
					new_join->conditions.push_back(std::move(cmp3));
					new_join->conditions.push_back(std::move(cmp4));
					new_join->children = std::move(any_join.children);

					// Also, we need to create a filter with the original predicate
					auto filter = make_uniq<LogicalFilter>(any_join.condition->Copy());
					filter->children.push_back(std::move(new_join));

					plan = std::move(filter);
				}
			}
		}

		// Recursively optimize the children
		for (auto &child : plan->children) {
			Optimize(context, info, child);
		}
	}
};

void CoreOptimizerRules::Register(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);

	// Register the optimizer rules
	config.optimizer_extensions.push_back(RangeJoinSpatialPredicateRewriter());
}

} // namespace core

} // namespace spatial