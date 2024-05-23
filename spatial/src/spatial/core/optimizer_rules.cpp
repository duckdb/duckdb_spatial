#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/optimizer_rules.hpp"

namespace spatial {

namespace core {

//------------------------------------------------------------------------------
// Range Join Spatial Predicate Rewriter
//------------------------------------------------------------------------------
//
//	Rewrites joins on spatial predicates to range joins on their bounding boxes
//  combined with a spatial predicate filter. This turns the joins from a
//  blockwise-nested loop join into a inequality join + filter, which is much
//  faster.
//
//	All spatial predicates (except st_disjoint) imply an intersection of the
//  bounding boxes of the two geometries.
//
class RangeJoinSpatialPredicateRewriter : public OptimizerExtension {
public:
	RangeJoinSpatialPredicateRewriter() {
		optimize_function = RangeJoinSpatialPredicateRewriter::Optimize;
	}

	static void AddComparison(unique_ptr<LogicalComparisonJoin> &join, unique_ptr<BoundFunctionExpression> left,
	                          unique_ptr<BoundFunctionExpression> right, ExpressionType type) {
		JoinCondition cmp;
		cmp.comparison = type;
		cmp.left = std::move(left);
		cmp.right = std::move(right);
		join->conditions.push_back(std::move(cmp));
	}

	static bool IsTableRefsDisjoint(unordered_set<idx_t> &left_table_indexes, unordered_set<idx_t> &right_table_indexes,
	                                unordered_set<idx_t> &left_bindings, unordered_set<idx_t> &right_bindings) {

		// Check that all the left-side bindings reference the left-side tables of the join,
		// as well as that all the right-side bindings reference the right-side tables of the join.
		// and that the left and right side bindings are disjoint.

		for (auto &left_binding : left_bindings) {
			if (right_bindings.find(left_binding) != right_bindings.end()) {
				// The left side bindings reference the right side tables of the join.
				return false;
			}
			// Also check that the left side bindings are on the left side of the join
			if (left_table_indexes.find(left_binding) == left_table_indexes.end()) {
				// The left side bindings are not on the left side of the join.
				return false;
			}
		}

		for (auto &right_binding : right_bindings) {
			if (left_bindings.find(right_binding) != left_bindings.end()) {
				// The right side bindings reference the left side tables of the join.
				return false;
			}
			// Also check that the right side bindings are on the right side of the join
			if (right_table_indexes.find(right_binding) == right_table_indexes.end()) {
				// The right side bindings are not on the right side of the join.
				return false;
			}
		}

		return true;
	}

	static void TryOptimize(ClientContext &context, OptimizerExtensionInfo *info, unique_ptr<LogicalOperator> &plan) {

		auto &op = *plan;

		// Look for ANY_JOIN operators
		if (op.type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
			auto &any_join = op.Cast<LogicalAnyJoin>();

			// Check if the join condition is a spatial predicate and the join type is INNER
			if (any_join.condition->type == ExpressionType::BOUND_FUNCTION && any_join.join_type == JoinType::INNER) {
				auto bound_func_expr = any_join.condition->Copy();
				auto &bound_function = bound_func_expr->Cast<BoundFunctionExpression>();

				// Note that we cant perform this optimization for st_disjoint as all comparisons have to be AND'd
				case_insensitive_set_t predicates = {"st_equals",    "st_intersects",      "st_touches",  "st_crosses",
				                                     "st_within",    "st_contains",        "st_overlaps", "st_covers",
				                                     "st_coveredby", "st_containsproperly"};

				if (predicates.find(bound_function.function.name) != predicates.end()) {
					// Found a spatial predicate we can optimize

					// Convert this into a comparison join on st_xmin, st_xmax, st_ymin, st_ymax of the two input
					// geometries
					auto left_pred_expr = std::move(bound_function.children[0]);
					auto right_pred_expr = std::move(bound_function.children[1]);

					// We need to place the left side of the predicate on the left side of the join
					// and the right side of the predicate on the right side of the join
					// So look at the table indexes of the left and right side of the predicate
					unordered_set<idx_t> left_table_indexes;
					LogicalJoin::GetTableReferences(*any_join.children[0], left_table_indexes);

					unordered_set<idx_t> right_table_indexes;
					LogicalJoin::GetTableReferences(*any_join.children[1], right_table_indexes);

					unordered_set<idx_t> left_pred_bindings;
					LogicalJoin::GetExpressionBindings(*left_pred_expr, left_pred_bindings);

					unordered_set<idx_t> right_pred_bindings;
					LogicalJoin::GetExpressionBindings(*right_pred_expr, right_pred_bindings);

					// Check if we can optimize this join
					// We need to make sure that the left and right side of the predicate are disjoint
					// e.g.
					// 		a JOIN b ON st_intersects(a.geom, b.geom) 						=> OK
					//		a JOIN b ON st_intersects(b.geom, a.geom)				 		=> OK
					//		a JOIN b ON st_intersects(a.geom, st_union(a.geom, b.geom)) 	=> NOT OK
					auto can_split = IsTableRefsDisjoint(left_table_indexes, right_table_indexes, left_pred_bindings,
					                                     right_pred_bindings);
					if (!can_split) {
						// Try again with the left and right side of the predicate swapped
						// We can safely swap because the intersection operation we encode with the comparison join
						// is symmetric, so the order of the arguments wont matter in the "new" join condition we're
						// about to create.
						can_split = IsTableRefsDisjoint(left_table_indexes, right_table_indexes, right_pred_bindings,
						                                left_pred_bindings);
						if (!can_split) {
							// We cant optimize this join
							return;
						}
						// Swap the left and right side of the predicate
						std::swap(left_pred_expr, right_pred_expr);
					}

					// Lookup the st_xmin, st_xmax, st_ymin, st_ymax functions in the catalog
					auto &catalog = Catalog::GetSystemCatalog(context);

					auto &extent_func_set =
					    catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, "st_extent")
					        .Cast<ScalarFunctionCatalogEntry>();

					auto &xmin_func_set =
					    catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, "st_xmin")
					        .Cast<ScalarFunctionCatalogEntry>();
					auto &xmax_func_set =
					    catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, "st_xmax")
					        .Cast<ScalarFunctionCatalogEntry>();
					auto &ymin_func_set =
					    catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, "st_ymin")
					        .Cast<ScalarFunctionCatalogEntry>();
					auto &ymax_func_set =
					    catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, "st_ymax")
					        .Cast<ScalarFunctionCatalogEntry>();

					auto &left_arg_type = left_pred_expr->return_type;
					auto &right_arg_type = right_pred_expr->return_type;

					auto extent_func_left = extent_func_set.functions.GetFunctionByArguments(context, {left_arg_type});
					auto extent_func_right =
					    extent_func_set.functions.GetFunctionByArguments(context, {right_arg_type});

					auto xmin_func_left =
					    xmin_func_set.functions.GetFunctionByArguments(context, {extent_func_left.return_type});
					auto xmax_func_left =
					    xmax_func_set.functions.GetFunctionByArguments(context, {extent_func_left.return_type});
					auto ymin_func_left =
					    ymin_func_set.functions.GetFunctionByArguments(context, {extent_func_left.return_type});
					auto ymax_func_left =
					    ymax_func_set.functions.GetFunctionByArguments(context, {extent_func_left.return_type});

					auto xmin_func_right =
					    xmin_func_set.functions.GetFunctionByArguments(context, {extent_func_right.return_type});
					auto xmax_func_right =
					    xmax_func_set.functions.GetFunctionByArguments(context, {extent_func_right.return_type});
					auto ymin_func_right =
					    ymin_func_set.functions.GetFunctionByArguments(context, {extent_func_right.return_type});
					auto ymax_func_right =
					    ymax_func_set.functions.GetFunctionByArguments(context, {extent_func_right.return_type});

					// Create the new join condition
					vector<unique_ptr<Expression>> left_extent_args;
					left_extent_args.push_back(left_pred_expr->Copy());
					auto left_extent = make_uniq<BoundFunctionExpression>(
					    GeoTypes::BOX_2D(), std::move(extent_func_left), std::move(left_extent_args), nullptr);

					vector<unique_ptr<Expression>> right_extent_args;
					right_extent_args.push_back(right_pred_expr->Copy());
					auto right_extent = make_uniq<BoundFunctionExpression>(
					    GeoTypes::BOX_2D(), std::move(extent_func_right), std::move(right_extent_args), nullptr);

					// Left
					vector<unique_ptr<Expression>> left_xmin_args;
					left_xmin_args.push_back(left_extent->Copy());
					auto a_x_min = make_uniq<BoundFunctionExpression>(LogicalType::DOUBLE, std::move(xmin_func_left),
					                                                  std::move(left_xmin_args), nullptr);

					vector<unique_ptr<Expression>> left_xmax_args;
					left_xmax_args.push_back(left_extent->Copy());
					auto a_x_max = make_uniq<BoundFunctionExpression>(LogicalType::DOUBLE, std::move(xmax_func_left),
					                                                  std::move(left_xmax_args), nullptr);

					vector<unique_ptr<Expression>> left_ymin_args;
					left_ymin_args.push_back(left_extent->Copy());
					auto a_y_min = make_uniq<BoundFunctionExpression>(LogicalType::DOUBLE, std::move(ymin_func_left),
					                                                  std::move(left_ymin_args), nullptr);

					vector<unique_ptr<Expression>> left_ymax_args;
					left_ymax_args.push_back(left_extent->Copy());
					auto a_y_max = make_uniq<BoundFunctionExpression>(LogicalType::DOUBLE, std::move(ymax_func_left),
					                                                  std::move(left_ymax_args), nullptr);

					// Right
					vector<unique_ptr<Expression>> right_xmin_args;
					right_xmin_args.push_back(right_extent->Copy());
					auto b_x_min = make_uniq<BoundFunctionExpression>(LogicalType::DOUBLE, std::move(xmin_func_right),
					                                                  std::move(right_xmin_args), nullptr);

					vector<unique_ptr<Expression>> right_xmax_args;
					right_xmax_args.push_back(right_extent->Copy());
					auto b_x_max = make_uniq<BoundFunctionExpression>(LogicalType::DOUBLE, std::move(xmax_func_right),
					                                                  std::move(right_xmax_args), nullptr);

					vector<unique_ptr<Expression>> right_ymin_args;
					right_ymin_args.push_back(right_extent->Copy());
					auto b_y_min = make_uniq<BoundFunctionExpression>(LogicalType::DOUBLE, std::move(ymin_func_right),
					                                                  std::move(right_ymin_args), nullptr);

					vector<unique_ptr<Expression>> right_ymax_args;
					right_ymax_args.push_back(right_extent->Copy());
					auto b_y_max = make_uniq<BoundFunctionExpression>(LogicalType::DOUBLE, std::move(ymax_func_right),
					                                                  std::move(right_ymax_args), nullptr);

					// Now create the new join operator
					auto new_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
					AddComparison(new_join, std::move(a_x_min), std::move(b_x_max),
					              ExpressionType::COMPARE_LESSTHANOREQUALTO);
					AddComparison(new_join, std::move(a_x_max), std::move(b_x_min),
					              ExpressionType::COMPARE_GREATERTHANOREQUALTO);
					AddComparison(new_join, std::move(a_y_min), std::move(b_y_max),
					              ExpressionType::COMPARE_LESSTHANOREQUALTO);
					AddComparison(new_join, std::move(a_y_max), std::move(b_y_min),
					              ExpressionType::COMPARE_GREATERTHANOREQUALTO);

					new_join->children = std::move(any_join.children);
					if (any_join.has_estimated_cardinality) {
						new_join->estimated_cardinality = any_join.estimated_cardinality;
						new_join->has_estimated_cardinality = true;
					}

					auto filter = make_uniq<LogicalFilter>(std::move(any_join.condition));
					filter->children.push_back(std::move(new_join));

					plan = std::move(filter);
				}
			}
		}
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {

		TryOptimize(input.context, input.info.get(), plan);

		// Recursively optimize the children
		for (auto &child : plan->children) {
			Optimize(input, child);
		}
	}
};

//------------------------------------------------------------------------------
// Register optimizers
//------------------------------------------------------------------------------
void CoreOptimizerRules::Register(DatabaseInstance &db) {
	Connection con(db);
	auto &context = *con.context;

	con.BeginTransaction();
	auto &config = DBConfig::GetConfig(context);

	// Register the optimizer rules
	config.optimizer_extensions.push_back(RangeJoinSpatialPredicateRewriter());

	con.Commit();
}

} // namespace core

} // namespace spatial