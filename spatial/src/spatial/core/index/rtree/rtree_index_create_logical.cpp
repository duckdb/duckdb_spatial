#include "spatial/core/index/rtree/rtree_index_create_logical.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "spatial/core/index/rtree/rtree_index.hpp"
#include "spatial/core/index/rtree/rtree_index_create_physical.hpp"
#include "spatial/core/types.hpp"

#include <duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp>

namespace spatial {

namespace core {

LogicalCreateRTreeIndex::LogicalCreateRTreeIndex(unique_ptr<CreateIndexInfo> info_p,
                                               vector<unique_ptr<Expression>> expressions_p, TableCatalogEntry &table_p)
    : LogicalExtensionOperator(), info(std::move(info_p)), table(table_p) {
	for (auto &expr : expressions_p) {
		this->unbound_expressions.push_back(expr->Copy());
	}
	this->expressions = std::move(expressions_p);
}

void LogicalCreateRTreeIndex::ResolveTypes() {
	types.emplace_back(LogicalType::BIGINT);
}

void LogicalCreateRTreeIndex::ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bindings) {
	bindings = LogicalOperator::GenerateColumnBindings(0, table.GetColumns().LogicalColumnCount());

	// Visit the operator's expressions
	LogicalOperatorVisitor::EnumerateExpressions(*this,
	                                             [&](unique_ptr<Expression> *child) { res.VisitExpression(child); });
}

string LogicalCreateRTreeIndex::GetExtensionName() const {
	return "rtree_create_index";
}

unique_ptr<PhysicalOperator> LogicalCreateRTreeIndex::CreatePlan(ClientContext &context,
                                                                PhysicalPlanGenerator &generator) {

	auto &op = *this;

	// generate a physical plan for the parallel index creation which consists of the following operators
	// table scan - projection (for expression execution) - filter (NOT NULL) - order - create index
	D_ASSERT(op.children.size() == 1);
	auto table_scan = generator.CreatePlan(std::move(op.children[0]));

	// Validate that we only have one expression
	if (op.unbound_expressions.size() != 1) {
		throw BinderException("RTree indexes can only be created over a single column of keys.");
	}

	auto &expr = op.unbound_expressions[0];

	// Validate that the expression does not have side effects
	if (!expr->IsConsistent()) {
		throw BinderException("RTRee index keys cannot contain expressions with side "
		                      "effects.");
	}

	// Validate that we have the right type of expression (float array)
	if (expr->return_type != GeoTypes::GEOMETRY()) {
		throw BinderException("RTree index can only be created over GEOMETRY keys.");
	}

	// Assert that we got the right index type
	D_ASSERT(op.info->index_type == RTreeIndex::TYPE_NAME);

	// table scan operator for index key columns and row IDs
	generator.dependencies.AddDependency(op.table);

	D_ASSERT(op.info->scan_types.size() - 1 <= op.info->names.size());
	D_ASSERT(op.info->scan_types.size() - 1 <= op.info->column_ids.size());

	// projection to execute expressions on the key columns

	vector<LogicalType> new_column_types;
	vector<unique_ptr<Expression>> select_list;

	// Add the geometry expression to the select list
	auto &geom_expr = op.expressions[0];
	new_column_types.push_back(geom_expr->return_type);
	select_list.push_back(std::move(geom_expr));

	// Add the row ID to the select list
	new_column_types.emplace_back(LogicalType::ROW_TYPE);
	select_list.push_back(make_uniq<BoundReferenceExpression>(LogicalType::ROW_TYPE, op.info->scan_types.size() - 1));

	// Project the expressions
	auto projection = make_uniq<PhysicalProjection>(new_column_types, std::move(select_list), op.estimated_cardinality);
	projection->children.push_back(std::move(table_scan));

	// filter operator for IS_NOT_NULL on each key column
	vector<LogicalType> filter_types;
	vector<unique_ptr<Expression>> filter_select_list;

	for (idx_t i = 0; i < new_column_types.size() - 1; i++) {
		filter_types.push_back(new_column_types[i]);
		auto is_not_null_expr =
		    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
		auto bound_ref = make_uniq<BoundReferenceExpression>(new_column_types[i], i);
		is_not_null_expr->children.push_back(std::move(bound_ref));
		filter_select_list.push_back(std::move(is_not_null_expr));
	}

	auto null_filter =
	    make_uniq<PhysicalFilter>(std::move(filter_types), std::move(filter_select_list), op.estimated_cardinality);
	null_filter->types.emplace_back(LogicalType::ROW_TYPE);
	null_filter->children.push_back(std::move(projection));


	// Now create an ORDER_BY operator to sort the data by the hilbert value
	// optional order operator
	vector<BoundOrderByNode> orders;
	vector<idx_t> projections;

	// Project out the geometry expression and the rowid expression
	projections.emplace_back(0);
	projections.emplace_back(1);

	// But order by a hilbert value function
	auto &catalog = Catalog::GetSystemCatalog(context);

	auto &hilbert_func_entry = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, "st_hilbert").Cast<ScalarFunctionCatalogEntry>();
	auto hilbert_func = hilbert_func_entry.functions.GetFunctionByArguments(context, {GeoTypes::GEOMETRY()});

	auto geom_ref_expr = make_uniq_base<Expression, BoundReferenceExpression>(new_column_types[0], 0);
	vector<unique_ptr<Expression>> hilbert_args;
	hilbert_args.push_back(std::move(geom_ref_expr));
	auto hilbert_expr = make_uniq_base<Expression, BoundFunctionExpression>(LogicalType::UINTEGER, hilbert_func, std::move(hilbert_args), nullptr);

	orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, std::move(hilbert_expr));

	//for (idx_t i = 0; i < new_column_types.size() - 1; i++) {
		//auto col_expr = make_uniq_base<Expression, BoundReferenceExpression>(new_column_types[i], i);
		//orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, std::move(col_expr));
		//projections.emplace_back(i);
	//}
	//projections.emplace_back(new_column_types.size() - 1);

	auto physical_order = make_uniq<PhysicalOrder>(new_column_types, std::move(orders), std::move(projections),
												   op.estimated_cardinality);
	physical_order->children.push_back(std::move(null_filter));

	// Now finally create the actual physical create index operator
	auto physical_create_index =
	    make_uniq<PhysicalCreateRTreeIndex>(op, op.table, op.info->column_ids, std::move(op.info),
	                                       std::move(op.unbound_expressions), op.estimated_cardinality);

	physical_create_index->children.push_back(std::move(physical_order));

	return std::move(physical_create_index);
}

} // namespace core

} // namespace spatial
