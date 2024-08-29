#include "spatial/core/index/rtree/rtree_index_create_logical.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"

#include "spatial/core/index/rtree/rtree_index.hpp"
#include "spatial/core/index/rtree/rtree_index_create_physical.hpp"
#include "spatial/core/types.hpp"

namespace spatial {

namespace core {

LogicalCreateRTreeIndex::LogicalCreateRTreeIndex(unique_ptr<CreateIndexInfo> info_p,
                                                 vector<unique_ptr<Expression>> expressions_p,
                                                 TableCatalogEntry &table_p)
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

static unique_ptr<PhysicalOperator> CreateNullFilter(const LogicalCreateRTreeIndex &op,
                                                     const vector<LogicalType> &types, ClientContext &context) {
	vector<unique_ptr<Expression>> filter_select_list;

	// Filter NOT NULL on the GEOMETRY column
	auto is_not_null_expr =
	    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
	auto bound_ref = make_uniq<BoundReferenceExpression>(types[0], 0);
	is_not_null_expr->children.push_back(bound_ref->Copy());

	// Filter IS_NOT_EMPTY on the GEOMETRY column
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &is_empty_entry = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, "ST_IsEmpty")
	                           .Cast<ScalarFunctionCatalogEntry>();

	auto is_empty_func = is_empty_entry.functions.GetFunctionByArguments(context, {GeoTypes::GEOMETRY()});
	vector<unique_ptr<Expression>> is_empty_args;
	is_empty_args.push_back(std::move(bound_ref));
	auto is_empty_expr = make_uniq_base<Expression, BoundFunctionExpression>(LogicalType::BOOLEAN, is_empty_func,
	                                                                         std::move(is_empty_args), nullptr);

	auto is_not_empty_expr = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_NOT, LogicalType::BOOLEAN);
	is_not_empty_expr->children.push_back(std::move(is_empty_expr));

	// Combine into an AND
	auto and_expr = make_uniq_base<Expression, BoundConjunctionExpression>(
	    ExpressionType::CONJUNCTION_AND, std::move(is_not_null_expr), std::move(is_not_empty_expr));

	filter_select_list.push_back(std::move(and_expr));

	return make_uniq<PhysicalFilter>(types, std::move(filter_select_list), op.estimated_cardinality);
}

static unique_ptr<PhysicalOperator> CreateBoundingBoxProjection(const LogicalCreateRTreeIndex &op,
                                                                const vector<LogicalType> &types,
                                                                ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	// Get the bounding box function
	auto &bbox_func_entry =
	    catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, "ST_Extent_Approx")
	        .Cast<ScalarFunctionCatalogEntry>();
	auto bbox_func = bbox_func_entry.functions.GetFunctionByArguments(context, {GeoTypes::GEOMETRY()});

	auto geom_ref_expr = make_uniq_base<Expression, BoundReferenceExpression>(GeoTypes::GEOMETRY(), 0);
	vector<unique_ptr<Expression>> bbox_args;
	bbox_args.push_back(std::move(geom_ref_expr));

	auto bbox_expr = make_uniq_base<Expression, BoundFunctionExpression>(GeoTypes::BOX_2DF(), bbox_func,
	                                                                     std::move(bbox_args), nullptr);

	// Also project the rowid column
	auto rowid_expr = make_uniq_base<Expression, BoundReferenceExpression>(LogicalType::ROW_TYPE, 1);

	vector<unique_ptr<Expression>> select_list;
	select_list.push_back(std::move(bbox_expr));
	select_list.push_back(std::move(rowid_expr));

	return make_uniq<PhysicalProjection>(types, std::move(select_list), op.estimated_cardinality);
}

static unique_ptr<PhysicalOperator> CreateOrderByMinX(const LogicalCreateRTreeIndex &op,
                                                      const vector<LogicalType> &types, ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	// Get the centroid value function
	auto &centroid_func_entry =
	    catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, "st_centroid")
	        .Cast<ScalarFunctionCatalogEntry>();
	auto centroid_func = centroid_func_entry.functions.GetFunctionByArguments(context, {GeoTypes::BOX_2DF()});
	vector<unique_ptr<Expression>> centroid_func_args;

	// Reference the geometry column
	auto geom_ref_expr = make_uniq_base<Expression, BoundReferenceExpression>(GeoTypes::BOX_2DF(), 0);
	centroid_func_args.push_back(make_uniq_base<Expression, BoundReferenceExpression>(GeoTypes::BOX_2DF(), 0));
	auto centroid_expr = make_uniq_base<Expression, BoundFunctionExpression>(GeoTypes::POINT_2D(), centroid_func,
	                                                                         std::move(centroid_func_args), nullptr);

	// Get the xmin value function
	auto &xmin_func_entry = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, "st_xmin")
	                            .Cast<ScalarFunctionCatalogEntry>();
	auto xmin_func = xmin_func_entry.functions.GetFunctionByArguments(context, {GeoTypes::POINT_2D()});
	vector<unique_ptr<Expression>> xmin_func_args;

	// Reference the centroid
	xmin_func_args.push_back(std::move(centroid_expr));
	auto xmin_expr = make_uniq_base<Expression, BoundFunctionExpression>(LogicalType::DOUBLE, xmin_func,
	                                                                     std::move(xmin_func_args), nullptr);

	vector<BoundOrderByNode> orders;
	orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, std::move(xmin_expr));
	vector<idx_t> projections = {0, 1};
	return make_uniq<PhysicalOrder>(types, std::move(orders), projections, op.estimated_cardinality);
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
		throw BinderException("RTree indexes can only be created over a single column.");
	}

	auto &expr = op.unbound_expressions[0];

	// Validate that we have the right type of expression (float array)
	if (expr->return_type != GeoTypes::GEOMETRY()) {
		throw BinderException("RTree indexes can only be created over GEOMETRY columns.");
	}

	// Validate that the expression does not have side effects
	if (!expr->IsConsistent()) {
		throw BinderException("RTree index keys cannot contain expressions with side "
		                      "effects.");
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
	auto geom_expr = op.expressions[0]->Copy();
	new_column_types.push_back(geom_expr->return_type);
	select_list.push_back(std::move(geom_expr));

	// Add the row ID to the select list
	new_column_types.emplace_back(LogicalType::ROW_TYPE);
	select_list.push_back(make_uniq<BoundReferenceExpression>(LogicalType::ROW_TYPE, op.info->scan_types.size() - 1));

	// Project the expressions
	auto projection = make_uniq<PhysicalProjection>(new_column_types, std::move(select_list), op.estimated_cardinality);
	projection->children.push_back(std::move(table_scan));

	// Filter operator for (IS_NOT_NULL) and (NOT ST_IsEmpty) on the geometry column
	auto null_filter = CreateNullFilter(op, new_column_types, context);
	null_filter->children.push_back(std::move(projection));

	// Project the bounding box and the row ID
	vector<LogicalType> projected_types = {GeoTypes::BOX_2DF(), LogicalType::ROW_TYPE};
	auto bbox_proj = CreateBoundingBoxProjection(op, projected_types, context);
	bbox_proj->children.push_back(std::move(null_filter));

	// Create an ORDER_BY operator to sort the bounding boxes by the xmin value
	auto physical_order = CreateOrderByMinX(op, projected_types, context);
	physical_order->children.push_back(std::move(bbox_proj));

	// Now finally create the actual physical create index operator
	auto physical_create_index =
	    make_uniq<PhysicalCreateRTreeIndex>(op, op.table, op.info->column_ids, std::move(op.info),
	                                        std::move(op.unbound_expressions), op.estimated_cardinality);

	physical_create_index->children.push_back(std::move(physical_order));

	return std::move(physical_create_index);
}

} // namespace core

} // namespace spatial
