#pragma once
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "spatial/common.hpp"

namespace spatial {

namespace core {

class LogicalCreateRTreeIndex final : public LogicalExtensionOperator {
public:
	// Info for index creation
	unique_ptr<CreateIndexInfo> info;

	//! The table to create the index for
	TableCatalogEntry &table;

	//! Unbound expressions to be used in the optimizer
	vector<unique_ptr<Expression>> unbound_expressions;

public:
	LogicalCreateRTreeIndex(unique_ptr<CreateIndexInfo> info_p, vector<unique_ptr<Expression>> expressions_p,
						   TableCatalogEntry &table_p);
	void ResolveTypes() override;
	void ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bindings) override;
	string GetExtensionName() const override;

	// Actually create and plan the index creation
	unique_ptr<PhysicalOperator> CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) override;
};

} // namespace core

} // namespace spatial