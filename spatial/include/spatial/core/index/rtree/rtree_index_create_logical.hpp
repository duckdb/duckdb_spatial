#pragma once
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"

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

	// Actually create and plan the index creation
	unique_ptr<PhysicalOperator> CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) override;

	void Serialize(Serializer &writer) const override {
		LogicalExtensionOperator::Serialize(writer);
		writer.WritePropertyWithDefault(300, "operator_type", string("logical_rtree_create_index"));
		writer.WritePropertyWithDefault<unique_ptr<CreateIndexInfo>>(400, "info", info);
		writer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(401, "unbound_expressions", unbound_expressions);
	}

	string GetExtensionName() const override {
		return "duckdb_spatial";
	}
};

class LogicalCreateRTreeIndexOperatorExtension : public OperatorExtension {
	std::string GetName() override {
		return "duckdb_spatial";
	}
	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &reader) override {
		const auto operator_type = reader.ReadPropertyWithDefault<string>(300, "operator_type");
		// We only have one extension operator type right now
		if (operator_type != "logical_rtree_create_index") {
			throw SerializationException("This version of the spatial extension does not support operator type '%s!", operator_type);
		}
		auto create_info = reader.ReadPropertyWithDefault<unique_ptr<CreateInfo>>(400, "info");
		auto unbound_expressions = reader.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(401, "unbound_expressions");

		auto info = unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(create_info));

		// We also need to rebind the table
		auto &context = reader.Get<ClientContext &>();
		const auto &catalog = info->catalog;
		const auto &schema = info->schema;
		const auto &table_name = info->table;
		auto &table_entry = Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, table_name);

		// Return the new operator
		return make_uniq<LogicalCreateRTreeIndex>(std::move(info), std::move(unbound_expressions), table_entry);
	}
};


} // namespace core

} // namespace spatial