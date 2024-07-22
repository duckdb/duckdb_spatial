#pragma once
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/storage/data_table.hpp"
#include "spatial/common.hpp"

namespace duckdb {
class DuckTableEntry;
}

namespace spatial {

namespace core {

class PhysicalCreateRTreeIndex final : public PhysicalOperator {
public:
	static constexpr auto TYPE = PhysicalOperatorType::EXTENSION;

public:
	PhysicalCreateRTreeIndex(LogicalOperator &op, TableCatalogEntry &table, const vector<column_t> &column_ids,
	                        unique_ptr<CreateIndexInfo> info, vector<unique_ptr<Expression>> unbound_expressions,
	                        idx_t estimated_cardinality);

	//! The table to create the index for
	DuckTableEntry &table;
	//! The list of column IDs required for the index
	vector<column_t> storage_ids;
	//! Info for index creation
	unique_ptr<CreateIndexInfo> info;
	//! Unbound expressions to be used in the optimizer
	vector<unique_ptr<Expression>> unbound_expressions;

public:
	//! Source interface, NOOP for this operator
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override {
		return SourceResultType::FINISHED;
	}
	bool IsSource() const override {
		return true;
	}

public:
	//! Sink interface, global sink state
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		// Not parallel, the sink order is important
		return false;
	}
};

} // namespace core

} // namespace duckdb