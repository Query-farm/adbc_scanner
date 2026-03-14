#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"
#include "adbc_connection.hpp"

namespace adbc_scanner {
using namespace duckdb;

class AdbcInsert : public PhysicalOperator {
public:
	//! INSERT INTO
	AdbcInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
	           physical_index_vector_t<idx_t> column_index_map);
	//! CREATE TABLE AS
	AdbcInsert(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema,
	           unique_ptr<BoundCreateTableInfo> info);

	optional_ptr<TableCatalogEntry> table;
	optional_ptr<SchemaCatalogEntry> schema;
	unique_ptr<BoundCreateTableInfo> info;
	physical_index_vector_t<idx_t> column_index_map;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override { return true; }

	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	bool IsSink() const override { return true; }
	bool ParallelSink() const override { return false; }

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace adbc_scanner
