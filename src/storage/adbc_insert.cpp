#include "storage/adbc_insert.hpp"
#include "storage/adbc_catalog.hpp"
#include "storage/adbc_transaction.hpp"
#include "storage/adbc_table_entry.hpp"
#include "adbc_insert_stream.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include <nanoarrow/nanoarrow.h>
#include <atomic>

namespace adbc_scanner {
using namespace duckdb;

// The streaming machinery (bounded queue + background ExecuteUpdate consumer)
// lives in adbc_insert_stream.hpp and is shared with the adbc_insert table
// function. Earlier this path used an unbounded queue drained only after the
// entire input had been buffered, which OOM'd on large INSERT/CTAS — the shared
// AdbcStreamingInsertConsumer streams with backpressure so RSS stays flat.

// ============================================================================
// AdbcInsert — PhysicalOperator
// ============================================================================

AdbcInsert::AdbcInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
                       physical_index_vector_t<idx_t> column_index_map_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1),
      table(&table), schema(nullptr), column_index_map(std::move(column_index_map_p)) {
}

AdbcInsert::AdbcInsert(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema,
                       unique_ptr<BoundCreateTableInfo> info_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1),
      table(nullptr), schema(&schema), info(std::move(info_p)) {
}

// ============================================================================
// Global State
// ============================================================================

class AdbcStorageInsertGlobalState : public GlobalSinkState {
public:
	AdbcStorageInsertGlobalState() : consumer(ResolveMaxPendingBatches(0)) {
	}

	// Owns the bound statement, the bounded stream, and the background
	// ExecuteUpdate consumer thread (see adbc_insert_stream.hpp).
	AdbcStreamingInsertConsumer consumer;
	// Producer-side row count. Sink() may run on multiple threads, so make it
	// atomic; the producer count is what we report (the driver's rows_affected is
	// advisory and not all drivers populate it).
	std::atomic<int64_t> insert_count {0};
	ClientProperties client_properties;
	vector<LogicalType> column_types;
	vector<string> column_names;
};

unique_ptr<GlobalSinkState> AdbcInsert::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<AdbcStorageInsertGlobalState>();
	state->client_properties = context.GetClientProperties();

	// Determine target table name and get connection
	string target_table;
	string ingest_mode;
	shared_ptr<AdbcConnectionWrapper> connection;

	if (table) {
		// INSERT INTO existing table
		auto &adbc_table = table->Cast<AdbcTableEntry>();
		target_table = adbc_table.name;
		ingest_mode = "adbc.ingest.mode.append";

		auto &catalog = adbc_table.catalog.Cast<AdbcCatalog>();
		// Use the transaction's write connection (autocommit disabled) so the
		// insert participates in the active transaction and can be rolled back.
		connection = AdbcTransaction::Get(context, catalog).GetWriteConnection();

		// Get column info from the table
		auto &columns = adbc_table.GetColumns();
		for (idx_t i = 0; i < columns.LogicalColumnCount(); i++) {
			auto &col = columns.GetColumn(LogicalIndex(i));
			state->column_types.push_back(col.GetType());
			state->column_names.push_back(col.GetName());
		}
	} else {
		// CREATE TABLE AS
		auto &schema_ref = schema->Cast<AdbcSchemaEntry>();
		auto &catalog = const_cast<AdbcCatalog &>(schema_ref.ParentCatalog().Cast<AdbcCatalog>());
		connection = AdbcTransaction::Get(context, catalog).GetWriteConnection();
		target_table = info->Base().table;
		ingest_mode = "adbc.ingest.mode.create";

		// Get types from the bound create info
		for (auto &col : info->Base().columns.Logical()) {
			state->column_types.push_back(col.GetType());
			state->column_names.push_back(col.GetName());
		}
	}

	// Create ADBC statement for bulk ingestion
	state->consumer.statement = make_shared_ptr<AdbcStatementWrapper>(connection);
	state->consumer.statement->Init();
	state->consumer.statement->SetOption("adbc.ingest.target_table", target_table);
	state->consumer.statement->SetOption("adbc.ingest.mode", ingest_mode);

	// Set the schema on the bounded stream
	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, state->column_types, state->column_names,
	                               state->client_properties);
	state->consumer.SetSchema(&arrow_schema);

	// Bind the stream, then start the background ExecuteUpdate consumer so it
	// drains batches concurrently with the producer (keeps RSS flat).
	try {
		state->consumer.BindStream();
	} catch (std::exception &e) {
		throw IOException("ADBC insert: failed to bind stream: " + string(e.what()));
	}
	state->consumer.StartConsumer();

	return std::move(state);
}

// ============================================================================
// Sink — receives DataChunks and converts to Arrow batches
// ============================================================================

SinkResultType AdbcInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &state = input.global_state.Cast<AdbcStorageInsertGlobalState>();

	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	// Convert DuckDB DataChunk to Arrow batch
	ArrowAppender appender(state.column_types, chunk.size(), state.client_properties,
	                       ArrowTypeExtensionData::GetExtensionTypes(context.client, state.column_types));
	appender.Append(chunk, 0, chunk.size(), chunk.size());
	ArrowArray arr = appender.Finalize();

	// Hand the batch to the consumer; blocks for backpressure when the queue is
	// full. Returns false only if the consumer thread already stopped (driver
	// error / cancellation) — surface that as a query error.
	if (!state.consumer.AddBatch(&arr)) {
		string err = state.consumer.GetConsumerError();
		throw IOException("ADBC insert: ingestion stopped early: " +
		                  (err.empty() ? string("consumer terminated") : err));
	}
	state.insert_count += chunk.size();

	return SinkResultType::NEED_MORE_INPUT;
}

// ============================================================================
// Finalize — execute the ADBC statement
// ============================================================================

SinkFinalizeType AdbcInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                      OperatorSinkFinalizeInput &input) const {
	auto &state = input.global_state.Cast<AdbcStorageInsertGlobalState>();

	// Signal end of input and wait for the background ExecuteUpdate to finish
	// draining the stream. Rethrows as IOException if the consumer errored.
	state.consumer.FinishAndJoin();

	// CREATE TABLE AS creates a new remote table that the attached catalog's
	// cached table set does not yet know about. Invalidate the cache so the new
	// table is rediscovered (otherwise a query against it in the same session
	// fails with "table does not exist"). INSERT INTO an existing table does not
	// change the schema, so only CTAS needs this.
	if (!table && schema) {
		auto &adbc_catalog = const_cast<AdbcCatalog &>(schema->ParentCatalog().Cast<AdbcCatalog>());
		adbc_catalog.ClearCache();
	}

	return SinkFinalizeType::READY;
}

// ============================================================================
// Source — return the insert count
// ============================================================================

SourceResultType AdbcInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSourceInput &input) const {
	auto &state = sink_state->Cast<AdbcStorageInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(state.insert_count.load()));
	return SourceResultType::FINISHED;
}

string AdbcInsert::GetName() const {
	return table ? "ADBC_INSERT" : "ADBC_CREATE_TABLE_AS";
}

InsertionOrderPreservingMap<string> AdbcInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table ? table->name : info->Base().table;
	return result;
}

// ============================================================================
// Catalog Planning — wire into AdbcCatalog
// ============================================================================

PhysicalOperator &AdbcCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                           optional_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for ADBC insert");
	}
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT clause not yet supported for ADBC insert");
	}

	D_ASSERT(plan);
	auto &insert = planner.Make<AdbcInsert>(op, op.table, op.column_index_map);
	insert.children.push_back(*plan);
	return insert;
}

PhysicalOperator &AdbcCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                  LogicalCreateTable &op, PhysicalOperator &plan) {
	auto &insert = planner.Make<AdbcInsert>(op, op.schema, std::move(op.info));
	insert.children.push_back(plan);
	return insert;
}

} // namespace adbc_scanner
