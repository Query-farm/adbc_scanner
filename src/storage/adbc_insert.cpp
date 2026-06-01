#include "storage/adbc_insert.hpp"
#include "storage/adbc_catalog.hpp"
#include "storage/adbc_transaction.hpp"
#include "storage/adbc_table_entry.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include <nanoarrow/nanoarrow.h>
#include <queue>

namespace adbc_scanner {
using namespace duckdb;

// Reuse the same AdbcInsertStream from adbc_insert.cpp (the table function version).
// This is a custom ArrowArrayStream that we feed batches into.
struct AdbcCatalogInsertStream {
	ArrowArrayStream stream;
	ArrowSchema schema;
	bool schema_set = false;
	queue<ArrowArray> pending_batches;
	mutex lock;
	bool finished = false;
	string last_error;

	AdbcCatalogInsertStream() {
		memset(&stream, 0, sizeof(stream));
		memset(&schema, 0, sizeof(schema));
		stream.private_data = this;
		stream.get_schema = GetSchema;
		stream.get_next = GetNext;
		stream.get_last_error = GetLastError;
		stream.release = Release;
	}

	~AdbcCatalogInsertStream() {
		if (schema.release) {
			schema.release(&schema);
		}
		while (!pending_batches.empty()) {
			auto &batch = pending_batches.front();
			if (batch.release) {
				batch.release(&batch);
			}
			pending_batches.pop();
		}
	}

	void SetSchema(ArrowSchema *new_schema) {
		lock_guard<mutex> l(lock);
		if (schema.release) {
			schema.release(&schema);
		}
		schema = *new_schema;
		memset(new_schema, 0, sizeof(*new_schema));
		schema_set = true;
	}

	void AddBatch(ArrowArray *batch) {
		lock_guard<mutex> l(lock);
		pending_batches.push(*batch);
		memset(batch, 0, sizeof(*batch));
	}

	void Finish() {
		lock_guard<mutex> l(lock);
		finished = true;
	}

	static int GetSchema(ArrowArrayStream *stream, ArrowSchema *out) {
		auto *self = static_cast<AdbcCatalogInsertStream *>(stream->private_data);
		lock_guard<mutex> l(self->lock);
		if (!self->schema_set) {
			self->last_error = "Schema not set";
			return EINVAL;
		}
		return ArrowSchemaDeepCopy(&self->schema, out);
	}

	static int GetNext(ArrowArrayStream *stream, ArrowArray *out) {
		auto *self = static_cast<AdbcCatalogInsertStream *>(stream->private_data);
		lock_guard<mutex> l(self->lock);
		if (self->pending_batches.empty()) {
			if (self->finished) {
				memset(out, 0, sizeof(*out));
				return 0;
			}
			self->last_error = "No batches available";
			return EAGAIN;
		}
		*out = self->pending_batches.front();
		self->pending_batches.pop();
		return 0;
	}

	static const char *GetLastError(ArrowArrayStream *stream) {
		auto *self = static_cast<AdbcCatalogInsertStream *>(stream->private_data);
		return self->last_error.empty() ? nullptr : self->last_error.c_str();
	}

	static void Release(ArrowArrayStream *stream) {
		stream->release = nullptr;
	}
};

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

class AdbcInsertGlobalState : public GlobalSinkState {
public:
	shared_ptr<AdbcStatementWrapper> statement;
	unique_ptr<AdbcCatalogInsertStream> insert_stream;
	idx_t insert_count = 0;
	bool stream_bound = false;
	bool executed = false;
	ClientProperties client_properties;
	vector<LogicalType> column_types;
	vector<string> column_names;
};

unique_ptr<GlobalSinkState> AdbcInsert::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<AdbcInsertGlobalState>();
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
		connection = catalog.GetConnection();

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
		auto &catalog = schema_ref.ParentCatalog().Cast<AdbcCatalog>();
		connection = catalog.GetConnection();
		target_table = info->Base().table;
		ingest_mode = "adbc.ingest.mode.create";

		// Get types from the bound create info
		for (auto &col : info->Base().columns.Logical()) {
			state->column_types.push_back(col.GetType());
			state->column_names.push_back(col.GetName());
		}
	}

	// Create ADBC statement for bulk ingestion
	state->statement = make_shared_ptr<AdbcStatementWrapper>(connection);
	state->statement->Init();
	state->statement->SetOption("adbc.ingest.target_table", target_table);
	state->statement->SetOption("adbc.ingest.mode", ingest_mode);

	// Create the insert stream and set the schema
	state->insert_stream = make_uniq<AdbcCatalogInsertStream>();
	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, state->column_types, state->column_names,
	                               state->client_properties);
	state->insert_stream->SetSchema(&arrow_schema);

	// Bind the stream
	state->statement->BindStream(&state->insert_stream->stream);
	state->stream_bound = true;

	return std::move(state);
}

// ============================================================================
// Sink — receives DataChunks and converts to Arrow batches
// ============================================================================

SinkResultType AdbcInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &state = input.global_state.Cast<AdbcInsertGlobalState>();

	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	// Convert DuckDB DataChunk to Arrow batch
	ArrowAppender appender(state.column_types, chunk.size(), state.client_properties,
	                       ArrowTypeExtensionData::GetExtensionTypes(context.client, state.column_types));
	appender.Append(chunk, 0, chunk.size(), chunk.size());
	ArrowArray arr = appender.Finalize();

	// Feed to the insert stream
	state.insert_stream->AddBatch(&arr);
	state.insert_count += chunk.size();

	return SinkResultType::NEED_MORE_INPUT;
}

// ============================================================================
// Finalize — execute the ADBC statement
// ============================================================================

SinkFinalizeType AdbcInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                      OperatorSinkFinalizeInput &input) const {
	auto &state = input.global_state.Cast<AdbcInsertGlobalState>();

	state.insert_stream->Finish();

	if (!state.executed && state.stream_bound) {
		int64_t rows_affected = -1;
		try {
			state.statement->ExecuteUpdate(&rows_affected);
			state.executed = true;
		} catch (Exception &e) {
			throw IOException("ADBC insert failed: " + string(e.what()));
		}
	}

	return SinkFinalizeType::READY;
}

// ============================================================================
// Source — return the insert count
// ============================================================================

SourceResultType AdbcInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSourceInput &input) const {
	auto &state = sink_state->Cast<AdbcInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(state.insert_count));
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
