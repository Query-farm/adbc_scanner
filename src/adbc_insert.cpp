#include "adbc_connection.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include <nanoarrow/nanoarrow.h>
#include <queue>

namespace adbc_scanner {
using namespace duckdb;

struct AdbcInsertBindData : public TableFunctionData {
    int64_t connection_id;
    string target_table;
    string mode;  // "create", "append", "replace", "create_append"
    shared_ptr<AdbcConnectionWrapper> connection;
    vector<LogicalType> input_types;
    vector<string> input_names;
};

// Custom ArrowArrayStream that we can feed batches into
// This allows us to use BindStream for proper streaming ingestion
struct AdbcInsertStream {
    ArrowArrayStream stream;
    ArrowSchema schema;
    bool schema_set = false;
    queue<ArrowArray> pending_batches;
    mutex lock;
    bool finished = false;
    string last_error;

    AdbcInsertStream() {
        memset(&stream, 0, sizeof(stream));
        memset(&schema, 0, sizeof(schema));
        stream.private_data = this;
        stream.get_schema = GetSchema;
        stream.get_next = GetNext;
        stream.get_last_error = GetLastError;
        stream.release = Release;
    }

    ~AdbcInsertStream() {
        if (schema.release) {
            schema.release(&schema);
        }
        // Release any pending batches
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
        memset(new_schema, 0, sizeof(*new_schema));  // Transfer ownership
        schema_set = true;
    }

    void AddBatch(ArrowArray *batch) {
        lock_guard<mutex> l(lock);
        pending_batches.push(*batch);
        memset(batch, 0, sizeof(*batch));  // Transfer ownership
    }

    void Finish() {
        lock_guard<mutex> l(lock);
        finished = true;
    }

    static int GetSchema(ArrowArrayStream *stream, ArrowSchema *out) {
        auto *self = static_cast<AdbcInsertStream *>(stream->private_data);
        lock_guard<mutex> l(self->lock);
        if (!self->schema_set) {
            self->last_error = "Schema not set";
            return EINVAL;
        }
        // Copy the schema (don't transfer ownership)
        return ArrowSchemaDeepCopy(&self->schema, out);
    }

    static int GetNext(ArrowArrayStream *stream, ArrowArray *out) {
        auto *self = static_cast<AdbcInsertStream *>(stream->private_data);
        lock_guard<mutex> l(self->lock);

        if (self->pending_batches.empty()) {
            if (self->finished) {
                // Signal end of stream
                memset(out, 0, sizeof(*out));
                return 0;
            }
            // No batches available yet - this shouldn't happen in our usage
            self->last_error = "No batches available";
            return EAGAIN;
        }

        *out = self->pending_batches.front();
        self->pending_batches.pop();
        return 0;
    }

    static const char *GetLastError(ArrowArrayStream *stream) {
        auto *self = static_cast<AdbcInsertStream *>(stream->private_data);
        return self->last_error.empty() ? nullptr : self->last_error.c_str();
    }

    static void Release(ArrowArrayStream *stream) {
        // Don't delete - we manage lifetime externally
        stream->release = nullptr;
    }
};

struct AdbcInsertGlobalState : public GlobalTableFunctionState {
    mutex lock;
    shared_ptr<AdbcStatementWrapper> statement;
    unique_ptr<AdbcInsertStream> insert_stream;
    int64_t rows_inserted = 0;
    bool stream_bound = false;
    bool executed = false;
    ClientProperties client_properties;

    idx_t MaxThreads() const override {
        return 1;
    }
};

static unique_ptr<FunctionData> AdbcInsertBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<AdbcInsertBindData>();

    // Check for NULL connection handle
    if (input.inputs[0].IsNull()) {
        throw InvalidInputException("adbc_insert: Connection handle cannot be NULL");
    }

    // First argument is connection handle
    bind_data->connection_id = input.inputs[0].GetValue<int64_t>();

    // Check for NULL table name
    if (input.inputs[1].IsNull()) {
        throw InvalidInputException("adbc_insert: Target table name cannot be NULL");
    }

    // Second argument is target table name
    bind_data->target_table = input.inputs[1].GetValue<string>();

    // Check for optional mode parameter (default is "append")
    auto mode_it = input.named_parameters.find("mode");
    if (mode_it != input.named_parameters.end() && !mode_it->second.IsNull()) {
        bind_data->mode = mode_it->second.GetValue<string>();
        // Validate mode
        if (bind_data->mode != "create" && bind_data->mode != "append" &&
            bind_data->mode != "replace" && bind_data->mode != "create_append") {
            throw InvalidInputException("adbc_insert: Invalid mode '" + bind_data->mode +
                                         "'. Must be one of: create, append, replace, create_append");
        }
    } else {
        bind_data->mode = "append";  // Default to append
    }

    // Get and validate connection
    bind_data->connection = GetValidatedConnection(bind_data->connection_id, "adbc_insert");

    // Store input table types and names for Arrow conversion
    bind_data->input_types = input.input_table_types;
    bind_data->input_names = input.input_table_names;

    // Return schema: rows_inserted (BIGINT)
    return_types = {LogicalType::BIGINT};
    names = {"rows_inserted"};

    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> AdbcInsertInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<AdbcInsertBindData>();
    auto global_state = make_uniq<AdbcInsertGlobalState>();

    // Store client properties for Arrow conversion
    global_state->client_properties = context.GetClientProperties();

    // Create the statement and set up for bulk ingestion
    global_state->statement = make_shared_ptr<AdbcStatementWrapper>(bind_data.connection);
    global_state->statement->Init();
    global_state->statement->SetOption("adbc.ingest.target_table", bind_data.target_table);

    // Set mode
    string mode_value;
    if (bind_data.mode == "create") {
        mode_value = "adbc.ingest.mode.create";
    } else if (bind_data.mode == "append") {
        mode_value = "adbc.ingest.mode.append";
    } else if (bind_data.mode == "replace") {
        mode_value = "adbc.ingest.mode.replace";
    } else if (bind_data.mode == "create_append") {
        mode_value = "adbc.ingest.mode.create_append";
    }
    global_state->statement->SetOption("adbc.ingest.mode", mode_value);

    // Create the insert stream
    global_state->insert_stream = make_uniq<AdbcInsertStream>();

    // Set up the schema from the input types
    ArrowSchema schema;
    ArrowConverter::ToArrowSchema(&schema, bind_data.input_types, bind_data.input_names,
                                   global_state->client_properties);
    global_state->insert_stream->SetSchema(&schema);

    // Bind the stream to the statement
    try {
        global_state->statement->BindStream(&global_state->insert_stream->stream);
        global_state->stream_bound = true;
    } catch (Exception &e) {
        throw IOException("adbc_insert: Failed to bind stream: " + string(e.what()));
    }

    return std::move(global_state);
}

static OperatorResultType AdbcInsertInOut(ExecutionContext &context, TableFunctionInput &data_p,
                                           DataChunk &input, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<AdbcInsertBindData>();
    auto &global_state = data_p.global_state->Cast<AdbcInsertGlobalState>();
    lock_guard<mutex> l(global_state.lock);

    if (input.size() == 0) {
        output.SetCardinality(0);
        return OperatorResultType::NEED_MORE_INPUT;
    }

    // Convert DuckDB DataChunk to Arrow
    ArrowAppender appender(bind_data.input_types, input.size(),
                           global_state.client_properties,
                           ArrowTypeExtensionData::GetExtensionTypes(context.client, bind_data.input_types));
    appender.Append(input, 0, input.size(), input.size());

    ArrowArray arr = appender.Finalize();

    // Add the batch to our stream
    global_state.insert_stream->AddBatch(&arr);
    global_state.rows_inserted += input.size();

    // Don't output anything during processing - we output the total at the end
    output.SetCardinality(0);
    return OperatorResultType::NEED_MORE_INPUT;
}

static OperatorFinalizeResultType AdbcInsertFinalize(ExecutionContext &context, TableFunctionInput &data_p,
                                                      DataChunk &output) {
    auto &global_state = data_p.global_state->Cast<AdbcInsertGlobalState>();
    lock_guard<mutex> l(global_state.lock);

    // Mark the stream as finished
    global_state.insert_stream->Finish();

    // Execute the statement to perform the actual insert
    if (!global_state.executed && global_state.stream_bound) {
        int64_t rows_affected = -1;
        try {
            global_state.statement->ExecuteUpdate(&rows_affected);
            global_state.executed = true;
        } catch (Exception &e) {
            throw IOException("adbc_insert: Failed to execute insert: " + string(e.what()));
        }
    }

    // Output the total rows inserted
    output.SetCardinality(1);
    output.SetValue(0, 0, Value::BIGINT(global_state.rows_inserted));

    return OperatorFinalizeResultType::FINISHED;
}

// Register adbc_insert table in-out function
void RegisterAdbcInsertFunction(DatabaseInstance &db) {
    ExtensionLoader loader(db, "adbc");

    // adbc_insert(connection_id, table_name, <table>) - Bulk insert data
    TableFunction adbc_insert_function("adbc_insert",
                                        {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::TABLE},
                                        nullptr,  // No regular function - use in_out
                                        AdbcInsertBind,
                                        AdbcInsertInitGlobal);
    adbc_insert_function.in_out_function = AdbcInsertInOut;
    adbc_insert_function.in_out_function_final = AdbcInsertFinalize;
    adbc_insert_function.named_parameters["mode"] = LogicalType::VARCHAR;

    CreateTableFunctionInfo info(adbc_insert_function);
    FunctionDescription desc;
    desc.description = "Bulk insert data from a query into an ADBC table";
    desc.parameter_names = {"connection_handle", "table_name", "data", "mode"};
    desc.parameter_types = {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::TABLE, LogicalType::VARCHAR};
    desc.examples = {"SELECT * FROM adbc_insert(conn, 'target_table', (SELECT * FROM source_table))",
                     "SELECT * FROM adbc_insert(conn, 'target', (SELECT * FROM source), mode := 'create')",
                     "SELECT * FROM adbc_insert(conn, 'target', (SELECT * FROM source), mode := 'append')"};
    desc.categories = {"adbc"};
    info.descriptions.push_back(std::move(desc));
    loader.RegisterFunction(info);
}

} // namespace adbc_scanner
