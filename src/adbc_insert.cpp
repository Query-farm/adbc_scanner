#include "adbc_insert_stream.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace adbc_scanner {
using namespace duckdb;

struct AdbcInsertBindData : public TableFunctionData {
    int64_t connection_id;
    string target_table;
    string mode;  // "create", "append", "replace", "create_append"
    shared_ptr<AdbcConnectionWrapper> connection;
    vector<LogicalType> input_types;
    vector<string> input_names;
    // Per-call override for the bounded-queue depth (0 = fall back to
    // ADBC_INSERT_MAX_PENDING_BATCHES / the built-in default). Lets callers with
    // fat rows (e.g. raster BLOB blocks) cap memory tighter than the default.
    int64_t max_batches = 0;
};

struct AdbcInsertGlobalState : public GlobalTableFunctionState {
    mutex lock;
    shared_ptr<AdbcStatementWrapper> statement;
    unique_ptr<AdbcInsertStream> insert_stream;
    int64_t rows_inserted = 0;
    bool stream_bound = false;
    ClientProperties client_properties;

    // Background consumer: runs AdbcStatement::ExecuteUpdate, which pulls from
    // insert_stream via GetNext concurrently with the producer pushing batches.
    std::thread exec_thread;
    bool exec_ok = false;
    string exec_error;
    int64_t exec_rows_affected = -1;

    idx_t MaxThreads() const override {
        return 1;  // single producer — keep AddBatch ordering simple
    }

    void StartConsumer() {
        exec_thread = std::thread([this]() {
            try {
                statement->ExecuteUpdate(&exec_rows_affected);
                exec_ok = true;
                insert_stream->MarkConsumerStopped(string());
            } catch (std::exception &e) {
                exec_ok = false;
                exec_error = e.what();
                insert_stream->MarkConsumerStopped(exec_error);
            } catch (...) {
                exec_ok = false;
                exec_error = "unknown error during ExecuteUpdate";
                insert_stream->MarkConsumerStopped(exec_error);
            }
        });
    }

    void JoinConsumer() {
        if (exec_thread.joinable()) {
            exec_thread.join();
        }
    }

    ~AdbcInsertGlobalState() override {
        // Abnormal teardown (producer threw, query cancelled): make sure the
        // consumer thread can never block forever, then join it.
        if (insert_stream && exec_thread.joinable()) {
            insert_stream->Abort("adbc_insert: aborted before completion");
        }
        JoinConsumer();
    }
};

static unique_ptr<FunctionData> AdbcInsertBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
    (void)context;
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

    // Optional per-call queue-depth override.
    auto mb_it = input.named_parameters.find("max_batches");
    if (mb_it != input.named_parameters.end() && !mb_it->second.IsNull()) {
        bind_data->max_batches = mb_it->second.GetValue<int64_t>();
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

    // Create the bounded insert stream
    global_state->insert_stream = make_uniq<AdbcInsertStream>(ResolveMaxPendingBatches(bind_data.max_batches));

    // Set up the schema from the input types
    ArrowSchema schema;
    ArrowConverter::ToArrowSchema(&schema, bind_data.input_types, bind_data.input_names,
                                   global_state->client_properties);
    global_state->insert_stream->SetSchema(&schema);

    // Bind the stream to the statement (stores the stream; does not consume yet)
    try {
        global_state->statement->BindStream(&global_state->insert_stream->stream);
        global_state->stream_bound = true;
    } catch (Exception &e) {
        throw IOException("adbc_insert: Failed to bind stream: " + string(e.what()));
    }

    // Start draining concurrently: ExecuteUpdate runs on its own thread and
    // pulls batches from the bound stream as we push them. Without this overlap
    // the queue would have to hold the entire source before ExecuteUpdate ran.
    global_state->StartConsumer();

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

    // Hand the batch to the consumer; blocks for backpressure when the queue is
    // full. Returns false only if the consumer thread already stopped (error /
    // cancellation) — surface that as a query error.
    if (!global_state.insert_stream->AddBatch(&arr)) {
        string err = global_state.insert_stream->GetConsumerError();
        throw IOException("adbc_insert: ingestion stopped early: " +
                          (err.empty() ? string("consumer terminated") : err));
    }
    global_state.rows_inserted += input.size();

    // Don't output anything during processing - we output the total at the end
    output.SetCardinality(0);
    return OperatorResultType::NEED_MORE_INPUT;
}

static OperatorFinalizeResultType AdbcInsertFinalize(ExecutionContext &context, TableFunctionInput &data_p,
                                                      DataChunk &output) {
    (void)context;
    auto &global_state = data_p.global_state->Cast<AdbcInsertGlobalState>();
    lock_guard<mutex> l(global_state.lock);

    // Signal end of input, then wait for ExecuteUpdate to finish draining.
    global_state.insert_stream->Finish();
    global_state.JoinConsumer();

    if (global_state.stream_bound && !global_state.exec_ok) {
        throw IOException("adbc_insert: Failed to execute insert: " + global_state.exec_error);
    }

    // Output the total rows inserted (producer-side count is reliable across all
    // drivers; the driver's rows_affected is advisory).
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
    // Optional bounded-queue depth override (default 32 / ADBC_INSERT_MAX_PENDING_BATCHES).
    adbc_insert_function.named_parameters["max_batches"] = LogicalType::BIGINT;

    CreateTableFunctionInfo info(adbc_insert_function);
    FunctionDescription desc;
    desc.description = "Bulk insert data from a query into an ADBC table";
    desc.parameter_names = {"connection_handle", "table_name", "data", "mode", "max_batches"};
    desc.parameter_types = {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::TABLE, LogicalType::VARCHAR, LogicalType::BIGINT};
    desc.examples = {"SELECT * FROM adbc_insert(conn, 'target_table', (SELECT * FROM source_table))",
                     "SELECT * FROM adbc_insert(conn, 'target', (SELECT * FROM source), mode := 'create')",
                     "SELECT * FROM adbc_insert(conn, 'target', (SELECT * FROM source), mode := 'append')"};
    desc.categories = {"adbc"};
    info.descriptions.push_back(std::move(desc));
    loader.RegisterFunction(info);
}

} // namespace adbc_scanner
