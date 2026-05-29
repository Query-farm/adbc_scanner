#include "adbc_connection.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include <nanoarrow/nanoarrow.h>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <cstdlib>

namespace adbc_scanner {
using namespace duckdb;

// Default upper bound on the number of Arrow record batches buffered between the
// DuckDB producer (AdbcInsertInOut) and the ADBC driver consumer (ExecuteUpdate
// → GetNext). Each DuckDB DataChunk is at most STANDARD_VECTOR_SIZE (2048) rows,
// so this caps resident memory at roughly N * 2048 * row_width rather than the
// full source size. Overridable via ADBC_INSERT_MAX_PENDING_BATCHES.
static constexpr size_t DEFAULT_MAX_PENDING_BATCHES = 32;

static size_t ResolveMaxPendingBatches() {
    const char *env = std::getenv("ADBC_INSERT_MAX_PENDING_BATCHES");
    if (env && *env) {
        char *end = nullptr;
        long v = std::strtol(env, &end, 10);
        if (end != env && v > 0) {
            return static_cast<size_t>(v);
        }
    }
    return DEFAULT_MAX_PENDING_BATCHES;
}

struct AdbcInsertBindData : public TableFunctionData {
    int64_t connection_id;
    string target_table;
    string mode;  // "create", "append", "replace", "create_append"
    shared_ptr<AdbcConnectionWrapper> connection;
    vector<LogicalType> input_types;
    vector<string> input_names;
};

// Bounded, blocking ArrowArrayStream bridging the DuckDB producer and the ADBC
// driver consumer.
//
// The driver pulls batches via GetNext from inside AdbcStatement::ExecuteUpdate,
// which we run on a dedicated thread (see AdbcInsertGlobalState). The DuckDB
// execution engine pushes batches via AddBatch from the (single) in-out worker.
// A bounded queue with two condition variables provides backpressure in both
// directions:
//   - AddBatch blocks when the queue is full until the consumer drains one
//     (this is what keeps RSS flat — DuckDB stops decoding ahead of the driver).
//   - GetNext blocks when the queue is empty until the producer pushes one or
//     signals completion.
// Abort()/consumer-stop signalling prevents either side from deadlocking when
// the other fails or the query is cancelled.
struct AdbcInsertStream {
    ArrowArrayStream stream;
    ArrowSchema schema;
    bool schema_set = false;

    std::mutex lock;
    std::condition_variable cv_not_empty;  // consumer waits for a batch
    std::condition_variable cv_not_full;   // producer waits for free space
    std::queue<ArrowArray> pending_batches;
    size_t max_batches;

    bool finished = false;          // producer: no more batches will arrive
    bool aborted = false;           // hard stop: consumer should error out
    bool consumer_stopped = false;  // consumer thread has exited (ok or error)
    string consumer_error;          // error message from the consumer side
    string last_error;              // surfaced to the driver via get_last_error

    explicit AdbcInsertStream(size_t max_batches_p) : max_batches(max_batches_p) {
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
        while (!pending_batches.empty()) {
            auto &batch = pending_batches.front();
            if (batch.release) {
                batch.release(&batch);
            }
            pending_batches.pop();
        }
    }

    void SetSchema(ArrowSchema *new_schema) {
        std::lock_guard<std::mutex> l(lock);
        if (schema.release) {
            schema.release(&schema);
        }
        schema = *new_schema;
        memset(new_schema, 0, sizeof(*new_schema));  // Transfer ownership
        schema_set = true;
    }

    // Producer side. Blocks while the queue is full to apply backpressure.
    // Returns false if the consumer has stopped/aborted and the batch could not
    // be handed off (the batch is released in that case).
    bool AddBatch(ArrowArray *batch) {
        std::unique_lock<std::mutex> l(lock);
        cv_not_full.wait(l, [&] {
            return pending_batches.size() < max_batches || consumer_stopped || aborted;
        });
        if (consumer_stopped || aborted) {
            if (batch->release) {
                batch->release(batch);
            }
            memset(batch, 0, sizeof(*batch));
            return false;
        }
        pending_batches.push(*batch);
        memset(batch, 0, sizeof(*batch));  // Transfer ownership
        cv_not_empty.notify_one();
        return true;
    }

    // Producer side: no more batches will be produced.
    void Finish() {
        std::lock_guard<std::mutex> l(lock);
        finished = true;
        cv_not_empty.notify_all();
    }

    // Hard abort (query cancelled or producer errored). Wakes both sides; the
    // next GetNext returns an error so ExecuteUpdate unwinds without ingesting a
    // partial result.
    void Abort(const string &reason) {
        std::lock_guard<std::mutex> l(lock);
        aborted = true;
        if (last_error.empty()) {
            last_error = reason;
        }
        cv_not_empty.notify_all();
        cv_not_full.notify_all();
    }

    // Consumer side: record that the ExecuteUpdate thread has exited so a blocked
    // producer can stop waiting.
    void MarkConsumerStopped(const string &error) {
        std::lock_guard<std::mutex> l(lock);
        consumer_stopped = true;
        if (!error.empty()) {
            consumer_error = error;
        }
        cv_not_full.notify_all();
    }

    string GetConsumerError() {
        std::lock_guard<std::mutex> l(lock);
        return consumer_error;
    }

    static int GetSchema(ArrowArrayStream *stream, ArrowSchema *out) {
        auto *self = static_cast<AdbcInsertStream *>(stream->private_data);
        std::lock_guard<std::mutex> l(self->lock);
        if (!self->schema_set) {
            self->last_error = "Schema not set";
            return EINVAL;
        }
        return ArrowSchemaDeepCopy(&self->schema, out);
    }

    static int GetNext(ArrowArrayStream *stream, ArrowArray *out) {
        auto *self = static_cast<AdbcInsertStream *>(stream->private_data);
        std::unique_lock<std::mutex> l(self->lock);
        self->cv_not_empty.wait(l, [&] {
            return !self->pending_batches.empty() || self->finished || self->aborted;
        });

        if (self->aborted && self->pending_batches.empty()) {
            self->last_error = "adbc_insert: ingestion aborted";
            return EIO;
        }

        if (self->pending_batches.empty()) {
            // finished and fully drained → end of stream
            memset(out, 0, sizeof(*out));
            return 0;
        }

        *out = self->pending_batches.front();
        self->pending_batches.pop();
        self->cv_not_full.notify_one();
        return 0;
    }

    static const char *GetLastError(ArrowArrayStream *stream) {
        auto *self = static_cast<AdbcInsertStream *>(stream->private_data);
        return self->last_error.empty() ? nullptr : self->last_error.c_str();
    }

    static void Release(ArrowArrayStream *stream) {
        // Lifetime managed externally (by AdbcInsertGlobalState).
        stream->release = nullptr;
    }
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
    global_state->insert_stream = make_uniq<AdbcInsertStream>(ResolveMaxPendingBatches());

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
