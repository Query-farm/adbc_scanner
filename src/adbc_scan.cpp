#include "adbc_connection.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"

namespace duckdb {
namespace adbc {

// Bind data for adbc_scan - holds the connection, query, and schema information
struct AdbcScanBindData : public TableFunctionData {
    // Connection handle
    int64_t connection_id;
    // SQL query to execute
    string query;
    // Connection wrapper (kept alive during scan)
    shared_ptr<AdbcConnectionWrapper> connection;
    // Prepared statement (kept for execution in InitGlobal)
    shared_ptr<AdbcStatementWrapper> statement;
    // Arrow schema from the result
    ArrowSchemaWrapper schema_root;
    // Arrow table schema for type conversion
    ArrowTableSchema arrow_table;
};

// Global state for adbc_scan - holds the Arrow stream
struct AdbcScanGlobalState : public GlobalTableFunctionState {
    // The Arrow array stream from ADBC
    ArrowArrayStream stream;
    // Whether the stream is initialized
    bool stream_initialized = false;
    // Whether we're done reading
    bool done = false;
    // Mutex for thread safety
    mutex main_mutex;
    // Current batch index
    idx_t batch_index = 0;

    ~AdbcScanGlobalState() {
        if (stream_initialized && stream.release) {
            stream.release(&stream);
        }
    }

    idx_t MaxThreads() const override {
        return 1; // ADBC streams are typically single-threaded
    }
};

// Local state for adbc_scan - extends DuckDB's ArrowScanLocalState
struct AdbcScanLocalState : public ArrowScanLocalState {
    explicit AdbcScanLocalState(unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &ctx)
        : ArrowScanLocalState(std::move(current_chunk), ctx) {}
};

// Bind function - validates inputs and gets schema
static unique_ptr<FunctionData> AdbcScanBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<AdbcScanBindData>();

    // Get connection ID from first argument
    bind_data->connection_id = input.inputs[0].GetValue<int64_t>();

    // Get SQL query from second argument
    bind_data->query = input.inputs[1].GetValue<string>();

    // Look up connection in registry
    auto &registry = ConnectionRegistry::Get();
    bind_data->connection = registry.Get(bind_data->connection_id);
    if (!bind_data->connection) {
        throw InvalidInputException("adbc_scan: Invalid connection handle: " + to_string(bind_data->connection_id));
    }

    // Create and prepare statement
    bind_data->statement = make_shared_ptr<AdbcStatementWrapper>(bind_data->connection);
    bind_data->statement->Init();
    bind_data->statement->SetSqlQuery(bind_data->query);
    bind_data->statement->Prepare();

    // Try to get schema without executing using ExecuteSchema (ADBC 1.1.0+)
    bool got_schema = bind_data->statement->ExecuteSchema(&bind_data->schema_root.arrow_schema);

    if (!got_schema) {
        // Fallback: driver doesn't support ExecuteSchema, need to execute to get schema
        // Execute and get schema from the stream, then we'll need to re-prepare for actual scan
        ArrowArrayStream stream;
        memset(&stream, 0, sizeof(stream));
        bind_data->statement->ExecuteQuery(&stream, nullptr);

        int ret = stream.get_schema(&stream, &bind_data->schema_root.arrow_schema);
        if (ret != 0) {
            const char *error_msg = stream.get_last_error(&stream);
            string msg = "Failed to get schema from ADBC stream";
            if (error_msg) {
                msg += ": ";
                msg += error_msg;
            }
            if (stream.release) {
                stream.release(&stream);
            }
            throw IOException(msg);
        }

        // Release the stream and re-prepare for actual execution
        if (stream.release) {
            stream.release(&stream);
        }

        // Re-create statement for the actual execution
        bind_data->statement = make_shared_ptr<AdbcStatementWrapper>(bind_data->connection);
        bind_data->statement->Init();
        bind_data->statement->SetSqlQuery(bind_data->query);
        bind_data->statement->Prepare();
    }

    // Convert Arrow schema to DuckDB types
    ArrowTableFunction::PopulateArrowTableSchema(DBConfig::GetConfig(context), bind_data->arrow_table,
                                                  bind_data->schema_root.arrow_schema);

    // Extract column names and types
    auto &arrow_schema = bind_data->schema_root.arrow_schema;
    for (int64_t i = 0; i < arrow_schema.n_children; i++) {
        auto &child = *arrow_schema.children[i];
        names.push_back(child.name ? child.name : "column" + to_string(i));

        auto arrow_type = bind_data->arrow_table.GetColumns().at(i);
        return_types.push_back(arrow_type->GetDuckType());
    }

    return std::move(bind_data);
}

// Global init - execute the prepared statement
static unique_ptr<GlobalTableFunctionState> AdbcScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->CastNoConst<AdbcScanBindData>();
    auto global_state = make_uniq<AdbcScanGlobalState>();

    // Execute the already-prepared statement
    memset(&global_state->stream, 0, sizeof(global_state->stream));
    bind_data.statement->ExecuteQuery(&global_state->stream, nullptr);
    global_state->stream_initialized = true;

    return std::move(global_state);
}

// Local init - prepare for scanning
static unique_ptr<LocalTableFunctionState> AdbcScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *global_state_p) {
    auto current_chunk = make_uniq<ArrowArrayWrapper>();
    auto local_state = make_uniq<AdbcScanLocalState>(std::move(current_chunk), context.client);
    // Don't populate column_ids - we return all columns and let DuckDB project
    return std::move(local_state);
}

// Get the next batch from the Arrow stream
static bool GetNextBatch(AdbcScanGlobalState &global_state, AdbcScanLocalState &local_state) {
    lock_guard<mutex> lock(global_state.main_mutex);

    if (global_state.done) {
        return false;
    }

    auto chunk = make_uniq<ArrowArrayWrapper>();

    int ret = global_state.stream.get_next(&global_state.stream, &chunk->arrow_array);
    if (ret != 0) {
        const char *error_msg = global_state.stream.get_last_error(&global_state.stream);
        string msg = "Failed to get next batch from ADBC stream";
        if (error_msg) {
            msg += ": ";
            msg += error_msg;
        }
        throw IOException(msg);
    }

    if (!chunk->arrow_array.release) {
        global_state.done = true;
        return false;
    }

    local_state.chunk = shared_ptr<ArrowArrayWrapper>(chunk.release());
    local_state.chunk_offset = 0;
    local_state.Reset();
    local_state.batch_index = global_state.batch_index++;

    return true;
}

// Main scan function - converts Arrow data to DuckDB
static void AdbcScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &bind_data = data.bind_data->Cast<AdbcScanBindData>();
    auto &global_state = data.global_state->Cast<AdbcScanGlobalState>();
    auto &local_state = data.local_state->Cast<AdbcScanLocalState>();

    // Get a batch if we don't have one or we've exhausted the current one
    while (!local_state.chunk || !local_state.chunk->arrow_array.release ||
           local_state.chunk_offset >= (idx_t)local_state.chunk->arrow_array.length) {
        if (!GetNextBatch(global_state, local_state)) {
            output.SetCardinality(0);
            return;
        }
    }

    // Calculate output size
    idx_t output_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE,
                                         local_state.chunk->arrow_array.length - local_state.chunk_offset);

    output.SetCardinality(output_size);

    // Convert Arrow data to DuckDB using ArrowTableFunction::ArrowToDuckDB
    if (output_size > 0) {
        ArrowTableFunction::ArrowToDuckDB(local_state,
                                          bind_data.arrow_table.GetColumns(),
                                          output,
                                          0,      // start
                                          false); // arrow_scan_is_projected = false
    }

    local_state.chunk_offset += output.size();
    output.Verify();
}

// Register the adbc_scan table function
void RegisterAdbcTableFunctions(DatabaseInstance &db) {
    ExtensionLoader loader(db, "adbc");

    TableFunction adbc_scan_function("adbc_scan", {LogicalType::BIGINT, LogicalType::VARCHAR}, AdbcScanFunction,
                                      AdbcScanBind, AdbcScanInitGlobal, AdbcScanInitLocal);

    // Disable projection pushdown - we always return all columns from the ADBC query
    adbc_scan_function.projection_pushdown = false;

    loader.RegisterFunction(adbc_scan_function);
}

} // namespace adbc
} // namespace duckdb
