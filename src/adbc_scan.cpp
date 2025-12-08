#include "adbc_connection.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"

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
    // Arrow schema from the result
    ArrowSchemaWrapper schema_root;
    // Arrow table schema for type conversion
    ArrowTableSchema arrow_table;
};

// Global state for adbc_scan - holds the Arrow stream and statement
struct AdbcScanGlobalState : public GlobalTableFunctionState {
    // The prepared statement (owned by global state for proper lifecycle)
    shared_ptr<AdbcStatementWrapper> statement;
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
    // Total rows read so far (for progress reporting)
    atomic<idx_t> total_rows_read{0};
    // Row count from ExecuteQuery (if provided by driver)
    int64_t rows_affected = -1;
    bool has_rows_affected = false;

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

// Helper to format error messages with query context
static string FormatError(const string &message, const string &query) {
    string result = message;
    // Truncate query if too long for error message
    if (query.length() > 100) {
        result += " [Query: " + query.substr(0, 100) + "...]";
    } else {
        result += " [Query: " + query + "]";
    }
    return result;
}

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

    // Validate connection is still active
    if (!bind_data->connection->IsInitialized()) {
        throw InvalidInputException(FormatError("adbc_scan: Connection has been closed", bind_data->query));
    }

    // Create and prepare statement
    auto statement = make_shared_ptr<AdbcStatementWrapper>(bind_data->connection);
    statement->Init();
    statement->SetSqlQuery(bind_data->query);

    try {
        statement->Prepare();
    } catch (Exception &e) {
        throw InvalidInputException(FormatError("adbc_scan: Failed to prepare statement: " + string(e.what()), bind_data->query));
    }

    // Try to get schema without executing using ExecuteSchema (ADBC 1.1.0+)
    bool got_schema = false;
    try {
        got_schema = statement->ExecuteSchema(&bind_data->schema_root.arrow_schema);
    } catch (Exception &e) {
        // ExecuteSchema failed, will fall back to execute
        got_schema = false;
    }

    if (!got_schema) {
        // Fallback: driver doesn't support ExecuteSchema, need to execute to get schema
        ArrowArrayStream stream;
        memset(&stream, 0, sizeof(stream));

        try {
            statement->ExecuteQuery(&stream, nullptr);
        } catch (Exception &e) {
            throw IOException(FormatError("adbc_scan: Failed to execute query: " + string(e.what()), bind_data->query));
        }

        int ret = stream.get_schema(&stream, &bind_data->schema_root.arrow_schema);
        if (ret != 0) {
            const char *error_msg = stream.get_last_error(&stream);
            string msg = "adbc_scan: Failed to get schema from stream";
            if (error_msg) {
                msg += ": ";
                msg += error_msg;
            }
            if (stream.release) {
                stream.release(&stream);
            }
            throw IOException(FormatError(msg, bind_data->query));
        }

        // Release the stream and re-prepare for actual execution
        if (stream.release) {
            stream.release(&stream);
        }

        // Re-create statement for the actual execution
        statement = make_shared_ptr<AdbcStatementWrapper>(bind_data->connection);
        statement->Init();
        statement->SetSqlQuery(bind_data->query);
        statement->Prepare();
    }

    // Note: statement is not stored in bind_data - it will be recreated in InitGlobal
    // This is because bind_data may be reused across multiple scans

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

// Global init - create and execute the prepared statement
static unique_ptr<GlobalTableFunctionState> AdbcScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<AdbcScanBindData>();
    auto global_state = make_uniq<AdbcScanGlobalState>();

    // Validate connection is still active
    if (!bind_data.connection->IsInitialized()) {
        throw InvalidInputException(FormatError("adbc_scan: Connection has been closed", bind_data.query));
    }

    // Create fresh statement for this scan (allows multiple scans of same bind_data)
    global_state->statement = make_shared_ptr<AdbcStatementWrapper>(bind_data.connection);
    global_state->statement->Init();
    global_state->statement->SetSqlQuery(bind_data.query);
    global_state->statement->Prepare();

    // Execute the statement and capture row count if available
    memset(&global_state->stream, 0, sizeof(global_state->stream));
    int64_t rows_affected = -1;
    try {
        global_state->statement->ExecuteQuery(&global_state->stream, &rows_affected);
    } catch (Exception &e) {
        throw IOException(FormatError("adbc_scan: Failed to execute query: " + string(e.what()), bind_data.query));
    }
    global_state->stream_initialized = true;

    // Store row count for progress reporting (if driver provided it)
    if (rows_affected >= 0) {
        global_state->rows_affected = rows_affected;
        global_state->has_rows_affected = true;
    }

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
static bool GetNextBatch(AdbcScanGlobalState &global_state, AdbcScanLocalState &local_state, const string &query) {
    lock_guard<mutex> lock(global_state.main_mutex);

    if (global_state.done) {
        return false;
    }

    auto chunk = make_uniq<ArrowArrayWrapper>();

    int ret = global_state.stream.get_next(&global_state.stream, &chunk->arrow_array);
    if (ret != 0) {
        const char *error_msg = global_state.stream.get_last_error(&global_state.stream);
        string msg = "adbc_scan: Failed to get next batch from stream";
        if (error_msg) {
            msg += ": ";
            msg += error_msg;
        }
        throw IOException(FormatError(msg, query));
    }

    if (!chunk->arrow_array.release) {
        global_state.done = true;
        return false;
    }

    // Track rows for progress reporting
    global_state.total_rows_read += chunk->arrow_array.length;

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
        if (!GetNextBatch(global_state, local_state, bind_data.query)) {
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

// Progress reporting callback
static double AdbcScanProgress(ClientContext &context, const FunctionData *bind_data_p,
                                const GlobalTableFunctionState *global_state_p) {
    auto &global_state = global_state_p->Cast<AdbcScanGlobalState>();

    if (global_state.done) {
        return 100.0;
    }

    // If driver provided row count, use it for progress
    if (global_state.has_rows_affected && global_state.rows_affected > 0) {
        idx_t rows_read = global_state.total_rows_read.load();
        double progress = (static_cast<double>(rows_read) / static_cast<double>(global_state.rows_affected)) * 100.0;
        return MinValue(progress, 99.9); // Cap at 99.9% until done
    }

    // No estimate available - return -1 to indicate unknown progress
    return -1.0;
}

// Cardinality estimation callback
// Note: ADBC doesn't provide row count until execution, so we can't estimate cardinality at plan time
static unique_ptr<NodeStatistics> AdbcScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
    // No cardinality information available at bind time
    // (rows_affected is only available after ExecuteQuery in global init)
    return make_uniq<NodeStatistics>();
}

// ToString callback for EXPLAIN output
static InsertionOrderPreservingMap<string> AdbcScanToString(TableFunctionToStringInput &input) {
    InsertionOrderPreservingMap<string> result;
    auto &bind_data = input.bind_data->Cast<AdbcScanBindData>();

    // Show the SQL query being executed
    if (bind_data.query.length() > 80) {
        result["Query"] = bind_data.query.substr(0, 80) + "...";
    } else {
        result["Query"] = bind_data.query;
    }

    // Show connection ID for debugging
    result["Connection"] = to_string(bind_data.connection_id);

    return result;
}

// Register the adbc_scan table function
void RegisterAdbcTableFunctions(DatabaseInstance &db) {
    ExtensionLoader loader(db, "adbc");

    TableFunction adbc_scan_function("adbc_scan", {LogicalType::BIGINT, LogicalType::VARCHAR}, AdbcScanFunction,
                                      AdbcScanBind, AdbcScanInitGlobal, AdbcScanInitLocal);

    // Disable projection pushdown - we always return all columns from the ADBC query
    adbc_scan_function.projection_pushdown = false;

    // Add progress, cardinality, and to_string callbacks
    adbc_scan_function.table_scan_progress = AdbcScanProgress;
    adbc_scan_function.cardinality = AdbcScanCardinality;
    adbc_scan_function.to_string = AdbcScanToString;

    loader.RegisterFunction(adbc_scan_function);
}

} // namespace adbc
} // namespace duckdb
