#include "adbc_connection.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/main/client_context.hpp"
#include <nanoarrow/nanoarrow.h>
#include <queue>

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
    // Bound parameters (if any)
    vector<Value> params;
    vector<LogicalType> param_types;
    bool has_params = false;
    // Batch size hint for the driver (0 = use driver default)
    idx_t batch_size = 0;
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

// Helper to bind parameters to a statement
// Converts DuckDB Values to Arrow format and calls AdbcStatementBind
static void BindParameters(ClientContext &context, AdbcStatementWrapper &statement,
                           const vector<Value> &params, const vector<LogicalType> &param_types) {
    if (params.empty()) {
        return;
    }

    // Create column names for the parameters (p0, p1, p2, ...)
    vector<string> names;
    for (idx_t i = 0; i < params.size(); i++) {
        names.push_back("p" + to_string(i));
    }

    // Create a DataChunk with the parameter values
    DataChunk chunk;
    chunk.Initialize(Allocator::DefaultAllocator(), param_types);
    chunk.SetCardinality(1);

    for (idx_t i = 0; i < params.size(); i++) {
        chunk.SetValue(i, 0, params[i]);
    }

    // Convert to Arrow schema
    ArrowSchema arrow_schema;
    memset(&arrow_schema, 0, sizeof(arrow_schema));
    ClientProperties options = context.GetClientProperties();
    ArrowConverter::ToArrowSchema(&arrow_schema, param_types, names, options);

    // Convert to Arrow array
    ArrowArray arrow_array;
    memset(&arrow_array, 0, sizeof(arrow_array));
    unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> extension_type_cast;
    ArrowConverter::ToArrowArray(chunk, &arrow_array, options, extension_type_cast);

    // Bind to statement (statement takes ownership)
    statement.Bind(&arrow_array, &arrow_schema);
}

// Bind function - validates inputs and gets schema
static unique_ptr<FunctionData> AdbcScanBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<AdbcScanBindData>();

    // Get connection ID from first argument
    bind_data->connection_id = input.inputs[0].GetValue<int64_t>();

    // Get SQL query from second argument
    bind_data->query = input.inputs[1].GetValue<string>();

    // Check for params named parameter
    auto params_it = input.named_parameters.find("params");
    if (params_it != input.named_parameters.end()) {
        auto &params_value = params_it->second;
        if (!params_value.IsNull()) {
            // params should be a STRUCT (created by row(...))
            auto &params_type = params_value.type();
            if (params_type.id() != LogicalTypeId::STRUCT) {
                throw InvalidInputException("adbc_scan: 'params' must be a STRUCT (use row(...) to create it)");
            }

            // Extract child values and types from the STRUCT
            auto &children = StructValue::GetChildren(params_value);
            auto &child_types = StructType::GetChildTypes(params_type);

            for (idx_t i = 0; i < children.size(); i++) {
                bind_data->params.push_back(children[i]);
                bind_data->param_types.push_back(child_types[i].second);
            }
            bind_data->has_params = true;
        }
    }

    // Check for batch_size named parameter
    auto batch_size_it = input.named_parameters.find("batch_size");
    if (batch_size_it != input.named_parameters.end()) {
        auto &batch_size_value = batch_size_it->second;
        if (!batch_size_value.IsNull()) {
            auto batch_size = batch_size_value.GetValue<int64_t>();
            if (batch_size < 0) {
                throw InvalidInputException("adbc_scan: 'batch_size' must be a positive integer");
            }
            bind_data->batch_size = static_cast<idx_t>(batch_size);
        }
    }

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

    // Bind parameters if present (needed for schema inference)
    if (bind_data->has_params) {
        try {
            BindParameters(context, *statement, bind_data->params, bind_data->param_types);
        } catch (Exception &e) {
            throw InvalidInputException(FormatError("adbc_scan: Failed to bind parameters: " + string(e.what()), bind_data->query));
        }
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

    // Set batch size hint if provided (best-effort, driver-specific)
    // Different drivers may use different option names, so we try common ones
    if (bind_data.batch_size > 0) {
        string batch_size_str = to_string(bind_data.batch_size);
        // Try common batch size option names - ignore errors as not all drivers support these
        try {
            global_state->statement->SetOption("adbc.statement.batch_size", batch_size_str);
        } catch (...) {
            // Option not supported by this driver, ignore
        }
    }

    global_state->statement->SetSqlQuery(bind_data.query);
    global_state->statement->Prepare();

    // Bind parameters if present
    if (bind_data.has_params) {
        try {
            BindParameters(context, *global_state->statement, bind_data.params, bind_data.param_types);
        } catch (Exception &e) {
            throw InvalidInputException(FormatError("adbc_scan: Failed to bind parameters: " + string(e.what()), bind_data.query));
        }
    }

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
                                          false); // arrow_scan_is_projected = false (no projection pushdown)
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

    // Show number of bound parameters if any
    if (bind_data.has_params) {
        result["Parameters"] = to_string(bind_data.params.size());
    }

    // Show batch size if set
    if (bind_data.batch_size > 0) {
        result["BatchSize"] = to_string(bind_data.batch_size);
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

    // Add named parameter for bind parameters (accepts a STRUCT from row(...))
    adbc_scan_function.named_parameters["params"] = LogicalType::ANY;

    // Add named parameter for batch size hint (driver-specific, best-effort)
    adbc_scan_function.named_parameters["batch_size"] = LogicalType::BIGINT;

    // Disable projection pushdown - we always return all columns from the ADBC query
    adbc_scan_function.projection_pushdown = false;

    // Add progress, cardinality, and to_string callbacks
    adbc_scan_function.table_scan_progress = AdbcScanProgress;
    adbc_scan_function.cardinality = AdbcScanCardinality;
    adbc_scan_function.to_string = AdbcScanToString;

    loader.RegisterFunction(adbc_scan_function);
}

// ============================================================================
// adbc_execute - Execute DDL/DML statements (CREATE, INSERT, UPDATE, DELETE)
// ============================================================================

// Bind data for adbc_execute
struct AdbcExecuteBindData : public FunctionData {
    int64_t connection_id;
    string query;
    shared_ptr<AdbcConnectionWrapper> connection;
    vector<Value> params;
    vector<LogicalType> param_types;
    bool has_params = false;

    unique_ptr<FunctionData> Copy() const override {
        auto copy = make_uniq<AdbcExecuteBindData>();
        copy->connection_id = connection_id;
        copy->query = query;
        copy->connection = connection;
        copy->params = params;
        copy->param_types = param_types;
        copy->has_params = has_params;
        return copy;
    }

    bool Equals(const FunctionData &other_p) const override {
        auto &other = other_p.Cast<AdbcExecuteBindData>();
        return connection_id == other.connection_id && query == other.query;
    }
};

// Bind function for adbc_execute
static unique_ptr<FunctionData> AdbcExecuteBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
    auto bind_data = make_uniq<AdbcExecuteBindData>();
    return bind_data;
}

// Helper to execute a single DDL/DML statement and return rows affected
static int64_t ExecuteStatement(int64_t connection_id, const string &query) {
    // Look up connection in registry
    auto &registry = ConnectionRegistry::Get();
    auto connection = registry.Get(connection_id);
    if (!connection) {
        throw InvalidInputException("adbc_execute: Invalid connection handle: " + to_string(connection_id));
    }

    // Validate connection is still active
    if (!connection->IsInitialized()) {
        throw InvalidInputException(FormatError("adbc_execute: Connection has been closed", query));
    }

    // Create and prepare statement
    auto statement = make_shared_ptr<AdbcStatementWrapper>(connection);
    statement->Init();
    statement->SetSqlQuery(query);

    try {
        statement->Prepare();
    } catch (Exception &e) {
        throw InvalidInputException(FormatError("adbc_execute: Failed to prepare statement: " + string(e.what()), query));
    }

    // Execute the statement
    ArrowArrayStream stream;
    memset(&stream, 0, sizeof(stream));
    int64_t rows_affected = -1;

    try {
        statement->ExecuteQuery(&stream, &rows_affected);
    } catch (Exception &e) {
        throw IOException(FormatError("adbc_execute: Failed to execute statement: " + string(e.what()), query));
    }

    // Release the stream if it was created (DDL/DML may or may not create one)
    if (stream.release) {
        stream.release(&stream);
    }

    // Return rows affected (or 0 if not available)
    return rows_affected >= 0 ? rows_affected : 0;
}

// Execute function - runs DDL/DML and returns rows affected
static void AdbcExecuteFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &conn_vector = args.data[0];
    auto &query_vector = args.data[1];
    auto count = args.size();

    // Handle constant input (for constant folding optimization)
    if (conn_vector.GetVectorType() == VectorType::CONSTANT_VECTOR &&
        query_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
        if (ConstantVector::IsNull(conn_vector) || ConstantVector::IsNull(query_vector)) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
            ConstantVector::SetNull(result, true);
        } else {
            auto connection_id = conn_vector.GetValue(0).GetValue<int64_t>();
            auto query = query_vector.GetValue(0).GetValue<string>();
            auto rows_affected = ExecuteStatement(connection_id, query);
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
            ConstantVector::GetData<int64_t>(result)[0] = rows_affected;
        }
        return;
    }

    // Handle flat/dictionary vectors
    result.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data = FlatVector::GetData<int64_t>(result);

    for (idx_t row_idx = 0; row_idx < count; row_idx++) {
        auto connection_id = conn_vector.GetValue(row_idx).GetValue<int64_t>();
        auto query = query_vector.GetValue(row_idx).GetValue<string>();
        result_data[row_idx] = ExecuteStatement(connection_id, query);
    }
}

// Register adbc_execute scalar function
void RegisterAdbcExecuteFunction(DatabaseInstance &db) {
    ExtensionLoader loader(db, "adbc");

    ScalarFunction adbc_execute_function(
        "adbc_execute",
        {LogicalType::BIGINT, LogicalType::VARCHAR},
        LogicalType::BIGINT,
        AdbcExecuteFunction,
        AdbcExecuteBind
    );

    loader.RegisterFunction(adbc_execute_function);
}

//===--------------------------------------------------------------------===//
// adbc_insert - Bulk insert data into an ADBC table
//===--------------------------------------------------------------------===//

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

    // First argument is connection handle
    bind_data->connection_id = input.inputs[0].GetValue<int64_t>();
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

    // Get connection from registry
    auto &registry = ConnectionRegistry::Get();
    bind_data->connection = registry.Get(bind_data->connection_id);
    if (!bind_data->connection) {
        throw InvalidInputException("adbc_insert: Invalid connection handle: " + to_string(bind_data->connection_id));
    }

    if (!bind_data->connection->IsInitialized()) {
        throw InvalidInputException("adbc_insert: Connection has been closed");
    }

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

    loader.RegisterFunction(adbc_insert_function);
}

} // namespace adbc
} // namespace duckdb
