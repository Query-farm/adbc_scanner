#include "adbc_connection.hpp"
#include "adbc_filter_pushdown.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include <nanoarrow/nanoarrow.h>
#include <optional>
#include <variant>

namespace adbc_scanner {
using namespace duckdb;

// Numeric value from ADBC statistics stored as a variant (int64, uint64, or double)
struct AdbcStatValue {
    std::optional<std::variant<int64_t, uint64_t, double>> value;

    bool HasValue() const { return value.has_value(); }
    void SetInt64(int64_t v) { value = v; }
    void SetUInt64(uint64_t v) { value = v; }
    void SetFloat64(double v) { value = v; }

    // Type index: 0=int64, 1=uint64, 2=double (matches variant order)
    size_t TypeIndex() const { return value.has_value() ? value->index() : static_cast<size_t>(-1); }
};

// Column statistics from ADBC
struct AdbcColumnStatistics {
    std::optional<idx_t> distinct_count;
    std::optional<idx_t> null_count;
    AdbcStatValue min_value;
    AdbcStatValue max_value;
};

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
    // Batch size hint for the driver (0 = use driver default)
    idx_t batch_size = 0;
    // For adbc_scan_table: store catalog, schema, table name and column names for projection pushdown
    string catalog_name;  // Optional catalog (empty = default)
    string schema_name;   // Optional schema (empty = default)
    string table_name;    // Empty for adbc_scan, set for adbc_scan_table
    vector<string> all_column_names;  // All columns in the table (for building projected query)
    // Cached row count from ADBC statistics (for cardinality estimation)
    std::optional<idx_t> estimated_row_count;
    // Cached column statistics from ADBC (for optimizer statistics)
    // Key is column name (case-sensitive)
    unordered_map<string, AdbcColumnStatistics> column_statistics;
    // Return types for each column (needed for creating BaseStatistics)
    vector<LogicalType> return_types;

    // Helper to check if we have bound parameters
    bool HasParams() const { return !params.empty(); }
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
    std::optional<int64_t> rows_affected;
    // For adbc_scan_table with projection: store the projected schema
    ArrowSchemaWrapper projected_schema;
    ArrowTableSchema projected_arrow_table;
    bool has_projected_schema = false;

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

// ============================================================================
// Shared bind logic for adbc_scan and adbc_scan_table
// ============================================================================

// Helper to extract batch_size from named parameters
static idx_t ExtractBatchSize(TableFunctionBindInput &input, const string &func_name) {
    auto batch_size_it = input.named_parameters.find("batch_size");
    if (batch_size_it != input.named_parameters.end()) {
        auto &batch_size_value = batch_size_it->second;
        if (!batch_size_value.IsNull()) {
            auto batch_size = batch_size_value.GetValue<int64_t>();
            if (batch_size < 0) {
                throw InvalidInputException(func_name + ": 'batch_size' must be a positive integer");
            }
            return static_cast<idx_t>(batch_size);
        }
    }
    return 0;
}

// Helper to get schema from a prepared statement (tries ExecuteSchema first, falls back to execute)
// Returns true if schema was obtained, populates schema_root
static void GetSchemaFromStatement(AdbcStatementWrapper &statement, const string &query,
                                    ArrowSchemaWrapper &schema_root, const string &func_name) {
    // Try to get schema without executing using ExecuteSchema (ADBC 1.1.0+)
    bool got_schema = false;
    try {
        got_schema = statement.ExecuteSchema(&schema_root.arrow_schema);
    } catch (Exception &e) {
        got_schema = false;
    }

    if (!got_schema) {
        // Fallback: driver doesn't support ExecuteSchema, need to execute to get schema
        ArrowArrayStream stream;
        memset(&stream, 0, sizeof(stream));

        try {
            statement.ExecuteQuery(&stream, nullptr);
        } catch (Exception &e) {
            throw IOException(FormatError(func_name + ": Failed to execute query: " + string(e.what()), query));
        }

        int ret = stream.get_schema(&stream, &schema_root.arrow_schema);
        if (ret != 0) {
            const char *error_msg = stream.get_last_error(&stream);
            string msg = func_name + ": Failed to get schema from stream";
            if (error_msg) {
                msg += ": ";
                msg += error_msg;
            }
            if (stream.release) {
                stream.release(&stream);
            }
            throw IOException(FormatError(msg, query));
        }

        // Release the stream
        if (stream.release) {
            stream.release(&stream);
        }
    }
}

// Helper to populate return types and names from Arrow schema
static void PopulateReturnTypesFromSchema(ClientContext &context, AdbcScanBindData &bind_data,
                                           vector<LogicalType> &return_types, vector<string> &names) {
    // Convert Arrow schema to DuckDB types
    ArrowTableFunction::PopulateArrowTableSchema(DBConfig::GetConfig(context), bind_data.arrow_table,
                                                  bind_data.schema_root.arrow_schema);

    // Extract column names and types
    auto &arrow_schema = bind_data.schema_root.arrow_schema;
    for (int64_t i = 0; i < arrow_schema.n_children; i++) {
        auto &child = *arrow_schema.children[i];
        string col_name = child.name ? child.name : "column" + to_string(i);
        names.push_back(col_name);

        auto arrow_type = bind_data.arrow_table.GetColumns().at(i);
        return_types.push_back(arrow_type->GetDuckType());
    }
}

// Helper to extract a statistic value from the ADBC statistics union
// Returns true if value was successfully extracted (as double for count-type stats)
static bool ExtractStatisticValue(ArrowArrayView *stat_value_view, int64_t row_idx, double &out_value) {
    if (!stat_value_view || stat_value_view->n_children == 0) {
        return false;
    }

    int8_t type_id = ArrowArrayViewUnionTypeId(stat_value_view, row_idx);
    int64_t child_idx = ArrowArrayViewUnionChildIndex(stat_value_view, row_idx);

    if (type_id == 0 && stat_value_view->children[0]) {
        // int64
        out_value = static_cast<double>(ArrowArrayViewGetIntUnsafe(stat_value_view->children[0], child_idx));
        return true;
    } else if (type_id == 1 && stat_value_view->children[1]) {
        // uint64
        out_value = static_cast<double>(static_cast<uint64_t>(ArrowArrayViewGetIntUnsafe(stat_value_view->children[1], child_idx)));
        return true;
    } else if (type_id == 2 && stat_value_view->children[2]) {
        // float64
        out_value = ArrowArrayViewGetDoubleUnsafe(stat_value_view->children[2], child_idx);
        return true;
    }
    // type_id == 3 is binary, which we don't handle for numeric stats

    return false;
}

// Helper to extract min/max values into AdbcStatValue
static bool ExtractStatValueTyped(ArrowArrayView *stat_value_view, int64_t row_idx, AdbcStatValue &out) {
    if (!stat_value_view || stat_value_view->n_children == 0) {
        return false;
    }

    int8_t type_id = ArrowArrayViewUnionTypeId(stat_value_view, row_idx);
    int64_t child_idx = ArrowArrayViewUnionChildIndex(stat_value_view, row_idx);

    if (type_id == 0 && stat_value_view->children[0]) {
        out.SetInt64(ArrowArrayViewGetIntUnsafe(stat_value_view->children[0], child_idx));
        return true;
    } else if (type_id == 1 && stat_value_view->children[1]) {
        out.SetUInt64(static_cast<uint64_t>(ArrowArrayViewGetIntUnsafe(stat_value_view->children[1], child_idx)));
        return true;
    } else if (type_id == 2 && stat_value_view->children[2]) {
        out.SetFloat64(ArrowArrayViewGetDoubleUnsafe(stat_value_view->children[2], child_idx));
        return true;
    }
    return false;
}

// Helper to get column name from ArrowArrayView at given index
// Returns empty string if column_name is null (table-level statistic)
static string GetColumnName(ArrowArrayView *column_name_view, int64_t row_idx) {
    if (ArrowArrayViewIsNull(column_name_view, row_idx)) {
        return "";
    }
    ArrowStringView sv = ArrowArrayViewGetStringUnsafe(column_name_view, row_idx);
    return string(sv.data, sv.size_bytes);
}

// Fetch all statistics (table-level and column-level) from ADBC
// Populates bind_data with row count and column statistics
static void TryGetStatisticsFromADBC(AdbcConnectionWrapper &connection, const string &catalog,
                                      const string &schema, const string &table_name,
                                      AdbcScanBindData &bind_data) {
    ArrowArrayStream stream;
    memset(&stream, 0, sizeof(stream));

    // Convert empty strings to nullptr for ADBC API
    const char *catalog_ptr = catalog.empty() ? nullptr : catalog.c_str();
    const char *schema_ptr = schema.empty() ? nullptr : schema.c_str();

    // Try to get statistics (approximate is fine)
    bool got_stats = false;
    try {
        got_stats = connection.GetStatistics(catalog_ptr, schema_ptr, table_name.c_str(), 1, &stream);
    } catch (...) {
        return;  // Statistics not supported - that's okay
    }

    if (!got_stats) {
        return;
    }

    ArrowArray batch;
    memset(&batch, 0, sizeof(batch));

    while (true) {
        int ret = stream.get_next(&stream, &batch);
        if (ret != 0 || batch.release == nullptr) {
            break;
        }

        ArrowSchema schema;
        memset(&schema, 0, sizeof(schema));
        ret = stream.get_schema(&stream, &schema);
        if (ret != 0) {
            if (batch.release) batch.release(&batch);
            break;
        }

        ArrowArrayView view;
        ArrowArrayViewInitFromSchema(&view, &schema, nullptr);
        ret = ArrowArrayViewSetArray(&view, &batch, nullptr);
        if (ret != 0) {
            ArrowArrayViewReset(&view);
            if (schema.release) schema.release(&schema);
            if (batch.release) batch.release(&batch);
            break;
        }

        // Navigate the nested structure
        // Root has: catalog_name (0), catalog_db_schemas (1)
        if (view.n_children >= 2) {
            ArrowArrayView *db_schemas_list = view.children[1];
            if (db_schemas_list && db_schemas_list->n_children >= 1) {
                ArrowArrayView *db_schema_struct = db_schemas_list->children[0];
                // db_schema_struct has: db_schema_name (0), db_schema_statistics (1)
                if (db_schema_struct && db_schema_struct->n_children >= 2) {
                    ArrowArrayView *stats_list = db_schema_struct->children[1];
                    if (stats_list && stats_list->n_children >= 1) {
                        ArrowArrayView *stats_struct = stats_list->children[0];
                        // stats_struct has: table_name (0), column_name (1), statistic_key (2),
                        //                   statistic_value (3), statistic_is_approximate (4)
                        if (stats_struct && stats_struct->n_children >= 4) {
                            ArrowArrayView *column_name_view = stats_struct->children[1];
                            ArrowArrayView *stat_key_view = stats_struct->children[2];
                            ArrowArrayView *stat_value_view = stats_struct->children[3];

                            int64_t n_stats = stats_struct->array->length;
                            for (int64_t i = 0; i < n_stats; i++) {
                                string col_name = GetColumnName(column_name_view, i);
                                int16_t stat_key = ArrowArrayViewGetIntUnsafe(stat_key_view, i);
                                double value;
                                bool got_value = ExtractStatisticValue(stat_value_view, i, value);

                                if (!got_value) {
                                    continue;
                                }

                                if (col_name.empty()) {
                                    // Table-level statistic
                                    if (stat_key == ADBC_STATISTIC_ROW_COUNT_KEY && value >= 0) {
                                        bind_data.estimated_row_count = static_cast<idx_t>(value);
                                    }
                                } else {
                                    // Column-level statistic
                                    auto &col_stats = bind_data.column_statistics[col_name];

                                    switch (stat_key) {
                                    case ADBC_STATISTIC_DISTINCT_COUNT_KEY:
                                        col_stats.distinct_count = static_cast<idx_t>(value);
                                        break;
                                    case ADBC_STATISTIC_NULL_COUNT_KEY:
                                        col_stats.null_count = static_cast<idx_t>(value);
                                        break;
                                    case ADBC_STATISTIC_MIN_VALUE_KEY:
                                        ExtractStatValueTyped(stat_value_view, i, col_stats.min_value);
                                        break;
                                    case ADBC_STATISTIC_MAX_VALUE_KEY:
                                        ExtractStatValueTyped(stat_value_view, i, col_stats.max_value);
                                        break;
                                    default:
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        ArrowArrayViewReset(&view);
        if (schema.release) schema.release(&schema);
        if (batch.release) batch.release(&batch);
    }

    if (stream.release) {
        stream.release(&stream);
    }
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

    // Check for NULL connection handle first
    if (input.inputs[0].IsNull()) {
        throw InvalidInputException("adbc_scan: Connection handle cannot be NULL");
    }
    bind_data->connection_id = input.inputs[0].GetValue<int64_t>();

    // Check for NULL query before connection validation
    if (input.inputs[1].IsNull()) {
        throw InvalidInputException("adbc_scan: Query cannot be NULL");
    }
    bind_data->query = input.inputs[1].GetValue<string>();

    // Extract batch_size parameter
    bind_data->batch_size = ExtractBatchSize(input, "adbc_scan");

    // Now validate and get connection wrapper
    bind_data->connection = GetValidatedConnection(bind_data->connection_id, "adbc_scan");

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
        }
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
    if (bind_data->HasParams()) {
        try {
            BindParameters(context, *statement, bind_data->params, bind_data->param_types);
        } catch (Exception &e) {
            throw InvalidInputException(FormatError("adbc_scan: Failed to bind parameters: " + string(e.what()), bind_data->query));
        }
    }

    // Get schema from statement (tries ExecuteSchema, falls back to execute)
    GetSchemaFromStatement(*statement, bind_data->query, bind_data->schema_root, "adbc_scan");

    // Note: statement is not stored in bind_data - it will be recreated in InitGlobal
    // This is because bind_data may be reused across multiple scans

    // Populate return types and names from Arrow schema
    PopulateReturnTypesFromSchema(context, *bind_data, return_types, names);

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
    if (bind_data.HasParams()) {
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
    }

    return std::move(global_state);
}

// Local init - prepare for scanning
static unique_ptr<LocalTableFunctionState> AdbcScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *global_state_p) {
    auto current_chunk = make_uniq<ArrowArrayWrapper>();
    auto local_state = make_uniq<AdbcScanLocalState>(std::move(current_chunk), context.client);

    // Populate column_ids for projection pushdown
    // ArrowToDuckDB uses these to map output column indices to Arrow array child indices
    for (auto &col_id : input.column_ids) {
        local_state->column_ids.push_back(col_id);
    }

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
    // arrow_scan_is_projected = false because the ADBC driver returns all columns,
    // but ArrowToDuckDB will use local_state.column_ids to extract only the needed columns
    if (output_size > 0) {
        ArrowTableFunction::ArrowToDuckDB(local_state,
                                          bind_data.arrow_table.GetColumns(),
                                          output,
                                          false);
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
    if (global_state.rows_affected.has_value() && *global_state.rows_affected > 0) {
        idx_t rows_read = global_state.total_rows_read.load();
        double progress = (static_cast<double>(rows_read) / static_cast<double>(*global_state.rows_affected)) * 100.0;
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
    if (bind_data.HasParams()) {
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

// ============================================================================
// adbc_scan_table - Bind function for scanning an entire table
// ============================================================================

// Bind function for adbc_scan_table - similar to AdbcScanBind but takes table name instead of query
// Helper to build a fully qualified table name with proper quoting
static string BuildQualifiedTableName(const string &catalog, const string &schema, const string &table) {
    string result;
    if (!catalog.empty()) {
        result += "\"" + catalog + "\".";
    }
    if (!schema.empty()) {
        result += "\"" + schema + "\".";
    }
    result += "\"" + table + "\"";
    return result;
}

static unique_ptr<FunctionData> AdbcScanTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<AdbcScanBindData>();

    // Check for NULL connection handle first
    if (input.inputs[0].IsNull()) {
        throw InvalidInputException("adbc_scan_table: Connection handle cannot be NULL");
    }
    bind_data->connection_id = input.inputs[0].GetValue<int64_t>();

    // Check for NULL table name before connection validation
    if (input.inputs[1].IsNull()) {
        throw InvalidInputException("adbc_scan_table: Table name cannot be NULL");
    }
    bind_data->table_name = input.inputs[1].GetValue<string>();

    // Check for catalog named parameter
    auto catalog_it = input.named_parameters.find("catalog");
    if (catalog_it != input.named_parameters.end() && !catalog_it->second.IsNull()) {
        bind_data->catalog_name = catalog_it->second.GetValue<string>();
    }

    // Check for schema named parameter
    auto schema_it = input.named_parameters.find("schema");
    if (schema_it != input.named_parameters.end() && !schema_it->second.IsNull()) {
        bind_data->schema_name = schema_it->second.GetValue<string>();
    }

    // Construct a SELECT * FROM [catalog.][schema.]table_name query for schema discovery
    string qualified_name = BuildQualifiedTableName(bind_data->catalog_name, bind_data->schema_name, bind_data->table_name);
    bind_data->query = "SELECT * FROM " + qualified_name;

    // Extract batch_size parameter
    bind_data->batch_size = ExtractBatchSize(input, "adbc_scan_table");

    // Now validate and get connection wrapper
    bind_data->connection = GetValidatedConnection(bind_data->connection_id, "adbc_scan_table");

    // Create and prepare statement
    auto statement = make_shared_ptr<AdbcStatementWrapper>(bind_data->connection);
    statement->Init();
    statement->SetSqlQuery(bind_data->query);

    try {
        statement->Prepare();
    } catch (Exception &e) {
        throw InvalidInputException("adbc_scan_table: Failed to prepare statement for table '" + bind_data->table_name + "': " + string(e.what()));
    }

    // Get schema from statement (tries ExecuteSchema, falls back to execute)
    GetSchemaFromStatement(*statement, bind_data->query, bind_data->schema_root, "adbc_scan_table");

    // Populate return types and names from Arrow schema
    PopulateReturnTypesFromSchema(context, *bind_data, return_types, names);

    // Store all column names for projection pushdown
    bind_data->all_column_names = names;

    // Store return types for statistics callback
    bind_data->return_types = return_types;

    // Try to get statistics from ADBC (row count and column statistics)
    TryGetStatisticsFromADBC(*bind_data->connection, bind_data->catalog_name, bind_data->schema_name,
                              bind_data->table_name, *bind_data);

    return std::move(bind_data);
}

// Global init for adbc_scan_table - builds projected query based on column_ids and filters
static unique_ptr<GlobalTableFunctionState> AdbcScanTableInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<AdbcScanBindData>();
    auto global_state = make_uniq<AdbcScanGlobalState>();

    // Validate connection is still active
    if (!bind_data.connection->IsInitialized()) {
        throw InvalidInputException("adbc_scan_table: Connection has been closed");
    }

    // Build the query with projection pushdown
    // If we have column_ids and they're a subset of all columns, build a projected query
    string query;
    bool needs_projection = false;

    // Count how many valid column IDs we have (excluding special IDs like COLUMN_IDENTIFIER_ROW_ID)
    idx_t valid_column_count = 0;
    for (auto col_id : input.column_ids) {
        if (col_id < bind_data.all_column_names.size()) {
            valid_column_count++;
        }
    }

    if (!bind_data.table_name.empty() && valid_column_count > 0) {
        // Check if we need all columns or just a subset
        needs_projection = valid_column_count < bind_data.all_column_names.size();

        // Also check if columns are in order and consecutive from 0
        // If column_ids = [0, 1, 2, ...] matching all_column_names size, no projection needed
        if (!needs_projection) {
            idx_t expected_idx = 0;
            for (auto col_id : input.column_ids) {
                if (col_id < bind_data.all_column_names.size()) {
                    if (col_id != expected_idx) {
                        needs_projection = true;
                        break;
                    }
                    expected_idx++;
                }
            }
        }

        if (needs_projection) {
            // Build SELECT with only the needed columns
            query = "SELECT ";
            bool first = true;
            for (auto col_id : input.column_ids) {
                if (col_id < bind_data.all_column_names.size()) {
                    if (!first) {
                        query += ", ";
                    }
                    // Quote column name to handle special characters
                    query += "\"" + bind_data.all_column_names[col_id] + "\"";
                    first = false;
                }
            }
            // Use fully qualified table name
            string qualified_name = BuildQualifiedTableName(bind_data.catalog_name, bind_data.schema_name, bind_data.table_name);
            query += " FROM " + qualified_name;
        } else {
            // Use SELECT * as no projection needed
            query = bind_data.query;
        }
    } else {
        // No valid columns requested (e.g., count(*)) or empty column_ids - use SELECT *
        // We need to select something, so fall back to original query
        query = bind_data.query;
    }

    // Apply filter pushdown - transform DuckDB filters into SQL WHERE clause with parameter placeholders
    auto filter_result = AdbcFilterPushdown::TransformFilters(input.column_ids, input.filters, bind_data.all_column_names);
    if (filter_result.HasFilters()) {
        query += " WHERE " + filter_result.where_clause;
    }

    // Create fresh statement for this scan (allows multiple scans of same bind_data)
    global_state->statement = make_shared_ptr<AdbcStatementWrapper>(bind_data.connection);
    global_state->statement->Init();

    // Set batch size hint if provided (best-effort, driver-specific)
    if (bind_data.batch_size > 0) {
        string batch_size_str = to_string(bind_data.batch_size);
        try {
            global_state->statement->SetOption("adbc.statement.batch_size", batch_size_str);
        } catch (...) {
            // Option not supported by this driver, ignore
        }
    }

    global_state->statement->SetSqlQuery(query);
    global_state->statement->Prepare();

    // Bind filter parameters if we have any
    if (!filter_result.params.empty()) {
        try {
            BindParameters(context, *global_state->statement, filter_result.params, filter_result.param_types);
        } catch (Exception &e) {
            throw InvalidInputException("adbc_scan_table: Failed to bind filter parameters: " + string(e.what()) + " [Query: " + query + "]");
        }
    }

    // Execute the statement and capture row count if available
    memset(&global_state->stream, 0, sizeof(global_state->stream));
    int64_t rows_affected = -1;
    try {
        global_state->statement->ExecuteQuery(&global_state->stream, &rows_affected);
    } catch (Exception &e) {
        throw IOException("adbc_scan_table: Failed to execute query: " + string(e.what()) + " [Query: " + query + "]");
    }
    global_state->stream_initialized = true;

    // If we projected, we need to get the schema from the stream and populate projected_arrow_table
    if (needs_projection) {
        int ret = global_state->stream.get_schema(&global_state->stream, &global_state->projected_schema.arrow_schema);
        if (ret != 0) {
            const char *error_msg = global_state->stream.get_last_error(&global_state->stream);
            string msg = "adbc_scan_table: Failed to get schema from projected query";
            if (error_msg) {
                msg += ": ";
                msg += error_msg;
            }
            throw IOException(msg);
        }
        ArrowTableFunction::PopulateArrowTableSchema(DBConfig::GetConfig(context), global_state->projected_arrow_table,
                                                      global_state->projected_schema.arrow_schema);
        global_state->has_projected_schema = true;
    }

    // Store row count for progress reporting (if driver provided it)
    if (rows_affected >= 0) {
        global_state->rows_affected = rows_affected;
    }

    return std::move(global_state);
}

// Local init for adbc_scan_table - since we push down projection, arrow columns match output columns directly
static unique_ptr<LocalTableFunctionState> AdbcScanTableInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                                    GlobalTableFunctionState *global_state_p) {
    auto current_chunk = make_uniq<ArrowArrayWrapper>();
    auto local_state = make_uniq<AdbcScanLocalState>(std::move(current_chunk), context.client);

    // With projection pushdown, the Arrow stream returns exactly the columns we need
    // So column_ids should be 0, 1, 2, ... matching the projected query columns
    for (idx_t i = 0; i < input.column_ids.size(); i++) {
        local_state->column_ids.push_back(i);
    }

    return std::move(local_state);
}

// Scan function for adbc_scan_table - uses projected schema when available
static void AdbcScanTableFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
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

    // Convert Arrow data to DuckDB
    // If we have a projected schema, use that; otherwise use the bind_data schema
    if (output_size > 0) {
        if (global_state.has_projected_schema) {
            // With projection, arrow columns match output columns 1:1
            ArrowTableFunction::ArrowToDuckDB(local_state,
                                              global_state.projected_arrow_table.GetColumns(),
                                              output,
                                              true);  // arrow_scan_is_projected = true since columns match 1:1
        } else {
            // No projection, use original bind_data schema
            ArrowTableFunction::ArrowToDuckDB(local_state,
                                              bind_data.arrow_table.GetColumns(),
                                              output,
                                              false);
        }
    }

    local_state.chunk_offset += output.size();
    output.Verify();
}

// ToString callback for adbc_scan_table EXPLAIN output
static InsertionOrderPreservingMap<string> AdbcScanTableToString(TableFunctionToStringInput &input) {
    InsertionOrderPreservingMap<string> result;
    auto &bind_data = input.bind_data->Cast<AdbcScanBindData>();

    // Show table name
    result["Table"] = bind_data.table_name;

    // Show batch size if set
    if (bind_data.batch_size > 0) {
        result["BatchSize"] = to_string(bind_data.batch_size);
    }

    // Show connection ID for debugging
    result["Connection"] = to_string(bind_data.connection_id);

    return result;
}

// Cardinality estimation callback for adbc_scan_table
// Uses ADBC statistics when available
static unique_ptr<NodeStatistics> AdbcScanTableCardinality(ClientContext &context, const FunctionData *bind_data_p) {
    auto &bind_data = bind_data_p->Cast<AdbcScanBindData>();

    if (bind_data.estimated_row_count.has_value()) {
        auto result = make_uniq<NodeStatistics>();
        result->has_estimated_cardinality = true;
        result->estimated_cardinality = *bind_data.estimated_row_count;
        return result;
    }

    // No cardinality information available
    return make_uniq<NodeStatistics>();
}

// Progress reporting callback for adbc_scan_table
// Uses estimated row count from ADBC statistics when available
static double AdbcScanTableProgress(ClientContext &context, const FunctionData *bind_data_p,
                                    const GlobalTableFunctionState *global_state_p) {
    auto &bind_data = bind_data_p->Cast<AdbcScanBindData>();
    auto &global_state = global_state_p->Cast<AdbcScanGlobalState>();

    if (global_state.done) {
        return 100.0;
    }

    idx_t rows_read = global_state.total_rows_read.load();

    // First priority: use rows_affected from ExecuteQuery if driver provided it
    if (global_state.rows_affected.has_value() && *global_state.rows_affected > 0) {
        double progress = (static_cast<double>(rows_read) / static_cast<double>(*global_state.rows_affected)) * 100.0;
        return MinValue(progress, 99.9); // Cap at 99.9% until done
    }

    // Second priority: use estimated row count from ADBC statistics (fetched at bind time)
    if (bind_data.estimated_row_count.has_value() && *bind_data.estimated_row_count > 0) {
        double progress = (static_cast<double>(rows_read) / static_cast<double>(*bind_data.estimated_row_count)) * 100.0;
        return MinValue(progress, 99.9); // Cap at 99.9% until done
    }

    // No estimate available - return -1 to indicate unknown progress
    return -1.0;
}

// Column statistics callback for adbc_scan_table
// Returns BaseStatistics for a specific column using cached ADBC statistics
static unique_ptr<BaseStatistics> AdbcScanTableStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                                           column_t column_id) {
    auto &bind_data = bind_data_p->Cast<AdbcScanBindData>();

    // Validate column_id is in range
    if (column_id >= bind_data.all_column_names.size()) {
        return nullptr;
    }

    // Get the column name and type
    const string &col_name = bind_data.all_column_names[column_id];
    const LogicalType &col_type = bind_data.return_types[column_id];

    // Look up column statistics
    auto it = bind_data.column_statistics.find(col_name);
    if (it == bind_data.column_statistics.end()) {
        // No statistics available for this column
        return nullptr;
    }

    const AdbcColumnStatistics &col_stats = it->second;

    // Create BaseStatistics based on what information we have
    auto result = BaseStatistics::CreateUnknown(col_type);

    // Set distinct count if available
    if (col_stats.distinct_count.has_value()) {
        result.SetDistinctCount(*col_stats.distinct_count);
    }

    // Set null information based on null_count and row_count
    if (col_stats.null_count.has_value() && bind_data.estimated_row_count.has_value()) {
        if (*col_stats.null_count == 0) {
            // No nulls in this column
            result.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
        } else if (*col_stats.null_count == *bind_data.estimated_row_count) {
            // All values are null
            result.Set(StatsInfo::CANNOT_HAVE_VALID_VALUES);
        } else {
            // Some nulls, some valid
            result.Set(StatsInfo::CAN_HAVE_NULL_AND_VALID_VALUES);
        }
    }

    // Set min/max for numeric types (integers, floats, timestamps, dates, times, decimals, booleans)
    if (col_stats.min_value.HasValue() || col_stats.max_value.HasValue()) {
        if (result.GetStatsType() == StatisticsType::NUMERIC_STATS) {
            try {
                // Convert AdbcStatValue to DuckDB Value based on target column type
                auto to_duckdb_value = [&col_type](const AdbcStatValue &sv) -> Value {
                    if (!sv.HasValue()) return Value();

                    auto target_type = col_type.id();
                    auto type_index = sv.TypeIndex();

                    if (type_index == 0) {  // int64 source - handle temporal types directly
                        auto val = std::get<int64_t>(*sv.value);
                        switch (target_type) {
                        case LogicalTypeId::TIMESTAMP:
                        case LogicalTypeId::TIMESTAMP_TZ:
                            return Value::TIMESTAMP(timestamp_t(val));
                        case LogicalTypeId::TIMESTAMP_SEC:
                            return Value::TIMESTAMPSEC(timestamp_sec_t(val));
                        case LogicalTypeId::TIMESTAMP_MS:
                            return Value::TIMESTAMPMS(timestamp_ms_t(val));
                        case LogicalTypeId::TIMESTAMP_NS:
                            return Value::TIMESTAMPNS(timestamp_ns_t(val));
                        case LogicalTypeId::DATE:
                            return Value::DATE(date_t(static_cast<int32_t>(val)));
                        case LogicalTypeId::TIME:
                        case LogicalTypeId::TIME_TZ:
                            return Value::TIME(dtime_t(val));
                        default:
                            return Value::BIGINT(val);
                        }
                    } else if (type_index == 1) {  // uint64 source
                        return Value::UBIGINT(std::get<uint64_t>(*sv.value));
                    } else if (type_index == 2) {  // float64 source
                        auto val = std::get<double>(*sv.value);
                        return (target_type == LogicalTypeId::FLOAT)
                            ? Value::FLOAT(static_cast<float>(val))
                            : Value::DOUBLE(val);
                    }
                    return Value();
                };

                auto set_stat = [&col_type, &result](Value val, bool is_min) {
                    if (val.IsNull()) return;
                    if (val.type().id() != col_type.id()) {
                        val = val.DefaultCastAs(col_type);
                    }
                    if (is_min) {
                        NumericStats::SetMin(result, val);
                    } else {
                        NumericStats::SetMax(result, val);
                    }
                };

                set_stat(to_duckdb_value(col_stats.min_value), true);
                set_stat(to_duckdb_value(col_stats.max_value), false);
            } catch (...) {
                // Conversion failed - ignore min/max stats
            }
        }
    }

    return result.ToUnique();
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

    // Enable projection pushdown - DuckDB will request only the columns it needs
    // The ADBC driver still returns all columns, but ArrowToDuckDB will extract only the needed ones
    adbc_scan_function.projection_pushdown = true;

    // Add progress, cardinality, and to_string callbacks
    adbc_scan_function.table_scan_progress = AdbcScanProgress;
    adbc_scan_function.cardinality = AdbcScanCardinality;
    adbc_scan_function.to_string = AdbcScanToString;

    CreateTableFunctionInfo info(adbc_scan_function);
    FunctionDescription desc;
    desc.description = "Execute a SELECT query on an ADBC connection and return the results as a table";
    desc.parameter_names = {"connection_handle", "query", "params", "batch_size"};
    desc.parameter_types = {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::ANY, LogicalType::BIGINT};
    desc.examples = {"SELECT * FROM adbc_scan(conn, 'SELECT * FROM users')",
                     "SELECT * FROM adbc_scan(conn, 'SELECT * FROM users WHERE id = ?', params := row(42))",
                     "SELECT * FROM adbc_scan(conn, 'SELECT * FROM large_table', batch_size := 65536)"};
    desc.categories = {"adbc"};
    info.descriptions.push_back(std::move(desc));
    loader.RegisterFunction(info);

    // ========================================================================
    // adbc_scan_table - Scan an entire table from an ADBC connection
    // ========================================================================

    TableFunction adbc_scan_table_function("adbc_scan_table", {LogicalType::BIGINT, LogicalType::VARCHAR},
                                            AdbcScanTableFunction, AdbcScanTableBind, AdbcScanTableInitGlobal, AdbcScanTableInitLocal);

    // Add named parameters for catalog, schema, and batch size
    adbc_scan_table_function.named_parameters["catalog"] = LogicalType::VARCHAR;
    adbc_scan_table_function.named_parameters["schema"] = LogicalType::VARCHAR;
    adbc_scan_table_function.named_parameters["batch_size"] = LogicalType::BIGINT;

    // Enable projection pushdown and filter pushdown
    adbc_scan_table_function.projection_pushdown = true;
    adbc_scan_table_function.filter_pushdown = true;

    // Add progress, cardinality, statistics, and to_string callbacks
    adbc_scan_table_function.table_scan_progress = AdbcScanTableProgress;
    adbc_scan_table_function.cardinality = AdbcScanTableCardinality;
    adbc_scan_table_function.statistics = AdbcScanTableStatistics;
    adbc_scan_table_function.to_string = AdbcScanTableToString;

    CreateTableFunctionInfo scan_table_info(adbc_scan_table_function);
    FunctionDescription scan_table_desc;
    scan_table_desc.description = "Scan an entire table from an ADBC connection";
    scan_table_desc.parameter_names = {"connection_handle", "table_name", "catalog", "schema", "batch_size"};
    scan_table_desc.parameter_types = {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT};
    scan_table_desc.examples = {"SELECT * FROM adbc_scan_table(conn, 'users')",
                                "SELECT * FROM adbc_scan_table(conn, 'users', schema := 'public')",
                                "SELECT * FROM adbc_scan_table(conn, 'large_table', batch_size := 65536)"};
    scan_table_desc.categories = {"adbc"};
    scan_table_info.descriptions.push_back(std::move(scan_table_desc));
    loader.RegisterFunction(scan_table_info);
}

} // namespace adbc_scanner
