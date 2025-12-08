#include "adbc_connection.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"

namespace duckdb {
namespace adbc {

// Info code to name mapping
static string GetInfoName(uint32_t info_code) {
    switch (info_code) {
        case 0: return "vendor_name";
        case 1: return "vendor_version";
        case 2: return "vendor_arrow_version";
        case 100: return "driver_name";
        case 101: return "driver_version";
        case 102: return "driver_arrow_version";
        case 103: return "driver_adbc_version";
        default: return "info_" + to_string(info_code);
    }
}

//===--------------------------------------------------------------------===//
// adbc_info - Get driver/database information
//===--------------------------------------------------------------------===//

struct AdbcInfoBindData : public TableFunctionData {
    int64_t connection_id;
    shared_ptr<AdbcConnectionWrapper> connection;
};

struct AdbcInfoGlobalState : public GlobalTableFunctionState {
    ArrowArrayStream stream;
    bool stream_initialized = false;
    bool done = false;
    mutex main_mutex;

    // Extracted info rows
    vector<pair<string, string>> info_rows;
    idx_t current_row = 0;

    ~AdbcInfoGlobalState() {
        if (stream_initialized && stream.release) {
            stream.release(&stream);
        }
    }

    idx_t MaxThreads() const override {
        return 1;
    }
};

static unique_ptr<FunctionData> AdbcInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<AdbcInfoBindData>();

    bind_data->connection_id = input.inputs[0].GetValue<int64_t>();

    auto &registry = ConnectionRegistry::Get();
    bind_data->connection = registry.Get(bind_data->connection_id);
    if (!bind_data->connection) {
        throw InvalidInputException("adbc_info: Invalid connection handle: " + to_string(bind_data->connection_id));
    }

    if (!bind_data->connection->IsInitialized()) {
        throw InvalidInputException("adbc_info: Connection has been closed");
    }

    // Return simple key-value schema
    names = {"info_name", "info_value"};
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};

    return std::move(bind_data);
}

// Helper to extract string from Arrow union value
static string ExtractUnionValue(ArrowArray *union_array, int64_t row_idx) {
    // Dense union: types buffer contains type code, offsets buffer contains offset into child
    auto types_buffer = static_cast<const int8_t *>(union_array->buffers[0]);
    auto offsets_buffer = static_cast<const int32_t *>(union_array->buffers[1]);

    int8_t type_code = types_buffer[row_idx];
    int32_t offset = offsets_buffer[row_idx];

    // Get the child array for this type code
    ArrowArray *child = union_array->children[type_code];

    switch (type_code) {
        case 0: { // string_value (utf8)
            auto offsets = static_cast<const int32_t *>(child->buffers[1]);
            auto data = static_cast<const char *>(child->buffers[2]);
            int32_t start = offsets[offset];
            int32_t end = offsets[offset + 1];
            return string(data + start, end - start);
        }
        case 1: { // bool_value
            auto data = static_cast<const uint8_t *>(child->buffers[1]);
            bool value = (data[offset / 8] >> (offset % 8)) & 1;
            return value ? "true" : "false";
        }
        case 2: { // int64_value
            auto data = static_cast<const int64_t *>(child->buffers[1]);
            return to_string(data[offset]);
        }
        case 3: { // int32_bitmask
            auto data = static_cast<const int32_t *>(child->buffers[1]);
            return to_string(data[offset]);
        }
        case 4: { // string_list - just return placeholder for now
            return "[string_list]";
        }
        case 5: { // int32_to_int32_list_map - just return placeholder for now
            return "[map]";
        }
        default:
            return "[unknown]";
    }
}

static unique_ptr<GlobalTableFunctionState> AdbcInfoInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<AdbcInfoBindData>();
    auto global_state = make_uniq<AdbcInfoGlobalState>();

    if (!bind_data.connection->IsInitialized()) {
        throw InvalidInputException("adbc_info: Connection has been closed");
    }

    memset(&global_state->stream, 0, sizeof(global_state->stream));
    try {
        bind_data.connection->GetInfo(nullptr, 0, &global_state->stream);
    } catch (Exception &e) {
        throw IOException("adbc_info: Failed to get info: " + string(e.what()));
    }
    global_state->stream_initialized = true;

    // Pre-extract all info into simple key-value pairs
    // This avoids the union type issue by converting to strings
    ArrowArray batch;
    while (true) {
        memset(&batch, 0, sizeof(batch));
        int ret = global_state->stream.get_next(&global_state->stream, &batch);
        if (ret != 0) {
            const char *error_msg = global_state->stream.get_last_error(&global_state->stream);
            string msg = "adbc_info: Failed to get next batch";
            if (error_msg) {
                msg += ": ";
                msg += error_msg;
            }
            throw IOException(msg);
        }

        if (!batch.release) {
            break; // End of stream
        }

        // batch has 2 children: info_name (uint32) and info_value (union)
        if (batch.n_children >= 2) {
            ArrowArray *info_name_array = batch.children[0];
            ArrowArray *info_value_array = batch.children[1];

            auto info_codes = static_cast<const uint32_t *>(info_name_array->buffers[1]);

            for (int64_t i = 0; i < batch.length; i++) {
                uint32_t info_code = info_codes[i];
                string name = GetInfoName(info_code);
                string value = ExtractUnionValue(info_value_array, i);
                global_state->info_rows.emplace_back(name, value);
            }
        }

        if (batch.release) {
            batch.release(&batch);
        }
    }

    return std::move(global_state);
}

static unique_ptr<LocalTableFunctionState> AdbcInfoInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *global_state_p) {
    return nullptr;
}

static void AdbcInfoFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &global_state = data.global_state->Cast<AdbcInfoGlobalState>();

    if (global_state.current_row >= global_state.info_rows.size()) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    auto &name_vector = output.data[0];
    auto &value_vector = output.data[1];

    while (global_state.current_row < global_state.info_rows.size() && count < STANDARD_VECTOR_SIZE) {
        auto &row = global_state.info_rows[global_state.current_row];
        name_vector.SetValue(count, Value(row.first));
        value_vector.SetValue(count, Value(row.second));
        count++;
        global_state.current_row++;
    }

    output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// adbc_tables - Get tables from the database
//===--------------------------------------------------------------------===//

// Structure to hold a flattened table row
struct TableRow {
    string catalog_name;
    string schema_name;
    string table_name;
    string table_type;
};

struct AdbcTablesBindData : public TableFunctionData {
    int64_t connection_id;
    shared_ptr<AdbcConnectionWrapper> connection;
    // Filter parameters
    string catalog_filter;
    string schema_filter;
    string table_filter;
    bool has_catalog_filter = false;
    bool has_schema_filter = false;
    bool has_table_filter = false;
};

struct AdbcTablesGlobalState : public GlobalTableFunctionState {
    ArrowArrayStream stream;
    bool stream_initialized = false;

    // Flattened table rows
    vector<TableRow> table_rows;
    idx_t current_row = 0;

    ~AdbcTablesGlobalState() {
        if (stream_initialized && stream.release) {
            stream.release(&stream);
        }
    }

    idx_t MaxThreads() const override {
        return 1;
    }
};

// Helper to extract a string from an Arrow utf8 array
static string ExtractString(ArrowArray *array, int64_t idx) {
    if (!array || !array->buffers[2]) {
        return "";
    }

    // Check validity
    if (array->buffers[0]) {
        auto validity = static_cast<const uint8_t *>(array->buffers[0]);
        if (!((validity[idx / 8] >> (idx % 8)) & 1)) {
            return "";  // NULL value
        }
    }

    auto offsets = static_cast<const int32_t *>(array->buffers[1]);
    auto data = static_cast<const char *>(array->buffers[2]);
    int32_t start = offsets[idx];
    int32_t end = offsets[idx + 1];
    return string(data + start, end - start);
}

// Helper to extract tables from the hierarchical GetObjects result
static void ExtractTables(ArrowArray *batch, vector<TableRow> &table_rows) {
    if (!batch || batch->n_children < 2) {
        return;
    }

    // GetObjects schema:
    // catalog_name (utf8)
    // catalog_db_schemas (list<struct{db_schema_name, db_schema_tables}>)

    ArrowArray *catalog_names = batch->children[0];
    ArrowArray *catalog_db_schemas = batch->children[1];

    if (!catalog_db_schemas || catalog_db_schemas->n_children < 1) {
        return;
    }

    // The list child is the struct array
    ArrowArray *schemas_struct = catalog_db_schemas->children[0];
    auto list_offsets = static_cast<const int32_t *>(catalog_db_schemas->buffers[1]);

    for (int64_t cat_idx = 0; cat_idx < batch->length; cat_idx++) {
        string catalog_name = ExtractString(catalog_names, cat_idx);

        int32_t schema_start = list_offsets[cat_idx];
        int32_t schema_end = list_offsets[cat_idx + 1];

        if (!schemas_struct || schemas_struct->n_children < 2) {
            continue;
        }

        ArrowArray *schema_names = schemas_struct->children[0];
        ArrowArray *schema_tables = schemas_struct->children[1];

        if (!schema_tables || schema_tables->n_children < 1) {
            continue;
        }

        ArrowArray *tables_struct = schema_tables->children[0];
        auto tables_list_offsets = static_cast<const int32_t *>(schema_tables->buffers[1]);

        for (int32_t schema_idx = schema_start; schema_idx < schema_end; schema_idx++) {
            string schema_name = ExtractString(schema_names, schema_idx);

            int32_t table_start = tables_list_offsets[schema_idx];
            int32_t table_end = tables_list_offsets[schema_idx + 1];

            if (!tables_struct || tables_struct->n_children < 2) {
                continue;
            }

            ArrowArray *table_names = tables_struct->children[0];
            ArrowArray *table_types = tables_struct->children[1];

            for (int32_t table_idx = table_start; table_idx < table_end; table_idx++) {
                TableRow row;
                row.catalog_name = catalog_name;
                row.schema_name = schema_name;
                row.table_name = ExtractString(table_names, table_idx);
                row.table_type = ExtractString(table_types, table_idx);
                table_rows.push_back(row);
            }
        }
    }
}

static unique_ptr<FunctionData> AdbcTablesBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<AdbcTablesBindData>();

    bind_data->connection_id = input.inputs[0].GetValue<int64_t>();

    // Check for optional filter parameters
    auto catalog_it = input.named_parameters.find("catalog");
    if (catalog_it != input.named_parameters.end() && !catalog_it->second.IsNull()) {
        bind_data->catalog_filter = catalog_it->second.GetValue<string>();
        bind_data->has_catalog_filter = true;
    }

    auto schema_it = input.named_parameters.find("schema");
    if (schema_it != input.named_parameters.end() && !schema_it->second.IsNull()) {
        bind_data->schema_filter = schema_it->second.GetValue<string>();
        bind_data->has_schema_filter = true;
    }

    auto table_it = input.named_parameters.find("table_name");
    if (table_it != input.named_parameters.end() && !table_it->second.IsNull()) {
        bind_data->table_filter = table_it->second.GetValue<string>();
        bind_data->has_table_filter = true;
    }

    auto &registry = ConnectionRegistry::Get();
    bind_data->connection = registry.Get(bind_data->connection_id);
    if (!bind_data->connection) {
        throw InvalidInputException("adbc_tables: Invalid connection handle: " + to_string(bind_data->connection_id));
    }

    if (!bind_data->connection->IsInitialized()) {
        throw InvalidInputException("adbc_tables: Connection has been closed");
    }

    // Return a simple schema for tables: catalog, schema, table_name, table_type
    names = {"catalog_name", "schema_name", "table_name", "table_type"};
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};

    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> AdbcTablesInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<AdbcTablesBindData>();
    auto global_state = make_uniq<AdbcTablesGlobalState>();

    if (!bind_data.connection->IsInitialized()) {
        throw InvalidInputException("adbc_tables: Connection has been closed");
    }

    memset(&global_state->stream, 0, sizeof(global_state->stream));

    const char *catalog = bind_data.has_catalog_filter ? bind_data.catalog_filter.c_str() : nullptr;
    const char *schema = bind_data.has_schema_filter ? bind_data.schema_filter.c_str() : nullptr;
    const char *table_name = bind_data.has_table_filter ? bind_data.table_filter.c_str() : nullptr;

    try {
        // depth=3 means catalogs, schemas, and tables (but not columns)
        bind_data.connection->GetObjects(3, catalog, schema, table_name, nullptr, nullptr, &global_state->stream);
    } catch (Exception &e) {
        throw IOException("adbc_tables: Failed to get tables: " + string(e.what()));
    }
    global_state->stream_initialized = true;

    // Pre-extract all tables by flattening the hierarchical structure
    ArrowArray batch;
    while (true) {
        memset(&batch, 0, sizeof(batch));
        int ret = global_state->stream.get_next(&global_state->stream, &batch);
        if (ret != 0) {
            const char *error_msg = global_state->stream.get_last_error(&global_state->stream);
            string msg = "adbc_tables: Failed to get next batch";
            if (error_msg) {
                msg += ": ";
                msg += error_msg;
            }
            throw IOException(msg);
        }

        if (!batch.release) {
            break; // End of stream
        }

        ExtractTables(&batch, global_state->table_rows);

        if (batch.release) {
            batch.release(&batch);
        }
    }

    return std::move(global_state);
}

static unique_ptr<LocalTableFunctionState> AdbcTablesInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                                GlobalTableFunctionState *global_state_p) {
    return nullptr;
}

static void AdbcTablesFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &global_state = data.global_state->Cast<AdbcTablesGlobalState>();

    if (global_state.current_row >= global_state.table_rows.size()) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    auto &catalog_vector = output.data[0];
    auto &schema_vector = output.data[1];
    auto &name_vector = output.data[2];
    auto &type_vector = output.data[3];

    while (global_state.current_row < global_state.table_rows.size() && count < STANDARD_VECTOR_SIZE) {
        auto &row = global_state.table_rows[global_state.current_row];
        catalog_vector.SetValue(count, row.catalog_name.empty() ? Value() : Value(row.catalog_name));
        schema_vector.SetValue(count, row.schema_name.empty() ? Value() : Value(row.schema_name));
        name_vector.SetValue(count, Value(row.table_name));
        type_vector.SetValue(count, Value(row.table_type));
        count++;
        global_state.current_row++;
    }

    output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// Register all catalog functions
//===--------------------------------------------------------------------===//

void RegisterAdbcCatalogFunctions(DatabaseInstance &db) {
    ExtensionLoader loader(db, "adbc");

    // adbc_info(connection_id) - Get driver/database information
    TableFunction adbc_info_function("adbc_info", {LogicalType::BIGINT}, AdbcInfoFunction,
                                      AdbcInfoBind, AdbcInfoInitGlobal, AdbcInfoInitLocal);
    adbc_info_function.projection_pushdown = false;
    loader.RegisterFunction(adbc_info_function);

    // adbc_tables(connection_id, catalog, schema, table_name) - Get tables
    TableFunction adbc_tables_function("adbc_tables", {LogicalType::BIGINT}, AdbcTablesFunction,
                                        AdbcTablesBind, AdbcTablesInitGlobal, AdbcTablesInitLocal);
    adbc_tables_function.named_parameters["catalog"] = LogicalType::VARCHAR;
    adbc_tables_function.named_parameters["schema"] = LogicalType::VARCHAR;
    adbc_tables_function.named_parameters["table_name"] = LogicalType::VARCHAR;
    adbc_tables_function.projection_pushdown = false;
    loader.RegisterFunction(adbc_tables_function);
}

} // namespace adbc
} // namespace duckdb
