#include "adbc_connection.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include <nanoarrow/nanoarrow.h>

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

    // Check for NULL connection handle
    if (input.inputs[0].IsNull()) {
        throw InvalidInputException("adbc_info: Connection handle cannot be NULL");
    }

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

    // Check for NULL connection handle
    if (input.inputs[0].IsNull()) {
        throw InvalidInputException("adbc_tables: Connection handle cannot be NULL");
    }

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
// adbc_table_types - Get supported table types
//===--------------------------------------------------------------------===//

struct AdbcTableTypesBindData : public TableFunctionData {
    int64_t connection_id;
    shared_ptr<AdbcConnectionWrapper> connection;
};

struct AdbcTableTypesGlobalState : public GlobalTableFunctionState {
    ArrowArrayStream stream;
    bool stream_initialized = false;

    // Extracted table types
    vector<string> table_types;
    idx_t current_row = 0;

    ~AdbcTableTypesGlobalState() {
        if (stream_initialized && stream.release) {
            stream.release(&stream);
        }
    }

    idx_t MaxThreads() const override {
        return 1;
    }
};

static unique_ptr<FunctionData> AdbcTableTypesBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<AdbcTableTypesBindData>();

    // Check for NULL connection handle
    if (input.inputs[0].IsNull()) {
        throw InvalidInputException("adbc_table_types: Connection handle cannot be NULL");
    }

    bind_data->connection_id = input.inputs[0].GetValue<int64_t>();

    auto &registry = ConnectionRegistry::Get();
    bind_data->connection = registry.Get(bind_data->connection_id);
    if (!bind_data->connection) {
        throw InvalidInputException("adbc_table_types: Invalid connection handle: " + to_string(bind_data->connection_id));
    }

    if (!bind_data->connection->IsInitialized()) {
        throw InvalidInputException("adbc_table_types: Connection has been closed");
    }

    // Return single column schema
    names = {"table_type"};
    return_types = {LogicalType::VARCHAR};

    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> AdbcTableTypesInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<AdbcTableTypesBindData>();
    auto global_state = make_uniq<AdbcTableTypesGlobalState>();

    if (!bind_data.connection->IsInitialized()) {
        throw InvalidInputException("adbc_table_types: Connection has been closed");
    }

    memset(&global_state->stream, 0, sizeof(global_state->stream));
    try {
        bind_data.connection->GetTableTypes(&global_state->stream);
    } catch (Exception &e) {
        throw IOException("adbc_table_types: Failed to get table types: " + string(e.what()));
    }
    global_state->stream_initialized = true;

    // Extract all table types from the Arrow stream
    ArrowArray batch;
    while (true) {
        memset(&batch, 0, sizeof(batch));
        int ret = global_state->stream.get_next(&global_state->stream, &batch);
        if (ret != 0) {
            const char *error_msg = global_state->stream.get_last_error(&global_state->stream);
            string msg = "adbc_table_types: Failed to get next batch";
            if (error_msg) {
                msg += ": ";
                msg += error_msg;
            }
            throw IOException(msg);
        }

        if (!batch.release) {
            break; // End of stream
        }

        // The result has a single column: table_type (utf8)
        if (batch.n_children >= 1) {
            ArrowArray *table_type_array = batch.children[0];
            for (int64_t i = 0; i < batch.length; i++) {
                string table_type = ExtractString(table_type_array, i);
                global_state->table_types.push_back(table_type);
            }
        }

        if (batch.release) {
            batch.release(&batch);
        }
    }

    return std::move(global_state);
}

static unique_ptr<LocalTableFunctionState> AdbcTableTypesInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                                    GlobalTableFunctionState *global_state_p) {
    return nullptr;
}

static void AdbcTableTypesFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &global_state = data.global_state->Cast<AdbcTableTypesGlobalState>();

    if (global_state.current_row >= global_state.table_types.size()) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    auto &type_vector = output.data[0];

    while (global_state.current_row < global_state.table_types.size() && count < STANDARD_VECTOR_SIZE) {
        auto &table_type = global_state.table_types[global_state.current_row];
        type_vector.SetValue(count, Value(table_type));
        count++;
        global_state.current_row++;
    }

    output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// adbc_columns - Get column metadata for tables
//===--------------------------------------------------------------------===//

// Structure to hold a flattened column row
struct ColumnRow {
    string catalog_name;
    string schema_name;
    string table_name;
    string column_name;
    int32_t ordinal_position;
    string remarks;
    string type_name;
    bool is_nullable;
};

struct AdbcColumnsBindData : public TableFunctionData {
    int64_t connection_id;
    shared_ptr<AdbcConnectionWrapper> connection;
    // Filter parameters
    string catalog_filter;
    string schema_filter;
    string table_filter;
    string column_filter;
    bool has_catalog_filter = false;
    bool has_schema_filter = false;
    bool has_table_filter = false;
    bool has_column_filter = false;
};

struct AdbcColumnsGlobalState : public GlobalTableFunctionState {
    ArrowArrayStream stream;
    bool stream_initialized = false;

    // Flattened column rows
    vector<ColumnRow> column_rows;
    idx_t current_row = 0;

    ~AdbcColumnsGlobalState() {
        if (stream_initialized && stream.release) {
            stream.release(&stream);
        }
    }

    idx_t MaxThreads() const override {
        return 1;
    }
};

// Helper to extract an int16 from an Arrow array
static int16_t ExtractInt16(ArrowArray *array, int64_t idx) {
    if (!array || !array->buffers[1]) {
        return 0;
    }
    // Check validity
    if (array->buffers[0]) {
        auto validity = static_cast<const uint8_t *>(array->buffers[0]);
        if (!((validity[idx / 8] >> (idx % 8)) & 1)) {
            return 0;  // NULL value
        }
    }
    auto data = static_cast<const int16_t *>(array->buffers[1]);
    return data[idx];
}

// Helper to extract an int32 from an Arrow array
static int32_t ExtractInt32(ArrowArray *array, int64_t idx) {
    if (!array || !array->buffers[1]) {
        return 0;
    }
    // Check validity
    if (array->buffers[0]) {
        auto validity = static_cast<const uint8_t *>(array->buffers[0]);
        if (!((validity[idx / 8] >> (idx % 8)) & 1)) {
            return 0;  // NULL value
        }
    }
    auto data = static_cast<const int32_t *>(array->buffers[1]);
    return data[idx];
}

// Helper to check if a value is null in an Arrow array
static bool IsNull(ArrowArray *array, int64_t idx) {
    if (!array) {
        return true;
    }
    if (!array->buffers[0]) {
        return false;  // No validity bitmap means all values are valid
    }
    auto validity = static_cast<const uint8_t *>(array->buffers[0]);
    return !((validity[idx / 8] >> (idx % 8)) & 1);
}

// Helper to extract columns from the hierarchical GetObjects result (depth=ALL)
static void ExtractColumns(ArrowArray *batch, vector<ColumnRow> &column_rows) {
    if (!batch || batch->n_children < 2) {
        return;
    }

    // GetObjects schema (depth=ALL):
    // catalog_name (utf8)
    // catalog_db_schemas (list<struct{db_schema_name, db_schema_tables}>)

    ArrowArray *catalog_names = batch->children[0];
    ArrowArray *catalog_db_schemas = batch->children[1];

    if (!catalog_db_schemas || catalog_db_schemas->n_children < 1) {
        return;
    }

    ArrowArray *schemas_struct = catalog_db_schemas->children[0];
    auto catalog_list_offsets = static_cast<const int32_t *>(catalog_db_schemas->buffers[1]);

    for (int64_t cat_idx = 0; cat_idx < batch->length; cat_idx++) {
        string catalog_name = ExtractString(catalog_names, cat_idx);

        int32_t schema_start = catalog_list_offsets[cat_idx];
        int32_t schema_end = catalog_list_offsets[cat_idx + 1];

        if (!schemas_struct || schemas_struct->n_children < 2) {
            continue;
        }

        ArrowArray *schema_names = schemas_struct->children[0];
        ArrowArray *schema_tables = schemas_struct->children[1];

        if (!schema_tables || schema_tables->n_children < 1) {
            continue;
        }

        ArrowArray *tables_struct = schema_tables->children[0];
        auto schema_list_offsets = static_cast<const int32_t *>(schema_tables->buffers[1]);

        for (int32_t schema_idx = schema_start; schema_idx < schema_end; schema_idx++) {
            string schema_name = ExtractString(schema_names, schema_idx);

            int32_t table_start = schema_list_offsets[schema_idx];
            int32_t table_end = schema_list_offsets[schema_idx + 1];

            if (!tables_struct || tables_struct->n_children < 3) {
                // Need at least: table_name, table_type, table_columns
                continue;
            }

            ArrowArray *table_names = tables_struct->children[0];
            // tables_struct->children[1] is table_type (skip)
            ArrowArray *table_columns = tables_struct->children[2];  // list<COLUMN_SCHEMA>

            if (!table_columns || table_columns->n_children < 1) {
                continue;
            }

            ArrowArray *columns_struct = table_columns->children[0];
            auto table_list_offsets = static_cast<const int32_t *>(table_columns->buffers[1]);

            for (int32_t table_idx = table_start; table_idx < table_end; table_idx++) {
                string table_name = ExtractString(table_names, table_idx);

                int32_t column_start = table_list_offsets[table_idx];
                int32_t column_end = table_list_offsets[table_idx + 1];

                if (!columns_struct || columns_struct->n_children < 1) {
                    continue;
                }

                // COLUMN_SCHEMA fields:
                // 0: column_name (utf8 not null)
                // 1: ordinal_position (int32)
                // 2: remarks (utf8)
                // 3: xdbc_data_type (int16)
                // 4: xdbc_type_name (utf8)
                // ... more xdbc fields
                // 7: xdbc_nullable (int16)
                // 13: xdbc_is_nullable (utf8)

                ArrowArray *column_names = columns_struct->children[0];
                ArrowArray *ordinal_positions = columns_struct->n_children > 1 ? columns_struct->children[1] : nullptr;
                ArrowArray *remarks_array = columns_struct->n_children > 2 ? columns_struct->children[2] : nullptr;
                ArrowArray *type_names = columns_struct->n_children > 4 ? columns_struct->children[4] : nullptr;
                ArrowArray *nullable_array = columns_struct->n_children > 7 ? columns_struct->children[7] : nullptr;
                ArrowArray *is_nullable_str = columns_struct->n_children > 13 ? columns_struct->children[13] : nullptr;

                for (int32_t col_idx = column_start; col_idx < column_end; col_idx++) {
                    ColumnRow row;
                    row.catalog_name = catalog_name;
                    row.schema_name = schema_name;
                    row.table_name = table_name;
                    row.column_name = ExtractString(column_names, col_idx);
                    row.ordinal_position = ordinal_positions ? ExtractInt32(ordinal_positions, col_idx) : 0;
                    row.remarks = remarks_array ? ExtractString(remarks_array, col_idx) : "";
                    row.type_name = type_names ? ExtractString(type_names, col_idx) : "";

                    // Determine nullability - try xdbc_is_nullable string first, then xdbc_nullable int16
                    if (is_nullable_str && !IsNull(is_nullable_str, col_idx)) {
                        string nullable_str = ExtractString(is_nullable_str, col_idx);
                        row.is_nullable = (nullable_str == "YES");
                    } else if (nullable_array && !IsNull(nullable_array, col_idx)) {
                        // xdbc_nullable: 0 = not nullable, 1 = nullable, 2 = unknown
                        int16_t nullable_val = ExtractInt16(nullable_array, col_idx);
                        row.is_nullable = (nullable_val == 1);
                    } else {
                        row.is_nullable = true;  // Default to nullable if unknown
                    }

                    column_rows.push_back(row);
                }
            }
        }
    }
}

static unique_ptr<FunctionData> AdbcColumnsBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<AdbcColumnsBindData>();

    // Check for NULL connection handle
    if (input.inputs[0].IsNull()) {
        throw InvalidInputException("adbc_columns: Connection handle cannot be NULL");
    }

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

    auto column_it = input.named_parameters.find("column_name");
    if (column_it != input.named_parameters.end() && !column_it->second.IsNull()) {
        bind_data->column_filter = column_it->second.GetValue<string>();
        bind_data->has_column_filter = true;
    }

    auto &registry = ConnectionRegistry::Get();
    bind_data->connection = registry.Get(bind_data->connection_id);
    if (!bind_data->connection) {
        throw InvalidInputException("adbc_columns: Invalid connection handle: " + to_string(bind_data->connection_id));
    }

    if (!bind_data->connection->IsInitialized()) {
        throw InvalidInputException("adbc_columns: Connection has been closed");
    }

    // Return schema for columns
    names = {"catalog_name", "schema_name", "table_name", "column_name", "ordinal_position", "remarks", "type_name", "is_nullable"};
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                    LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN};

    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> AdbcColumnsInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<AdbcColumnsBindData>();
    auto global_state = make_uniq<AdbcColumnsGlobalState>();

    if (!bind_data.connection->IsInitialized()) {
        throw InvalidInputException("adbc_columns: Connection has been closed");
    }

    memset(&global_state->stream, 0, sizeof(global_state->stream));

    const char *catalog = bind_data.has_catalog_filter ? bind_data.catalog_filter.c_str() : nullptr;
    const char *schema = bind_data.has_schema_filter ? bind_data.schema_filter.c_str() : nullptr;
    const char *table_name = bind_data.has_table_filter ? bind_data.table_filter.c_str() : nullptr;
    const char *column_name = bind_data.has_column_filter ? bind_data.column_filter.c_str() : nullptr;

    try {
        // depth=0 (ADBC_OBJECT_DEPTH_ALL) means catalogs, schemas, tables, and columns
        bind_data.connection->GetObjects(0, catalog, schema, table_name, nullptr, column_name, &global_state->stream);
    } catch (Exception &e) {
        throw IOException("adbc_columns: Failed to get columns: " + string(e.what()));
    }
    global_state->stream_initialized = true;

    // Pre-extract all columns by flattening the hierarchical structure
    ArrowArray batch;
    while (true) {
        memset(&batch, 0, sizeof(batch));
        int ret = global_state->stream.get_next(&global_state->stream, &batch);
        if (ret != 0) {
            const char *error_msg = global_state->stream.get_last_error(&global_state->stream);
            string msg = "adbc_columns: Failed to get next batch";
            if (error_msg) {
                msg += ": ";
                msg += error_msg;
            }
            throw IOException(msg);
        }

        if (!batch.release) {
            break; // End of stream
        }

        ExtractColumns(&batch, global_state->column_rows);

        if (batch.release) {
            batch.release(&batch);
        }
    }

    return std::move(global_state);
}

static unique_ptr<LocalTableFunctionState> AdbcColumnsInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                                 GlobalTableFunctionState *global_state_p) {
    return nullptr;
}

static void AdbcColumnsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &global_state = data.global_state->Cast<AdbcColumnsGlobalState>();

    if (global_state.current_row >= global_state.column_rows.size()) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    auto &catalog_vector = output.data[0];
    auto &schema_vector = output.data[1];
    auto &table_vector = output.data[2];
    auto &column_vector = output.data[3];
    auto &ordinal_vector = output.data[4];
    auto &remarks_vector = output.data[5];
    auto &type_vector = output.data[6];
    auto &nullable_vector = output.data[7];

    while (global_state.current_row < global_state.column_rows.size() && count < STANDARD_VECTOR_SIZE) {
        auto &row = global_state.column_rows[global_state.current_row];
        catalog_vector.SetValue(count, row.catalog_name.empty() ? Value() : Value(row.catalog_name));
        schema_vector.SetValue(count, row.schema_name.empty() ? Value() : Value(row.schema_name));
        table_vector.SetValue(count, Value(row.table_name));
        column_vector.SetValue(count, Value(row.column_name));
        ordinal_vector.SetValue(count, row.ordinal_position > 0 ? Value(row.ordinal_position) : Value());
        remarks_vector.SetValue(count, row.remarks.empty() ? Value() : Value(row.remarks));
        type_vector.SetValue(count, row.type_name.empty() ? Value() : Value(row.type_name));
        nullable_vector.SetValue(count, Value(row.is_nullable));
        count++;
        global_state.current_row++;
    }

    output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// adbc_schema - Get Arrow schema for a specific table
//===--------------------------------------------------------------------===//

// Structure to hold a schema field row
struct SchemaFieldRow {
    string field_name;
    string field_type;
    bool nullable;
};

struct AdbcSchemaBindData : public TableFunctionData {
    int64_t connection_id;
    shared_ptr<AdbcConnectionWrapper> connection;
    string table_name;
    string catalog_filter;
    string schema_filter;
    bool has_catalog_filter = false;
    bool has_schema_filter = false;
};

struct AdbcSchemaGlobalState : public GlobalTableFunctionState {
    // Extracted schema fields
    vector<SchemaFieldRow> field_rows;
    idx_t current_row = 0;

    idx_t MaxThreads() const override {
        return 1;
    }
};

// Helper to convert Arrow format string to human-readable type name
static string ArrowFormatToTypeName(const char *format) {
    if (!format) return "unknown";

    // Handle basic types - see Arrow C Data Interface spec
    switch (format[0]) {
        case 'n': return "null";
        case 'b': return "boolean";
        case 'c': return "int8";
        case 'C': return "uint8";
        case 's': return "int16";
        case 'S': return "uint16";
        case 'i': return "int32";
        case 'I': return "uint32";
        case 'l': return "int64";
        case 'L': return "uint64";
        case 'e': return "float16";
        case 'f': return "float32";
        case 'g': return "float64";
        case 'z': return "binary";
        case 'Z': return "large_binary";
        case 'u': return "utf8";
        case 'U': return "large_utf8";
        case 'd': {
            // Decimal: d:precision,scale or d:precision,scale,bitwidth
            return "decimal" + string(format + 1);
        }
        case 'w': {
            // Fixed-width binary: w:bytewidth
            return "fixed_binary" + string(format + 1);
        }
        case 't': {
            // Temporal types
            if (strlen(format) < 2) return "temporal";
            switch (format[1]) {
                case 'd': {
                    // Date: tdD (days) or tdm (milliseconds)
                    if (strlen(format) >= 3 && format[2] == 'D') return "date32";
                    if (strlen(format) >= 3 && format[2] == 'm') return "date64";
                    return "date";
                }
                case 't': {
                    // Time: tt[smun] (seconds/millis/micros/nanos)
                    if (strlen(format) >= 3) {
                        switch (format[2]) {
                            case 's': return "time32[s]";
                            case 'm': return "time32[ms]";
                            case 'u': return "time64[us]";
                            case 'n': return "time64[ns]";
                        }
                    }
                    return "time";
                }
                case 's': {
                    // Timestamp: ts[smun]:timezone
                    string result = "timestamp";
                    if (strlen(format) >= 3) {
                        switch (format[2]) {
                            case 's': result += "[s]"; break;
                            case 'm': result += "[ms]"; break;
                            case 'u': result += "[us]"; break;
                            case 'n': result += "[ns]"; break;
                        }
                    }
                    // Include timezone if present
                    const char *tz = strchr(format, ':');
                    if (tz && strlen(tz) > 1) {
                        result += " tz=" + string(tz + 1);
                    }
                    return result;
                }
                case 'D': {
                    // Duration: tD[smun]
                    if (strlen(format) >= 3) {
                        switch (format[2]) {
                            case 's': return "duration[s]";
                            case 'm': return "duration[ms]";
                            case 'u': return "duration[us]";
                            case 'n': return "duration[ns]";
                        }
                    }
                    return "duration";
                }
                case 'i': {
                    // Interval: tiM (months), tiD (days/time), tin (month/day/nano)
                    if (strlen(format) >= 3) {
                        switch (format[2]) {
                            case 'M': return "interval[months]";
                            case 'D': return "interval[days]";
                            case 'n': return "interval[month_day_nano]";
                        }
                    }
                    return "interval";
                }
            }
            return "temporal";
        }
        case '+': {
            // Nested types
            if (strlen(format) < 2) return "nested";
            switch (format[1]) {
                case 'l': return "list";
                case 'L': return "large_list";
                case 'w': return "fixed_list" + string(format + 2);
                case 's': return "struct";
                case 'm': return "map";
                case 'u': {
                    // Union: +ud:type_ids or +us:type_ids
                    if (strlen(format) >= 3) {
                        if (format[2] == 'd') return "dense_union";
                        if (format[2] == 's') return "sparse_union";
                    }
                    return "union";
                }
                case 'r': return "run_end_encoded";
                case 'v': {
                    // List view types
                    if (strlen(format) >= 3) {
                        if (format[2] == 'l') return "list_view";
                        if (format[2] == 'L') return "large_list_view";
                    }
                    return "list_view";
                }
            }
            return "nested";
        }
        default:
            // Return format string directly for unknown types
            return string(format);
    }
}

// Helper to extract fields from an ArrowSchema
static void ExtractSchemaFields(ArrowSchema *schema, vector<SchemaFieldRow> &field_rows) {
    if (!schema) return;

    for (int64_t i = 0; i < schema->n_children; i++) {
        ArrowSchema *child = schema->children[i];
        if (!child) continue;

        SchemaFieldRow row;
        row.field_name = child->name ? child->name : "";
        row.field_type = ArrowFormatToTypeName(child->format);
        // In Arrow C Data Interface, nullable is indicated by absence of ARROW_FLAG_NULLABLE bit NOT being set
        // flags & 2 means nullable (ARROW_FLAG_NULLABLE = 2)
        row.nullable = (child->flags & 2) != 0;
        field_rows.push_back(row);
    }
}

static unique_ptr<FunctionData> AdbcSchemaBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<AdbcSchemaBindData>();

    // Check for NULL connection handle
    if (input.inputs[0].IsNull()) {
        throw InvalidInputException("adbc_schema: Connection handle cannot be NULL");
    }

    bind_data->connection_id = input.inputs[0].GetValue<int64_t>();

    // Check for NULL table name
    if (input.inputs[1].IsNull()) {
        throw InvalidInputException("adbc_schema: Table name cannot be NULL");
    }

    bind_data->table_name = input.inputs[1].GetValue<string>();

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

    auto &registry = ConnectionRegistry::Get();
    bind_data->connection = registry.Get(bind_data->connection_id);
    if (!bind_data->connection) {
        throw InvalidInputException("adbc_schema: Invalid connection handle: " + to_string(bind_data->connection_id));
    }

    if (!bind_data->connection->IsInitialized()) {
        throw InvalidInputException("adbc_schema: Connection has been closed");
    }

    // Return schema for fields
    names = {"field_name", "field_type", "nullable"};
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN};

    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> AdbcSchemaInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<AdbcSchemaBindData>();
    auto global_state = make_uniq<AdbcSchemaGlobalState>();

    if (!bind_data.connection->IsInitialized()) {
        throw InvalidInputException("adbc_schema: Connection has been closed");
    }

    const char *catalog = bind_data.has_catalog_filter ? bind_data.catalog_filter.c_str() : nullptr;
    const char *db_schema = bind_data.has_schema_filter ? bind_data.schema_filter.c_str() : nullptr;

    ArrowSchema schema;
    memset(&schema, 0, sizeof(schema));

    try {
        bind_data.connection->GetTableSchema(catalog, db_schema, bind_data.table_name.c_str(), &schema);
    } catch (Exception &e) {
        throw IOException("adbc_schema: Failed to get table schema: " + string(e.what()));
    }

    // Extract fields from the schema
    ExtractSchemaFields(&schema, global_state->field_rows);

    // Release the schema
    if (schema.release) {
        schema.release(&schema);
    }

    return std::move(global_state);
}

static unique_ptr<LocalTableFunctionState> AdbcSchemaInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                                GlobalTableFunctionState *global_state_p) {
    return nullptr;
}

static void AdbcSchemaFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &global_state = data.global_state->Cast<AdbcSchemaGlobalState>();

    if (global_state.current_row >= global_state.field_rows.size()) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    auto &name_vector = output.data[0];
    auto &type_vector = output.data[1];
    auto &nullable_vector = output.data[2];

    while (global_state.current_row < global_state.field_rows.size() && count < STANDARD_VECTOR_SIZE) {
        auto &row = global_state.field_rows[global_state.current_row];
        name_vector.SetValue(count, Value(row.field_name));
        type_vector.SetValue(count, Value(row.field_type));
        nullable_vector.SetValue(count, Value(row.nullable));
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
    {
        TableFunction adbc_info_function("adbc_info", {LogicalType::BIGINT}, AdbcInfoFunction,
                                          AdbcInfoBind, AdbcInfoInitGlobal, AdbcInfoInitLocal);
        adbc_info_function.projection_pushdown = false;
        CreateTableFunctionInfo info(adbc_info_function);
        FunctionDescription desc;
        desc.description = "Get driver and database information from an ADBC connection";
        desc.parameter_names = {"connection_handle"};
        desc.parameter_types = {LogicalType::BIGINT};
        desc.examples = {"SELECT * FROM adbc_info(connection_handle)"};
        desc.categories = {"adbc"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(info);
    }

    // adbc_tables(connection_id, catalog, schema, table_name) - Get tables
    {
        TableFunction adbc_tables_function("adbc_tables", {LogicalType::BIGINT}, AdbcTablesFunction,
                                            AdbcTablesBind, AdbcTablesInitGlobal, AdbcTablesInitLocal);
        adbc_tables_function.named_parameters["catalog"] = LogicalType::VARCHAR;
        adbc_tables_function.named_parameters["schema"] = LogicalType::VARCHAR;
        adbc_tables_function.named_parameters["table_name"] = LogicalType::VARCHAR;
        adbc_tables_function.projection_pushdown = false;
        CreateTableFunctionInfo info(adbc_tables_function);
        FunctionDescription desc;
        desc.description = "Get list of tables from an ADBC data source";
        desc.parameter_names = {"connection_handle", "catalog", "schema", "table_name"};
        desc.parameter_types = {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM adbc_tables(conn)",
                         "SELECT * FROM adbc_tables(conn, catalog := 'main')",
                         "SELECT * FROM adbc_tables(conn, table_name := 'users')"};
        desc.categories = {"adbc"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(info);
    }

    // adbc_table_types(connection_id) - Get supported table types
    {
        TableFunction adbc_table_types_function("adbc_table_types", {LogicalType::BIGINT}, AdbcTableTypesFunction,
                                                 AdbcTableTypesBind, AdbcTableTypesInitGlobal, AdbcTableTypesInitLocal);
        adbc_table_types_function.projection_pushdown = false;
        CreateTableFunctionInfo info(adbc_table_types_function);
        FunctionDescription desc;
        desc.description = "Get supported table types from an ADBC data source (e.g., 'table', 'view')";
        desc.parameter_names = {"connection_handle"};
        desc.parameter_types = {LogicalType::BIGINT};
        desc.examples = {"SELECT * FROM adbc_table_types(conn)"};
        desc.categories = {"adbc"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(info);
    }

    // adbc_columns(connection_id, ...) - Get column metadata
    {
        TableFunction adbc_columns_function("adbc_columns", {LogicalType::BIGINT}, AdbcColumnsFunction,
                                             AdbcColumnsBind, AdbcColumnsInitGlobal, AdbcColumnsInitLocal);
        adbc_columns_function.named_parameters["catalog"] = LogicalType::VARCHAR;
        adbc_columns_function.named_parameters["schema"] = LogicalType::VARCHAR;
        adbc_columns_function.named_parameters["table_name"] = LogicalType::VARCHAR;
        adbc_columns_function.named_parameters["column_name"] = LogicalType::VARCHAR;
        adbc_columns_function.projection_pushdown = false;
        CreateTableFunctionInfo info(adbc_columns_function);
        FunctionDescription desc;
        desc.description = "Get column metadata for tables in an ADBC data source";
        desc.parameter_names = {"connection_handle", "catalog", "schema", "table_name", "column_name"};
        desc.parameter_types = {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM adbc_columns(conn)",
                         "SELECT * FROM adbc_columns(conn, table_name := 'users')",
                         "SELECT * FROM adbc_columns(conn, table_name := 'users', column_name := 'id')"};
        desc.categories = {"adbc"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(info);
    }

    // adbc_schema(connection_id, table_name, ...) - Get Arrow schema for a table
    {
        TableFunction adbc_schema_function("adbc_schema", {LogicalType::BIGINT, LogicalType::VARCHAR}, AdbcSchemaFunction,
                                            AdbcSchemaBind, AdbcSchemaInitGlobal, AdbcSchemaInitLocal);
        adbc_schema_function.named_parameters["catalog"] = LogicalType::VARCHAR;
        adbc_schema_function.named_parameters["schema"] = LogicalType::VARCHAR;
        adbc_schema_function.projection_pushdown = false;
        CreateTableFunctionInfo info(adbc_schema_function);
        FunctionDescription desc;
        desc.description = "Get the Arrow schema for a specific table in an ADBC data source";
        desc.parameter_names = {"connection_handle", "table_name", "catalog", "schema"};
        desc.parameter_types = {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
        desc.examples = {"SELECT * FROM adbc_schema(conn, 'users')",
                         "SELECT * FROM adbc_schema(conn, 'users', catalog := 'main')"};
        desc.categories = {"adbc"};
        info.descriptions.push_back(std::move(desc));
        loader.RegisterFunction(info);
    }
}

} // namespace adbc
} // namespace duckdb
