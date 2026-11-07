#include "adbc_insert_stream.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace adbc_scanner {
using namespace duckdb;

struct AdbcInsertBindData : public TableFunctionData {
    string database;
    string target_table;
    string mode;  // "create", "append", "replace", "create_append"
    shared_ptr<AdbcConnectionWrapper> connection;
    vector<LogicalType> input_types;
    vector<string> input_names;
    // Per-call override for the bounded-queue depth (0 = fall back to
    // ADBC_INSERT_MAX_PENDING_BATCHES / the built-in default). Lets callers with
    // fat rows (e.g. raster BLOB blocks) cap memory tighter than the default.
    int64_t max_batches = 0;
    // Extra driver-specific statement options (e.g. a driver's ingest type
    // overrides), applied after target_table and mode.
    vector<pair<string, string>> statement_options;
};

static vector<pair<string, string>> ExtractStatementOptions(const Value &value) {
    vector<pair<string, string>> options;
    auto &type = value.type();
    if (type.id() == LogicalTypeId::STRUCT) {
        auto &children = StructValue::GetChildren(value);
        for (idx_t i = 0; i < children.size(); i++) {
            if (!children[i].IsNull()) {
                options.emplace_back(StructType::GetChildName(type, i), children[i].ToString());
            }
        }
    } else if (type.id() == LogicalTypeId::MAP) {
        for (auto &entry : MapValue::GetChildren(value)) {
            auto &kv = StructValue::GetChildren(entry);
            if (kv.size() == 2 && !kv[0].IsNull() && !kv[1].IsNull()) {
                options.emplace_back(kv[0].ToString(), kv[1].ToString());
            }
        }
    } else {
        throw InvalidInputException("adbc_insert: options must be a STRUCT or MAP, got " + type.ToString());
    }
    for (auto &option : options) {
        if (option.first == "adbc.ingest.target_table" || option.first == "adbc.ingest.mode") {
            throw InvalidInputException("adbc_insert: option '" + option.first +
                                        "' is set by adbc_insert itself; use the table_name argument or mode := instead");
        }
    }
    return options;
}

struct AdbcInsertGlobalState : public GlobalTableFunctionState {
    mutex lock;
    // Statement, bounded stream and the background BindStream/ExecuteUpdate
    // thread (shared with the ATTACH write path, see adbc_insert_stream.hpp)
    unique_ptr<AdbcStreamingInsertConsumer> consumer;
    int64_t rows_inserted = 0;
    ClientProperties client_properties;
    // Per input column: the existing target table declares it NOT NULL (append mode)
    vector<bool> not_null_columns;

    idx_t MaxThreads() const override {
        return 1;  // single producer — keep AddBatch ordering simple
    }
};

static unique_ptr<FunctionData> AdbcInsertBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names) {
    (void)context;
    auto bind_data = make_uniq<AdbcInsertBindData>();

    // Check for NULL database name
    if (input.inputs[0].IsNull()) {
        throw InvalidInputException("adbc_insert: database name cannot be NULL");
    }

    // First argument is the attached database name
    bind_data->database = input.inputs[0].IsNull() ? string() : input.inputs[0].GetValue<string>();

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

    auto opts_it = input.named_parameters.find("options");
    if (opts_it != input.named_parameters.end() && !opts_it->second.IsNull()) {
        bind_data->statement_options = ExtractStatementOptions(opts_it->second);
    }

    // Validate the database name; execution resolves it again (see AdbcInsertInitGlobal)
    bind_data->connection = GetAttachedConnection(context, Value(bind_data->database), "adbc_insert", true);

    // Store input table types and names for Arrow conversion
    bind_data->input_types = input.input_table_types;
	bind_data->input_names = IdentifiersToStrings(input.input_table_names);

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
    // Resolve the database again: a prepared insert may run after DETACH, or
    // inside a transaction it was not bound in.
    auto connection = GetAttachedConnection(context, Value(bind_data.database), "adbc_insert", true);
    if (bind_data.mode == "append") {
        // The table must already exist: match its NOT NULL columns, which drivers
        // that compare schemas strictly (DataFusion) require. Ask before creating
        // the statement, which holds the connection.
        const char *db_schema = nullptr;
        for (auto &option : bind_data.statement_options) {
            if (option.first == ADBC_INGEST_OPTION_TARGET_DB_SCHEMA) {
                db_schema = option.second.c_str();
            }
        }
        global_state->not_null_columns =
            GetTargetNotNullColumns(*connection, db_schema, bind_data.target_table, bind_data.input_names);
    }
    global_state->consumer = make_uniq<AdbcStreamingInsertConsumer>(ResolveMaxPendingBatches(bind_data.max_batches));
    auto &statement = global_state->consumer->statement;
    statement = make_shared_ptr<AdbcStatementWrapper>(connection);
    statement->Init();
    statement->SetOption("adbc.ingest.target_table", bind_data.target_table);

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
    statement->SetOption("adbc.ingest.mode", mode_value);

    for (auto &option : bind_data.statement_options) {
        statement->SetOption(option.first, option.second);
    }

    // Set up the schema from the input types
    ArrowSchema schema;
    ArrowConverter::ToArrowSchema(&schema, bind_data.input_types, bind_data.input_names,
                                   global_state->client_properties);
    ApplyNotNullColumns(schema, global_state->not_null_columns);
    global_state->consumer->SetSchema(&schema);

    // Start binding and execution concurrently with the producer. A driver can
    // consume the stream in either call; neither may run on the producer thread.
    global_state->consumer->StartConsumer();

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

    CheckNotNullColumns(input, global_state.not_null_columns, bind_data.input_names, bind_data.target_table);

    // Convert DuckDB DataChunk to Arrow
    ArrowAppender appender(bind_data.input_types, input.size(),
                           global_state.client_properties,
                           ArrowTypeExtensionData::GetExtensionTypes(context.client, bind_data.input_types));
    appender.Append(input, 0, input.size(), input.size());

    ArrowArray arr = appender.Finalize();

    // Hand the batch to the consumer; blocks for backpressure when the queue is
    // full. Returns false only if the consumer thread already stopped (error /
    // cancellation) — surface that as a query error.
    global_state.consumer->PushBatch(&arr, context.client, "adbc_insert");
    global_state.rows_inserted += input.size();

    // Don't output anything during processing - we output the total at the end
    output.SetCardinality(0);
    return OperatorResultType::NEED_MORE_INPUT;
}

static OperatorFinalizeResultType AdbcInsertFinalize(ExecutionContext &context, TableFunctionInput &data_p,
                                                      DataChunk &output) {
    auto &global_state = data_p.global_state->Cast<AdbcInsertGlobalState>();
    lock_guard<mutex> l(global_state.lock);

    // Signal end of input, then wait for ExecuteUpdate to finish draining.
    global_state.consumer->FinishAndJoin(context.client);

    // Output the total rows inserted (producer-side count is reliable across all
    // drivers; the driver's rows_affected is advisory).
    output.SetCardinality(1);
    output.SetValue(0, 0, Value::BIGINT(global_state.rows_inserted));

    return OperatorFinalizeResultType::FINISHED;
}

// Register adbc_insert table in-out function
void RegisterAdbcInsertFunction(DatabaseInstance &db) {
    ExtensionLoader loader(db, "adbc");

    // adbc_insert(database, table_name, <table>) - Bulk insert data
    TableFunction adbc_insert_function("adbc_insert",
                                        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::TABLE},
                                        nullptr,  // No regular function - use in_out
                                        AdbcInsertBind,
                                        AdbcInsertInitGlobal);
    adbc_insert_function.in_out_function = AdbcInsertInOut;
    adbc_insert_function.in_out_function_final = AdbcInsertFinalize;
    // Optional named parameters: a call that leaves one out does not pass it.
    adbc_insert_function.GetSignature().WithTypedKwargs("kwargs", [](TypedKwargs &options) {
        options.Add("mode", LogicalType::VARCHAR);
        // Optional bounded-queue depth override (default 32 / ADBC_INSERT_MAX_PENDING_BATCHES).
        options.Add("max_batches", LogicalType::BIGINT);
        // Driver-specific statement options as a STRUCT or MAP of key/value pairs.
        options.Add("options", LogicalType::ANY);
    });

    NameParameters(adbc_insert_function, {"database", "table_name", "data"});
    CreateTableFunctionInfo info(adbc_insert_function);
    FunctionDescription desc;
    desc.description = "Bulk insert data from a query into an ADBC table";
    desc.examples = {"SELECT * FROM adbc_insert('pg', 'target_table', (SELECT * FROM source_table))",
                     "SELECT * FROM adbc_insert('pg', 'target', (SELECT * FROM source), mode := 'create')",
                     "SELECT * FROM adbc_insert('pg', 'target', (SELECT * FROM source), mode := 'append')",
                     "SELECT * FROM adbc_insert('pg', 'target', (SELECT * FROM source), mode := 'create', options := {'adbc.ingest.temporary': 'true'})"};
    desc.categories = {"adbc"};
    info.descriptions.push_back(std::move(desc));
    loader.RegisterFunction(info);
}

} // namespace adbc_scanner
