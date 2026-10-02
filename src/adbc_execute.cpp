#include "adbc_connection.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace adbc_scanner {
using namespace duckdb;

struct AdbcExecuteBindData : public TableFunctionData {
    string database;
    string query;
};

struct AdbcExecuteState : public GlobalTableFunctionState {
    bool finished = false;
};

static unique_ptr<FunctionData> AdbcExecuteBind(ClientContext &, TableFunctionBindInput &input,
                                              vector<LogicalType> &types, vector<string> &names) {
    if (input.inputs[0].IsNull()) {
        throw InvalidInputException("adbc_execute: database name cannot be NULL");
    }
    if (input.inputs[1].IsNull()) {
        throw InvalidInputException("adbc_execute: Query cannot be NULL");
    }
    auto data = make_uniq<AdbcExecuteBindData>();
    data->database = input.inputs[0].IsNull() ? string() : input.inputs[0].GetValue<string>();
    data->query = input.inputs[1].GetValue<string>();
    types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("rows_affected");
    return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> AdbcExecuteInit(ClientContext &, TableFunctionInitInput &) {
    return make_uniq<AdbcExecuteState>();
}

static void AdbcExecute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    auto &state = input.global_state->Cast<AdbcExecuteState>();
    if (state.finished) {
        return;
    }
    // Completion belongs to runtime state, not reusable prepared-statement bind data.
    state.finished = true;
    auto &data = input.bind_data->Cast<AdbcExecuteBindData>();
    auto connection = GetAttachedConnection(context, Value(data.database), "adbc_execute", true);
    AdbcStatementWrapper statement(connection);
    statement.Init();
    statement.SetSqlQuery(data.query);
    int64_t affected = -1;
    statement.ExecuteUpdate(&affected);
    output.SetCardinality(1);
    output.SetValue(0, 0, affected < 0 ? Value(LogicalType::BIGINT) : Value::BIGINT(affected));
}

void RegisterAdbcExecuteFunction(DatabaseInstance &db) {
    ExtensionLoader loader(db, "adbc");
    TableFunction function("adbc_execute", {LogicalType::VARCHAR, LogicalType::VARCHAR},
                           AdbcExecute, AdbcExecuteBind, AdbcExecuteInit);
    CreateTableFunctionInfo info(function);
    FunctionDescription description;
    description.description = "Execute a remote command at runtime; return NULL when the affected-row count is unknown";
    description.parameter_names = {"database", "query"};
    description.parameter_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
    description.examples = {"CALL adbc_execute('pg', 'INSERT INTO example VALUES (1)')"};
    description.categories = {"adbc"};
    info.descriptions.push_back(std::move(description));
    loader.RegisterFunction(info);
}
} // namespace adbc_scanner
