#include "adbc_connection.hpp"
#include "adbc_secrets.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"

namespace adbc_scanner {
using namespace duckdb;

struct AdbcClientHandles : public ClientContextState {
    unordered_set<int64_t> handles;
    ~AdbcClientHandles() override {
        for (auto handle : handles) {
            ConnectionRegistry::Get().Remove(handle);
        }
    }
};

static AdbcOptions ExtractOptions(const Value &value) {
    AdbcOptions options;
    if (value.type().id() == LogicalTypeId::STRUCT) {
        const auto &children = StructValue::GetChildren(value);
        for (idx_t i = 0; i < children.size(); i++) {
            if (!children[i].IsNull()) {
                options.emplace_back(StructType::GetChildName(value.type(), i), children[i]);
            }
        }
    } else if (value.type().id() == LogicalTypeId::MAP) {
        for (const auto &entry : MapValue::GetChildren(value)) {
            const auto &pair = StructValue::GetChildren(entry);
            if (!pair[0].IsNull() && !pair[1].IsNull()) {
                options.emplace_back(pair[0].GetValue<string>(), pair[1]);
            }
        }
    } else {
        throw InvalidInputException("adbc_connect: options must be a STRUCT or MAP");
    }
    for (const auto &option : options) {
        if (option.second.type().id() == LogicalTypeId::STRUCT ||
            option.second.type().id() == LogicalTypeId::MAP ||
            option.second.type().id() == LogicalTypeId::LIST) {
            throw InvalidInputException("adbc_connect: nested option values are not supported; pass driver options directly");
        }
    }
    return options;
}

static void AdbcConnect(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &context = state.GetContext();
    auto owned = context.registered_state->GetOrCreate<AdbcClientHandles>("adbc.handles");
    // Volatile scalars have per-row semantics even when their input is constant.
    result.SetVectorType(VectorType::FLAT_VECTOR);
    auto values = FlatVector::GetData<int64_t>(result);
    vector<int64_t> created;
    created.reserve(args.size());
    try {
        for (idx_t row = 0; row < args.size(); row++) {
            auto value = args.data[0].GetValue(row);
            if (value.IsNull()) {
                throw InvalidInputException("adbc_connect: options must not be NULL");
            }
            auto options = MergeSecretOptions(context, ExtractOptions(value));
            auto connection = CreateConnectionFromOptions(options);
            auto handle = ConnectionRegistry::Get().Add(std::move(connection), &context);
            created.push_back(handle);
            owned->handles.insert(handle);
            values[row] = handle;
        }
    } catch (...) {
        // No handles from this output vector can reach the caller on failure.
        for (auto handle : created) {
            ConnectionRegistry::Get().Remove(handle);
            owned->handles.erase(handle);
        }
        throw;
    }
}

struct AdbcCommandBindData : public TableFunctionData {
    string command;
    int64_t handle;
    bool enabled = false;
};

struct AdbcCommandState : public GlobalTableFunctionState {
    bool finished = false;
};

static unique_ptr<FunctionData> BindCommand(ClientContext &, TableFunctionBindInput &input,
                                            vector<LogicalType> &types, vector<string> &names) {
    for (const auto &argument : input.inputs) {
        if (argument.IsNull()) {
            throw InvalidInputException("ADBC command arguments must not be NULL");
        }
    }
    auto data = make_uniq<AdbcCommandBindData>();
    data->command = input.table_function.name;
    data->handle = input.inputs[0].GetValue<int64_t>();
    if (input.inputs.size() == 2) {
        data->enabled = input.inputs[1].GetValue<bool>();
    }
    types.emplace_back(LogicalType::BOOLEAN);
    names.emplace_back("success");
    return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> InitCommand(ClientContext &, TableFunctionInitInput &) {
    return make_uniq<AdbcCommandState>();
}

static void RunCommand(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    auto &state = input.global_state->Cast<AdbcCommandState>();
    if (state.finished) {
        return;
    }
    state.finished = true;
    const auto &data = input.bind_data->Cast<AdbcCommandBindData>();
    auto connection = GetValidatedConnection(context, data.handle, data.command);
    if (data.command == "adbc_disconnect") {
        connection->Close();
        ConnectionRegistry::Get().Remove(data.handle, &context);
        context.registered_state->GetOrCreate<AdbcClientHandles>("adbc.handles")->handles.erase(data.handle);
    } else if (data.command == "adbc_commit") {
        connection->Commit();
    } else if (data.command == "adbc_rollback") {
        connection->Rollback();
    } else {
        connection->SetAutocommit(data.enabled);
    }
    output.SetCardinality(1);
    output.SetValue(0, 0, Value::BOOLEAN(true));
}

void RegisterAdbcScalarFunctions(DatabaseInstance &db) {
    ExtensionLoader loader(db, "adbc");
    ScalarFunction connect("adbc_connect", {LogicalType::ANY}, LogicalType::BIGINT, AdbcConnect);
    connect.stability = FunctionStability::VOLATILE;
    connect.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
    CreateScalarFunctionInfo connect_info(connect);
    FunctionDescription connect_description;
    connect_description.description = "Open an ADBC connection owned by this DuckDB client; evaluated once per input row";
    connect_description.parameter_names = {"options"};
    connect_description.parameter_types = {LogicalType::ANY};
    connect_description.examples = {"SELECT adbc_connect({'driver': 'sqlite', 'uri': ':memory:'})"};
    connect_description.categories = {"adbc"};
    connect_info.descriptions.push_back(std::move(connect_description));
    loader.RegisterFunction(connect_info);
    for (auto name : {"adbc_disconnect", "adbc_commit", "adbc_rollback", "adbc_set_autocommit"}) {
        vector<LogicalType> arguments = {LogicalType::BIGINT};
        if (string(name) == "adbc_set_autocommit") {
            arguments.push_back(LogicalType::BOOLEAN);
        }
        TableFunction command(name, arguments, RunCommand, BindCommand, InitCommand);
        CreateTableFunctionInfo info(command);
        FunctionDescription description;
        description.description = string(name) + ": perform the connection operation at execution time";
        description.parameter_names = {"connection_handle"};
        if (arguments.size() == 2) {
            description.parameter_names.push_back("enabled");
        }
        description.parameter_types = arguments;
        description.examples = {"CALL " + string(name) + (arguments.size() == 2 ? "(conn, false)" : "(conn)")};
        description.categories = {"adbc"};
        info.descriptions.push_back(std::move(description));
        loader.RegisterFunction(info);
    }
}
} // namespace adbc_scanner
