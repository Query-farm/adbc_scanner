#include "adbc_connection.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace adbc {

// Helper to extract key-value pairs from either a STRUCT or MAP
static vector<pair<string, string>> ExtractOptions(Vector &options_vector, idx_t row_idx) {
    vector<pair<string, string>> options;
    auto value = options_vector.GetValue(row_idx);
    auto &type = value.type();

    if (type.id() == LogicalTypeId::STRUCT) {
        // Handle STRUCT - iterate over named fields
        auto &children = StructValue::GetChildren(value);
        for (idx_t i = 0; i < children.size(); i++) {
            auto key = StructType::GetChildName(type, i);
            auto &child_value = children[i];
            if (!child_value.IsNull()) {
                options.emplace_back(key, child_value.ToString());
            }
        }
    } else if (type.id() == LogicalTypeId::MAP) {
        // Handle MAP - iterate over key-value pairs
        auto &map_children = MapValue::GetChildren(value);
        for (auto &entry : map_children) {
            auto &entry_children = StructValue::GetChildren(entry);
            if (entry_children.size() == 2 && !entry_children[0].IsNull()) {
                auto key = entry_children[0].ToString();
                auto val = entry_children[1].IsNull() ? "" : entry_children[1].ToString();
                options.emplace_back(key, val);
            }
        }
    } else {
        throw InvalidInputException("adbc_connect: options must be a STRUCT or MAP, got " + type.ToString());
    }

    return options;
}

// Helper to create a connection from options
static int64_t CreateConnection(const vector<pair<string, string>> &options) {
    string driver;
    string entrypoint;
    vector<pair<string, string>> db_options;
    vector<pair<string, string>> conn_options;

    for (const auto &opt : options) {
        if (opt.first == "driver") {
            driver = opt.second;
        } else if (opt.first == "entrypoint") {
            entrypoint = opt.second;
        } else {
            // All other options go to database (driver-specific options)
            db_options.emplace_back(opt.first, opt.second);
        }
    }

    // Validate required options
    if (driver.empty()) {
        throw InvalidInputException("adbc_connect: 'driver' option is required");
    }

    // Create database wrapper
    auto database = make_shared_ptr<AdbcDatabaseWrapper>();
    database->Init();

    // Set driver (required)
    database->SetOption("driver", driver);

    // Store driver name for error messages
    database->SetDriverName(driver);

    // Set entrypoint if provided
    if (!entrypoint.empty()) {
        database->SetOption("entrypoint", entrypoint);
    }

    // Set driver-specific database options
    for (const auto &opt : db_options) {
        database->SetOption(opt.first, opt.second);
    }

    // Initialize database
    database->Initialize();

    // Create connection wrapper
    auto connection = make_shared_ptr<AdbcConnectionWrapper>(database);
    connection->Init();

    // Set connection options
    for (const auto &opt : conn_options) {
        connection->SetOption(opt.first, opt.second);
    }

    // Initialize connection
    connection->Initialize();

    // Register connection and return handle
    auto &registry = ConnectionRegistry::Get();
    return registry.Add(std::move(connection));
}

// adbc_connect(options STRUCT or MAP) -> BIGINT
// Returns a connection handle that can be used with other ADBC functions
static void AdbcConnectFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &options_vector = args.data[0];
    auto count = args.size();

    // Handle constant input (for constant folding optimization)
    if (options_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
        if (ConstantVector::IsNull(options_vector)) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
            ConstantVector::SetNull(result, true);
        } else {
            auto options = ExtractOptions(options_vector, 0);
            auto conn_id = CreateConnection(options);
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
            ConstantVector::GetData<int64_t>(result)[0] = conn_id;
        }
        return;
    }

    // Handle flat/dictionary vectors
    result.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data = FlatVector::GetData<int64_t>(result);

    for (idx_t row_idx = 0; row_idx < count; row_idx++) {
        auto options = ExtractOptions(options_vector, row_idx);
        result_data[row_idx] = CreateConnection(options);
    }
}

// adbc_disconnect(connection_id BIGINT) -> BOOLEAN
// Disconnects and removes a connection from the registry
static void AdbcDisconnectFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &connection_vector = args.data[0];

    UnaryExecutor::Execute<int64_t, bool>(connection_vector, result, args.size(), [&](int64_t connection_id) {
        auto &registry = ConnectionRegistry::Get();
        auto connection = registry.Remove(connection_id);
        if (!connection) {
            throw InvalidInputException("adbc_disconnect: Invalid connection handle: " + to_string(connection_id));
        }
        // Connection is automatically released when shared_ptr goes out of scope
        return true;
    });
}

// Register the ADBC scalar functions using ExtensionLoader
void RegisterAdbcScalarFunctions(DatabaseInstance &db) {
    ExtensionLoader loader(db, "adbc");

    // adbc_connect: Create a new ADBC connection
    // Accepts either STRUCT or MAP with string keys/values
    auto adbc_connect_function = ScalarFunction(
        "adbc_connect",
        {LogicalType::ANY},
        LogicalType::BIGINT,
        AdbcConnectFunction
    );
    loader.RegisterFunction(adbc_connect_function);

    // adbc_disconnect: Close an ADBC connection
    auto adbc_disconnect_function = ScalarFunction(
        "adbc_disconnect",
        {LogicalType::BIGINT},
        LogicalType::BOOLEAN,
        AdbcDisconnectFunction
    );
    loader.RegisterFunction(adbc_disconnect_function);
}

} // namespace adbc
} // namespace duckdb
