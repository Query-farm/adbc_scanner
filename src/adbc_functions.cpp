#include "adbc_connection.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace adbc {

// adbc_connect(options MAP(VARCHAR, VARCHAR)) -> BIGINT
// Returns a connection handle that can be used with other ADBC functions
static void AdbcConnectFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &options_vector = args.data[0];

    UnaryExecutor::Execute<list_entry_t, int64_t>(options_vector, result, args.size(), [&](list_entry_t options_entry) {
        // Get the MAP key and value vectors
        auto &key_vector = MapVector::GetKeys(options_vector);
        auto &value_vector = MapVector::GetValues(options_vector);

        // Extract options from MAP
        string driver;
        string entrypoint;
        vector<pair<string, string>> db_options;
        vector<pair<string, string>> conn_options;

        for (idx_t i = options_entry.offset; i < options_entry.offset + options_entry.length; i++) {
            auto key = key_vector.GetValue(i).ToString();
            auto value = value_vector.GetValue(i).ToString();

            if (key == "driver") {
                driver = value;
            } else if (key == "entrypoint") {
                entrypoint = value;
            } else {
                // All other options go to database (driver-specific options)
                db_options.emplace_back(key, value);
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
    });
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
    auto adbc_connect_function = ScalarFunction(
        "adbc_connect",
        {LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)},
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
