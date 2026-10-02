#include "adbc_connection.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include "storage/adbc_catalog.hpp"
#include "storage/adbc_transaction.hpp"

namespace adbc_scanner {
using namespace duckdb;

shared_ptr<AdbcConnectionWrapper> GetAttachedConnection(ClientContext &context, const Value &database,
                                                        const string &function_name, bool write) {
	if (database.IsNull()) {
		throw InvalidInputException("%s: database name must not be NULL", function_name);
	}
	auto name = database.GetValue<string>();
	auto attached = DatabaseManager::Get(context).GetDatabase(context, name);
	if (!attached) {
		throw BinderException("%s: no attached database named \"%s\"; attach one with "
		                      "ATTACH '<uri>' AS %s (TYPE adbc, driver '...')",
		                      function_name, name, name);
	}
	auto &catalog = attached->GetCatalog();
	if (catalog.GetCatalogType() != "adbc") {
		throw BinderException("%s: \"%s\" is a %s database, not an ADBC one", function_name, name,
		                      catalog.GetCatalogType());
	}
	auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
	if (write && adbc_catalog.access_mode == AccessMode::READ_ONLY) {
		throw PermissionException("%s: \"%s\" is attached read-only", function_name, name);
	}
	auto &transaction = AdbcTransaction::Get(context, catalog);
	bool explicit_transaction = !context.transaction.IsAutoCommit();
	// Not a pooled connection for reads: session state (temp tables, SET ...)
	// made by adbc_execute / adbc_insert lives on the attachment's connection.
	auto connection = (write ? explicit_transaction : transaction.HasWriteConnection())
	                      ? transaction.GetWriteConnection()
	                      : adbc_catalog.GetConnection();
	if (!connection->IsInitialized()) {
		throw InvalidInputException("%s: the connection for \"%s\" has been closed", function_name, name);
	}
	return connection;
}

shared_ptr<AdbcConnectionWrapper> CreateConnectionFromOptions(const AdbcOptions &options) {
	string driver;
	string entrypoint;
	string uri;
	string search_paths;
	string profile;
	bool use_manifests = true;
	AdbcOptions db_options;

	for (const auto &opt : options) {
		if (opt.first == "driver") {
			driver = opt.second.GetValue<string>();
		} else if (opt.first == "entrypoint") {
			entrypoint = opt.second.GetValue<string>();
		} else if (opt.first == "uri") {
			uri = opt.second.GetValue<string>();
		} else if (opt.first == "search_paths") {
			search_paths = opt.second.GetValue<string>();
		} else if (opt.first == "profile") {
			profile = opt.second.GetValue<string>();
		} else if (opt.first == "use_manifests") {
			use_manifests = (opt.second.ToString() == "true" || opt.second.ToString() == "1");
		} else if (opt.first == "secret") {
			// Skip the secret option itself - it was used for lookup
			continue;
		} else {
			// Pass other options to the ADBC driver
			db_options.emplace_back(opt.first, opt.second);
		}
	}

	// A connection profile may be supplied either via the 'profile' option or a
	// 'profile://<name>' URI. When present, the driver manager resolves the driver
	// and options from the profile TOML, so an explicit 'driver' is not required.
	const bool has_profile = !profile.empty() || StringUtil::StartsWith(uri, "profile://");

	// Validate required options
	if (driver.empty() && !has_profile) {
		throw InvalidInputException(
		    "ADBC connection requires a 'driver' option (or a 'profile' option / 'profile://' URI)");
	}

	// Create database wrapper
	auto database = make_shared_ptr<AdbcDatabaseWrapper>();
	database->Init();

	// Enable manifest-based driver discovery by default
	if (use_manifests) {
		database->SetLoadFlags(ADBC_LOAD_FLAG_DEFAULT);
	} else {
		database->SetLoadFlags(ADBC_LOAD_FLAG_ALLOW_RELATIVE_PATHS);
	}

	// Set additional search paths if provided. These apply both to driver manifest
	// discovery and to connection profile resolution.
	if (!search_paths.empty()) {
		database->SetAdditionalSearchPaths(search_paths);
		database->SetOption("additional_profile_search_path_list", search_paths);
	}

	// Set the connection profile to resolve (the driver and options come from it)
	if (!profile.empty()) {
		database->SetOption("profile", profile);
	}

	// Set driver if provided (optional when a profile supplies it)
	if (!driver.empty()) {
		database->SetOption("driver", driver);
		database->SetDriverName(driver);
	}

	// Set entrypoint if provided
	if (!entrypoint.empty()) {
		database->SetOption("entrypoint", entrypoint);
	}

	// Set URI if provided
	if (!uri.empty()) {
		database->SetOption("uri", uri);
	}

	// Set other driver-specific options
	for (const auto &opt : db_options) {
		database->SetOption(opt.first, opt.second);
	}

	// Initialize database
	database->Initialize();

	// Create connection wrapper
	auto connection = make_shared_ptr<AdbcConnectionWrapper>(database);
	connection->Init();
	connection->Initialize();

	return connection;
}

} // namespace adbc_scanner
