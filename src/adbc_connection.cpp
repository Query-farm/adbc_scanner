#include "adbc_connection.hpp"

namespace adbc_scanner {
using namespace duckdb;

shared_ptr<AdbcConnectionWrapper> GetValidatedConnection(int64_t connection_id, const string &function_name) {
	auto &registry = ConnectionRegistry::Get();
	auto connection = registry.Get(connection_id);
	if (!connection) {
		throw InvalidInputException(function_name + ": Invalid connection handle: " + to_string(connection_id));
	}
	if (!connection->IsInitialized()) {
		throw InvalidInputException(function_name + ": Connection has been closed");
	}
	return connection;
}

shared_ptr<AdbcConnectionWrapper> CreateConnectionFromOptions(const vector<pair<string, string>> &options) {
	string driver;
	string entrypoint;
	string uri;
	string search_paths;
	string profile;
	bool use_manifests = true;
	vector<pair<string, string>> db_options;

	for (const auto &opt : options) {
		if (opt.first == "driver") {
			driver = opt.second;
		} else if (opt.first == "entrypoint") {
			entrypoint = opt.second;
		} else if (opt.first == "uri") {
			uri = opt.second;
		} else if (opt.first == "search_paths") {
			search_paths = opt.second;
		} else if (opt.first == "profile") {
			profile = opt.second;
		} else if (opt.first == "use_manifests") {
			use_manifests = (opt.second == "true" || opt.second == "1");
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
