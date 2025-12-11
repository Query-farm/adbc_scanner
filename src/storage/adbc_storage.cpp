#include "storage/adbc_storage.hpp"
#include "storage/adbc_catalog.hpp"
#include "storage/adbc_transaction_manager.hpp"
#include "adbc_connection.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Adbc Attach
//===--------------------------------------------------------------------===//

static unique_ptr<Catalog> AdbcAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                      AttachedDatabase &db, const string &name, AttachInfo &info,
                                      AttachOptions &attach_options) {
	// Parse the connection options from the attach info
	// Format: ATTACH 'connection_string' AS name (TYPE adbc, driver 'path')
	string driver_path;
	string entrypoint;
	string uri;
	string search_paths;
	bool use_manifests = true;
	idx_t batch_size = 0;
	vector<pair<string, string>> db_options;

	for (auto &entry : attach_options.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "driver") {
			driver_path = entry.second.ToString();
		} else if (lower_name == "entrypoint") {
			entrypoint = entry.second.ToString();
		} else if (lower_name == "search_paths") {
			search_paths = entry.second.ToString();
		} else if (lower_name == "use_manifests") {
			auto val = entry.second.ToString();
			use_manifests = (val == "true" || val == "1");
		} else if (lower_name == "batch_size") {
			batch_size = entry.second.GetValue<idx_t>();
		} else {
			// Pass other options to the ADBC driver
			db_options.emplace_back(lower_name, entry.second.ToString());
		}
	}

	// If no driver specified in options, try to extract from path
	if (driver_path.empty()) {
		// Check if path contains key=value pairs
		auto parts = StringUtil::Split(info.path, ';');
		for (auto &part : parts) {
			auto eq_pos = part.find('=');
			if (eq_pos != string::npos) {
				string key_part = part.substr(0, eq_pos);
				string value_part = part.substr(eq_pos + 1);
				StringUtil::Trim(key_part);
				StringUtil::Trim(value_part);
				auto key = StringUtil::Lower(key_part);
				if (key == "driver") {
					driver_path = value_part;
				} else if (key == "uri") {
					uri = value_part;
				} else {
					db_options.emplace_back(key, value_part);
				}
			}
		}
	}

	// Use the path as URI if not already set and it doesn't look like key=value format
	if (uri.empty() && !info.path.empty() && info.path.find('=') == string::npos) {
		uri = info.path;
	}

	if (driver_path.empty()) {
		throw BinderException("ADBC attach requires a 'driver' option specifying the ADBC driver path or name");
	}

	// Create database wrapper
	auto database = make_shared_ptr<adbc::AdbcDatabaseWrapper>();
	database->Init();

	// Enable manifest-based driver discovery by default
	if (use_manifests) {
		database->SetLoadFlags(ADBC_LOAD_FLAG_DEFAULT);
	} else {
		database->SetLoadFlags(ADBC_LOAD_FLAG_ALLOW_RELATIVE_PATHS);
	}

	// Set additional search paths if provided
	if (!search_paths.empty()) {
		database->SetAdditionalSearchPaths(search_paths);
	}

	// Set driver (required)
	database->SetOption("driver", driver_path);
	database->SetDriverName(driver_path);

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
	auto connection = make_shared_ptr<adbc::AdbcConnectionWrapper>(database);
	connection->Init();
	connection->Initialize();

	// Create and return the catalog
	auto catalog = make_uniq<AdbcCatalog>(db, connection, info.path, attach_options.access_mode);
	catalog->batch_size = batch_size;
	return catalog;
}

static unique_ptr<TransactionManager> AdbcCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                    AttachedDatabase &db, Catalog &catalog) {
	auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
	return make_uniq<AdbcTransactionManager>(db, adbc_catalog);
}

AdbcStorageExtension::AdbcStorageExtension() {
	attach = AdbcAttach;
	create_transaction_manager = AdbcCreateTransactionManager;
}

} // namespace duckdb
