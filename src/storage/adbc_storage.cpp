#include "storage/adbc_storage.hpp"
#include "storage/adbc_catalog.hpp"
#include "storage/adbc_transaction_manager.hpp"
#include "adbc_connection.hpp"
#include "adbc_secrets.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace adbc_scanner {
using namespace duckdb;

//===--------------------------------------------------------------------===//
// Adbc Attach
//===--------------------------------------------------------------------===//

static unique_ptr<Catalog> AdbcAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                      AttachedDatabase &db, const string &name, AttachInfo &info,
                                      AttachOptions &attach_options) {
	// Parse the connection options from the attach info
	// Format: ATTACH 'connection_string' AS name (TYPE adbc, driver 'path')

	// First, collect explicit options into a vector for secret merging
	AdbcOptions explicit_options;
	idx_t batch_size = 0;

	for (auto &entry : attach_options.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "batch_size") {
			// batch_size is handled separately as it's DuckDB-specific, not passed to ADBC
			batch_size = entry.second.GetValue<idx_t>();
		} else {
			explicit_options.emplace_back(lower_name, entry.second);
		}
	}

	// The path is either a URI or 'key=value;key=value' options. Decide by the
	// text before the first '=': an option key is a bare identifier, while a URI
	// such as 'postgresql://h/db?sslmode=require' has ':' or '/' before it.
	auto first_eq = info.path.find('=');
	bool path_is_options = first_eq != string::npos;
	if (path_is_options) {
		auto first_key = info.path.substr(0, first_eq);
		StringUtil::Trim(first_key);
		path_is_options = !first_key.empty();
		for (char c : first_key) {
			if (!StringUtil::CharacterIsAlphaNumeric(c) && c != '_' && c != '.' && c != '-') {
				path_is_options = false;
				break;
			}
		}
	}

	if (path_is_options) {
		for (auto &part : StringUtil::Split(info.path, ';')) {
			auto eq_pos = part.find('=');
			if (eq_pos != string::npos) {
				string key_part = part.substr(0, eq_pos);
				string value_part = part.substr(eq_pos + 1);
				StringUtil::Trim(key_part);
				StringUtil::Trim(value_part);
				explicit_options.emplace_back(StringUtil::Lower(key_part), value_part);
			}
		}
	} else if (!info.path.empty()) {
		// Use the path as the URI unless one was given as an option
		bool has_uri = false;
		for (const auto &opt : explicit_options) {
			if (opt.first == "uri") {
				has_uri = true;
				break;
			}
		}
		if (!has_uri) {
			explicit_options.emplace_back("uri", info.path);
		}
	}

	// Merge with secrets (this looks up secrets by name or URI scope)
	auto options = MergeSecretOptions(context, explicit_options);

	// Create the connection using the shared helper
	auto connection = CreateConnectionFromOptions(options);

	// Create and return the catalog
	auto catalog = make_uniq<AdbcCatalog>(db, connection, info.path, attach_options.access_mode);
	catalog->batch_size = batch_size;
	string driver, uri;
	for (const auto &opt : options) {
		if (opt.first == "driver") {
			driver = StringUtil::Lower(opt.second.ToString());
		} else if (opt.first == "uri") {
			uri = opt.second.ToString();
		}
	}
	catalog->per_connection_memory = driver.find("sqlite") != string::npos && uri == ":memory:";
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

} // namespace adbc_scanner
