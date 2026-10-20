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

struct DatabaseSettings {
	string driver;
	string entrypoint;
	string search_paths;
	string profile;
	bool use_manifests = true;
};

static shared_ptr<AdbcDatabaseWrapper> OpenDatabase(const DatabaseSettings &settings, const string &uri,
                                                    const AdbcOptions &db_options) {
	// Create database wrapper
	auto database = make_shared_ptr<AdbcDatabaseWrapper>();
	database->Init();

	// Enable manifest-based driver discovery by default
	if (settings.use_manifests) {
		database->SetLoadFlags(ADBC_LOAD_FLAG_DEFAULT);
	} else {
		database->SetLoadFlags(ADBC_LOAD_FLAG_ALLOW_RELATIVE_PATHS);
	}

	// Set additional search paths if provided. These apply both to driver manifest
	// discovery and to connection profile resolution.
	if (!settings.search_paths.empty()) {
		database->SetAdditionalSearchPaths(settings.search_paths);
		database->SetOption("additional_profile_search_path_list", settings.search_paths);
	}

	// Set the connection profile to resolve (the driver and options come from it)
	if (!settings.profile.empty()) {
		database->SetOption("profile", settings.profile);
	}

	// Set driver if provided (optional when a profile supplies it)
	if (!settings.driver.empty()) {
		database->SetOption("driver", settings.driver);
		database->SetDriverName(settings.driver);
	}

	// Set entrypoint if provided
	if (!settings.entrypoint.empty()) {
		database->SetOption("entrypoint", settings.entrypoint);
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
	return database;
}

// Percent-encode everything but RFC 3986 unreserved characters.
static string PercentEncode(const string &value) {
	static const char *hex = "0123456789ABCDEF";
	string result;
	for (unsigned char c : value) {
		if (StringUtil::CharacterIsAlphaNumeric(static_cast<char>(c)) || c == '-' || c == '.' || c == '_' ||
		    c == '~') {
			result += static_cast<char>(c);
		} else {
			result += '%';
			result += hex[c >> 4];
			result += hex[c & 0xF];
		}
	}
	return result;
}

// Drivers such as PostgreSQL and SQLite accept only a 'uri' database option and
// reject the standard 'username' / 'password' options (and the secret's
// 'database'). Rewrite them into a scheme://host URI as userinfo and path.
// Returns false when there is nothing to fold or the URI cannot take it.
static bool FoldCredentialsIntoUri(const string &uri, const AdbcOptions &db_options, string &folded_uri,
                                   AdbcOptions &remaining) {
	string username, password, database;
	bool has_username = false, has_password = false, has_database = false;
	remaining.clear();
	for (const auto &opt : db_options) {
		if (opt.first == "username") {
			username = opt.second.ToString();
			has_username = true;
		} else if (opt.first == "password") {
			password = opt.second.ToString();
			has_password = true;
		} else if (opt.first == "database") {
			database = opt.second.ToString();
			has_database = true;
		} else {
			remaining.push_back(opt);
		}
	}
	if (!has_username && !has_password && !has_database) {
		return false;
	}

	auto scheme_end = uri.find("://");
	if (scheme_end == string::npos || scheme_end == 0 || StringUtil::StartsWith(uri, "profile://")) {
		return false;
	}
	auto authority_start = scheme_end + 3;
	auto authority_end = uri.find_first_of("/?#", authority_start);
	if (authority_end == string::npos) {
		authority_end = uri.size();
	}
	auto authority = uri.substr(authority_start, authority_end - authority_start);
	auto rest = uri.substr(authority_end);

	if (has_username || has_password) {
		// Credentials already in the URI are ambiguous to merge with.
		if (authority.find('@') != string::npos) {
			return false;
		}
		string userinfo = PercentEncode(username);
		if (has_password) {
			userinfo += ":" + PercentEncode(password);
		}
		authority = userinfo + "@" + authority;
	}

	if (has_database) {
		// Only fill an empty path; a URI that already names a database wins.
		auto path_end = rest.find_first_of("?#");
		auto path = rest.substr(0, path_end == string::npos ? rest.size() : path_end);
		if (!path.empty() && path != "/") {
			return false;
		}
		rest = "/" + PercentEncode(database) + rest.substr(path.size());
	}

	folded_uri = uri.substr(0, authority_start) + authority + rest;
	return true;
}

shared_ptr<AdbcConnectionWrapper> CreateConnectionFromOptions(const AdbcOptions &options) {
	DatabaseSettings settings;
	string uri;
	AdbcOptions db_options;

	for (const auto &opt : options) {
		if (opt.first == "driver") {
			settings.driver = opt.second.GetValue<string>();
		} else if (opt.first == "entrypoint") {
			settings.entrypoint = opt.second.GetValue<string>();
		} else if (opt.first == "uri") {
			uri = opt.second.GetValue<string>();
		} else if (opt.first == "search_paths") {
			settings.search_paths = opt.second.GetValue<string>();
		} else if (opt.first == "profile") {
			settings.profile = opt.second.GetValue<string>();
		} else if (opt.first == "use_manifests") {
			settings.use_manifests = (opt.second.ToString() == "true" || opt.second.ToString() == "1");
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
	const bool has_profile = !settings.profile.empty() || StringUtil::StartsWith(uri, "profile://");

	// Validate required options
	if (settings.driver.empty() && !has_profile) {
		throw InvalidInputException(
		    "ADBC connection requires a 'driver' option (or a 'profile' option / 'profile://' URI)");
	}

	shared_ptr<AdbcDatabaseWrapper> database;
	try {
		database = OpenDatabase(settings, uri, db_options);
	} catch (NotImplementedException &) {
		// The driver rejected an option. If it was a credential the URI can
		// carry, retry once with it moved into the URI.
		string folded_uri;
		AdbcOptions remaining;
		if (!FoldCredentialsIntoUri(uri, db_options, folded_uri, remaining)) {
			throw;
		}
		database = OpenDatabase(settings, folded_uri, remaining);
	}

	// Create connection wrapper
	auto connection = make_shared_ptr<AdbcConnectionWrapper>(database);
	connection->Init();
	connection->Initialize();

	return connection;
}

} // namespace adbc_scanner
