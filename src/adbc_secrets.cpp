#include "adbc_secrets.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/string_util.hpp"

namespace adbc_scanner {
using namespace duckdb;

unique_ptr<SecretEntry> AdbcGetSecretByName(ClientContext &context, const string &secret_name) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

	// Try memory storage first
	auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "memory");
	if (secret_entry) {
		return secret_entry;
	}

	// Fall back to local_file storage
	secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "local_file");
	if (secret_entry) {
		return secret_entry;
	}

	return nullptr;
}

SecretMatch AdbcGetSecretByUri(ClientContext &context, const string &uri) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	return secret_manager.LookupSecret(transaction, uri, "adbc");
}

vector<pair<string, string>> MergeSecretOptions(ClientContext &context,
                                                 const vector<pair<string, string>> &explicit_options) {
	// First, check if there's a uri option to use as scope for secret lookup
	string uri;
	string secret_name;

	for (const auto &opt : explicit_options) {
		if (opt.first == "uri") {
			uri = opt.second;
		} else if (opt.first == "secret") {
			secret_name = opt.second;
		}
	}

	vector<pair<string, string>> merged_options;
	bool found_secret = false;

	// If a secret name is explicitly provided, use that
	if (!secret_name.empty()) {
		auto secret_entry = AdbcGetSecretByName(context, secret_name);
		if (!secret_entry) {
			throw BinderException("Secret with name \"%s\" not found", secret_name);
		}

		const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

		// Add all secret options first
		for (const auto &entry : kv_secret.secret_map) {
			if (!entry.second.IsNull()) {
				merged_options.emplace_back(entry.first, entry.second.ToString());
			}
		}
		found_secret = true;
	}
	// Otherwise, try to find a secret by URI scope
	else if (!uri.empty()) {
		auto secret_match = AdbcGetSecretByUri(context, uri);
		if (secret_match.HasMatch()) {
			const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(secret_match.GetSecret());

			// Add all secret options first
			for (const auto &entry : kv_secret.secret_map) {
				if (!entry.second.IsNull()) {
					merged_options.emplace_back(entry.first, entry.second.ToString());
				}
			}
			found_secret = true;
		}
	}

	// Build set of keys that were explicitly provided (these override secret values)
	unordered_set<string> explicit_keys;
	bool used_uri_for_scope_lookup = !secret_name.empty() ? false : found_secret;

	for (const auto &opt : explicit_options) {
		// Skip the 'secret' option itself - it's not passed to ADBC
		if (opt.first == "secret") {
			continue;
		}
		// If we found a secret by URI scope lookup (not by name), and the secret
		// already has a 'uri' option, skip the explicit 'uri' (it was just for lookup)
		if (opt.first == "uri" && used_uri_for_scope_lookup) {
			bool secret_has_uri = false;
			for (const auto &merged : merged_options) {
				if (merged.first == "uri") {
					secret_has_uri = true;
					break;
				}
			}
			if (secret_has_uri) {
				continue;
			}
		}
		explicit_keys.insert(opt.first);
	}

	// Filter out secret options that are overridden by explicit options
	vector<pair<string, string>> result;
	for (const auto &opt : merged_options) {
		if (explicit_keys.find(opt.first) == explicit_keys.end()) {
			result.push_back(opt);
		}
	}

	// Add all explicit options (except 'secret' and 'uri' when used for scope lookup only)
	for (const auto &opt : explicit_options) {
		if (explicit_keys.find(opt.first) != explicit_keys.end()) {
			result.push_back(opt);
		}
	}

	return result;
}

// Create secret function
static unique_ptr<BaseSecret> CreateAdbcSecretFunction(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;

	// Scope is required and should be a URI pattern
	if (scope.empty()) {
		throw InvalidInputException(
		    "ADBC secret requires a SCOPE (e.g., SCOPE 'postgresql://host:5432')");
	}

	auto result = make_uniq<KeyValueSecret>(scope, "adbc", "config", input.name);

	// Process known named parameters
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "driver") {
			result->secret_map["driver"] = named_param.second.ToString();
		} else if (lower_name == "uri") {
			result->secret_map["uri"] = named_param.second.ToString();
		} else if (lower_name == "username") {
			result->secret_map["username"] = named_param.second.ToString();
		} else if (lower_name == "password") {
			result->secret_map["password"] = named_param.second.ToString();
		} else if (lower_name == "database") {
			result->secret_map["database"] = named_param.second.ToString();
		} else if (lower_name == "entrypoint") {
			result->secret_map["entrypoint"] = named_param.second.ToString();
		} else if (lower_name == "extra_options") {
			// extra_options is a MAP of string -> string for driver-specific options
			auto &map_value = named_param.second;
			if (map_value.type().id() == LogicalTypeId::MAP) {
				auto &map_children = MapValue::GetChildren(map_value);
				for (auto &entry : map_children) {
					auto &entry_children = StructValue::GetChildren(entry);
					if (entry_children.size() == 2 && !entry_children[0].IsNull()) {
						auto key = entry_children[0].ToString();
						auto val = entry_children[1].IsNull() ? "" : entry_children[1].ToString();
						result->secret_map[key] = val;
					}
				}
			}
		}
	}

	// Redact sensitive keys by default
	result->redact_keys = {"password", "auth_token", "token", "secret", "api_key", "apikey", "credential"};

	return result;
}

void RegisterAdbcSecrets(ExtensionLoader &loader) {
	// Register the secret type
	SecretType secret_type;
	secret_type.name = "adbc";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	loader.RegisterSecretType(secret_type);

	// Register the create secret function with known parameters
	CreateSecretFunction adbc_secret_function = {"adbc", "config", CreateAdbcSecretFunction};

	// Common ADBC connection parameters
	adbc_secret_function.named_parameters["driver"] = LogicalType::VARCHAR;
	adbc_secret_function.named_parameters["uri"] = LogicalType::VARCHAR;
	adbc_secret_function.named_parameters["username"] = LogicalType::VARCHAR;
	adbc_secret_function.named_parameters["password"] = LogicalType::VARCHAR;
	adbc_secret_function.named_parameters["database"] = LogicalType::VARCHAR;
	adbc_secret_function.named_parameters["entrypoint"] = LogicalType::VARCHAR;

	// extra_options allows arbitrary driver-specific key-value pairs
	adbc_secret_function.named_parameters["extra_options"] =
	    LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);

	loader.RegisterFunction(adbc_secret_function);
}

} // namespace adbc_scanner
