#pragma once

#include "duckdb.hpp"
#include "adbc_options.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace adbc_scanner {
using namespace duckdb;

// Get a secret by name, checking both memory and local_file storage
unique_ptr<SecretEntry> AdbcGetSecretByName(ClientContext &context, const string &secret_name);

// Get a secret by URI scope (e.g., "postgresql://host:5432/db")
SecretMatch AdbcGetSecretByUri(ClientContext &context, const string &uri);

// Merge secret options with explicitly provided options
// Returns combined options with explicit options taking precedence
AdbcOptions MergeSecretOptions(ClientContext &context, const AdbcOptions &explicit_options);

// Register the ADBC secret type and create secret function
void RegisterAdbcSecrets(ExtensionLoader &loader);

} // namespace adbc_scanner
