#define DUCKDB_EXTENSION_MAIN

#include "adbc_scanner_extension.hpp"
#include "adbc_functions.hpp"
#include "adbc_secrets.hpp"
#include "storage/adbc_storage.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/config.hpp"
#include "query_farm_telemetry.hpp"

namespace adbc_scanner {

static void LoadInternal(duckdb::ExtensionLoader &loader) {
	// Register ADBC secret type and create secret function
	RegisterAdbcSecrets(loader);

	// Register ADBC scalar functions (adbc_connect, adbc_disconnect)
	RegisterAdbcScalarFunctions(loader.GetDatabaseInstance());

	// Register ADBC table functions (adbc_scan)
	RegisterAdbcTableFunctions(loader.GetDatabaseInstance());

	// Register ADBC catalog functions (adbc_info, adbc_tables)
	RegisterAdbcCatalogFunctions(loader.GetDatabaseInstance());

	// Register ADBC execute function (adbc_execute for DDL/DML)
	RegisterAdbcExecuteFunction(loader.GetDatabaseInstance());

	// Register ADBC insert function (adbc_insert for bulk ingestion)
	RegisterAdbcInsertFunction(loader.GetDatabaseInstance());

	// Register ADBC clear cache function
	RegisterAdbcClearCacheFunction(loader.GetDatabaseInstance());

	// Register ADBC storage extension for ATTACH support
	auto &config = duckdb::DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.storage_extensions["adbc"] = duckdb::make_uniq<AdbcStorageExtension>();

	QueryFarmSendTelemetry(loader, "adbc", "2025120801");
}

} // namespace adbc_scanner

namespace duckdb {

void AdbcScannerExtension::Load(ExtensionLoader &loader) {
	adbc_scanner::LoadInternal(loader);
}

std::string AdbcScannerExtension::Name() {
	return "adbc";
}

std::string AdbcScannerExtension::Version() const {
	return "2025120801";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(adbc_scanner, loader) {
	adbc_scanner::LoadInternal(loader);
}
}
