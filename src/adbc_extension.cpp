#define DUCKDB_EXTENSION_MAIN

#include "adbc_extension.hpp"
#include "adbc_functions.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
    // Register ADBC scalar functions (adbc_connect, adbc_disconnect)
    adbc::RegisterAdbcScalarFunctions(loader.GetDatabaseInstance());

    // Register ADBC table functions (adbc_scan)
    adbc::RegisterAdbcTableFunctions(loader.GetDatabaseInstance());

    // Register ADBC catalog functions (adbc_info, adbc_tables)
    adbc::RegisterAdbcCatalogFunctions(loader.GetDatabaseInstance());

    // Register ADBC execute function (adbc_execute for DDL/DML)
    adbc::RegisterAdbcExecuteFunction(loader.GetDatabaseInstance());
}

void AdbcExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}

std::string AdbcExtension::Name() {
    return "adbc";
}

std::string AdbcExtension::Version() const {
#ifdef EXT_VERSION_ADBC
    return EXT_VERSION_ADBC;
#else
    return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(adbc, loader) {
    duckdb::LoadInternal(loader);
}
}
