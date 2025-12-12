#pragma once

#include "duckdb.hpp"

namespace adbc_scanner {
using namespace duckdb;

// Register scalar functions (adbc_connect, adbc_disconnect)
void RegisterAdbcScalarFunctions(DatabaseInstance &db);

// Register table functions (adbc_scan)
void RegisterAdbcTableFunctions(DatabaseInstance &db);

// Register catalog functions (adbc_info, adbc_tables)
void RegisterAdbcCatalogFunctions(DatabaseInstance &db);

// Register execute function (adbc_execute for DDL/DML)
void RegisterAdbcExecuteFunction(DatabaseInstance &db);

// Register insert function (adbc_insert for bulk ingestion)
void RegisterAdbcInsertFunction(DatabaseInstance &db);

// Register adbc_clear_cache scalar function
void RegisterAdbcClearCacheFunction(DatabaseInstance &db);

} // namespace adbc_scanner
