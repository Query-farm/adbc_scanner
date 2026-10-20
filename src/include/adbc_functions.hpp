#pragma once

#include "duckdb.hpp"

namespace adbc_scanner {
using namespace duckdb;


// Register table functions (adbc_scan)
void RegisterAdbcTableFunctions(DatabaseInstance &db);

class AdbcConnectionWrapper;
// Bind adbc_scan_table over an already-resolved connection (ATTACH scans)
unique_ptr<FunctionData> AdbcScanTableBindWithConnection(ClientContext &context, TableFunctionBindInput &input,
                                                         shared_ptr<AdbcConnectionWrapper> connection,
                                                         vector<LogicalType> &return_types, vector<string> &names);

// Register catalog functions (adbc_info, adbc_tables)
void RegisterAdbcCatalogFunctions(DatabaseInstance &db);

// Register execute function (adbc_execute for DDL/DML)
void RegisterAdbcExecuteFunction(DatabaseInstance &db);

// Register insert function (adbc_insert for bulk ingestion)
void RegisterAdbcInsertFunction(DatabaseInstance &db);

// Register adbc_clear_cache scalar function
void RegisterAdbcClearCacheFunction(DatabaseInstance &db);

// Register adbc_profiles table function (enumerate connection profiles)
void RegisterAdbcProfilesFunction(DatabaseInstance &db);

} // namespace adbc_scanner
