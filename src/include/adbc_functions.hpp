#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace adbc {

// Register scalar functions (adbc_connect, adbc_disconnect)
void RegisterAdbcScalarFunctions(DatabaseInstance &db);

// Register table functions (adbc_scan)
void RegisterAdbcTableFunctions(DatabaseInstance &db);

// Register catalog functions (adbc_info, adbc_tables)
void RegisterAdbcCatalogFunctions(DatabaseInstance &db);

} // namespace adbc
} // namespace duckdb
