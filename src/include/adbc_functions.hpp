#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace adbc {

// Register scalar functions (adbc_connect, adbc_disconnect)
void RegisterAdbcScalarFunctions(DatabaseInstance &db);

// Register table functions (adbc_scan, adbc_query) - to be implemented in Phase 2
void RegisterAdbcTableFunctions(DatabaseInstance &db);

} // namespace adbc
} // namespace duckdb
