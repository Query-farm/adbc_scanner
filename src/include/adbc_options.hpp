#pragma once

#include "duckdb.hpp"

namespace adbc_scanner {
using AdbcOptions = duckdb::vector<duckdb::pair<duckdb::string, duckdb::Value>>;
}
