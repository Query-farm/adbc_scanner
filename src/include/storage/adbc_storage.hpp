//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/adbc_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace adbc_scanner {
using namespace duckdb;

class AdbcStorageExtension : public StorageExtension {
public:
	AdbcStorageExtension();
};

} // namespace adbc_scanner
