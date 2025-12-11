//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/adbc_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class AdbcStorageExtension : public StorageExtension {
public:
	AdbcStorageExtension();
};

} // namespace duckdb
