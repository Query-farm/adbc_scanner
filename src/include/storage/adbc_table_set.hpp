//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/adbc_table_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/adbc_catalog_set.hpp"
#include "storage/adbc_table_entry.hpp"

namespace duckdb {
struct CreateTableInfo;
class AdbcSchemaEntry;

class AdbcTableSet : public AdbcInSchemaSet {
public:
	explicit AdbcTableSet(AdbcSchemaEntry &schema);

public:
	optional_ptr<CatalogEntry> CreateTable(AdbcTransaction &transaction, BoundCreateTableInfo &info);

	static unique_ptr<AdbcTableInfo> GetTableInfo(AdbcTransaction &transaction, AdbcSchemaEntry &schema,
	                                              const string &table_name);

protected:
	void LoadEntries(AdbcTransaction &transaction) override;
};

} // namespace duckdb
