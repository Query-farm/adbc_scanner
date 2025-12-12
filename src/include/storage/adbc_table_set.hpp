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
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace adbc_scanner {
using namespace duckdb;

// Forward declaration for type defined in this namespace
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

} // namespace adbc_scanner
