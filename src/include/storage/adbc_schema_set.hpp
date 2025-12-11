//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/adbc_schema_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/adbc_catalog_set.hpp"
#include "storage/adbc_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;

class AdbcSchemaSet : public AdbcCatalogSet {
public:
	explicit AdbcSchemaSet(Catalog &catalog);

public:
	optional_ptr<CatalogEntry> CreateSchema(AdbcTransaction &transaction, CreateSchemaInfo &info);

protected:
	void LoadEntries(AdbcTransaction &transaction) override;
};

} // namespace duckdb
