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
#include "adbc_connection.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace adbc_scanner {
using namespace duckdb;

class AdbcSchemaSet : public AdbcCatalogSet {
public:
	explicit AdbcSchemaSet(Catalog &catalog);

public:
	optional_ptr<CatalogEntry> CreateSchema(AdbcTransaction &transaction, CreateSchemaInfo &info);

	//! The schema names the driver lists for this connection. A driver that lists
	//! none (or cannot list them) gets the single placeholder schema "main".
	static vector<string> ListSchemaNames(AdbcConnectionWrapper &connection);

protected:
	void LoadEntries(AdbcTransaction &transaction) override;
};

} // namespace adbc_scanner
