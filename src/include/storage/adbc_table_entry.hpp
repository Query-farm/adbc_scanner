//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/adbc_table_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

struct AdbcTableInfo {
	AdbcTableInfo() {
		create_info = make_uniq<CreateTableInfo>();
		create_info->columns.SetAllowDuplicates(true);
	}
	AdbcTableInfo(const string &schema, const string &table) {
		create_info = make_uniq<CreateTableInfo>(string(), schema, table);
		create_info->columns.SetAllowDuplicates(true);
	}
	AdbcTableInfo(const SchemaCatalogEntry &schema, const string &table) {
		create_info = make_uniq<CreateTableInfo>((SchemaCatalogEntry &)schema, table);
		create_info->columns.SetAllowDuplicates(true);
	}

	const string &GetTableName() const {
		return create_info->table;
	}

	unique_ptr<CreateTableInfo> create_info;
	//! Column names as they are within the remote database
	vector<string> column_names;
};

class AdbcTableEntry : public TableCatalogEntry {
public:
	AdbcTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);
	AdbcTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, AdbcTableInfo &info);

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;

public:
	//! Column names as they are within the remote database
	vector<string> column_names;
};

} // namespace duckdb
