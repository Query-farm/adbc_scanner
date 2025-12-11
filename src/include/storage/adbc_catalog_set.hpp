//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/adbc_catalog_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {
struct DropInfo;
class AdbcSchemaEntry;
class AdbcTransaction;

class AdbcCatalogSet {
public:
	AdbcCatalogSet(Catalog &catalog, bool is_loaded);

	optional_ptr<CatalogEntry> GetEntry(AdbcTransaction &transaction, const string &name);
	void DropEntry(AdbcTransaction &transaction, DropInfo &info);
	void Scan(AdbcTransaction &transaction, const std::function<void(CatalogEntry &)> &callback);
	virtual optional_ptr<CatalogEntry> CreateEntry(AdbcTransaction &transaction, shared_ptr<CatalogEntry> entry);
	void ClearEntries();

protected:
	virtual void LoadEntries(AdbcTransaction &transaction) = 0;
	void TryLoadEntries(AdbcTransaction &transaction);

protected:
	Catalog &catalog;

private:
	mutex entry_lock;
	mutex load_lock;
	unordered_map<string, shared_ptr<CatalogEntry>> entries;
	case_insensitive_map_t<string> entry_map;
	atomic<bool> is_loaded;
};

class AdbcInSchemaSet : public AdbcCatalogSet {
public:
	AdbcInSchemaSet(AdbcSchemaEntry &schema, bool is_loaded);

	optional_ptr<CatalogEntry> CreateEntry(AdbcTransaction &transaction, shared_ptr<CatalogEntry> entry) override;

protected:
	AdbcSchemaEntry &schema;
};

} // namespace duckdb
