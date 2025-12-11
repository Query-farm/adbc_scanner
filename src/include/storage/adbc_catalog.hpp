//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/adbc_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "adbc_connection.hpp"
#include "storage/adbc_schema_set.hpp"

namespace duckdb {
class AdbcCatalog;
class AdbcSchemaEntry;
class PhysicalPlanGenerator;
class LogicalCreateTable;
class LogicalInsert;
class LogicalDelete;
class LogicalUpdate;

class AdbcCatalog : public Catalog {
public:
	explicit AdbcCatalog(AttachedDatabase &db_p, shared_ptr<adbc::AdbcConnectionWrapper> connection,
	                     const string &path, AccessMode access_mode);
	~AdbcCatalog();

	//! The ADBC connection (shared ownership with the catalog)
	shared_ptr<adbc::AdbcConnectionWrapper> connection;
	//! The connection handle registered in the ConnectionRegistry
	int64_t connection_handle;
	//! The attach path (driver info)
	string attach_path;
	//! Access mode (read-only or read-write)
	AccessMode access_mode;
	//! Optional batch size for scan operations (0 means use driver default)
	idx_t batch_size = 0;

public:
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "adbc";
	}
	string GetDefaultSchema() const override {
		return default_schema.empty() ? "main" : default_schema;
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	//! Whether or not this is an in-memory database
	bool InMemory() override;
	string GetDBPath() override;

	shared_ptr<adbc::AdbcConnectionWrapper> GetConnection() {
		return connection;
	}

	void ClearCache();

	//! Whether or not this catalog should search a specific type with the standard priority
	CatalogLookupBehavior CatalogTypeLookupRule(CatalogType type) const override {
		switch (type) {
		case CatalogType::TABLE_ENTRY:
		case CatalogType::VIEW_ENTRY:
			return CatalogLookupBehavior::STANDARD;
		default:
			return CatalogLookupBehavior::NEVER_LOOKUP;
		}
	}

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

private:
	AdbcSchemaSet schemas;
	string default_schema;
};

} // namespace duckdb
