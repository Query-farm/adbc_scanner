#include "storage/adbc_catalog.hpp"
#include "storage/adbc_schema_entry.hpp"
#include "storage/adbc_transaction.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

namespace duckdb {

AdbcCatalog::AdbcCatalog(AttachedDatabase &db_p, shared_ptr<adbc::AdbcConnectionWrapper> connection_p,
                         const string &path, AccessMode access_mode_p)
    : Catalog(db_p), connection(std::move(connection_p)), attach_path(path),
      access_mode(access_mode_p), schemas(*this) {
	// Register the connection in the ConnectionRegistry so adbc_scan_table can use it
	auto &registry = adbc::ConnectionRegistry::Get();
	connection_handle = registry.Add(connection);

	// Try to determine default schema from the connection
	// For most databases, "main" is a reasonable default
	default_schema = "main";
}

AdbcCatalog::~AdbcCatalog() {
	// Remove the connection from the registry when the catalog is destroyed
	auto &registry = adbc::ConnectionRegistry::Get();
	registry.Remove(connection_handle);
}

void AdbcCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> AdbcCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	auto &adbc_transaction = AdbcTransaction::Get(transaction.GetContext(), *this);

	// Check if schema already exists
	auto existing = schemas.GetEntry(adbc_transaction, info.schema);
	if (existing) {
		switch (info.on_conflict) {
		case OnCreateConflict::REPLACE_ON_CONFLICT:
			// Drop and recreate - not typically supported, throw error
			throw BinderException("ADBC databases do not support replacing schemas");
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			return nullptr;
		case OnCreateConflict::ERROR_ON_CONFLICT:
		default:
			throw BinderException("Schema with name \"%s\" already exists", info.schema);
		}
	}

	return schemas.CreateSchema(adbc_transaction, info);
}

void AdbcCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw BinderException("ADBC databases do not support dropping schemas");
}

void AdbcCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	auto &adbc_transaction = AdbcTransaction::Get(context, *this);
	schemas.Scan(adbc_transaction, [&](CatalogEntry &schema) { callback(schema.Cast<AdbcSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> AdbcCatalog::LookupSchema(CatalogTransaction transaction,
                                                           const EntryLookupInfo &schema_lookup,
                                                           OnEntryNotFound if_not_found) {
	auto schema_name = schema_lookup.GetEntryName();
	auto &adbc_transaction = AdbcTransaction::Get(transaction.GetContext(), *this);
	auto entry = schemas.GetEntry(adbc_transaction, schema_name);
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	}
	return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
}

bool AdbcCatalog::InMemory() {
	return false;
}

string AdbcCatalog::GetDBPath() {
	return attach_path;
}

DatabaseSize AdbcCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	size.free_blocks = 0;
	size.total_blocks = 0;
	size.used_blocks = 0;
	size.wal_size = 0;
	size.block_size = 0;
	size.bytes = 0;
	return size;
}

void AdbcCatalog::ClearCache() {
	schemas.ClearEntries();
}

PhysicalOperator &AdbcCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                  LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("ADBC databases do not support CREATE TABLE AS");
}

PhysicalOperator &AdbcCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                           optional_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("ADBC databases do not support INSERT");
}

PhysicalOperator &AdbcCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                           PhysicalOperator &plan) {
	throw NotImplementedException("ADBC databases do not support DELETE");
}

PhysicalOperator &AdbcCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                           PhysicalOperator &plan) {
	throw NotImplementedException("ADBC databases do not support UPDATE");
}

} // namespace duckdb
