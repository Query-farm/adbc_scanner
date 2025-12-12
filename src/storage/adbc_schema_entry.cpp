#include "storage/adbc_schema_entry.hpp"
#include "storage/adbc_table_entry.hpp"
#include "storage/adbc_transaction.hpp"
#include "storage/adbc_catalog.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace adbc_scanner {
using namespace duckdb;

AdbcSchemaEntry::AdbcSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), tables(*this) {
}

AdbcTransaction &GetAdbcTransaction(CatalogTransaction transaction) {
	if (!transaction.transaction) {
		throw InternalException("No transaction!?");
	}
	return transaction.transaction->Cast<AdbcTransaction>();
}

optional_ptr<CatalogEntry> AdbcSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	throw BinderException("ADBC databases do not support creating tables through DDL");
}

optional_ptr<CatalogEntry> AdbcSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw BinderException("ADBC databases do not support creating functions");
}

optional_ptr<CatalogEntry> AdbcSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                        TableCatalogEntry &table) {
	throw BinderException("ADBC databases do not support creating indexes");
}

optional_ptr<CatalogEntry> AdbcSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw BinderException("ADBC databases do not support creating views");
}

optional_ptr<CatalogEntry> AdbcSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw BinderException("ADBC databases do not support creating sequences");
}

optional_ptr<CatalogEntry> AdbcSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                CreateTableFunctionInfo &info) {
	throw BinderException("ADBC databases do not support creating table functions");
}

optional_ptr<CatalogEntry> AdbcSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                               CreateCopyFunctionInfo &info) {
	throw BinderException("ADBC databases do not support creating copy functions");
}

optional_ptr<CatalogEntry> AdbcSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                 CreatePragmaFunctionInfo &info) {
	throw BinderException("ADBC databases do not support creating pragma functions");
}

optional_ptr<CatalogEntry> AdbcSchemaEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw BinderException("ADBC databases do not support creating collations");
}

optional_ptr<CatalogEntry> AdbcSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("ADBC databases do not support creating types");
}

void AdbcSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw BinderException("ADBC databases do not support ALTER operations");
}

bool CatalogTypeIsSupported(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return true;
	default:
		return false;
	}
}

void AdbcSchemaEntry::Scan(ClientContext &context, CatalogType type,
                           const std::function<void(CatalogEntry &)> &callback) {
	if (!CatalogTypeIsSupported(type)) {
		return;
	}
	auto &adbc_transaction = AdbcTransaction::Get(context, catalog);
	tables.Scan(adbc_transaction, callback);
}

void AdbcSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without context not supported");
}

void AdbcSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	throw BinderException("ADBC databases do not support dropping entries");
}

optional_ptr<CatalogEntry> AdbcSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                        const EntryLookupInfo &lookup_info) {
	auto catalog_type = lookup_info.GetCatalogType();
	if (!CatalogTypeIsSupported(catalog_type)) {
		return nullptr;
	}
	auto &adbc_transaction = GetAdbcTransaction(transaction);
	return tables.GetEntry(adbc_transaction, lookup_info.GetEntryName());
}

} // namespace adbc_scanner
