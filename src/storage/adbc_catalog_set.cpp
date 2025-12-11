#include "storage/adbc_catalog_set.hpp"
#include "storage/adbc_schema_entry.hpp"
#include "storage/adbc_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace duckdb {

AdbcCatalogSet::AdbcCatalogSet(Catalog &catalog, bool is_loaded_p)
    : catalog(catalog), is_loaded(is_loaded_p) {
}

void AdbcCatalogSet::TryLoadEntries(AdbcTransaction &transaction) {
	if (is_loaded) {
		return;
	}
	lock_guard<mutex> l(load_lock);
	if (is_loaded) {
		return;
	}
	LoadEntries(transaction);
	is_loaded = true;
}

optional_ptr<CatalogEntry> AdbcCatalogSet::GetEntry(AdbcTransaction &transaction, const string &name) {
	TryLoadEntries(transaction);

	lock_guard<mutex> l(entry_lock);
	auto entry = entry_map.find(name);
	if (entry == entry_map.end()) {
		return nullptr;
	}
	auto it = entries.find(entry->second);
	if (it == entries.end()) {
		return nullptr;
	}
	return it->second.get();
}

void AdbcCatalogSet::DropEntry(AdbcTransaction &transaction, DropInfo &info) {
	throw BinderException("ADBC databases do not support dropping entries");
}

void AdbcCatalogSet::Scan(AdbcTransaction &transaction, const std::function<void(CatalogEntry &)> &callback) {
	TryLoadEntries(transaction);

	lock_guard<mutex> l(entry_lock);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> AdbcCatalogSet::CreateEntry(AdbcTransaction &transaction, shared_ptr<CatalogEntry> entry) {
	lock_guard<mutex> l(entry_lock);
	auto name = entry->name;
	entries[name] = std::move(entry);
	entry_map[name] = name;
	return entries[name].get();
}

void AdbcCatalogSet::ClearEntries() {
	lock_guard<mutex> l(entry_lock);
	entries.clear();
	entry_map.clear();
	is_loaded = false;
}

AdbcInSchemaSet::AdbcInSchemaSet(AdbcSchemaEntry &schema, bool is_loaded)
    : AdbcCatalogSet(schema.ParentCatalog(), is_loaded), schema(schema) {
}

optional_ptr<CatalogEntry> AdbcInSchemaSet::CreateEntry(AdbcTransaction &transaction, shared_ptr<CatalogEntry> entry) {
	return AdbcCatalogSet::CreateEntry(transaction, std::move(entry));
}

} // namespace duckdb
