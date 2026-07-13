#include "storage/adbc_catalog.hpp"
#include "storage/adbc_table_entry.hpp"
#include "storage/adbc_transaction.hpp"
#include "storage/adbc_schema_entry.hpp"
#include "adbc_connection.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/main/database.hpp"

namespace adbc_scanner {
using namespace duckdb;

AdbcTableEntry::AdbcTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
	for (idx_t c = 0; c < columns.LogicalColumnCount(); c++) {
		auto &col = columns.GetColumnMutable(LogicalIndex(c));
		column_names.push_back(col.GetName());
	}
}

AdbcTableEntry::AdbcTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, AdbcTableInfo &info)
    : TableCatalogEntry(catalog, schema, *info.create_info), column_names(std::move(info.column_names)) {
}

unique_ptr<BaseStatistics> AdbcTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void AdbcTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                           ClientContext &) {
}

//===--------------------------------------------------------------------===//
// Helper to look up a table function from the system catalog
//===--------------------------------------------------------------------===//

static TableFunctionCatalogEntry &GetTableFunction(DatabaseInstance &db, const string &name) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto entry = schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, name);
	if (!entry) {
		throw InvalidInputException("Function with name \"%s\" not found", name);
	}
	return entry->Cast<TableFunctionCatalogEntry>();
}

//===--------------------------------------------------------------------===//
// GetScanFunction - Return the table function for scanning this table
//===--------------------------------------------------------------------===//

TableFunction AdbcTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
	auto &db = DatabaseInstance::GetDatabase(context);

	// Lease a connection from the pool for this scan so concurrent scans (joins
	// across attached tables, or parallel queries) never share one ADBC connection
	// — ADBC connections are not safe for concurrent statement execution. The
	// connection is registered under a temporary handle so the existing
	// adbc_scan_table bind can resolve it; the scan's bind data then holds a strong
	// reference, keeping it alive for the whole query and returning it to the pool
	// when the query (and its bind data) is destroyed.
	auto scan_connection = adbc_catalog.GetPool().GetConnectionShared();
	auto &registry = ConnectionRegistry::Get();
	auto temp_handle = registry.Add(scan_connection);

	// Look up adbc_scan_table from the catalog
	auto &adbc_scan_table_function_set = GetTableFunction(db, "adbc_scan_table");
	auto adbc_scan_table_function = adbc_scan_table_function_set.functions.GetFunctionByArguments(
	    context,
	    {LogicalType::BIGINT, LogicalType::VARCHAR});

	// Build the inputs: temp connection handle, table_name.
	// NOTE: use the Value(string) VARCHAR constructor, NOT Value::CreateValue(name):
	// the CreateValue(string) overload returns Value::BLOB(...), so a table or schema
	// name with any non-ASCII byte (e.g. "naïve_café") throws "Invalid byte encountered
	// in STRING -> BLOB conversion" before the scan even runs. adbc_scan_table expects a
	// VARCHAR table_name argument.
	vector<Value> inputs = {
	    Value::BIGINT(temp_handle),
	    Value(name)
	};

	// Set up named parameters for schema (if not "main") and batch_size (if set)
	named_parameter_map_t param_map;
	if (schema.name != "main") {
		param_map["schema"] = Value(schema.name);
	}
	if (adbc_catalog.batch_size > 0) {
		param_map["batch_size"] = Value::BIGINT(static_cast<int64_t>(adbc_catalog.batch_size));
	}

	vector<LogicalType> return_types;
	vector<string> names;
	TableFunctionRef empty_ref;

	TableFunctionBindInput bind_input(inputs,
	                                  param_map,
	                                  return_types,
	                                  names,
	                                  nullptr,
	                                  nullptr,
	                                  adbc_scan_table_function,
	                                  empty_ref);

	unique_ptr<FunctionData> result;
	try {
		result = adbc_scan_table_function.bind(context, bind_input, return_types, names);
	} catch (...) {
		// Bind failed: drop the temporary registry entry so the leased connection
		// (now only referenced by the local shared_ptr) returns to the pool.
		registry.Remove(temp_handle);
		throw;
	}
	bind_data = std::move(result);

	// The scan's bind data now holds its own strong reference to the leased
	// connection; remove the temporary registry entry. This does not free the
	// connection — bind_data keeps it alive until the query ends, at which point
	// the pool reclaims it.
	registry.Remove(temp_handle);

	return adbc_scan_table_function;
}

TableStorageInfo AdbcTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	result.cardinality = 0;
	return result;
}

} // namespace adbc_scanner
