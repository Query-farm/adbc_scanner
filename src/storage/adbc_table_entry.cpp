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

	// Look up adbc_scan_table from the catalog
	auto &adbc_scan_table_function_set = GetTableFunction(db, "adbc_scan_table");
	auto adbc_scan_table_function = adbc_scan_table_function_set.functions.GetFunctionByArguments(
	    context,
	    {LogicalType::BIGINT, LogicalType::VARCHAR});

	// Build the inputs: connection_handle, table_name
	vector<Value> inputs = {
	    Value::BIGINT(adbc_catalog.connection_handle),
	    Value::CreateValue(name)
	};

	// Set up named parameters for schema (if not "main") and batch_size (if set)
	named_parameter_map_t param_map;
	if (schema.name != "main") {
		param_map["schema"] = Value::CreateValue(schema.name);
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

	auto result = adbc_scan_table_function.bind(context, bind_input, return_types, names);
	bind_data = std::move(result);

	return adbc_scan_table_function;
}

TableStorageInfo AdbcTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	result.cardinality = 0;
	return result;
}

} // namespace adbc_scanner
