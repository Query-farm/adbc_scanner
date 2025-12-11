#include "storage/adbc_catalog.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// adbc_clear_cache - Clear cached schemas/tables for all ADBC catalogs
//===--------------------------------------------------------------------===//

static void AdbcClearCacheFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();

	auto &db_manager = DatabaseManager::Get(context);
	auto databases = db_manager.GetDatabases(context);

	idx_t cleared_count = 0;

	for (auto db : databases) {
		auto &catalog = db->GetCatalog();

		if (catalog.GetCatalogType() == "adbc") {
			auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
			adbc_catalog.ClearCache();
			cleared_count++;
		}
	}

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto result_data = ConstantVector::GetData<bool>(result);
	result_data[0] = cleared_count > 0;
}

void RegisterAdbcClearCacheFunction(DatabaseInstance &db) {
	ExtensionLoader loader(db, "adbc");

	ScalarFunction clear_cache_function("adbc_clear_cache", {}, LogicalType::BOOLEAN, AdbcClearCacheFunction);

	CreateScalarFunctionInfo info(clear_cache_function);
	FunctionDescription desc;
	desc.description = "Clear the cached schema and table metadata for all attached ADBC databases";
	desc.examples = {"SELECT adbc_clear_cache()"};
	desc.categories = {"adbc"};
	info.descriptions.push_back(std::move(desc));

	loader.RegisterFunction(info);
}

} // namespace duckdb
