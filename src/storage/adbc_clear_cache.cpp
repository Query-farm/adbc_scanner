#include "storage/adbc_catalog.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace adbc_scanner {
using namespace duckdb;

struct ClearCacheState : public GlobalTableFunctionState {
    bool finished = false;
};

static unique_ptr<FunctionData> BindClear(ClientContext &, TableFunctionBindInput &,
                                         vector<LogicalType> &types, vector<string> &names) {
    types.emplace_back(LogicalType::BOOLEAN);
    names.emplace_back("cleared");
    return nullptr;
}

static unique_ptr<GlobalTableFunctionState> InitClear(ClientContext &, TableFunctionInitInput &) {
    return make_uniq<ClearCacheState>();
}

static void ClearCache(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    auto &state = input.global_state->Cast<ClearCacheState>();
    if (state.finished) {
        return;
    }
    state.finished = true;
    bool cleared = false;
    for (auto database : DatabaseManager::Get(context).GetDatabases(context)) {
        auto &catalog = database->GetCatalog();
        if (catalog.GetCatalogType() == "adbc") {
            catalog.Cast<AdbcCatalog>().ClearCache();
            cleared = true;
        }
    }
    output.SetCardinality(1);
    output.SetValue(0, 0, Value::BOOLEAN(cleared));
}

void RegisterAdbcClearCacheFunction(DatabaseInstance &db) {
    ExtensionLoader loader(db, "adbc");
    loader.RegisterFunction(TableFunction("adbc_clear_cache", {}, ClearCache, BindClear, InitClear));
}
} // namespace adbc_scanner
