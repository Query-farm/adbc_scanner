#include "storage/adbc_schema_set.hpp"
#include "storage/adbc_transaction.hpp"
#include "storage/adbc_catalog.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include <nanoarrow/nanoarrow.h>

namespace duckdb {

AdbcSchemaSet::AdbcSchemaSet(Catalog &catalog)
    : AdbcCatalogSet(catalog, false) {
}

// Helper to extract a string from an Arrow utf8 array
static string ExtractArrowString(ArrowArray *array, int64_t idx) {
	if (!array || !array->buffers[2]) {
		return "";
	}
	// Check validity
	if (array->buffers[0]) {
		auto validity = static_cast<const uint8_t *>(array->buffers[0]);
		if (!((validity[idx / 8] >> (idx % 8)) & 1)) {
			return "";  // NULL value
		}
	}
	auto offsets = static_cast<const int32_t *>(array->buffers[1]);
	auto data = static_cast<const char *>(array->buffers[2]);
	int32_t start = offsets[idx];
	int32_t end = offsets[idx + 1];
	return string(data + start, end - start);
}

void AdbcSchemaSet::LoadEntries(AdbcTransaction &transaction) {
	auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
	auto connection = adbc_catalog.GetConnection();

	// Use GetObjects with depth=2 to get catalogs and schemas
	ArrowArrayStream stream;
	memset(&stream, 0, sizeof(stream));

	try {
		// depth=2 means get catalogs and schemas (but not tables)
		connection->GetObjects(2, nullptr, nullptr, nullptr, nullptr, nullptr, &stream);
	} catch (Exception &e) {
		// If GetObjects fails, create a default "main" schema
		CreateSchemaInfo info;
		info.schema = "main";
		info.internal = false;
		auto schema = make_shared_ptr<AdbcSchemaEntry>(catalog, info);
		CreateEntry(transaction, std::move(schema));
		return;
	}

	unordered_set<string> schema_names;

	// Parse the hierarchical Arrow result to extract schema names
	ArrowArray batch;
	while (true) {
		memset(&batch, 0, sizeof(batch));
		int ret = stream.get_next(&stream, &batch);
		if (ret != 0 || !batch.release) {
			break;
		}

		// GetObjects schema (depth=2):
		// catalog_name (utf8)
		// catalog_db_schemas (list<struct{db_schema_name, db_schema_tables}>)
		if (batch.n_children >= 2) {
			ArrowArray *catalog_db_schemas = batch.children[1];
			if (catalog_db_schemas && catalog_db_schemas->n_children >= 1) {
				ArrowArray *schemas_struct = catalog_db_schemas->children[0];
				auto list_offsets = static_cast<const int32_t *>(catalog_db_schemas->buffers[1]);

				for (int64_t cat_idx = 0; cat_idx < batch.length; cat_idx++) {
					int32_t schema_start = list_offsets[cat_idx];
					int32_t schema_end = list_offsets[cat_idx + 1];

					if (schemas_struct && schemas_struct->n_children >= 1) {
						ArrowArray *schema_names_array = schemas_struct->children[0];
						for (int32_t schema_idx = schema_start; schema_idx < schema_end; schema_idx++) {
							string schema_name = ExtractArrowString(schema_names_array, schema_idx);
							if (!schema_name.empty()) {
								schema_names.insert(schema_name);
							}
						}
					}
				}
			}
		}

		if (batch.release) {
			batch.release(&batch);
		}
	}

	if (stream.release) {
		stream.release(&stream);
	}

	// If no schemas were found, create a default "main" schema
	if (schema_names.empty()) {
		schema_names.insert("main");
	}

	// Create schema entries for each discovered schema
	for (const auto &schema_name : schema_names) {
		CreateSchemaInfo info;
		info.schema = schema_name;
		info.internal = false;
		auto schema = make_shared_ptr<AdbcSchemaEntry>(catalog, info);
		CreateEntry(transaction, std::move(schema));
	}
}

optional_ptr<CatalogEntry> AdbcSchemaSet::CreateSchema(AdbcTransaction &transaction, CreateSchemaInfo &info) {
	auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
	auto connection = adbc_catalog.GetConnection();

	// Build CREATE SCHEMA SQL
	string sql = "CREATE SCHEMA ";
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		sql += "IF NOT EXISTS ";
	}
	sql += KeywordHelper::WriteQuoted(info.schema, '"');

	// Execute on remote database
	auto statement = make_uniq<adbc::AdbcStatementWrapper>(connection);
	statement->Init();
	statement->SetSqlQuery(sql);
	statement->ExecuteUpdate();

	// Create local schema entry
	auto schema_entry = make_shared_ptr<AdbcSchemaEntry>(catalog, info);
	return CreateEntry(transaction, std::move(schema_entry));
}

} // namespace duckdb
