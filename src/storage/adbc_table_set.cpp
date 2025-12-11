#include "storage/adbc_table_set.hpp"
#include "storage/adbc_transaction.hpp"
#include "storage/adbc_catalog.hpp"
#include "storage/adbc_schema_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include <nanoarrow/nanoarrow.h>

namespace duckdb {

AdbcTableSet::AdbcTableSet(AdbcSchemaEntry &schema)
    : AdbcInSchemaSet(schema, false) {
}

// Helper to extract a string from an Arrow utf8 array
// Returns empty string for NULL values
static string ExtractArrowString(ArrowArray *array, int64_t idx) {
	if (!array || !array->buffers[2]) {
		return "";
	}
	// Check validity (buffer[0] may be null if all values are non-null)
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

// Helper to convert Arrow format string to DuckDB LogicalType
static LogicalType ArrowFormatToLogicalType(const char *format) {
	if (!format) return LogicalType::VARCHAR;

	switch (format[0]) {
	case 'n': return LogicalType::SQLNULL;
	case 'b': return LogicalType::BOOLEAN;
	case 'c': return LogicalType::TINYINT;
	case 'C': return LogicalType::UTINYINT;
	case 's': return LogicalType::SMALLINT;
	case 'S': return LogicalType::USMALLINT;
	case 'i': return LogicalType::INTEGER;
	case 'I': return LogicalType::UINTEGER;
	case 'l': return LogicalType::BIGINT;
	case 'L': return LogicalType::UBIGINT;
	case 'e': return LogicalType::FLOAT;  // float16 mapped to float
	case 'f': return LogicalType::FLOAT;
	case 'g': return LogicalType::DOUBLE;
	case 'z': return LogicalType::BLOB;
	case 'Z': return LogicalType::BLOB;
	case 'u': return LogicalType::VARCHAR;
	case 'U': return LogicalType::VARCHAR;
	case 'd': {
		// Decimal: d:precision,scale
		if (strlen(format) > 2) {
			int precision = 0, scale = 0;
			sscanf(format + 2, "%d,%d", &precision, &scale);
			if (precision > 0) {
				return LogicalType::DECIMAL(precision, scale);
			}
		}
		return LogicalType::DOUBLE;
	}
	case 't': {
		// Temporal types
		if (strlen(format) < 2) return LogicalType::VARCHAR;
		switch (format[1]) {
		case 'd': return LogicalType::DATE;
		case 't': return LogicalType::TIME;
		case 's': return LogicalType::TIMESTAMP;
		case 'D': return LogicalType::INTERVAL;
		case 'i': return LogicalType::INTERVAL;
		default: return LogicalType::VARCHAR;
		}
	}
	default:
		return LogicalType::VARCHAR;
	}
}

void AdbcTableSet::LoadEntries(AdbcTransaction &transaction) {
	auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
	auto connection = adbc_catalog.GetConnection();
	auto &schema_name = schema.name;

	// Use GetObjects with depth=3 to get ALL tables, then filter by schema
	ArrowArrayStream stream;
	memset(&stream, 0, sizeof(stream));

	// For "main" schema, also accept NULL schema_name from drivers like SQLite
	bool accept_null_schema = (schema_name == "main");

	try {
		// depth=3 means get tables; pass nullptr to get all
		connection->GetObjects(3, nullptr, nullptr, nullptr, nullptr, nullptr, &stream);
	} catch (Exception &e) {
		// If GetObjects fails, we can't enumerate tables
		return;
	}

	vector<string> table_names;

	// Parse the hierarchical Arrow result to extract table names
	ArrowArray batch;
	while (true) {
		memset(&batch, 0, sizeof(batch));
		int ret = stream.get_next(&stream, &batch);
		if (ret != 0 || !batch.release) {
			break;
		}

		// GetObjects schema (depth=3):
		// [0] catalog_name (utf8)
		// [1] catalog_db_schemas (list<struct{db_schema_name, db_schema_tables}>)
		//       [0] db_schema_name (utf8)
		//       [1] db_schema_tables (list<struct{table_name, table_type, ...}>)
		//             [0] table_name (utf8)
		//             [1] table_type (utf8)
		if (batch.n_children >= 2) {
			ArrowArray *catalog_db_schemas = batch.children[1];
			if (!catalog_db_schemas || catalog_db_schemas->n_children < 1) {
				if (batch.release) batch.release(&batch);
				continue;
			}

			ArrowArray *schemas_struct = catalog_db_schemas->children[0];
			if (!schemas_struct || !catalog_db_schemas->buffers[1]) {
				if (batch.release) batch.release(&batch);
				continue;
			}

			auto catalog_list_offsets = static_cast<const int32_t *>(catalog_db_schemas->buffers[1]);

			for (int64_t cat_idx = 0; cat_idx < batch.length; cat_idx++) {
				int32_t schema_start = catalog_list_offsets[cat_idx];
				int32_t schema_end = catalog_list_offsets[cat_idx + 1];

				if (schemas_struct->n_children < 2) continue;

				ArrowArray *schema_names_array = schemas_struct->children[0];
				ArrowArray *schema_tables = schemas_struct->children[1];

				if (!schema_tables || schema_tables->n_children < 1 || !schema_tables->buffers[1]) continue;

				ArrowArray *tables_struct = schema_tables->children[0];
				auto schema_list_offsets = static_cast<const int32_t *>(schema_tables->buffers[1]);

				for (int32_t schema_idx = schema_start; schema_idx < schema_end; schema_idx++) {
					string current_schema = ExtractArrowString(schema_names_array, schema_idx);

					// Match schema: either exact match, or NULL/empty schema when we're looking for "main"
					bool matches = false;
					if (current_schema == schema_name) {
						matches = true;
					} else if (accept_null_schema && current_schema.empty()) {
						matches = true;
					}

					if (!matches) {
						continue;
					}

					int32_t table_start = schema_list_offsets[schema_idx];
					int32_t table_end = schema_list_offsets[schema_idx + 1];

					if (!tables_struct || tables_struct->n_children < 1) continue;

					ArrowArray *table_names_array = tables_struct->children[0];
					for (int32_t table_idx = table_start; table_idx < table_end; table_idx++) {
						string table_name = ExtractArrowString(table_names_array, table_idx);
						if (!table_name.empty()) {
							table_names.push_back(table_name);
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

	// Now get the schema for each table and create table entries
	for (const auto &table_name : table_names) {
		auto table_info = GetTableInfo(transaction, schema, table_name);
		if (table_info) {
			auto table_entry = make_shared_ptr<AdbcTableEntry>(catalog, schema, *table_info);
			CreateEntry(transaction, std::move(table_entry));
		}
	}
}

unique_ptr<AdbcTableInfo> AdbcTableSet::GetTableInfo(AdbcTransaction &transaction, AdbcSchemaEntry &schema,
                                                      const string &table_name) {
	auto &adbc_catalog = schema.ParentCatalog().Cast<AdbcCatalog>();
	auto connection = adbc_catalog.GetConnection();
	auto &schema_name = schema.name;

	auto table_info = make_uniq<AdbcTableInfo>(schema, table_name);

	// Get the Arrow schema for the table
	// For "main" schema, pass nullptr to handle SQLite which uses NULL for schema
	ArrowSchema arrow_schema;
	memset(&arrow_schema, 0, sizeof(arrow_schema));

	const char *schema_param = (schema_name == "main") ? nullptr : schema_name.c_str();

	try {
		connection->GetTableSchema(nullptr, schema_param, table_name.c_str(), &arrow_schema);
	} catch (Exception &e) {
		// If GetTableSchema fails, return nullptr
		return nullptr;
	}

	// Convert Arrow schema to DuckDB columns
	for (int64_t i = 0; i < arrow_schema.n_children; i++) {
		ArrowSchema *child = arrow_schema.children[i];
		if (!child) continue;

		string col_name = child->name ? child->name : "column" + to_string(i);
		LogicalType col_type = ArrowFormatToLogicalType(child->format);

		table_info->column_names.push_back(col_name);

		ColumnDefinition column(col_name, col_type);
		table_info->create_info->columns.AddColumn(std::move(column));
	}

	// Release the schema
	if (arrow_schema.release) {
		arrow_schema.release(&arrow_schema);
	}

	return table_info;
}

optional_ptr<CatalogEntry> AdbcTableSet::CreateTable(AdbcTransaction &transaction, BoundCreateTableInfo &info) {
	throw BinderException("ADBC databases do not support creating tables through DDL");
}

} // namespace duckdb
