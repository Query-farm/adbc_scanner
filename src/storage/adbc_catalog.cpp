#include "storage/adbc_catalog.hpp"
#include "storage/adbc_schema_entry.hpp"
#include "storage/adbc_transaction.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

namespace adbc_scanner {
using namespace duckdb;

AdbcCatalog::AdbcCatalog(AttachedDatabase &db_p, shared_ptr<AdbcConnectionWrapper> connection_p, const string &path,
                         AccessMode access_mode_p)
    : Catalog(db_p), connection(std::move(connection_p)), attach_path(path), access_mode(access_mode_p), schemas(*this),
      dialect(AdbcSQLDialectProfile::Detect(connection->GetDriverName())) {
	// Build a connection pool over the same shared database so concurrent reads
	// (scans, catalog introspection) each get their own ADBC connection.
	connection_pool = make_uniq<AdbcConnectionPool>(connection->GetDatabase());
}

void AdbcCatalog::SetDefaultSchema(const string &schema) {
	lock_guard<mutex> l(default_schema_lock);
	default_schema = schema;
}

optional<Identifier> AdbcCatalog::GetDefaultSchema() const {
	return optional<Identifier>(Identifier(GetDefaultSchemaName()));
}

string AdbcCatalog::GetDefaultSchemaName() const {
	lock_guard<mutex> l(default_schema_lock);
	if (default_schema.empty()) {
		auto resolved = ResolveDefaultSchema();
		if (resolved.empty()) {
			return "main";
		}
		default_schema = std::move(resolved);
	}
	return default_schema;
}

string AdbcCatalog::ResolveDefaultSchema() const {
	try {
		// A pooled connection, like the scans that will use the answer: the
		// attachment's own connection may have had its schema changed by adbc_execute.
		auto lease = connection_pool->GetConnection();
		auto &conn = *lease.GetConnection();
		string current_schema;
		bool has_current = conn.TryGetOption(ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA, current_schema);
		auto schema_names = AdbcSchemaSet::ListSchemaNames(conn);
		// 1. The schema the connection says it is using (e.g. PostgreSQL's "public"),
		//    as long as it is one this catalog exposes.
		if (has_current && std::find(schema_names.begin(), schema_names.end(), current_schema) != schema_names.end()) {
			return current_schema;
		}
		// 2. The only schema there is.
		if (schema_names.size() == 1) {
			return schema_names[0];
		}
		// 3. No way to tell: keep "main", as for drivers that have no schemas.
		return "main";
	} catch (std::exception &) {
		return string();
	}
}

AdbcCatalog::~AdbcCatalog() = default;

void AdbcCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> AdbcCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	auto &adbc_transaction = AdbcTransaction::Get(transaction.GetContext(), *this);

	// Check if schema already exists
	auto existing = schemas.GetEntry(adbc_transaction, info.SchemaName().GetIdentifierName());
	if (existing) {
		switch (info.on_conflict) {
		case OnCreateConflict::REPLACE_ON_CONFLICT:
			// Drop and recreate - not typically supported, throw error
			throw BinderException("ADBC databases do not support replacing schemas");
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			return nullptr;
		case OnCreateConflict::ERROR_ON_CONFLICT:
		default:
			throw BinderException("Schema with name \"%s\" already exists", info.SchemaName());
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

bool AdbcCatalog::Supports(RemoteCapability capability) const {
	switch (capability) {
	case RemoteCapability::IS_REMOTE:
	case RemoteCapability::CONNECT:
		return true;
	case RemoteCapability::EXECUTE_QUERY_NODE:
		return query_pushdown && AdbcSQLDialectProfile::SupportsStructuredPushdown(dialect);
	default:
		return false;
	}
}

unique_ptr<TableRef> AdbcCatalog::RemoteExecute(ClientContext &context, unique_ptr<QueryNode> node) {
	return RemoteExecute(context, AdbcSQLDialectProfile::WriteQuery(dialect, *node));
}

unique_ptr<TableRef> AdbcCatalog::RemoteExecute(ClientContext &context, const string &sql) {
	(void)context;
	auto remote_sql = sql;
	StringUtil::RTrim(remote_sql);
	while (!remote_sql.empty() && remote_sql.back() == ';') {
		remote_sql.pop_back();
		StringUtil::RTrim(remote_sql);
	}
	vector<unique_ptr<ParsedExpression>> args;
	args.push_back(ConstantExpression::FromValue(Value(GetName())));
	args.push_back(ConstantExpression::FromValue(Value(remote_sql)));
	auto result = make_uniq<TableFunctionRef>();
	result->function = make_uniq<FunctionExpression>("adbc_remote_query", std::move(args));
	return std::move(result);
}

bool AdbcCatalog::SupportsPushdown(const ParsedExpression &expression) {
	return AdbcSQLDialectProfile::SupportsExpression(dialect, expression);
}

bool AdbcCatalog::SupportsPushdown(const TableRef &ref) {
	return AdbcSQLDialectProfile::SupportsTableRef(dialect, ref);
}

bool AdbcCatalog::SupportsPushdown(const QueryNode &node) {
	return AdbcSQLDialectProfile::SupportsQueryNode(dialect, node);
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

// PlanInsert and PlanCreateTableAs are implemented in adbc_insert.cpp

PhysicalOperator &AdbcCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                          PhysicalOperator &plan) {
	throw NotImplementedException("ADBC databases do not yet support DELETE");
}

PhysicalOperator &AdbcCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                          PhysicalOperator &plan) {
	throw NotImplementedException("ADBC databases do not yet support UPDATE");
}

} // namespace adbc_scanner
