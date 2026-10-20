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
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
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

optional_ptr<TableCatalogEntry> AdbcCatalog::GetLoadedTable(const QualifiedName &name) {
	auto &path = name.Path();
	if (path.empty()) {
		return nullptr;
	}
	// The optimizer has not stripped the catalog yet, and a two-part name may be catalog.table or
	// schema.table, so try every reading.
	vector<string> schema_names;
	if (path.size() >= 3) {
		schema_names.push_back(path[path.size() - 2].GetIdentifierName());
	} else if (path.size() == 2) {
		schema_names.push_back(path[0].GetIdentifierName());
		if (path[0] == GetName()) {
			schema_names.push_back(GetDefaultSchemaName());
		}
	} else {
		schema_names.push_back(GetDefaultSchemaName());
	}
	if (std::find(schema_names.begin(), schema_names.end(), DEFAULT_SCHEMA) != schema_names.end()) {
		// "main" also stands for the default schema
		schema_names.push_back(GetDefaultSchemaName());
	}
	for (auto &schema_name : schema_names) {
		auto schema = schemas.GetLoadedEntry(schema_name);
		if (!schema) {
			continue;
		}
		auto table = schema->Cast<AdbcSchemaEntry>().GetTableSet().GetLoadedEntry(path.back().GetIdentifierName());
		if (table && table->type == CatalogType::TABLE_ENTRY) {
			return &table->Cast<TableCatalogEntry>();
		}
	}
	return nullptr;
}

namespace {

//! Checks that every column a query node references is a real column of a remote table it reads (or a
//! name the query itself defines). DuckDB's remote pushdown works on the unbound query, so without this
//! a reference to a column the remote table does not have - DuckDB's rowid, or a pseudo-column of the
//! remote system such as SQLite's rowid or PostgreSQL's ctid - would be shipped verbatim and answered by
//! the remote database, instead of failing to bind as it does on the regular scan path.
//!
//! This is a name check, not a binder: it is conservative, and declining pushdown only costs speed.
//!  - Scoping is flattened: a name is known if any table in the node (including nested subqueries) has
//!    such a column, or any select list in the node defines such an alias.
//!  - A qualified reference none of whose parts names a table, alias, schema or column in the node is
//!    taken to be a correlated reference to an enclosing query, which is checked when that query is.
//!  - An unqualified reference that is not known makes the node decline, even if it would have resolved
//!    against an enclosing query.
class PushdownColumnChecker {
public:
	explicit PushdownColumnChecker(AdbcCatalog &catalog) : catalog(catalog) {
	}

	bool Check(const QueryNode &node) {
		VisitNode(node);
		if (!supported) {
			return false;
		}
		for (auto &name : using_columns) {
			if (columns.find(name) == columns.end()) {
				return false;
			}
		}
		for (auto &ref_p : column_refs) {
			if (!IsKnown(ref_p.get())) {
				return false;
			}
		}
		return true;
	}

private:
	bool IsKnown(const ColumnRefExpression &ref) {
		auto &names = ref.ColumnNames();
		for (auto &name : names) {
			// a column of some table (possibly reached through table/schema qualifiers, or followed by
			// struct field names), or a select list alias
			if (columns.find(name) != columns.end()) {
				return true;
			}
		}
		if (names.size() < 2) {
			return false;
		}
		for (auto &name : names) {
			if (qualifiers.find(name) != qualifiers.end()) {
				// qualified by a table of this node, but not one of its columns
				return false;
			}
		}
		// qualified by something this node does not define: a correlated reference to an enclosing query
		return true;
	}

	void VisitModifiers(const QueryNode &node) {
		for (auto &modifier : node.modifiers) {
			switch (modifier->type) {
			case ResultModifierType::ORDER_MODIFIER:
				for (auto &order : modifier->Cast<OrderModifier>().orders) {
					VisitExpression(*order.expression);
				}
				break;
			case ResultModifierType::LIMIT_MODIFIER: {
				auto &limit = modifier->Cast<LimitModifier>();
				if (limit.limit) {
					VisitExpression(*limit.limit);
				}
				if (limit.offset) {
					VisitExpression(*limit.offset);
				}
				break;
			}
			case ResultModifierType::DISTINCT_MODIFIER:
				for (auto &target : modifier->Cast<DistinctModifier>().distinct_on_targets) {
					VisitExpression(*target);
				}
				break;
			default:
				supported = false;
				break;
			}
		}
	}

	void VisitNode(const QueryNode &node) {
		if (!node.cte_map.map.empty()) {
			supported = false;
			return;
		}
		switch (node.type) {
		case QueryNodeType::SELECT_NODE: {
			auto &select = node.Cast<SelectNode>();
			if (select.from_table) {
				VisitTableRef(*select.from_table);
			}
			for (auto &expression : select.select_list) {
				if (!expression->GetAlias().empty()) {
					columns.insert(expression->GetAlias());
				}
				VisitExpression(*expression);
			}
			if (select.where_clause) {
				VisitExpression(*select.where_clause);
			}
			for (auto &group : select.groups.group_expressions) {
				VisitExpression(*group);
			}
			if (select.having) {
				VisitExpression(*select.having);
			}
			if (select.qualify) {
				VisitExpression(*select.qualify);
			}
			break;
		}
		case QueryNodeType::SET_OPERATION_NODE:
			for (auto &child : node.Cast<SetOperationNode>().children) {
				VisitNode(*child);
			}
			break;
		default:
			supported = false;
			return;
		}
		VisitModifiers(node);
	}

	void VisitTableRef(const TableRef &ref) {
		if (!ref.alias.empty()) {
			qualifiers.insert(ref.alias);
		}
		for (auto &column_alias : ref.column_name_alias) {
			columns.insert(column_alias);
		}
		switch (ref.type) {
		case TableReferenceType::BASE_TABLE: {
			auto &base = ref.Cast<BaseTableRef>();
			for (auto &part : base.GetQualifiedName().Path()) {
				qualifiers.insert(part);
			}
			auto table = catalog.GetLoadedTable(base.GetQualifiedName());
			if (!table) {
				// unknown columns: let the binder handle this query
				supported = false;
				return;
			}
			for (auto &column : table->GetColumns().Logical()) {
				columns.insert(column.Name());
			}
			break;
		}
		case TableReferenceType::JOIN: {
			auto &join = ref.Cast<JoinRef>();
			VisitTableRef(*join.left);
			VisitTableRef(*join.right);
			if (join.condition) {
				VisitExpression(*join.condition);
			}
			for (auto &name : join.using_columns) {
				using_columns.push_back(name);
			}
			break;
		}
		case TableReferenceType::SUBQUERY:
			VisitNode(*ref.Cast<SubqueryRef>().subquery->node);
			break;
		case TableReferenceType::EMPTY_FROM:
			break;
		default:
			supported = false;
			break;
		}
	}

	void VisitExpression(const ParsedExpression &expression) {
		switch (expression.GetExpressionClass()) {
		case ExpressionClass::COLUMN_REF:
			column_refs.push_back(expression.Cast<ColumnRefExpression>());
			break;
		case ExpressionClass::SUBQUERY:
			VisitNode(*expression.Cast<SubqueryExpression>().Subquery()->node);
			break;
		default:
			break;
		}
		ParsedExpressionIterator::EnumerateChildren(
		    expression, [&](const ParsedExpression &child) { VisitExpression(child); });
	}

	AdbcCatalog &catalog;
	bool supported = true;
	//! real column names of the tables read, plus select list and table column aliases
	identifier_set_t columns;
	//! table names, aliases, schema and catalog names that may qualify a column reference
	identifier_set_t qualifiers;
	vector<reference<const ColumnRefExpression>> column_refs;
	vector<Identifier> using_columns;
};

} // namespace

bool AdbcCatalog::SupportsPushdown(const QueryNode &node) {
	if (!AdbcSQLDialectProfile::SupportsQueryNode(dialect, node)) {
		return false;
	}
	// decline (rather than error) for a column the remote tables do not have: the query then runs on
	// the regular scan path, where the binder reports it - or resolves it, if the check was too cautious
	return PushdownColumnChecker(*this).Check(node);
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
