#include "adbc_sql_dialect.hpp"

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"

#include <cmath>

namespace adbc_scanner {
using namespace duckdb;

AdbcSQLDialect AdbcSQLDialectProfile::Detect(const string &driver_name) {
	auto lower = StringUtil::Lower(driver_name);
	if (lower.find("sqlite") != string::npos) {
		return AdbcSQLDialect::SQLITE;
	}
	if (lower.find("mysql") != string::npos || lower.find("mariadb") != string::npos) {
		return AdbcSQLDialect::MYSQL;
	}
	if (lower.find("postgres") != string::npos) {
		return AdbcSQLDialect::POSTGRES;
	}
	if (lower.find("duckdb") != string::npos) {
		return AdbcSQLDialect::DUCKDB;
	}
	return AdbcSQLDialect::NONE;
}

bool AdbcSQLDialectProfile::SupportsStructuredPushdown(AdbcSQLDialect dialect) {
	return dialect == AdbcSQLDialect::SQLITE || dialect == AdbcSQLDialect::MYSQL ||
	       dialect == AdbcSQLDialect::POSTGRES || dialect == AdbcSQLDialect::DUCKDB;
}

static bool SupportsLiteral(AdbcSQLDialect dialect, const Value &value) {
	auto &type = value.type();
	if (value.IsNull() && type.id() != LogicalTypeId::SQLNULL) {
		if (dialect == AdbcSQLDialect::SQLITE || dialect == AdbcSQLDialect::MYSQL) {
			return false;
		}
	}
	if (dialect == AdbcSQLDialect::SQLITE) {
		switch (type.id()) {
		case LogicalTypeId::SQLNULL:
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
		case LogicalTypeId::VARCHAR:
			return true;
		default:
			return false;
		}
	}
	if (dialect == AdbcSQLDialect::MYSQL) {
		switch (type.id()) {
		case LogicalTypeId::SQLNULL:
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::DECIMAL:
			return true;
		case LogicalTypeId::FLOAT:
			return std::isfinite(value.GetValue<float>());
		case LogicalTypeId::DOUBLE:
			return std::isfinite(value.GetValue<double>());
		case LogicalTypeId::VARCHAR: {
			auto str = value.GetValue<string>();
			return str.find('\\') == string::npos && str.find('\0') == string::npos;
		}
		default:
			return false;
		}
	}
	if (dialect == AdbcSQLDialect::POSTGRES) {
		switch (type.id()) {
		case LogicalTypeId::SQLNULL:
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::DECIMAL:
			return true;
		case LogicalTypeId::VARCHAR:
			if (!value.IsNull() && value.GetValue<string>().find('\0') != string::npos) {
				return false;
			}
			return true;
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ:
		case LogicalTypeId::TIME_TZ:
		case LogicalTypeId::UUID:
			return true;
		case LogicalTypeId::TINYINT:
			return !value.IsNull();
		case LogicalTypeId::FLOAT:
			return !value.IsNull() && std::isfinite(value.GetValue<float>());
		case LogicalTypeId::DOUBLE:
			return !value.IsNull() && std::isfinite(value.GetValue<double>());
		default:
			return false;
		}
	}
	switch (type.id()) {
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
		return true;
	default:
		return false;
	}
}

static bool SupportsCastType(AdbcSQLDialect dialect, const LogicalType &type) {
	if (type.id() == LogicalTypeId::UNBOUND) {
		auto bound_type = UnboundType::TryDefaultBind(type);
		return bound_type.id() != LogicalTypeId::INVALID && SupportsCastType(dialect, bound_type);
	}
	if (dialect == AdbcSQLDialect::DUCKDB) {
		return SupportsLiteral(dialect, Value(type));
	}
	if (dialect != AdbcSQLDialect::POSTGRES) {
		return false;
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::UUID:
		return true;
	default:
		return false;
	}
}

static bool HasUnsafeMySQLIdentifier(const Identifier &identifier) {
	return identifier.GetIdentifierName().find('\\') != string::npos;
}

static bool SupportsFunctionArity(const FunctionExpression &function) {
	auto name = StringUtil::Lower(function.FunctionName().GetIdentifierName());
	auto count = function.GetArguments().size();
	if (name == "count_star") {
		return count == 0 && !function.Distinct();
	}
	if (name == "round") {
		return count == 1 || count == 2;
	}
	return count == 1;
}

static bool SupportsTypeExpression(AdbcSQLDialect dialect, const TypeExpression &expression) {
	if (dialect == AdbcSQLDialect::DUCKDB || dialect == AdbcSQLDialect::SQLITE) {
		return true;
	}
	if (dialect != AdbcSQLDialect::POSTGRES || expression.GetQualifiedName().Path().size() != 1) {
		return false;
	}
	static const case_insensitive_set_t POSTGRES_TYPES = {"date", "time", "timestamp", "timestamptz"};
	return POSTGRES_TYPES.count(expression.GetTypeName().GetIdentifierName()) > 0;
}

bool AdbcSQLDialectProfile::SupportsExpression(AdbcSQLDialect dialect, const ParsedExpression &expression) {
	if (!SupportsStructuredPushdown(dialect)) {
		return false;
	}
	if (dialect == AdbcSQLDialect::MYSQL && HasUnsafeMySQLIdentifier(expression.GetAlias())) {
		return false;
	}
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::CONSTANT:
		return SupportsLiteral(dialect, expression.Cast<ConstantExpression>().GetValue());
	case ExpressionClass::CAST: {
		auto &cast = expression.Cast<CastExpression>();
		return !cast.IsTryCast() && SupportsCastType(dialect, cast.TargetType());
	}
	case ExpressionClass::FUNCTION: {
		auto &function = expression.Cast<FunctionExpression>();
		if (function.GetQualifiedName().Path().size() != 1 || function.ExportState() ||
		    (function.OrderBy() && !function.OrderBy()->orders.empty())) {
			return false;
		}
		if (function.Filter() && dialect != AdbcSQLDialect::POSTGRES && dialect != AdbcSQLDialect::DUCKDB) {
			return false;
		}
		if (!SupportsFunctionArity(function)) {
			return false;
		}
		for (auto &argument : function.GetArguments()) {
			if (argument.HasName()) {
				return false;
			}
		}
		static const case_insensitive_set_t PORTABLE_FUNCTIONS = {
		    "abs", "avg", "count", "count_star", "length", "lower", "max", "min", "round", "sum", "upper"};
		static const case_insensitive_set_t POSTGRES_FUNCTIONS = {"avg",   "count", "count_star",
		                                                          "lower", "sum",   "upper"};
		static const case_insensitive_set_t MYSQL_FUNCTIONS = {"avg", "count", "count_star", "lower",
		                                                       "max", "min",   "sum",        "upper"};
		auto &supported_functions = dialect == AdbcSQLDialect::POSTGRES ? POSTGRES_FUNCTIONS
		                            : dialect == AdbcSQLDialect::MYSQL  ? MYSQL_FUNCTIONS
		                                                                : PORTABLE_FUNCTIONS;
		return supported_functions.count(function.FunctionName().GetIdentifierName()) > 0;
	}
	case ExpressionClass::COMPARISON:
		switch (expression.GetExpressionType()) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOTEQUAL:
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		case ExpressionType::COMPARE_DISTINCT_FROM:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			return true;
		default:
			return false;
		}
	case ExpressionClass::OPERATOR:
		switch (expression.GetExpressionType()) {
		case ExpressionType::OPERATOR_NOT:
		case ExpressionType::OPERATOR_IS_NULL:
		case ExpressionType::OPERATOR_IS_NOT_NULL:
		case ExpressionType::OPERATOR_COALESCE:
		case ExpressionType::OPERATOR_NULLIF:
		case ExpressionType::COMPARE_IN:
		case ExpressionType::COMPARE_NOT_IN:
			return true;
		default:
			return false;
		}
	case ExpressionClass::STAR: {
		auto &star = expression.Cast<StarExpression>();
		return star.ExcludeList().empty() && star.ReplaceList().empty() && star.RenameList().empty() &&
		       !star.Expression() && !star.IsColumns();
	}
	case ExpressionClass::BETWEEN:
	case ExpressionClass::CASE:
	case ExpressionClass::CONJUNCTION:
		return true;
	case ExpressionClass::COLUMN_REF: {
		if (dialect != AdbcSQLDialect::MYSQL) {
			return true;
		}
		for (auto &name : expression.Cast<ColumnRefExpression>().ColumnNames()) {
			if (HasUnsafeMySQLIdentifier(name)) {
				return false;
			}
		}
		return true;
	}
	case ExpressionClass::TYPE:
		return SupportsTypeExpression(dialect, expression.Cast<TypeExpression>());
	case ExpressionClass::SUBQUERY:
		return expression.Cast<SubqueryExpression>().GetSubqueryType() != SubqueryType::ANY;
	default:
		return false;
	}
}

bool AdbcSQLDialectProfile::SupportsTableRef(AdbcSQLDialect dialect, const TableRef &ref) {
	if (!SupportsStructuredPushdown(dialect) || ref.sample || !ref.column_name_alias.empty()) {
		return false;
	}
	if (dialect == AdbcSQLDialect::MYSQL && HasUnsafeMySQLIdentifier(ref.alias)) {
		return false;
	}
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base = ref.Cast<BaseTableRef>();
		if (base.at_clause) {
			return false;
		}
		if (dialect == AdbcSQLDialect::MYSQL) {
			for (auto &name : base.GetQualifiedName().Path()) {
				if (HasUnsafeMySQLIdentifier(name)) {
					return false;
				}
			}
		}
		return true;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		if (join.ref_type != JoinRefType::REGULAR && join.ref_type != JoinRefType::CROSS) {
			return false;
		}
		return join.type == JoinType::INNER || join.type == JoinType::LEFT ||
		       (join.type == JoinType::RIGHT && dialect != AdbcSQLDialect::SQLITE);
	}
	case TableReferenceType::SUBQUERY:
	case TableReferenceType::EMPTY_FROM:
		return true;
	default:
		return false;
	}
}

bool AdbcSQLDialectProfile::SupportsQueryNode(AdbcSQLDialect dialect, const QueryNode &node) {
	if (!SupportsStructuredPushdown(dialect) || !node.cte_map.map.empty()) {
		return false;
	}
	for (auto &modifier : node.modifiers) {
		if (modifier->type == ResultModifierType::LEGACY_LIMIT_PERCENT_MODIFIER) {
			return false;
		}
		if (modifier->type == ResultModifierType::LIMIT_MODIFIER &&
		    modifier->Cast<LimitModifier>().limit_type != LimitValueType::ROW_COUNT) {
			return false;
		}
		if (modifier->type == ResultModifierType::DISTINCT_MODIFIER &&
		    !modifier->Cast<DistinctModifier>().distinct_on_targets.empty() && dialect != AdbcSQLDialect::POSTGRES &&
		    dialect != AdbcSQLDialect::DUCKDB) {
			return false;
		}
		if (dialect == AdbcSQLDialect::MYSQL && modifier->type == ResultModifierType::ORDER_MODIFIER) {
			for (auto &order : modifier->Cast<OrderModifier>().orders) {
				if (order.null_order != OrderByNullType::ORDER_DEFAULT) {
					return false;
				}
			}
		}
	}
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		return !select.qualify && !select.sample && select.aggregate_handling != AggregateHandling::FORCE_AGGREGATES &&
		       select.groups.grouping_sets.size() <= 1 && (!select.having || !select.groups.grouping_sets.empty());
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto setop_type = node.Cast<SetOperationNode>().setop_type;
		if (dialect == AdbcSQLDialect::MYSQL) {
			return setop_type == SetOperationType::UNION;
		}
		return setop_type != SetOperationType::UNION_BY_NAME;
	}
	default:
		return false;
	}
}

static void RewriteQueryForDialect(AdbcSQLDialect dialect, QueryNode &node);

static void RewriteExpressionForDialect(AdbcSQLDialect dialect, ParsedExpression &expression) {
	if ((dialect == AdbcSQLDialect::SQLITE || dialect == AdbcSQLDialect::MYSQL ||
	     dialect == AdbcSQLDialect::POSTGRES) &&
	    expression.GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto &function = expression.Cast<FunctionExpression>();
		if (StringUtil::CIEquals(function.FunctionName().GetIdentifierName(), "count_star")) {
			function.SetFunctionName("count");
			function.GetArgumentsMutable().emplace_back(make_uniq<StarExpression>());
		}
	}
	if (expression.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery = expression.Cast<SubqueryExpression>();
		RewriteQueryForDialect(dialect, *subquery.SubqueryMutable()->node);
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expression, [&](ParsedExpression &child) { RewriteExpressionForDialect(dialect, child); });
}

static void RewriteQueryForDialect(AdbcSQLDialect dialect, QueryNode &node) {
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    node, [&](unique_ptr<ParsedExpression> &expression) { RewriteExpressionForDialect(dialect, *expression); },
	    [&](TableRef &ref) {
		    if (dialect != AdbcSQLDialect::MYSQL || ref.type != TableReferenceType::BASE_TABLE) {
			    return;
		    }
		    auto &base = ref.Cast<BaseTableRef>();
		    auto name = base.GetQualifiedName();
		    if (name.Catalog().empty() && StringUtil::CIEquals(name.Schema().GetIdentifierName(), "main")) {
			    base.SetQualifiedName(QualifiedName(Identifier(), Identifier(), name.Name()));
		    }
	    });
}

static string RewriteMySQLIdentifierQuotes(const string &sql) {
	string result;
	result.reserve(sql.size());
	bool in_string = false;
	for (idx_t index = 0; index < sql.size(); index++) {
		auto current = sql[index];
		if (in_string) {
			result += current;
			if (current == '\'' && index + 1 < sql.size() && sql[index + 1] == '\'') {
				result += sql[++index];
			} else if (current == '\'') {
				in_string = false;
			}
			continue;
		}
		if (current == '\'') {
			in_string = true;
			result += current;
			continue;
		}
		if (current != '"') {
			result += current;
			continue;
		}
		result += '`';
		bool closed = false;
		while (++index < sql.size()) {
			current = sql[index];
			if (current == '"') {
				if (index + 1 < sql.size() && sql[index + 1] == '"') {
					result += '"';
					index++;
					continue;
				}
				closed = true;
				break;
			}
			if (current == '`') {
				result += "``";
			} else {
				result += current;
			}
		}
		if (!closed) {
			throw InternalException("Unterminated quoted identifier in generated MySQL query");
		}
		result += '`';
	}
	return result;
}

string AdbcSQLDialectProfile::WriteQuery(AdbcSQLDialect dialect, const QueryNode &node) {
	if (!SupportsStructuredPushdown(dialect)) {
		throw NotImplementedException("The ADBC driver does not have a structured SQL dialect profile");
	}
	auto result = node.Copy();
	RewriteQueryForDialect(dialect, *result);
	auto sql = result->ToString();
	return dialect == AdbcSQLDialect::MYSQL ? RewriteMySQLIdentifierQuotes(sql) : sql;
}

} // namespace adbc_scanner
