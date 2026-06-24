//===----------------------------------------------------------------------===//
//                         DuckDB
//
// adbc_filter_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/common/string_util.hpp"

namespace adbc_scanner {
using namespace duckdb;

// Positional parameter placeholder syntax. Most drivers (SQLite, DuckDB, MySQL,
// Snowflake) use a bare "?"; PostgreSQL requires "$1", "$2", ... numbered by bind
// order. The wrong style makes the remote prepare fail with a syntax error.
enum class ParamPlaceholderStyle { QUESTION_MARK, DOLLAR_NUMBERED };

// Pick the placeholder style for a driver, keyed on its name (e.g. "postgresql").
inline ParamPlaceholderStyle PlaceholderStyleForDriver(const string &driver_name) {
	auto lower = StringUtil::Lower(driver_name);
	if (lower.find("postgres") != string::npos) {
		return ParamPlaceholderStyle::DOLLAR_NUMBERED;
	}
	return ParamPlaceholderStyle::QUESTION_MARK;
}

// Pick the identifier-quote character for a driver. MySQL/MariaDB use backticks;
// most others (PostgreSQL, SQLite, DuckDB, standard SQL) use double quotes. The
// wrong quote makes the remote parse fail with a syntax error.
inline char IdentifierQuoteForDriver(const string &driver_name) {
	auto lower = StringUtil::Lower(driver_name);
	if (lower.find("mysql") != string::npos || lower.find("mariadb") != string::npos) {
		return '`';
	}
	return '"';
}

// Result of transforming filters - contains both the WHERE clause and bound parameters
struct FilterPushdownResult {
	// The WHERE clause (without the "WHERE" keyword), e.g., "col1 = ? AND col2 > ?"
	string where_clause;
	// Parameter values in order they appear in the WHERE clause
	vector<Value> params;
	// Parameter types corresponding to params
	vector<LogicalType> param_types;

	bool HasFilters() const {
		return !where_clause.empty();
	}
};

class AdbcFilterPushdown {
public:
	// Transform DuckDB filters into a WHERE clause with parameter placeholders.
	// `style` selects the placeholder syntax for the target driver. Returns the
	// WHERE clause string and collects parameter values for binding.
	static FilterPushdownResult TransformFilters(const vector<column_t> &column_ids,
	                                             optional_ptr<TableFilterSet> filters,
	                                             const vector<string> &names,
	                                             ParamPlaceholderStyle style = ParamPlaceholderStyle::QUESTION_MARK,
	                                             char quote_char = '"');

private:
	// Emit the placeholder for the next parameter (1-based index = params.size()
	// after the value has been pushed).
	static string MakePlaceholder(ParamPlaceholderStyle style, idx_t one_based_index);
	static string TransformConstantFilter(string &column_name, ConstantFilter &filter,
	                                      vector<Value> &params, vector<LogicalType> &param_types,
	                                      ParamPlaceholderStyle style);
	static string TransformFilter(string &column_name, TableFilter &filter,
	                              vector<Value> &params, vector<LogicalType> &param_types,
	                              ParamPlaceholderStyle style);
	static string TransformComparison(ExpressionType type);
	static string CreateExpression(string &column_name, vector<unique_ptr<TableFilter>> &filters,
	                               string op, vector<Value> &params, vector<LogicalType> &param_types,
	                               ParamPlaceholderStyle style);
};

} // namespace adbc_scanner
