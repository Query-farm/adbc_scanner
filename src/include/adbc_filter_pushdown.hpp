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

namespace adbc_scanner {
using namespace duckdb;

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
	// Transform DuckDB filters into a WHERE clause with parameter placeholders
	// Returns the WHERE clause string and collects parameter values for binding
	static FilterPushdownResult TransformFilters(const vector<column_t> &column_ids,
	                                             optional_ptr<TableFilterSet> filters,
	                                             const vector<string> &names);

private:
	static string TransformConstantFilter(string &column_name, ConstantFilter &filter,
	                                      vector<Value> &params, vector<LogicalType> &param_types);
	static string TransformFilter(string &column_name, TableFilter &filter,
	                              vector<Value> &params, vector<LogicalType> &param_types);
	static string TransformComparison(ExpressionType type);
	static string CreateExpression(string &column_name, vector<unique_ptr<TableFilter>> &filters,
	                               string op, vector<Value> &params, vector<LogicalType> &param_types);
};

} // namespace adbc_scanner
