#include "adbc_filter_pushdown.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/constants.hpp"

namespace adbc_scanner {
using namespace duckdb;

string AdbcFilterPushdown::CreateExpression(string &column_name, vector<unique_ptr<TableFilter>> &filters,
                                            string op, vector<Value> &params, vector<LogicalType> &param_types) {
	vector<string> filter_entries;
	for (auto &filter : filters) {
		auto filter_str = TransformFilter(column_name, *filter, params, param_types);
		if (!filter_str.empty()) {
			filter_entries.push_back(std::move(filter_str));
		}
	}
	if (filter_entries.empty()) {
		return string();
	}
	return "(" + StringUtil::Join(filter_entries, " " + op + " ") + ")";
}

string AdbcFilterPushdown::TransformComparison(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "=";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "<>";
	case ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	default:
		throw NotImplementedException("Unsupported expression type");
	}
}

string AdbcFilterPushdown::TransformConstantFilter(string &column_name, ConstantFilter &constant_filter,
                                                   vector<Value> &params, vector<LogicalType> &param_types) {
	// Add the constant value as a parameter
	params.push_back(constant_filter.constant);
	param_types.push_back(constant_filter.constant.type());

	auto operator_string = TransformComparison(constant_filter.comparison_type);
	return StringUtil::Format("%s %s ?", column_name, operator_string);
}

string AdbcFilterPushdown::TransformFilter(string &column_name, TableFilter &filter,
                                           vector<Value> &params, vector<LogicalType> &param_types) {
	switch (filter.filter_type) {
	case TableFilterType::IS_NULL:
		return column_name + " IS NULL";
	case TableFilterType::IS_NOT_NULL:
		return column_name + " IS NOT NULL";
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_filter = filter.Cast<ConjunctionAndFilter>();
		return CreateExpression(column_name, conjunction_filter.child_filters, "AND", params, param_types);
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction_filter = filter.Cast<ConjunctionOrFilter>();
		return CreateExpression(column_name, conjunction_filter.child_filters, "OR", params, param_types);
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		return TransformConstantFilter(column_name, constant_filter, params, param_types);
	}
	case TableFilterType::STRUCT_EXTRACT: {
		auto &struct_filter = filter.Cast<StructFilter>();
		auto child_name = KeywordHelper::WriteQuoted(struct_filter.child_name, '\"');
		auto new_name = "(" + column_name + ")." + child_name;
		return TransformFilter(new_name, *struct_filter.child_filter, params, param_types);
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		return TransformFilter(column_name, *optional_filter.child_filter, params, param_types);
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		string placeholders;
		for (auto &val : in_filter.values) {
			if (!placeholders.empty()) {
				placeholders += ", ";
			}
			placeholders += "?";
			params.push_back(val);
			param_types.push_back(val.type());
		}
		return column_name + " IN (" + placeholders + ")";
	}
	case TableFilterType::DYNAMIC_FILTER:
		// Dynamic filters can't be pushed down
		return string();
	default:
		throw InternalException("Unsupported table filter type");
	}
}

FilterPushdownResult AdbcFilterPushdown::TransformFilters(const vector<column_t> &column_ids,
                                                          optional_ptr<TableFilterSet> filters,
                                                          const vector<string> &names) {
	FilterPushdownResult result;

	if (!filters || filters->filters.empty()) {
		// no filters
		return result;
	}

	string where_clause;
	for (auto &entry : filters->filters) {
		auto column_id = column_ids[entry.first];

		// Skip virtual columns (like row_id) - they don't exist in the remote table
		if (IsVirtualColumn(column_id)) {
			continue;
		}

		// Validate column_id is within bounds
		if (column_id >= names.size()) {
			// Invalid column index - skip this filter
			continue;
		}

		string column_name = KeywordHelper::WriteQuoted(names[column_id], '"');
		auto &filter = *entry.second;
		auto filter_text = TransformFilter(column_name, filter, result.params, result.param_types);

		if (filter_text.empty()) {
			continue;
		}
		if (!where_clause.empty()) {
			where_clause += " AND ";
		}
		where_clause += filter_text;
	}

	result.where_clause = std::move(where_clause);
	return result;
}

} // namespace adbc_scanner
