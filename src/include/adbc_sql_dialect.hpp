//===----------------------------------------------------------------------===//
//                         DuckDB
//
// adbc_sql_dialect.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/tableref.hpp"

namespace adbc_scanner {
using namespace duckdb;

enum class AdbcSQLDialect : uint8_t { NONE, DUCKDB, MYSQL, POSTGRES, SQLITE };

class AdbcSQLDialectProfile {
public:
	static AdbcSQLDialect Detect(const string &driver_name);
	static bool SupportsStructuredPushdown(AdbcSQLDialect dialect);
	static bool SupportsExpression(AdbcSQLDialect dialect, const ParsedExpression &expression);
	static bool SupportsTableRef(AdbcSQLDialect dialect, const TableRef &ref);
	static bool SupportsQueryNode(AdbcSQLDialect dialect, const QueryNode &node);
	static string WriteQuery(AdbcSQLDialect dialect, const QueryNode &node);
};

} // namespace adbc_scanner
