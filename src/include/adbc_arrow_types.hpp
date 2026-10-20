//===----------------------------------------------------------------------===//
//                         DuckDB
//
// adbc_arrow_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"

namespace adbc_scanner {
using namespace duckdb;

// Map an Arrow field to a DuckDB ArrowType. Wraps ArrowType::GetArrowLogicalType
// and additionally handles decimals wider than DuckDB's DECIMAL (precision > 38,
// e.g. MySQL's DECIMAL(41,0) for SUM(BIGINT), sent as Decimal256), which DuckDB's
// Arrow reader rejects. Those are returned as DOUBLE, matching what the DuckDB
// postgres and mysql scanners do for wide numerics.
shared_ptr<duckdb::ArrowType> AdbcGetArrowType(ClientContext &context, ArrowSchema &schema);

// Drop-in replacement for ArrowTableFunction::PopulateArrowTableSchema that uses
// AdbcGetArrowType for each top-level column.
void AdbcPopulateArrowTableSchema(ClientContext &context, ArrowTableSchema &arrow_table,
                                  const ArrowSchema &arrow_schema);

} // namespace adbc_scanner
