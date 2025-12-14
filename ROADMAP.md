# DuckDB ADBC Extension Roadmap

## Near Term (weeks)

1. **Partition parallelism for table scans** — Snowflake can produce result data in partitions; DuckDB could consume that data in parallel.

2. **Connection pooling** — Similar to duckdb-postgres, pool connections when an attached ADBC database is referenced multiple times in the same query.

3. **Driver connectivity examples** — Provide documentation and examples for all available ADBC drivers.

## Medium Term

1. **INSERT INTO fallback** — Support `INSERT INTO` for drivers that lack ADBC bulk copy.  FlightSQL is one.

## Longer Term

All items below apply to ADBC databases created via `ATTACH`:

1. **UPDATE/DELETE without row IDs** — The Airport extension demonstrates a possible approach using a custom optimizer pass with replacement `LogicalOperators`.

2. **DDL support** — Implement `CREATE TABLE` and `DROP TABLE`.

3. **Query passthrough** — When the DuckDB parser detects that a query operates entirely on an attached ADBC database, send it directly to the foreign server. This would enable server-side aggregation.

4. **Transaction support** — Enable proper transaction semantics for attached ADBC databases.

---

## ADBC Feature Gaps (as of 2025-12-13)

The following capabilities would strengthen ADBC as a database federation layer. However, ADBC's primary focus appears to be connectivity rather than federation, so these may be out of scope for the project.

| Gap | Description |
|-----|-------------|
| **Type mapping** | A mapping from Arrow types to driver-native types would allow translating DuckDB → Arrow → driver-specific types for table creation. |
| **Identifier escaping** | A driver-specific method to properly escape table and column names. |
| **Parameter binding introspection** | A way for drivers to indicate whether they support positional bound parameters (`?`). Currently, `adbc_scanner` can perform predicate pushdown but cannot determine if the driver actually accepts parameterized queries. |
| **Row ID support** | DuckDB's `UPDATE` and `DELETE` rely on a `row_id` pseudocolumn, scanning the table first to identify rows to modify. Drivers could indicate whether they support an equivalent concept. |
| **Column constraint metadata** | Drivers could expose constraints such as `NOT NULL`, `UNIQUE`, and primary keys alongside column metadata. |

This roadmap is subject to change, if you'd like to accelerate any of it please contact [Query.Farm](https://query.farm)