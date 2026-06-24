# DuckDB ADBC Extension Roadmap

## Completed

- **Connection pooling** — concurrent reads against an attached ADBC database each lease their own
  connection from a per-catalog pool (ADBC connections are not safe for concurrent statement execution).
- **Transaction support** — `BEGIN` / `COMMIT` / `ROLLBACK` on attached databases map to ADBC
  autocommit + commit/rollback on a pinned write connection. Drivers that cannot disable autocommit fail
  loudly rather than silently committing. *(Caveat: in an explicit transaction, reads use pooled
  autocommit connections and do not yet observe the transaction's own uncommitted writes.)*
- **Streaming writes** — `CREATE TABLE … AS` / `INSERT INTO …` stream into the driver's bulk-ingest with
  backpressure, so memory stays flat regardless of input size.
- **Predicate & projection pushdown with per-driver dialect** — filter pushdown emits the right
  positional-parameter style per driver (`?`, `$1`, `@p1`) and the right identifier quoting (`"…"` or
  `` `…` ``). Unsupported filters are dropped (re-applied locally) instead of failing the query.
- **Arrow → DuckDB type mapping** — attached-table column types come from DuckDB's Arrow type mapping
  (timestamps with unit/timezone, lists, structs, decimals) instead of a lossy hand-rolled parser.
- **Read-only enforcement** — `ATTACH … (READ_ONLY)` rejects writes.
- **Driver connectivity examples & CI** — seven drivers (SQLite, PostgreSQL, MySQL, Arrow Flight SQL,
  DataFusion, Trino, SQL Server) are documented in the [README](./README.md) and exercised end-to-end in
  the `driver-tests` CI workflow against live service containers.

## Near Term (weeks)

1. **Partition parallelism for table scans** — Snowflake can produce result data in partitions; DuckDB
   could consume that data in parallel.

2. **Read-your-writes in transactions** — route reads through the transaction's write connection while an
   explicit transaction is active, so in-transaction reads observe uncommitted writes.

## Medium Term

1. **INSERT INTO fallback** — Support `INSERT INTO` for drivers that lack ADBC bulk copy. Flight SQL is one.

2. **Adopt DuckDB's built-in Arrow-scan machinery** — would give scan parallelism and cancellation for
   free and shed hand-rolled scan code; the work is re-integrating pushdown, statistics, progress, and the
   schema-vs-data type guard onto it.

## Longer Term

All items below apply to ADBC databases created via `ATTACH`:

1. **UPDATE/DELETE without row IDs** — The Airport extension demonstrates a possible approach using a
   custom optimizer pass with replacement `LogicalOperators`.

2. **DDL support** — Implement `CREATE TABLE` and `DROP TABLE`.

3. **Query passthrough** — When the DuckDB parser detects that a query operates entirely on an attached
   ADBC database, send it directly to the foreign server. This would enable server-side aggregation.

---

## ADBC Feature Gaps

The following ADBC-spec capabilities would let the extension do more without per-driver heuristics. ADBC's
primary focus is connectivity rather than federation, so some may stay out of scope.

| Gap | Description | Status |
|-----|-------------|--------|
| **Type mapping** | A mapping from Arrow types to driver-native types for table creation (DuckDB → Arrow → driver types). | Reads use DuckDB's Arrow→type mapping; driver-native mapping for DDL is still a gap. |
| **Identifier escaping** | A driver-specific way to escape table/column names. | Worked around per driver (`"` vs `` ` ``); a driver-reported quote char would remove the heuristic. |
| **Parameter binding introspection** | A way for drivers to advertise positional-parameter support and syntax (`?` vs `$1` vs `@p1`). | Worked around per driver by name; native introspection would remove the lookup table. |
| **Row ID support** | DuckDB's `UPDATE`/`DELETE` rely on a `row_id` pseudocolumn. Drivers could indicate an equivalent. | Open — blocks UPDATE/DELETE. |
| **Column constraint metadata** | Drivers could expose `NOT NULL`, `UNIQUE`, and primary keys with column metadata. | Open. |

This roadmap is subject to change. If you'd like to accelerate any of it please contact [Query.Farm](https://query.farm).
