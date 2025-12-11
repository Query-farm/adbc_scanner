Roadmap Items:

1. DuckDB ATTACH style interface.
2. With the ATTACH style interface in place:
  1. Implement statistics for tables and columns.
  2. Transform DuckDB predicates into ADBC predicates.
  3. Implement INSERT INTO for attached tables.
  4. Implement projection.
  5. Implement catalog change detection.
3. Implement partition parallelism for scans. Snowflake can produce partition data in parallel, DuckDB could consume in parallel.


To implement ATTACH we need to implement a catalog then a replacement scan lookup.

The problem with predicate pushdown is that the entire predicate can have DuckDB specific functions that may not be possible to send to the remote server, so it would be nice if the TableFunction can have a dynamic predicate pushdown result.

