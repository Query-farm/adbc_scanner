Roadmap Items:

  3. Implement INSERT INTO for attached tables.
    * Roadmap for pseudo rows its.
3. Implement partition parallelism for scans. Snowflake can produce partition data in parallel, DuckDB could consume in parallel.


To implement ATTACH we need to implement a catalog then a replacement scan lookup.

The problem with predicate pushdown is that the entire predicate can have DuckDB specific functions that may not be possible to send to the remote server, so it would be nice if the TableFunction can have a dynamic predicate pushdown result.

