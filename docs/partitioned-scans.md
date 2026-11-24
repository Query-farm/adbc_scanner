# Opt-in parallel partition reads (implementation candidate)

The default `scan_threads 1` retains existing serial ADBC streaming. An attached
**read-only** catalog may opt into independent partition readers for its generated
table scans:

```sql
ATTACH '<uri>' AS remote (
    TYPE adbc,
    driver '<driver>',
    READ_ONLY,
    query_pushdown false,
    scan_threads 16
);
SELECT * FROM remote.schema_name.table_name;
```

`scan_threads` is an integer from 1 through 1024. Values above one require
`READ_ONLY` and `query_pushdown false`; conflicting options are rejected. Only
attached-table scans participate. Arbitrary `adbc_scan(database, query)`, direct
`adbc_scan_table(...)`, remote whole-query pushdown and prefetched command results
remain on their established paths. Parallel table scans require autocommit,
including when executing a prepared query later; explicit transactions are
rejected rather than moved to different worker connections.

Pipelines that can stop consuming early (for example streaming LIMIT or limited
DISTINCT), or have no available pipeline context, admit **one** reader. Normal
LIMIT completion does not necessarily interrupt the client context, so starting
blocked peers there would be unsafe. This is an intentional initial eligibility
limit, not a claim that every scan uses all requested threads. Full-consumption
scan/join/aggregate pipelines may use concurrent readers.

The ceiling is the minimum of `scan_threads`, available DuckDB scheduler threads
and the number of returned partition descriptors. Partition count is chosen by
the driver; asking for 16 readers does not prove 16 partitions exist or run.
For example, Redis currently returns eight partitions by default; its statement
partition-count option is separate and is not hardcoded by this generic scanner.

## Execution and ownership

Binding still uses schema/catalog metadata without executing a query. At runtime
the scanner builds the same projected SELECT, quoted identifiers and bound filter
parameters once, then calls ADBC `ExecutePartitions` once. It retains the producer
statement and shared initialized database, and copies opaque descriptor bytes
before releasing the driver's descriptor container. The implementation refuses
more than 65,536 descriptors or more than 64 MiB of descriptor bytes. A successful
zero-partition set with a valid schema is an empty result.

Each DuckDB local worker owns a different ADBC connection and operation lease,
and reads one claimed partition stream at a time. Each descriptor index is
claimed exactly once. No SQL is re-executed per worker and no Arrow stream's
`get_next` is called concurrently. Connections originate from the same initialized
database, preserving its driver configuration, TLS and authentication settings;
private descriptor contents and credentials are never added to scan diagnostics.
The caller remains responsible for choosing a driver whose partitions support
independent connections, not connection-local session objects.

Each stream has local schema/Arrow conversion state and must match the schema
returned by `ExecutePartitions`, including nested/dictionary structure and
metadata. Existing filter/projection handling remains. Partition scans do not
promise physical result order; use SQL `ORDER BY` where order matters. Independent
table SELECTs keep their existing independent snapshot semantics: this does not
create a common cross-table snapshot. A driver that captures a whole query before
partitioning its finished output may only parallelize downloading that result;
this feature does not make its query computation parallel.

## Errors and cancellation

Only an initial `NOT_IMPLEMENTED` with entirely untouched schema/partition outputs
may fall back once to the original `ExecuteQuery`. Successful descriptor exposure,
invalid output, authentication/I/O errors, schema mismatches and later read errors
never retry or replay the query. A failure from any reader fails the query. The first observed source exception is retained and rethrown by peers, so cancellation does not replace it with a generic peer-failure message.

The producer's statement watcher covers partition execution. Reader watchers call
**connection** cancellation, because `ReadPartition` has no statement, on query
interrupt or a peer failure. Each watcher retains its connection and is stopped
before releasing that stream/lease. Worker state must be destroyed before producer
and database state. Cancellation support is driver-dependent; no unsafe concurrent
stream release or forced connection destruction substitutes for a driver that
cannot interrupt a blocked call. Opening/initializing a connection precedes its reader watcher: a blocked `ConnectionInit` is a separate connect-time limitation, not covered by the initialized-reader cancellation guarantee. No cancellation is issued on an uninitialized connection.

## Validation status

The extension-only loadable target was built against exact DuckDB 2.0.1 headers
and tested on Linux ARM64 with Python DuckDB 2.0.1. The local Python suite passed
55 tests (26 new partition cases plus 29 existing runtime/SQLite cases). Tests target real
DuckDB scheduling with independent streams, NULL/exact decimal results, descriptor
copy ownership, filter binding/projection, cancellation, empty results, failure
without replay, malformed/over-budget descriptors and cleanup counters. No
Redis query, benchmark or full suite is started by those tests.

The adversarial early-stop test first reproduced a 10-second subprocess timeout:
partition zero yielded rows while another reader blocked until cancellation.
After the early-stop admission guard, `preserve_insertion_order=false` plus
`LIMIT 1` finishes with only token zero admitted and balanced lifecycle counters.
A separate full-consumption test asserts at least two simultaneous independent
streams. Cancel tests separately block ReadPartition and get_next, while a gated
peer-failure test preserves its original source error. Producer-statement,
database, connection and stream counters check cleanup after LIMIT, error and
interrupt. Descriptor, NULL/decimal and fallback tests run with the optimizer on.

The SDK-only test build used the same existing Python DuckDB core, promoting that
already loaded module to global symbol visibility for parent and subprocess
Python tests. This is a test-runtime linking control, not a feature change or an
ABI/version-check bypass. The ordinary compiler RTTI setting remained enabled.
No DuckDB core or repository-wide C++ test suite was built. These are local
synthetic/SQLite correctness checks, not Redis compatibility or performance
measurements.
