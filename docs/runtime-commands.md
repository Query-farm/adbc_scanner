# Migrating to runtime commands

The previous scalar command API could execute remote SQL while DuckDB was
optimizing an expression, including during `EXPLAIN`. This version removes those
scalar functions. Keep the optimizer enabled and use standalone `CALL` statements.

| Previous expression | Replacement |
| --- | --- |
| `SELECT adbc_execute(handle, sql)` | `CALL adbc_execute(handle, sql)` |
| `SELECT adbc_set_autocommit(handle, enabled)` | `CALL adbc_set_autocommit(handle, enabled)` |
| `SELECT adbc_commit(handle)` | `CALL adbc_commit(handle)` |
| `SELECT adbc_rollback(handle)` | `CALL adbc_rollback(handle)` |
| `SELECT adbc_disconnect(handle)` | `CALL adbc_disconnect(handle)` |
| `SELECT adbc_clear_cache()` | `CALL adbc_clear_cache()` |

Commands perform their work in the table-function execution callback. Binding
only validates arguments and describes the output. Every execution has its own
completion state, including repeated execution of a prepared statement:

```sql
PREPARE increment AS SELECT * FROM adbc_execute(1, 'UPDATE counters SET value = value + 1');
EXECUTE increment;
EXECUTE increment;
```

Use an actual connection handle in place of `1`. Preparing or explaining this
statement does not update the remote database; each execution updates it once.
`EXPLAIN ANALYZE` executes its input. This is an execution-lifecycle guarantee,
not a guarantee that retrying a command after a network failure is safe.
Use standalone `CALL` rather than embedding commands in joins or filtered queries,
where normal relational execution can skip an operator.

`adbc_execute` uses `AdbcStatementExecuteQuery` with a null result-stream pointer,
as specified by ADBC for updates. It returns one BIGINT `rows_affected`, with SQL
`NULL` for an unknown count. Transaction and disconnect commands return one
BOOLEAN `success`. Cache clearing returns BOOLEAN `cleared`, false when there
are no attached ADBC catalogs.

## Transactions and connection ownership

```sql
CALL adbc_set_autocommit(getvariable('conn')::BIGINT, false);
CALL adbc_execute(getvariable('conn')::BIGINT, 'INSERT INTO messages VALUES (2, ''transaction'')');
CALL adbc_commit(getvariable('conn')::BIGINT);
CALL adbc_set_autocommit(getvariable('conn')::BIGINT, true);
```

Use `CALL adbc_rollback(...)` to discard an open transaction. These operations
control the remote ADBC connection; they do not make local and remote writes
part of a distributed transaction.

`adbc_connect` remains a volatile scalar, producing a separate handle for every
evaluated input row. It is not constant-folded during planning. Handles belong
to the DuckDB client connection that created them, cannot be used by another
client connection, and are never recycled during the process lifetime. Closing
the owning DuckDB connection releases its remaining handles. Explicit disconnect
invalidates the handle, including in previously prepared statements.

Only one operation may use an ADBC connection at a time. Statements and metadata
streams retain an operation lease until released. Overlap fails promptly rather
than waiting on a lock. Use independent handles for concurrent clients and joins
that need simultaneous remote scans, or the `ATTACH` connection pool.

## Query schemas

`adbc_scan` obtains metadata through `AdbcStatementExecuteSchema`. It never falls
back to executing a query while binding it. Where the driver cannot supply usable
metadata, declare the full ordered result schema:

```sql
SELECT * FROM adbc_scan(getvariable('conn')::BIGINT,
    'SELECT id, body FROM messages WHERE id = ?',
    params := row(1), columns := {'id': 'BIGINT', 'body': 'VARCHAR'});
```

SQLite requires explicit columns for arbitrary queries because its ADBC driver
does not implement query-schema discovery. SQLite integers are returned as BIGINT.
`adbc_scan_table(handle, 'messages')` uses `AdbcConnectionGetTableSchema`; it also
accepts an explicit `columns` declaration. Table metadata and statistics calls
can still contact the driver during binding. Driver implementations are responsible
for honoring the metadata-only semantics of their ADBC methods.

The declaration must include all remote result columns in order, even when the
outer DuckDB query projects fewer columns. Names come from the bound declaration.
The runtime stream must match its column count and, for nonempty results, logical
types. A mismatch fails before conversion; Arrow buffers are interpreted using
the actual stream schema. Empty results can use the declared types even when
the driver guesses a different type for columns with no values.
Drivers whose table metadata disagrees with their result types may require an
explicit schema for direct scans. `ATTACH` relies on accurate table metadata.

## Driver options and secrets

Connection option STRUCT values preserve their types: VARCHAR and BOOLEAN use
the ADBC string setter, integral values the int64 setter, FLOAT/DOUBLE the double
setter, and BLOB the bytes setter. Unsupported types and nested containers are
rejected. For example, pass `'grainlift.request_timeout_ms': 10000` as an integer.
Do not nest direct driver options inside an `options` STRUCT.

Secret `EXTRA_OPTIONS` remains a string-to-string MAP. Its values are all redacted
from secret display, including driver-specific private keys. Supply non-string
options directly in the connection STRUCT. Scan errors no longer append the SQL
query; a downstream driver may still include SQL in its own error message.

## Validation

Validated on EC2 Linux ARM64 with DuckDB 1.5.5, GCC 14.2.1, and the optimizer
enabled on 2026-09-27:

- Release extension and SQL test runner built successfully.
- 26 Python regressions passed, including planning, prepared execution, affected
  row counts, typed options, ownership, partial initialization, and cleanup.
- 1,026 SQLLogicTest assertions passed across 20 test cases using SQLite,
  PostgreSQL, MySQL, DataFusion, Flight SQL, and Trino. SQL Server was unavailable
  and its test was skipped.
- Independent Alice/Bob processes passed the Iroh lock-contention test. A
  3,000 ms busy timeout returned in 3.006 seconds; commit unblocked the other
  writer, rollback/retry recovered, and both clients saw the same 50 subsequent
  updates. `EXPLAIN` caused no write.
- Alice and Bob also passed read-only Iroh catalog discovery and simultaneous
  reads, bidirectional committed-change visibility without reattaching, transaction
  isolation and rollback, joins through the connection pool, and rejection of
  INSERT, UPDATE, DELETE, CREATE TABLE, and DROP TABLE through each catalog.
- Ruff checks, formatting checks, and Python compilation passed.

[Machine-readable evidence](validation/2026-09-27-runtime-commands.json) records
the source and extension hashes. These results were captured before committing
the changes. GitHub CI and community publishing are separate validation/release steps.
