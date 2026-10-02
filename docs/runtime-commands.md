# Migrating to attached databases

Earlier versions opened connections with `adbc_connect` and passed the returned
BIGINT handle to every function. The functions now take the alias of an
`ATTACH … (TYPE adbc)` database, and the handle commands are gone.

| Previous | Replacement |
| --- | --- |
| `SET VARIABLE conn = (SELECT adbc_connect({'driver': 'sqlite', 'uri': 'x.db'}))` | `ATTACH 'x.db' AS db (TYPE adbc, driver 'sqlite')` |
| `adbc_connect({'profile': 'mydb'})` | `ATTACH 'profile://mydb' AS db (TYPE adbc)` |
| `adbc_connect({'secret': 's'})` / URI scope lookup | `ATTACH … (TYPE adbc, secret 's')` / the ATTACH path's URI |
| `adbc_scan(getvariable('conn')::BIGINT, sql)` | `adbc_scan('db', sql)` (likewise every `adbc_*` function) |
| `CALL adbc_set_autocommit(conn, false)` … `CALL adbc_commit(conn)` | `BEGIN` … `COMMIT` |
| `CALL adbc_rollback(conn)` | `ROLLBACK` |
| `CALL adbc_disconnect(conn)` | `DETACH db` |
| `SELECT adbc_execute(...)` | `CALL adbc_execute('db', sql)` |
| `SELECT adbc_clear_cache()` | `CALL adbc_clear_cache()` |

An unknown alias, or one naming a non-ADBC database, fails at bind with the
alias in the message. Aliases resolve case-insensitively, like any catalog name.

DuckDB treats an `http://` or `https://` ATTACH path as a remote file and asks
for the httpfs extension, so pass such URIs (e.g. Trino's) as the `uri` option:
`ATTACH '' AS tr (TYPE adbc, driver 'trino', uri 'http://host:8080')`.

## Runtime commands

`adbc_execute` performs its work in the table-function execution callback.
Binding only validates arguments and describes the output. Every execution has
its own completion state, including repeated execution of a prepared statement:

```sql
PREPARE increment AS SELECT * FROM adbc_execute('db', 'UPDATE counters SET value = value + 1');
EXECUTE increment;
EXECUTE increment;
```

Preparing or explaining this statement does not update the remote database;
each execution updates it once. `EXPLAIN ANALYZE` executes its input. This is
an execution-lifecycle guarantee, not a guarantee that retrying a command after
a network failure is safe. Use standalone `CALL` rather than embedding commands
in joins or filtered queries, where normal relational execution can skip an
operator.

`adbc_execute` uses `AdbcStatementExecuteQuery` with a null result-stream pointer,
as specified by ADBC for updates. It returns one BIGINT `rows_affected`, with SQL
`NULL` for an unknown count. Cache clearing returns BOOLEAN `cleared`, false when
there are no attached ADBC catalogs.

## Transactions and connections

```sql
BEGIN;
CALL adbc_execute('db', 'INSERT INTO messages VALUES (2, ''transaction'')');
INSERT INTO db.messages VALUES (3, 'through the catalog');
COMMIT;
```

Inside an explicit transaction, `adbc_execute` and `adbc_insert` use the
attachment's write connection (autocommit disabled), so they commit or roll
back together with writes made through the catalog. Reads (`adbc_scan`,
`adbc_scan_table`, the metadata functions) see the transaction's uncommitted
writes once it has written. A driver that cannot disable autocommit fails the
first write in the transaction rather than silently autocommitting. These
transactions control the remote ADBC connection; local and remote writes are
not one distributed transaction.

Outside an explicit transaction, the `adbc_*` functions use the attachment's
own connection in autocommit, so session state such as a temporary table
created by `adbc_insert` or a `SET` run by `adbc_execute` is visible to later
calls. Writes to an attachment made with `READ_ONLY` are rejected.

Only one operation may use an ADBC connection at a time. Statements and metadata
streams retain an operation lease until released; overlap fails promptly rather
than waiting on a lock. A query that needs two simultaneous `adbc_*` scans of
one database (a self-join) should attach it twice; scans of attached tables
(`db.schema.table`) lease their own pooled connections and are not limited.

## Query schemas

`adbc_scan` obtains metadata through `AdbcStatementExecuteSchema`. It never falls
back to executing a query while binding it. Where the driver cannot supply usable
metadata, declare the full ordered result schema:

```sql
SELECT * FROM adbc_scan('db',
    'SELECT id, body FROM messages WHERE id = ?',
    params := row(1), columns := {'id': 'BIGINT', 'body': 'VARCHAR'});
```

SQLite requires explicit columns for arbitrary queries because its ADBC driver
does not implement query-schema discovery. SQLite integers are returned as BIGINT.
`adbc_scan_table('db', 'messages')` uses `AdbcConnectionGetTableSchema`; it also
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

ATTACH option values preserve their types: VARCHAR and BOOLEAN use the ADBC
string setter, integral values the int64 setter, FLOAT/DOUBLE the double setter,
and BLOB the bytes setter. Unsupported types and nested containers are rejected.
For example, pass `"grainlift.request_timeout_ms" 10000` as an integer. ATTACH
option names are lowercased, so a driver option whose name needs uppercase
letters has to come from a secret's `EXTRA_OPTIONS` or a connection profile.

Secret `EXTRA_OPTIONS` remains a string-to-string MAP. Its values are all redacted
from secret display, including driver-specific private keys. Supply non-string
options directly as ATTACH options. Scan errors no longer append the SQL
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
