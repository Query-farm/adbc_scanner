# ADBC Scanner repository notes

For installation and the full user documentation, see the
[Query Farm ADBC Scanner documentation](https://query.farm/products/extensions/adbc_scanner/).

> The extension registers as `adbc_scanner` internally; its functions and the `ATTACH ... (TYPE adbc)` storage type are exposed under the `adbc` name.

## Remote Query Pushdown

On DuckDB 2.0, `ATTACH ... (TYPE adbc)` can send a complete query plan to drivers with a compatible SQL dialect.
PostgreSQL, MySQL, SQLite, and DuckDB currently have structured-query profiles. Filters, projections, aggregates,
supported joins, ordering, limits, and nested queries can execute as one remote statement instead of separate table
scans. Unsupported expressions fall back to DuckDB's regular ADBC scan path.

`EXPLAIN` includes the exact generated statement under `Remote SQL`. Pushdown can be disabled per attachment when
comparing plans or troubleshooting:

```sql
ATTACH 'postgresql://user:password@host/database' AS pg (
    TYPE adbc,
    driver 'postgresql',
    query_pushdown false
);
```

## Naming attached databases (breaking change)

The `adbc_*` functions take the alias of an `ATTACH … (TYPE adbc)` database
instead of a connection handle. `adbc_connect`, `adbc_disconnect`,
`adbc_commit`, `adbc_rollback` and `adbc_set_autocommit` are removed: connect
with `ATTACH`, disconnect with `DETACH`, and use DuckDB's `BEGIN` / `COMMIT` /
`ROLLBACK`.

```sql
ATTACH 'shared.sqlite' AS db (TYPE adbc, driver 'sqlite');
CALL adbc_execute('db', 'CREATE TABLE IF NOT EXISTS messages (id INTEGER, body TEXT)');
CALL adbc_execute('db', 'INSERT INTO messages VALUES (1, ''hello'')');
SELECT * FROM adbc_scan_table('db', 'messages');
SELECT * FROM adbc_scan('db',
    'SELECT id, body FROM messages WHERE id = ?', params := row(1),
    columns := {'id': 'BIGINT', 'body': 'VARCHAR'});

BEGIN;
CALL adbc_execute('db', 'INSERT INTO messages VALUES (2, ''pending'')');
INSERT INTO db.messages VALUES (3, 'also pending');
ROLLBACK;  -- discards both writes

DETACH db;
```

Inside a `BEGIN … COMMIT` transaction, `adbc_execute` and `adbc_insert` join
the attachment's transaction (they commit or roll back with writes made through
`db.…`), and reads see its uncommitted writes; otherwise they autocommit on the
attachment's own connection, which reads also use, so session state such as
temporary tables carries across calls. Two `adbc_*` reads of one attachment
cannot run at the same time (for example a self-join); attach the database
twice for that. Writes to a `READ_ONLY` attachment are rejected. Secrets and connection profiles work through `ATTACH` options.

An unqualified name (`db.orders`, or `orders` after `USE db`) resolves to the
attachment's default schema: the schema the connection reports as current
(PostgreSQL's `public`, for example), or the database's only schema, or `main`.
Set it yourself with `ATTACH '…' AS db (TYPE adbc, driver '…', default_schema 'sales')`.

A driver without transactions (one that cannot turn autocommit off) can still be
written to outside `BEGIN … COMMIT`, through `adbc_execute`, `adbc_insert`,
`INSERT INTO db.…` and `CREATE TABLE db.… AS`. Each statement runs in the driver's
autocommit, so one that fails part-way can leave some rows written. Inside
`BEGIN … COMMIT` the first write fails, because `ROLLBACK` could not undo it.

`adbc_execute` is a `CALL`-only command: `EXPLAIN` and `PREPARE` do not execute
it, `EXPLAIN ANALYZE` does. It returns one `rows_affected` value, or SQL `NULL`
if the driver does not supply a count.

Bulk ingestion with `adbc_insert` uses a bounded producer queue. Stream binding
and execution run together on its consumer thread, so drivers that read during
`BindStream` (including Grainlift) can ingest without blocking producer startup.

Query binding uses ADBC schema metadata only. Drivers such as SQLite that cannot
describe arbitrary queries without executing them require `columns := {...}`.
`adbc_scan_table` uses table metadata. Returned column counts and types are checked
against the bound schema before Arrow data is read.

See [the migration guide](runtime-commands.md) for the full mapping from
handles to attached databases, typed options, and driver limitations.

## Secret scope defaults

ADBC secrets may omit `SCOPE` when they specify a non-empty `URI`; the URI then
becomes the default lookup scope. Explicit scopes remain unchanged and can
match a different or broader prefix. Named secret references work either way.

```sql
CREATE SECRET example (
    TYPE adbc,
    DRIVER 'postgresql',
    URI 'postgresql://host/database'
);
```

This default requires an extension build containing the change. A secret with
neither a URI nor an explicit scope is rejected.
