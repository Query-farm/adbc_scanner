<p align="center">
  <a href="https://query.farm">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://query.farm/media-kit/logo/wordmark-dark.svg">
      <img alt="Query.Farm" src="https://query.farm/media-kit/logo/wordmark-light.svg" height="64">
    </picture>
  </a>
</p>

# DuckDB ADBC Extension (`adbc`)

[![DuckDB](https://img.shields.io/badge/DuckDB-community_extension-fdf1e0?logo=duckdb&logoColor=fff000)](https://duckdb.org/community_extensions/extensions/adbc_scanner.html)
[![v1.5 build](https://github.com/Query-farm/adbc_scanner/actions/workflows/MainDistributionPipeline.yml/badge.svg?branch=v1.5)](https://github.com/Query-farm/adbc_scanner/actions/workflows/MainDistributionPipeline.yml?query=branch%3Av1.5)

Query [Snowflake](https://www.snowflake.com), [PostgreSQL](https://www.postgresql.org), [MySQL](https://www.mysql.com), [Trino](https://trino.io), [Microsoft SQL Server](https://www.microsoft.com/sql-server), [Apache DataFusion](https://datafusion.apache.org), [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html), and any other system with an [ADBC driver](https://arrow.apache.org/adbc/) directly from [DuckDB](https://duckdb.org).

> The extension registers as `adbc_scanner` internally; its functions and the `ATTACH ... (TYPE adbc)` storage type are exposed under the `adbc` name.

## Documentation

Full documentation, including installation, driver setup, the `ATTACH` storage layer, the function reference, secrets, connection profiles, and cookbook examples, is available at:

**[https://query.farm/products/extensions/adbc_scanner](https://query.farm/products/extensions/adbc_scanner)**

## Installation

```sql
INSTALL adbc_scanner FROM community;
LOAD adbc_scanner;
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

See [the migration guide](docs/runtime-commands.md) for the full mapping from
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

## Development

For instructions on building the extension from source and running its tests, see [docs/BUILDING.md](docs/BUILDING.md).
