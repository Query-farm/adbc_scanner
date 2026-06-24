# DuckDB ADBC Extension (`adbc`)

Query [Snowflake](https://www.snowflake.com), [PostgreSQL](https://www.postgresql.org),
[MySQL](https://www.mysql.com), [Trino](https://trino.io), [Microsoft SQL Server](https://www.microsoft.com/sql-server),
[Apache DataFusion](https://datafusion.apache.org), [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html),
and any other system with an [ADBC driver](https://arrow.apache.org/adbc/) directly from
[DuckDB](https://duckdb.org).

[ADBC](https://arrow.apache.org/adbc/) (Arrow Database Connectivity) is a vendor-neutral, Arrow-native
database API. Because both ADBC and DuckDB speak Apache Arrow, result data crosses the boundary
column-oriented and (where the driver allows) zero-copy — without the row-by-row conversions of ODBC/JDBC.

This extension lets you:

- **`ATTACH`** a remote database and query it with ordinary SQL (`SELECT * FROM remote.schema.table`),
  with **projection and filter pushdown**, **cardinality/column statistics**, a **connection pool**,
  **streaming writes**, and **real transactions**.
- Call remote systems directly with **table functions** (`adbc_scan`, `adbc_scan_table`, `adbc_execute`,
  `adbc_insert`) and **catalog functions** (`adbc_tables`, `adbc_columns`, `adbc_schema`, …).
- Store credentials in **DuckDB secrets** and reference drivers by name via **ADBC driver manifests**.

> The extension registers as `adbc_scanner` internally; its functions and the `ATTACH ... (TYPE adbc)`
> storage type are exposed under the `adbc` name. It targets **DuckDB v1.5** (and v1.4.x).

---

## Installing ADBC drivers

The extension loads ADBC drivers by name through the standard ADBC driver-manifest mechanism. The easiest
way to install drivers is [`dbc`](https://columnar.tech/dbc/):

```sh
curl -LsSf https://dbc.columnar.tech/install.sh | sh
dbc search                 # list available drivers
dbc install postgresql     # install one
```

You can also point `driver` at a shared library path directly (e.g. `'/path/to/libadbc_driver_sqlite.dylib'`).

### Tested drivers

These are exercised end-to-end in CI (`.github/workflows/driver-tests.yml`):

| Driver       | `driver` name | Notes |
|--------------|---------------|-------|
| SQLite       | `sqlite`      | file / `:memory:` |
| PostgreSQL   | `postgresql`  | `$1` placeholders |
| MySQL / MariaDB | `mysql`    | reads (backtick quoting); the driver lacks autocommit control, so writes are rejected rather than silently committed |
| Arrow Flight SQL | `flightsql` | `grpc://` / `grpc+tls://` with `username`/`password` |
| Apache DataFusion | `datafusion` | embedded, in-process; `$1` placeholders |
| Trino        | `trino`       | `http://host:port`, `username` |
| Microsoft SQL Server | `mssql` | T-SQL `@p1` placeholders; database via `?database=` |

The extension automatically adapts the SQL it generates to each driver's dialect (positional-parameter
style — `?`, `$1`, or `@p1` — and identifier quoting — `"…"` or `` `…` ``). Other ADBC drivers
(Snowflake, BigQuery, Databricks, Redshift, Oracle, …) generally work too; cloud drivers need
credentials. The DuckDB ADBC driver is **not** supported: loading `libduckdb` inside a DuckDB host
process collides on shared symbols.

---

## Quick start

```sql
-- Load the extension (unsigned mode while developing locally)
-- duckdb -unsigned

-- Attach a PostgreSQL database and query it as if it were local
ATTACH 'postgresql://user:pass@localhost:5432/mydb' AS pg (TYPE adbc, driver 'postgresql');

SELECT * FROM pg.public.events WHERE id > 100 ORDER BY ts;   -- filter/projection pushed down
SELECT count(*) FROM pg.public.events;

-- Bulk-load a local table into the remote (streams with backpressure, flat memory)
CREATE TABLE pg.public.summary AS SELECT region, sum(amount) FROM local_sales GROUP BY region;

-- Transactions against the attached database
BEGIN;
INSERT INTO pg.public.summary VALUES ('emea', 0);
ROLLBACK;   -- actually rolls back on the remote

DETACH pg;
```

### Attaching other systems

```sql
ATTACH 'mysql://root:pw@localhost:3306/testdb'        AS my (TYPE adbc, driver 'mysql');
ATTACH 'http://localhost:8080'                         AS tr (TYPE adbc, driver 'trino', username 'trino');
ATTACH 'grpc://localhost:31337'                        AS fl (TYPE adbc, driver 'flightsql', username 'u', password 'p');
ATTACH 'sqlserver://sa:pw@localhost:1433?database=db'  AS ms (TYPE adbc, driver 'mssql');
ATTACH '/path/to/file.sqlite'                          AS sl (TYPE adbc, driver 'sqlite');
```

**`ATTACH` options:** `driver` (required), `entrypoint`, `search_paths`, `use_manifests` (default `true`),
`batch_size` (rows/batch hint for network drivers), plus any driver-specific options (`username`,
`password`, …). Add `READ_ONLY` to reject writes.

---

## Function reference

### Connections

| Function | Description |
|----------|-------------|
| `adbc_connect(options)` → `BIGINT` | Open a connection; returns a handle. `options` is a `STRUCT` (preferred) or `MAP`. Required: `driver`. Optional: `entrypoint`, `search_paths`, `use_manifests`, `secret`, plus driver options. |
| `adbc_disconnect(handle)` | Close a connection. |
| `adbc_set_autocommit(handle, enabled)` / `adbc_commit(handle)` / `adbc_rollback(handle)` | Transaction control on a handle. |

### Queries & writes

| Function | Description |
|----------|-------------|
| `adbc_scan(handle, query, [params := row(...)], [batch_size := N])` | Run a `SELECT` and return rows. Supports parameterized queries. |
| `adbc_scan_table(handle, table_name, [catalog := ...], [schema := ...], [batch_size := N])` | Scan a table by name with projection pushdown, filter pushdown (parameter-bound), cardinality estimation, progress, and column statistics. |
| `adbc_execute(handle, query)` → affected rows | Run DDL/DML (`CREATE`, `INSERT`, `UPDATE`, `DELETE`). |
| `adbc_insert(handle, table_name, <subquery>, [mode := ...])` | Bulk-insert from a subquery (streamed). Modes: `create`, `append`, `replace`, `create_append`. |

### Catalog introspection

`adbc_info(handle)`, `adbc_tables(handle)`, `adbc_table_types(handle)`,
`adbc_columns(handle, [table_name := ...])`, `adbc_schema(handle, table_name)`.

```sql
SET VARIABLE conn = (SELECT adbc_connect({'driver': 'sqlite', 'uri': ':memory:'}));
SELECT * FROM adbc_scan(getvariable('conn')::BIGINT, 'SELECT 1 AS a, 2 AS b');
SELECT * FROM adbc_scan(getvariable('conn')::BIGINT, 'SELECT ? AS v', params := row(42));
SELECT * FROM adbc_tables(getvariable('conn')::BIGINT);
SELECT adbc_disconnect(getvariable('conn')::BIGINT);
```

---

## The `ATTACH` storage layer

Attaching a database (`TYPE adbc`) makes its tables queryable with normal SQL and adds:

- **Connection pool** — concurrent reads (parallel scans, catalog lookups) each lease their own ADBC
  connection, since ADBC connections are not safe for concurrent statement execution.
- **Real transactions** — `BEGIN` / `COMMIT` / `ROLLBACK` map to ADBC autocommit + commit/rollback on a
  pinned write connection. If a driver cannot disable autocommit, writes fail loudly instead of silently
  committing.
- **Streaming writes** — `CREATE TABLE … AS` and `INSERT INTO …` stream into the driver's bulk-ingest
  with backpressure, so memory stays flat regardless of input size.
- **Read-only** — `ATTACH … (TYPE adbc, …, READ_ONLY)` rejects writes.
- **Type fidelity** — column types come from DuckDB's Arrow type mapping (timestamps with unit/timezone,
  lists, structs, decimals).

> Some drivers scope table discovery to a search path or default schema. If `ATTACH` doesn't list a
> table, set the driver's search path, or use `adbc_scan_table(..., schema := ...)`.

---

## Secrets

Store credentials as a DuckDB secret instead of inline in the URI:

```sql
CREATE SECRET my_pg (
    TYPE adbc,
    SCOPE 'postgresql://myhost:5432',
    driver 'postgresql',
    uri 'postgresql://myhost:5432/mydb',
    username 'user',
    password 'secret'
);

-- Looked up automatically by URI scope, or by name:
SELECT adbc_connect({'uri': 'postgresql://myhost:5432/mydb'});
SELECT adbc_connect({'secret': 'my_pg'});
```

Passwords/tokens are redacted in logs. Parameters: `driver`, `uri`, `username`, `password`, `database`,
`entrypoint`, `extra_options` (a `MAP`).

---

## Building from source

Requires a C++17 toolchain, CMake, Ninja, and [vcpkg](https://github.com/microsoft/vcpkg).

```sh
# One-time vcpkg setup
git clone https://github.com/microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake

# Build (with submodules: duckdb, extension-ci-tools)
GEN=ninja make release      # or: make debug
```

Outputs:
- `./build/release/duckdb` — DuckDB shell with the extension auto-loaded
- `./build/release/extension/adbc/adbc.duckdb_extension` — the loadable extension
- `./build/release/test/unittest` — the test runner

## Testing

Tests are [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html) under `test/sql/`.

```sh
# SQLite-backed tests
HAS_ADBC_SQLITE_DRIVER=1 make test

# Real-driver tests are gated on env vars and a reachable server, e.g.:
ADBC_POSTGRES_TEST_AVAILABLE=1 ./build/release/test/unittest test/sql/adbc_postgres.test
```

The **ADBC Driver Tests** workflow builds on Linux, installs each driver via `dbc`, spins up PostgreSQL,
MySQL, Flight SQL, Trino, and SQL Server service containers, and runs the full suite against all seven
drivers on every push and pull request.

---

## Limitations

- `UPDATE` / `DELETE` and DDL `CREATE TABLE` / `DROP TABLE` against attached databases are not yet
  implemented (use `adbc_execute` for raw DDL/DML on a handle).
- Within an explicit multi-statement transaction, reads use pooled (autocommit) connections and so do
  not observe that transaction's own uncommitted writes.
- Drivers that cannot disable autocommit (e.g. the current MySQL/DataFusion ADBC drivers) support reads
  but not the transactional write path.

See [`ROADMAP.md`](./ROADMAP.md) for planned work.
