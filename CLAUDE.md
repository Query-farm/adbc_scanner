# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DuckDB extension called `adbc` that integrates Arrow ADBC (Arrow Database Connectivity) with DuckDB. It's built using the DuckDB extension template. Familiarize yourself with the ADBC interface.

There is a checkout of a similar project under ./odbc-scanner which is the ODBC scanner for DuckDB extension. This adbc extension is modeled after that extension but uses the ADBC interface instead.

There is also a checkout of the Airport DuckDB extension under ./airport. The Airport extension integrates DuckDB with Apache Arrow Flight and demonstrates C++ code that can read Arrow record batches and return them to DuckDB. The docs are under ./airport/docs/README.md, but you're mostly interested in airport_take_flight.cpp.

There is also a checkout of the DuckDB postgresql extension under ./duckdb-postgres.  The postgresql extension integrates DuckDB with psotgres and demonstrates C++ code that can interact with the foreign postgresql tables.

## Extension Functions

The extension provides the following functions:

### Connecting

There are no connection handles. Connect with `ATTACH '<uri>' AS <alias> (TYPE adbc, driver '...')` and pass the alias (a VARCHAR) as the first argument of every `adbc_*` function; disconnect with `DETACH`. `GetAttachedConnection` (`src/adbc_connection.cpp`) resolves the alias: an unknown or non-ADBC name fails at bind. Inside an explicit `BEGIN … COMMIT`, writes (`adbc_execute`, `adbc_insert`) use the attachment's transaction write connection, and reads use it too once the transaction has written; otherwise both use the catalog's own connection in autocommit (so session state like temp tables carries across calls; two simultaneous alias reads in one query conflict). Writes to a READ_ONLY attachment are rejected.

- **ATTACH options** (see Storage Extension below for more):
  - `driver` - Driver name, path to shared library, or path to manifest file (.toml). Not required when a connection profile is supplied (via `profile` or a `profile://` URI), since the profile provides the driver.
  - `entrypoint` - Custom entry point function name
  - `profile` - Name of a connection profile to resolve (see Connection Profiles). Equivalent to a `profile://<name>` path.
  - `search_paths` - Additional paths to search for driver manifests *and* connection profiles (colon-separated on Unix, semicolon on Windows)
  - `use_manifests` - Enable/disable manifest search (default: 'true'). Set to 'false' to only use direct library paths.
  - `secret` - Name of a DuckDB secret to use for connection parameters
  - Other options are passed directly to the ADBC driver

#### Secrets Support
The extension supports DuckDB secrets for storing connection credentials. Secrets are automatically looked up based on the `uri` option (scope matching) or can be explicitly referenced by name.

**Creating a secret:**
```sql
CREATE SECRET my_postgres (
    TYPE adbc,
    SCOPE 'postgresql://myhost:5432',
    driver 'postgresql',
    uri 'postgresql://myhost:5432/mydb',
    username 'user',
    password 'secret'
);
```

**Secret parameters:**
- `driver` - ADBC driver name or path
- `uri` - Connection URI passed to the driver
- `username` - Database username
- `password` - Database password (automatically redacted in logs)
- `database` - Database name (no driver accepts it as an option; it is folded into an empty `scheme://` URI path)

`username` / `password` / `database` are sent as driver options first; if the driver rejects one (NOT_IMPLEMENTED, as PostgreSQL and SQLite do — they accept only `uri`) and the URI is `scheme://…` without userinfo, `CreateConnectionFromOptions` retries once with them percent-encoded into the URI (`FoldCredentialsIntoUri` in `src/adbc_connection.cpp`).
- `entrypoint` - Custom driver entry point
- `extra_options` - MAP of additional driver-specific options

**Using secrets:**
```sql
-- Automatic lookup by URI scope (the ATTACH path is the URI)
ATTACH 'postgresql://myhost:5432/mydb' AS pg (TYPE adbc);

-- Explicit secret by name
ATTACH '' AS pg (TYPE adbc, secret 'my_postgres');

-- Override secret options with explicit values
ATTACH 'postgresql://otherhost:5432/otherdb' AS pg (TYPE adbc, secret 'my_postgres');
```

#### Driver Manifest Support
The extension supports ADBC driver manifests, which allow referencing drivers by name instead of full paths. When `use_manifests` is enabled (default), the driver manager searches for manifests in these locations:

**macOS/Linux:**
1. `ADBC_DRIVER_PATH` environment variable (colon-separated paths)
2. `$VIRTUAL_ENV/etc/adbc/drivers` (if in a virtual environment)
3. `$CONDA_PREFIX/etc/adbc/drivers` (if in a Conda environment)
4. `~/.config/adbc/drivers` (Linux) or `~/Library/Application Support/ADBC/Drivers` (macOS)
5. `/etc/adbc/drivers`

**Windows:**
1. `ADBC_DRIVER_PATH` environment variable (semicolon-separated paths)
2. Registry: `HKEY_CURRENT_USER\SOFTWARE\ADBC\Drivers\{name}`
3. `%LOCAL_APPDATA%\ADBC\Drivers`
4. Registry: `HKEY_LOCAL_MACHINE\SOFTWARE\ADBC\Drivers\{name}`

A manifest file is a TOML file (e.g., `sqlite.toml`) containing driver metadata and the path to the shared library.

#### Connection Profiles
The extension supports [ADBC connection profiles](https://arrow.apache.org/adbc/main/format/connection_profiles.html) — named TOML files that bundle a driver name plus a set of connection options, so credentials and endpoints do not need to be hardcoded in SQL. Profile resolution is handled natively by the ADBC driver manager (arrow-adbc 23+).

A profile is referenced by a `profile://<name>` URI or by the `profile` option, and the driver manager loads `<name>.toml` from the standard profile search paths:

**macOS/Linux:**
1. Paths passed via the `search_paths` option
2. `ADBC_PROFILE_PATH` environment variable
3. `$CONDA_PREFIX/etc/adbc/profiles` (Conda builds only)
4. `~/.config/adbc/profiles` (Linux, honoring `XDG_CONFIG_HOME`) or `~/Library/Application Support/ADBC/Profiles` (macOS)

**Windows:**
1. Paths passed via the `search_paths` option
2. `ADBC_PROFILE_PATH` environment variable (semicolon-separated)
3. `%LOCALAPPDATA%\ADBC\Profiles`

A `profile://<name>` with a `.toml` extension or absolute path is loaded directly; otherwise `<name>.toml` is searched for in the directories above.

**Profile file format** (`mydb.toml`):
```toml
profile_version = 1
driver = "sqlite"

[Options]
uri = ":memory:"
# Values support environment-variable interpolation:
# password = "{{ env_var(MYDB_PASSWORD) }}"
```

Options explicitly passed to `ATTACH` take precedence over those from the profile. Use `adbc_profiles()` to list discoverable profiles.

**Examples:**
```sql
-- Attach via a profile:// URI (driver and options come from the profile)
ATTACH 'profile://mydb' AS mydb (TYPE adbc);

-- Attach via the 'profile' option
ATTACH '' AS mydb (TYPE adbc, profile 'mydb');

-- Point at a directory of profiles explicitly
ATTACH '' AS mydb (TYPE adbc, profile 'mydb', search_paths '/opt/adbc/profiles');
```

### Transaction Control
Use DuckDB's `BEGIN` / `COMMIT` / `ROLLBACK`. Inside a transaction, `adbc_execute` and `adbc_insert` commit or roll back together with writes made through the attached catalog (`INSERT INTO db.t …`), and reads see the transaction's uncommitted writes. A driver that cannot disable autocommit (NOT_IMPLEMENTED) fails the first write in an explicit transaction; outside one, `AdbcTransaction::GetWriteConnection` lets catalog writes run in the driver's autocommit, without per-statement atomicity.

### Query Execution
Binding must not execute user SQL. `adbc_scan` uses ADBC `ExecuteSchema`, or an
explicit `columns := {'name': 'TYPE'}` declaration when metadata is unavailable
(including SQLite). `adbc_scan_table` uses `GetTableSchema`. Runtime result types
are checked before reading values. See [docs/runtime-commands.md](docs/runtime-commands.md).

- `adbc_scan(database, query, [params := row(...)], [batch_size := N])` - Execute a SELECT query and return results as a table. Supports parameterized queries via the optional `params` named parameter. The optional `batch_size` parameter hints to the driver how many rows to return per batch (default: driver-specific, typically 2048). This is a best-effort hint that may be ignored by drivers that don't support it.
- `adbc_scan_table(database, table_name, [catalog := ...], [schema := ...], [batch_size := N])` - Scan an entire table by name and return all rows. Supports optional `catalog` and `schema` parameters for fully qualified table names. Supports projection pushdown (only requested columns are fetched), filter pushdown (WHERE clauses are pushed to the remote database with parameter binding), cardinality estimation, progress reporting, and column-level statistics for query optimization (distinct count, null count, min/max when available from the driver via `AdbcConnectionGetStatistics`).
- `CALL adbc_execute(database, query)` - Execute DDL/DML statements (CREATE, INSERT, UPDATE, DELETE) at runtime. Returns affected row count, or NULL when unknown. No scalar form is registered.
- `adbc_insert(database, table_name, <table>, [mode := ...], [max_batches := ...], [options := ...])` - Bulk insert data from a subquery. Modes: 'create', 'append', 'replace', 'create_append'. `options` is a STRUCT or MAP of driver-specific statement options (e.g. `{'adbc.ingest.temporary': 'true'}`), applied after the target table and mode.

### Catalog Functions
- `adbc_info(database)` - Returns driver/database information (vendor name, version, etc.).
- `adbc_tables(database)` - Returns list of tables in the database.
- `adbc_table_types(database)` - Returns supported table types (e.g., "table", "view").
- `adbc_columns(database, [table_name := ...])` - Returns column metadata (name, type, ordinal position, nullability).
- `adbc_schema(database, table_name)` - Returns the Arrow schema for a specific table (field names, Arrow types, nullability).
- `adbc_profiles([search_paths := ...])` - Lists discoverable ADBC connection profiles from the standard search paths (plus any directories in the optional `search_paths` parameter). Returns one row per `*.toml` profile found: `name`, `driver`, `path`, `source` ('additional' | 'env' | 'user'), and `profile_version`. Requires no driver or connection.

### Storage Extension (ATTACH)

The extension also provides a storage extension that allows attaching ADBC data sources as DuckDB databases. This enables querying remote tables using standard SQL syntax without explicit function calls.

```sql
-- Attach an ADBC data source
ATTACH 'path/to/database.db' AS my_db (TYPE adbc, driver 'sqlite');

-- Query tables directly
SELECT * FROM my_db.my_table;
```

**ATTACH options:**
- `driver` (required) - Driver name, path to shared library, or manifest name
- `entrypoint` - Custom entry point function name
- `search_paths` - Additional paths to search for driver manifests
- `use_manifests` - Enable/disable manifest search (default: 'true')
- `batch_size` - Hint for number of rows per batch when scanning tables (default: driver-specific). Larger batch sizes can reduce network round-trips for remote databases.
- `default_schema` - Schema that unqualified names (`db.table`, `USE db`) resolve to. When omitted, `AdbcCatalog::GetDefaultSchema` resolves it on first use: the connection's `adbc.connection.db_schema` if the catalog lists that schema, else the only schema listed, else `main`. Catalog writes pass their schema as `adbc.ingest.target_db_schema` (`main` is the placeholder for drivers without schemas and is not passed).
- Other options are passed directly to the ADBC driver (e.g., `username`, `password`)

**Examples:**
```sql
-- Attach SQLite database
ATTACH '/path/to/mydb.sqlite' AS sqlite_db (TYPE adbc, driver 'sqlite');

-- Attach with custom batch size (useful for network databases)
ATTACH 'postgresql://localhost/mydb' AS pg_db (TYPE adbc, driver 'postgresql', batch_size 65536);

-- Query attached tables
SELECT * FROM pg_db.public.users WHERE id > 100;
SELECT COUNT(*) FROM sqlite_db.main.orders;
```

### Example Usage

```sql
-- Attach using a driver manifest (if sqlite.toml is installed in a search path)
ATTACH 'my.db' AS db (TYPE adbc, driver 'sqlite');

-- Or with an explicit driver path, or extra manifest search paths
-- ATTACH 'my.db' AS db (TYPE adbc, driver '/path/to/libadbc_driver_sqlite.dylib');
-- ATTACH 'my.db' AS db (TYPE adbc, driver 'sqlite', search_paths '/opt/adbc/drivers');

-- Query data
SELECT * FROM adbc_scan('db', 'SELECT 1 AS a, 2 AS b', columns := {'a': 'BIGINT', 'b': 'BIGINT'});

-- Scan an entire table by name
SELECT * FROM adbc_scan_table('db', 'test');

-- Scan a table with schema / catalog qualification (e.g., PostgreSQL)
SELECT * FROM adbc_scan_table('pg', 'users', schema := 'public');
SELECT * FROM adbc_scan_table('pg', 'users', catalog := 'mydb', schema := 'public');

-- Parameterized query
SELECT * FROM adbc_scan('db', 'SELECT ? AS value', params := row(42), columns := {'value': 'BIGINT'});

-- Query with batch size hint (for network drivers, larger batches reduce round-trips)
SELECT * FROM adbc_scan_table('db', 'large_table', batch_size := 65536);

-- Execute DDL/DML
CALL adbc_execute('db', 'CREATE TABLE test (id INTEGER, name TEXT)');
CALL adbc_execute('db', 'INSERT INTO test VALUES (1, ''hello'')');

-- Bulk insert from DuckDB query
SELECT * FROM adbc_insert('db', 'target', (SELECT * FROM local_table), mode := 'create');

-- Catalog functions
SELECT * FROM adbc_info('db');
SELECT * FROM adbc_tables('db');
SELECT * FROM adbc_table_types('db');
SELECT * FROM adbc_columns('db', table_name := 'test');
SELECT * FROM adbc_schema('db', 'test');

-- Transactions
BEGIN;
CALL adbc_execute('db', 'INSERT INTO test VALUES (2, ''world'')');
COMMIT;  -- or ROLLBACK;

-- Disconnect
DETACH db;
```

## Build Commands

```bash
# Build the extension (release)
VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja make release

# Build debug version
VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja make debug

# Faster builds with ninja and ccache (recommended)
GEN=ninja make
```

### VCPKG Setup (required for dependencies)

```bash
cd <your-working-dir-not-the-plugin-repo>
git clone https://github.com/Microsoft/vcpkg.git
sh ./vcpkg/scripts/bootstrap.sh -disableMetrics
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

## Test Commands

```bash
# Run all SQL tests
make test

# Run debug tests
make test_debug

# Run tests with SQLite driver (requires both environment variables)
HAS_ADBC_SQLITE_DRIVER=1 make test
```

**Note:** Tests that use the SQLite ADBC driver require the `HAS_ADBC_SQLITE_DRIVER` environment variable to be set (to any value) in addition to `ADBC_SQLITE_DRIVER` pointing to the driver library path.

Tests are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html) in `test/sql/`.

## Build Outputs

- `./build/release/duckdb` - DuckDB shell with extension auto-loaded
- `./build/release/test/unittest` - Test runner binary
- `./build/release/extension/adbc/adbc.duckdb_extension` - Distributable extension binary

## Architecture

- **Extension entry point**: `src/adbc_scanner_extension.cpp` - Registers all functions with DuckDB via `LoadInternal()`
- **Connections**: `src/adbc_connection.cpp` - `CreateConnectionFromOptions` (used by ATTACH) and `GetAttachedConnection` (resolves an ATTACH alias for the `adbc_*` functions)
- **Scan/Execute**: `src/adbc_scan.cpp` - Implements adbc_scan, adbc_execute, and adbc_insert table functions
- **Arrow type mapping**: `src/adbc_arrow_types.cpp` - Wraps DuckDB's Arrow-to-DuckDB type mapping for every scan path (adbc_scan, adbc_scan_table, ATTACH, adbc_schema). Decimals wider than DuckDB's 38-digit DECIMAL (e.g. MySQL's DECIMAL(41,0) for SUM(BIGINT), sent as Decimal256) are read as fixed-size binary and converted to DOUBLE, matching the DuckDB postgres/mysql scanners
- **Catalog functions**: `src/adbc_catalog.cpp` - Implements adbc_info, adbc_tables, adbc_columns, adbc_schema
- **Connection profiles**: `src/adbc_profiles.cpp` - Implements adbc_profiles (enumerates profile TOML files; connection-time profile resolution is delegated to the driver manager)
- **Secrets**: `src/adbc_secrets.cpp` - DuckDB secrets integration for secure credential storage
- **Extension class**: `src/include/adbc_scanner_extension.hpp` - Defines `AdbcScannerExtension` class inheriting from `duckdb::Extension`
- **Connection wrappers**: `src/include/adbc_connection.hpp` - RAII wrappers for ADBC database, connection, and statement objects
- **Utilities**: `src/include/adbc_utils.hpp` - Error handling and helper functions
- **Configuration**: `extension_config.cmake` - Tells DuckDB build system to load this extension
- **Dependencies**: `vcpkg.json` - Depends on `arrow-adbc` via vcpkg with custom overlay ports in `vcpkg-overlay/`

The ADBC driver manager is linked statically via `AdbcDriverManager::adbc_driver_manager_static`. The overlay port pins **arrow-adbc 24** (connection-profile support landed in 23) and renames the driver manager's internal `SetError` helper to avoid a duplicate-symbol clash with DuckDB's own bundled ADBC implementation.

## DuckDB Version

This extension targets DuckDB v1.4.0 (configured in `.github/workflows/MainDistributionPipeline.yml`).
