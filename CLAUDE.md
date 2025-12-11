# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DuckDB extension called `adbc` that integrates Arrow ADBC (Arrow Database Connectivity) with DuckDB. It's built using the DuckDB extension template. Familiarize yourself with the ADBC interface.

There is a checkout of a similar project under ./odbc-scanner which is the ODBC scanner for DuckDB extension. This adbc extension is modeled after that extension but uses the ADBC interface instead.

There is also a checkout of the Airport DuckDB extension under ./airport. The Airport extension integrates DuckDB with Apache Arrow Flight and demonstrates C++ code that can read Arrow record batches and return them to DuckDB. The docs are under ./airport/docs/README.md, but you're mostly interested in airport_take_flight.cpp.

There is also a checkout of the DuckDB postgresql extension under ./duckdb-postgres.  The postgresql extension integrates DuckDB with psotgres and demonstrates C++ code that can interact with the foreign postgresql tables.

## Extension Functions

The extension provides the following functions:

### Connection Management
- `adbc_connect(options)` - Connect to an ADBC data source. Returns a connection handle (BIGINT). Options can be passed as a STRUCT (preferred) or MAP.
  - **Required options:**
    - `driver` - Driver name, path to shared library, or path to manifest file (.toml)
  - **Optional options:**
    - `entrypoint` - Custom entry point function name
    - `search_paths` - Additional paths to search for driver manifests (colon-separated on Unix, semicolon on Windows)
    - `use_manifests` - Enable/disable manifest search (default: 'true'). Set to 'false' to only use direct library paths.
    - `secret` - Name of a DuckDB secret to use for connection parameters
    - Other options are passed directly to the ADBC driver
- `adbc_disconnect(handle)` - Disconnect from an ADBC data source. Returns true on success.

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
- `database` - Database name
- `entrypoint` - Custom driver entry point
- `extra_options` - MAP of additional driver-specific options

**Using secrets:**
```sql
-- Automatic lookup by URI scope
SELECT adbc_connect({'uri': 'postgresql://myhost:5432/mydb'});

-- Explicit secret by name
SELECT adbc_connect({'secret': 'my_postgres'});

-- Override secret options with explicit values
SELECT adbc_connect({'secret': 'my_postgres', 'uri': 'postgresql://otherhost:5432/otherdb'});
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

### Transaction Control
- `adbc_set_autocommit(handle, enabled)` - Enable or disable autocommit mode. When disabled, changes require explicit commit.
- `adbc_commit(handle)` - Commit the current transaction.
- `adbc_rollback(handle)` - Rollback the current transaction, discarding all uncommitted changes.

### Query Execution
- `adbc_scan(handle, query, [params := row(...)], [batch_size := N])` - Execute a SELECT query and return results as a table. Supports parameterized queries via the optional `params` named parameter. The optional `batch_size` parameter hints to the driver how many rows to return per batch (default: driver-specific, typically 2048). This is a best-effort hint that may be ignored by drivers that don't support it.
- `adbc_scan_table(handle, table_name, [catalog := ...], [schema := ...], [batch_size := N])` - Scan an entire table by name and return all rows. Supports optional `catalog` and `schema` parameters for fully qualified table names. Supports projection pushdown (only requested columns are fetched), filter pushdown (WHERE clauses are pushed to the remote database with parameter binding), cardinality estimation, progress reporting, and column-level statistics for query optimization (distinct count, null count, min/max when available from the driver via `AdbcConnectionGetStatistics`).
- `adbc_execute(handle, query)` - Execute DDL/DML statements (CREATE, INSERT, UPDATE, DELETE). Returns affected row count.
- `adbc_insert(handle, table_name, <table>, [mode := ...])` - Bulk insert data from a subquery. Modes: 'create', 'append', 'replace', 'create_append'.

### Catalog Functions
- `adbc_info(handle)` - Returns driver/database information (vendor name, version, etc.).
- `adbc_tables(handle)` - Returns list of tables in the database.
- `adbc_table_types(handle)` - Returns supported table types (e.g., "table", "view").
- `adbc_columns(handle, [table_name := ...])` - Returns column metadata (name, type, ordinal position, nullability).
- `adbc_schema(handle, table_name)` - Returns the Arrow schema for a specific table (field names, Arrow types, nullability).

### Example Usage

```sql
-- Connect using a driver manifest (if sqlite.toml is installed in a search path)
SET VARIABLE conn = (SELECT adbc_connect({'driver': 'sqlite', 'uri': ':memory:'}));

-- Connect with explicit driver path (traditional method)
SET VARIABLE conn = (SELECT adbc_connect({'driver': '/path/to/libadbc_driver_sqlite.dylib', 'uri': ':memory:'}));

-- Connect with additional search paths
SET VARIABLE conn = (SELECT adbc_connect({'driver': 'sqlite', 'uri': ':memory:', 'search_paths': '/opt/adbc/drivers'}));

-- Query data
SELECT * FROM adbc_scan(getvariable('conn')::BIGINT, 'SELECT 1 AS a, 2 AS b');

-- Scan an entire table by name
SELECT * FROM adbc_scan_table(getvariable('conn')::BIGINT, 'test');

-- Scan a table with schema qualification (e.g., PostgreSQL)
SELECT * FROM adbc_scan_table(getvariable('conn')::BIGINT, 'users', schema := 'public');

-- Scan a table with full catalog.schema.table qualification
SELECT * FROM adbc_scan_table(getvariable('conn')::BIGINT, 'users', catalog := 'mydb', schema := 'public');

-- Parameterized query
SELECT * FROM adbc_scan(getvariable('conn')::BIGINT, 'SELECT ? AS value', params := row(42));

-- Query with batch size hint (for network drivers, larger batches reduce round-trips)
SELECT * FROM adbc_scan(getvariable('conn')::BIGINT, 'SELECT * FROM large_table', batch_size := 65536);

-- Execute DDL/DML
SELECT adbc_execute(getvariable('conn')::BIGINT, 'CREATE TABLE test (id INTEGER, name TEXT)');
SELECT adbc_execute(getvariable('conn')::BIGINT, 'INSERT INTO test VALUES (1, ''hello'')');

-- Bulk insert from DuckDB query
SELECT * FROM adbc_insert(getvariable('conn')::BIGINT, 'target', (SELECT * FROM local_table), mode := 'create');

-- Catalog functions
SELECT * FROM adbc_info(getvariable('conn')::BIGINT);
SELECT * FROM adbc_tables(getvariable('conn')::BIGINT);
SELECT * FROM adbc_table_types(getvariable('conn')::BIGINT);
SELECT * FROM adbc_columns(getvariable('conn')::BIGINT, table_name := 'test');
SELECT * FROM adbc_schema(getvariable('conn')::BIGINT, 'test');

-- Transaction control
SELECT adbc_set_autocommit(getvariable('conn')::BIGINT, false);  -- Start transaction
SELECT adbc_execute(getvariable('conn')::BIGINT, 'INSERT INTO test VALUES (2, ''world'')');
SELECT adbc_commit(getvariable('conn')::BIGINT);  -- Commit changes
-- Or: SELECT adbc_rollback(getvariable('conn')::BIGINT);  -- Discard changes
SELECT adbc_set_autocommit(getvariable('conn')::BIGINT, true);  -- Back to autocommit

-- Disconnect
SELECT adbc_disconnect(getvariable('conn')::BIGINT);
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
- **ADBC functions**: `src/adbc_functions.cpp` - Implements connection management (adbc_connect, adbc_disconnect, transaction functions)
- **Scan/Execute**: `src/adbc_scan.cpp` - Implements adbc_scan, adbc_execute, and adbc_insert table functions
- **Catalog functions**: `src/adbc_catalog.cpp` - Implements adbc_info, adbc_tables, adbc_columns, adbc_schema
- **Secrets**: `src/adbc_secrets.cpp` - DuckDB secrets integration for secure credential storage
- **Extension class**: `src/include/adbc_scanner_extension.hpp` - Defines `AdbcScannerExtension` class inheriting from `duckdb::Extension`
- **Connection wrappers**: `src/include/adbc_connection.hpp` - RAII wrappers for ADBC database, connection, and statement objects
- **Utilities**: `src/include/adbc_utils.hpp` - Error handling and helper functions
- **Configuration**: `extension_config.cmake` - Tells DuckDB build system to load this extension
- **Dependencies**: `vcpkg.json` - Depends on `arrow-adbc` via vcpkg with custom overlay ports in `vcpkg-overlay/`

The ADBC driver manager is linked statically via `AdbcDriverManager::adbc_driver_manager_static`.

## DuckDB Version

This extension targets DuckDB v1.4.0 (configured in `.github/workflows/MainDistributionPipeline.yml`).
