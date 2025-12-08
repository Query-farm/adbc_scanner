# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DuckDB extension called `adbc` that integrates Arrow ADBC (Arrow Database Connectivity) with DuckDB. It's built using the DuckDB extension template. Familiarize yourself with the ADBC interface.

There is a checkout of a similar project under ./odbc-scanner which is the ODBC scanner for DuckDB extension. This adbc extension is modeled after that extension but uses the ADBC interface instead.

There is also a checkout of the Airport DuckDB extension under ./airport. The Airport extension integrates DuckDB with Apache Arrow Flight and demonstrates C++ code that can read Arrow record batches and return them to DuckDB. The docs are under ./airport/docs/README.md, but you're mostly interested in airport_take_flight.cpp.

## Extension Functions

The extension provides the following functions:

### Connection Management
- `adbc_connect(options)` - Connect to an ADBC data source. Returns a connection handle (BIGINT). Options can be passed as a STRUCT (preferred) or MAP. The `driver` option is required.
- `adbc_disconnect(handle)` - Disconnect from an ADBC data source. Returns true on success.

### Query Execution
- `adbc_scan(handle, query, [params := row(...)])` - Execute a SELECT query and return results as a table. Supports parameterized queries via the optional `params` named parameter.
- `adbc_execute(handle, query)` - Execute DDL/DML statements (CREATE, INSERT, UPDATE, DELETE). Returns affected row count.

### Catalog Functions
- `adbc_info(handle)` - Returns driver/database information (vendor name, version, etc.).
- `adbc_tables(handle)` - Returns list of tables in the database.

### Example Usage

```sql
-- Connect to SQLite (driver path varies by installation)
SET VARIABLE conn = (SELECT adbc_connect({'driver': '/path/to/libadbc_driver_sqlite.dylib', 'uri': ':memory:'}));

-- Query data
SELECT * FROM adbc_scan(getvariable('conn')::BIGINT, 'SELECT 1 AS a, 2 AS b');

-- Parameterized query
SELECT * FROM adbc_scan(getvariable('conn')::BIGINT, 'SELECT ? AS value', params := row(42));

-- Execute DDL/DML
SELECT adbc_execute(getvariable('conn')::BIGINT, 'CREATE TABLE test (id INTEGER, name TEXT)');
SELECT adbc_execute(getvariable('conn')::BIGINT, 'INSERT INTO test VALUES (1, ''hello'')');

-- Catalog functions
SELECT * FROM adbc_info(getvariable('conn')::BIGINT);
SELECT * FROM adbc_tables(getvariable('conn')::BIGINT);

-- Disconnect
SELECT adbc_disconnect(getvariable('conn')::BIGINT);
```

## Build Commands

```bash
# Build the extension (release)
make

# Build debug version
make debug

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

# Run tests with SQLite driver (requires ADBC_SQLITE_DRIVER env var)
ADBC_SQLITE_DRIVER="/path/to/libadbc_driver_sqlite.dylib" make test
```

Tests are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html) in `test/sql/`.

## Build Outputs

- `./build/release/duckdb` - DuckDB shell with extension auto-loaded
- `./build/release/test/unittest` - Test runner binary
- `./build/release/extension/adbc/adbc.duckdb_extension` - Distributable extension binary

## Architecture

- **Extension entry point**: `src/adbc_extension.cpp` - Registers all functions with DuckDB via `LoadInternal()`
- **ADBC functions**: `src/adbc_functions.cpp` - Implements connection management, scanning, catalog, and execute functions
- **Extension class**: `src/include/adbc_extension.hpp` - Defines `AdbcExtension` class inheriting from `duckdb::Extension`
- **Configuration**: `extension_config.cmake` - Tells DuckDB build system to load this extension
- **Dependencies**: `vcpkg.json` - Depends on `arrow-adbc` via vcpkg with custom overlay ports in `vcpkg-overlay/`

The ADBC driver manager is linked statically via `AdbcDriverManager::adbc_driver_manager_static`.

## DuckDB Version

This extension targets DuckDB v1.4.0 (configured in `.github/workflows/MainDistributionPipeline.yml`).
