# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DuckDB extension called `adbc` that integrates Arrow ADBC (Arrow Database Connectivity) with DuckDB. It's built using the DuckDB extension template.  Familairize yourself with the ADBC interface.

There is a checkout of a similar project under ./odbc-scanner which is the ODBC scanner for DuckDB extension, this adbc extension wants to be very similar to that extension but use the ADBC interface.

There is also a checkout of the Airport DuckDB extension under ./airport.  The Airport extension integrates DuckDB with Apache Arrow Flight, but it demonstrates C++ code that can read Arrow record batches and return them to DuckDB. The docs for the extension are under ./airports/docs/README.md, but you're mostly going to be interested in airport_take_flight.cpp.

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
```

Tests are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html) in `test/sql/`.

## Build Outputs

- `./build/release/duckdb` - DuckDB shell with extension auto-loaded
- `./build/release/test/unittest` - Test runner binary
- `./build/release/extension/adbc/adbc.duckdb_extension` - Distributable extension binary

## Architecture

- **Extension entry point**: `src/adbc_extension.cpp` - Registers functions with DuckDB via `LoadInternal()`
- **Extension class**: `src/include/adbc_extension.hpp` - Defines `AdbcExtension` class inheriting from `duckdb::Extension`
- **Configuration**: `extension_config.cmake` - Tells DuckDB build system to load this extension
- **Dependencies**: `vcpkg.json` - Depends on `arrow-adbc` via vcpkg with custom overlay ports in `vcpkg-overlay/`

The extension currently registers a scalar function `adbc(varchar) -> varchar`. The ADBC driver manager is linked statically via `AdbcDriverManager::adbc_driver_manager_static`.

## DuckDB Version

This extension targets DuckDB v1.4.0 (configured in `.github/workflows/MainDistributionPipeline.yml`).
