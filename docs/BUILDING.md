# Building from source

Requires a C++17 toolchain, CMake, Ninja, and [vcpkg](https://github.com/microsoft/vcpkg).

```sh
# Clone this repo with submodules (duckdb, extension-ci-tools)
git clone --recurse-submodules git@github.com:Query-farm/adbc_scanner.git

# One-time vcpkg setup
git clone https://github.com/microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake

# Build (with ninja, recommended)
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
