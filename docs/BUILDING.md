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
- `./build/release/extension/adbc_scanner/adbc_scanner.duckdb_extension` — the loadable extension
- `./build/release/test/unittest` — the test runner

## Testing

Tests are [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html) under `test/sql/`.

```sh
# SQLite-backed tests
HAS_ADBC_SQLITE_DRIVER=1 make test

# Real-driver tests are gated on env vars and a reachable server, e.g.:
ADBC_POSTGRES_TEST_AVAILABLE=1 ./build/release/test/unittest test/sql/adbc_postgres.test
```

Python regressions exercise the loadable extension through the DuckDB package,
with the optimizer enabled. They need a C compiler for the small counting driver
used to verify option types and partial-initialization cleanup. Match the DuckDB
package to the exact DuckDB version used to build the extension.

```sh
uv venv .runtime-tests
uv pip install --python .runtime-tests/bin/python duckdb==1.5.5 adbc-driver-sqlite==1.12.0 pytest
ADBC_SCANNER_EXTENSION="$PWD/build/release/extension/adbc_scanner/adbc_scanner.duckdb_extension" \
  .runtime-tests/bin/python -m pytest test/python -q
```

The isolated Iroh test starts its own Grainlift server and temporary SQLite WAL
database. It creates separate identities and DuckDB processes for Alice and Bob,
checks lock release, busy timeout, rollback/retry, shared visibility, and planning
without writes. Both clients also use `READ_ONLY` attached catalogs: schema/table/
column discovery, simultaneous reads, joins, committed-change visibility in both
directions, uncommitted-change isolation, and rejection of DML/DDL writes are tested.
Writes use separate explicit command handles. The test does not change an existing
server or disable the optimizer.

```sh
uv pip install --python .runtime-tests/bin/python cryptography
.runtime-tests/bin/python test/iroh_sqlite_contention.py \
  --server /path/to/grainlift --driver /path/to/libadbc_driver_grainlift.so \
  --sqlite-driver /path/to/libadbc_driver_sqlite.so \
  --extension build/release/extension/adbc_scanner/adbc_scanner.duckdb_extension \
  --output /tmp/contention.json
```

The **ADBC Driver Tests** workflow builds on Linux, installs each driver via `dbc`, spins up PostgreSQL,
MySQL, Flight SQL, Trino, and SQL Server service containers, and runs the full suite against all seven
drivers on every push and pull request.
