# DuckDB ADBC Scanner Extension

The ADBC Scanner extension by [Query.Farm](https://query.farm) enables DuckDB to connect to external databases using [Arrow Database Connectivity (ADBC)](https://arrow.apache.org/adbc/), a column-oriented API standard for database access. ADBC provides efficient data transfer using Apache Arrow's columnar format.

## Installation

```sql
INSTALL adbc_scanner FROM community;
LOAD adbc_scanner;
```

## Quick Start

```sql
-- Connect to a SQLite database using driver name (requires installed manifest)
SET VARIABLE conn = (SELECT adbc_connect({
    'driver': 'sqlite',
    'uri': ':memory:'
}));

-- Or connect with explicit driver path
SET VARIABLE conn = (SELECT adbc_connect({
    'driver': '/path/to/libadbc_driver_sqlite.dylib',
    'uri': ':memory:'
}));

-- Query data
SELECT * FROM adbc_scan(getvariable('conn')::BIGINT, 'SELECT * FROM my_table');

-- Execute DDL/DML
SELECT adbc_execute(getvariable('conn')::BIGINT, 'CREATE TABLE users (id INT, name TEXT)');
SELECT adbc_execute(getvariable('conn')::BIGINT, 'INSERT INTO users VALUES (1, ''Alice'')');

-- Disconnect when done
SELECT adbc_disconnect(getvariable('conn')::BIGINT);
```

## Functions

### adbc_connect

Creates a connection to an external database via ADBC.

```sql
adbc_connect(options) -> BIGINT
```

**Parameters:**

- `options`: A STRUCT or MAP containing connection options

**Required Options:**

- `driver`: Driver name (e.g., `'sqlite'`, `'postgresql'`), path to shared library, or path to manifest file (`.toml`)

**Connection Options:**

- `uri`: Connection URI (driver-specific)
- `username`: Database username
- `password`: Database password
- `secret`: Name of a DuckDB secret containing connection parameters (see [Secrets](#secrets))

**Driver Resolution Options:**

- `entrypoint`: Custom driver entrypoint function name (rarely needed)
- `search_paths`: Additional paths to search for driver manifests (colon-separated on Unix, semicolon on Windows)
- `use_manifests`: Enable/disable manifest search (default: `'true'`). Set to `'false'` to only use direct library paths.

**Returns:** A connection handle (BIGINT) used with other ADBC functions.

**Examples:**

```sql
-- Using driver name (requires installed manifest)
SELECT adbc_connect({
    'driver': 'sqlite',
    'uri': '/path/to/database.db'
});

-- Using explicit driver path
SELECT adbc_connect({
    'driver': '/path/to/libadbc_driver_sqlite.dylib',
    'uri': '/path/to/database.db'
});

-- PostgreSQL with credentials
SELECT adbc_connect({
    'driver': 'postgresql',
    'uri': 'postgresql://localhost:5432/mydb',
    'username': 'user',
    'password': 'pass'
});

-- Using MAP syntax
SELECT adbc_connect(MAP {
    'driver': 'postgresql',
    'uri': 'postgresql://localhost:5432/mydb',
    'username': 'user',
    'password': 'pass'
});

-- With custom search paths for driver manifests
SELECT adbc_connect({
    'driver': 'sqlite',
    'uri': ':memory:',
    'search_paths': '/opt/adbc/drivers:/custom/path'
});

-- Disable manifest search (only use direct library paths)
SELECT adbc_connect({
    'driver': '/explicit/path/to/driver.dylib',
    'uri': ':memory:',
    'use_manifests': 'false'
});

-- Store connection handle in a variable for reuse
SET VARIABLE conn = (SELECT adbc_connect({
    'driver': 'sqlite',
    'uri': ':memory:'
}));

-- Connect using a secret (see Secrets section below)
SELECT adbc_connect({'secret': 'my_postgres_secret'});

-- Connect using URI scope lookup (automatically finds matching secret)
SELECT adbc_connect({'uri': 'postgresql://myhost:5432/mydb'});
```

### adbc_disconnect

Closes an ADBC connection and releases resources.

```sql
adbc_disconnect(connection_id) -> BOOLEAN
```

**Parameters:**

- `connection_id`: Connection handle from `adbc_connect`

**Returns:** `true` on success.

**Example:**

```sql
SELECT adbc_disconnect(getvariable('conn')::BIGINT);
```

### adbc_set_autocommit

Enables or disables autocommit mode on a connection. When autocommit is disabled, changes require an explicit `adbc_commit()` call.

```sql
adbc_set_autocommit(connection_id, enabled) -> BOOLEAN
```

**Parameters:**

- `connection_id`: Connection handle from `adbc_connect`
- `enabled`: `true` to enable autocommit, `false` to disable

**Returns:** `true` on success.

**Example:**

```sql
-- Disable autocommit to start a transaction
SELECT adbc_set_autocommit(getvariable('conn')::BIGINT, false);

-- Make changes...
SELECT adbc_execute(getvariable('conn')::BIGINT, 'INSERT INTO users VALUES (1, ''Alice'')');

-- Commit or rollback
SELECT adbc_commit(getvariable('conn')::BIGINT);

-- Re-enable autocommit
SELECT adbc_set_autocommit(getvariable('conn')::BIGINT, true);
```

### adbc_commit

Commits the current transaction. Only applicable when autocommit is disabled.

```sql
adbc_commit(connection_id) -> BOOLEAN
```

**Parameters:**

- `connection_id`: Connection handle from `adbc_connect`

**Returns:** `true` on success.

**Example:**

```sql
SELECT adbc_commit(getvariable('conn')::BIGINT);
```

### adbc_rollback

Rolls back the current transaction, discarding all uncommitted changes. Only applicable when autocommit is disabled.

```sql
adbc_rollback(connection_id) -> BOOLEAN
```

**Parameters:**

- `connection_id`: Connection handle from `adbc_connect`

**Returns:** `true` on success.

**Example:**

```sql
-- Start a transaction
SELECT adbc_set_autocommit(getvariable('conn')::BIGINT, false);

-- Make changes
SELECT adbc_execute(getvariable('conn')::BIGINT, 'DELETE FROM users WHERE id = 1');

-- Oops, rollback!
SELECT adbc_rollback(getvariable('conn')::BIGINT);

-- Re-enable autocommit
SELECT adbc_set_autocommit(getvariable('conn')::BIGINT, true);
```

### adbc_scan

Executes a SELECT query and returns the results as a table.

```sql
adbc_scan(connection_id, query, [params := row(...)]) -> TABLE
```

**Parameters:**

- `connection_id`: Connection handle from `adbc_connect`
- `query`: SQL SELECT query to execute
- `params` (optional): Query parameters as a STRUCT created with `row(...)`

**Returns:** A table with columns matching the query result.

**Examples:**

```sql
-- Simple query
SELECT * FROM adbc_scan(getvariable('conn')::BIGINT, 'SELECT * FROM users');

-- Query with parameters
SELECT * FROM adbc_scan(
    getvariable('conn')::BIGINT,
    'SELECT * FROM users WHERE id = ? AND status = ?',
    params := row(42, 'active')
);

-- Aggregate results
SELECT COUNT(*), AVG(price)
FROM adbc_scan(getvariable('conn')::BIGINT, 'SELECT * FROM orders');
```

### adbc_execute

Executes DDL or DML statements (CREATE, INSERT, UPDATE, DELETE).

```sql
adbc_execute(connection_id, statement) -> BIGINT
```

**Parameters:**

- `connection_id`: Connection handle from `adbc_connect`
- `statement`: SQL DDL or DML statement to execute

**Returns:** Number of rows affected (or 0 if not reported by driver).

**Examples:**

```sql
-- Create a table
SELECT adbc_execute(getvariable('conn')::BIGINT,
    'CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price DECIMAL)');

-- Insert data
SELECT adbc_execute(getvariable('conn')::BIGINT,
    'INSERT INTO products VALUES (1, ''Widget'', 9.99), (2, ''Gadget'', 19.99)');

-- Update data
SELECT adbc_execute(getvariable('conn')::BIGINT,
    'UPDATE products SET price = 14.99 WHERE id = 1');

-- Delete data
SELECT adbc_execute(getvariable('conn')::BIGINT,
    'DELETE FROM products WHERE id = 2');
```

### adbc_insert

Bulk insert data from a DuckDB query into a table via ADBC. This is more efficient than executing individual INSERT statements.

```sql
adbc_insert(connection_id, table_name, <table>, [mode:=]) -> TABLE(rows_inserted BIGINT)
```

**Parameters:**

- `connection_id`: Connection handle from `adbc_connect`
- `table_name`: Target table name in the remote database
- `<table>`: A subquery providing the data to insert
- `mode` (optional): Insert mode, one of:
  - `'create'`: Create the table; error if it exists (default for new tables)
  - `'append'`: Append to existing table; error if table doesn't exist
  - `'replace'`: Drop and recreate the table if it exists
  - `'create_append'`: Create if doesn't exist, append if it does

**Returns:** A table with a single row containing the number of rows inserted.

**Examples:**

```sql
-- Create a new table and insert data
SELECT * FROM adbc_insert(getvariable('conn')::BIGINT, 'users',
    (SELECT id, name, email FROM local_users),
    mode := 'create');

-- Append data to an existing table
SELECT * FROM adbc_insert(getvariable('conn')::BIGINT, 'users',
    (SELECT id, name, email FROM new_users),
    mode := 'append');

-- Replace an existing table with new data
SELECT * FROM adbc_insert(getvariable('conn')::BIGINT, 'users',
    (SELECT * FROM updated_users),
    mode := 'replace');
```

Output:
```
┌───────────────┐
│ rows_inserted │
├───────────────┤
│          1000 │
└───────────────┘
```

### adbc_info

Returns driver and database information for a connection.

```sql
adbc_info(connection_id) -> TABLE(info_name VARCHAR, info_value VARCHAR)
```

**Parameters:**

- `connection_id`: Connection handle from `adbc_connect`

**Returns:** A table with info_name and info_value columns.

**Common Info Names:**

- `vendor_name`: Database vendor (e.g., "SQLite", "PostgreSQL")
- `vendor_version`: Database version
- `driver_name`: ADBC driver name
- `driver_version`: Driver version
- `driver_arrow_version`: Arrow version used by driver

**Example:**

```sql
SELECT * FROM adbc_info(getvariable('conn')::BIGINT);
```

Output:
```
┌──────────────────────┬────────────────────┐
│      info_name       │     info_value     │
├──────────────────────┼────────────────────┤
│ vendor_name          │ SQLite             │
│ vendor_version       │ 3.50.4             │
│ driver_name          │ ADBC SQLite Driver │
│ driver_version       │ (unknown)          │
│ driver_arrow_version │ 0.7.0              │
└──────────────────────┴────────────────────┘
```

### adbc_tables

Lists tables in the connected database with optional filtering.

```sql
adbc_tables(connection_id, [catalog:=], [schema:=], [table_name:=]) -> TABLE
```

**Parameters:**

- `connection_id`: Connection handle from `adbc_connect`
- `catalog` (optional): Filter by catalog name pattern
- `schema` (optional): Filter by schema name pattern
- `table_name` (optional): Filter by table name pattern

**Returns:** A table with columns:

- `catalog_name`: Catalog containing the table
- `schema_name`: Schema containing the table
- `table_name`: Name of the table
- `table_type`: Type (e.g., "table", "view")

**Examples:**

```sql
-- List all tables
SELECT * FROM adbc_tables(getvariable('conn')::BIGINT);

-- Filter by table name pattern
SELECT * FROM adbc_tables(getvariable('conn')::BIGINT, table_name := 'user%');

-- Filter by schema
SELECT * FROM adbc_tables(getvariable('conn')::BIGINT, schema := 'public');
```

### adbc_table_types

Returns the types of tables supported by the database (e.g., "table", "view").

```sql
adbc_table_types(connection_id) -> TABLE(table_type VARCHAR)
```

**Parameters:**

- `connection_id`: Connection handle from `adbc_connect`

**Returns:** A table with a single `table_type` column listing supported types.

**Example:**

```sql
SELECT * FROM adbc_table_types(getvariable('conn')::BIGINT);
```

Output:
```
┌────────────┐
│ table_type │
├────────────┤
│ table      │
│ view       │
└────────────┘
```

### adbc_columns

Returns column metadata for tables in the connected database.

```sql
adbc_columns(connection_id, [catalog:=], [schema:=], [table_name:=], [column_name:=]) -> TABLE
```

**Parameters:**

- `connection_id`: Connection handle from `adbc_connect`
- `catalog` (optional): Filter by catalog name pattern
- `schema` (optional): Filter by schema name pattern
- `table_name` (optional): Filter by table name pattern
- `column_name` (optional): Filter by column name pattern

**Returns:** A table with columns:

- `catalog_name`: Catalog containing the table
- `schema_name`: Schema containing the table
- `table_name`: Name of the table
- `column_name`: Name of the column
- `ordinal_position`: Column position (1-based)
- `remarks`: Database-specific description
- `type_name`: Data type name (e.g., "INTEGER", "TEXT")
- `is_nullable`: Whether the column allows NULL values

**Examples:**

```sql
-- Get all columns for a specific table
SELECT * FROM adbc_columns(getvariable('conn')::BIGINT, table_name := 'users');

-- Get specific column
SELECT * FROM adbc_columns(getvariable('conn')::BIGINT,
    table_name := 'users',
    column_name := 'email');

-- List all columns in the database
SELECT table_name, column_name, type_name
FROM adbc_columns(getvariable('conn')::BIGINT)
ORDER BY table_name, ordinal_position;
```

Output:
```
┌──────────────┬─────────────┬────────────┬─────────────┬──────────────────┬─────────┬───────────┬─────────────┐
│ catalog_name │ schema_name │ table_name │ column_name │ ordinal_position │ remarks │ type_name │ is_nullable │
├──────────────┼─────────────┼────────────┼─────────────┼──────────────────┼─────────┼───────────┼─────────────┤
│ main         │ NULL        │ users      │ id          │                1 │ NULL    │ INTEGER   │ true        │
│ main         │ NULL        │ users      │ name        │                2 │ NULL    │ TEXT      │ true        │
│ main         │ NULL        │ users      │ email       │                3 │ NULL    │ TEXT      │ true        │
└──────────────┴─────────────┴────────────┴─────────────┴──────────────────┴─────────┴───────────┴─────────────┘
```

### adbc_schema

Returns the Arrow schema for a specific table, showing field names, Arrow data types, and nullability.

```sql
adbc_schema(connection_id, table_name, [catalog:=], [schema:=]) -> TABLE
```

**Parameters:**

- `connection_id`: Connection handle from `adbc_connect`
- `table_name`: Name of the table to get the schema for
- `catalog` (optional): Catalog containing the table
- `schema` (optional): Database schema containing the table

**Returns:** A table with columns:

- `field_name`: Name of the field/column
- `field_type`: Arrow data type (e.g., "int64", "utf8", "float64", "timestamp[us]")
- `nullable`: Whether the field allows NULL values

**Example:**

```sql
SELECT * FROM adbc_schema(getvariable('conn')::BIGINT, 'users');
```

Output:
```
┌────────────┬────────────┬──────────┐
│ field_name │ field_type │ nullable │
├────────────┼────────────┼──────────┤
│ id         │ int64      │ true     │
│ name       │ utf8       │ true     │
│ email      │ utf8       │ true     │
│ created_at │ timestamp  │ true     │
└────────────┴────────────┴──────────┘
```

**Note:** The `field_type` shows Arrow types, which may differ from the SQL types defined in the table. The mapping depends on the ADBC driver implementation.

## Secrets

DuckDB secrets provide secure credential storage for ADBC connections. Instead of hardcoding credentials in your queries, you can store them in secrets and reference them by name or have them automatically looked up by URI scope.

### Creating Secrets

```sql
-- Create a secret for PostgreSQL
CREATE SECRET my_postgres (
    TYPE adbc,
    SCOPE 'postgresql://prod-server:5432',
    driver 'postgresql',
    uri 'postgresql://prod-server:5432/mydb',
    username 'app_user',
    password 'secret_password'
);

-- Create a secret for SQLite
CREATE SECRET my_sqlite (
    TYPE adbc,
    SCOPE 'sqlite://data',
    driver 'sqlite',
    uri '/var/data/app.db'
);
```

### Secret Parameters

| Parameter | Description |
|-----------|-------------|
| `TYPE` | Must be `adbc` |
| `SCOPE` | URI pattern for automatic secret lookup |
| `driver` | ADBC driver name or path to shared library |
| `uri` | Connection URI passed to the driver |
| `username` | Database username |
| `password` | Database password (automatically redacted in logs) |
| `database` | Database name (if not in URI) |
| `entrypoint` | Custom driver entry point function |

### Using Secrets

There are three ways to use secrets with `adbc_connect`:

**1. Explicit Secret Name**

Reference a secret directly by name:

```sql
-- Use a specific secret
SET VARIABLE conn = (SELECT adbc_connect({'secret': 'my_postgres'}));
```

**2. Automatic URI Scope Lookup**

When you provide a `uri` option, the extension automatically searches for a secret whose `SCOPE` matches:

```sql
-- This will find 'my_postgres' secret because the URI matches its scope
SET VARIABLE conn = (SELECT adbc_connect({
    'uri': 'postgresql://prod-server:5432/mydb'
}));
```

**3. Override Secret Values**

You can reference a secret and override specific options:

```sql
-- Use my_postgres secret but connect to a different database
SET VARIABLE conn = (SELECT adbc_connect({
    'secret': 'my_postgres',
    'uri': 'postgresql://prod-server:5432/other_db'
}));

-- Use secret but with different credentials
SET VARIABLE conn = (SELECT adbc_connect({
    'secret': 'my_postgres',
    'username': 'admin',
    'password': 'admin_pass'
}));
```

### Persistent Secrets

By default, secrets are stored in memory and lost when DuckDB closes. To persist secrets:

```sql
-- Create a persistent secret (stored in ~/.duckdb/secrets/)
CREATE PERSISTENT SECRET my_postgres (
    TYPE adbc,
    SCOPE 'postgresql://prod-server:5432',
    driver 'postgresql',
    uri 'postgresql://prod-server:5432/mydb',
    username 'app_user',
    password 'secret_password'
);
```

### Managing Secrets

```sql
-- List all secrets (passwords are redacted)
SELECT * FROM duckdb_secrets();

-- Drop a secret
DROP SECRET my_postgres;

-- Drop a persistent secret
DROP PERSISTENT SECRET my_postgres;
```

### Security Considerations

- Passwords and sensitive keys (`password`, `auth_token`, `token`, `secret`, `api_key`, `credential`) are automatically redacted when displaying secrets
- Persistent secrets are stored encrypted on disk
- Secrets are scoped to the current DuckDB connection/session

## ADBC Drivers

ADBC drivers are available for many databases. When using driver manifests (see below), you can reference drivers by their short name:

| Driver Name | Database | Developer |
|-------------|----------|-----------|
| `bigquery` | Google BigQuery | ADBC Driver Foundry |
| `duckdb` | DuckDB | DuckDB Foundation |
| `flightsql` | Apache Arrow Flight SQL | Apache Software Foundation |
| `mssql` | Microsoft SQL Server | Columnar |
| `mysql` | MySQL | ADBC Driver Foundry |
| `postgresql` | PostgreSQL | Apache Software Foundation |
| `redshift` | Amazon Redshift | Columnar |
| `snowflake` | Snowflake | Apache Software Foundation |
| `sqlite` | SQLite | Apache Software Foundation |

### Installing Drivers

There are a few options for installing drivers:

1. [Columnar's `dbc`](https://columnar.tech/dbc/) is a command-line tool that makes installing and managing ADBC drivers easy. It automatically creates driver manifests for you.
2. ADBC drivers can be installed from the [Apache Arrow ADBC releases](https://github.com/apache/arrow-adbc/releases) or built from source.
3. On macOS with Homebrew: `brew install apache-arrow-adbc`

### Driver Manifests

Driver manifests allow you to reference ADBC drivers by name (e.g., `'sqlite'`) instead of specifying the full path to the shared library. A manifest is a TOML file that contains metadata about the driver and the path to its shared library.

**Example manifest file (`sqlite.toml`):**
```toml
[driver]
name = "sqlite"
description = "ADBC SQLite Driver"
library = "/usr/local/lib/libadbc_driver_sqlite.dylib"
```

**Manifest Search Locations:**

The extension searches for driver manifests in these locations (in order):

**macOS:**

1. `ADBC_DRIVER_PATH` environment variable (colon-separated paths)
2. `$VIRTUAL_ENV/etc/adbc/drivers` (if in a Python virtual environment)
3. `$CONDA_PREFIX/etc/adbc/drivers` (if in a Conda environment)
4. `~/Library/Application Support/ADBC/Drivers`
5. `/etc/adbc/drivers`

**Linux:**

1. `ADBC_DRIVER_PATH` environment variable (colon-separated paths)
2. `$VIRTUAL_ENV/etc/adbc/drivers` (if in a Python virtual environment)
3. `$CONDA_PREFIX/etc/adbc/drivers` (if in a Conda environment)
4. `~/.config/adbc/drivers`
5. `/etc/adbc/drivers`

**Windows:**

1. `ADBC_DRIVER_PATH` environment variable (semicolon-separated paths)
2. Registry: `HKEY_CURRENT_USER\SOFTWARE\ADBC\Drivers\{name}`
3. `%LOCAL_APPDATA%\ADBC\Drivers`
4. Registry: `HKEY_LOCAL_MACHINE\SOFTWARE\ADBC\Drivers\{name}`

You can also specify additional search paths using the `search_paths` option in `adbc_connect()`.

## Complete Example

```sql
-- Load the extension
LOAD adbc_scanner;

-- Connect to SQLite using driver name (requires installed manifest)
SET VARIABLE sqlite_conn = (SELECT adbc_connect({
    'driver': 'sqlite',
    'uri': '/tmp/example.db'
}));

-- Or connect with explicit driver path
-- SET VARIABLE sqlite_conn = (SELECT adbc_connect({
--     'driver': '/opt/homebrew/lib/libadbc_driver_sqlite.dylib',
--     'uri': '/tmp/example.db'
-- }));

-- Check connection info
SELECT * FROM adbc_info(getvariable('sqlite_conn')::BIGINT);

-- Create a table
SELECT adbc_execute(getvariable('sqlite_conn')::BIGINT,
    'CREATE TABLE IF NOT EXISTS employees (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        department TEXT,
        salary REAL
    )');

-- Insert data
SELECT adbc_execute(getvariable('sqlite_conn')::BIGINT,
    'INSERT INTO employees VALUES
        (1, ''Alice'', ''Engineering'', 95000),
        (2, ''Bob'', ''Sales'', 75000),
        (3, ''Charlie'', ''Engineering'', 105000)');

-- Query with DuckDB operations
SELECT department, AVG(salary) as avg_salary, COUNT(*) as count
FROM adbc_scan(getvariable('sqlite_conn')::BIGINT, 'SELECT * FROM employees')
GROUP BY department
ORDER BY avg_salary DESC;

-- List tables
SELECT * FROM adbc_tables(getvariable('sqlite_conn')::BIGINT);

-- List supported table types
SELECT * FROM adbc_table_types(getvariable('sqlite_conn')::BIGINT);

-- Parameterized query
SELECT * FROM adbc_scan(
    getvariable('sqlite_conn')::BIGINT,
    'SELECT * FROM employees WHERE department = ? AND salary > ?',
    params := row('Engineering', 90000.0)
);

-- Transaction control
SELECT adbc_set_autocommit(getvariable('sqlite_conn')::BIGINT, false);  -- Start transaction
SELECT adbc_execute(getvariable('sqlite_conn')::BIGINT,
    'INSERT INTO employees VALUES (4, ''Diana'', ''Marketing'', 85000)');
SELECT adbc_commit(getvariable('sqlite_conn')::BIGINT);  -- Commit changes
-- Or use: SELECT adbc_rollback(getvariable('sqlite_conn')::BIGINT);  -- To discard changes
SELECT adbc_set_autocommit(getvariable('sqlite_conn')::BIGINT, true);  -- Back to autocommit

-- Clean up
SELECT adbc_disconnect(getvariable('sqlite_conn')::BIGINT);
```

## Error Handling

ADBC functions throw exceptions on errors with descriptive messages:

```sql
-- Invalid connection handle
SELECT * FROM adbc_scan(12345, 'SELECT 1');
-- Error: Invalid Input Error: adbc_scan: Invalid connection handle: 12345

-- SQL syntax error
SELECT adbc_execute(getvariable('conn')::BIGINT, 'INVALID SQL');
-- Error: adbc_execute: Failed to prepare statement: ... [Query: INVALID SQL]

-- Missing driver option
SELECT adbc_connect({'uri': ':memory:'});
-- Error: Invalid Input Error: adbc_connect: 'driver' option is required
```

## Limitations

- ADBC connections are not automatically closed; always call `adbc_disconnect()` when done
- The `rows_affected` count from `adbc_execute` depends on driver support; some drivers return 0 for all operations
- Parameterized queries in `adbc_scan` require the `params` named parameter syntax

## Building from Source

See the [development documentation](../CLAUDE.md) for build instructions.

```bash
# Clone with submodules
git clone --recurse-submodules https://github.com/your-repo/adbc_scanner.git

# Set up vcpkg
export VCPKG_TOOLCHAIN_PATH=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake

# Build
GEN=ninja make

# Test
make test
```
