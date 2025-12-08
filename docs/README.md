# DuckDB ADBC Scanner Extension

The ADBC Scanner extension by [Query.Farm](https://query.farm) enables DuckDB to connect to external databases using [Arrow Database Connectivity (ADBC)](https://arrow.apache.org/adbc/), a column-oriented API standard for database access. ADBC provides efficient data transfer using Apache Arrow's columnar format.

## Installation

```sql
INSTALL adbc_scanner FROM community;
LOAD adbc_scanner;
```

## Quick Start

```sql
-- Connect to a SQLite database
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
- `driver`: Path to the ADBC driver shared library

**Common Options:**
- `uri`: Connection URI (driver-specific)
- `username`: Database username
- `password`: Database password
- `entrypoint`: Driver entrypoint function name (rarely needed)

**Returns:** A connection handle (BIGINT) used with other ADBC functions.

**Examples:**

```sql
-- Using STRUCT syntax (preferred)
SELECT adbc_connect({
    'driver': '/path/to/libadbc_driver_sqlite.dylib',
    'uri': '/path/to/database.db'
});

-- Using MAP syntax
SELECT adbc_connect(MAP {
    'driver': '/path/to/libadbc_driver_postgresql.dylib',
    'uri': 'postgresql://localhost:5432/mydb',
    'username': 'user',
    'password': 'pass'
});

-- Store connection handle in a variable for reuse
SET VARIABLE conn = (SELECT adbc_connect({
    'driver': '/path/to/driver.dylib',
    'uri': ':memory:'
}));
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

## ADBC Drivers

ADBC drivers are available for many databases. Here are some common ones:

• `bigquery` - An ADBC driver for Google BigQuery developed by the ADBC Driver Foundry
• `duckdb` - An ADBC driver for DuckDB developed by the DuckDB Foundation - **but this would be kind of silly to use in DuckDB**.
• `flightsql` - An ADBC driver for Apache Arrow Flight SQL developed under the Apache Software Foundation
• `mssql` - An ADBC driver for Microsoft SQL Server developed by Columnar
• `mysql` - An ADBC Driver for MySQL developed by the ADBC Driver Foundry
• `postgresql` - An ADBC driver for PostgreSQL developed under the Apache Software Foundation
• `redshift` - An ADBC driver for Amazon Redshift developed by Columnar
• `snowflake` - An ADBC driver for Snowflake developed under the Apache Software Foundation
• `sqlite` - An ADBC driver for SQLite developed under the Apache Software Foundation

### Installing Drivers

There are a few options for installing drivers:

1. [Columnar's `dbc`](https://columnar.tech/dbc/) is a command-line tool that makes installing and managing ADBC drivers easy.
2. ADBC drivers can be installed from the [Apache Arrow ADBC releases](https://github.com/apache/arrow-adbc/releases) or built from source.
3. On macOS with Homebrew: ```brew install apache-arrow-adbc```

## Complete Example

```sql
-- Load the extension
LOAD adbc_scanner;

-- Connect to SQLite
SET VARIABLE sqlite_conn = (SELECT adbc_connect({
    'driver': '/opt/homebrew/lib/libadbc_driver_sqlite.dylib',
    'uri': '/tmp/example.db'
}));

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
