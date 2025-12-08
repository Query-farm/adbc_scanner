# DuckDB ADBC Extension

The ADBC extension by [Query.Farm](https://query.farm) enables DuckDB to connect to external databases using [Arrow Database Connectivity (ADBC)](https://arrow.apache.org/adbc/), a column-oriented API standard for database access. ADBC provides efficient data transfer using Apache Arrow's columnar format.

## Installation

```sql
INSTALL adbc FROM community;
LOAD adbc;
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

## ADBC Drivers

ADBC drivers are available for many databases. Here are some common ones:

| Database | Driver | Notes |
|----------|--------|-------|
| SQLite | `libadbc_driver_sqlite.dylib` | Lightweight, file-based |
| PostgreSQL | `libadbc_driver_postgresql.dylib` | Full-featured RDBMS |
| Snowflake | `libadbc_driver_snowflake.dylib` | Cloud data warehouse |
| Flight SQL | `libadbc_driver_flightsql.dylib` | Arrow Flight SQL servers |
| DuckDB | `libadbc_driver_duckdb.dylib` | Connect to other DuckDB instances |

### Installing Drivers

ADBC drivers can be installed from the [Apache Arrow ADBC releases](https://github.com/apache/arrow-adbc/releases) or built from source.

On macOS with Homebrew:
```bash
brew install apache-arrow-adbc
```

## Complete Example

```sql
-- Load the extension
LOAD adbc;

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
- Connection handles are process-global; be careful with concurrent access from multiple threads

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
