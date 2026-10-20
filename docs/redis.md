# Redis through ADBC

The [Redis ADBC driver](https://github.com/alberttwong/redis-adbc-driver)
provides SQL queries and Arrow ingestion over Redis HASHes and Redis Search.
It works with `adbc_scanner` through the normal `TYPE adbc` attachment.

## Install the driver

Download and unpack the native driver archive for your platform from the
[driver releases](https://github.com/alberttwong/redis-adbc-driver/releases).
Install its manifest with `./install.sh --user`, keeping the unpacked library
in place. Alternatively, use the absolute library path in the `driver` option
below (`.so` on Linux, `.dylib` on macOS).

A Python release wheel also bundles the native driver. Installing the wheel
alone does not install a manifest for DuckDB; run
`python -m adbc_driver_redis install-manifest --user` afterward. See the
[driver installation guide](https://github.com/alberttwong/redis-adbc-driver/blob/main/docs/python-integrations.md#install-and-discover-the-driver).

The server must provide Redis Search (the Query Engine). Redis Cloud, Redis
Software, and Redis 8.x with Search are supported by the driver. Redis Flex,
Valkey, and Redis 7.2 derivatives are unsupported. Use `rediss://` for TLS and
give the Redis user the permissions described in the
[driver's ACL guide](https://github.com/alberttwong/redis-adbc-driver#acl-users).

## Read and join

```sql
INSTALL adbc_scanner FROM community;
LOAD adbc_scanner;

ATTACH 'redis://localhost:6379/0' AS r
    (TYPE adbc, driver 'redis', READ_ONLY);

-- public is the driver's default schema.
SELECT * FROM r.public.orders LIMIT 10;

-- Send SQL explicitly to the Redis driver's query engine.
SELECT * FROM adbc_scan('r',
    'SELECT country, COUNT(*) AS n FROM public.orders GROUP BY country');

-- DuckDB plans the local join.
SELECT o.id, o.country, c.label
FROM r.public.orders AS o
JOIN (VALUES ('US', 'United States'), ('CA', 'Canada')) AS c(code, label)
    ON o.country = c.code;

DETACH r;
```

These queries assume an existing driver-managed `public.orders` table with
`id` and `country` columns. Arbitrary existing HASHes need to be copied or
adopted with the driver's `redis-adbc` utility before they appear as tables.
The driver can configure a different default schema; use its actual schema.
`READ_ONLY` rejects writes through the attachment; Redis ACLs control server
permissions independently.

## Write in autocommit

Use a dedicated database for this example. Run without a surrounding `BEGIN`.

```sql
ATTACH 'redis://localhost:6379/0' AS r (TYPE adbc, driver 'redis');

CALL adbc_execute('r',
    'CREATE TABLE public.duckdb_adbc_example (id integer, country text)');
SELECT * FROM adbc_insert(
    'r', 'duckdb_adbc_example',
    (SELECT * FROM (VALUES (1, 'US'), (2, 'CA')) AS source(id, country)),
    mode := 'append'
);
SELECT * FROM r.public.duckdb_adbc_example ORDER BY id;

CALL adbc_execute('r', 'DROP TABLE public.duckdb_adbc_example');
DETACH r;
```

The Redis driver does not implement multi-statement transactions. Writes inside
`BEGIN` fail; `ROLLBACK` cannot undo autocommit writes. A failing autocommit
statement can leave partial changes. Current `adbc_scanner` builds also support
`INSERT INTO r.public.t` and `CREATE TABLE r.public.t AS SELECT ...` outside
explicit transactions; older builds require `adbc_insert` / `adbc_execute`.

SQL passed to `adbc_scan` executes in the Redis driver, which can push supported
predicates and aggregates to Search. SQL over an attached table is planned by
DuckDB; inspect `EXPLAIN` and measure your query before assuming full remote
execution. For driver capabilities and limitations, see its
[DuckDB integration guide](https://github.com/alberttwong/redis-adbc-driver/blob/main/docs/duckdb.md).
