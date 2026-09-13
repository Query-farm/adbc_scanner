<p align="center">
  <a href="https://query.farm">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://query.farm/media-kit/logo/wordmark-dark.svg">
      <img alt="Query.Farm" src="https://query.farm/media-kit/logo/wordmark-light.svg" height="64">
    </picture>
  </a>
</p>

# DuckDB ADBC Extension (`adbc`)

[![DuckDB](https://img.shields.io/badge/DuckDB-community_extension-fdf1e0?logo=duckdb&logoColor=fff000)](https://duckdb.org/community_extensions/extensions/adbc_scanner.html)
[![v1.5 build](https://github.com/Query-farm/adbc_scanner/actions/workflows/MainDistributionPipeline.yml/badge.svg?branch=v1.5)](https://github.com/Query-farm/adbc_scanner/actions/workflows/MainDistributionPipeline.yml?query=branch%3Av1.5)

Query [Snowflake](https://www.snowflake.com), [PostgreSQL](https://www.postgresql.org), [MySQL](https://www.mysql.com), [Trino](https://trino.io), [Microsoft SQL Server](https://www.microsoft.com/sql-server), [Apache DataFusion](https://datafusion.apache.org), [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html), and any other system with an [ADBC driver](https://arrow.apache.org/adbc/) directly from [DuckDB](https://duckdb.org).

> The extension registers as `adbc_scanner` internally; its functions and the `ATTACH ... (TYPE adbc)` storage type are exposed under the `adbc` name.

## Documentation

Full documentation, including installation, driver setup, the `ATTACH` storage layer, the function reference, secrets, connection profiles, and cookbook examples, is available at:

**[https://query.farm/products/extensions/adbc_scanner](https://query.farm/products/extensions/adbc_scanner)**

## Installation

```sql
INSTALL adbc_scanner FROM community;
LOAD adbc_scanner;
```

## Remote Query Pushdown

On DuckDB 2.0, `ATTACH ... (TYPE adbc)` can send a complete query plan to drivers with a compatible SQL dialect.
PostgreSQL, MySQL, SQLite, and DuckDB currently have structured-query profiles. Filters, projections, aggregates,
supported joins, ordering, limits, and nested queries can execute as one remote statement instead of separate table
scans. Unsupported expressions fall back to DuckDB's regular ADBC scan path.

`EXPLAIN` includes the exact generated statement under `Remote SQL`. Pushdown can be disabled per attachment when
comparing plans or troubleshooting:

```sql
ATTACH 'postgresql://user:password@host/database' AS pg (
    TYPE adbc,
    driver 'postgresql',
    query_pushdown false
);
```

## Development

For instructions on building the extension from source and running its tests, see [docs/BUILDING.md](docs/BUILDING.md).
