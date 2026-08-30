<p align="center">
  <a href="https://query.farm">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://query.farm/media-kit/logo/wordmark-dark.svg">
      <img alt="Query.Farm" src="https://query.farm/media-kit/logo/wordmark-light.svg" height="64">
    </picture>
  </a>
</p>

# DuckDB ADBC Scanner Extension

[![DuckDB](https://img.shields.io/badge/DuckDB-community_extension-fdf1e0?logo=duckdb&logoColor=fff000)](https://duckdb.org/community_extensions/extensions/adbc_scanner.html)
[![v1.5 build](https://github.com/Query-farm/adbc-scanner/actions/workflows/MainDistributionPipeline.yml/badge.svg?branch=v1.5)](https://github.com/Query-farm/adbc-scanner/actions/workflows/MainDistributionPipeline.yml?query=branch%3Av1.5)

The **ADBC Scanner** extension by [Query.Farm](https://query.farm) enables DuckDB to connect to external databases using [Arrow Database Connectivity (ADBC)](https://arrow.apache.org/adbc/), a column-oriented API standard for database access. ADBC provides efficient data transfer using Apache Arrow's columnar format.

## Documentation

Full documentation, including installation, usage, the function reference, and cookbook examples, is available at:

**[https://query.farm/products/extensions/adbc_scanner](https://query.farm/products/extensions/adbc_scanner)**

## Installation

```sql
INSTALL adbc_scanner FROM community;
LOAD adbc_scanner;
```

## Development

For instructions on building the extension from source and running its tests, see [BUILDING.md](BUILDING.md).
