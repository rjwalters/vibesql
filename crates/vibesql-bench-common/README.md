# vibesql-bench-common

Shared benchmark infrastructure for VibeSQL benchmarks.

## Overview

This crate provides the common benchmark code used by the `vibesql-executor` and `vibesql-server` benchmark suites: portable data generators, schema definitions, and timing/statistics infrastructure for industry-standard database benchmarks.

It is primarily internal plumbing for the VibeSQL workspace's benchmark harness, published to crates.io for dependency-graph completeness. External users are welcome to reuse the generators, but the API tracks the needs of VibeSQL's own benchmarks.

## Features

- **TPC-C**: OLTP transaction processing benchmark (data generators and types)
- **TPC-H**: Decision support (OLAP) benchmark
- **Sysbench**: MySQL-compatible micro-benchmarks
- **Harness**: timing and statistics infrastructure shared across suites

### Cargo features

Engine-specific schema loading is behind optional feature flags (all off by default, so the core generators stay dependency-light):

- `vibesql` — VibeSQL schema loading (pulls in `vibesql-types`, `vibesql-ast`, `vibesql-catalog`, `vibesql-storage`)
- `sqlite` — comparison-engine schema loading via `rusqlite`
- `duckdb` — comparison-engine schema loading via `duckdb`
- `mysql` — comparison-engine schema loading via `mysql`

## Usage

Add this to your `Cargo.toml` (typically as a dev-dependency):

```toml
[dev-dependencies]
vibesql-bench-common = "0.2"
```

From the VibeSQL repository, the benchmark suites built on this crate run via:

```bash
make benchmark-tpch       # TPC-H decision support (22 queries)
make benchmark-tpcc       # TPC-C OLTP transactions
make benchmark-sysbench   # Sysbench micro-benchmarks
```

## Documentation

- [API Documentation](https://docs.rs/vibesql-bench-common)
- [Main VibeSQL Repository](https://github.com/rjwalters/vibesql)

## License

This project is licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
- MIT License ([LICENSE-MIT](../../LICENSE-MIT))

at your option.
