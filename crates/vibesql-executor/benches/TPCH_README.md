# TPC-H Native Rust Benchmarks

## Overview

This benchmark suite provides **apples-to-apples SQL engine performance comparison** across:
- **VibeSQL** (native Rust API - no PyO3 overhead)
- **SQLite** (via rusqlite - native C bindings)
- **DuckDB** (via duckdb-rs - native bindings)

All databases run **in-memory** with **no Python/FFI overhead**, ensuring pure SQL engine comparison.

## Why Rust-Native Benchmarks?

### The PyO3 Problem

When benchmarking VibeSQL from Python via PyO3 bindings:
```
VibeSQL (Python) = SQL Engine + PyO3 Serialization + Python GIL + FFI overhead
SQLite (Python)  = SQL Engine + (minimal C-Python overhead)
```

This creates an **unfair comparison** - you're measuring binding overhead, not SQL performance.

### The Rust-Native Solution

With Rust-native benchmarks:
```
VibeSQL (Rust) = SQL Engine only
SQLite (Rust)  = SQL Engine only (via rusqlite)
DuckDB (Rust)  = SQL Engine only (via duckdb-rs)
```

This gives you **true SQL engine performance** without language binding artifacts.

## Engine Selection

### Runtime Engine Filtering

Use the `ENGINE_FILTER` environment variable to run only specific engines without recompiling:

```bash
# Run VibeSQL only (fastest iteration)
ENGINE_FILTER=vibesql ./target/release/deps/tpch_benchmark-*

# Run VibeSQL and SQLite only
ENGINE_FILTER=vibesql,sqlite ./target/release/deps/tpch_benchmark-*

# Run all embedded engines
ENGINE_FILTER=vibesql,sqlite,duckdb ./target/release/deps/tpch_benchmark-*
```

This eliminates redundant data loading and significantly reduces benchmark runtime.

### MySQL Note

MySQL is a client-server database and is tested separately via the server benchmark:
```bash
# Use server benchmark for MySQL comparison
make benchmark-servers
```

For embedded engine comparisons (VibeSQL, SQLite, DuckDB), use the standard benchmark targets.

## Quick Start

### Run All TPC-H Benchmarks

```bash
# Run all benchmarks at SF 0.01 (fast iteration)
cargo bench --bench tpch_benchmark

# Generate HTML reports
cargo bench --bench tpch_benchmark -- --save-baseline main

# Compare against baseline
cargo bench --bench tpch_benchmark -- --baseline main
```

### Run Specific Queries

```bash
# Run only Q1 (pricing summary)
cargo bench --bench tpch_benchmark -- q1

# Run only Q6 (revenue forecast)
cargo bench --bench tpch_benchmark -- q6

# Run only VibeSQL benchmarks
cargo bench --bench tpch_benchmark -- vibesql

# Run only SQLite benchmarks
cargo bench --bench tpch_benchmark -- sqlite

# Run only DuckDB benchmarks
cargo bench --bench tpch_benchmark -- duckdb
```

## Scale Factors

The benchmark supports multiple TPC-H scale factors:

| Scale Factor | Data Size | Customer Rows | Orders Rows | LineItem Rows | Use Case |
|--------------|-----------|---------------|-------------|---------------|----------|
| SF 0.01      | ~10 MB    | 1,500         | 15,000      | 60,000        | Fast iteration, CI |
| SF 0.1       | ~100 MB   | 15,000        | 150,000     | 600,000       | Realistic testing |
| SF 1.0       | ~1 GB     | 150,000       | 1,500,000   | 6,000,000     | Full benchmark |

**Default**: SF 0.01 for fast iteration

To change scale factor, edit the benchmark code:
```rust
for &sf in &[0.01, 0.1, 1.0] {  // Add more scale factors
    let db = load_vibesql(sf);
    // ...
}
```

## TPC-H Queries Implemented

### ✅ Complete Coverage: All 22 Queries

**Basic Aggregation & Filtering (Q1, Q6)**
1. **Q1: Pricing Summary Report** - Aggregate with GROUP BY, ORDER BY
2. **Q6: Forecasting Revenue Change** - WHERE filters, BETWEEN, SUM

**Advanced Joins (Q2-Q5, Q7-Q10)**
3. **Q2: Minimum Cost Supplier** - 3-table JOIN, ORDER BY, LIMIT
4. **Q3: Shipping Priority** - 3-table JOIN with aggregation
5. **Q4: Order Priority Checking** - Correlated EXISTS subquery
6. **Q5: Local Supplier Volume** - 6-table JOIN, complex filtering
7. **Q7: Volume Shipping** - 6-table JOIN with SUBSTR and date filtering
8. **Q8: National Market Share** - 7-table JOIN with CASE expressions
9. **Q9: Product Type Profit Measure** - 4-table JOIN with aggregation
10. **Q10: Returned Item Reporting** - 4-table JOIN, TOP-N with LIMIT

**Subqueries & HAVING (Q11-Q13)**
11. **Q11: Important Stock Identification** - Subquery in HAVING clause
12. **Q12: Shipping Modes Priority** - CASE aggregation with date logic
13. **Q13: Customer Distribution** - LEFT OUTER JOIN with subquery

**Complex Analytics (Q14-Q22)**
14. **Q14: Promotion Effect** - Conditional aggregation with CASE
15. **Q15: Top Supplier** - Nested subqueries with MAX
16. **Q16: Parts/Supplier Relationship** - NOT IN subquery, DISTINCT
17. **Q17: Small-Quantity-Order Revenue** - Correlated subquery in WHERE
18. **Q18: Large Volume Customer** - GROUP BY with HAVING
19. **Q19: Discounted Revenue** - Complex OR conditions
20. **Q20: Potential Part Promotion** - IN subquery with GROUP BY/HAVING
21. **Q21: Suppliers Who Kept Orders Waiting** - Multi-table EXISTS
22. **Q22: Global Sales Opportunity** - SUBSTR, NOT EXISTS, nested subquery

**To add more queries**: Follow the pattern in `tpch_benchmark.rs`:

```rust
const TPCH_QX: &str = r#"
    SELECT ... FROM ... WHERE ...
"#;

fn benchmark_qX_vibesql(c: &mut Criterion) {
    let db = load_vibesql(0.01);
    c.bench_function("tpch_qX_vibesql", |b| {
        b.iter(|| {
            let stmt = Parser::parse_sql(TPCH_QX).unwrap();
            // ... execute and consume results
        });
    });
}

// Add similar functions for _sqlite and _duckdb
// Then add to criterion_group!() at bottom
```

## Understanding Results

### Criterion Output

```
tpch_q1/vibesql/SF0.01  time:   [25.123 ms 25.456 ms 25.789 ms]
tpch_q1/sqlite/SF0.01   time:   [15.234 ms 15.456 ms 15.678 ms]
tpch_q1/duckdb/SF0.01   time:   [8.123 ms 8.234 ms 8.345 ms]
```

**Interpretation**:
- VibeSQL: 25.5 ms (1.65x slower than SQLite, 3.1x slower than DuckDB)
- SQLite: 15.5 ms (baseline)
- DuckDB: 8.2 ms (1.9x faster than SQLite)

### HTML Reports

Criterion generates detailed HTML reports:
```bash
open target/criterion/tpch_q1/report/index.html
```

Shows:
- Performance over time
- Violin plots of distribution
- Comparison to baseline
- Statistical confidence intervals

## Performance Targets

Based on the benchmark strategy document (docs/performance/BENCHMARK_STRATEGY.md):

| Performance Ratio (vs SQLite) | Assessment | Action |
|-------------------------------|------------|--------|
| < 1.5x                        | Competitive | Ship it! |
| 1.5x - 2.5x                   | Acceptable | Document trade-offs |
| 2.5x - 5.0x                   | Needs improvement | Identify bottlenecks |
| > 5.0x                        | Performance issue | Investigate immediately |

**Current expectations** (pre-optimization):
- **Simple queries**: 1.5-2x slower (parsing overhead)
- **Joins**: 2.5-3x slower (no join optimization yet)
- **Aggregations**: 1.5-2x slower (reasonable)

## CI Integration

### GitHub Actions Example

```yaml
# .github/workflows/performance.yml
name: TPC-H Performance Benchmarks

on:
  push:
    branches: [main]
  pull_request:

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable

      - name: Run TPC-H benchmarks
        run: |
          cargo bench --bench tpch_benchmark -- --save-baseline pr-${{ github.event.number }}

      - name: Compare to main
        run: |
          cargo bench --bench tpch_benchmark -- --baseline main

      - name: Upload results
        uses: actions/upload-artifact@v3
        with:
          name: criterion-results
          path: target/criterion/
```

### Regression Detection

Set up performance regression alerts:
```bash
# After each run, check for regressions > 10%
cargo bench --bench tpch_benchmark -- --baseline main --save-baseline current

# Script can parse Criterion output to detect regressions
# Exit code 1 if any query is >10% slower
./scripts/check_performance_regression.sh
```

## Troubleshooting

### Compilation Errors

**Error**: `cannot find type rusqlite in scope`
```bash
# Fix: Ensure dev-dependencies are correct
cargo update
cargo clean
cargo check --benches
```

**Error**: `duckdb not found`
```bash
# DuckDB requires bundled feature for static linking
# Already configured in Cargo.toml:
# duckdb = { version = "1.1", features = ["bundled"] }
```

### Runtime Errors

**Error**: `failed to create table`
- Check schema compatibility between databases
- VibeSQL may have stricter SQL:1999 compliance
- Adjust queries to use compatible syntax

**Error**: `out of memory`
- Reduce scale factor (use SF 0.01 instead of SF 1.0)
- Close other applications
- Increase system memory limits

### Benchmark Issues

**Results vary wildly between runs**:
```bash
# Increase measurement time for stability
# Edit benchmark code:
group.measurement_time(Duration::from_secs(30));  // Longer sampling
group.sample_size(100);  // More iterations
```

**Benchmarks take too long**:
```bash
# Use smaller scale factor
# Edit benchmark code to use SF 0.01
# Or run specific queries only:
cargo bench --bench tpch_benchmark -- q6  # Fastest query
```

## Data Generation

### TPC-H Data Generator

The benchmark includes a built-in TPC-H data generator (no external tools needed):

- **Deterministic**: Uses seed 42 for reproducibility
- **Spec-compliant**: Matches official TPC-H schema
- **In-memory**: No .tbl file generation
- **Configurable**: Scale factor adjustable at runtime

### Schema

Tables generated:
- `region` (5 rows, reference data)
- `nation` (25 rows, reference data)
- `supplier` (SF × 10,000 rows)
- `customer` (SF × 150,000 rows)
- `orders` (SF × 1,500,000 rows)
- `lineitem` (SF × 6,000,000 rows)

## Comparison to Python Benchmarks

You can run **both** Python and Rust benchmarks to understand PyO3 overhead:

### Python (with PyO3 overhead)
```bash
cd benchmarks/
pytest test_tpch_queries.py --benchmark-only
```

### Rust (native, no overhead)
```bash
cargo bench --bench tpch_benchmark
```

### Calculate PyO3 Overhead
```
PyO3 Overhead = (Python time - Rust time) / Rust time × 100%

Example:
  Python: 35ms
  Rust: 25ms
  Overhead: (35 - 25) / 25 = 40% overhead from PyO3
```

## Next Steps

### Phase 1: Complete Query Suite
- [ ] Implement all 22 TPC-H queries
- [ ] Test at SF 0.01, 0.1, 1.0
- [ ] Generate baseline performance report

### Phase 2: Analysis
- [ ] Identify bottleneck queries
- [ ] Profile slow queries with flamegraph
- [ ] Document performance characteristics

### Phase 3: Optimization (Post-100% Compliance)
- [ ] Implement hash joins
- [ ] Add index support
- [ ] Optimize aggregation algorithms
- [ ] Re-run benchmarks to measure improvement

## References

- [TPC-H Specification](http://www.tpc.org/tpch/)
- [VibeSQL Benchmark Strategy](../../../docs/performance/BENCHMARK_STRATEGY.md)
- [Criterion.rs Documentation](https://bheisler.github.io/criterion.rs/book/)
- [rusqlite Documentation](https://docs.rs/rusqlite/)
- [duckdb-rs Documentation](https://docs.rs/duckdb/)

## Contributing

When adding new TPC-H queries:

1. Add SQL query constant (follow naming: `TPCH_Q##`)
2. Implement 3 benchmark functions: `_vibesql`, `_sqlite`, `_duckdb`
3. Add to `criterion_group!()` at bottom
4. Test with `cargo bench --bench tpch_benchmark -- q##`
5. Document query purpose and what it tests
6. Update this README with query description

**Pull requests welcome** for remaining TPC-H queries!
