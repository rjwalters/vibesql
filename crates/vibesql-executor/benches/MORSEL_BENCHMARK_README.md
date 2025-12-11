# Morsel Operation Benchmark

This benchmark provides **targeted, isolated benchmarks** for each SQL operation that uses morsel parallelism. Unlike TPC-H queries which mix multiple operations, these benchmarks isolate each operation to clearly measure the effect of morsel size on that specific operation.

## Philosophy

The goal is to understand:
- Which operations benefit most from smaller morsel sizes (better cache locality)
- Which operations benefit from larger morsel sizes (lower scheduling overhead)
- How work-stealing effectiveness varies by operation type
- Thread scaling characteristics per operation

## Operations Benchmarked

| Operation | Function | Description |
|-----------|----------|-------------|
| **Filter** | `morsel_parallel_filter` | WHERE clause evaluation |
| **Group By** | `morsel_parallel_group` | Hash-based GROUP BY aggregation |
| **Hash Join** | `build_hash_table_parallel` + `morsel_parallel_probe_sqlvalue` | Build and probe phases |
| **Join + Filter** | Combined join and filter | Tests filter on join output |
| **Sort** | `par_sort_by` | ORDER BY operations |
| **Aggregate** | Parallel fold/reduce | COUNT, SUM, AVG without GROUP BY |
| **Scan** | `parallel_scan_materialize` | Table materialization |

## Quick Start

```bash
# Build
cargo build --release --package vibesql-executor --bench morsel_operation_benchmark

# Run all operations
./target/release/deps/morsel_operation_benchmark-*

# Run specific operation
OPERATION_FILTER=filter ./target/release/deps/morsel_operation_benchmark-*
OPERATION_FILTER=groupby ./target/release/deps/morsel_operation_benchmark-*
OPERATION_FILTER=join ./target/release/deps/morsel_operation_benchmark-*
OPERATION_FILTER=sort ./target/release/deps/morsel_operation_benchmark-*
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OPERATION_FILTER` | (all) | Run specific operation: `filter`, `groupby`, `join`, `join_filter`, `sort`, `agg`, `scan` |
| `ROW_COUNTS` | `100000,500000,1000000` | Comma-separated row counts to test |
| `MORSEL_SIZES` | `1024,2048,4096,8192,16384,32768,50000` | Comma-separated morsel sizes |
| `MAX_THREADS` | `16` | Maximum thread count to test |
| `WARMUP_ITERATIONS` | `2` | Warmup runs before timing |
| `BENCHMARK_ITERATIONS` | `5` | Timed iterations |
| `MORSEL_DEBUG` | (off) | Set to `1` for detailed morsel execution logging |

### Example: Quick Smoke Test

```bash
ROW_COUNTS=50000 \
MORSEL_SIZES=2048,50000 \
WARMUP_ITERATIONS=1 \
BENCHMARK_ITERATIONS=3 \
MAX_THREADS=4 \
./target/release/deps/morsel_operation_benchmark-*
```

### Example: Comprehensive Analysis

```bash
ROW_COUNTS=100000,500000,1000000 \
MORSEL_SIZES=1024,2048,4096,8192,16384,32768,50000 \
WARMUP_ITERATIONS=2 \
BENCHMARK_ITERATIONS=5 \
MAX_THREADS=16 \
./target/release/deps/morsel_operation_benchmark-*
```

## Output Format

Results are displayed as a matrix of morsel sizes vs thread counts:

```
=== Filter Operation (WHERE clause) ===

--- 500000 rows ---

  Query: filter_50pct
             2K       8K      50K
       -------- -------- --------
   1T  890.62µs 867.97µs 869.14µs
   2T  799.36µs 888.88µs 895.08µs
   4T  881.31µs 914.14µs 867.53µs
```

## Synthetic Data Generator

The benchmark uses a purpose-built data generator that creates tables optimized for isolating each operation:

### Filter Data
- 4 columns: `ID`, `CATEGORY` (0-99), `VALUE`, `FLAG` (0/1)
- Tests various selectivities: 50%, 10%, 1%, compound predicates

### Group By Data
- 3 columns: `ID`, `GROUP_KEY`, `VALUE`
- Configurable cardinality: 10, 1000, or 10000 groups

### Join Data
- Build table: `ID`, `VALUE`
- Probe table: `ID`, `BUILD_ID`, `DATA`
- Configurable build:probe ratios (1:10, 1:100)

### Sort Data
- 3 columns: `ID`, `SORT_KEY` (reverse sorted), `CATEGORY`
- Tests integer, string, and multi-column sorts

### Join Filter Data
Uses the same join tables but with varying post-join filter selectivities (1%, 50%, 90%).

## Interpreting Results

### Cache Size Reference

| Cache | Size | Approx Rows (100 bytes/row) |
|-------|------|----------------------------|
| L1 | ~32KB | ~300 rows |
| L2 | ~256KB | ~2,500 rows |
| L3 | ~8-32MB | ~80,000-320,000 rows |

### What to Look For

1. **Morsel size sweet spot**: Find where smaller sizes improve cache locality without excessive scheduling overhead

2. **Thread scaling**: Calculate speedup = T1_time / Tn_time
   - Good scaling: speedup approaches thread count
   - Diminishing returns suggest memory bandwidth limits

3. **Operation characteristics**:
   - **Filter**: Often benefits from smaller morsels (SIMD vectorization, L2 cache fits)
   - **Group By**: Depends on cardinality (low = small hash table, high = memory pressure)
   - **Join**: Build phase may have different optimal size than probe phase
   - **Sort**: Larger morsels reduce merge overhead

## Comparison with morsel_scaling Benchmark

| Aspect | `morsel_scaling` | `morsel_operation_benchmark` |
|--------|------------------|------------------------------|
| **Data Source** | TPC-H tables | Synthetic generated |
| **Query Types** | TPC-H Q1, Q5, Q6 variants | Isolated per-operation |
| **Isolation** | Mixed operations | Single operation focus |
| **Use Case** | Overall scaling validation | Per-operation tuning |
| **Customization** | Limited | Highly configurable |

## Extending the Benchmark

To add a new operation benchmark:

1. Add a data generator method to `DataGenerator`
2. Create a `bench_X_operation()` function
3. Add the filter case to `main()`
4. Update this README

## Related Files

- `morsel_scaling.rs` - TPC-H based scaling benchmarks
- `hash_join_parallel.rs` - Detailed hash join benchmarks
- `parallel_sort.rs` - Detailed sort benchmarks
- `crates/vibesql-executor/src/select/morsel.rs` - Morsel primitives
- `crates/vibesql-executor/src/select/parallel.rs` - Parallel config
