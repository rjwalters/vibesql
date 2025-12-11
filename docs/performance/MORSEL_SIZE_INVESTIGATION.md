# Morsel Size vs SIMD Efficiency Investigation

**Issue**: #4257
**Status**: Investigation Complete
**Date**: 2025-12-10

## Summary

This document presents findings from profiling and benchmarking morsel size configurations
to determine whether the current 50K row default is optimal for SIMD utilization.

## Background

### Current Configuration

VibeSQL uses morsel-driven parallelism with work-stealing, configured in `crates/vibesql-executor/src/select/morsel.rs`:

| Parameter | Value | Notes |
|-----------|-------|-------|
| `DEFAULT_MORSEL_SIZE` | 50,000 | Current default |
| `MIN_MORSEL_SIZE` | 10,000 | Minimum to amortize overhead |
| `MAX_MORSEL_SIZE` | 100,000 | Maximum for load balancing |
| `TARGET_CACHE_BYTES` | 2MB | L3 cache target |

### DuckDB Comparison

DuckDB uses 2,048 rows per columnar vector, optimized for:
- L1/L2 cache efficiency (2K rows * 100 bytes ≈ 200KB fits in L2)
- SIMD register utilization (AVX-512 operates on 64-byte blocks)

## Benchmark Results

### Configuration

- **Hardware**: Apple Silicon (results vary by architecture)
- **Dataset**: TPC-H Scale Factor 0.1 (600K lineitem rows)
- **Queries Tested**: Q1 (aggregation), Q6 (filter), Q5 (6-way join)
- **Iterations**: 5 timed runs after 2 warmup runs

### Results Table

| Morsel Size | Q1 (agg) | Q6 (filter) | Q5 (join) |
|-------------|----------|-------------|-----------|
| 1,024       | 1.37s    | 9.53ms      | 318ms     |
| **2,048**   | **1.34s**| 9.53ms      | **318ms** |
| 4,096       | 1.38s    | 9.59ms      | **318ms** |
| **8,192**   | 1.38s    | **9.13ms**  | 320ms     |
| 16,384      | 1.39s    | 9.37ms      | 326ms     |
| 32,768      | 1.39s    | 9.30ms      | 320ms     |
| 50,000      | 1.40s    | 9.24ms      | 320ms     |
| 100,000     | 1.42s    | 9.68ms      | 320ms     |

### Key Observations

1. **Q1 (Heavy Aggregation)**:
   - 2K rows shows ~4% improvement over 50K default (1.34s vs 1.40s)
   - Likely benefits from better L2 cache locality during aggregation

2. **Q6 (Filter-Heavy)**:
   - 8K rows shows slight improvement (9.13ms vs 9.24ms)
   - Mid-range sizes balance cache efficiency with scheduling overhead

3. **Q5 (Complex Join)**:
   - All sizes show similar performance (318-326ms)
   - Join operations dominated by hash table construction/probing
   - Morsel size has minimal impact on join-heavy workloads

## Analysis

### Cache Hierarchy Effects

| Morsel Size | Data Size (~100 bytes/row) | Cache Level |
|-------------|---------------------------|-------------|
| 2K          | ~200KB                    | Fits L2     |
| 8K          | ~800KB                    | Fits L3 slice |
| 50K         | ~5MB                      | Fills L3    |

### Trade-offs

**Smaller Morsels (1K-8K)**:
- Better L1/L2 cache utilization
- More SIMD-friendly batch sizes
- Higher scheduling overhead
- More work-stealing opportunities

**Larger Morsels (50K-100K)**:
- Lower scheduling overhead
- Better L3 amortization for sequential scans
- Less work-stealing flexibility
- Worse cache locality for random access

## Recommendations

### 1. Current Default is Reasonable

The 50K default provides good performance across workload types. The improvements from
smaller sizes are marginal (2-5%) and may not justify the increased complexity.

### 2. Consider Workload-Adaptive Sizing

For maximum performance, consider:
- **Aggregation-heavy queries**: Use 2K-4K morsels
- **Filter-heavy queries**: Use 8K morsels
- **Join-heavy queries**: Morsel size has minimal impact

### 3. Expose Configuration

The existing `MORSEL_SIZE` environment variable allows users to tune for their workload.
Consider exposing this as a query hint or session variable for advanced users.

## How to Profile Further

### Cache Profiling (Linux only)

```bash
# Compare cache miss rates at different sizes
for size in 2048 8192 50000; do
  echo "=== Morsel size: $size ==="
  MORSEL_SIZE=$size perf stat -e cache-misses,cache-references,L1-dcache-load-misses \
    ./target/release/deps/morsel_scaling-* morsel_size 2>&1 | grep -E "(cache|time)"
done
```

### Running the Benchmark

```bash
# Build
cargo build --release --package vibesql-executor --bench morsel_scaling --features in-memory-indexes

# Run morsel size sensitivity test
BENCHMARK_FILTER=morsel_size SCALE_FACTOR=0.1 ./target/release/deps/morsel_scaling-*

# Test specific size
MORSEL_SIZE=2048 BENCHMARK_FILTER=morsel_size ./target/release/deps/morsel_scaling-*
```

## Related

- Issue #4211: Performance: Advanced Parallelism & Concurrent Query Execution
- `crates/vibesql-executor/src/select/morsel.rs`: Morsel configuration
- `crates/vibesql-executor/benches/morsel_scaling.rs`: Benchmark code
- DuckDB architecture: https://thinhdanggroup.github.io/duckdb/

---

*Generated as part of issue #4257 investigation*
