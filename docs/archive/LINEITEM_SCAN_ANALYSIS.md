# Lineitem Table Scan Performance Analysis

Issue #2962 - Performance profiling of lineitem table scans

## Executive Summary

Benchmarks reveal that VibeSQL's lineitem table scan performance lags behind DuckDB by **27-294x** depending on the operation. The primary bottleneck is **eager row materialization** - cloning all rows from storage into `Vec<Row>` before any processing occurs.

## Benchmark Results (SF 0.01, ~60K rows)

| Benchmark | VibeSQL | DuckDB | Gap | Notes |
|-----------|---------|--------|-----|-------|
| **Full Scan** (SELECT *) | 295ms | 10.9ms | **27x** | Baseline overhead |
| **COUNT(*)** | 71ms | 242µs | **294x** | No data needed, but we clone all rows |
| **Date Filter** (SELECT * WHERE) | 61ms | 7.8ms | **8x** | Best case - columnar filter helps |
| **Date COUNT** (COUNT(*) WHERE) | 68ms | 290µs | **234x** | Same problem as COUNT(*) |
| **Single Column** (SELECT l_orderkey) | 39ms | 413µs | **95x** | Should only read 1 column |
| **Two Columns** | 41ms | 573µs | **72x** | Still reads all columns |
| **LIMIT 100** | 25ms | 579µs | **43x** | Scans all rows, then limits |
| **Date + LIMIT 100** | 22ms | 760µs | **29x** | No early termination |

## Root Cause Analysis

### 1. Eager Full-Row Materialization

**Location**: `crates/vibesql-executor/src/select/scan/table.rs:167-218`

Every table scan follows this pattern:
```rust
// Get row slice from table (zero-copy reference)
let row_slice = table.scan();

// ... processing ...

// PROBLEM: Always clones all rows
let rows = row_slice.to_vec();
```

Even when:
- Only SELECT COUNT(*) is needed (no data)
- Only 1 column is projected (l_orderkey)
- LIMIT 100 would stop at 100 rows

### 2. Row-Based Storage Model

**Row Structure** (`crates/vibesql-storage/src/row.rs`):
```rust
pub struct Row {
    pub values: Vec<SqlValue>,  // 16 columns for lineitem
}
```

**SqlValue enum** (`crates/vibesql-types/src/sql_value/mod.rs`):
- Contains String variants (Character, Varchar) = 24 bytes
- Enum discriminant + padding = ~32 bytes per value
- lineitem has 16 columns = ~512 bytes per row (excluding string content)

**Memory overhead per scan**:
- 60K rows × 512 bytes = ~30MB copied
- Plus heap allocations for String values (l_comment: up to 44 chars)
- Each clone triggers 60K allocations for Vec<SqlValue> headers

### 3. No Column Pruning

When executing `SELECT l_orderkey FROM lineitem`:
1. All 16 columns are read from storage
2. All 16 columns are cloned into Vec<Row>
3. Projection happens AFTER materialization

### 4. No Early Termination for LIMIT

`LIMIT 100` on 60K rows:
1. Scans all 60K rows from storage
2. Clones all 60K rows
3. THEN applies LIMIT (discards 59,900 rows)

See comment at line 213:
```rust
// TODO: Future optimization - use zero-copy iterator over row slice
```

### 5. COUNT(*) Materializes All Data

For COUNT(*), ideal behavior:
- Just return table.row_count() = O(1)
- No data access needed

Current behavior:
- Clone all rows
- Build full result set
- Count result.len()

## Performance Gap Breakdown

| Factor | Impact | Evidence |
|--------|--------|----------|
| Row cloning | ~20x | Full scan is 27x vs ideal 1-2x |
| No column pruning | ~3-4x | Single column (95x) vs full scan (27x) |
| No LIMIT pushdown | ~10x | LIMIT 100 takes 25ms instead of <1ms |
| COUNT(*) not optimized | ~290x | Could be O(1) |

## Optimization Targets

### High Impact (100x+ improvement potential)

1. **Zero-Copy Iterator for Table Scans**
   - Return `impl Iterator<Item = &Row>` instead of `Vec<Row>`
   - Stop cloning 60K rows per scan
   - Location: `scan/table.rs:167-221`

2. **Optimize COUNT(*) without FROM data**
   - Return `table.row_count()` directly
   - Location: Detect in `executor/execute.rs`

3. **LIMIT Pushdown to Scan**
   - Stop iteration after LIMIT rows collected
   - Current: `execute_iter` materializes all, then iterates
   - Location: `executor/execute.rs:129-137`

### Medium Impact (10-50x improvement potential)

4. **Column Pruning at Scan Time**
   - Project only needed columns during scan
   - Avoid loading unused columns from storage
   - Requires: Late projection pattern

5. **Predicate Pushdown to Storage Layer**
   - Filter rows in storage before Row construction
   - Currently: Construct all Rows, then filter

### Lower Impact (2-10x improvement)

6. **Small String Optimization in SqlValue**
   - Use SmallVec or inline small strings
   - Reduce allocations for l_returnflag (1 char), l_linestatus (1 char)

7. **Arena Allocation for Query Rows**
   - QueryBufferPool exists but not used for scan
   - Could pool Row allocations

## DuckDB's Approach (for reference)

DuckDB achieves sub-millisecond scans through:
1. **Columnar Storage**: Data stored by column, not row
2. **Vectorized Execution**: Process batches of 1024 values
3. **Late Materialization**: Only materialize output columns
4. **Push-Based Execution**: Operators push tuples, LIMIT stops early
5. **Zone Maps**: Skip blocks that don't match predicates

## Recommended Next Steps

### Phase 1: Quick Wins (1-2 days)
- [ ] Implement COUNT(*) optimization (bypass scan)
- [ ] Add LIMIT pushdown to stop early

### Phase 2: Iterator Refactoring (1 week)
- [ ] Replace `Vec<Row>` returns with `impl Iterator<Item = &Row>`
- [ ] Stop cloning during scan

### Phase 3: Column Pruning (1-2 weeks)
- [ ] Add column mask to scan API
- [ ] Only materialize needed columns

### Phase 4: Columnar Storage (longer term)
- [ ] Store tables in columnar format
- [ ] Enable SIMD filtering directly on storage

## Appendix: Benchmark Reproduction

```bash
# Run lineitem scan profiling benchmark
cd crates/vibesql-executor
cargo bench --bench lineitem_scan_profiling --features benchmark-comparison

# Results saved to: target/criterion/
```

## References

- Issue #2962: Profile lineitem table scan performance
- Issue #2804: TPC-H Phase 2 Optimization (parent)
- `crates/vibesql-executor/benches/lineitem_scan_profiling.rs`
