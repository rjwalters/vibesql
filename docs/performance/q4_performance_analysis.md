# TPC-H Q4 Performance Analysis

## Summary

After deep profiling of TPC-H Q4, I identified the root cause of the **48x performance gap** vs SQLite.

**Root cause**: Column-to-column predicate evaluation (`l_commitdate < l_receiptdate`) forces generic row-by-row evaluation instead of columnar filtering, resulting in ~35ms of unnecessary overhead.

## Profiling Results

### Query Timing Breakdown (SF=0.01)

| Component | Time | % of Total |
|-----------|------|------------|
| Semi-join (build + probe) | ~7ms | 14% |
| LINEITEM scan + filter | ~35ms | 73% |
| ORDERS scan + filter | ~3ms | 6% |
| Aggregation | ~3ms | 6% |
| **Total** | **~48ms** | 100% |

### Key Findings

1. **EXISTS transformation works correctly**
   - EXISTS subquery is properly converted to SEMI JOIN
   - `SUBQUERY_TRANSFORM_VERBOSE` confirms: `Converted subquery to join`

2. **Semi-join filter pushdown works correctly**
   - Filter `l_commitdate < l_receiptdate` is correctly identified as "right-only"
   - Pushed to build phase, not probe phase
   - `right_only_filter=Some(...)`, `probe_filter=None`

3. **Semi-join is efficient**
   - Build time: 6.8ms for 29,899 rows
   - Probe time: 125µs for 480 rows
   - Total: ~7ms (only 14% of execution time)

4. **LINEITEM table scan is the bottleneck** (73% of time)
   - 60,000 rows scanned
   - Filter: `l_commitdate < l_receiptdate`
   - `extract_column_predicates returned None` → falls back to generic path
   - Reason: Column-to-column comparisons not supported by columnar optimizer

5. **ORDERS table scan is efficient**
   - 15,000 rows scanned
   - Filter: `o_orderdate >= '1993-07-01' AND o_orderdate < '1993-10-01'`
   - `extracted 2 predicates` → columnar path used
   - Result: 480 rows (3.2% selectivity)

## Root Cause: Column-to-Column Predicate

The columnar predicate extractor (`crates/vibesql-executor/src/select/columnar/filter/predicates.rs`) only handles:

```rust
// Supported: column op literal
if let Expression::ColumnRef { table, column } = left.as_ref() {
    if let Some(value) = try_fold_constant(right) {  // <- requires literal/constant
        ...
    }
}
```

It cannot handle column-to-column comparisons like `l_commitdate < l_receiptdate`.

### Why SQLite is faster

SQLite uses its B-tree engine to evaluate predicates more efficiently:
- Single-pass evaluation during scan
- No separate columnar vs row-based paths
- Optimized bytecode for comparisons

## Optimization Opportunities

### Option 1: Columnar Column-to-Column Predicates (Recommended)

Add support for `ColumnPredicate::ColumnCompare`:

```rust
enum ColumnPredicate {
    // Existing...
    
    /// column1 < column2 (column-to-column comparison)
    ColumnCompare { 
        left_column_idx: usize, 
        op: CompareOp,
        right_column_idx: usize 
    },
}
```

**Estimated impact**: 35ms → ~5ms (LINEITEM scan)
**Total improvement**: 48ms → ~18ms (2.6x faster)

### Option 2: Lazy Semi-Join Evaluation

Instead of scanning LINEITEM first and passing to semi-join:
1. Build hash table from LINEITEM.l_orderkey only (no predicate evaluation)
2. During probe, evaluate `l_commitdate < l_receiptdate` lazily per matching row

**Impact**: Would reduce wasted work on non-matching rows

### Option 3: Index on l_orderkey with Filter

Create index strategy that can push `l_commitdate < l_receiptdate` filter.

**Impact**: Limited - column-to-column predicates aren't good index candidates

## Recommended Actions

1. **Create follow-up issue** for columnar column-to-column predicate support
2. **Priority**: High - affects multiple TPC-H queries (Q4, Q21, etc.)
3. **Target**: Reduce Q4 from 48ms to <20ms

## Debug Commands Used

```bash
# EXISTS transformation
SUBQUERY_TRANSFORM_VERBOSE=1 SCALE_FACTOR=0.01 ./target/release/deps/tpch_profiling-* Q4

# Semi-join timing
SEMI_JOIN_DEBUG=1 SCALE_FACTOR=0.01 ./target/release/deps/tpch_profiling-* Q4

# Scan path selection
TABLE_SCAN_DEBUG=1 COLUMNAR_DEBUG=1 SCALE_FACTOR=0.01 ./target/release/deps/tpch_profiling-* Q4
```

## Files Analyzed

- `crates/vibesql-executor/src/optimizer/subquery_to_join/exists.rs` - EXISTS transformation
- `crates/vibesql-executor/src/select/join/hash_semi_join.rs` - Semi-join implementation
- `crates/vibesql-executor/src/select/columnar/filter/predicates.rs` - Columnar predicate extraction
- `crates/vibesql-executor/src/select/scan/table.rs` - Table scan logic
- `crates/vibesql-executor/src/select/scan/predicates.rs` - Predicate evaluation
