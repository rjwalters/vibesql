# DELETE Operation Bottleneck Analysis

**Issue**: #3696 - perf(delete): Profile and optimize remaining DELETE bottlenecks

**Date**: 2025-12-05

**Profiling Method**: Flamegraph using cargo-flamegraph with Time Profiler template on macOS

## Executive Summary

Profiling 5,000 DELETE operations on a 10,000-row sysbench table reveals that DELETE performance is primarily bottlenecked by **row vector operations** inside `Table::delete_by_indices`. The Vec::remove() operation accounts for approximately **35% of total DELETE time**.

## Baseline Performance

- **Per-operation time**: ~162μs (includes DELETE + INSERT cycle)
- **Target**: ~80μs (SQLite single-row DELETE)
- **Current gap**: ~2x slower than target

## Top 5 Bottlenecks

### 1. `Table::delete_by_indices` - 34.95% of DELETE time

**Location**: `crates/vibesql-storage/src/table/mod.rs:740-777`

**Root Cause**:
```rust
// Delete rows in reverse order to maintain correct indices during removal
for &idx in sorted_indices.iter().rev() {
    self.rows.remove(idx);  // O(n-idx) per removal
}
```

The `Vec::remove()` operation is O(n) as it must shift all subsequent elements. For a 10k row table, removing a row near the beginning requires shifting up to 10,000 elements.

**Optimization Proposals**:
1. **swap_remove + rebuild**: Use `swap_remove()` which is O(1), then rebuild indexes at the end
2. **Deletion bitmap**: Mark rows as deleted instead of physical removal, compact periodically
3. **Segment-based storage**: Store rows in segments (e.g., 1000 rows each) to limit shift operations

---

### 2. `adjust_indexes_after_delete` - 2.31% of DELETE time (+ hidden cost)

**Location**: `crates/vibesql-storage/src/table/indexes.rs:333-357`

**Current Implementation**:
```rust
// Adjust primary key index - O(n log d) where n=entries, d=deletions
for row_idx in pk_index.values_mut() {
    let decrement = deleted_indices.partition_point(|&d| d < *row_idx);
    *row_idx -= decrement;
}
```

This must iterate over ALL index entries to adjust row indices after deletion.

**Optimization Proposals**:
1. **Lazy adjustment**: Store pending deletions with index, apply during lookups
2. **Stable row IDs**: Use stable row identifiers instead of vector indices
3. **Skip for single deletes**: When deleting 1 row, only entries > deleted_idx need adjustment

---

### 3. `batch_update_indexes_for_delete` - 3.01% of DELETE time

**Location**: `crates/vibesql-storage/src/database/indexes/index_maintenance.rs:524-608`

User-defined index updates for deleted rows. Already batched but still significant.

**Optimization Proposals**:
1. **Async index updates**: Queue index updates for background processing
2. **Skip unused indexes**: Track index usage, defer updates for rarely-accessed indexes

---

### 4. `ExpressionEvaluator::with_database` - 2.55% of DELETE time

**Location**: `crates/vibesql-executor/src/evaluator/single.rs`

Creating the expression evaluator for each DELETE statement.

**Optimization Proposals**:
1. **Pool evaluators**: Reuse evaluator instances across operations
2. **Skip for PK-only deletes**: When WHERE clause is PK equality, skip evaluator creation

---

### 5. `IndexManager::update_for_delete` (PK index) - inside delete_by_indices

**Location**: `crates/vibesql-storage/src/table/indexes.rs`

Removing entries from the primary key HashMap.

**Optimization Proposals**:
1. **Tombstone markers**: Instead of removing, mark as deleted
2. **Batch removals**: Collect all keys to remove, then rebuild HashMap

## Memory Operations Breakdown

| Operation | Samples | Notes |
|-----------|---------|-------|
| `Vec<T>::clone` | 3-6 | Row cloning for triggers/WAL |
| `SqlValue::clone` | 6 | Value cloning for index operations |
| `String::clone` | 2 | String value cloning |
| `memmove/memcpy` | 2 | Vec::remove element shifting |

## Recommended Optimization Priority

1. **High Impact**: Replace `Vec::remove()` with deletion bitmap
   - Expected improvement: 20-30% reduction in DELETE time
   - Complexity: Medium
   - Risk: Low (isolated change)

2. **Medium Impact**: Lazy index adjustment
   - Expected improvement: 5-10% reduction
   - Complexity: Medium
   - Risk: Medium (affects index correctness)

3. **Medium Impact**: Skip evaluator for PK-only WHERE clauses
   - Expected improvement: 2-3% reduction
   - Complexity: Low
   - Risk: Low

4. **Low Impact**: Evaluator pooling
   - Expected improvement: 1-2% reduction
   - Complexity: Low
   - Risk: Low

## Profiling Commands Used

```bash
# Create targeted profiling benchmark
cargo bench --bench delete_profiling

# Generate flamegraph with debug symbols
CARGO_PROFILE_BENCH_DEBUG=true cargo flamegraph --bench delete_profiling \
    --deterministic -o delete_flamegraph.svg

# View flamegraph
open delete_flamegraph.svg
```

## Flamegraph Files

- `delete_flamegraph.svg` - Focused DELETE profiling (5000 operations)
- `flamegraph_debug.svg` - Full sysbench_delete benchmark with debug symbols

## Next Steps

1. Create separate issues for each high-priority optimization
2. Implement deletion bitmap (highest impact)
3. Add microbenchmarks for each component
4. Re-profile after optimizations to validate improvements

## Related Issues

- #3637 - Batch index updates (COMPLETED - PR #3693)
- Original issue mentions trigger overhead - not significant in profiling (< 1% when no triggers)
