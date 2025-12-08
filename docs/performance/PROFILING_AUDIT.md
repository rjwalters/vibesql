# Profiling Environment Variables Audit

This document audits the custom profiling environment variables in VibeSQL to determine which provide unique value vs. which are redundant with samply CPU profiling.

**Issue**: #4051
**Parent Epic**: #4050 (Rationalize profiling infrastructure)

## Audit Methodology

For each environment variable, we evaluated:
1. **What it logs** - The specific information output
2. **Semantic vs Debug** - Does it explain WHY decisions were made, or just WHAT happened?
3. **Redundancy with samply** - Can a CPU profiler (flamegraph) provide equivalent insight?
4. **Active usage** - Is it used in benchmarks or debugging workflows?

## Categorization Summary

| Category | Count | Recommendation |
|----------|-------|----------------|
| **KEEP - Semantic** | 4 | Essential for understanding optimizer decisions |
| **KEEP - Phase Timing** | 6 | Provides structured timing samply can't replicate |
| **REMOVE - Debug Spam** | 4 | Function entry/exit, redundant with CPU profiling |
| **CONSOLIDATE** | 3 | Merge into unified patterns |

---

## KEEP: Semantic Logging (4 vars)

These variables explain **WHY** the optimizer made specific decisions - information that CPU profiling cannot provide.

### `JOIN_REORDER_VERBOSE`

**Location**: `crates/vibesql-executor/src/select/scan/reorder/optimizer.rs`, `predicates.rs`
**Usage Count**: 15+ locations
**Added**: For TPC-H Q19 optimization (#3243)

**What it logs**:
- Schema-based column mapping results
- WHERE clause predicates and extracted equijoins
- Table-local predicates for cardinality estimation
- Original vs optimal join order comparison
- Optimizer time budget and search decisions

**Example output**:
```
[JOIN_REORDER] Schema-based column mapping: 45 columns resolved from 6 tables
[JOIN_REORDER] Extracted 3 WHERE equijoins
[JOIN_REORDER] Original order: ["lineitem", "orders", "customer"]
[JOIN_REORDER] Optimal order:  ["customer", "orders", "lineitem"]
```

**Verdict**: **KEEP** - Essential for debugging join optimization. Explains the reasoning behind join order choices with cost estimates.

---

### `SUBQUERY_TRANSFORM_VERBOSE`

**Location**: `crates/vibesql-executor/src/optimizer/subquery_to_join/`
**Usage Count**: 8+ locations
**Added**: For subquery-to-join optimization

**What it logs**:
- Self-join detection and alias creation
- Column reference rewriting decisions
- Derived table creation for aggregate subqueries
- Final transformed FROM clause structure

**Example output**:
```
[SUBQUERY_TRANSFORM] Self-join detected: table=item, new_alias=__subquery_item
[SUBQUERY_TRANSFORM] Converting aggregate IN subquery to derived table semi-join
[SUBQUERY_TRANSFORM] Derived table alias: __in_agg_0
```

**Verdict**: **KEEP** - Critical for understanding subquery optimization. Explains how IN/EXISTS are transformed to SEMI/ANTI joins.

---

### `TABLE_ELIM_VERBOSE`

**Location**: `crates/vibesql-executor/src/optimizer/table_elimination.rs`
**Usage Count**: 1 location
**Added**: For table elimination optimization

**What it logs**:
- Which tables were eliminated and why
- Foreign key relationships used for elimination
- Before/after table counts

**Verdict**: **KEEP** - Important for understanding query simplification decisions.

---

### `DML_COST_DEBUG`

**Location**: `crates/vibesql-executor/src/delete/executor.rs`, `update/mod.rs`
**Usage Count**: 4 locations
**Added**: For DML cost estimation

**What it logs**:
- Row count thresholds for chunked operations
- Index impact analysis (hash vs btree count)
- Early compaction recommendations
- Cost model reasoning

**Example output**:
```
DML_COST_DEBUG: DELETE on orders - 1500 rows qualifies for chunked delete
DML_COST_DEBUG: UPDATE 100 rows in lineitem - estimated_cost: 4.50 (hash_indexes: 2, btree_indexes: 1)
```

**Verdict**: **KEEP** - Explains cost model decisions for DML operations.

---

## KEEP: Structured Phase Timing (6 vars)

These variables provide granular timing breakdowns that CPU profiling cannot replicate because they measure logical phases, not just function call stacks.

### `DELETE_PROFILE` / `DELETE_PROFILE_VERBOSE` / `DELETE_PROFILE_SUMMARY`

**Location**: `crates/vibesql-storage/src/database/index_ops.rs`
**Added**: Dec 2025 (#3873)

**What it logs**:
- Per-operation timing: pk_lookup, value_clone, wal, index_update, row_remove, cache
- Percentage breakdown of each phase
- Aggregate statistics on thread exit

**Example output**:
```
DELETE_PROFILE: total=45.2us | pk_lookup=12.1us (27%) | value_clone=3.2us (7%) |
  wal=8.5us (19%) | index_update=15.3us (34%) | row_remove=4.8us (11%) | cache=1.3us (3%)
```

**Verdict**: **KEEP** - Well-designed instrumentation. Shows which DELETE sub-operation is the bottleneck. Samply can show function time but not these logical phases.

---

### `JOIN_PROFILE`

**Location**: `crates/vibesql-executor/src/select/scan/reorder/optimizer.rs`
**Added**: Nov 2025 (#2971)

**What it logs**:
- Per-join timing in multi-way joins
- Intermediate result sizes
- Hash vs nested-loop decision

**Verdict**: **KEEP** - Shows join execution phases that span multiple functions.

---

### `RANGE_SCAN_PROFILE`

**Location**: `crates/vibesql-executor/src/select/scan/index_scan/execution.rs`
**Added**: Dec 2025 (#3830)

**What it logs**:
- Range scan setup time
- Key iteration time
- Row fetch time

**Verdict**: **KEEP** - Shows index scan sub-phases.

---

### `RANGE_QUERY_BREAKDOWN`

**Location**: Storage crate
**Added**: For sysbench analysis

**What it logs**:
- Detailed range query timing

**Verdict**: **KEEP** - Granular timing for range operations.

---

## REMOVE: Debug Spam (4 vars)

These variables log function entry/exit or trace information that can be obtained more effectively with samply CPU profiling.

### `SIMD_DEBUG`

**Location**: `crates/vibesql-executor/src/select/executor/nonagg/simd.rs`
**Usage Count**: 1 location (6 log points)
**Added**: Nov 2025 (#2551) for SIMD date optimization investigation

**What it logs**:
```
[SIMD] Skipping: 50 rows < 100 threshold
[SIMD] Attempting SIMD filter on 1000 rows
[SIMD] Successfully converted to RecordBatch
```

**Analysis**: This is pure trace logging - it tells you that SIMD was attempted but not why it succeeded/failed in a meaningful way. Samply shows if time is spent in SIMD code paths.

**Verdict**: **REMOVE** - Function entry/exit logging. Use samply to see if SIMD paths are hot.

---

### `INL_DEBUG`

**Location**: `crates/vibesql-executor/src/select/scan/join_scan.rs`
**Usage Count**: 2 locations (10+ log points)

**What it logs**:
```
[INL] SEMI join detected, left_row_count=5, threshold=100
[INL] try_index_nested_loop_semi_join called
[INL] Right table: stock
[INL] No condition, returning None
```

**Analysis**: Traces through index nested loop decision logic. Most messages are "entering function X" or "condition Y was false". The actual decision is "did INL happen or not" which samply shows clearly.

**Verdict**: **REMOVE** - Function trace logging. Samply shows if INL code paths execute.

---

### `JOIN_SCAN_DEBUG`

**Location**: `crates/vibesql-executor/src/select/scan/join_scan.rs`
**Usage Count**: 2 locations

**What it logs**:
```
[JOIN_SCAN] Extracted 2 right-only predicates for tables ["orders"]
[JOIN_SCAN] filter_out_nullable_side_predicates: kept 3/4 predicates
```

**Analysis**: Traces predicate extraction. Useful for debugging specific bugs but not for general performance analysis. The decision (which predicates were extracted) can be inferred from query plans.

**Verdict**: **REMOVE** - Debugging trace, not needed for performance work.

---

### `SEMI_JOIN_DEBUG`

**Location**: `crates/vibesql-executor/src/select/scan/reorder/optimizer.rs`
**Usage Count**: 1 location

**What it logs**:
```
[SEMI_JOIN_DEBUG] Condition: BinaryOp { ... }
[SEMI_JOIN_DEBUG] Inner tables: ["stock"]
[SEMI_JOIN_DEBUG] Target table: Some("stock")
```

**Analysis**: Traces semi-join target table resolution. Very low-level debugging.

**Verdict**: **REMOVE** - Low-level debugging, use for specific bug investigation only.

---

## CONSOLIDATE: Mixed Value (3 vars)

These provide some useful information but could be improved or consolidated.

### `INDEX_SELECT_DEBUG`

**Location**: `crates/vibesql-executor/src/select/scan/index_scan/selection.rs`
**Usage Count**: 10 locations

**What it logs**:
```
[INDEX_SELECT] table=stock, index=idx_stock_item, first_col=s_i_id, can_use_for_where=true
[INDEX_SELECT] idx_stock_item selectivity=0.0033, access_method=IndexScan, is_index_scan=true
[INDEX_SELECT] selected best_index=idx_stock_item for table=stock
```

**Analysis**: This is borderline - it explains the index selection decision (semantic) but also has many trace-level messages. The selectivity and access_method logs are valuable; the "skipping X" messages are trace spam.

**Verdict**: **CONSOLIDATE** - Split into:
- Keep decision logging (rename to `INDEX_SELECT_VERBOSE`)
- Remove trace messages

---

### `TABLE_SCAN_DEBUG`

**Location**: `crates/vibesql-executor/src/select/scan/table.rs`
**Usage Count**: 2 locations

**What it logs**:
```
[TABLE_SCAN] Using index scan: table=orders, index=idx_orders_cust
[TABLE_SCAN] Falling back to table scan: table=nation, available_indexes=["pk_nation"]
```

**Analysis**: These two messages are actually useful - they show WHEN index vs table scan was chosen. But the name suggests debug spam.

**Verdict**: **CONSOLIDATE** - Rename to `SCAN_PATH_VERBOSE` to clarify it shows scan path selection.

---

### `COLUMNAR_DEBUG`

**Location**: `crates/vibesql-executor/src/select/scan/table.rs`
**Usage Count**: 3 locations

**What it logs**:
```
[COLUMNAR_DEBUG] orders (alias=o) table: has_filters=true
[COLUMNAR_DEBUG] orders table: extracted 2 predicates for 6000 rows
[COLUMNAR_DEBUG] orders table: extract_column_predicates returned None, using generic path
```

**Analysis**: Mildly useful - shows columnar optimization decisions. Could be merged with TABLE_SCAN_DEBUG.

**Verdict**: **CONSOLIDATE** - Merge with `SCAN_PATH_VERBOSE`.

---

## Recommendations

### Immediate Actions (This PR)

1. **Remove 4 debug variables**:
   - `SIMD_DEBUG` - Use samply for SIMD performance analysis
   - `INL_DEBUG` - Use samply to see if INL paths are hot
   - `JOIN_SCAN_DEBUG` - Low-level debugging only
   - `SEMI_JOIN_DEBUG` - Low-level debugging only

2. **Keep 10 variables unchanged**:
   - `JOIN_REORDER_VERBOSE` - Essential semantic logging
   - `SUBQUERY_TRANSFORM_VERBOSE` - Essential semantic logging
   - `TABLE_ELIM_VERBOSE` - Essential semantic logging
   - `DML_COST_DEBUG` - Essential semantic logging
   - `DELETE_PROFILE` family (3) - Well-designed phase timing
   - `JOIN_PROFILE` - Phase timing
   - `RANGE_SCAN_PROFILE` - Phase timing
   - `RANGE_QUERY_BREAKDOWN` - Phase timing

### Follow-up Issues

1. **Consolidate INDEX_SELECT_DEBUG** (#TBD):
   - Split valuable selectivity logging from trace spam
   - Rename to `INDEX_SELECT_VERBOSE`

2. **Consolidate scan path logging** (#TBD):
   - Merge `TABLE_SCAN_DEBUG` and `COLUMNAR_DEBUG`
   - Create unified `SCAN_PATH_VERBOSE`

3. **Document naming convention** (#TBD):
   - `*_VERBOSE` = Semantic decision logging (keep)
   - `*_PROFILE` = Phase timing instrumentation (keep)
   - `*_DEBUG` = Reserved for temporary debugging (remove after use)

---

## Appendix: Control Variables (Not Audited)

These are configuration/control variables, not profiling instrumentation:

| Variable | Purpose |
|----------|---------|
| `SCALE_FACTOR` | TPC-H/TPC-DS scale |
| `QUERY_FILTER` | Run specific queries |
| `SKIP_SLOW` | Skip slow queries |
| `VALIDATE` | Enable result validation |
| `TABLE_ELIM_DISABLED` | Disable optimization |
| `JOIN_REORDER_DISABLED` | Disable optimization |
| `VIBESQL_DISABLE_COLUMNAR*` | Feature toggles |
| `JOIN_REORDER_TIME_BUDGET_MS` | Tuning parameter |
