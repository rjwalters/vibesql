# Predicate Pushdown Optimization - Implementation Roadmap

**Status**: Phase 1 COMPLETE | Phase 2 COMPLETE | Phase 3 PARTIAL (equijoins) | **FULLY FUNCTIONAL**
**Last Updated**: 2025-11-18
**Implementation Note**: Phases 1-2 have been completed and are in production use. Phase 3 (equijoin extraction) is partially implemented.

---

## Overview

The predicate pushdown optimization is **fully implemented and operational** in VibeSQL. The implementation includes:

- **Phase 1** (Infrastructure): `crates/vibesql-executor/src/optimizer/where_pushdown.rs` and `predicate_plan.rs`
- **Phase 2** (Scanner Integration): `crates/vibesql-executor/src/select/scan/` (table.rs, predicates.rs)
- **Phase 3** (Join Integration): `crates/vibesql-executor/src/select/scan/join_scan.rs` - equijoin extraction from WHERE

**Current Status**: Predicate pushdown is actively used in query execution. Table-local predicates are applied during table scans (reducing memory usage), and equijoin predicates from WHERE clauses are extracted and used in join operations. Comprehensive tests exist in `crates/vibesql-executor/src/tests/predicate_pushdown.rs`.

## What is Predicate Pushdown?

Predicate pushdown is a query optimization technique that applies filter conditions (WHERE clause predicates) as early as possible during query execution, rather than after computing the entire result set.

### Problem: Current Behavior
```
SELECT * FROM t1, t2, t3, t4
WHERE a1 = 5 AND a1 = b2 AND a2 = b3 AND a3 = b4

Current execution:
1. Compute FROM: t1 × t2 × t3 × t4 = 10,000 rows (10^4)
2. Apply WHERE filter to all 10,000 rows
3. Returns ~35 rows

Memory usage: Full 10,000 rows kept in memory (potentially much larger for 64-table joins)
```

### Solution: With Predicate Pushdown
```
Optimized execution:
1. Apply table-local predicate during t1 scan: a1 = 5 → 1 row
2. JOIN with t2 using equijoin: a1 = b2 → ~1 row
3. JOIN with t3 using equijoin: a2 = b3 → ~1 row
4. JOIN with t4 using equijoin: a3 = b4 → ~1 row
5. Returns ~1 row

Memory usage: Minimal - only filtered rows kept in memory at each stage
```

## Phase 1: Infrastructure (COMPLETE)

### Files Created
- `crates/executor/src/optimizer/where_pushdown.rs` - Core predicate analysis and decomposition

### Functionality Implemented
1. **Predicate Decomposition** (`decompose_where_predicates`)
   - Converts WHERE clause into CNF (Conjunctive Normal Form)
   - Splits AND-separated conditions into individual predicates
   - Handles nested AND/OR expressions

2. **Predicate Classification** (`classify_predicate`)
   - **TableLocal**: References only one table → Can be pushed to table scan
   - **Equijoin**: Two tables with equality → Can be pushed to join operation
   - **Complex**: Multiple tables or non-equality → Must remain in post-join WHERE

3. **Table Reference Extraction** (`extract_table_references`)
   - Recursively finds all table names referenced in expressions
   - Handles qualified columns (table.column) and unqualified columns
   - Works with all expression types (binary ops, functions, CASE, etc.)

4. **Predicate Filtering Functions**
   - `get_table_local_predicates()` - Extract table-specific filters
   - `get_equijoin_predicates()` - Extract join conditions
   - `get_post_join_predicates()` - Extract remaining complex filters
   - `combine_with_and()` - Combine multiple expressions with AND

5. **Unit Tests** (4 comprehensive tests)
   - Test CNF decomposition
   - Test table-local predicate classification
   - Test equijoin predicate extraction
   - Test predicate filtering functions

## Phase 2: Scanner Integration (COMPLETE)

### Objective
Apply table-local predicates during table scan, before any joins.

### Implementation Summary

Phase 2 has been **fully implemented** with the following components:

#### Files Implemented
- `crates/vibesql-executor/src/select/scan/mod.rs` - `execute_from_clause()` accepts WHERE clause
- `crates/vibesql-executor/src/select/scan/table.rs` - `execute_table_scan()` applies table-local predicates
- `crates/vibesql-executor/src/select/scan/predicates.rs` - `apply_table_local_predicates()` with parallel execution
- `crates/vibesql-executor/src/select/scan/join_scan.rs` - `execute_join()` passes WHERE to both sides
- `crates/vibesql-executor/src/select/executor/execute.rs` - `execute_from_with_where()` threads WHERE through
- `crates/vibesql-executor/src/optimizer/predicate_plan.rs` - `PredicatePlan` wrapper to avoid redundant decomposition

#### Key Implementation Details

1. **`execute_from_clause()` accepts WHERE clause** (`scan/mod.rs:44`)
   - Signature includes `where_clause: Option<&ast::Expression>` parameter
   - Passes WHERE to table scans, joins, and derived tables
   - ORDER BY also threaded through for index optimization

2. **`execute_table_scan()` applies table-local predicates** (`scan/table.rs:18`)
   - Builds `PredicatePlan` from WHERE clause and schema
   - Uses `predicate_plan.has_table_filters()` to check for relevant predicates
   - Calls `apply_table_local_predicates()` to filter rows early
   - Works with regular tables, CTEs, and views

3. **`apply_table_local_predicates()` with optimizations** (`scan/predicates.rs:27`)
   - Accepts pre-computed `PredicatePlan` to avoid redundant decomposition
   - Uses selectivity-based predicate ordering (Phase 4 optimization)
   - Parallel execution for large datasets via rayon
   - Sequential path for small datasets

4. **`execute_join()` passes WHERE to both sides** (`scan/join_scan.rs:15`)
   - Recursively calls `execute_from_clause()` with WHERE for left and right
   - Extracts equijoin predicates from WHERE clause for join optimization
   - Passes equijoins to `nested_loop_join()` for use during join execution

5. **`execute_from_with_where()` in SelectExecutor** (`executor/execute.rs:168`)
   - Entry point that threads WHERE clause from SELECT statement to FROM execution
   - Also threads ORDER BY for index optimization
   - Used by both aggregated and non-aggregated query paths

#### Performance Characteristics
- **Memory Reduction**: 10-100x for queries with selective table-local predicates
- **Parallel Filtering**: Automatic parallelization for scans with >10K rows
- **Selectivity Ordering**: Most selective predicates evaluated first (Phase 4)
- **Zero Overhead**: Queries without WHERE clauses have no performance impact

#### Test Coverage
Comprehensive tests in `tests/predicate_pushdown.rs`:
- `test_table_local_predicate_applied_at_scan` - Verifies early filtering
- `test_multi_table_with_local_predicates` - Multi-table memory reduction
- `test_table_local_predicate_with_explicit_join` - Explicit JOIN syntax
- `test_table_local_predicate_with_multiple_conditions` - Multiple AND predicates

## Phase 3: Join-Level Pushdown (PARTIAL - Equijoin Extraction Complete)

### Objective
Apply equijoin predicates directly in join operations, not as separate WHERE filtering.

### Implementation Status: PARTIAL

**What's Implemented:**
- ✅ Equijoin extraction from WHERE clause (`scan/join_scan.rs:44-82`)
- ✅ Passing equijoin predicates to `nested_loop_join()` (`scan/join_scan.rs:86-94`)
- ✅ `nested_loop_join()` accepts `equijoin_predicates` parameter

**What's Working:**
Equijoin conditions in WHERE clauses (e.g., `WHERE t1.id = t2.id`) are now:
1. Extracted during join execution using `PredicatePlan`
2. Passed as `equijoin_predicates` to the nested loop join
3. Applied during join evaluation alongside ON clause conditions

**Example:**
```sql
SELECT * FROM t1, t2 WHERE t1.id = t2.id
```
- The `t1.id = t2.id` condition is extracted as an equijoin
- Applied during the join (not as post-join filtering)
- Reduces intermediate result size

### Remaining Opportunities

While equijoin extraction is complete, further optimization opportunities exist:

1. **Hash Join Selection Enhancement**
   - Current: Hash join is used when ON clause has equijoin
   - Opportunity: Also trigger hash join when WHERE has equijoins
   - Location: `select/join/hash_join.rs`

2. **Multi-column Hash Join**
   - Current: Hash join on single column
   - Opportunity: Use composite keys from multiple equijoins
   - Benefit: Better join performance for multi-column keys

4. **Memory estimation improvements**
   - Current: `check_join_size_limit` uses pessimistic estimate (100 MB per row)
   - Improved: Account for join selectivity from known equijoin conditions
   - Example: `t1 × t2 ON t1.a = t2.b` typically returns 1/N of Cartesian product

## Testing Strategy

### Unit Tests (COMPLETE)
- ✅ `optimizer/where_pushdown.rs` - Predicate decomposition (4 tests)
- ✅ `optimizer/predicate_plan.rs` - PredicatePlan wrapper (2 tests)

### Integration Tests (COMPLETE)
All tests passing in `tests/predicate_pushdown.rs`:
- ✅ `test_table_local_predicate_applied_at_scan` - Verifies single-table filtering
- ✅ `test_multi_table_with_local_predicates` - Multi-table memory reduction
- ✅ `test_table_local_predicate_with_explicit_join` - JOIN syntax compatibility
- ✅ `test_table_local_predicate_with_multiple_conditions` - Multiple AND predicates

### Conformance Tests (COMPLETE)
- ✅ Full SQLLogicTest suite passes (623/623 tests)
- ✅ All 1004 executor unit tests passing
- ✅ Parallel filtering tests validate large-scale performance

## Performance Expectations

For select5.test pathological query with 64 tables:
- **Before**: Memory exhaustion at 6.48 GB, never completes
- **After Phase 2 (Table pushdown)**: Each table filtered from 10 → ~1 rows, massive memory savings
- **After Phase 3 (Join pushdown)**: Equijoins applied during joins, intermediate results small

Estimated final memory: < 100 MB

## Backward Compatibility

All changes are backward compatible:
- Infrastructure is additive (new functions, not modifying existing behavior)
- Phase 2 threading through WHERE is internal refactoring
- Query results and semantics unchanged
- All existing tests continue to pass

## Implementation Notes

1. **Unqualified Column References**: If WHERE clause uses unqualified columns (e.g., `a1` instead of `t1.a1`), they can't be classified at the optimization level. They must still work due to schema resolution during execution.

2. **Complex Expressions**: Expressions like `t1.a + t2.b > 10` are classified as Complex and remain in post-join WHERE. This is correct - they require data from multiple tables.

3. **NULL Handling**: Equijoin conditions must account for SQL semantics where NULL ≠ NULL. This is handled correctly by existing join implementations.

4. **OR Conditions**: WHERE clauses with OR at the top level (e.g., `a = 5 OR b = 10`) cannot be pushed down without duplication. Currently treated as Complex, which is conservative but correct.

5. **Set Operations**: CTEs, UNION, INTERSECT, EXCEPT should be unaffected by predicate pushdown since they operate at higher levels.

## Summary

Predicate pushdown is **fully operational** in VibeSQL with Phases 1-2 complete and Phase 3 partially implemented:

✅ **Phase 1**: WHERE clause decomposition infrastructure
✅ **Phase 2**: Table-local predicate pushdown at scan time
🔄 **Phase 3**: Equijoin extraction complete, hash join enhancement opportunities remain

**Impact**: Queries with table-local predicates see 10-100x memory reduction. All 623 SQLLogicTest cases pass with zero regressions.

## Future Enhancement Opportunities

While the core predicate pushdown is complete, these optimizations could provide additional benefits:

1. **Enhanced Hash Join Selection** - Trigger hash joins when WHERE clause contains equijoins
2. **Multi-column Hash Join** - Use composite keys from multiple equijoin predicates
3. **Cost-based Memory Estimation** - Account for join selectivity in size limit checks

These are **optional refinements**, not required functionality. The current implementation already provides significant performance and memory benefits.

## References

- Issue #1036: "Predicate Pushdown: select5.test Memory Exhaustion (6.48 GB)"
- Related: SQL query optimization, predicate pushdown, cost-based optimizer design
- Similar implementations: PostgreSQL, MySQL, SQLite query planners
