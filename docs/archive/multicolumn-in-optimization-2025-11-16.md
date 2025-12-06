# Issue #1922: Multi-Column IN Clause Optimization - Architectural Analysis

## Executive Summary

The 'other' category test failures (7/10 failing, 30% pass rate) are caused by a regression in multi-column IN clause optimization introduced by PR #1905. The root cause is an architectural conflict between predicate pushdown optimization and multi-column index prefix matching.

## Timeline of Events

### PR #1845 (Nov 16, 00:49) - Initial Fix
- **What it did**: Fixed multi-column IN clause queries by selectively disabling predicate pushdown
- **How**:
  - Added `requires_predicate_pushdown_disable()` to detect multi-column IN predicates
  - Implemented `try_index_for_in_clause()` at nonagg-level to handle multi-column IN optimization
  - Disabled predicate pushdown for these specific cases to preserve correct row indices
- **Status**: Tests passing

### PR #1905 (Nov 16, 12:51 - 12 hours later) - Refactoring
- **What it did**: Moved index optimization from nonagg-level to scan-level
- **Changes**:
  - Removed conditional predicate pushdown logic from execute.rs
  - Deleted `try_index_for_in_clause()` as "duplicate"
  - Re-enabled predicate pushdown for ALL queries
  - Assumed scan-level implementation could handle multi-column IN
- **Status**: Broke multi-column IN queries (regression)

## Root Cause Analysis

### The Storage Layer Bug

File: `crates/vibesql-storage/src/database/indexes/index_operations.rs:388-407`

```rust
pub fn multi_lookup(&self, values: &[SqlValue]) -> Vec<usize> {
    match self {
        IndexData::InMemory { data } => {
            let mut matching_row_indices = Vec::new();

            for value in values {
                let normalized_value = normalize_for_comparison(value);
                let search_key = vec![normalized_value];  // <-- BUG: Single-element key!
                if let Some(row_indices) = data.get(&search_key) {
                    matching_row_indices.extend(row_indices);
                }
            }

            matching_row_indices
        }
        ...
    }
}
```

**The Problem**: Line 396 creates `search_key = vec![normalized_value]`, which is a **single-element vector**.

**Why it fails**:
- Multi-column index on `(a, b)` contains keys like: `[10, 20]`, `[10, 30]`, `[20, 40]`
- Query: `WHERE a IN (10)`
- Code creates: `search_key = [10]` (single element)
- BTreeMap lookup: `data.get(&[10])` searches for EXACT match
- But index has `[10, 20]` and `[10, 30]`, NOT `[10]`
- Result: **No rows found** (should find rows with a=10)

### The Architectural Conflict

The executor-level code has the same issue:

File: `crates/vibesql-executor/src/select/scan/index_scan/execution.rs:45-53`

```rust
// Get the first indexed column (for predicate extraction)
let indexed_column = index_metadata
    .columns
    .first()
    .map(|col| col.column_name.as_str())
    .unwrap_or("");

// Try to extract index predicate (range or IN) for the indexed column
let index_predicate = where_clause.and_then(|expr| extract_index_predicate(expr, indexed_column));
```

When `extract_index_predicate()` is called with a multi-column index:
1. It only checks the FIRST column
2. For `WHERE a IN (10)`, it extracts `IndexPredicate::In(vec![10])`
3. This gets passed to `multi_lookup(&[10])`
4. Which creates single-element keys that don't match multi-column index keys

### Why PR #1845's Approach Worked

PR #1845 disabled predicate pushdown for multi-column IN, which meant:
1. The WHERE clause wasn't evaluated at scan time
2. ALL rows from the table were returned
3. The nonagg-level `try_index_for_in_clause()` could properly filter using the index
4. Row indices remained correct because no early filtering occurred

### Why PR #1905 Broke It

PR #1905 re-enabled predicate pushdown, which means:
1. The scan-level code tries to optimize using `multi_lookup()`
2. `multi_lookup()` creates single-element keys for multi-column indexes
3. No matches found → empty result set
4. Row indices are irrelevant because there are no rows

## Affected Tests

From the dogfooding database, 7 tests are failing:
1. `third_party/sqllogictest/test/select4.test`
2. `third_party/sqllogictest/test/select5.test`
3. `tests/issue-1807/multicolumn_in_basic.test`
4. `tests/issue-1807/multicolumn_in_btreemap_ordering.test`
5. `tests/issue-1807/multicolumn_in_distinct.test`
6. `tests/issue-1807/multicolumn_in_edge_cases.test`
7. `tests/issue-1807/multicolumn_in_predicate_pushdown.test`

All involve multi-column indexes with IN clauses on the first column.

## Proposed Solutions

### Option 1: Prefix Matching in Storage Layer (Comprehensive)

**Approach**: Extend `multi_lookup()` to support prefix matching for multi-column indexes.

**Implementation**:
1. Modify `IndexData::multi_lookup()` signature to include index metadata
2. Check if index is multi-column
3. For multi-column indexes with partial predicates:
   - Use `range_scan()` with prefix bounds instead of exact lookup
   - For each value `v` in IN list, scan from `[v]` to `[v+1]` (exclusive)
   - This returns all rows where first column matches `v`
4. Keep exact lookup for single-column indexes

**Pros**:
- Keeps predicate pushdown optimization
- Proper architectural solution at storage layer
- Works for all multi-column index scenarios

**Cons**:
- More complex implementation
- Requires range scans instead of point lookups (slightly slower)

### Option 2: Re-enable Conditional Predicate Pushdown (Quick Fix)

**Approach**: Restore PR #1845's conditional predicate pushdown logic.

**Implementation**:
1. Re-add `requires_predicate_pushdown_disable()` check in execute.rs
2. Re-add `try_index_for_in_clause()` to nonagg.rs
3. Disable predicate pushdown for multi-column IN predicates

**Pros**:
- Known working solution
- Minimal code changes
- Can be implemented quickly

**Cons**:
- Returns to pre-PR #1905 architecture
- Loses some predicate pushdown benefits
- Doesn't fix the underlying storage layer issue

### Option 3: Hybrid Approach (Recommended)

**Approach**: Implement Option 1 properly, with Option 2 as temporary fallback.

**Implementation Phase 1** (Quick):
1. Re-enable conditional predicate pushdown (Option 2)
2. Get tests passing immediately

**Implementation Phase 2** (Comprehensive):
1. Implement prefix matching in storage layer (Option 1)
2. Remove conditional predicate pushdown logic
3. Full predicate pushdown with proper multi-column support

**Pros**:
- Immediate fix for failing tests
- Clear path to proper architectural solution
- Each phase is independently testable

**Cons**:
- Two-phase implementation requires coordination

## Recommendation

Implement **Option 3 (Hybrid Approach)**:
1. Phase 1: Quick fix to restore test passing
2. Phase 2: Comprehensive architectural fix

This balances immediate progress with long-term code quality.

## References

- Issue #1922: Fix 'other' category test failures
- Issue #1807: Original multi-column IN optimization issue
- PR #1845: Initial multi-column IN fix
- PR #1905: Index optimization refactoring (introduced regression)
- Test files: `tests/issue-1807/multicolumn_in_*.test`

---

**Analysis Date**: 2025-11-16
**Analyzed By**: Claude (Builder agent)
**Worktree**: `.loom/worktrees/issue-1922`
