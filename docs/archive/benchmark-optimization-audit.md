# Benchmark Optimization Audit Report

Issue: #3155

## Executive Summary

This audit examined the vibesql codebase for benchmark-specific optimizations that don't generalize to real workloads. **The codebase is clean** - no benchmark-specific cheating was found. All optimizations are general-purpose, with TPC-H/TPC-DS references serving only as documentation explaining what query patterns benefit from each optimization.

## Audit Methodology

Searched for:
1. Pattern matching on specific TPC table/column names (e.g., `== "lineitem"`)
2. Hardcoded TPC-H/TPC-DS/TPC-C table names in optimizer logic
3. Magic numbers matching TPC schema cardinalities or column indices
4. Hardcoded query plans
5. Comments mentioning specific TPC queries (to review the referenced code)

## Findings

### Already Resolved: Hardcoded Query Plans (#3154)

PR #3142 (merged Dec 1, 2025) removed all hardcoded TPC-H query plans:
- `tpch.rs` - removed entirely
- `TpchQ1Plan`, `TpchQ3Plan`, `TpchQ6Plan` - removed
- `TpchQ6JitPlan` from `jit.rs` - removed
- All monomorphic execution code replaced by general columnar execution

**Status**: Issue #3154 closed as completed.

### No Benchmark-Specific Pattern Matching Found

Searched for table name comparisons (`== "lineitem"`, `== "orders"`, etc.) in production code:
- **Result**: No matches in optimizer or executor logic
- The only match was in test code (`alias_resolution.rs:183` - a test assertion)

### TPC-DS and TPC-C Table References

All references to TPC-DS tables (`store_sales`, `catalog_sales`, `web_sales`) and TPC-C tables (`warehouse`, `district`, `stock`) are in:
- Benchmark files (`benches/tpcds/`, `benches/tpcc/`)
- Test files
- Example code

**No production optimizer code references these tables.**

### No Hardcoded Magic Numbers

Searched for TPC-specific cardinalities (6,000,000 lineitem rows, 60,175, etc.):
- All magic number references are in test cases only
- Optimizer uses runtime statistics for cardinality estimation

## Documentation vs. Implementation

The codebase contains many comments referencing TPC queries (e.g., "TPC-H Q1 pattern", "TPC-H Q6 optimization"). These are **documentation**, not implementation logic:

| File | Reference | Assessment |
|------|-----------|------------|
| `grouping/keys.rs:38` | "TPC-H Q1: l_returnflag, l_linestatus" | **General-purpose** - optimizes ANY two single-char VARCHAR columns |
| `grouping/keys.rs:48` | "TPC-H Q3 pattern" | **General-purpose** - optimizes ANY three integer columns |
| `simd_ops/aggregate.rs:173` | "TPC-H Q6: SUM(price * discount)" | **General-purpose** - optimizes ANY SUM(col * col) |
| `columnar_execution.rs:26` | "TPC-H Q1 style queries" | **General-purpose** - enables columnar path for matching patterns |
| `join_analyzer.rs:116` | "TPC-H Q3, Q7, Q10" | **General-purpose** - multi-way join optimization |

## Confirmed General-Purpose Optimizations

These optimizations were discovered through TPC analysis but apply to ANY schema with matching patterns:

1. **GroupKeySpec variants** (`keys.rs`)
   - TwoChars: Any two single-char string columns
   - ThreeI64: Any three integer columns
   - Detection based on data types, not table names

2. **DateRangeEvaluator** (`specialized.rs`)
   - Works on any date column with range predicate
   - No schema-specific logic

3. **Arithmetic Equijoin** (recent optimization)
   - Pattern: `col = col2 - constant`
   - Works on any columns matching the pattern

4. **Pivot Aggregate Optimization**
   - Pattern: Multiple `SUM(CASE WHEN...)` aggregates
   - Works on any query with this structure

5. **Compiled CASE Expressions**
   - Optimizes CASE expressions in aggregates
   - No schema-specific logic

## Minor Concern: TPC-H Naming Convention Heuristic

**File**: `optimizer/subquery_rewrite/correlation.rs:104-121`

**Description**: Uses a heuristic that matches TPC-H-style column naming (`x_column` format like `o_orderkey`, `l_shipdate`) to detect correlation in subqueries.

**Analysis**:
```rust
// Only apply TPC-H heuristic if column appears to follow TPC-H naming
// convention (prefix + underscore, e.g., "o_orderkey", "l_shipdate")
if column.chars().nth(1) == Some('_') {
    let from_table_prefixes = extract_table_prefixes(from);
    !from_table_prefixes.contains(&col_prefix)
} else {
    // Not TPC-H style column name: assume internal (conservative)
    false
}
```

**Verdict**: **Low concern**
- Well-documented with clear rationale
- Falls back to conservative/safe behavior for non-matching columns
- The `x_column` naming convention is common outside TPC-H (many real databases use table-prefix naming)
- Does not break correctness, only affects optimization choices

**Recommendation**: Document that this heuristic also benefits any schema using table-prefixed column names, not just TPC-H.

## Items Needing Remediation

None. No benchmark-specific cheating was found.

## Documentation Updates Recommended

1. Update comments in `correlation.rs` to note the heuristic applies to any schema using `x_column` naming, not just TPC-H

2. Consider adding a section to developer documentation explaining the distinction between:
   - **Benchmark-discovered**: Optimizations found through benchmark analysis that apply generally
   - **Benchmark-specific**: Optimizations that only work for specific benchmark schemas (none exist)

## Conclusion

The vibesql codebase maintains a clear separation between:
- **Production code**: Contains only general-purpose optimizations
- **Benchmark code**: TPC-H/TPC-DS/TPC-C schemas and queries are isolated in `benches/` directories
- **Test code**: Uses TPC-style names for realistic testing, but no special-casing

All performance optimizations are based on query structure and data types, not schema-specific patterns. The codebase is free of benchmark cheating.
