# Derived Table UNION+ORDER BY Bug Investigation

**Issue**: #4476  
**Status**: Open - Architectural changes needed  
**Impact**: HIGH - Blocks 50-100+ TCL tests

## Quick Summary

Query pattern fails: `SELECT x FROM (SELECT a AS x FROM t3 UNION SELECT a FROM t3 ORDER BY a)`

**Root Cause**: Schema context not properly propagated through compound query execution. The evaluator uses the underlying table schema (`t3`) instead of the derived table's schema with aliased columns.

**Recommended Fix**: Implement `SchemaContext` wrapper (similar to `TableIdentifier` pattern) to track effective schema at each execution level.

## Reproducing

```bash
# Minimal test case
echo "
CREATE TABLE t3(a,b);
INSERT INTO t3 VALUES(1,'a'),(2,'b');
SELECT x FROM (SELECT a AS x FROM t3 UNION SELECT a FROM t3 ORDER BY a);
" | ./target/release/vibesql test.db

# Expected: Returns rows with column 'x'
# Actual: Error: Column 'x' not found (searched tables: t3)
```

## What Works vs What Fails

| Query Pattern | Status | Notes |
|--------------|--------|-------|
| `SELECT x FROM (SELECT a AS x FROM t3)` | ✅ Works | Simple derived table |
| `SELECT x FROM (SELECT a AS x FROM t3 UNION SELECT a FROM t3)` | ✅ Works | UNION without ORDER BY |
| `SELECT x FROM (SELECT a AS x FROM t3 UNION SELECT a FROM t3 ORDER BY a)` | ❌ Fails | UNION with ORDER BY inside |
| `SELECT x FROM (SELECT a AS x FROM t3) ORDER BY x` | ✅ Works | ORDER BY outside |

## Debug Evidence

Error occurs during UNION subquery execution:

```
[DEBUG execute_derived_table] START - alias='(subquery-0)'
[DEBUG execute_derived_table] Subquery has ORDER BY with 1 items
[DEBUG execute_derived_table]   ORDER BY[0]: ColumnRef { table: None, column: "a" }
[DEBUG combined eval] Column 'x' not found
[DEBUG combined eval] searched_tables=["t3"]  ← WRONG SCHEMA!
[DEBUG combined eval] available_columns=["a", "b"]
```

The error happens at:
- `crates/vibesql-executor/src/select/scan/derived.rs:96` - during `execute_subquery(query)?`
- `crates/vibesql-executor/src/evaluator/combined/eval.rs:127` - when looking up column 'x'

## Key Code Locations

**Derived table execution**:
```rust
// crates/vibesql-executor/src/select/scan/derived.rs:84
pub(crate) fn execute_derived_table<F>(query: &vibesql_ast::SelectStmt, ...) {
    let subquery_result = execute_subquery(query)?;  // ← Error here
}
```

**Subquery executor closure**:
```rust
// crates/vibesql-executor/src/select/executor/execute.rs:1115-1129
|query| {
    if !cte_results.is_empty() {
        let child = SelectExecutor::new_with_cte_and_depth(...);
        child.execute_with_columns(query)
    } else {
        self.execute_with_columns(query)  // ← Reuses parent executor
    }
}
```

**Error thrown**:
```rust
// crates/vibesql-executor/src/evaluator/combined/eval.rs:127
let searched_tables: Vec<String> = self.schema.table_names();
// Returns ["t3"] instead of ["(subquery-0)"] or the derived table schema
```

## Proposed Architecture

Introduce `SchemaContext` (similar to `TableIdentifier`):

```rust
pub struct SchemaContext<'a> {
    schema: &'a CombinedSchema,
    effective_name: Option<String>,  // For derived tables, CTEs
    parent: Option<&'a SchemaContext<'a>>,
}
```

This would:
- Track schema transformations (aliases, derived tables, CTEs) explicitly
- Propagate through evaluators instead of raw `CombinedSchema`
- Handle nested contexts (subqueries within subqueries)
- Make column resolution deterministic

## Impact Assessment

**TCL Tests Blocked**: 50-100+ tests including:
- select1-6.9.6, select1-6.9.7, select1-6.9.8
- select1-6.11, select1-6.23
- select1-7.9, select1-12.9, select1-12.10
- select1-13.1, select1-18.x series
- Many more...

**Current**: 55.9% pass rate (377/674) on Priority 1 TCL tests  
**After Fix**: Estimated 70-80% pass rate

## Related Work

- **TableIdentifier pattern** (commit 25605a2f): Solved similar aliasing issue for table names
- **full_column_names PRAGMA**: Related schema name handling
- **Issue #4218**: Overall 100% TCL conformance effort

## Next Steps

1. Design `SchemaContext` API
2. Update evaluators to use SchemaContext
3. Modify executor to create proper context at derived table boundaries
4. Add comprehensive tests for nested derived tables
5. Verify with TCL test suite

---

**Investigation Date**: 2025-12-14  
**Documented By**: Claude Opus 4.5 via dogfooding VibeSQL  
**See Also**: /tmp/dogfooding_summary.md
