# SQLite Affinity and Collation: Architectural Design

## Problem Statement

VibeSQL's TCL test pass rate is limited (~26%) partly because comparison operators don't implement SQLite's type affinity rules. SQLite's comparison behavior depends on:

1. **Column affinity** - The declared type of a column (TEXT, INTEGER, REAL, NUMERIC, BLOB, or NONE)
2. **Value origin** - Whether a value comes from a column reference or a literal
3. **Collation** - The collating sequence (BINARY, NOCASE, RTRIM)

## SQLite Comparison Rules (Empirically Verified)

### Rule 1: TEXT Affinity Column vs INTEGER Literal
```sql
CREATE TABLE t(a TEXT);
INSERT INTO t VALUES('10');
SELECT a > 2 FROM t;   -- Returns 0 (false)
SELECT a < 2 FROM t;   -- Returns 1 (true)
```
**Behavior**: Convert INTEGER to TEXT, do string comparison.
- '10' vs '2' → '1' < '2' → '10' < '2' is TRUE

### Rule 2: No Affinity (Bare Column) vs INTEGER Literal
```sql
CREATE TABLE t(a);  -- No type = no affinity
INSERT INTO t VALUES('10');
SELECT a > 2 FROM t;   -- Returns 1 (true)
SELECT a < 2 FROM t;   -- Returns 0 (false)
```
**Behavior**: Use storage class ordering (TEXT > INTEGER always).

### Rule 3: INTEGER/NUMERIC Affinity Column vs TEXT Literal
```sql
CREATE TABLE t(a INTEGER);
INSERT INTO t VALUES(10);
SELECT a > '5' FROM t;   -- Returns 1 (numeric: 10 > 5)
SELECT a > 'abc' FROM t; -- Returns 0 (can't convert, INTEGER < TEXT)
```
**Behavior**: Try to convert TEXT to number. If successful, numeric compare. Otherwise, use storage class ordering.

### Rule 4: Literal vs Literal (No Columns)
```sql
SELECT '10' > 2;  -- Returns 1 (TEXT > INTEGER in type ordering)
SELECT 10 > '5';  -- Returns 1 (INTEGER < TEXT, so 10 > '5' is false... wait)
```
**Behavior**: Use storage class ordering for mixed types.

### Rule 5: Same Storage Class
```sql
SELECT '10' > '2';  -- String comparison: '1' < '2', so false (0)
SELECT 10 > 2;      -- Numeric comparison: true (1)
```
**Behavior**: Compare directly using appropriate comparison for that type.

## Current VibeSQL Architecture

### SqlValue (vibesql-types)
```rust
pub enum SqlValue {
    Null,
    Boolean(bool),
    Integer(i64),
    // ... other variants
    Varchar(ArcStr),
    // ...
}
```
- No tracking of origin (column vs literal)
- No tracking of affinity
- No tracking of collation

### Comparison Operators (evaluator/operators/comparison)
```rust
pub fn compare(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, Error> {
    // Currently uses simple type ordering for mismatched types
    // TEXT > INTEGER always
}
```

### Expression Evaluation
```rust
fn eval_column_ref(&self, col: &str) -> SqlValue {
    // Returns just the value, loses affinity information
}
```

## Proposed Architecture

### Option A: Annotated Values (Recommended)

Add optional metadata to track value context:

```rust
/// Metadata about a value's origin and comparison behavior
#[derive(Clone, Debug, Default)]
pub struct ValueContext {
    /// The column affinity if this value came from a column
    pub affinity: Option<TypeAffinity>,
    /// The collation sequence
    pub collation: Option<Collation>,
    /// Whether this is from a column reference (vs literal)
    pub from_column: bool,
}

#[derive(Clone, Debug)]
pub enum TypeAffinity {
    Text,
    Numeric,
    Integer,
    Real,
    Blob,
    None,  // Bare column with no declared type
}

#[derive(Clone, Debug)]
pub enum Collation {
    Binary,
    Nocase,
    Rtrim,
}
```

**During evaluation**, when reading a column:
```rust
fn eval_column_ref(&self, col_ref: &ColumnRef, row: &Row) -> (SqlValue, ValueContext) {
    let value = row.get(col_idx);
    let schema = self.get_column_schema(col_ref);
    let context = ValueContext {
        affinity: Some(schema.affinity()),
        collation: schema.collation(),
        from_column: true,
    };
    (value, context)
}
```

**During comparison**:
```rust
fn compare_with_context(
    left: &SqlValue, left_ctx: &ValueContext,
    right: &SqlValue, right_ctx: &ValueContext,
) -> Ordering {
    match (left_ctx.affinity, right_ctx.affinity) {
        // TEXT column vs INTEGER literal
        (Some(TypeAffinity::Text), None) if right.is_integer() => {
            // Convert right to text, string compare
            let right_text = right.to_string();
            left.as_str().cmp(&right_text)
        }
        // No affinity vs INTEGER (type ordering)
        (Some(TypeAffinity::None), None) | (None, None) => {
            // Use storage class ordering
            storage_class_compare(left, right)
        }
        // ... other cases
    }
}
```

### Option B: Evaluation Context Map

Track column origins in a side structure:

```rust
struct EvalContext {
    /// Maps evaluated value indices to their source column info
    column_sources: HashMap<ValueId, ColumnInfo>,
}
```

**Pros**: Doesn't change SqlValue
**Cons**: Complex to maintain through expression evaluation

### Option C: Comparison-Time Schema Lookup

Pass schema access through comparison:

```rust
fn compare_expr(
    left_expr: &Expression,
    right_expr: &Expression,
    schema: &TableSchema,
) -> Ordering {
    // Determine affinity from expressions
    let left_affinity = get_affinity(left_expr, schema);
    let right_affinity = get_affinity(right_expr, schema);
    // ...
}
```

**Pros**: Pure, no side effects
**Cons**: Requires expression context during comparison, complex threading

## Recommended Implementation Plan

### Phase 1: Value Context Infrastructure
1. Add `ValueContext` struct to vibesql-types
2. Modify expression evaluators to return `(SqlValue, ValueContext)` tuples
3. Update comparison operators to accept context

### Phase 2: Column Affinity Tracking
1. Extract affinity from column schema during column reference evaluation
2. Propagate affinity through expression evaluation
3. Implement affinity-aware comparison rules

### Phase 3: Collation Propagation
1. Extract collation from column schema
2. Handle explicit COLLATE expressions (partially done)
3. Propagate column-level collation

### Phase 4: CAST Behavior
1. Make CAST more permissive (SQLite allows CAST('abc' AS INTEGER) = 0)
2. Handle partial numeric parsing

## Impact Assessment

**Files to modify**:
- `vibesql-types/src/value.rs` - Add ValueContext
- `vibesql-executor/src/evaluator/expressions/eval.rs` - Context propagation
- `vibesql-executor/src/evaluator/combined/eval.rs` - Context propagation
- `vibesql-executor/src/evaluator/operators/comparison/mod.rs` - Affinity-aware comparison
- `vibesql-executor/src/evaluator/expressions/predicates.rs` - BETWEEN, etc.

**Risk**: Medium-high
- Changes touch core evaluation path
- Need extensive testing to avoid regressions
- Some edge cases may be tricky

**Expected Impact**:
- Should fix between-2.1.1-3 tests (TEXT vs INTEGER)
- Should fix many other comparison-related failures
- Estimated 5-10% improvement in TCL pass rate

## Alternative: Minimal Fix

If full architecture change is too risky, a minimal fix could:

1. **In comparison operators only**: Check if comparing TEXT string that looks numeric to INTEGER
2. Apply string comparison (convert INTEGER to TEXT)
3. This is a heuristic that would work for most cases but not be fully correct

```rust
fn compare_text_to_integer(text: &str, int: i64) -> Ordering {
    // Convert integer to string and do string comparison
    let int_str = int.to_string();
    text.cmp(&int_str)
}
```

This would be simpler but less correct (can't distinguish column vs literal).
