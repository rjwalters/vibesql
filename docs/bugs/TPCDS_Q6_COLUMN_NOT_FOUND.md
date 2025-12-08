# TPC-DS Q6 Column Not Found Bug

## Error Message
```
Q6 ERROR: Column 'I_CURRENT_PRICE' not found (searched tables: J).
Available columns: i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date,
i_item_desc, i_current_price, i_wholesale_cost, ...
```

## The Query (Q6)
```sql
SELECT
    a.ca_state state,
    COUNT(*) cnt
FROM customer_address a, customer c, store_sales s, date_dim d, item i
WHERE a.ca_address_sk = c.c_current_addr_sk
    AND c.c_customer_sk = s.ss_customer_sk
    AND s.ss_sold_date_sk = d.d_date_sk
    AND s.ss_item_sk = i.i_item_sk
    AND d.d_month_seq = (
        SELECT DISTINCT d_month_seq
        FROM date_dim
        WHERE d_year = 2000 AND d_moy = 1
    )
    AND i.i_current_price > 1.2 * (
        SELECT AVG(j.i_current_price)    -- <-- ERROR HERE
        FROM item j
        WHERE j.i_category = i.i_category
    )
GROUP BY a.ca_state
HAVING COUNT(*) >= 10
ORDER BY cnt
LIMIT 100
```

## Root Cause Analysis

### Key Observations
1. **Case mismatch**: Error shows `I_CURRENT_PRICE` (uppercase) but available columns are `i_current_price` (lowercase)
2. **Table alias**: Error shows `J` (uppercase) but query uses `j` (lowercase)
3. **Correlated subquery**: The problematic subquery references `j.i_current_price` and correlates on `i.i_category`

### The Bug
The column lookup is failing because:
- The **parser** is preserving or uppercasing the column name from the SQL
- The **schema** stores column names in lowercase
- The **lookup** should be case-insensitive but something is breaking

### Code Path
1. Query is parsed, AST contains `j.i_current_price` (parser may uppercase to `J.I_CURRENT_PRICE`)
2. Correlated subquery execution builds schema for table `item` with alias `j`
3. Column lookup in `CombinedSchema::get_column_index()` at `schema.rs:170`
4. Delegates to `TableSchema::get_column_index()` at `table.rs:204`
5. TableSchema lookup has case-insensitive fallback but something fails

### Relevant Files
- `crates/vibesql-executor/src/schema.rs:170` - CombinedSchema column lookup
- `crates/vibesql-catalog/src/table.rs:204` - TableSchema column lookup
- `crates/vibesql-executor/src/evaluator/combined/eval.rs:61-116` - Column resolution and error generation
- `crates/vibesql-executor/src/evaluator/combined/subqueries/scalar.rs` - Scalar subquery execution
- `crates/vibesql-executor/benches/tpcds/queries.rs:415-438` - Q6 definition

## Hypotheses to Test

### Hypothesis 1: Parser Case Handling
The parser may be uppercasing identifiers. Check:
- `crates/vibesql-parser/` for identifier normalization
- Are unquoted identifiers being uppercased per SQL standard?

### Hypothesis 2: Schema Building for Subquery
When building the schema for the correlated subquery:
- Is the alias `j` being registered correctly?
- Is the `item` table schema being copied with correct column names?

### Hypothesis 3: Cache Key Mismatch
The column cache in `get_column_index_cached()` uses exact string hashing:
- If parser gives uppercase and schema has lowercase, cache miss
- Then fallback lookup should work, but maybe it's not being called?

## Reproduction Steps
```bash
make benchmark-smoke
# or
SCALE_FACTOR=0.001 QUERY_FILTER=Q6 cargo bench --bench tpcds_runner
```

## Key Findings

### Finding 1: Parser Uppercases All Identifiers
File: `crates/vibesql-parser/src/lexer/identifiers.rs:55`
```rust
// Not a keyword - now allocate for the identifier
Ok(Token::Identifier(upper.to_string()))
```
All unquoted identifiers like `j` and `i_current_price` become `J` and `I_CURRENT_PRICE`.

### Finding 2: Schema Stores Lowercase Column Names
File: `crates/vibesql-executor/benches/tpcds/schema.rs:794`
```rust
name: "i_current_price".to_string(),
```
TPC-DS schema is created with lowercase column names.

### Finding 3: TableKey Normalizes to Lowercase
File: `crates/vibesql-executor/src/schema.rs:16`
```rust
TableKey(name.as_ref().to_lowercase())
```
Table lookups are case-insensitive via TableKey normalization.

### Finding 4: Column Lookup Has Case-Insensitive Fallback
File: `crates/vibesql-catalog/src/table.rs:212-216`
```rust
let name_lower = name.to_lowercase();
self.column_index_cache.iter()
    .find(|(k, _)| k.to_lowercase() == name_lower)
    .map(|(_, &idx)| idx)
```
This fallback SHOULD find `i_current_price` when looking for `I_CURRENT_PRICE`.

## Current Hypothesis

The error message shows `searched tables: J` which is **uppercase**. But `table_names()`
returns from `TableKey::to_string()` which stores lowercase values. This suggests:

**The schema being searched does NOT contain the subquery's table `j` at all.**

The available columns show the `item` table columns twice (same columns repeated),
suggesting the outer query's `item i` table is visible but the subquery's `item j` is not.

### Possible Root Cause
When the correlated subquery expression `j.i_current_price` is evaluated, the evaluator
is using a schema that doesn't include the subquery's own FROM clause tables.

The flow might be:
1. Outer query builds schema with `item` as alias `i` (plus other tables)
2. Scalar subquery is evaluated with outer_schema containing `i`
3. Subquery executes `FROM item j` - builds its own schema with alias `j`
4. BUT when evaluating `j.i_current_price` in WHERE clause, evaluator might
   only see outer schema (with `i`) not the subquery's schema (with `j`)

## Files to Investigate

1. `crates/vibesql-executor/src/select/executor/execute.rs` - How subquery execution builds its schema
2. `crates/vibesql-executor/src/select/scan/predicates.rs` - How WHERE clause is evaluated
3. Check if `apply_table_local_predicates` or similar is using wrong schema

## Diagnosis Summary

After extensive code tracing, the issue appears to be one of the following:

### Most Likely: Uppercase "J" in Error is from table_name Field
The error shows "searched tables: J" but `table_names()` returns lowercase. This could mean:
1. The `table_name` field (from AST) is being displayed somewhere
2. Or there's a code path that doesn't use `table_names()`

### Alternative: Schema Doesn't Contain Subquery Table
The error could indicate the evaluator's schema doesn't have table `j` at all,
meaning the outer schema tables are being searched instead of the subquery's schema.

## Test to Reproduce

Create a minimal test case:

```rust
#[test]
fn test_correlated_subquery_column_resolution() {
    let mut db = vibesql_storage::Database::new();

    // Create table with lowercase column names
    db.execute_sql("CREATE TABLE item (i_item_sk INT, i_current_price DECIMAL(7,2), i_category VARCHAR(50))");
    db.execute_sql("INSERT INTO item VALUES (1, 10.00, 'A'), (2, 20.00, 'A'), (3, 15.00, 'B')");

    // Query similar to TPC-DS Q6 scalar subquery
    let sql = r"
        SELECT i.i_current_price
        FROM item i
        WHERE i.i_current_price > (
            SELECT AVG(j.i_current_price)
            FROM item j
            WHERE j.i_category = i.i_category
        )
    ";

    let result = db.execute_sql(sql);
    assert!(result.is_ok(), "Correlated subquery should resolve j.i_current_price");
}
```

## Next Steps
1. Add debug logging in `get_column_index` when column not found to see exact schema contents
2. Trace the schema passed to evaluator when executing subquery WHERE clause
3. Check if `table_names()` is actually returning uppercase or if error comes from different path
4. Verify subquery's own FROM clause tables are included in evaluation schema
