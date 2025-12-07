#![allow(clippy::doc_lazy_continuation)]

use std::collections::HashSet;

use super::hash_join::build_existence_hash_table_parallel;
use super::{combine_rows, FromResult};
use crate::errors::ExecutorError;
use crate::evaluator::CombinedExpressionEvaluator;
use crate::optimizer::combine_with_and;
use crate::optimizer::where_pushdown::{extract_referenced_tables_branch, flatten_conjuncts};
use crate::schema::CombinedSchema;
use crate::timeout::{TimeoutContext, CHECK_INTERVAL};

/// Hash semi-join implementation
///
/// Semi-join returns rows from the LEFT table that have a match in the RIGHT table.
/// Unlike inner join, each left row is returned at most ONCE, regardless of how many
/// right rows match.
///
/// Use cases:
/// - EXISTS subqueries: SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM lineitem WHERE l_orderkey = o_orderkey)
/// - IN subqueries: SELECT * FROM orders WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem)
///
/// Algorithm:
/// 1. Build phase: Hash the RIGHT table into a HashSet (O(n))
/// 2. Probe phase: For each row in LEFT table, check if key exists in hash set (O(m))
/// 3. If key exists, emit the LEFT row (only once)
/// Total: O(n + m) vs O(n*m) for nested loop
///
/// Performance characteristics:
/// - Time: O(n + m) vs O(n*m) for nested loop
/// - Space: O(n) where n is the size of the right table (smaller than inner join because we don't store indices)
/// - Expected speedup: 100-10,000x for large semi-joins
pub(super) fn hash_semi_join(
    left: FromResult,
    right: FromResult,
    left_col_idx: usize,
    right_col_idx: usize,
) -> Result<FromResult, ExecutorError> {
    // Use default timeout context (proper propagation from SelectExecutor is a future improvement)
    let timeout_ctx = TimeoutContext::new_default();

    // Use as_slice() for zero-cost access without triggering row materialization
    let left_slice = left.as_slice();
    let right_slice = right.as_slice();

    // Build phase: Create hash table from right side (using parallel algorithm)
    // Key: join column value
    // Value: () (we only need to know if the key exists, not store row indices)
    // Automatically uses parallel build when beneficial (based on row count and hardware)
    let hash_table = build_existence_hash_table_parallel(right_slice, right_col_idx, &timeout_ctx)?;

    // Probe phase: Check each left row for a match
    // We only emit left rows that have a match in the right table
    let estimated_capacity = left_slice.len().min(100_000);
    let mut result_rows = Vec::with_capacity(estimated_capacity);

    for (idx, left_row) in left_slice.iter().enumerate() {
        // Check timeout periodically during probe phase
        if idx % CHECK_INTERVAL == 0 {
            timeout_ctx.check()?;
        }

        let key = &left_row.values[left_col_idx];

        // Skip NULL values - they never match in equi-joins
        if key == &vibesql_types::SqlValue::Null {
            continue;
        }

        // If key exists in hash table, emit this left row (only once)
        if hash_table.contains_key(key) {
            result_rows.push(left_row.clone());
        }
    }

    // Return result with left schema only (we don't combine with right schema)
    Ok(FromResult::from_rows(left.schema.clone(), result_rows))
}

/// Hash semi-join with additional filter conditions
///
/// This is an optimized version of hash_semi_join that supports additional filter predicates
/// beyond the equi-join condition. This is essential for EXISTS subqueries with complex WHERE clauses.
///
/// Example use case (TPC-H Q21):
/// ```sql
/// EXISTS (
///     SELECT * FROM lineitem l2
///     WHERE l2.l_orderkey = l1.l_orderkey    -- Equi-join (used for hash table)
///       AND l2.l_suppkey <> l1.l_suppkey     -- Additional filter (checked during probe)
/// )
/// ```
///
/// ## Optimization: Right-Side Predicate Pushdown
///
/// When the filter contains predicates that reference only the right table (e.g., `s_quantity < 10`),
/// these predicates are applied during the hash table build phase, reducing the hash table size
/// and avoiding unnecessary probing. This is critical for IN subquery performance.
///
/// Example (TPC-C Stock-Level):
/// ```sql
/// ol_i_id IN (SELECT s_i_id FROM stock WHERE s_w_id = 1 AND s_quantity < 10)
/// ```
/// The predicates `s_w_id = 1 AND s_quantity < 10` are applied during build, not probe.
///
/// Algorithm:
/// 1. **Predicate partitioning**: Split filter into right-only and cross-table predicates
/// 2. **Build phase**: Hash the RIGHT table, applying right-only predicates as a pre-filter
/// 3. **Probe phase**: For each LEFT row:
///    a. Check if hash table contains matching key
///    b. If yes, verify cross-table predicates against matching right rows
///    c. If any right row passes, emit the left row (only once)
///
/// Performance: O(n + m) average case with reduced hash table size
pub(super) fn hash_semi_join_with_filter(
    left: FromResult,
    right: FromResult,
    left_col_idx: usize,
    right_col_idx: usize,
    additional_filter: Option<&vibesql_ast::Expression>,
    combined_schema: &CombinedSchema,
    database: &vibesql_storage::Database,
) -> Result<FromResult, ExecutorError> {
    let debug = std::env::var("SEMI_JOIN_DEBUG").is_ok();
    let start = std::time::Instant::now();

    // If no additional filter, use the simpler version
    if additional_filter.is_none() {
        return hash_semi_join(left, right, left_col_idx, right_col_idx);
    }

    // Use default timeout context (proper propagation from SelectExecutor is a future improvement)
    let timeout_ctx = TimeoutContext::new_default();

    let filter = additional_filter.unwrap();

    // Use as_slice() for zero-cost access without triggering row materialization
    let left_slice = left.as_slice();
    let right_slice = right.as_slice();

    if debug {
        eprintln!("[SEMI_JOIN] left_rows={}, right_rows={}", left_slice.len(), right_slice.len());
        eprintln!("[SEMI_JOIN] filter={:?}", filter);
    }

    // Partition the filter into right-only and cross-table predicates
    // Right-only predicates are applied during build, cross-table during probe
    let (right_only_filter, probe_filter) =
        partition_filter_predicates(filter, combined_schema, &left.schema, &right.schema);

    if debug {
        eprintln!("[SEMI_JOIN] right_only_filter={:?}", right_only_filter);
        eprintln!("[SEMI_JOIN] probe_filter={:?}", probe_filter);
    }

    // Create evaluators for build and probe phases
    // Right-only evaluator uses only right schema
    let right_only_evaluator = right_only_filter
        .as_ref()
        .map(|_| CombinedExpressionEvaluator::with_database(&right.schema, database));

    // Combined evaluator for probe phase (if we have cross-table predicates)
    let probe_evaluator = probe_filter
        .as_ref()
        .map(|_| CombinedExpressionEvaluator::with_database(combined_schema, database));

    // Build phase: Create hash table from right side with right-only predicate filtering
    use ahash::AHashMap;
    let mut hash_table: AHashMap<vibesql_types::SqlValue, Vec<usize>> = AHashMap::new();
    let mut _filtered_count = 0usize;

    for (idx, row) in right_slice.iter().enumerate() {
        // Check timeout periodically during build phase
        if idx % CHECK_INTERVAL == 0 {
            timeout_ctx.check()?;
        }

        let key = row.values[right_col_idx].clone();
        // Skip NULL values - they never match in equi-joins
        if key == vibesql_types::SqlValue::Null {
            continue;
        }

        // Apply right-only predicates as a pre-filter during build
        if let (Some(right_filter), Some(evaluator)) = (&right_only_filter, &right_only_evaluator) {
            evaluator.clear_cse_cache();
            match evaluator.eval(right_filter, row) {
                Ok(vibesql_types::SqlValue::Boolean(true)) => {
                    // Row passes filter, add to hash table
                    hash_table.entry(key).or_default().push(idx);
                }
                Ok(vibesql_types::SqlValue::Boolean(false)) | Ok(vibesql_types::SqlValue::Null) => {
                    // Row doesn't pass filter, skip it
                    if debug && _filtered_count < 3 {
                        eprintln!(
                            "[SEMI_JOIN] Filtered out row: {:?}",
                            &row.values[..3.min(row.values.len())]
                        );
                    }
                    _filtered_count += 1;
                    continue;
                }
                Err(e) => {
                    // Filter evaluation error, skip this row
                    if debug && _filtered_count < 3 {
                        eprintln!("[SEMI_JOIN] Error evaluating filter: {:?}", e);
                    }
                    _filtered_count += 1;
                    continue;
                }
                Ok(v) => {
                    // Filter didn't return boolean, skip this row
                    if debug && _filtered_count < 3 {
                        eprintln!("[SEMI_JOIN] Non-boolean filter result: {:?}", v);
                    }
                    _filtered_count += 1;
                    continue;
                }
            }
        } else {
            // No right-only filter, add all rows
            hash_table.entry(key).or_default().push(idx);
        }
    }

    let build_time = start.elapsed();
    if debug {
        eprintln!(
            "[SEMI_JOIN] build_time={:?}, hash_table_size={}, filtered_count={}",
            build_time,
            hash_table.len(),
            _filtered_count
        );
    }

    // Probe phase: Check each left row for a match that passes the probe filter
    let estimated_capacity = left_slice.len().min(100_000);
    let mut result_rows = Vec::with_capacity(estimated_capacity);
    let mut probe_iterations = 0;

    for left_row in left_slice.iter() {
        // Check timeout periodically during probe phase
        probe_iterations += 1;
        if probe_iterations % CHECK_INTERVAL == 0 {
            timeout_ctx.check()?;
        }

        let key = &left_row.values[left_col_idx];

        // Skip NULL values - they never match in equi-joins
        if key == &vibesql_types::SqlValue::Null {
            continue;
        }

        // Check if key exists in hash table
        if let Some(right_indices) = hash_table.get(key) {
            // If no probe filter, any match is sufficient (right-only filter already applied)
            if probe_filter.is_none() {
                if !right_indices.is_empty() {
                    result_rows.push(left_row.clone());
                }
                continue;
            }

            // Check if any matching right row passes the probe filter
            let mut found_match = false;
            let evaluator = probe_evaluator.as_ref().unwrap();
            let pf = probe_filter.as_ref().unwrap();

            for &right_idx in right_indices {
                let right_row = &right_slice[right_idx];

                // Create combined row for filter evaluation
                let combined_row = combine_rows(left_row, right_row);

                // Clear CSE cache before evaluation
                evaluator.clear_cse_cache();

                // Evaluate the probe filter
                match evaluator.eval(pf, &combined_row) {
                    Ok(vibesql_types::SqlValue::Boolean(true)) => {
                        found_match = true;
                        break; // Semi-join: we only need one match
                    }
                    Ok(vibesql_types::SqlValue::Boolean(false))
                    | Ok(vibesql_types::SqlValue::Null) => continue,
                    Err(_) => continue, // Filter evaluation error, skip this row
                    Ok(_) => continue,  // Filter didn't return boolean, skip this row
                }
            }

            if found_match {
                result_rows.push(left_row.clone());
            }
        }
    }

    if debug {
        let total_time = start.elapsed();
        eprintln!(
            "[SEMI_JOIN] probe_time={:?}, total_time={:?}, result_rows={}",
            total_time - build_time,
            total_time,
            result_rows.len()
        );
    }

    // Return result with left schema only
    Ok(FromResult::from_rows(left.schema.clone(), result_rows))
}

/// Partition filter predicates into right-only and cross-table predicates
///
/// Right-only predicates reference only columns from the right table and can be
/// applied during the hash table build phase for early filtering.
///
/// Cross-table predicates reference columns from both tables and must be
/// evaluated during the probe phase.
pub(super) fn partition_filter_predicates(
    filter: &vibesql_ast::Expression,
    combined_schema: &CombinedSchema,
    left_schema: &CombinedSchema,
    right_schema: &CombinedSchema,
) -> (Option<vibesql_ast::Expression>, Option<vibesql_ast::Expression>) {
    // Get the set of right-only table names
    let right_table_names: HashSet<String> = right_schema.table_names().into_iter().collect();

    // Get the set of left table names
    let left_table_names: HashSet<String> = left_schema.table_names().into_iter().collect();

    // Flatten the filter into individual conjuncts
    let conjuncts = flatten_conjuncts(filter);

    let mut right_only_predicates = Vec::new();
    let mut cross_table_predicates = Vec::new();

    for conjunct in conjuncts {
        // Extract tables referenced by this predicate
        if let Some(referenced_tables) =
            extract_referenced_tables_branch(&conjunct, combined_schema)
        {
            // Check if predicate references only right tables
            let refs_left = referenced_tables.iter().any(|t| left_table_names.contains(t));
            let refs_right = referenced_tables.iter().any(|t| right_table_names.contains(t));

            if refs_right && !refs_left {
                // Right-only predicate: can be applied during build phase
                right_only_predicates.push(conjunct);
            } else {
                // Cross-table or left-only predicate: apply during probe
                cross_table_predicates.push(conjunct);
            }
        } else {
            // Couldn't determine table references, treat as cross-table
            cross_table_predicates.push(conjunct);
        }
    }

    // Combine predicates back into expressions
    let right_only = combine_with_and(right_only_predicates);
    let cross_table = combine_with_and(cross_table_predicates);

    (right_only, cross_table)
}

#[cfg(test)]
mod tests {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_storage::Row;
    use vibesql_types::{DataType, SqlValue};

    use super::*;
    use crate::schema::CombinedSchema;

    /// Helper to create a simple FromResult for testing
    fn create_test_from_result(
        table_name: &str,
        columns: Vec<(&str, DataType)>,
        rows: Vec<Vec<SqlValue>>,
    ) -> FromResult {
        let schema = TableSchema::new(
            table_name.to_string(),
            columns
                .iter()
                .map(|(name, dtype)| {
                    ColumnSchema::new(
                        name.to_string(),
                        dtype.clone(),
                        true, // nullable
                    )
                })
                .collect(),
        );

        let combined_schema = CombinedSchema::from_table(table_name.to_string(), schema);

        let rows = rows.into_iter().map(Row::new).collect();

        FromResult::from_rows(combined_schema, rows)
    }

    #[test]
    fn test_hash_semi_join_basic() {
        // Left table: users(id, name)
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
                vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))],
                vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))],
            ],
        );

        // Right table: orders(user_id, amount)
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Integer(100)],
                vec![SqlValue::Integer(2), SqlValue::Integer(200)],
                vec![SqlValue::Integer(1), SqlValue::Integer(150)],
            ],
        );

        // Semi-join on users.id = orders.user_id (column 0 from both sides)
        let mut result = hash_semi_join(left, right, 0, 0).unwrap();

        // Should have 2 rows (users 1 and 2 have orders, user 3 does not)
        // Note: user 1 appears only ONCE despite having 2 orders
        assert_eq!(result.rows().len(), 2);

        // Verify result rows only have left table columns (2 columns: id, name)
        for row in result.rows() {
            assert_eq!(row.values.len(), 2);
        }

        // Check that we have users 1 and 2
        let user_ids: Vec<i64> = result
            .rows()
            .iter()
            .map(|r| match &r.values[0] {
                SqlValue::Integer(id) => *id,
                _ => panic!("Expected integer"),
            })
            .collect();
        assert!(user_ids.contains(&1));
        assert!(user_ids.contains(&2));
        assert!(!user_ids.contains(&3)); // Charlie has no orders
    }

    #[test]
    fn test_hash_semi_join_null_values() {
        // Left table with NULL id
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
                vec![SqlValue::Null, SqlValue::Varchar(arcstr::ArcStr::from("Unknown"))],
            ],
        );

        // Right table with NULL user_id
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Integer(100)],
                vec![SqlValue::Null, SqlValue::Integer(200)],
            ],
        );

        let mut result = hash_semi_join(left, right, 0, 0).unwrap();

        // Only Alice should match (id=1)
        // NULL values should not match each other in equi-joins
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values[0], SqlValue::Integer(1));
        assert_eq!(result.rows()[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
    }

    #[test]
    fn test_hash_semi_join_no_matches() {
        // Left table
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer)],
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]],
        );

        // Right table with non-matching ids
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer)],
            vec![vec![SqlValue::Integer(3)], vec![SqlValue::Integer(4)]],
        );

        let mut result = hash_semi_join(left, right, 0, 0).unwrap();

        // No matches
        assert_eq!(result.rows().len(), 0);
    }

    #[test]
    fn test_hash_semi_join_duplicate_right_keys() {
        // Left table
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
                vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))],
            ],
        );

        // Right table with many duplicate user_ids
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Integer(100)],
                vec![SqlValue::Integer(1), SqlValue::Integer(150)],
                vec![SqlValue::Integer(1), SqlValue::Integer(200)],
                vec![SqlValue::Integer(1), SqlValue::Integer(250)],
            ],
        );

        let mut result = hash_semi_join(left, right, 0, 0).unwrap();

        // Should return only 1 row for Alice, despite 4 matching orders
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values[0], SqlValue::Integer(1));
        assert_eq!(result.rows()[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
    }
}
