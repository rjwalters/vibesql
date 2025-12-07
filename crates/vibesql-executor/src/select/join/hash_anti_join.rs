#![allow(clippy::doc_lazy_continuation)]

use ahash::AHashMap;

use super::hash_join::build_existence_hash_table_parallel;
use super::hash_semi_join::partition_filter_predicates;
use super::{combine_rows, FromResult};
use crate::errors::ExecutorError;
use crate::evaluator::CombinedExpressionEvaluator;
use crate::schema::CombinedSchema;
use crate::timeout::{TimeoutContext, CHECK_INTERVAL};

/// Hash anti-join implementation
///
/// Anti-join returns rows from the LEFT table that have NO match in the RIGHT table.
/// This is the opposite of semi-join.
///
/// Use cases:
/// - NOT EXISTS subqueries: SELECT * FROM orders WHERE NOT EXISTS (SELECT 1 FROM lineitem WHERE l_orderkey = o_orderkey)
/// - NOT IN subqueries: SELECT * FROM orders WHERE o_orderkey NOT IN (SELECT l_orderkey FROM lineitem)
///
/// Algorithm:
/// 1. Build phase: Hash the RIGHT table into a HashSet (O(n))
/// 2. Probe phase: For each row in LEFT table, check if key exists in hash set (O(m))
/// 3. If key does NOT exist, emit the LEFT row
/// Total: O(n + m) vs O(n*m) for nested loop
///
/// Performance characteristics:
/// - Time: O(n + m) vs O(n*m) for nested loop
/// - Space: O(n) where n is the size of the right table (smaller than inner join because we don't store indices)
/// - Expected speedup: 100-10,000x for large anti-joins
pub(super) fn hash_anti_join(
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

    // Probe phase: Check each left row for absence of a match
    // We only emit left rows that have NO match in the right table
    let estimated_capacity = left_slice.len().min(100_000);
    let mut result_rows = Vec::with_capacity(estimated_capacity);

    for (idx, left_row) in left_slice.iter().enumerate() {
        // Check timeout periodically during probe phase
        if idx % CHECK_INTERVAL == 0 {
            timeout_ctx.check()?;
        }

        let key = &left_row.values[left_col_idx];

        // Skip NULL values - they never match in equi-joins, so they should be returned
        // for anti-join (since they have "no match")
        if key == &vibesql_types::SqlValue::Null {
            result_rows.push(left_row.clone());
            continue;
        }

        // If key does NOT exist in hash table, emit this left row
        if !hash_table.contains_key(key) {
            result_rows.push(left_row.clone());
        }
    }

    // Return result with left schema only (we don't combine with right schema)
    Ok(FromResult::from_rows(left.schema.clone(), result_rows))
}

/// Hash anti-join with additional filter conditions
///
/// This is an optimized version of hash_anti_join that supports additional filter predicates
/// beyond the equi-join condition. This is essential for NOT EXISTS subqueries with complex WHERE clauses.
///
/// Example use case:
/// ```sql
/// NOT EXISTS (
///     SELECT * FROM lineitem l2
///     WHERE l2.l_orderkey = l1.l_orderkey    -- Equi-join (used for hash table)
///       AND l2.l_suppkey <> l1.l_suppkey     -- Additional filter (checked during probe)
/// )
/// ```
///
/// ## Optimization: Right-Side Predicate Pushdown
///
/// When the filter contains predicates that reference only the right table (e.g., `s_quantity < 10`),
/// these predicates are applied during the hash table build phase, reducing the hash table size.
/// This is critical for NOT IN subquery performance.
///
/// Algorithm:
/// 1. **Predicate partitioning**: Split filter into right-only and cross-table predicates
/// 2. **Build phase**: Hash the RIGHT table, applying right-only predicates as a pre-filter
/// 3. **Probe phase**: For each LEFT row:
///    a. Check if hash table contains matching key
///    b. If yes, verify cross-table predicates against ALL matching right rows
///    c. If NO right row passes, emit the left row
///    d. If no matching key, emit the left row
///
/// Performance: O(n + m) average case with reduced hash table size
pub(super) fn hash_anti_join_with_filter(
    left: FromResult,
    right: FromResult,
    left_col_idx: usize,
    right_col_idx: usize,
    additional_filter: Option<&vibesql_ast::Expression>,
    combined_schema: &CombinedSchema,
    database: &vibesql_storage::Database,
) -> Result<FromResult, ExecutorError> {
    // If no additional filter, use the simpler version
    if additional_filter.is_none() {
        return hash_anti_join(left, right, left_col_idx, right_col_idx);
    }

    // Use default timeout context (proper propagation from SelectExecutor is a future improvement)
    let timeout_ctx = TimeoutContext::new_default();

    let filter = additional_filter.unwrap();

    // Use as_slice() for zero-cost access without triggering row materialization
    let left_slice = left.as_slice();
    let right_slice = right.as_slice();

    // Partition the filter into right-only and cross-table predicates
    let (right_only_filter, probe_filter) =
        partition_filter_predicates(filter, combined_schema, &left.schema, &right.schema);

    // Create evaluators for build and probe phases
    let right_only_evaluator = right_only_filter
        .as_ref()
        .map(|_| CombinedExpressionEvaluator::with_database(&right.schema, database));

    let probe_evaluator = probe_filter
        .as_ref()
        .map(|_| CombinedExpressionEvaluator::with_database(combined_schema, database));

    // Build phase: Create hash table from right side with right-only predicate filtering
    let mut hash_table: AHashMap<vibesql_types::SqlValue, Vec<usize>> = AHashMap::new();
    let mut filtered_count = 0usize;

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
                    filtered_count += 1;
                    continue;
                }
                Err(_) | Ok(_) => {
                    // Filter evaluation error or non-boolean, skip this row
                    filtered_count += 1;
                    continue;
                }
            }
        } else {
            // No right-only filter, add all rows
            hash_table.entry(key).or_default().push(idx);
        }
    }

    // Log build-time filtering if significant
    if std::env::var("SEMI_JOIN_DEBUG").is_ok() && filtered_count > 0 {
        eprintln!(
            "[ANTI_JOIN] Build-time filtering: {} rows filtered out of {} ({:.1}%)",
            filtered_count,
            right_slice.len(),
            (filtered_count as f64 / right_slice.len() as f64) * 100.0
        );
    }

    // Probe phase: Check each left row for absence of a match that passes the filter
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

        // Skip NULL values - they never match in equi-joins, so they should be returned
        // for anti-join (since they have "no match")
        if key == &vibesql_types::SqlValue::Null {
            result_rows.push(left_row.clone());
            continue;
        }

        // Check if key exists in hash table
        if let Some(right_indices) = hash_table.get(key) {
            // If no probe filter, any match means we skip this left row
            if probe_filter.is_none() {
                if right_indices.is_empty() {
                    result_rows.push(left_row.clone());
                }
                // Otherwise, there's a match, so don't emit this left row
                continue;
            }

            // Check if ANY matching right row passes the probe filter
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
                        break; // Anti-join: if we find one match, skip this left row
                    }
                    Ok(vibesql_types::SqlValue::Boolean(false))
                    | Ok(vibesql_types::SqlValue::Null) => continue,
                    Err(_) => continue, // Filter evaluation error, try next row
                    Ok(_) => continue,  // Filter didn't return boolean, try next row
                }
            }

            // If NO right row passed the filter, emit this left row
            if !found_match {
                result_rows.push(left_row.clone());
            }
        } else {
            // No matching key in hash table, so emit this left row
            result_rows.push(left_row.clone());
        }
    }

    // Return result with left schema only
    Ok(FromResult::from_rows(left.schema.clone(), result_rows))
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
    fn test_hash_anti_join_basic() {
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

        // Anti-join on users.id = orders.user_id (column 0 from both sides)
        let mut result = hash_anti_join(left, right, 0, 0).unwrap();

        // Should have 1 row (user 3/Charlie has no orders)
        assert_eq!(result.rows().len(), 1);

        // Verify result rows only have left table columns (2 columns: id, name)
        for row in result.rows() {
            assert_eq!(row.values.len(), 2);
        }

        // Check that we only have user 3 (Charlie)
        assert_eq!(result.rows()[0].values[0], SqlValue::Integer(3));
        assert_eq!(result.rows()[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Charlie")));
    }

    #[test]
    fn test_hash_anti_join_null_values() {
        // Left table with NULL id
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
                vec![SqlValue::Null, SqlValue::Varchar(arcstr::ArcStr::from("Unknown"))],
            ],
        );

        // Right table
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
            vec![vec![SqlValue::Integer(1), SqlValue::Integer(100)]],
        );

        let mut result = hash_anti_join(left, right, 0, 0).unwrap();

        // Only "Unknown" should be returned (NULL has no match, since NULLs don't match anything)
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values[0], SqlValue::Null);
        assert_eq!(result.rows()[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Unknown")));
    }

    #[test]
    fn test_hash_anti_join_all_match() {
        // Left table
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer)],
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]],
        );

        // Right table (all left keys have matches)
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer)],
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]],
        );

        let mut result = hash_anti_join(left, right, 0, 0).unwrap();

        // No rows should be returned (all left rows have matches)
        assert_eq!(result.rows().len(), 0);
    }

    #[test]
    fn test_hash_anti_join_no_matches() {
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

        let mut result = hash_anti_join(left, right, 0, 0).unwrap();

        // All left rows should be returned (no matches)
        assert_eq!(result.rows().len(), 2);
    }

    #[test]
    fn test_hash_anti_join_empty_right_table() {
        // Left table (non-empty)
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
                vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))],
            ],
        );

        // Right table (empty)
        let right = create_test_from_result("orders", vec![("user_id", DataType::Integer)], vec![]);

        let mut result = hash_anti_join(left, right, 0, 0).unwrap();

        // All left rows should be returned (no right rows means no matches)
        assert_eq!(result.rows().len(), 2);
    }

    #[test]
    fn test_hash_anti_join_duplicate_right_keys() {
        // Left table
        let left = create_test_from_result(
            "users",
            vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
                vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))],
                vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))],
            ],
        );

        // Right table with many duplicate user_ids for user 1
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

        let mut result = hash_anti_join(left, right, 0, 0).unwrap();

        // Should return Bob and Charlie (users 2 and 3), since Alice (user 1) has orders
        assert_eq!(result.rows().len(), 2);
        let user_ids: Vec<i64> = result
            .rows()
            .iter()
            .map(|r| match &r.values[0] {
                SqlValue::Integer(id) => *id,
                _ => panic!("Expected integer"),
            })
            .collect();
        assert!(user_ids.contains(&2));
        assert!(user_ids.contains(&3));
        assert!(!user_ids.contains(&1));
    }
}
