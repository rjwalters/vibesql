//! Hash join iterator implementation for lazy evaluation
//!
//! This module implements an iterator-based hash join that provides O(N+M)
//! performance while maintaining lazy evaluation for the left (probe) side.

#![allow(clippy::manual_is_multiple_of)]

use ahash::AHashMap;

use super::{combine_rows, FromResult};
use crate::{
    errors::ExecutorError,
    schema::CombinedSchema,
    select::RowIterator,
    timeout::{TimeoutContext, CHECK_INTERVAL},
};

/// Hash join iterator that lazily produces joined rows
///
/// This implementation uses a hash join algorithm with:
/// - Lazy left (probe) side: rows consumed on-demand from iterator
/// - Materialized right (build) side: all rows hashed into HashMap
///
/// Algorithm:
/// 1. Build phase: Materialize right side into hash table (one-time cost)
/// 2. Probe phase: Stream left rows, hash lookup for matches (O(1) per row)
///
/// Performance: O(N + M) where N=left rows, M=right rows
///
/// Memory: O(M) for right side hash table + O(K) for current matches
pub struct HashJoinIterator<L: RowIterator> {
    /// Lazy probe side (left)
    left: L,
    /// Materialized build side (right) - hash table mapping join key to rows
    right_hash_table: AHashMap<vibesql_types::SqlValue, Vec<vibesql_storage::Row>>,
    /// Combined schema for output rows
    schema: CombinedSchema,
    /// Column index in left table for join key
    left_col_idx: usize,
    /// Column index in right table for join key
    #[allow(dead_code)]
    right_col_idx: usize,
    /// Current left row being processed
    current_left_row: Option<vibesql_storage::Row>,
    /// Matching right rows for current left row
    current_matches: Vec<vibesql_storage::Row>,
    /// Index into current_matches
    match_index: usize,
    /// Number of right columns (for NULL padding)
    #[allow(dead_code)]
    right_col_count: usize,
    /// Timeout context for query timeout enforcement
    timeout_ctx: TimeoutContext,
    /// Iteration counter for periodic timeout checks
    iteration_count: usize,
}

impl<L: RowIterator> HashJoinIterator<L> {
    /// Create a new hash join iterator for INNER JOIN
    ///
    /// # Arguments
    /// * `left` - Lazy iterator for left (probe) side
    /// * `right` - Materialized right (build) side
    /// * `left_col_idx` - Column index in left table for join key
    /// * `right_col_idx` - Column index in right table for join key
    ///
    /// # Returns
    /// * `Ok(HashJoinIterator)` - Successfully created iterator
    /// * `Err(ExecutorError)` - Failed due to memory limits or schema issues
    #[allow(private_interfaces)]
    pub fn new(
        left: L,
        right: FromResult,
        left_col_idx: usize,
        right_col_idx: usize,
    ) -> Result<Self, ExecutorError> {
        // Extract right table schema
        let right_table_name = right
            .schema
            .table_schemas
            .keys()
            .next()
            .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
            .clone();

        let right_schema = right
            .schema
            .table_schemas
            .get(&right_table_name)
            .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
            .1
            .clone();

        let right_col_count = right_schema.columns.len();

        // Combine schemas (left schema from iterator + right schema)
        let combined_schema =
            CombinedSchema::combine(left.schema().clone(), right_table_name, right_schema);

        // Use default timeout context (proper propagation from SelectExecutor is a future improvement)
        let timeout_ctx = TimeoutContext::new_default();

        // Build phase: Create hash table from right side
        // This is the one-time materialization cost
        let mut hash_table: AHashMap<vibesql_types::SqlValue, Vec<vibesql_storage::Row>> =
            AHashMap::new();
        let mut build_iterations = 0;

        for row in right.into_rows() {
            // Check timeout periodically during build phase
            build_iterations += 1;
            if build_iterations % CHECK_INTERVAL == 0 {
                timeout_ctx.check()?;
            }

            let key = row.values[right_col_idx].clone();

            // Skip NULL values - they never match in equi-joins
            if key != vibesql_types::SqlValue::Null {
                hash_table.entry(key).or_default().push(row);
            }
        }

        Ok(Self {
            left,
            right_hash_table: hash_table,
            schema: combined_schema,
            left_col_idx,
            right_col_idx,
            current_left_row: None,
            current_matches: Vec::new(),
            match_index: 0,
            right_col_count,
            timeout_ctx,
            iteration_count: 0,
        })
    }

    /// Get the number of rows in the hash table (right side)
    pub fn hash_table_size(&self) -> usize {
        self.right_hash_table.values().map(|v| v.len()).sum()
    }
}

impl<L: RowIterator> Iterator for HashJoinIterator<L> {
    type Item = Result<vibesql_storage::Row, ExecutorError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Check timeout periodically during probe phase
            self.iteration_count += 1;
            if self.iteration_count % CHECK_INTERVAL == 0 {
                if let Err(e) = self.timeout_ctx.check() {
                    return Some(Err(e));
                }
            }

            // If we have remaining matches for current left row, return next match
            if self.match_index < self.current_matches.len() {
                let right_row = &self.current_matches[self.match_index];
                self.match_index += 1;

                // Combine left and right rows
                if let Some(ref left_row) = self.current_left_row {
                    let combined_row = combine_rows(left_row, right_row);
                    return Some(Ok(combined_row));
                }
            }

            // No more matches for current left row, get next left row
            match self.left.next() {
                Some(Ok(left_row)) => {
                    let key = &left_row.values[self.left_col_idx];

                    // Skip NULL values - they never match in equi-joins
                    if key == &vibesql_types::SqlValue::Null {
                        // For INNER JOIN, skip rows with NULL join keys
                        continue;
                    }

                    // Lookup matches in hash table
                    if let Some(matches) = self.right_hash_table.get(key) {
                        // Found matches - set up for iteration
                        self.current_left_row = Some(left_row);
                        self.current_matches = matches.clone();
                        self.match_index = 0;
                        // Continue loop to return first match
                    } else {
                        // No matches for this left row
                        // For INNER JOIN, skip this row
                        continue;
                    }
                }
                Some(Err(e)) => {
                    // Propagate error from left iterator
                    return Some(Err(e));
                }
                None => {
                    // Left iterator exhausted, we're done
                    return None;
                }
            }
        }
    }
}

impl<L: RowIterator> RowIterator for HashJoinIterator<L> {
    fn schema(&self) -> &CombinedSchema {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::select::TableScanIterator;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_storage::Row;
    use vibesql_types::{DataType, SqlValue};

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
    fn test_hash_join_iterator_simple() {
        // Left table: users(id, name)
        let left_result = create_test_from_result(
            "users",
            vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
                vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())],
                vec![SqlValue::Integer(3), SqlValue::Varchar("Charlie".to_string())],
            ],
        );

        let left_iter = TableScanIterator::new(left_result.schema.clone(), left_result.into_rows());

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

        // Join on users.id = orders.user_id (column 0 from both sides)
        let join_iter = HashJoinIterator::new(left_iter, right, 0, 0).unwrap();

        // Collect results
        let results: Result<Vec<_>, _> = join_iter.collect();
        let results = results.unwrap();

        // Should have 3 rows (user 1 has 2 orders, user 2 has 1 order, user 3 has no orders)
        assert_eq!(results.len(), 3);

        // Verify combined rows have correct structure (4 columns: id, name, user_id, amount)
        for row in &results {
            assert_eq!(row.values.len(), 4);
        }

        // Check specific matches
        // Alice (id=1) should appear twice (2 orders)
        let alice_orders: Vec<_> =
            results.iter().filter(|r| r.values[0] == SqlValue::Integer(1)).collect();
        assert_eq!(alice_orders.len(), 2);

        // Bob (id=2) should appear once (1 order)
        let bob_orders: Vec<_> =
            results.iter().filter(|r| r.values[0] == SqlValue::Integer(2)).collect();
        assert_eq!(bob_orders.len(), 1);

        // Charlie (id=3) should not appear (no orders)
        let charlie_orders: Vec<_> =
            results.iter().filter(|r| r.values[0] == SqlValue::Integer(3)).collect();
        assert_eq!(charlie_orders.len(), 0);
    }

    #[test]
    fn test_hash_join_iterator_null_values() {
        // Left table with NULL id
        let left_result = create_test_from_result(
            "users",
            vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
                vec![SqlValue::Null, SqlValue::Varchar("Unknown".to_string())],
            ],
        );

        let left_iter = TableScanIterator::new(left_result.schema.clone(), left_result.into_rows());

        // Right table with NULL user_id
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Integer(100)],
                vec![SqlValue::Null, SqlValue::Integer(200)],
            ],
        );

        let join_iter = HashJoinIterator::new(left_iter, right, 0, 0).unwrap();

        let results: Result<Vec<_>, _> = join_iter.collect();
        let results = results.unwrap();

        // Only one match: Alice (id=1) with order (user_id=1)
        // NULLs should not match each other in equi-joins
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values[0], SqlValue::Integer(1)); // user id
        assert_eq!(results[0].values[1], SqlValue::Varchar("Alice".to_string())); // user name
        assert_eq!(results[0].values[2], SqlValue::Integer(1)); // order user_id
        assert_eq!(results[0].values[3], SqlValue::Integer(100)); // order amount
    }

    #[test]
    fn test_hash_join_iterator_no_matches() {
        // Left table
        let left_result = create_test_from_result(
            "users",
            vec![("id", DataType::Integer)],
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]],
        );

        let left_iter = TableScanIterator::new(left_result.schema.clone(), left_result.into_rows());

        // Right table with non-matching ids
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer)],
            vec![vec![SqlValue::Integer(3)], vec![SqlValue::Integer(4)]],
        );

        let join_iter = HashJoinIterator::new(left_iter, right, 0, 0).unwrap();

        let results: Result<Vec<_>, _> = join_iter.collect();
        let results = results.unwrap();

        // No matches
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_hash_join_iterator_empty_tables() {
        // Left table (empty)
        let left_result = create_test_from_result("users", vec![("id", DataType::Integer)], vec![]);

        let left_iter = TableScanIterator::new(left_result.schema.clone(), left_result.into_rows());

        // Right table (empty)
        let right = create_test_from_result("orders", vec![("user_id", DataType::Integer)], vec![]);

        let join_iter = HashJoinIterator::new(left_iter, right, 0, 0).unwrap();

        let results: Result<Vec<_>, _> = join_iter.collect();
        let results = results.unwrap();

        // No rows
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_hash_join_iterator_duplicate_keys() {
        // Left table with duplicate ids
        let left_result = create_test_from_result(
            "users",
            vec![("id", DataType::Integer), ("type", DataType::Varchar { max_length: Some(10) })],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Varchar("admin".to_string())],
                vec![SqlValue::Integer(1), SqlValue::Varchar("user".to_string())],
            ],
        );

        let left_iter = TableScanIterator::new(left_result.schema.clone(), left_result.into_rows());

        // Right table with duplicate user_ids
        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1), SqlValue::Integer(100)],
                vec![SqlValue::Integer(1), SqlValue::Integer(200)],
            ],
        );

        let join_iter = HashJoinIterator::new(left_iter, right, 0, 0).unwrap();

        let results: Result<Vec<_>, _> = join_iter.collect();
        let results = results.unwrap();

        // Cartesian product of matching keys: 2 left rows * 2 right rows = 4 results
        assert_eq!(results.len(), 4);

        // All should have id=1
        for row in &results {
            assert_eq!(row.values[0], SqlValue::Integer(1));
        }
    }

    #[test]
    fn test_hash_join_iterator_lazy_evaluation() {
        // This test verifies that the left side is truly lazy
        // We'll create an iterator that tracks how many rows have been consumed

        struct CountingIterator {
            schema: CombinedSchema,
            rows: Vec<Row>,
            index: usize,
            consumed_count: std::sync::Arc<std::sync::Mutex<usize>>,
        }

        impl Iterator for CountingIterator {
            type Item = Result<Row, ExecutorError>;

            fn next(&mut self) -> Option<Self::Item> {
                if self.index < self.rows.len() {
                    let row = self.rows[self.index].clone();
                    self.index += 1;
                    *self.consumed_count.lock().unwrap() += 1;
                    Some(Ok(row))
                } else {
                    None
                }
            }
        }

        impl RowIterator for CountingIterator {
            fn schema(&self) -> &CombinedSchema {
                &self.schema
            }
        }

        let consumed = std::sync::Arc::new(std::sync::Mutex::new(0));

        let left_result = create_test_from_result(
            "users",
            vec![("id", DataType::Integer)],
            vec![
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Integer(2)],
                vec![SqlValue::Integer(3)],
                vec![SqlValue::Integer(4)],
                vec![SqlValue::Integer(5)],
            ],
        );

        let counting_iter = CountingIterator {
            schema: left_result.schema.clone(),
            rows: left_result.into_rows(),
            index: 0,
            consumed_count: consumed.clone(),
        };

        let right = create_test_from_result(
            "orders",
            vec![("user_id", DataType::Integer)],
            vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]],
        );

        let join_iter = HashJoinIterator::new(counting_iter, right, 0, 0).unwrap();

        // Take only first 2 results
        let results: Vec<_> = join_iter.take(2).collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 2);

        // Verify that we didn't consume all left rows (lazy evaluation)
        // We should have consumed at most 2 rows (matching ids 1 and 2)
        let consumed_count = *consumed.lock().unwrap();
        assert!(consumed_count <= 3, "Expected at most 3 rows consumed, got {}", consumed_count);
    }
}
