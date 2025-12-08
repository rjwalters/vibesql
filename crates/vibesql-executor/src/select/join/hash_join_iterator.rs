//! Hash join iterator implementation for lazy evaluation
//!
//! This module implements an iterator-based hash join that provides O(N+M)
//! performance while maintaining lazy evaluation for the left (probe) side.
//!
//! # Bloom Filter Optimization
//!
//! This implementation uses a Bloom filter to quickly reject probe-side rows
//! that cannot possibly match any build-side rows. This optimization is
//! inspired by SQLite's `WHERE_BLOOMFILTER` optimization.
//!
//! Benefits:
//! - Bloom filter check is O(1) with excellent cache behavior
//! - For selective joins, eliminates most hash table lookups
//! - Memory overhead is small (~10 bits per build-side element)

#![allow(clippy::manual_is_multiple_of)]

use std::hash::{Hash, Hasher};

use ahash::{AHashMap, AHasher};

use super::{combine_rows, BloomFilter, FromResult};
use crate::{
    errors::ExecutorError,
    schema::CombinedSchema,
    select::RowIterator,
    timeout::{TimeoutContext, CHECK_INTERVAL},
};

/// Minimum number of build-side rows to enable Bloom filter optimization.
/// For small tables, the overhead of maintaining the Bloom filter may exceed benefits.
const BLOOM_FILTER_MIN_ROWS: usize = 100;

/// Target false positive rate for Bloom filter (1%).
/// Lower rates require more memory but provide better filtering.
const BLOOM_FILTER_FPR: f64 = 0.01;

/// Hash a SqlValue for use in Bloom filter.
/// Uses AHash for fast, high-quality hashing.
#[inline]
fn hash_sql_value(value: &vibesql_types::SqlValue) -> u64 {
    let mut hasher = AHasher::default();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Hash join iterator that lazily produces joined rows
///
/// This implementation uses a hash join algorithm with:
/// - Lazy left (probe) side: rows consumed on-demand from iterator
/// - Materialized right (build) side: all rows hashed into HashMap
/// - Bloom filter for quick rejection of non-matching probe rows
///
/// Algorithm:
/// 1. Build phase: Materialize right side into hash table AND Bloom filter (one-time cost)
/// 2. Probe phase: For each left row:
///    a. Check Bloom filter - if negative, skip immediately (no hash lookup)
///    b. If Bloom filter positive, do actual hash table lookup
///
/// Performance: O(N + M) where N=left rows, M=right rows
/// For selective joins, Bloom filter reduces constant factor significantly.
///
/// Memory: O(M) for right side hash table + O(M * 10 bits) for Bloom filter
pub struct HashJoinIterator<L: RowIterator> {
    /// Lazy probe side (left)
    left: L,
    /// Materialized build side (right) - hash table mapping join key to rows
    right_hash_table: AHashMap<vibesql_types::SqlValue, Vec<vibesql_storage::Row>>,
    /// Bloom filter for quick rejection of non-matching keys (None if disabled)
    bloom_filter: Option<BloomFilter>,
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
    /// Count of rows rejected by Bloom filter (for debugging/profiling)
    #[allow(dead_code)]
    bloom_rejections: usize,
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
        let right_rows = right.into_rows();
        let num_build_rows = right_rows.len();

        let mut hash_table: AHashMap<vibesql_types::SqlValue, Vec<vibesql_storage::Row>> =
            AHashMap::new();

        // Create Bloom filter if we have enough rows to benefit
        // Check environment variable for disabling (useful for A/B testing)
        let bloom_disabled = std::env::var("VIBESQL_DISABLE_BLOOM_FILTER").is_ok();
        let mut bloom_filter = if !bloom_disabled && num_build_rows >= BLOOM_FILTER_MIN_ROWS {
            Some(BloomFilter::new(num_build_rows, BLOOM_FILTER_FPR))
        } else {
            None
        };

        let mut build_iterations = 0;

        for row in right_rows {
            // Check timeout periodically during build phase
            build_iterations += 1;
            if build_iterations % CHECK_INTERVAL == 0 {
                timeout_ctx.check()?;
            }

            let key = row.values[right_col_idx].clone();

            // Skip NULL values - they never match in equi-joins
            if key != vibesql_types::SqlValue::Null {
                // Insert into Bloom filter before hash table
                if let Some(ref mut bf) = bloom_filter {
                    // Hash the SqlValue and insert into Bloom filter
                    let hash = hash_sql_value(&key);
                    bf.insert_hash(hash);
                }

                hash_table.entry(key).or_default().push(row);
            }
        }

        Ok(Self {
            left,
            right_hash_table: hash_table,
            bloom_filter,
            schema: combined_schema,
            left_col_idx,
            right_col_idx,
            current_left_row: None,
            current_matches: Vec::new(),
            match_index: 0,
            right_col_count,
            timeout_ctx,
            iteration_count: 0,
            bloom_rejections: 0,
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

                    // BLOOM FILTER OPTIMIZATION:
                    // Quick check to see if this key MIGHT be in the build side.
                    // If the Bloom filter says "definitely not present", skip the
                    // expensive hash table lookup entirely.
                    if let Some(ref bf) = self.bloom_filter {
                        let hash = hash_sql_value(key);
                        if !bf.might_contain_hash(hash) {
                            // Bloom filter says definitely no match - skip this row
                            self.bloom_rejections += 1;
                            continue;
                        }
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
                        // (This can happen due to Bloom filter false positives)
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
                vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
                vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))],
                vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))],
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
                vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
                vec![SqlValue::Null, SqlValue::Varchar(arcstr::ArcStr::from("Unknown"))],
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
        assert_eq!(results[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Alice"))); // user name
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
                vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("admin"))],
                vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("user"))],
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
