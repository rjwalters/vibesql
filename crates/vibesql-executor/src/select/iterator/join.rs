//! Lazy nested loop join iterator implementation

#![allow(dead_code)]

use std::cell::RefCell;

use crate::{
    errors::ExecutorError, evaluator::CombinedExpressionEvaluator, schema::CombinedSchema,
};

use super::RowIterator;

/// Iterator that performs lazy nested loop join between two row sources
///
/// This iterator streams through the left side while materializing the right side.
/// For each left row, it iterates through all right rows, checking the join condition
/// and yielding matching combinations.
///
/// # Join Types Supported
///
/// - **INNER**: Only matching row pairs
/// - **CROSS**: All combinations (condition ignored)
/// - **LEFT OUTER**: All left rows + NULLs for unmatched
/// - **RIGHT OUTER**: All right rows + NULLs for unmatched (requires full left scan first)
/// - **FULL OUTER**: Combination of LEFT and RIGHT
/// - **SEMI**: Left rows that have at least one match in right (no duplicates)
/// - **ANTI**: Left rows that have NO match in right
///
/// # Current Limitation
///
/// The right side must be fully materialized (Vec<Row>). This is a pragmatic compromise:
/// - Nested loop joins inherently need to scan the right side multiple times
/// - True streaming would require a hash join or other algorithm
/// - This still provides memory benefits for the left side (which can be very large)
///
/// # Example
///
/// ```text
/// let left_iter = TableScanIterator::new(left_schema, left_rows);
/// let right_rows = vec![/* right side rows */];
///
/// let join = LazyNestedLoopJoin::new(
///     left_iter,
///     right_schema,
///     right_rows,
///     vibesql_ast::JoinType::Inner,
///     Some(condition),
///     evaluator,
/// );
///
/// // Only materializes left rows on-demand
/// for row in join.take(10) {
///     println!("{:?}", row?);
/// }
/// ```
pub struct LazyNestedLoopJoin<'schema, I: RowIterator> {
    /// Left side iterator (streaming)
    left: I,
    /// Right side schema (needed for combined schema)
    right_schema: CombinedSchema,
    /// Right side rows (materialized)
    right_rows: Vec<vibesql_storage::Row>,
    /// Type of join
    join_type: vibesql_ast::JoinType,
    /// Optional join condition
    condition: Option<vibesql_ast::Expression>,
    /// Combined schema for output rows
    combined_schema: CombinedSchema,
    /// Database reference (needed for SQL mode in expression evaluation)
    database: Option<&'schema vibesql_storage::Database>,
    /// Evaluator for join conditions (reused across rows to avoid per-row allocation)
    evaluator: RefCell<CombinedExpressionEvaluator<'schema>>,

    // State for iteration
    /// Current left row being processed
    current_left: Option<vibesql_storage::Row>,
    /// Current position in right rows
    right_index: usize,
    /// For LEFT/FULL OUTER: tracks if current left row has any matches
    current_left_matched: bool,
    /// For RIGHT/FULL OUTER: tracks which right rows have been matched
    right_matched: Vec<bool>,
    /// For RIGHT/FULL OUTER: after left exhausted, yield unmatched right rows
    left_exhausted: bool,
    /// For RIGHT/FULL OUTER: current position in emitting unmatched right rows
    unmatched_right_index: usize,
}

impl<'schema, I: RowIterator> LazyNestedLoopJoin<'schema, I> {
    /// Create a new lazy nested loop join iterator
    ///
    /// # Arguments
    /// * `left` - Iterator for left side rows (streaming)
    /// * `right_schema` - Schema for right side
    /// * `right_rows` - Materialized right side rows
    /// * `join_type` - Type of join (INNER, LEFT, RIGHT, FULL, CROSS)
    /// * `condition` - Optional join condition expression
    /// * `database` - Optional database reference (needed for SQL mode in expression evaluation)
    pub fn new(
        left: I,
        right_schema: CombinedSchema,
        right_rows: Vec<vibesql_storage::Row>,
        join_type: vibesql_ast::JoinType,
        condition: Option<vibesql_ast::Expression>,
        database: Option<&'schema vibesql_storage::Database>,
    ) -> Self {
        // Build combined schema (left columns + right columns)
        let left_schema = left.schema().clone();

        // Manually combine two CombinedSchema instances
        let mut table_schemas = left_schema.table_schemas.clone();
        let left_total = left_schema.total_columns;
        let mut right_total = 0;

        // Add all right-side tables with adjusted start indices
        // TableKey already handles case normalization
        for (table_key, (start_idx, schema)) in right_schema.table_schemas.iter() {
            let adjusted_start = left_total + start_idx;
            table_schemas.insert(table_key.clone(), (adjusted_start, schema.clone()));
            right_total += schema.columns.len();
        }

        let combined_schema =
            CombinedSchema { table_schemas, total_columns: left_total + right_total };

        let right_count = right_rows.len();

        // Use Box::leak to create a stable 'static reference for the evaluator
        // This leaks one schema per join iterator, but that's negligible compared to
        // the massive memory savings from avoiding millions of per-row allocations
        let evaluator_schema_ref: &'static CombinedSchema =
            Box::leak(Box::new(combined_schema.clone()));

        // Create evaluator once for reuse across all row evaluations
        // Use with_database if available to ensure correct SQL mode
        let evaluator = if let Some(db) = database {
            // SAFETY: We're transmuting the database reference to 'static lifetime to match
            // the leaked schema. This is safe because:
            // 1. The evaluator is only used within Self's lifetime ('schema)
            // 2. The database reference ('schema) outlives the iterator
            // 3. We never expose this 'static lifetime outside the struct
            let db_static: &'static vibesql_storage::Database = unsafe {
                std::mem::transmute::<
                    &'schema vibesql_storage::Database,
                    &'static vibesql_storage::Database,
                >(db)
            };
            CombinedExpressionEvaluator::with_database(evaluator_schema_ref, db_static)
        } else {
            CombinedExpressionEvaluator::new(evaluator_schema_ref)
        };

        Self {
            left,
            right_schema,
            right_rows,
            join_type,
            condition,
            combined_schema,
            database,
            evaluator: RefCell::new(evaluator),
            current_left: None,
            right_index: 0,
            current_left_matched: false,
            right_matched: vec![false; right_count],
            left_exhausted: false,
            unmatched_right_index: 0,
        }
    }

    /// Combine two rows into a single row (left + right)
    #[inline]
    fn combine_rows(
        left: &vibesql_storage::Row,
        right: &vibesql_storage::Row,
    ) -> vibesql_storage::Row {
        let mut values = Vec::with_capacity(left.values.len() + right.values.len());
        values.extend_from_slice(&left.values);
        values.extend_from_slice(&right.values);
        vibesql_storage::Row::new(values)
    }

    /// Create a row with NULL values for outer join
    #[inline]
    fn null_row(column_count: usize) -> Vec<vibesql_types::SqlValue> {
        vec![vibesql_types::SqlValue::Null; column_count]
    }

    /// Check if join condition is satisfied for a combined row
    fn check_condition(&self, combined_row: &vibesql_storage::Row) -> Result<bool, ExecutorError> {
        match &self.condition {
            None => Ok(true), // No condition means all rows match (CROSS JOIN)
            Some(expr) => {
                // Reuse evaluator from struct (avoids per-row allocations that caused memory leak)
                // Clear CSE cache to prevent stale values from previous row combinations
                let evaluator = self.evaluator.borrow();
                evaluator.clear_cse_cache();

                let value = evaluator.eval(expr, combined_row)?;
                // Use same truthy logic as FilterIterator
                match value {
                    vibesql_types::SqlValue::Boolean(b) => Ok(b),
                    vibesql_types::SqlValue::Null => Ok(false),
                    vibesql_types::SqlValue::Integer(0)
                    | vibesql_types::SqlValue::Smallint(0)
                    | vibesql_types::SqlValue::Bigint(0) => Ok(false),
                    vibesql_types::SqlValue::Integer(_)
                    | vibesql_types::SqlValue::Smallint(_)
                    | vibesql_types::SqlValue::Bigint(_) => Ok(true),
                    vibesql_types::SqlValue::Float(0.0) | vibesql_types::SqlValue::Real(0.0) => {
                        Ok(false)
                    }
                    vibesql_types::SqlValue::Float(_) | vibesql_types::SqlValue::Real(_) => {
                        Ok(true)
                    }
                    vibesql_types::SqlValue::Double(0.0) => Ok(false),
                    vibesql_types::SqlValue::Double(_) => Ok(true),
                    other => Err(ExecutorError::InvalidWhereClause(format!(
                        "Join condition must evaluate to boolean, got: {:?}",
                        other
                    ))),
                }
            }
        }
    }
}

impl<'schema, I: RowIterator> Iterator for LazyNestedLoopJoin<'schema, I> {
    type Item = Result<vibesql_storage::Row, ExecutorError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Handle RIGHT/FULL OUTER: after left exhausted, emit unmatched right rows
        if self.left_exhausted {
            match self.join_type {
                vibesql_ast::JoinType::RightOuter | vibesql_ast::JoinType::FullOuter => {
                    while self.unmatched_right_index < self.right_rows.len() {
                        let idx = self.unmatched_right_index;
                        self.unmatched_right_index += 1;

                        if !self.right_matched[idx] {
                            // Emit: NULL + right_row
                            let left_null_count = self
                                .combined_schema
                                .table_schemas
                                .values()
                                .map(|(_, s)| s.columns.len())
                                .sum::<usize>()
                                - self
                                    .right_schema
                                    .table_schemas
                                    .values()
                                    .map(|(_, s)| s.columns.len())
                                    .sum::<usize>();

                            let mut values = Self::null_row(left_null_count);
                            values.extend_from_slice(&self.right_rows[idx].values);
                            return Some(Ok(vibesql_storage::Row::new(values)));
                        }
                    }
                    return None; // Done with unmatched right rows
                }
                _ => return None, // Other join types done when left exhausted
            }
        }

        loop {
            // If no current left row, fetch the next one
            if self.current_left.is_none() {
                match self.left.next() {
                    Some(Ok(row)) => {
                        self.current_left = Some(row);
                        self.right_index = 0;
                        self.current_left_matched = false;
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => {
                        // Left side exhausted
                        self.left_exhausted = true;

                        // For RIGHT/FULL OUTER, continue to emit unmatched right rows
                        match self.join_type {
                            vibesql_ast::JoinType::RightOuter
                            | vibesql_ast::JoinType::FullOuter => {
                                return self.next(); // Recurse to handle unmatched right rows
                            }
                            _ => return None,
                        }
                    }
                }
            }

            // We have a left row - iterate through right rows
            // Clone to avoid long-lived borrow issues with Semi/Anti joins
            let left_row = self.current_left.as_ref().unwrap().clone();

            while self.right_index < self.right_rows.len() {
                let right_row = &self.right_rows[self.right_index];
                let right_idx = self.right_index;
                self.right_index += 1;

                // Combine rows for condition check
                let combined_row = Self::combine_rows(&left_row, right_row);

                // Check join condition
                match self.check_condition(&combined_row) {
                    Ok(true) => {
                        // Match found!
                        self.current_left_matched = true;
                        self.right_matched[right_idx] = true;

                        // Handle different join types
                        match self.join_type {
                            vibesql_ast::JoinType::Semi => {
                                // Semi-join: return left row only (no duplicates)
                                // Move to next left row immediately
                                self.current_left = None;
                                return Some(Ok(left_row));
                            }
                            vibesql_ast::JoinType::Anti => {
                                // Anti-join: skip this left row (it has a match)
                                self.current_left = None;
                                break; // Move to next left row
                            }
                            _ => {
                                // Regular joins: return combined row
                                return Some(Ok(combined_row));
                            }
                        }
                    }
                    Ok(false) => {
                        // No match, continue to next right row
                        continue;
                    }
                    Err(e) => return Some(Err(e)),
                }
            }

            // Finished all right rows for this left row
            // Handle unmatched left rows based on join type
            if !self.current_left_matched {
                match self.join_type {
                    vibesql_ast::JoinType::LeftOuter | vibesql_ast::JoinType::FullOuter => {
                        let right_null_count = self
                            .right_schema
                            .table_schemas
                            .values()
                            .map(|(_, s)| s.columns.len())
                            .sum::<usize>();

                        let mut values = left_row.values.clone();
                        values.extend(Self::null_row(right_null_count));

                        self.current_left = None; // Move to next left row
                        return Some(Ok(vibesql_storage::Row::new(values)));
                    }
                    vibesql_ast::JoinType::Anti => {
                        // Anti-join: return left row when NO matches found
                        self.current_left = None;
                        return Some(Ok(left_row));
                    }
                    _ => {
                        // INNER/CROSS/RIGHT/SEMI: no match means don't emit
                        self.current_left = None; // Move to next left row
                        continue;
                    }
                }
            }

            // Move to next left row
            self.current_left = None;
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (left_lower, left_upper) = self.left.size_hint();
        let right_count = self.right_rows.len();

        match self.join_type {
            vibesql_ast::JoinType::Inner => {
                // At minimum 0 (no matches), at most left * right
                (0, left_upper.and_then(|l| l.checked_mul(right_count)))
            }
            vibesql_ast::JoinType::Cross => {
                // Exactly left * right
                let lower = left_lower.saturating_mul(right_count);
                let upper = left_upper.and_then(|l| l.checked_mul(right_count));
                (lower, upper)
            }
            vibesql_ast::JoinType::LeftOuter | vibesql_ast::JoinType::FullOuter => {
                // At least as many as left side
                (left_lower, None) // Upper bound is hard to compute
            }
            vibesql_ast::JoinType::RightOuter => {
                // At least as many as right side
                (right_count, None)
            }
            vibesql_ast::JoinType::Semi | vibesql_ast::JoinType::Anti => {
                // At minimum 0 (no matches for anti, all filtered for semi), at most left side count
                // Each left row appears at most once in result
                (0, left_upper)
            }
        }
    }
}

impl<'schema, I: RowIterator> RowIterator for LazyNestedLoopJoin<'schema, I> {
    fn schema(&self) -> &CombinedSchema {
        &self.combined_schema
    }

    fn row_size_hint(&self) -> (usize, Option<usize>) {
        self.size_hint()
    }
}
