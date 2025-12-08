use std::collections::HashMap;
use std::sync::Arc;

use super::{cte::CteResult, from_iterator::FromIterator};
use crate::{
    errors::ExecutorError, evaluator::CombinedExpressionEvaluator, optimizer::combine_with_and,
    schema::CombinedSchema, timeout::TimeoutContext,
};

mod bloom_filter;
mod expression_mapper;
mod hash_anti_join;
pub(crate) mod hash_join;
mod hash_join_iterator;
mod hash_semi_join;
mod join_analyzer;
mod nested_loop;
pub mod reorder;
pub mod search;

// Re-export Bloom filter for use in hash join implementations
pub(crate) use bloom_filter::BloomFilter;

#[cfg(test)]
mod tests;

// Re-export join reorder analyzer for public tests
// Re-export hash_join functions for internal use
use hash_anti_join::{hash_anti_join, hash_anti_join_with_filter};
use hash_join::{
    hash_join_inner, hash_join_inner_arithmetic, hash_join_inner_multi, hash_join_left_outer,
    hash_join_left_outer_multi,
};
use hash_semi_join::{hash_semi_join, hash_semi_join_with_filter};
// Re-export hash join iterator for public use
pub use hash_join_iterator::HashJoinIterator;
// Re-export nested loop join variants for internal use
use nested_loop::{
    nested_loop_anti_join, nested_loop_cross_join, nested_loop_full_outer_join,
    nested_loop_inner_join, nested_loop_left_outer_join, nested_loop_right_outer_join,
    nested_loop_semi_join,
};
pub use reorder::JoinOrderAnalyzer;
// Re-export join order search for public tests
pub use search::JoinOrderSearch;

/// Iterator over `FromData` rows without forcing full materialization
///
/// This enum wraps either a Vec iterator or a lazy `FromIterator`, allowing
/// uniform iteration over rows regardless of how they were stored.
///
/// # Issue #4060
///
/// This type enables deferred row materialization for LIMIT queries:
/// - `SELECT * FROM t LIMIT 10` only clones 10 rows, not all of `t`
/// - Memory usage is O(LIMIT) instead of O(table_size)
#[allow(dead_code)]
pub(super) enum FromDataIterator {
    /// Iterator over a materialized Vec<Row>
    Vec(std::vec::IntoIter<vibesql_storage::Row>),
    /// Lazy iterator from FromIterator (table scan)
    Lazy(FromIterator),
}

impl Iterator for FromDataIterator {
    type Item = vibesql_storage::Row;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Vec(iter) => iter.next(),
            Self::Lazy(iter) => iter.next(),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Vec(iter) => iter.size_hint(),
            Self::Lazy(iter) => iter.size_hint(),
        }
    }
}

/// Data source for FROM clause results
///
/// This enum allows FROM results to be either materialized (Vec<Row>) or lazy (iterator).
/// Materialized results are used for JOINs, CTEs, and operations that need multiple passes.
/// Lazy results are used for simple table scans to enable streaming execution.
pub(super) enum FromData {
    /// Materialized rows (for JOINs, CTEs, operations needing multiple passes)
    Materialized(Vec<vibesql_storage::Row>),

    /// Shared rows (for zero-copy CTE references without filtering)
    ///
    /// This variant enables O(1) cloning when CTEs are referenced multiple times
    /// without any filtering. Only materializes to Vec when mutation is needed.
    SharedRows(Arc<Vec<vibesql_storage::Row>>),

    /// Lazy iterator (for streaming table scans)
    Iterator(FromIterator),
}

impl FromData {
    /// Get rows, materializing if needed
    ///
    /// For SharedRows, this will clone only if the Arc is shared.
    /// If the Arc has a single reference, the Vec is moved out efficiently.
    pub fn into_rows(self) -> Vec<vibesql_storage::Row> {
        match self {
            Self::Materialized(rows) => rows,
            Self::SharedRows(arc) => Arc::try_unwrap(arc).unwrap_or_else(|arc| (*arc).clone()),
            Self::Iterator(iter) => iter.collect_vec(),
        }
    }

    /// Returns an iterator over rows without forcing full materialization
    ///
    /// This is more efficient than `into_rows()` when you don't need all rows,
    /// particularly for LIMIT queries where only a subset will be consumed.
    ///
    /// For Materialized and SharedRows variants, this returns an iterator over
    /// the owned/cloned Vec. For Iterator variant, it returns the lazy iterator
    /// directly without collecting.
    ///
    /// # Performance
    ///
    /// - `into_rows()`: O(n) allocation + cloning for all rows
    /// - `into_iter()`: O(k) where k is the number of rows actually consumed
    ///
    /// Use `into_iter()` when:
    /// - Processing with LIMIT (only need first N rows)
    /// - Filtering results (may discard many rows)
    /// - Streaming output without full materialization
    #[allow(dead_code)]
    pub fn into_iter(self) -> FromDataIterator {
        match self {
            Self::Materialized(rows) => FromDataIterator::Vec(rows.into_iter()),
            Self::SharedRows(arc) => {
                // Try to unwrap the Arc; if shared, clone the Vec
                let rows = Arc::try_unwrap(arc).unwrap_or_else(|arc| (*arc).clone());
                FromDataIterator::Vec(rows.into_iter())
            }
            Self::Iterator(iter) => FromDataIterator::Lazy(iter),
        }
    }

    /// Get a reference to materialized rows, or materialize if iterator
    ///
    /// For SharedRows, returns a reference to the shared data without cloning.
    pub fn as_rows(&mut self) -> &Vec<vibesql_storage::Row> {
        // If we have an iterator, materialize it
        if let Self::Iterator(iter) = self {
            #[cfg(feature = "profile-q6")]
            let materialize_start = std::time::Instant::now();

            let rows = std::mem::replace(iter, FromIterator::from_vec(vec![])).collect_vec();
            *self = Self::Materialized(rows);

            #[cfg(feature = "profile-q6")]
            {
                let materialize_time = materialize_start.elapsed();
                if let Self::Materialized(rows) = self {
                    eprintln!(
                        "[Q6 PROFILE] Row materialization (collect_vec): {:?} ({} rows, {:?}/row)",
                        materialize_time,
                        rows.len(),
                        materialize_time / rows.len() as u32
                    );
                }
            }
        }

        // Now we're guaranteed to have materialized or shared rows
        match self {
            Self::Materialized(ref rows) => rows,
            Self::SharedRows(ref arc) => arc.as_ref(),
            Self::Iterator(_) => unreachable!(),
        }
    }

    /// Get a slice reference to the underlying rows without triggering materialization
    ///
    /// This is a zero-cost operation that directly accesses the underlying Vec<Row>
    /// without calling collect_vec(). This avoids the 137ms row materialization
    /// bottleneck in Q6 by skipping iteration entirely.
    ///
    /// Critical for columnar execution performance (#2521).
    pub fn as_slice(&self) -> &[vibesql_storage::Row] {
        match self {
            Self::Materialized(rows) => rows.as_slice(),
            Self::SharedRows(arc) => arc.as_slice(),
            Self::Iterator(iter) => iter.as_slice(),
        }
    }
}

/// Result of executing a FROM clause
///
/// Contains the combined schema and data (either materialized or lazy).
pub(super) struct FromResult {
    pub(super) schema: CombinedSchema,
    pub(super) data: FromData,
    /// If present, indicates that results are already sorted by the specified columns
    /// in the given order (ASC/DESC). This allows skipping ORDER BY sorting.
    pub(super) sorted_by: Option<Vec<(String, vibesql_ast::OrderDirection)>>,
    /// If true, indicates that WHERE clause filtering has already been fully applied
    /// during the scan (e.g., by index scan with predicate pushdown). This allows
    /// skipping redundant WHERE clause evaluation in the executor.
    pub(super) where_filtered: bool,
}

impl FromResult {
    /// Create a FromResult from materialized rows
    pub(super) fn from_rows(schema: CombinedSchema, rows: Vec<vibesql_storage::Row>) -> Self {
        Self { schema, data: FromData::Materialized(rows), sorted_by: None, where_filtered: false }
    }

    /// Create a FromResult from shared rows (zero-copy for CTEs)
    ///
    /// This variant is used when CTE rows can be shared without cloning,
    /// enabling O(1) memory usage for CTE references without filtering.
    pub(super) fn from_shared_rows(
        schema: CombinedSchema,
        rows: Arc<Vec<vibesql_storage::Row>>,
    ) -> Self {
        Self { schema, data: FromData::SharedRows(rows), sorted_by: None, where_filtered: false }
    }

    /// Create a FromResult from materialized rows with sorting metadata
    pub(super) fn from_rows_sorted(
        schema: CombinedSchema,
        rows: Vec<vibesql_storage::Row>,
        sorted_by: Vec<(String, vibesql_ast::OrderDirection)>,
    ) -> Self {
        Self {
            schema,
            data: FromData::Materialized(rows),
            sorted_by: Some(sorted_by),
            where_filtered: false,
        }
    }

    /// Create a FromResult from materialized rows with WHERE filtering already applied
    pub(super) fn from_rows_where_filtered(
        schema: CombinedSchema,
        rows: Vec<vibesql_storage::Row>,
        sorted_by: Option<Vec<(String, vibesql_ast::OrderDirection)>>,
    ) -> Self {
        Self { schema, data: FromData::Materialized(rows), sorted_by, where_filtered: true }
    }

    /// Create a FromResult from an iterator
    pub(super) fn from_iterator(schema: CombinedSchema, iterator: FromIterator) -> Self {
        Self { schema, data: FromData::Iterator(iterator), sorted_by: None, where_filtered: false }
    }

    /// Get the rows, materializing if needed
    pub(super) fn into_rows(self) -> Vec<vibesql_storage::Row> {
        self.data.into_rows()
    }

    /// Returns an iterator over rows without forcing full materialization
    ///
    /// This delegates to `FromData::into_iter()` and is more efficient than
    /// `into_rows()` when only a subset of rows will be consumed.
    ///
    /// # Example Use Cases
    ///
    /// - LIMIT queries: `result.into_iter().take(10).collect()`
    /// - Filtered iteration: `result.into_iter().filter(|r| ...).collect()`
    /// - Early termination: Stop iterating when a condition is met
    ///
    /// # Issue #4060
    #[allow(dead_code)]
    pub(super) fn into_iter(self) -> FromDataIterator {
        self.data.into_iter()
    }

    /// Take up to N rows without full materialization
    ///
    /// This is a convenience method equivalent to `self.into_iter().take(n).collect()`.
    /// It's optimized for LIMIT queries where only a small subset of rows is needed.
    ///
    /// # Performance
    ///
    /// For a table with 10,000 rows and `take(10)`:
    /// - `into_rows()` + truncate: clones all 10,000 rows, then discards 9,990
    /// - `take(10)`: clones only 10 rows
    ///
    /// # Issue #4060
    #[allow(dead_code)]
    pub(super) fn take(self, n: usize) -> Vec<vibesql_storage::Row> {
        self.into_iter().take(n).collect()
    }

    /// Get a mutable reference to the rows, materializing if needed
    ///
    /// For SharedRows, this triggers copy-on-write: the shared data is cloned
    /// into owned Materialized data to allow mutation.
    #[allow(dead_code)]
    pub(super) fn rows_mut(&mut self) -> &mut Vec<vibesql_storage::Row> {
        // Convert iterator or shared to materialized
        match &mut self.data {
            FromData::Iterator(iter) => {
                let rows = std::mem::replace(iter, FromIterator::from_vec(vec![])).collect_vec();
                self.data = FromData::Materialized(rows);
            }
            FromData::SharedRows(arc) => {
                // Copy-on-write: clone the shared data to allow mutation
                let rows = arc.as_ref().clone();
                self.data = FromData::Materialized(rows);
            }
            FromData::Materialized(_) => {}
        }

        // Now we're guaranteed to have materialized rows
        match &mut self.data {
            FromData::Materialized(rows) => rows,
            FromData::SharedRows(_) | FromData::Iterator(_) => unreachable!(),
        }
    }

    /// Get a reference to rows, materializing if needed
    pub(super) fn rows(&mut self) -> &Vec<vibesql_storage::Row> {
        self.data.as_rows()
    }

    /// Get a slice reference to rows without triggering materialization
    ///
    /// This is a zero-cost operation that accesses the underlying data directly
    /// without calling collect_vec(). This is critical for performance as it
    /// avoids the row materialization bottleneck (up to 57% of query time).
    ///
    /// Unlike `rows()` which may trigger iterator collection, this method
    /// provides direct access to the underlying Vec<Row> or iterator buffer.
    pub(super) fn as_slice(&self) -> &[vibesql_storage::Row] {
        self.data.as_slice()
    }
}

/// Helper function to combine two rows without unnecessary cloning
/// Only creates a single combined row, avoiding intermediate clones
#[inline]
fn combine_rows(
    left_row: &vibesql_storage::Row,
    right_row: &vibesql_storage::Row,
) -> vibesql_storage::Row {
    let mut combined_values = Vec::with_capacity(left_row.values.len() + right_row.values.len());
    combined_values.extend_from_slice(&left_row.values);
    combined_values.extend_from_slice(&right_row.values);
    vibesql_storage::Row::new(combined_values)
}

/// Apply a post-join filter expression to join result rows
///
/// This is used to filter rows produced by hash join with additional conditions
/// from the WHERE clause that weren't used in the hash join itself.
///
/// Issue #3562: Added cte_results parameter so IN subqueries in filter expressions
/// can resolve CTE references.
fn apply_post_join_filter(
    result: FromResult,
    filter_expr: &vibesql_ast::Expression,
    database: &vibesql_storage::Database,
    cte_results: &HashMap<String, CteResult>,
) -> Result<FromResult, ExecutorError> {
    // Extract schema before moving result
    let schema = result.schema.clone();
    // Issue #3562: Use evaluator with CTE context if CTEs exist
    let evaluator = if cte_results.is_empty() {
        CombinedExpressionEvaluator::with_database(&schema, database)
    } else {
        CombinedExpressionEvaluator::with_database_and_cte(&schema, database, cte_results)
    };

    // Filter rows based on the expression
    let mut filtered_rows = Vec::new();
    for row in result.into_rows() {
        match evaluator.eval(filter_expr, &row)? {
            vibesql_types::SqlValue::Boolean(true) => filtered_rows.push(row),
            vibesql_types::SqlValue::Boolean(false) => {} // Skip this row
            vibesql_types::SqlValue::Null => {}           // Skip NULL results
            // SQLLogicTest compatibility: treat integers as truthy/falsy
            vibesql_types::SqlValue::Integer(0) => {} // Skip 0
            vibesql_types::SqlValue::Integer(_) => filtered_rows.push(row),
            vibesql_types::SqlValue::Smallint(0) => {} // Skip 0
            vibesql_types::SqlValue::Smallint(_) => filtered_rows.push(row),
            vibesql_types::SqlValue::Bigint(0) => {} // Skip 0
            vibesql_types::SqlValue::Bigint(_) => filtered_rows.push(row),
            vibesql_types::SqlValue::Float(0.0) => {} // Skip 0.0
            vibesql_types::SqlValue::Float(_) => filtered_rows.push(row),
            vibesql_types::SqlValue::Real(0.0) => {} // Skip 0.0
            vibesql_types::SqlValue::Real(_) => filtered_rows.push(row),
            vibesql_types::SqlValue::Double(0.0) => {} // Skip 0.0
            vibesql_types::SqlValue::Double(_) => filtered_rows.push(row),
            other => {
                return Err(ExecutorError::InvalidWhereClause(format!(
                    "Filter expression must evaluate to boolean, got: {:?}",
                    other
                )))
            }
        }
    }

    Ok(FromResult::from_rows(schema, filtered_rows))
}

/// Perform join between two FROM results, optimizing with hash join when possible
///
/// This function now supports predicate pushdown from WHERE clauses. Additional equijoin
/// predicates from WHERE can be passed to optimize hash join selection and execution.
///
/// Note: This function combines rows from left and right according to the join type
/// and join condition. For queries with many tables and large intermediate results,
/// consider applying WHERE filters earlier to reduce memory usage.
///
/// Issue #3562: Added cte_results parameter so post-join filters with IN subqueries
/// can resolve CTE references.
#[allow(clippy::too_many_arguments)]
pub(super) fn nested_loop_join(
    left: FromResult,
    right: FromResult,
    join_type: &vibesql_ast::JoinType,
    condition: &Option<vibesql_ast::Expression>,
    natural: bool,
    database: &vibesql_storage::Database,
    additional_equijoins: &[vibesql_ast::Expression],
    timeout_ctx: &TimeoutContext,
    cte_results: &HashMap<String, CteResult>,
) -> Result<FromResult, ExecutorError> {
    // Try to use hash join for INNER JOINs with simple equi-join conditions
    if let vibesql_ast::JoinType::Inner = join_type {
        // Get column count and right table info once for analysis
        // IMPORTANT: Sum up columns from ALL tables in the left schema,
        // not just the first table, to handle accumulated multi-table joins
        let left_col_count: usize =
            left.schema.table_schemas.values().map(|(_, schema)| schema.columns.len()).sum();

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

        // Clone right_table_name before it gets moved into combine()
        let right_table_name_for_natural = right_table_name.clone();

        let temp_schema =
            CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema);

        // Phase 3.1: Try ON condition first (preferred for hash join)
        // Now supports multi-column composite keys for better performance
        if let Some(cond) = condition {
            // First try multi-column hash join for composite keys (2+ equi-join conditions)
            if let Some(multi_result) =
                join_analyzer::analyze_multi_column_equi_join(cond, &temp_schema, left_col_count)
            {
                // Use multi-column hash join if there are 2+ join columns
                if multi_result.equi_joins.left_col_indices.len() >= 2 {
                    // Save schemas for NATURAL JOIN processing before moving left/right
                    let (left_schema_for_natural, right_schema_for_natural) = if natural {
                        (Some(left.schema.clone()), Some(right.schema.clone()))
                    } else {
                        (None, None)
                    };

                    let mut result = hash_join_inner_multi(
                        left,
                        right,
                        &multi_result.equi_joins.left_col_indices,
                        &multi_result.equi_joins.right_col_indices,
                    )?;

                    // Apply remaining conditions as post-join filter
                    if !multi_result.remaining_conditions.is_empty() {
                        if let Some(filter_expr) =
                            combine_with_and(multi_result.remaining_conditions)
                        {
                            result = apply_post_join_filter(
                                result,
                                &filter_expr,
                                database,
                                cte_results,
                            )?;
                        }
                    }

                    // For NATURAL JOIN, remove duplicate columns from the result
                    if natural {
                        if let (Some(left_schema), Some(right_schema_orig)) =
                            (left_schema_for_natural, right_schema_for_natural)
                        {
                            let right_schema_for_removal = CombinedSchema {
                                table_schemas: vec![(
                                    right_table_name_for_natural.clone(),
                                    (
                                        0,
                                        right_schema_orig
                                            .table_schemas
                                            .values()
                                            .next()
                                            .unwrap()
                                            .1
                                            .clone(),
                                    ),
                                )]
                                .into_iter()
                                .collect(),
                                total_columns: right_schema_orig.total_columns,
                            };
                            result = remove_duplicate_columns_for_natural_join(
                                result,
                                &left_schema,
                                &right_schema_for_removal,
                            )?;
                        }
                    }

                    return Ok(result);
                }

                // Single-column equi-join: use standard hash join (more efficient for single key)
                // Save schemas for NATURAL JOIN processing before moving left/right
                let (left_schema_for_natural, right_schema_for_natural) = if natural {
                    (Some(left.schema.clone()), Some(right.schema.clone()))
                } else {
                    (None, None)
                };

                let mut result = hash_join_inner(
                    left,
                    right,
                    multi_result.equi_joins.left_col_indices[0],
                    multi_result.equi_joins.right_col_indices[0],
                )?;

                // Apply remaining conditions as post-join filter
                if !multi_result.remaining_conditions.is_empty() {
                    if let Some(filter_expr) = combine_with_and(multi_result.remaining_conditions) {
                        result =
                            apply_post_join_filter(result, &filter_expr, database, cte_results)?;
                    }
                }

                // For NATURAL JOIN, remove duplicate columns from the result
                if natural {
                    if let (Some(left_schema), Some(right_schema_orig)) =
                        (left_schema_for_natural, right_schema_for_natural)
                    {
                        let right_schema_for_removal = CombinedSchema {
                            table_schemas: vec![(
                                right_table_name_for_natural.clone(),
                                (
                                    0,
                                    right_schema_orig
                                        .table_schemas
                                        .values()
                                        .next()
                                        .unwrap()
                                        .1
                                        .clone(),
                                ),
                            )]
                            .into_iter()
                            .collect(),
                            total_columns: right_schema_orig.total_columns,
                        };
                        result = remove_duplicate_columns_for_natural_join(
                            result,
                            &left_schema,
                            &right_schema_for_removal,
                        )?;
                    }
                }

                return Ok(result);
            }
        }

        // Phase 3.2: Try OR conditions with common equi-join (TPC-H Q19 optimization)
        // For expressions like `(a.x = b.x AND ...) OR (a.x = b.x AND ...) OR (a.x = b.x AND ...)`,
        // extract the common equi-join `a.x = b.x` for hash join
        if let Some(cond) = condition {
            if let Some(or_result) =
                join_analyzer::analyze_or_equi_join(cond, &temp_schema, left_col_count)
            {
                // Save schemas for NATURAL JOIN processing before moving left/right
                let (left_schema_for_natural, right_schema_for_natural) = if natural {
                    (Some(left.schema.clone()), Some(right.schema.clone()))
                } else {
                    (None, None)
                };

                let mut result = hash_join_inner(
                    left,
                    right,
                    or_result.equi_join.left_col_idx,
                    or_result.equi_join.right_col_idx,
                )?;

                // Apply remaining OR conditions as post-join filter
                if !or_result.remaining_conditions.is_empty() {
                    if let Some(filter_expr) = combine_with_and(or_result.remaining_conditions) {
                        result =
                            apply_post_join_filter(result, &filter_expr, database, cte_results)?;
                    }
                }

                // For NATURAL JOIN, remove duplicate columns from the result
                if natural {
                    if let (Some(left_schema), Some(right_schema_orig)) =
                        (left_schema_for_natural, right_schema_for_natural)
                    {
                        let right_schema_for_removal = CombinedSchema {
                            table_schemas: vec![(
                                right_table_name_for_natural.clone(),
                                (
                                    0,
                                    right_schema_orig
                                        .table_schemas
                                        .values()
                                        .next()
                                        .unwrap()
                                        .1
                                        .clone(),
                                ),
                            )]
                            .into_iter()
                            .collect(),
                            total_columns: right_schema_orig.total_columns,
                        };
                        result = remove_duplicate_columns_for_natural_join(
                            result,
                            &left_schema,
                            &right_schema_for_removal,
                        )?;
                    }
                }

                return Ok(result);
            }
        }

        // Phase 3.3: Try arithmetic equi-join (TPC-DS Q2 optimization)
        // For expressions like `col1 = col2 - 53`, extract the arithmetic offset for hash join
        if let Some(cond) = condition {
            if let Some(arith_info) =
                join_analyzer::analyze_arithmetic_equi_join(cond, &temp_schema, left_col_count)
            {
                // Save schemas for NATURAL JOIN processing before moving left/right
                let (left_schema_for_natural, right_schema_for_natural) = if natural {
                    (Some(left.schema.clone()), Some(right.schema.clone()))
                } else {
                    (None, None)
                };

                let mut result = hash_join_inner_arithmetic(
                    left,
                    right,
                    arith_info.left_col_idx,
                    arith_info.right_col_idx,
                    arith_info.offset,
                )?;

                // For NATURAL JOIN, remove duplicate columns from the result
                if natural {
                    if let (Some(left_schema), Some(right_schema_orig)) =
                        (left_schema_for_natural, right_schema_for_natural)
                    {
                        let right_schema_for_removal = CombinedSchema {
                            table_schemas: vec![(
                                right_table_name_for_natural.clone(),
                                (
                                    0,
                                    right_schema_orig
                                        .table_schemas
                                        .values()
                                        .next()
                                        .unwrap()
                                        .1
                                        .clone(),
                                ),
                            )]
                            .into_iter()
                            .collect(),
                            total_columns: right_schema_orig.total_columns,
                        };
                        result = remove_duplicate_columns_for_natural_join(
                            result,
                            &left_schema,
                            &right_schema_for_removal,
                        )?;
                    }
                }

                return Ok(result);
            }
        }

        // Phase 3.4: Try multi-column hash join from WHERE clause conditions
        // When there are multiple equijoin conditions (e.g., ps_suppkey = l_suppkey AND ps_partkey = l_partkey),
        // using composite key hash join is critical for performance. Single-key hash join with post-filter
        // can cause catastrophic performance issues (48B cartesian products in Q9 at SF=0.1).
        if additional_equijoins.len() >= 2 {
            // Collect all valid equi-join conditions
            let mut left_col_indices = Vec::new();
            let mut right_col_indices = Vec::new();
            let mut used_indices = Vec::new();

            for (idx, equijoin) in additional_equijoins.iter().enumerate() {
                if let Some(equi_join_info) =
                    join_analyzer::analyze_equi_join(equijoin, &temp_schema, left_col_count)
                {
                    left_col_indices.push(equi_join_info.left_col_idx);
                    right_col_indices.push(equi_join_info.right_col_idx);
                    used_indices.push(idx);
                }
            }

            // If we found 2+ equi-join conditions, use multi-column hash join
            if left_col_indices.len() >= 2 {
                // Save schemas for NATURAL JOIN processing before moving left/right
                let (left_schema_for_natural, right_schema_for_natural) = if natural {
                    (Some(left.schema.clone()), Some(right.schema.clone()))
                } else {
                    (None, None)
                };

                let mut result = hash_join_inner_multi(
                    left,
                    right,
                    &left_col_indices,
                    &right_col_indices,
                )?;

                // Apply remaining conditions (non-equijoins) as post-join filters
                let remaining_conditions: Vec<_> = additional_equijoins
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| !used_indices.contains(i))
                    .map(|(_, e)| e.clone())
                    .collect();

                if !remaining_conditions.is_empty() {
                    if let Some(filter_expr) = combine_with_and(remaining_conditions) {
                        result =
                            apply_post_join_filter(result, &filter_expr, database, cte_results)?;
                    }
                }

                // For NATURAL JOIN, remove duplicate columns from the result
                if natural {
                    if let (Some(left_schema), Some(right_schema_orig)) =
                        (left_schema_for_natural, right_schema_for_natural)
                    {
                        let right_schema_for_removal = CombinedSchema {
                            table_schemas: vec![(
                                right_table_name_for_natural.clone(),
                                (
                                    0,
                                    right_schema_orig
                                        .table_schemas
                                        .values()
                                        .next()
                                        .unwrap()
                                        .1
                                        .clone(),
                                ),
                            )]
                            .into_iter()
                            .collect(),
                            total_columns: right_schema_orig.total_columns,
                        };
                        result = remove_duplicate_columns_for_natural_join(
                            result,
                            &left_schema,
                            &right_schema_for_removal,
                        )?;
                    }
                }

                return Ok(result);
            }
        }

        // Phase 3.5: If no multi-column hash join, try single-column WHERE clause equijoins
        // Iterate through all additional equijoins to find one suitable for hash join
        for (idx, equijoin) in additional_equijoins.iter().enumerate() {
            if let Some(equi_join_info) =
                join_analyzer::analyze_equi_join(equijoin, &temp_schema, left_col_count)
            {
                // Save schemas for NATURAL JOIN processing before moving left/right
                let (left_schema_for_natural, right_schema_for_natural) = if natural {
                    (Some(left.schema.clone()), Some(right.schema.clone()))
                } else {
                    (None, None)
                };

                // Found a WHERE clause equijoin suitable for hash join!
                let mut result = hash_join_inner(
                    left,
                    right,
                    equi_join_info.left_col_idx,
                    equi_join_info.right_col_idx,
                )?;

                // Apply remaining equijoins and conditions as post-join filters
                let remaining_conditions: Vec<_> = additional_equijoins
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != idx)
                    .map(|(_, e)| e.clone())
                    .collect();

                if !remaining_conditions.is_empty() {
                    if let Some(filter_expr) = combine_with_and(remaining_conditions) {
                        result =
                            apply_post_join_filter(result, &filter_expr, database, cte_results)?;
                    }
                }

                // For NATURAL JOIN, remove duplicate columns from the result
                if natural {
                    if let (Some(left_schema), Some(right_schema_orig)) =
                        (left_schema_for_natural, right_schema_for_natural)
                    {
                        let right_schema_for_removal = CombinedSchema {
                            table_schemas: vec![(
                                right_table_name_for_natural.clone(),
                                (
                                    0,
                                    right_schema_orig
                                        .table_schemas
                                        .values()
                                        .next()
                                        .unwrap()
                                        .1
                                        .clone(),
                                ),
                            )]
                            .into_iter()
                            .collect(),
                            total_columns: right_schema_orig.total_columns,
                        };
                        result = remove_duplicate_columns_for_natural_join(
                            result,
                            &left_schema,
                            &right_schema_for_removal,
                        )?;
                    }
                }

                return Ok(result);
            }
        }

        // Phase 3.6: Try arithmetic equijoins from WHERE clause for hash join
        // For expressions like `col1 = col2 - 53` in WHERE clause with Inner joins
        // This enables hash join for derived table joins with arithmetic conditions (TPC-DS Q2)
        for (idx, equijoin) in additional_equijoins.iter().enumerate() {
            if let Some(arith_info) =
                join_analyzer::analyze_arithmetic_equi_join(equijoin, &temp_schema, left_col_count)
            {
                // Save schemas for NATURAL JOIN processing before moving left/right
                let (left_schema_for_natural, right_schema_for_natural) = if natural {
                    (Some(left.schema.clone()), Some(right.schema.clone()))
                } else {
                    (None, None)
                };

                // Found an arithmetic equijoin suitable for hash join!
                let mut result = hash_join_inner_arithmetic(
                    left,
                    right,
                    arith_info.left_col_idx,
                    arith_info.right_col_idx,
                    arith_info.offset,
                )?;

                // Apply remaining equijoins as post-join filters
                let remaining_conditions: Vec<_> = additional_equijoins
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != idx)
                    .map(|(_, e)| e.clone())
                    .collect();

                if !remaining_conditions.is_empty() {
                    if let Some(filter_expr) = combine_with_and(remaining_conditions) {
                        result =
                            apply_post_join_filter(result, &filter_expr, database, cte_results)?;
                    }
                }

                // For NATURAL JOIN, remove duplicate columns from the result
                if natural {
                    if let (Some(left_schema), Some(right_schema_orig)) =
                        (left_schema_for_natural, right_schema_for_natural)
                    {
                        let right_schema_for_removal = CombinedSchema {
                            table_schemas: vec![(
                                right_table_name_for_natural.clone(),
                                (
                                    0,
                                    right_schema_orig
                                        .table_schemas
                                        .values()
                                        .next()
                                        .unwrap()
                                        .1
                                        .clone(),
                                ),
                            )]
                            .into_iter()
                            .collect(),
                            total_columns: right_schema_orig.total_columns,
                        };
                        result = remove_duplicate_columns_for_natural_join(
                            result,
                            &left_schema,
                            &right_schema_for_removal,
                        )?;
                    }
                }

                return Ok(result);
            }
        }
    }

    // Try to use hash join for LEFT OUTER JOINs with equi-join conditions
    // This optimization is critical for Q13 (customer LEFT JOIN orders)
    if let vibesql_ast::JoinType::LeftOuter = join_type {
        // Get column count and right table info for analysis
        let left_col_count: usize =
            left.schema.table_schemas.values().map(|(_, schema)| schema.columns.len()).sum();

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

        // Clone right_table_name before it gets moved into combine()
        let right_table_name_for_natural = right_table_name.clone();

        let temp_schema =
            CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema);

        // Try ON condition for hash join with multi-column support
        // Use multi-column analysis to include ALL equi-join conditions in the hash join
        // This is critical for LEFT JOIN correctness: post-filter conditions on NULL columns
        // (from unmatched rows) incorrectly filter out valid LEFT JOIN results
        if let Some(cond) = condition {
            if let Some(multi_result) =
                join_analyzer::analyze_multi_column_equi_join(cond, &temp_schema, left_col_count)
            {
                // Save schemas for NATURAL JOIN processing before moving left/right
                let (left_schema_for_natural, right_schema_for_natural) = if natural {
                    (Some(left.schema.clone()), Some(right.schema.clone()))
                } else {
                    (None, None)
                };

                // Use multi-column hash join when there are multiple equi-join columns
                // This ensures all equi-join conditions are matched during hash lookup,
                // not filtered afterward (which breaks LEFT JOIN semantics for NULL columns)
                let mut result = if multi_result.equi_joins.left_col_indices.len() > 1 {
                    hash_join_left_outer_multi(
                        left,
                        right,
                        &multi_result.equi_joins.left_col_indices,
                        &multi_result.equi_joins.right_col_indices,
                    )?
                } else {
                    // Single column - use optimized single-column hash join
                    hash_join_left_outer(
                        left,
                        right,
                        multi_result.equi_joins.left_col_indices[0],
                        multi_result.equi_joins.right_col_indices[0],
                    )?
                };

                // Apply remaining NON-equi-join conditions as post-join filter
                // Note: For LEFT JOIN, remaining conditions on right columns may evaluate
                // to NULL for unmatched rows. These rows should be preserved.
                // TODO: Consider a LEFT JOIN-aware post-filter that preserves NULL results
                if !multi_result.remaining_conditions.is_empty() {
                    if let Some(filter_expr) =
                        combine_with_and(multi_result.remaining_conditions)
                    {
                        result =
                            apply_post_join_filter(result, &filter_expr, database, cte_results)?;
                    }
                }

                // For NATURAL JOIN, remove duplicate columns from the result
                if natural {
                    if let (Some(left_schema), Some(right_schema_orig)) =
                        (left_schema_for_natural, right_schema_for_natural)
                    {
                        let right_schema_for_removal = CombinedSchema {
                            table_schemas: vec![(
                                right_table_name_for_natural.clone(),
                                (
                                    0,
                                    right_schema_orig
                                        .table_schemas
                                        .values()
                                        .next()
                                        .unwrap()
                                        .1
                                        .clone(),
                                ),
                            )]
                            .into_iter()
                            .collect(),
                            total_columns: right_schema_orig.total_columns,
                        };
                        result = remove_duplicate_columns_for_natural_join(
                            result,
                            &left_schema,
                            &right_schema_for_removal,
                        )?;
                    }
                }

                return Ok(result);
            }
        }
    }

    // Try to use hash join for SEMI/ANTI JOINs with equi-join conditions
    if matches!(join_type, vibesql_ast::JoinType::Semi | vibesql_ast::JoinType::Anti) {
        // Get column count for analysis
        let left_col_count: usize =
            left.schema.table_schemas.values().map(|(_, schema)| schema.columns.len()).sum();

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

        let temp_schema =
            CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema);

        // Try ON condition first - use analyze_compound_equi_join to handle complex conditions
        // This enables hash join optimization for EXISTS subqueries with additional predicates
        // Example: EXISTS (SELECT * FROM t WHERE t.x = outer.x AND t.y <> outer.y)
        // The equi-join (t.x = outer.x) is used for hash join, and the inequality is a post-filter
        if let Some(cond) = condition {
            if let Some(compound_result) =
                join_analyzer::analyze_compound_equi_join(cond, &temp_schema, left_col_count)
            {
                // Build the combined remaining condition (if any)
                let remaining_filter = combine_with_and(compound_result.remaining_conditions);

                let result = if matches!(join_type, vibesql_ast::JoinType::Semi) {
                    hash_semi_join_with_filter(
                        left,
                        right,
                        compound_result.equi_join.left_col_idx,
                        compound_result.equi_join.right_col_idx,
                        remaining_filter.as_ref(),
                        &temp_schema,
                        database,
                    )?
                } else {
                    hash_anti_join_with_filter(
                        left,
                        right,
                        compound_result.equi_join.left_col_idx,
                        compound_result.equi_join.right_col_idx,
                        remaining_filter.as_ref(),
                        &temp_schema,
                        database,
                    )?
                };

                return Ok(result);
            }
        }

        // Try WHERE clause equijoins
        for equijoin in additional_equijoins.iter() {
            if let Some(equi_join_info) =
                join_analyzer::analyze_equi_join(equijoin, &temp_schema, left_col_count)
            {
                let result = if matches!(join_type, vibesql_ast::JoinType::Semi) {
                    hash_semi_join(
                        left,
                        right,
                        equi_join_info.left_col_idx,
                        equi_join_info.right_col_idx,
                    )?
                } else {
                    hash_anti_join(
                        left,
                        right,
                        equi_join_info.left_col_idx,
                        equi_join_info.right_col_idx,
                    )?
                };

                return Ok(result);
            }
        }
    }

    // Try to use hash join for CROSS JOINs when equijoin conditions exist in WHERE clause
    // This is critical for Q21 and other TPC-H queries with implicit (comma-separated) joins
    // CROSS JOIN with equijoin predicates should be executed as hash INNER JOIN
    if let vibesql_ast::JoinType::Cross = join_type {
        if !additional_equijoins.is_empty() {
            // Get column count and right table info for analysis
            let left_col_count: usize =
                left.schema.table_schemas.values().map(|(_, schema)| schema.columns.len()).sum();

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

            let temp_schema =
                CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema);

            // Try WHERE clause equijoins for hash join
            for (idx, equijoin) in additional_equijoins.iter().enumerate() {
                if let Some(equi_join_info) =
                    join_analyzer::analyze_equi_join(equijoin, &temp_schema, left_col_count)
                {
                    // Found a WHERE clause equijoin suitable for hash join!
                    // Execute CROSS JOIN as hash INNER JOIN with the equijoin condition
                    let mut result = hash_join_inner(
                        left,
                        right,
                        equi_join_info.left_col_idx,
                        equi_join_info.right_col_idx,
                    )?;

                    // Apply remaining equijoins as post-join filters
                    let remaining_conditions: Vec<_> = additional_equijoins
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| *i != idx)
                        .map(|(_, e)| e.clone())
                        .collect();

                    if !remaining_conditions.is_empty() {
                        if let Some(filter_expr) = combine_with_and(remaining_conditions) {
                            result = apply_post_join_filter(
                                result,
                                &filter_expr,
                                database,
                                cte_results,
                            )?;
                        }
                    }

                    return Ok(result);
                }
            }

            // Try arithmetic equijoins for hash join (TPC-DS Q2 optimization)
            // For expressions like `col1 = col2 - 53` in WHERE clause
            for (idx, equijoin) in additional_equijoins.iter().enumerate() {
                if let Some(arith_info) = join_analyzer::analyze_arithmetic_equi_join(
                    equijoin,
                    &temp_schema,
                    left_col_count,
                ) {
                    // Found an arithmetic equijoin suitable for hash join!
                    let mut result = hash_join_inner_arithmetic(
                        left,
                        right,
                        arith_info.left_col_idx,
                        arith_info.right_col_idx,
                        arith_info.offset,
                    )?;

                    // Apply remaining equijoins as post-join filters
                    let remaining_conditions: Vec<_> = additional_equijoins
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| *i != idx)
                        .map(|(_, e)| e.clone())
                        .collect();

                    if !remaining_conditions.is_empty() {
                        if let Some(filter_expr) = combine_with_and(remaining_conditions) {
                            result = apply_post_join_filter(
                                result,
                                &filter_expr,
                                database,
                                cte_results,
                            )?;
                        }
                    }

                    return Ok(result);
                }
            }
        }
    }

    // Prepare combined join condition including additional equijoins from WHERE clause
    let mut all_join_conditions = Vec::new();
    if let Some(cond) = condition {
        all_join_conditions.push(cond.clone());
    }
    all_join_conditions.extend_from_slice(additional_equijoins);

    // Combine all join conditions with AND
    let combined_condition = combine_with_and(all_join_conditions);

    // Fall back to nested loop join for all other cases
    // For NATURAL JOIN, we need to preserve the original schemas for duplicate removal
    let (left_schema_for_natural, right_schema_for_natural) = if natural {
        (Some(left.schema.clone()), Some(right.schema.clone()))
    } else {
        (None, None)
    };

    let mut result = match join_type {
        vibesql_ast::JoinType::Inner => {
            nested_loop_inner_join(left, right, &combined_condition, database, timeout_ctx)
        }
        vibesql_ast::JoinType::LeftOuter => {
            nested_loop_left_outer_join(left, right, &combined_condition, database, timeout_ctx)
        }
        vibesql_ast::JoinType::RightOuter => {
            nested_loop_right_outer_join(left, right, &combined_condition, database, timeout_ctx)
        }
        vibesql_ast::JoinType::FullOuter => {
            nested_loop_full_outer_join(left, right, &combined_condition, database, timeout_ctx)
        }
        vibesql_ast::JoinType::Cross => {
            nested_loop_cross_join(left, right, &combined_condition, database, timeout_ctx)
        }
        vibesql_ast::JoinType::Semi => {
            nested_loop_semi_join(left, right, &combined_condition, database, timeout_ctx)
        }
        vibesql_ast::JoinType::Anti => {
            nested_loop_anti_join(left, right, &combined_condition, database, timeout_ctx)
        }
    }?;

    // For NATURAL JOIN, remove duplicate columns from the result
    if natural {
        if let (Some(left_schema), Some(right_schema)) =
            (left_schema_for_natural, right_schema_for_natural)
        {
            result =
                remove_duplicate_columns_for_natural_join(result, &left_schema, &right_schema)?;
        }
    }

    Ok(result)
}

/// Remove duplicate columns for NATURAL JOIN
///
/// NATURAL JOIN should only include common columns once (from the left side).
/// This function identifies common columns and removes duplicates from the right side.
fn remove_duplicate_columns_for_natural_join(
    mut result: FromResult,
    left_schema: &CombinedSchema,
    right_schema: &CombinedSchema,
) -> Result<FromResult, ExecutorError> {
    use std::collections::{HashMap, HashSet};

    // Find common column names (case-insensitive)
    let mut left_column_map: HashMap<String, Vec<(String, String, usize)>> = HashMap::new(); // lowercase -> [(table, actual_name, idx)]
    let mut col_idx = 0;
    for (table_name, (_table_idx, table_schema)) in &left_schema.table_schemas {
        for col in &table_schema.columns {
            let lowercase = col.name.to_lowercase();
            left_column_map.entry(lowercase).or_default().push((
                table_name.to_string(),
                col.name.clone(),
                col_idx,
            ));
            col_idx += 1;
        }
    }

    // Identify which columns from the right side are duplicates
    let mut right_duplicate_indices: HashSet<usize> = HashSet::new();
    let left_col_count = col_idx;
    col_idx = 0;
    for (_table_idx, table_schema) in right_schema.table_schemas.values() {
        for col in &table_schema.columns {
            let lowercase = col.name.to_lowercase();
            if left_column_map.contains_key(&lowercase) {
                // This is a common column, mark it as a duplicate to remove
                right_duplicate_indices.insert(left_col_count + col_idx);
            }
            col_idx += 1;
        }
    }

    // If no duplicates, return as-is
    if right_duplicate_indices.is_empty() {
        return Ok(result);
    }

    // Project out the duplicate columns from the result
    let total_cols = left_col_count + col_idx;
    let keep_indices: Vec<usize> =
        (0..total_cols).filter(|i| !right_duplicate_indices.contains(i)).collect();

    // Build new schema without duplicate columns
    let mut new_schema = CombinedSchema { table_schemas: HashMap::new(), total_columns: 0 };
    for (table_name, (table_start_idx, table_schema)) in &result.schema.table_schemas {
        let mut new_cols = Vec::new();

        for (idx, col) in table_schema.columns.iter().enumerate() {
            // Calculate absolute column index manually
            let abs_col_idx = table_start_idx + idx;

            if keep_indices.contains(&abs_col_idx) {
                new_cols.push(col.clone());
            }
        }

        if !new_cols.is_empty() {
            let new_table_schema =
                vibesql_catalog::TableSchema::new(table_schema.name.clone(), new_cols);
            new_schema
                .table_schemas
                .insert(table_name.clone(), (new_schema.total_columns, new_table_schema.clone()));
            new_schema.total_columns += new_table_schema.columns.len();
        }
    }

    // Project the rows - get mutable reference to rows to work with FromResult API
    let rows = result.rows();
    let new_rows: Vec<vibesql_storage::Row> = rows
        .iter()
        .map(|row| {
            let new_values: Vec<vibesql_types::SqlValue> =
                keep_indices.iter().filter_map(|&i| row.values.get(i).cloned()).collect();
            vibesql_storage::Row::new(new_values)
        })
        .collect();

    Ok(FromResult::from_rows(new_schema, new_rows))
}
