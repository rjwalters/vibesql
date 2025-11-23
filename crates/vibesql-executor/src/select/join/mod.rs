use crate::{
    errors::ExecutorError, evaluator::CombinedExpressionEvaluator, optimizer::combine_with_and,
    schema::CombinedSchema,
};
use super::from_iterator::FromIterator;

mod expression_mapper;
mod hash_join;
mod hash_join_iterator;
mod hash_semi_join;
mod hash_anti_join;
mod join_analyzer;
mod nested_loop;
pub mod reorder;
pub mod search;

#[cfg(test)]
mod tests;

// Re-export join reorder analyzer for public tests
// Re-export hash_join functions for internal use
use hash_join::{hash_join_inner, hash_join_left_outer};
use hash_semi_join::{hash_semi_join, hash_semi_join_with_filter};
use hash_anti_join::{hash_anti_join, hash_anti_join_with_filter};
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

/// Data source for FROM clause results
///
/// This enum allows FROM results to be either materialized (Vec<Row>) or lazy (iterator).
/// Materialized results are used for JOINs, CTEs, and operations that need multiple passes.
/// Lazy results are used for simple table scans to enable streaming execution.
pub(super) enum FromData {
    /// Materialized rows (for JOINs, CTEs, operations needing multiple passes)
    Materialized(Vec<vibesql_storage::Row>),

    /// Lazy iterator (for streaming table scans)
    Iterator(FromIterator),
}

impl FromData {
    /// Get rows, materializing if needed
    pub fn into_rows(self) -> Vec<vibesql_storage::Row> {
        match self {
            Self::Materialized(rows) => rows,
            Self::Iterator(iter) => iter.collect_vec(),
        }
    }

    /// Get a reference to materialized rows, or materialize if iterator
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
                    eprintln!("[Q6 PROFILE] Row materialization (collect_vec): {:?} ({} rows, {:?}/row)",
                        materialize_time, rows.len(), materialize_time / rows.len() as u32);
                }
            }
        }

        // Now we're guaranteed to have materialized rows
        match self {
            Self::Materialized(rows) => rows,
            Self::Iterator(_) => unreachable!(),
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

    /// Create a FromResult from materialized rows with sorting metadata
    pub(super) fn from_rows_sorted(
        schema: CombinedSchema,
        rows: Vec<vibesql_storage::Row>,
        sorted_by: Vec<(String, vibesql_ast::OrderDirection)>,
    ) -> Self {
        Self { schema, data: FromData::Materialized(rows), sorted_by: Some(sorted_by), where_filtered: false }
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

    /// Get a mutable reference to the rows, materializing if needed
    #[allow(dead_code)]
    pub(super) fn rows_mut(&mut self) -> &mut Vec<vibesql_storage::Row> {
        // First materialize if needed
        self.data.as_rows();

        // Now we're guaranteed to have materialized rows
        match &mut self.data {
            FromData::Materialized(rows) => rows,
            FromData::Iterator(_) => unreachable!(),
        }
    }

    /// Get a reference to rows, materializing if needed
    pub(super) fn rows(&mut self) -> &Vec<vibesql_storage::Row> {
        self.data.as_rows()
    }
}

/// Helper function to combine two rows without unnecessary cloning
/// Only creates a single combined row, avoiding intermediate clones
#[inline]
fn combine_rows(left_row: &vibesql_storage::Row, right_row: &vibesql_storage::Row) -> vibesql_storage::Row {
    let mut combined_values = Vec::with_capacity(left_row.values.len() + right_row.values.len());
    combined_values.extend_from_slice(&left_row.values);
    combined_values.extend_from_slice(&right_row.values);
    vibesql_storage::Row::new(combined_values)
}

/// Apply a post-join filter expression to join result rows
///
/// This is used to filter rows produced by hash join with additional conditions
/// from the WHERE clause that weren't used in the hash join itself.
fn apply_post_join_filter(
    result: FromResult,
    filter_expr: &vibesql_ast::Expression,
    database: &vibesql_storage::Database,
) -> Result<FromResult, ExecutorError> {
    // Extract schema before moving result
    let schema = result.schema.clone();
    let evaluator = CombinedExpressionEvaluator::with_database(&schema, database);

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
pub(super) fn nested_loop_join(
    left: FromResult,
    right: FromResult,
    join_type: &vibesql_ast::JoinType,
    condition: &Option<vibesql_ast::Expression>,
    natural: bool,
    database: &vibesql_storage::Database,
    additional_equijoins: &[vibesql_ast::Expression],
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
        // Now supports compound AND conditions for queries like TPC-H Q3
        if let Some(cond) = condition {
            if let Some(compound_result) =
                join_analyzer::analyze_compound_equi_join(cond, &temp_schema, left_col_count)
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
                    compound_result.equi_join.left_col_idx,
                    compound_result.equi_join.right_col_idx,
                )?;

                // Apply remaining conditions from compound AND as post-join filter
                if !compound_result.remaining_conditions.is_empty() {
                    if let Some(filter_expr) = combine_with_and(compound_result.remaining_conditions) {
                        result = apply_post_join_filter(result, &filter_expr, database)?;
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
                                (0, right_schema_orig.table_schemas.values().next().unwrap().1.clone()),
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
                        result = apply_post_join_filter(result, &filter_expr, database)?;
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
                                (0, right_schema_orig.table_schemas.values().next().unwrap().1.clone()),
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

        // Phase 3.1: If no ON condition hash join, try WHERE clause equijoins
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
                        result = apply_post_join_filter(result, &filter_expr, database)?;
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
                                (0, right_schema_orig.table_schemas.values().next().unwrap().1.clone()),
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

        // Try ON condition for hash join with compound AND support
        if let Some(cond) = condition {
            if let Some(compound_result) =
                join_analyzer::analyze_compound_equi_join(cond, &temp_schema, left_col_count)
            {
                // Save schemas for NATURAL JOIN processing before moving left/right
                let (left_schema_for_natural, right_schema_for_natural) = if natural {
                    (Some(left.schema.clone()), Some(right.schema.clone()))
                } else {
                    (None, None)
                };

                let mut result = hash_join_left_outer(
                    left,
                    right,
                    compound_result.equi_join.left_col_idx,
                    compound_result.equi_join.right_col_idx,
                )?;

                // Apply remaining conditions from compound AND as post-join filter
                if !compound_result.remaining_conditions.is_empty() {
                    if let Some(filter_expr) = combine_with_and(compound_result.remaining_conditions) {
                        result = apply_post_join_filter(result, &filter_expr, database)?;
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
                                (0, right_schema_orig.table_schemas.values().next().unwrap().1.clone()),
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
        vibesql_ast::JoinType::Inner => nested_loop_inner_join(left, right, &combined_condition, database),
        vibesql_ast::JoinType::LeftOuter => {
            nested_loop_left_outer_join(left, right, &combined_condition, database)
        }
        vibesql_ast::JoinType::RightOuter => {
            nested_loop_right_outer_join(left, right, &combined_condition, database)
        }
        vibesql_ast::JoinType::FullOuter => {
            nested_loop_full_outer_join(left, right, &combined_condition, database)
        }
        vibesql_ast::JoinType::Cross => nested_loop_cross_join(left, right, &combined_condition, database),
        vibesql_ast::JoinType::Semi => nested_loop_semi_join(left, right, &combined_condition, database),
        vibesql_ast::JoinType::Anti => nested_loop_anti_join(left, right, &combined_condition, database),
    }?;

    // For NATURAL JOIN, remove duplicate columns from the result
    if natural {
        if let (Some(left_schema), Some(right_schema)) = (left_schema_for_natural, right_schema_for_natural) {
            result = remove_duplicate_columns_for_natural_join(result, &left_schema, &right_schema)?;
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
            left_column_map
                .entry(lowercase)
                .or_default()
                .push((table_name.clone(), col.name.clone(), col_idx));
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
    let keep_indices: Vec<usize> = (0..total_cols)
        .filter(|i| !right_duplicate_indices.contains(i))
        .collect();

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
            let new_table_schema = vibesql_catalog::TableSchema::new(
                table_schema.name.clone(),
                new_cols,
            );
            new_schema.table_schemas.insert(
                table_name.clone(),
                (new_schema.total_columns, new_table_schema.clone()),
            );
            new_schema.total_columns += new_table_schema.columns.len();
        }
    }

    // Project the rows - get mutable reference to rows to work with FromResult API
    let rows = result.rows();
    let new_rows: Vec<vibesql_storage::Row> = rows
        .iter()
        .map(|row| {
            let new_values: Vec<vibesql_types::SqlValue> = keep_indices
                .iter()
                .filter_map(|&i| row.values.get(i).cloned())
                .collect();
            vibesql_storage::Row::new(new_values)
        })
        .collect();

    Ok(FromResult::from_rows(new_schema, new_rows))
}
