//! ORDER BY sorting logic
//!
//! This module handles all ORDER BY processing including:
//! - Column position extraction and validation (`position`)
//! - Alias and expression resolution (`resolution`)
//! - Value comparison with collation support (`comparison`)
//! - Parallel sorting for large datasets (`parallel`)

mod comparison;
mod parallel;
mod position;
mod resolution;

// Re-export the compare_sql_values function from the grouping module
// for use in comparison.rs
use super::grouping;

use crate::{errors::ExecutorError, evaluator::CombinedExpressionEvaluator};

// Re-export public API
pub(crate) use resolution::{
    extract_order_by_aggregates, extract_window_aggregates, order_by_volatile_output_index,
    resolve_order_by_alias, resolve_order_by_for_aggregates, resolve_where_aliases,
    resolve_where_aliases_with_schema, select_list_has_aliases,
};

/// Sort key for ORDER BY: (value, direction, nulls_order, collation)
pub(super) type SortKey = (
    vibesql_types::SqlValue,
    vibesql_ast::OrderDirection,
    Option<vibesql_ast::NullsOrder>,
    Option<String>,
);

/// Row with optional sort keys for ORDER BY
pub(super) type RowWithSortKeys = (vibesql_storage::Row, Option<Vec<SortKey>>);

/// Apply ORDER BY sorting to rows
///
/// Evaluates ORDER BY expressions for each row and sorts them according to the specified
/// directions (ASC/DESC). Supports multi-column sorting with stable sort behavior.
///
/// ORDER BY can reference:
/// - Columns from the FROM clause
/// - Aliases from the SELECT list
/// - Original column names that have been aliased in SELECT
/// - Arbitrary expressions
pub(super) fn apply_order_by(
    mut rows: Vec<RowWithSortKeys>,
    order_by: &[vibesql_ast::OrderByItem],
    evaluator: &CombinedExpressionEvaluator,
    select_list: &[vibesql_ast::SelectItem],
) -> Result<Vec<RowWithSortKeys>, ExecutorError> {
    // Get schema for proper wildcard expansion when counting columns (#4413)
    let schema = evaluator.schema();

    // Evaluate ORDER BY expressions for each row
    for (row, sort_keys) in &mut rows {
        // Clear CSE cache before evaluating this row's ORDER BY expressions
        // to prevent stale cached column values from previous rows
        evaluator.clear_cse_cache();

        let mut keys = Vec::new();
        for (term_index, order_item) in order_by.iter().enumerate() {
            // Check if ORDER BY expression is a SELECT list alias or matches an aliased column
            // Evaluator handles window functions via window_mapping if present
            // Pass schema for proper wildcard expansion in column count validation
            let expr_to_eval =
                resolve_order_by_alias(&order_item.expr, select_list, term_index, Some(schema))?;
            let key_value = evaluator.eval(expr_to_eval.as_ref(), row)?;
            // Get collation for this ORDER BY expression (explicit or inherited from column)
            let collation = evaluator.get_expression_collation(expr_to_eval.as_ref());
            keys.push((key_value, order_item.direction.clone(), order_item.nulls_order, collation));
        }
        *sort_keys = Some(keys);
    }

    // Sort by the evaluated keys (with automatic parallelism based on row count when feature
    // enabled)
    parallel::sort_rows(&mut rows);

    Ok(rows)
}

/// Sort already-projected output rows by ORDER BY terms that reference output
/// columns positionally.
///
/// This is used when at least one ORDER BY term references a SELECT output
/// column whose projected expression is non-deterministic (e.g.
/// `abs(random())%5`). In that situation the expression must be evaluated
/// exactly once — at projection time — and the sort must read the projected
/// output value rather than re-evaluating the expression (which would call the
/// volatile function again and produce a sort key inconsistent with the row's
/// output). See issue #5712 (distinct2-5020).
///
/// `term_output_indices[i]` gives the 0-based output column index that ORDER BY
/// term `i` sorts on. All terms must map to an output column for this path.
pub(crate) fn apply_order_by_on_projected_output(
    rows: Vec<vibesql_storage::Row>,
    order_by: &[vibesql_ast::OrderByItem],
    term_output_indices: &[usize],
) -> Vec<vibesql_storage::Row> {
    debug_assert_eq!(order_by.len(), term_output_indices.len());

    let mut rows_with_keys: Vec<RowWithSortKeys> = rows
        .into_iter()
        .map(|row| {
            let keys: Vec<SortKey> = order_by
                .iter()
                .zip(term_output_indices.iter())
                .map(|(order_item, &out_idx)| {
                    let value =
                        row.values.get(out_idx).cloned().unwrap_or(vibesql_types::SqlValue::Null);
                    (value, order_item.direction.clone(), order_item.nulls_order, None)
                })
                .collect();
            (row, Some(keys))
        })
        .collect();

    parallel::sort_rows(&mut rows_with_keys);

    rows_with_keys.into_iter().map(|(row, _)| row).collect()
}

#[cfg(test)]
mod tests {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    use super::comparison::compare_rows_by_sort_keys;
    use super::*;

    #[cfg(feature = "parallel")]
    use super::super::parallel::ParallelConfig;
    #[cfg(feature = "parallel")]
    use rayon::slice::ParallelSliceMut;

    /// Test the comparison function logic with pre-evaluated sort keys
    /// This tests the parallel/sequential sorting logic without needing full evaluator setup
    #[test]
    fn test_sort_with_keys_small_dataset() {
        // Small dataset with pre-populated sort keys
        let mut rows: Vec<RowWithSortKeys> = vec![
            (
                Row::from_vec(vec![SqlValue::Integer(3)]),
                Some(vec![(SqlValue::Integer(3), vibesql_ast::OrderDirection::Asc, None, None)]),
            ),
            (
                Row::from_vec(vec![SqlValue::Integer(1)]),
                Some(vec![(SqlValue::Integer(1), vibesql_ast::OrderDirection::Asc, None, None)]),
            ),
            (
                Row::from_vec(vec![SqlValue::Integer(2)]),
                Some(vec![(SqlValue::Integer(2), vibesql_ast::OrderDirection::Asc, None, None)]),
            ),
        ];

        // Apply sorting logic (mimics what apply_order_by does after key evaluation)
        let comparison_fn = |(_, keys_a): &RowWithSortKeys, (_, keys_b): &RowWithSortKeys| {
            let keys_a = keys_a.as_ref().unwrap();
            let keys_b = keys_b.as_ref().unwrap();
            compare_rows_by_sort_keys(keys_a, keys_b)
        };

        rows.sort_by(comparison_fn);

        // Verify sorted order
        assert_eq!(rows[0].0.values[0], SqlValue::Integer(1));
        assert_eq!(rows[1].0.values[0], SqlValue::Integer(2));
        assert_eq!(rows[2].0.values[0], SqlValue::Integer(3));
    }

    #[test]
    fn test_sort_with_keys_large_dataset() {
        // Create large dataset that will trigger parallel path
        let mut rows: Vec<RowWithSortKeys> = Vec::new();
        for i in (0..15000).rev() {
            rows.push((
                Row::from_vec(vec![SqlValue::Integer(i)]),
                Some(vec![(SqlValue::Integer(i), vibesql_ast::OrderDirection::Asc, None, None)]),
            ));
        }

        let comparison_fn = |(_, keys_a): &RowWithSortKeys, (_, keys_b): &RowWithSortKeys| {
            let keys_a = keys_a.as_ref().unwrap();
            let keys_b = keys_b.as_ref().unwrap();
            compare_rows_by_sort_keys(keys_a, keys_b)
        };

        #[cfg(feature = "parallel")]
        {
            let config = ParallelConfig::global();
            if config.should_parallelize_sort(rows.len()) {
                rows.par_sort_by(comparison_fn);
            } else {
                rows.sort_by(comparison_fn);
            }
        }

        #[cfg(not(feature = "parallel"))]
        {
            rows.sort_by(comparison_fn);
        }

        // Verify first few and last few are correctly sorted
        assert_eq!(rows[0].0.values[0], SqlValue::Integer(0));
        assert_eq!(rows[1].0.values[0], SqlValue::Integer(1));
        assert_eq!(rows[2].0.values[0], SqlValue::Integer(2));
        assert_eq!(rows[14997].0.values[0], SqlValue::Integer(14997));
        assert_eq!(rows[14998].0.values[0], SqlValue::Integer(14998));
        assert_eq!(rows[14999].0.values[0], SqlValue::Integer(14999));
    }

    #[test]
    fn test_sort_descending_with_keys() {
        let mut rows: Vec<RowWithSortKeys> = vec![
            (
                Row::from_vec(vec![SqlValue::Integer(1)]),
                Some(vec![(SqlValue::Integer(1), vibesql_ast::OrderDirection::Desc, None, None)]),
            ),
            (
                Row::from_vec(vec![SqlValue::Integer(3)]),
                Some(vec![(SqlValue::Integer(3), vibesql_ast::OrderDirection::Desc, None, None)]),
            ),
            (
                Row::from_vec(vec![SqlValue::Integer(2)]),
                Some(vec![(SqlValue::Integer(2), vibesql_ast::OrderDirection::Desc, None, None)]),
            ),
        ];

        let comparison_fn = |(_, keys_a): &RowWithSortKeys, (_, keys_b): &RowWithSortKeys| {
            let keys_a = keys_a.as_ref().unwrap();
            let keys_b = keys_b.as_ref().unwrap();
            compare_rows_by_sort_keys(keys_a, keys_b)
        };

        rows.sort_by(comparison_fn);

        assert_eq!(rows[0].0.values[0], SqlValue::Integer(3));
        assert_eq!(rows[1].0.values[0], SqlValue::Integer(2));
        assert_eq!(rows[2].0.values[0], SqlValue::Integer(1));
    }

    #[test]
    fn test_sort_with_nulls() {
        // SQLite treats NULL as smallest value:
        // - ASC: NULL comes first
        // - DESC: NULL comes last
        let mut rows_asc: Vec<RowWithSortKeys> = vec![
            (
                Row::from_vec(vec![SqlValue::Integer(2)]),
                Some(vec![(SqlValue::Integer(2), vibesql_ast::OrderDirection::Asc, None, None)]),
            ),
            (
                Row::from_vec(vec![SqlValue::Null]),
                Some(vec![(SqlValue::Null, vibesql_ast::OrderDirection::Asc, None, None)]),
            ),
            (
                Row::from_vec(vec![SqlValue::Integer(1)]),
                Some(vec![(SqlValue::Integer(1), vibesql_ast::OrderDirection::Asc, None, None)]),
            ),
        ];

        let comparison_fn = |(_, keys_a): &RowWithSortKeys, (_, keys_b): &RowWithSortKeys| {
            let keys_a = keys_a.as_ref().unwrap();
            let keys_b = keys_b.as_ref().unwrap();
            compare_rows_by_sort_keys(keys_a, keys_b)
        };

        rows_asc.sort_by(&comparison_fn);

        // ASC: NULL, 1, 2 (NULLs first, as smallest value)
        assert_eq!(rows_asc[0].0.values[0], SqlValue::Null);
        assert_eq!(rows_asc[1].0.values[0], SqlValue::Integer(1));
        assert_eq!(rows_asc[2].0.values[0], SqlValue::Integer(2));
    }

    #[test]
    #[cfg(feature = "parallel")]
    fn test_parallel_config_threshold() {
        let config = ParallelConfig::global();

        // Verify that parallel sorting is disabled for small datasets
        // (actual threshold depends on hardware, but should be > 100)
        assert!(!config.should_parallelize_sort(100));

        // Verify that parallel sorting is enabled for large datasets
        // (15000 rows should trigger parallel path on any reasonable hardware)
        // Note: On single-core systems this might still be false
        let large_dataset_size = 15000;
        let uses_parallel = config.should_parallelize_sort(large_dataset_size);

        // Just verify the threshold logic is working (result depends on hardware)
        if config.num_threads > 1 {
            // On multi-core, large datasets should use parallel
            assert!(uses_parallel || config.thresholds.sort > large_dataset_size);
        }
    }
}
