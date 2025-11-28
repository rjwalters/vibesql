//! ORDER BY sorting logic

use std::cmp::Ordering;

#[cfg(feature = "parallel")]
use rayon::slice::ParallelSliceMut;

#[cfg(feature = "parallel")]
use super::parallel::ParallelConfig;
use super::grouping::compare_sql_values;
use crate::{errors::ExecutorError, evaluator::CombinedExpressionEvaluator};

/// Row with optional sort keys for ORDER BY
pub(super) type RowWithSortKeys =
    (vibesql_storage::Row, Option<Vec<(vibesql_types::SqlValue, vibesql_ast::OrderDirection)>>);

/// Apply ORDER BY sorting to rows
///
/// Evaluates ORDER BY expressions for each row and sorts them according to the specified
/// directions (ASC/DESC). Supports multi-column sorting with stable sort behavior.
///
/// ORDER BY can reference:
/// - Columns from the FROM clause
/// - Aliases from the SELECT list
/// - Arbitrary expressions
pub(super) fn apply_order_by(
    mut rows: Vec<RowWithSortKeys>,
    order_by: &[vibesql_ast::OrderByItem],
    evaluator: &CombinedExpressionEvaluator,
    select_list: &[vibesql_ast::SelectItem],
) -> Result<Vec<RowWithSortKeys>, ExecutorError> {
    // Evaluate ORDER BY expressions for each row
    for (row, sort_keys) in &mut rows {
        // Clear CSE cache before evaluating this row's ORDER BY expressions
        // to prevent stale cached column values from previous rows
        evaluator.clear_cse_cache();

        let mut keys = Vec::new();
        for order_item in order_by {
            // Check if ORDER BY expression is a SELECT list alias
            // Evaluator handles window functions via window_mapping if present
            let expr_to_eval = resolve_order_by_alias(&order_item.expr, select_list);
            let key_value = evaluator.eval(expr_to_eval, row)?;
            keys.push((key_value, order_item.direction.clone()));
        }
        *sort_keys = Some(keys);
    }

    // Sort by the evaluated keys (with automatic parallelism based on row count when feature enabled)
    let comparison_fn = |(_, keys_a): &RowWithSortKeys, (_, keys_b): &RowWithSortKeys| {
        let keys_a = keys_a.as_ref().unwrap();
        let keys_b = keys_b.as_ref().unwrap();

        for ((val_a, dir), (val_b, _)) in keys_a.iter().zip(keys_b.iter()) {
            // Handle NULLs: always sort last regardless of ASC/DESC
            let cmp = match (val_a.is_null(), val_b.is_null()) {
                (true, true) => Ordering::Equal,
                (true, false) => return Ordering::Greater, // NULL always sorts last
                (false, true) => return Ordering::Less,    // non-NULL always sorts first
                (false, false) => {
                    // Compare non-NULL values, respecting direction
                    match dir {
                        vibesql_ast::OrderDirection::Asc => compare_sql_values(val_a, val_b),
                        vibesql_ast::OrderDirection::Desc => compare_sql_values(val_a, val_b).reverse(),
                    }
                }
            };

            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    };

    #[cfg(feature = "parallel")]
    {
        let config = ParallelConfig::global();
        if config.should_parallelize_sort(rows.len()) {
            // Parallel sort for large datasets
            rows.par_sort_by(comparison_fn);
        } else {
            // Sequential sort for small datasets
            rows.sort_by(comparison_fn);
        }
    }

    #[cfg(not(feature = "parallel"))]
    {
        // Always use sequential sort when parallel feature is disabled
        rows.sort_by(comparison_fn);
    }

    Ok(rows)
}

/// Resolve ORDER BY expression that might be a SELECT list alias or column position
///
/// Handles three cases:
/// 1. Numeric literal (e.g., ORDER BY 1, 2, 3) - returns the expression from that position in SELECT list
/// 2. Simple column reference that matches a SELECT list alias - returns the SELECT list expression
/// 3. Otherwise - returns the original ORDER BY expression
fn resolve_order_by_alias<'a>(
    order_expr: &'a vibesql_ast::Expression,
    select_list: &'a [vibesql_ast::SelectItem],
) -> &'a vibesql_ast::Expression {
    // Check for numeric column position (ORDER BY 1, 2, 3, etc.)
    if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(pos)) = order_expr {
        if *pos > 0 && (*pos as usize) <= select_list.len() {
            // Valid column position, return the expression at that position
            let idx = (*pos as usize) - 1;
            if let vibesql_ast::SelectItem::Expression { expr, .. } = &select_list[idx] {
                return expr;
            }
        }
    }

    // Check if ORDER BY expression is a simple column reference (no table qualifier)
    if let vibesql_ast::Expression::ColumnRef { table: None, column } = order_expr {
        // Search for matching alias in SELECT list
        for item in select_list {
            if let vibesql_ast::SelectItem::Expression { expr, alias: Some(alias_name) } = item {
                if alias_name == column {
                    // Found matching alias, use the SELECT list expression
                    return expr;
                }
            }
        }
    }

    // Not an alias or column position, use the original expression
    order_expr
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    /// Test the comparison function logic with pre-evaluated sort keys
    /// This tests the parallel/sequential sorting logic without needing full evaluator setup
    #[test]
    fn test_sort_with_keys_small_dataset() {
        // Small dataset with pre-populated sort keys
        let mut rows: Vec<RowWithSortKeys> = vec![
            (
                Row {
                    values: vec![SqlValue::Integer(3)],
                },
                Some(vec![(SqlValue::Integer(3), vibesql_ast::OrderDirection::Asc)]),
            ),
            (
                Row {
                    values: vec![SqlValue::Integer(1)],
                },
                Some(vec![(SqlValue::Integer(1), vibesql_ast::OrderDirection::Asc)]),
            ),
            (
                Row {
                    values: vec![SqlValue::Integer(2)],
                },
                Some(vec![(SqlValue::Integer(2), vibesql_ast::OrderDirection::Asc)]),
            ),
        ];

        // Apply sorting logic (mimics what apply_order_by does after key evaluation)
        let comparison_fn = |(_, keys_a): &RowWithSortKeys, (_, keys_b): &RowWithSortKeys| {
            let keys_a = keys_a.as_ref().unwrap();
            let keys_b = keys_b.as_ref().unwrap();

            for ((val_a, dir), (val_b, _)) in keys_a.iter().zip(keys_b.iter()) {
                let cmp = match (val_a.is_null(), val_b.is_null()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => return Ordering::Greater,
                    (false, true) => return Ordering::Less,
                    (false, false) => match dir {
                        vibesql_ast::OrderDirection::Asc => compare_sql_values(val_a, val_b),
                        vibesql_ast::OrderDirection::Desc => compare_sql_values(val_a, val_b).reverse(),
                    },
                };

                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
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
                Row {
                    values: vec![SqlValue::Integer(i)],
                },
                Some(vec![(SqlValue::Integer(i), vibesql_ast::OrderDirection::Asc)]),
            ));
        }

        let comparison_fn = |(_, keys_a): &RowWithSortKeys, (_, keys_b): &RowWithSortKeys| {
            let keys_a = keys_a.as_ref().unwrap();
            let keys_b = keys_b.as_ref().unwrap();

            for ((val_a, dir), (val_b, _)) in keys_a.iter().zip(keys_b.iter()) {
                let cmp = match (val_a.is_null(), val_b.is_null()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => return Ordering::Greater,
                    (false, true) => return Ordering::Less,
                    (false, false) => match dir {
                        vibesql_ast::OrderDirection::Asc => compare_sql_values(val_a, val_b),
                        vibesql_ast::OrderDirection::Desc => compare_sql_values(val_a, val_b).reverse(),
                    },
                };

                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
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
                Row {
                    values: vec![SqlValue::Integer(1)],
                },
                Some(vec![(SqlValue::Integer(1), vibesql_ast::OrderDirection::Desc)]),
            ),
            (
                Row {
                    values: vec![SqlValue::Integer(3)],
                },
                Some(vec![(SqlValue::Integer(3), vibesql_ast::OrderDirection::Desc)]),
            ),
            (
                Row {
                    values: vec![SqlValue::Integer(2)],
                },
                Some(vec![(SqlValue::Integer(2), vibesql_ast::OrderDirection::Desc)]),
            ),
        ];

        let comparison_fn = |(_, keys_a): &RowWithSortKeys, (_, keys_b): &RowWithSortKeys| {
            let keys_a = keys_a.as_ref().unwrap();
            let keys_b = keys_b.as_ref().unwrap();

            for ((val_a, dir), (val_b, _)) in keys_a.iter().zip(keys_b.iter()) {
                let cmp = match (val_a.is_null(), val_b.is_null()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => return Ordering::Greater,
                    (false, true) => return Ordering::Less,
                    (false, false) => match dir {
                        vibesql_ast::OrderDirection::Asc => compare_sql_values(val_a, val_b),
                        vibesql_ast::OrderDirection::Desc => compare_sql_values(val_a, val_b).reverse(),
                    },
                };

                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
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

        assert_eq!(rows[0].0.values[0], SqlValue::Integer(3));
        assert_eq!(rows[1].0.values[0], SqlValue::Integer(2));
        assert_eq!(rows[2].0.values[0], SqlValue::Integer(1));
    }

    #[test]
    fn test_sort_with_nulls() {
        // NULLs should always sort last regardless of ASC/DESC
        let mut rows_asc: Vec<RowWithSortKeys> = vec![
            (
                Row {
                    values: vec![SqlValue::Integer(2)],
                },
                Some(vec![(SqlValue::Integer(2), vibesql_ast::OrderDirection::Asc)]),
            ),
            (
                Row {
                    values: vec![SqlValue::Null],
                },
                Some(vec![(SqlValue::Null, vibesql_ast::OrderDirection::Asc)]),
            ),
            (
                Row {
                    values: vec![SqlValue::Integer(1)],
                },
                Some(vec![(SqlValue::Integer(1), vibesql_ast::OrderDirection::Asc)]),
            ),
        ];

        let comparison_fn = |(_, keys_a): &RowWithSortKeys, (_, keys_b): &RowWithSortKeys| {
            let keys_a = keys_a.as_ref().unwrap();
            let keys_b = keys_b.as_ref().unwrap();

            for ((val_a, dir), (val_b, _)) in keys_a.iter().zip(keys_b.iter()) {
                let cmp = match (val_a.is_null(), val_b.is_null()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => return Ordering::Greater,
                    (false, true) => return Ordering::Less,
                    (false, false) => match dir {
                        vibesql_ast::OrderDirection::Asc => compare_sql_values(val_a, val_b),
                        vibesql_ast::OrderDirection::Desc => compare_sql_values(val_a, val_b).reverse(),
                    },
                };

                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
        };

        rows_asc.sort_by(&comparison_fn);

        // ASC: 1, 2, NULL (NULLs last)
        assert_eq!(rows_asc[0].0.values[0], SqlValue::Integer(1));
        assert_eq!(rows_asc[1].0.values[0], SqlValue::Integer(2));
        assert_eq!(rows_asc[2].0.values[0], SqlValue::Null);
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
