//! ORDER BY sorting logic

use std::borrow::Cow;
use std::cmp::Ordering;

#[cfg(feature = "parallel")]
use rayon::slice::ParallelSliceMut;

use super::grouping::compare_sql_values;
#[cfg(feature = "parallel")]
use super::parallel::ParallelConfig;
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
/// - Original column names that have been aliased in SELECT
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
            // Check if ORDER BY expression is a SELECT list alias or matches an aliased column
            // Evaluator handles window functions via window_mapping if present
            let expr_to_eval = resolve_order_by_alias(&order_item.expr, select_list);
            let key_value = evaluator.eval(expr_to_eval.as_ref(), row)?;
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
                        vibesql_ast::OrderDirection::Desc => {
                            compare_sql_values(val_a, val_b).reverse()
                        }
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

/// Resolve ORDER BY expression for aggregate results to result schema column names
///
/// For aggregate queries, the result schema has columns named by their aliases.
/// This function maps ORDER BY expressions to ColumnRef expressions that can be
/// evaluated against the result schema.
///
/// Handles cases:
/// 1. Numeric position (ORDER BY 1) - returns ColumnRef to the alias/column at that position
/// 2. Alias name (ORDER BY alias) - returns ColumnRef to that alias
/// 3. Original column name (ORDER BY col where col is aliased to alias) - returns ColumnRef to alias
/// 4. Complex expressions containing GROUPING() - recursively resolves sub-expressions
/// 5. Otherwise - returns the original expression (for expressions not matching aliases)
pub(crate) fn resolve_order_by_for_aggregates(
    order_expr: &vibesql_ast::Expression,
    select_list: &[vibesql_ast::SelectItem],
) -> vibesql_ast::Expression {
    // Check for numeric column position (ORDER BY 1, 2, 3, etc.)
    if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(pos)) = order_expr {
        if *pos > 0 && (*pos as usize) <= select_list.len() {
            let idx = (*pos as usize) - 1;
            if let vibesql_ast::SelectItem::Expression { expr, alias } = &select_list[idx] {
                // Return a ColumnRef to the alias name (or derive from expression)
                let col_name = if let Some(alias_name) = alias {
                    alias_name.clone()
                } else if let vibesql_ast::Expression::ColumnRef { column, .. } = expr {
                    column.clone()
                } else {
                    format!("col{}", idx + 1)
                };
                return vibesql_ast::Expression::ColumnRef { table: None, column: col_name };
            }
        }
    }

    // Check if ORDER BY expression is a simple column reference (no table qualifier)
    if let vibesql_ast::Expression::ColumnRef { table: None, column } = order_expr {
        // First, check if column matches an alias name - return ColumnRef to that alias
        for item in select_list {
            if let vibesql_ast::SelectItem::Expression { alias: Some(alias_name), .. } = item {
                if alias_name.eq_ignore_ascii_case(column) {
                    // ORDER BY uses alias name, return ColumnRef to that alias
                    return vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: alias_name.clone(),
                    };
                }
            }
        }

        // Second, check if column matches an original column name that has an alias
        for item in select_list {
            if let vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef { column: select_col, .. },
                alias: Some(alias_name),
            } = item
            {
                if select_col.eq_ignore_ascii_case(column) {
                    // ORDER BY uses original column name, return ColumnRef to the alias
                    return vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: alias_name.clone(),
                    };
                }
            }
        }

        // Third, check if column matches a SELECT list column without alias
        for item in select_list {
            if let vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef { column: select_col, .. },
                alias: None,
            } = item
            {
                if select_col.eq_ignore_ascii_case(column) {
                    // Found matching non-aliased column, return as-is
                    return order_expr.clone();
                }
            }
        }
    }

    // IMPORTANT: Check if the ENTIRE expression matches any SELECT list expression FIRST
    // This handles cases like GROUPING(a) + GROUPING(b) matching an alias like "lochierarchy"
    // before we try to recursively decompose the expression
    if let Some(alias) = find_matching_select_expression(order_expr, select_list) {
        return vibesql_ast::Expression::ColumnRef { table: None, column: alias };
    }

    // Handle CASE expressions by recursively resolving sub-expressions
    if let vibesql_ast::Expression::Case { operand, when_clauses, else_result } = order_expr {
        let resolved_operand =
            operand.as_ref().map(|op| Box::new(resolve_order_by_for_aggregates(op, select_list)));

        let resolved_when_clauses: Vec<vibesql_ast::CaseWhen> = when_clauses
            .iter()
            .map(|clause| vibesql_ast::CaseWhen {
                conditions: clause
                    .conditions
                    .iter()
                    .map(|cond| resolve_order_by_for_aggregates(cond, select_list))
                    .collect(),
                result: resolve_order_by_for_aggregates(&clause.result, select_list),
            })
            .collect();

        let resolved_else =
            else_result.as_ref().map(|e| Box::new(resolve_order_by_for_aggregates(e, select_list)));

        return vibesql_ast::Expression::Case {
            operand: resolved_operand,
            when_clauses: resolved_when_clauses,
            else_result: resolved_else,
        };
    }

    // Handle BinaryOp expressions by recursively resolving sub-expressions
    // Try to match the entire binary expression first (already done above with find_matching_select_expression)
    // If no match, try matching each side separately
    if let vibesql_ast::Expression::BinaryOp { left, op, right } = order_expr {
        let resolved_left = resolve_order_by_for_aggregates(left, select_list);
        let resolved_right = resolve_order_by_for_aggregates(right, select_list);

        return vibesql_ast::Expression::BinaryOp {
            left: Box::new(resolved_left),
            op: *op,
            right: Box::new(resolved_right),
        };
    }

    // Handle Function calls (including GROUPING) by checking if they match a SELECT expression
    // This is already handled by find_matching_select_expression above, but keep as fallback
    if let vibesql_ast::Expression::Function { name, .. } = order_expr {
        if name.eq_ignore_ascii_case("GROUPING") || name.eq_ignore_ascii_case("GROUPING_ID") {
            // Try to find a matching GROUPING expression in the SELECT list
            if let Some(alias) = find_matching_select_expression(order_expr, select_list) {
                return vibesql_ast::Expression::ColumnRef { table: None, column: alias };
            }
        }
    }

    // Not an alias or column position, return the original expression
    order_expr.clone()
}

/// Find a matching expression in the SELECT list and return its alias or generated column name
fn find_matching_select_expression(
    expr: &vibesql_ast::Expression,
    select_list: &[vibesql_ast::SelectItem],
) -> Option<String> {
    for (idx, item) in select_list.iter().enumerate() {
        if let vibesql_ast::SelectItem::Expression { expr: select_expr, alias } = item {
            if expressions_equal(expr, select_expr) {
                // Found matching expression
                return Some(if let Some(alias_name) = alias {
                    alias_name.clone()
                } else if let vibesql_ast::Expression::ColumnRef { column, .. } = select_expr {
                    column.clone()
                } else {
                    format!("col{}", idx + 1)
                });
            }
        }
    }
    None
}

/// Check if two expressions are structurally equal (for matching ORDER BY expressions to SELECT list)
fn expressions_equal(a: &vibesql_ast::Expression, b: &vibesql_ast::Expression) -> bool {
    match (a, b) {
        (
            vibesql_ast::Expression::ColumnRef { table: t1, column: c1 },
            vibesql_ast::Expression::ColumnRef { table: t2, column: c2 },
        ) => t1 == t2 && c1.eq_ignore_ascii_case(c2),

        (vibesql_ast::Expression::Literal(v1), vibesql_ast::Expression::Literal(v2)) => v1 == v2,

        (
            vibesql_ast::Expression::BinaryOp { left: l1, op: o1, right: r1 },
            vibesql_ast::Expression::BinaryOp { left: l2, op: o2, right: r2 },
        ) => o1 == o2 && expressions_equal(l1, l2) && expressions_equal(r1, r2),

        (
            vibesql_ast::Expression::Function { name: n1, args: a1, .. },
            vibesql_ast::Expression::Function { name: n2, args: a2, .. },
        ) => {
            n1.eq_ignore_ascii_case(n2)
                && a1.len() == a2.len()
                && a1.iter().zip(a2.iter()).all(|(x, y)| expressions_equal(x, y))
        }

        // For other expression types, use debug representation comparison as fallback
        // This is not perfect but handles most common cases
        _ => format!("{:?}", a) == format!("{:?}", b),
    }
}

/// Resolve ORDER BY expression that might be a SELECT list alias or column position
///
/// Handles four cases:
/// 1. Numeric literal (e.g., ORDER BY 1, 2, 3) - returns the expression from that position in SELECT list
/// 2. Simple column reference that matches a SELECT list alias - returns the SELECT list expression
/// 3. Simple column reference that matches an aliased column's original name - returns a ColumnRef to the alias
/// 4. Otherwise - returns the original ORDER BY expression
pub(crate) fn resolve_order_by_alias<'a>(
    order_expr: &'a vibesql_ast::Expression,
    select_list: &'a [vibesql_ast::SelectItem],
) -> Cow<'a, vibesql_ast::Expression> {
    // Check for numeric column position (ORDER BY 1, 2, 3, etc.)
    if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(pos)) = order_expr {
        if *pos > 0 && (*pos as usize) <= select_list.len() {
            // Valid column position, return the expression at that position
            let idx = (*pos as usize) - 1;
            if let vibesql_ast::SelectItem::Expression { expr, .. } = &select_list[idx] {
                return Cow::Borrowed(expr);
            }
        }
    }

    // Check if ORDER BY expression is a simple column reference (no table qualifier)
    if let vibesql_ast::Expression::ColumnRef { table: None, column } = order_expr {
        // First, search for matching alias in SELECT list (ORDER BY using alias name)
        for item in select_list {
            if let vibesql_ast::SelectItem::Expression { expr, alias: Some(alias_name) } = item {
                if alias_name.eq_ignore_ascii_case(column) {
                    // Found matching alias, use the SELECT list expression
                    return Cow::Borrowed(expr);
                }
            }
        }

        // Second, check if column matches a SELECT list expression that has an alias
        // This handles: SELECT col AS alias ... ORDER BY col
        // In this case, we need to reference by the alias since that's what the result schema uses
        for item in select_list {
            // Check if the SELECT expression is a column reference to the same column
            if let vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef { column: select_col, .. },
                alias: Some(alias_name),
            } = item
            {
                if select_col.eq_ignore_ascii_case(column) {
                    // The ORDER BY column matches the original column, but it's aliased
                    // Return a new ColumnRef using the alias name
                    return Cow::Owned(vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: alias_name.clone(),
                    });
                }
            }
        }
    }

    // Not an alias or column position, use the original expression
    Cow::Borrowed(order_expr)
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
                Row::from_vec(vec![SqlValue::Integer(3)]),
                Some(vec![(SqlValue::Integer(3), vibesql_ast::OrderDirection::Asc)]),
            ),
            (
                Row::from_vec(vec![SqlValue::Integer(1)]),
                Some(vec![(SqlValue::Integer(1), vibesql_ast::OrderDirection::Asc)]),
            ),
            (
                Row::from_vec(vec![SqlValue::Integer(2)]),
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
                        vibesql_ast::OrderDirection::Desc => {
                            compare_sql_values(val_a, val_b).reverse()
                        }
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
                Row::from_vec(vec![SqlValue::Integer(i)]),
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
                        vibesql_ast::OrderDirection::Desc => {
                            compare_sql_values(val_a, val_b).reverse()
                        }
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
                Row::from_vec(vec![SqlValue::Integer(1)]),
                Some(vec![(SqlValue::Integer(1), vibesql_ast::OrderDirection::Desc)]),
            ),
            (
                Row::from_vec(vec![SqlValue::Integer(3)]),
                Some(vec![(SqlValue::Integer(3), vibesql_ast::OrderDirection::Desc)]),
            ),
            (
                Row::from_vec(vec![SqlValue::Integer(2)]),
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
                        vibesql_ast::OrderDirection::Desc => {
                            compare_sql_values(val_a, val_b).reverse()
                        }
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
                Row::from_vec(vec![SqlValue::Integer(2)]),
                Some(vec![(SqlValue::Integer(2), vibesql_ast::OrderDirection::Asc)]),
            ),
            (
                Row::from_vec(vec![SqlValue::Null]),
                Some(vec![(SqlValue::Null, vibesql_ast::OrderDirection::Asc)]),
            ),
            (
                Row::from_vec(vec![SqlValue::Integer(1)]),
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
                        vibesql_ast::OrderDirection::Desc => {
                            compare_sql_values(val_a, val_b).reverse()
                        }
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
