//! Aggregate function evaluation (COUNT, SUM, AVG, MIN, MAX)

use super::super::super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    evaluator::{compiled_case::CompiledCaseExpression, CombinedExpressionEvaluator},
    select::grouping::{compare_sql_values, AggregateAccumulator},
};

/// Sort key type: (sort_value, is_descending, nulls_first)
type SortKey = (vibesql_types::SqlValue, bool, Option<bool>);

/// Compare two sets of sort keys with collation and NULLS FIRST/LAST support.
/// This is a helper function to eliminate code duplication between GROUP_CONCAT and JSON_GROUP_ARRAY.
fn compare_sort_keys(
    a_keys: &[SortKey],
    b_keys: &[SortKey],
    collations: &[Option<String>],
) -> std::cmp::Ordering {
    for (idx, ((val_a, desc_a, nulls_first_a), (val_b, _, _))) in
        a_keys.iter().zip(b_keys.iter()).enumerate()
    {
        // Handle NULLs with explicit ordering
        let (a_null, b_null) = (val_a.is_null(), val_b.is_null());
        if a_null || b_null {
            if a_null && b_null {
                continue; // Both NULL, equal for this key
            }
            // Determine if NULLs should come first
            // Default: NULLS LAST for ASC, NULLS FIRST for DESC (SQLite behavior)
            let nulls_first = nulls_first_a.unwrap_or(*desc_a);
            if a_null {
                return if nulls_first {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                };
            } else {
                return if nulls_first {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                };
            }
        }

        // Use collation-aware comparison if collation specified
        let cmp = if let Some(Some(collation)) = collations.get(idx) {
            crate::select::grouping::compare_sql_values_with_collation(
                val_a,
                val_b,
                Some(collation.as_str()),
            )
        } else {
            compare_sql_values(val_a, val_b)
        };

        if cmp != std::cmp::Ordering::Equal {
            return if *desc_a { cmp.reverse() } else { cmp };
        }
    }
    std::cmp::Ordering::Equal
}

/// Extract collations from ORDER BY expressions (if wrapped in Collate).
fn extract_collations(order_items: &[vibesql_ast::OrderByItem]) -> Vec<Option<String>> {
    order_items
        .iter()
        .map(|item| {
            if let vibesql_ast::Expression::Collate { collation, .. } = &item.expr {
                Some(collation.clone())
            } else {
                None
            }
        })
        .collect()
}

/// Validate aggregate function argument count
/// Returns error with SQLite-compatible message if validation fails
fn validate_aggregate_args(name: &str, args: &[vibesql_ast::Expression]) -> Result<(), ExecutorError> {
    let name_upper = name.to_uppercase();
    let arg_count = args.len();

    // Check for wildcard in non-COUNT aggregates
    let has_wildcard = args.iter().any(|arg| {
        matches!(arg, vibesql_ast::Expression::Wildcard)
            || matches!(
                arg,
                vibesql_ast::Expression::ColumnRef(col_id) if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() && col_id.column_canonical() == "*"
            )
    });

    match name_upper.as_str() {
        "COUNT" => {
            // COUNT allows 0 args (COUNT(*) is handled specially), 1 arg, or multiple with DISTINCT
            // COUNT() with no args and no * is still allowed
            Ok(())
        }
        "MIN" | "MAX" => {
            // min() and max() with * is wrong number of args
            if has_wildcard || arg_count == 0 {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: name.to_string(), // Preserve original case
                });
            }
            // min/max with > 1 arg becomes scalar function, handled elsewhere
            Ok(())
        }
        "SUM" | "AVG" | "TOTAL" => {
            // These require exactly 1 argument, no wildcard allowed
            if has_wildcard || arg_count == 0 {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: name.to_string(), // Preserve original case
                });
            }
            if arg_count > 1 {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: name.to_string(), // Preserve original case
                });
            }
            Ok(())
        }
        "GROUP_CONCAT" | "STRING_AGG" => {
            // GROUP_CONCAT/STRING_AGG requires 1 or 2 arguments
            if arg_count == 0 || arg_count > 2 {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: name.to_string(), // Preserve original case
                });
            }
            Ok(())
        }
        _ => {
            // Unknown aggregate functions require at least 1 argument
            if arg_count == 0 {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: name.to_string(), // Preserve original case
                });
            }
            Ok(())
        }
    }
}

/// Evaluate aggregate function expressions (COUNT, SUM, AVG, MIN, MAX)
/// Only handles AggregateFunction variant
pub(super) fn evaluate(
    executor: &SelectExecutor,
    expr: &vibesql_ast::Expression,
    group_rows: &[vibesql_storage::Row],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    // Extract name, distinct, args, and order_by from AggregateFunction
    let (name, distinct, args, order_by) = match expr {
        vibesql_ast::Expression::AggregateFunction { name, distinct, args, order_by } => {
            (name, *distinct, args, order_by)
        }
        _ => unreachable!("evaluate called with non-aggregate expression"),
    };

    // Validate argument count first
    validate_aggregate_args(name.canonical(), args)?;

    // Generate cache key for this aggregate expression
    // Format: "{name}:{distinct}:{arg_debug}:{order_by_debug}"
    // Include order_by to distinguish aggregates with different ORDER BY clauses
    let cache_key = format!("{}:{}:{:?}:{:?}", name.to_uppercase(), distinct, args, order_by);

    // Check cache first (lazily initialized)
    if let Some(cached_result) = executor.get_aggregate_cache().borrow().get(&cache_key) {
        return Ok(cached_result.clone());
    }

    let mut acc = AggregateAccumulator::new(name.canonical(), distinct)?;

    // Special handling for COUNT(*)
    if name.to_uppercase() == "COUNT" && args.len() == 1 {
        let is_count_star = matches!(args[0], vibesql_ast::Expression::Wildcard)
            || matches!(
                &args[0],
                vibesql_ast::Expression::ColumnRef(col_id) if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() && col_id.column_canonical() == "*"
            );

        if is_count_star {
            // COUNT(*) - count all rows (DISTINCT not allowed with *)
            if distinct {
                return Err(ExecutorError::UnsupportedExpression(
                    "COUNT(DISTINCT *) is not valid SQL".to_string(),
                ));
            }
            // Fast path: COUNT(*) without DISTINCT is just row count (O(1) vs O(n))
            let result = vibesql_types::SqlValue::Integer(group_rows.len() as i64);
            // Cache the result (lazily initialized)
            executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
            return Ok(result);
        }
    }

    // Handle multi-argument COUNT(DISTINCT a, b, ...) - SQLite extension
    // This counts distinct combinations of values
    if name.to_uppercase() == "COUNT" && args.len() > 1 {
        if !distinct {
            // SQLite-compatible error message
            return Err(ExecutorError::WrongNumberOfArguments {
                function_name: name.to_string(),
            });
        }

        // Evaluate all arguments for each row and accumulate as tuples
        for row in group_rows {
            evaluator.clear_cse_cache();

            let mut tuple_values = Vec::with_capacity(args.len());
            for arg in args {
                let value = evaluator.eval(arg, row)?;
                tuple_values.push(value);
            }
            acc.accumulate_tuple(tuple_values);
        }

        let result = acc.finalize();
        executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
        return Ok(result);
    }

    // Handle GROUP_CONCAT/STRING_AGG with optional separator (2nd argument)
    // GROUP_CONCAT(expr) - uses comma separator
    // GROUP_CONCAT(expr, separator) - uses custom separator
    // GROUP_CONCAT(expr ORDER BY ...) - sorted concatenation
    // STRING_AGG is an alias for GROUP_CONCAT (SQLite 3.44+)
    let name_upper = name.to_uppercase();
    if name_upper == "GROUP_CONCAT" || name_upper == "STRING_AGG" {
        let separator = if args.len() == 2 {
            // Evaluate separator (second argument)
            // SQLite uses the LAST row's value when separator is an expression
            // (e.g., when separator is a column reference like b1 in group_concat(a1, b1))
            // Fix for aggnested-1.4 test
            if let Some(last_row) = group_rows.last() {
                evaluator.clear_cse_cache();
                let sep_value = evaluator.eval(&args[1], last_row)?;
                match sep_value {
                    vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                        s.to_string()
                    }
                    vibesql_types::SqlValue::Null => String::new(), // NULL separator = empty string (SQLite behavior)
                    other => other.to_string(), // Convert other types to string
                }
            } else {
                ",".to_string() // Empty group, use default
            }
        } else if args.len() == 1 {
            ",".to_string() // Default separator
        } else {
            return Err(ExecutorError::UnsupportedExpression(format!(
                "{} expects 1 or 2 arguments, got {}",
                name, args.len()
            )));
        };

        // Handle ORDER BY clause within the aggregate
        if let Some(order_items) = order_by {
            let collations = extract_collations(order_items);

            // Collect (value, sort_keys, row_index) for each row
            // We need row_index to find the last row after sorting for separator evaluation
            let mut value_sort_pairs: Vec<(vibesql_types::SqlValue, Vec<SortKey>, usize)> =
                Vec::with_capacity(group_rows.len());

            for (row_idx, row) in group_rows.iter().enumerate() {
                evaluator.clear_cse_cache();
                let value = evaluator.eval(&args[0], row)?;

                // Skip NULL values for GROUP_CONCAT
                if matches!(value, vibesql_types::SqlValue::Null) {
                    continue;
                }

                // Evaluate sort keys
                let mut sort_keys = Vec::with_capacity(order_items.len());
                for item in order_items {
                    let sort_value = evaluator.eval(&item.expr, row)?;
                    let is_desc = item.direction == vibesql_ast::OrderDirection::Desc;
                    let nulls_first = item
                        .nulls_order
                        .as_ref()
                        .map(|no| matches!(no, vibesql_ast::NullsOrder::First));
                    sort_keys.push((sort_value, is_desc, nulls_first));
                }

                value_sort_pairs.push((value, sort_keys, row_idx));
            }

            // Sort using the helper function
            value_sort_pairs.sort_by(|a, b| compare_sort_keys(&a.1, &b.1, &collations));

            // For ORDER BY, get separator from the last row after sorting
            let final_separator = if args.len() == 2 && !value_sort_pairs.is_empty() {
                let last_row_idx = value_sort_pairs.last().map(|(_, _, idx)| *idx).unwrap_or(0);
                let last_row = &group_rows[last_row_idx];
                evaluator.clear_cse_cache();
                let sep_value = evaluator.eval(&args[1], last_row)?;
                match sep_value {
                    vibesql_types::SqlValue::Varchar(s)
                    | vibesql_types::SqlValue::Character(s) => s.to_string(),
                    vibesql_types::SqlValue::Null => String::new(),
                    other => other.to_string(),
                }
            } else {
                separator.clone()
            };

            // Now accumulate in sorted order
            let mut acc = AggregateAccumulator::new_with_separator(
                name.canonical(),
                distinct,
                &final_separator,
            )?;
            for (value, _, _) in value_sort_pairs {
                acc.accumulate(&value);
            }

            let result = acc.finalize();
            executor
                .get_aggregate_cache()
                .borrow_mut()
                .insert(cache_key, result.clone());
            return Ok(result);
        }

        // No ORDER BY - use the original path
        let mut acc = AggregateAccumulator::new_with_separator(name.canonical(), distinct, &separator)?;

        for row in group_rows {
            evaluator.clear_cse_cache();
            let value = evaluator.eval(&args[0], row)?;
            acc.accumulate(&value);
        }

        let result = acc.finalize();
        executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
        return Ok(result);
    }

    // Handle JSON_GROUP_ARRAY with optional ORDER BY
    if name_upper == "JSON_GROUP_ARRAY" {
        if args.len() != 1 {
            return Err(ExecutorError::WrongNumberOfArguments {
                function_name: name.to_string(),
            });
        }

        // Handle ORDER BY clause within the aggregate
        if let Some(order_items) = order_by {
            let collations = extract_collations(order_items);

            // Collect (value, sort_keys) pairs for each row
            let mut value_sort_pairs: Vec<(vibesql_types::SqlValue, Vec<SortKey>)> =
                Vec::with_capacity(group_rows.len());

            for row in group_rows {
                evaluator.clear_cse_cache();
                let value = evaluator.eval(&args[0], row)?;

                // JSON_GROUP_ARRAY includes NULL values (unlike GROUP_CONCAT)

                // Evaluate sort keys
                let mut sort_keys = Vec::with_capacity(order_items.len());
                for item in order_items {
                    let sort_value = evaluator.eval(&item.expr, row)?;
                    let is_desc = item.direction == vibesql_ast::OrderDirection::Desc;
                    let nulls_first = item
                        .nulls_order
                        .as_ref()
                        .map(|no| matches!(no, vibesql_ast::NullsOrder::First));
                    sort_keys.push((sort_value, is_desc, nulls_first));
                }

                value_sort_pairs.push((value, sort_keys));
            }

            // Sort using the helper function
            value_sort_pairs.sort_by(|a, b| compare_sort_keys(&a.1, &b.1, &collations));

            // Now accumulate in sorted order
            let mut acc = AggregateAccumulator::new(name.canonical(), distinct)?;
            for (value, _) in value_sort_pairs {
                acc.accumulate(&value);
            }

            let result = acc.finalize();
            executor
                .get_aggregate_cache()
                .borrow_mut()
                .insert(cache_key, result.clone());
            return Ok(result);
        }

        // No ORDER BY - use the standard path
        let mut acc = AggregateAccumulator::new(name.canonical(), distinct)?;

        for row in group_rows {
            evaluator.clear_cse_cache();
            let value = evaluator.eval(&args[0], row)?;
            acc.accumulate(&value);
        }

        let result = acc.finalize();
        executor
            .get_aggregate_cache()
            .borrow_mut()
            .insert(cache_key, result.clone());
        return Ok(result);
    }

    // Regular aggregate - evaluate single argument for each row
    // This should be caught by validate_aggregate_args above, but keep as safety net
    if args.len() != 1 {
        return Err(ExecutorError::WrongNumberOfArguments {
            function_name: name.to_string(), // Preserve original case
        });
    }

    // Special handling for COUNT with any argument
    // For COUNT, we need to evaluate the expression and count non-NULL results
    // However, COUNT(*) should count ALL rows regardless of NULL values
    if name.to_uppercase() == "COUNT" {
        // Double-check for COUNT(*) with various representations
        // This handles cases where the wildcard might not be caught by the fast path above
        let is_count_star_fallback = matches!(&args[0], vibesql_ast::Expression::Wildcard)
            || matches!(
                &args[0],
                vibesql_ast::Expression::ColumnRef(col_id) if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() && col_id.column_canonical() == "*"
            );

        if is_count_star_fallback {
            // COUNT(*) fallback: just count all rows
            let result = vibesql_types::SqlValue::Integer(group_rows.len() as i64);
            executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
            return Ok(result);
        }
    }

    // Try to compile CASE expression for fast-path evaluation (#3079)
    // This optimization helps TPC-DS Q2 which has 7 SUM(CASE...) aggregates
    // For ~14K rows × 7 aggregates = ~98K evaluations, compiled CASE avoids:
    // - CSE cache clearing overhead per row
    // - Expression tree traversal
    // - Dynamic dispatch through evaluator
    // Provides ~5-10% improvement for CASE-heavy GROUP BY queries
    let compiled_case = if matches!(&args[0], vibesql_ast::Expression::Case { .. }) {
        CompiledCaseExpression::try_compile(&args[0], evaluator.schema())
    } else {
        None
    };

    if let Some(ref compiled) = compiled_case {
        // Fast path: use compiled CASE expression (no CSE cache, no expression traversal)
        for row in group_rows {
            let value = compiled.evaluate(row);
            acc.accumulate(&value);
        }
    } else {
        // Slow path: full expression evaluation
        for row in group_rows {
            // Clear CSE cache before evaluating each row to prevent column values
            // from being incorrectly cached across different rows
            evaluator.clear_cse_cache();

            let value = evaluator.eval(&args[0], row)?;
            acc.accumulate(&value);
        }
    }

    let result = acc.finalize();
    // Cache the result for reuse within this group (lazily initialized)
    executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
    Ok(result)
}
