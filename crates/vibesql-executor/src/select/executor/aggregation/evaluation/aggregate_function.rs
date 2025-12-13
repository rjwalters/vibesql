//! Aggregate function evaluation (COUNT, SUM, AVG, MIN, MAX)

use super::super::super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    evaluator::{compiled_case::CompiledCaseExpression, CombinedExpressionEvaluator},
    select::grouping::AggregateAccumulator,
};

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
                vibesql_ast::Expression::ColumnRef { table: None, column } if column == "*"
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
        "GROUP_CONCAT" => {
            // GROUP_CONCAT requires 1 or 2 arguments
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
    // Extract name, distinct, and args from AggregateFunction
    let (name, distinct, args) = match expr {
        vibesql_ast::Expression::AggregateFunction { name, distinct, args } => {
            (name, *distinct, args)
        }
        _ => unreachable!("evaluate called with non-aggregate expression"),
    };

    // Validate argument count first
    validate_aggregate_args(name, args)?;

    // Generate cache key for this aggregate expression
    // Format: "{name}:{distinct}:{arg_debug}"
    let cache_key = format!("{}:{}:{:?}", name.to_uppercase(), distinct, args);

    // Check cache first (lazily initialized)
    if let Some(cached_result) = executor.get_aggregate_cache().borrow().get(&cache_key) {
        return Ok(cached_result.clone());
    }

    let mut acc = AggregateAccumulator::new(name, distinct)?;

    // Special handling for COUNT(*)
    if name.to_uppercase() == "COUNT" && args.len() == 1 {
        let is_count_star = matches!(args[0], vibesql_ast::Expression::Wildcard)
            || matches!(
                &args[0],
                vibesql_ast::Expression::ColumnRef { table: None, column } if column == "*"
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
                function_name: name.clone(),
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

    // Handle GROUP_CONCAT with optional separator (2nd argument)
    // GROUP_CONCAT(expr) - uses comma separator
    // GROUP_CONCAT(expr, separator) - uses custom separator
    if name.to_uppercase() == "GROUP_CONCAT" {
        let separator = if args.len() == 2 {
            // Evaluate separator (second argument) - it should be a constant string
            // Use the first row to evaluate (separator should be the same for all rows)
            if let Some(first_row) = group_rows.first() {
                evaluator.clear_cse_cache();
                let sep_value = evaluator.eval(&args[1], first_row)?;
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
                "GROUP_CONCAT expects 1 or 2 arguments, got {}",
                args.len()
            )));
        };

        let mut acc = AggregateAccumulator::new_with_separator(name, distinct, &separator)?;

        for row in group_rows {
            evaluator.clear_cse_cache();
            let value = evaluator.eval(&args[0], row)?;
            acc.accumulate(&value);
        }

        let result = acc.finalize();
        executor.get_aggregate_cache().borrow_mut().insert(cache_key, result.clone());
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
                vibesql_ast::Expression::ColumnRef { table: None, column } if column == "*"
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
