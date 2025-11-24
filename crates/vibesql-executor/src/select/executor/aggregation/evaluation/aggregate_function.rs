//! Aggregate function evaluation (COUNT, SUM, AVG, MIN, MAX)

use super::super::super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError, evaluator::CombinedExpressionEvaluator,
    select::grouping::AggregateAccumulator,
};

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
        vibesql_ast::Expression::AggregateFunction { name, distinct, args } => (name, *distinct, args),
        _ => unreachable!("evaluate called with non-aggregate expression"),
    };

    // Generate cache key for this aggregate expression
    // Format: "{name}:{distinct}:{arg_debug}"
    let cache_key = format!("{}:{}:{:?}", name.to_uppercase(), distinct, args);

    // Check cache first
    if let Some(cached_result) = executor.aggregate_cache.borrow().get(&cache_key) {
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
            // Cache the result
            executor.aggregate_cache.borrow_mut().insert(cache_key, result.clone());
            return Ok(result);
        }
    }

    // Regular aggregate - evaluate argument for each row
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedExpression(format!(
            "Aggregate functions expect 1 argument, got {}",
            args.len()
        )));
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
            executor.aggregate_cache.borrow_mut().insert(cache_key, result.clone());
            return Ok(result);
        }
    }

    for row in group_rows {
        // Clear CSE cache before evaluating each row to prevent column values
        // from being incorrectly cached across different rows
        evaluator.clear_cse_cache();

        let value = evaluator.eval(&args[0], row)?;
        acc.accumulate(&value);
    }

    let result = acc.finalize();
    // Cache the result for reuse within this group
    executor.aggregate_cache.borrow_mut().insert(cache_key, result.clone());
    Ok(result)
}
