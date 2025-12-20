//! Regular function evaluation in aggregate context

use super::super::super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator},
};

/// Evaluate regular functions that may contain aggregate arguments
/// Handles backwards compatibility where Function variant contains aggregates
pub(super) fn evaluate(
    executor: &SelectExecutor,
    expr: &vibesql_ast::Expression,
    group_rows: &[vibesql_storage::Row],
    group_key: &[vibesql_types::SqlValue],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    let (name, args, character_unit) = match expr {
        vibesql_ast::Expression::Function { name, args, character_unit } => {
            (name, args, character_unit)
        }
        _ => unreachable!("evaluate called with non-function expression"),
    };

    // Check if this is actually an aggregate function (backwards compatibility)
    // Note: MIN/MAX with >1 argument are scalar functions (SQLite compatibility)
    let name_upper = name.to_uppercase();
    let is_aggregate = match name_upper.as_str() {
        "COUNT" | "SUM" | "AVG" => true,
        "MIN" | "MAX" => args.len() <= 1, // multi-arg min/max are scalar functions
        _ => false,
    };
    if is_aggregate {
        // Convert to AggregateFunction variant and evaluate
        let agg_expr = vibesql_ast::Expression::AggregateFunction {
            name: name.clone(),
            distinct: false,
            args: args.clone(),
            order_by: None,
            filter: None,
        };
        return super::aggregate_function::evaluate(executor, &agg_expr, group_rows, evaluator);
    }

    // Evaluate function arguments (which may contain aggregates)
    let evaluated_args: Result<Vec<_>, _> = args
        .iter()
        .map(|arg| executor.evaluate_with_aggregates(arg, group_rows, group_key, evaluator))
        .collect();
    let evaluated_args = evaluated_args?;

    // Create a temporary row with the evaluated arguments as its values
    let temp_row = vibesql_storage::Row::new(evaluated_args);

    // Build a new function call expression with the evaluated values as literal constants
    let literal_args: Vec<vibesql_ast::Expression> =
        temp_row.values.iter().map(|val| vibesql_ast::Expression::Literal(val.clone())).collect();
    let new_func_expr = vibesql_ast::Expression::Function {
        name: name.clone(),
        args: literal_args,
        character_unit: character_unit.clone(),
    };

    // Evaluate the function with literal arguments
    if let Some(first_row) = group_rows.first() {
        evaluator.eval(&new_func_expr, first_row)
    } else {
        // No rows - evaluate function with empty context
        let temp_schema = vibesql_catalog::TableSchema::new("temp".to_string(), vec![]);
        let temp_evaluator = ExpressionEvaluator::with_database(&temp_schema, executor.database);
        temp_evaluator.eval(&new_func_expr, &vibesql_storage::Row::new(vec![]))
    }
}
