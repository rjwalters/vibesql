//! Expression evaluation with aggregates for SelectExecutor
//!
//! This module coordinates expression evaluation in the context of aggregation,
//! delegating to specialized handlers for different expression types.

mod aggregate_function;
mod binary_op;
mod case;
mod function;
mod simple;
mod subquery;

use super::super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    evaluator::CombinedExpressionEvaluator,
    select::grouping::GroupingContext,
};

impl SelectExecutor<'_> {
    /// Evaluate an expression in the context of aggregation
    ///
    /// This is the main entry point for expression evaluation during aggregation.
    /// It delegates to specialized handlers based on expression type.
    #[allow(clippy::only_used_in_recursion)]
    pub(in crate::select::executor) fn evaluate_with_aggregates(
        &self,
        expr: &vibesql_ast::Expression,
        group_rows: &[vibesql_storage::Row],
        group_key: &[vibesql_types::SqlValue],
        evaluator: &CombinedExpressionEvaluator,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        match expr {
            // Aggregate functions (AggregateFunction variant only)
            vibesql_ast::Expression::AggregateFunction { .. } => {
                aggregate_function::evaluate(self, expr, group_rows, evaluator)
            }

            // Regular functions (e.g., NULLIF wrapping an aggregate) - may contain aggregates in
            // arguments
            vibesql_ast::Expression::Function { .. } => {
                function::evaluate(self, expr, group_rows, group_key, evaluator)
            }

            // Binary operation - recursively evaluate both sides
            vibesql_ast::Expression::BinaryOp { left, op, right } => {
                binary_op::evaluate_binary(self, left, op, right, group_rows, group_key, evaluator)
            }

            // Unary operations - recursively evaluate inner expression with aggregates
            vibesql_ast::Expression::UnaryOp { op, expr: inner_expr } => {
                binary_op::evaluate_unary(self, op, inner_expr, group_rows, group_key, evaluator)
            }

            // Scalar subquery - delegate to evaluator
            vibesql_ast::Expression::ScalarSubquery(_) | vibesql_ast::Expression::Exists { .. } => {
                subquery::evaluate_scalar(self, expr, group_rows, evaluator)
            }

            // IN with subquery - special handling for aggregate left-hand side
            vibesql_ast::Expression::In { .. } => {
                subquery::evaluate_in(self, expr, group_rows, group_key, evaluator)
            }

            // Quantified comparison - special handling for aggregate left-hand side
            vibesql_ast::Expression::QuantifiedComparison { .. } => {
                subquery::evaluate_quantified(self, expr, group_rows, group_key, evaluator)
            }

            // CASE expression - may contain aggregates in operand, conditions, or results
            vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
                case::evaluate(self, operand, when_clauses, else_result, group_rows, group_key, evaluator)
            }

            // Simple expressions that can potentially contain aggregates
            vibesql_ast::Expression::Cast { .. }
            | vibesql_ast::Expression::Between { .. }
            | vibesql_ast::Expression::InList { .. }
            | vibesql_ast::Expression::Like { .. }
            | vibesql_ast::Expression::IsNull { .. }
            | vibesql_ast::Expression::Position { .. }
            | vibesql_ast::Expression::Trim { .. }
            | vibesql_ast::Expression::Interval { .. } => {
                simple::evaluate(self, expr, group_rows, group_key, evaluator)
            }

            // Truly simple expressions: Literal, ColumnRef (cannot contain aggregates)
            vibesql_ast::Expression::Literal(_)
            | vibesql_ast::Expression::ColumnRef { .. }
            | vibesql_ast::Expression::Wildcard
            | vibesql_ast::Expression::CurrentDate
            | vibesql_ast::Expression::CurrentTime { .. }
            | vibesql_ast::Expression::CurrentTimestamp { .. }
            | vibesql_ast::Expression::Default
            | vibesql_ast::Expression::NextValue { .. }
            | vibesql_ast::Expression::DuplicateKeyValue { .. }
            | vibesql_ast::Expression::PseudoVariable { .. }
            | vibesql_ast::Expression::SessionVariable { .. } => {
                simple::evaluate_no_aggregates(expr, group_rows, evaluator)
            }

            // Window functions and match against require special handling
            vibesql_ast::Expression::WindowFunction { .. } => {
                Err(ExecutorError::UnsupportedExpression(
                    "Window functions not supported in aggregate context".to_string(),
                ))
            }
            vibesql_ast::Expression::MatchAgainst { .. } => {
                Err(ExecutorError::UnsupportedExpression(
                    "MATCH...AGAINST not supported in aggregate context".to_string(),
                ))
            }
        }
    }

    /// Apply ORDER BY to aggregated results
    pub(in crate::select::executor) fn apply_order_by_to_aggregates(
        &self,
        rows: Vec<vibesql_storage::Row>,
        _stmt: &vibesql_ast::SelectStmt,
        order_by: &[vibesql_ast::OrderByItem],
        expanded_select_list: &[vibesql_ast::SelectItem],
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Build a schema from the expanded SELECT list to enable ORDER BY column resolution
        let mut result_columns = Vec::new();
        for (idx, item) in expanded_select_list.iter().enumerate() {
            match item {
                vibesql_ast::SelectItem::Expression { expr, alias } => {
                    let column_name = if let Some(alias) = alias {
                        alias.clone()
                    } else {
                        // Try to extract column name from expression
                        match expr {
                            vibesql_ast::Expression::ColumnRef { column, .. } => column.clone(),
                            vibesql_ast::Expression::AggregateFunction { name, .. } => name.to_lowercase(),
                            _ => format!("col{}", idx + 1),
                        }
                    };
                    result_columns.push(vibesql_catalog::ColumnSchema::new(
                        column_name,
                        vibesql_types::DataType::Varchar { max_length: Some(255) }, // Placeholder type
                        true,
                    ));
                }
                vibesql_ast::SelectItem::Wildcard { .. } | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                    // This should not happen after expansion, but keep for safety
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * and qualified wildcards not supported with aggregates"
                            .to_string(),
                    ));
                }
            }
        }

        let result_table_schema = vibesql_catalog::TableSchema::new("result".to_string(), result_columns);

        // Create a CombinedSchema for the result set
        let mut table_schemas = std::collections::HashMap::new();
        table_schemas.insert("result".to_string(), (0, result_table_schema.clone()));
        let result_schema = crate::schema::CombinedSchema {
            table_schemas,
            total_columns: result_table_schema.columns.len(),
        };

        let result_evaluator = CombinedExpressionEvaluator::with_database(&result_schema, self.database);

        // Evaluate ORDER BY expressions and attach sort keys to rows
        let mut rows_with_keys: Vec<(vibesql_storage::Row, Vec<(vibesql_types::SqlValue, vibesql_ast::OrderDirection)>)> =
            Vec::new();
        for row in rows {
            // Clear CSE cache before evaluating each row to prevent column values
            // from being incorrectly cached across different rows
            result_evaluator.clear_cse_cache();

            let mut sort_keys = Vec::new();
            for order_item in order_by {
                let key_value = result_evaluator.eval(&order_item.expr, &row)?;
                sort_keys.push((key_value, order_item.direction.clone()));
            }
            rows_with_keys.push((row, sort_keys));
        }

        // Sort using the sort keys
        rows_with_keys.sort_by(|(_, keys_a), (_, keys_b)| {
            use crate::select::grouping::compare_sql_values;

            for ((val_a, dir), (val_b, _)) in keys_a.iter().zip(keys_b.iter()) {
                let cmp = match dir {
                    vibesql_ast::OrderDirection::Asc => compare_sql_values(val_a, val_b),
                    vibesql_ast::OrderDirection::Desc => compare_sql_values(val_a, val_b).reverse(),
                };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        // Extract rows without sort keys
        Ok(rows_with_keys.into_iter().map(|(row, _)| row).collect())
    }

    /// Evaluate an expression in the context of aggregation with grouping context
    ///
    /// This is used for ROLLUP/CUBE/GROUPING SETS to support the GROUPING() function
    /// and to return NULL for columns that are rolled up in the current grouping set.
    #[allow(clippy::only_used_in_recursion)]
    pub(in crate::select::executor) fn evaluate_with_aggregates_and_grouping(
        &self,
        expr: &vibesql_ast::Expression,
        group_rows: &[vibesql_storage::Row],
        group_key: &[vibesql_types::SqlValue],
        evaluator: &CombinedExpressionEvaluator,
        grouping_context: &GroupingContext,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Check for GROUPING() function call
        if let vibesql_ast::Expression::Function { name, args, .. } = expr {
            if name.eq_ignore_ascii_case("GROUPING") {
                // GROUPING() function - returns 1 if the column is rolled up, 0 otherwise
                if args.len() != 1 {
                    return Err(ExecutorError::UnsupportedExpression(format!(
                        "GROUPING() requires exactly 1 argument, got {}",
                        args.len()
                    )));
                }
                let result = grouping_context.is_rolled_up(&args[0]);
                return Ok(vibesql_types::SqlValue::Integer(result as i64));
            }
        }

        // For column references, check if the column is rolled up
        // If rolled up, return NULL instead of the actual value
        if let vibesql_ast::Expression::ColumnRef { .. } = expr {
            if grouping_context.is_rolled_up(expr) == 1 {
                return Ok(vibesql_types::SqlValue::Null);
            }
        }

        // For other expressions, delegate to the existing evaluation
        self.evaluate_with_aggregates(expr, group_rows, group_key, evaluator)
    }

    /// Check if a SQL value is truthy (for HAVING clause evaluation)
    pub(in crate::select::executor) fn is_truthy(
        &self,
        value: &vibesql_types::SqlValue,
    ) -> Result<bool, ExecutorError> {
        match value {
            vibesql_types::SqlValue::Boolean(true) => Ok(true),
            vibesql_types::SqlValue::Boolean(false) | vibesql_types::SqlValue::Null => Ok(false),
            // SQLLogicTest compatibility: treat integers as truthy/falsy (C-like behavior)
            vibesql_types::SqlValue::Integer(0) => Ok(false),
            vibesql_types::SqlValue::Integer(_) => Ok(true),
            vibesql_types::SqlValue::Smallint(0) => Ok(false),
            vibesql_types::SqlValue::Smallint(_) => Ok(true),
            vibesql_types::SqlValue::Bigint(0) => Ok(false),
            vibesql_types::SqlValue::Bigint(_) => Ok(true),
            vibesql_types::SqlValue::Float(0.0) => Ok(false),
            vibesql_types::SqlValue::Float(_) => Ok(true),
            vibesql_types::SqlValue::Real(0.0) => Ok(false),
            vibesql_types::SqlValue::Real(_) => Ok(true),
            vibesql_types::SqlValue::Double(0.0) => Ok(false),
            vibesql_types::SqlValue::Double(_) => Ok(true),
            other => Err(ExecutorError::InvalidWhereClause(format!(
                "HAVING must evaluate to boolean, got: {:?}",
                other
            ))),
        }
    }
}
