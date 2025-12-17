//! SELECT without FROM clause execution
//!
//! This module handles the special case of SELECT statements without a FROM clause,
//! which evaluates expressions without any table context.

use super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator},
    select::helpers::{apply_limit_offset, evaluate_limit, evaluate_offset},
};

impl SelectExecutor<'_> {
    /// Execute SELECT without FROM clause
    ///
    /// Evaluates expressions in the SELECT list without any table context.
    /// Returns a single row with the evaluated expressions.
    ///
    /// Example: `SELECT 1 + 1, 'hello', CURRENT_DATE`
    ///
    /// Fix for select1-18.1: When outer context is available (outer_row, outer_schema),
    /// column references can be resolved from the outer scope. This enables queries like:
    /// `SELECT * FROM t2 WHERE x IN (SELECT x)` where the inner `SELECT x` has no FROM
    /// but `x` is resolved from the outer `t2`.
    pub(in crate::select::executor) fn execute_select_without_from(
        &self,
        stmt: &vibesql_ast::SelectStmt,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Validate ORDER BY column positions early (SQLite compatibility)
        if let Some(order_by) = &stmt.order_by {
            for (term_index, order_item) in order_by.iter().enumerate() {
                // Check for numeric column positions
                if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(pos)) =
                    &order_item.expr
                {
                    if *pos <= 0 || (*pos as usize) > stmt.select_list.len() {
                        return Err(ExecutorError::OrderByOutOfRange {
                            term_position: term_index + 1,
                            column_number: *pos,
                            select_list_len: stmt.select_list.len(),
                        });
                    }
                }
                // Check for negative column positions (parsed as UnaryOp { Minus, Integer })
                if let vibesql_ast::Expression::UnaryOp {
                    op: vibesql_ast::UnaryOperator::Minus,
                    expr,
                } = &order_item.expr
                {
                    if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(pos)) =
                        expr.as_ref()
                    {
                        return Err(ExecutorError::OrderByOutOfRange {
                            term_position: term_index + 1,
                            column_number: -*pos,
                            select_list_len: stmt.select_list.len(),
                        });
                    }
                }
            }
        }

        // Check if we have outer context (for correlated subqueries)
        let has_outer_context = self.outer_row.is_some() && self.outer_schema.is_some();

        // Evaluate each item in the SELECT list
        let mut values = Vec::new();
        for item in &stmt.select_list {
            match item {
                vibesql_ast::SelectItem::Wildcard { .. }
                | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * and qualified wildcards require FROM clause".to_string(),
                    ));
                }
                vibesql_ast::SelectItem::Expression { expr, .. } => {
                    // Check if expression references a column
                    // FIX for select1-18.1: Only error if we don't have outer context
                    // When outer context exists, columns can be resolved from outer scope
                    if !has_outer_context && self.expression_references_column(expr) {
                        return Err(ExecutorError::UnsupportedFeature(
                            "Column reference requires FROM clause".to_string(),
                        ));
                    }

                    // Evaluate the expression
                    let value = if has_outer_context {
                        // Use outer context for column resolution
                        let outer_row = self.outer_row.unwrap();
                        let outer_schema = self.outer_schema.unwrap();
                        // Create an empty CombinedSchema for current level (no FROM clause)
                        let empty_combined_schema = crate::schema::CombinedSchema::empty();
                        // Create evaluator that can resolve columns from outer schema
                        let evaluator =
                            CombinedExpressionEvaluator::with_database_and_outer_context(
                                &empty_combined_schema,
                                self.database,
                                outer_row,
                                outer_schema,
                            );
                        evaluator.eval(expr, outer_row)?
                    } else {
                        // No outer context - use simple evaluation
                        let empty_schema = vibesql_catalog::TableSchema::new("".to_string(), vec![]);
                        let evaluator = ExpressionEvaluator::with_database(&empty_schema, self.database);
                        let empty_row = vibesql_storage::Row::new(vec![]);
                        evaluator.eval(expr, &empty_row)?
                    };
                    values.push(value);
                }
            }
        }

        // Build result row
        let result = vec![vibesql_storage::Row::new(values)];

        // Apply LIMIT and OFFSET (important for queries like SELECT 1 LIMIT 0)
        let limit = evaluate_limit(&stmt.limit, self.database)?;
        let offset = evaluate_offset(&stmt.offset, self.database)?;
        Ok(apply_limit_offset(result, limit, offset))
    }
}
