//! SELECT without FROM clause execution
//!
//! This module handles the special case of SELECT statements without a FROM clause,
//! which evaluates expressions without any table context.

use super::builder::SelectExecutor;
use crate::{errors::ExecutorError, evaluator::ExpressionEvaluator};

impl SelectExecutor<'_> {
    /// Execute SELECT without FROM clause
    ///
    /// Evaluates expressions in the SELECT list without any table context.
    /// Returns a single row with the evaluated expressions.
    ///
    /// Example: `SELECT 1 + 1, 'hello', CURRENT_DATE`
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

        // Create an empty schema (no table context)
        let empty_schema = vibesql_catalog::TableSchema::new("".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::with_database(&empty_schema, self.database);

        // Create an empty row (no data to reference)
        let empty_row = vibesql_storage::Row::new(vec![]);

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
                    if self.expression_references_column(expr) {
                        return Err(ExecutorError::UnsupportedFeature(
                            "Column reference requires FROM clause".to_string(),
                        ));
                    }

                    // Evaluate the expression
                    let value = evaluator.eval(expr, &empty_row)?;
                    values.push(value);
                }
            }
        }

        // Return a single row with the evaluated values
        Ok(vec![vibesql_storage::Row::new(values)])
    }
}
