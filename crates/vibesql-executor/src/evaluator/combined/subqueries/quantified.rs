//! Quantified comparison evaluation (ALL/ANY/SOME)
//!
//! This module handles evaluation of quantified comparisons like:
//! - expr op ALL (subquery)
//! - expr op ANY (subquery)
//! - expr op SOME (subquery)

use super::super::super::core::{CombinedExpressionEvaluator, ExpressionEvaluator};
use crate::errors::ExecutorError;

impl CombinedExpressionEvaluator<'_> {
    /// Evaluate quantified comparison: expr op ALL/ANY/SOME (SELECT ...)
    /// SQL:1999 Section 8.8: Quantified comparison predicate
    /// ALL: comparison must be TRUE for all rows
    /// ANY/SOME: comparison must be TRUE for at least one row
    pub(in crate::evaluator::combined) fn eval_quantified(
        &self,
        expr: &vibesql_ast::Expression,
        op: &vibesql_ast::BinaryOperator,
        quantifier: &vibesql_ast::Quantifier,
        subquery: &vibesql_ast::SelectStmt,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Check depth limit to prevent stack overflow
        if self.depth >= crate::limits::MAX_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.depth,
                max_depth: crate::limits::MAX_EXPRESSION_DEPTH,
            });
        }

        let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
            "Quantified comparison requires database reference".to_string(),
        ))?;

        // Evaluate the left-hand expression
        let left_val = self.eval(expr, row)?;

        // Execute the subquery with outer context and propagate depth
        let select_executor = if !self.schema.table_schemas.is_empty() {
            crate::select::SelectExecutor::new_with_outer_context_and_depth(
                database,
                row,
                self.schema,
                self.depth,
            )
        } else {
            crate::select::SelectExecutor::new(database)
        };
        let rows = select_executor.execute(subquery)?;

        // Delegate to shared logic
        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
        crate::evaluator::subqueries_shared::eval_quantified_core(
            &left_val,
            &rows,
            op,
            quantifier,
            |left, op, right| ExpressionEvaluator::eval_binary_op_static(left, op, right, sql_mode.clone()),
        )
    }
}
