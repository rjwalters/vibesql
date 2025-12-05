//! Quantified comparison evaluation (ALL/ANY/SOME)
//!
//! This module handles evaluation of quantified comparisons like:
//! - expr op ALL (subquery)
//! - expr op ANY (subquery)
//! - expr op SOME (subquery)

use super::super::super::core::{CombinedExpressionEvaluator, ExpressionEvaluator};
use super::schema_utils::{build_merged_outer_row, build_merged_outer_schema};
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
        // Build merged schema and row outside if-else to ensure they live long enough (fix for #2463)
        let merged_schema = if !self.schema.table_schemas.is_empty() {
            Some(build_merged_outer_schema(self.schema, self.outer_schema))
        } else {
            None
        };

        let merged_row = if merged_schema.is_some() {
            Some(build_merged_outer_row(row, self.outer_row))
        } else {
            None
        };

        // Pass CTE context for queries referencing CTEs from outer scope (#3044)
        let select_executor =
            if let (Some(ref schema), Some(ref outer_row)) = (&merged_schema, &merged_row) {
                if let Some(cte_ctx) = self.cte_context {
                    crate::select::SelectExecutor::new_with_outer_and_cte_and_depth(
                        database, outer_row, schema, cte_ctx, self.depth,
                    )
                } else {
                    crate::select::SelectExecutor::new_with_outer_context_and_depth(
                        database, outer_row, schema, self.depth,
                    )
                }
            } else if let Some(cte_ctx) = self.cte_context {
                crate::select::SelectExecutor::new_with_cte_and_depth(database, cte_ctx, self.depth)
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
            |left, op, right| {
                ExpressionEvaluator::eval_binary_op_static(left, op, right, sql_mode.clone())
            },
        )
    }
}
