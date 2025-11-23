//! EXISTS predicate evaluation
//!
//! This module handles evaluation of EXISTS and NOT EXISTS predicates,
//! which test whether a subquery returns any rows.

use super::schema_utils::build_merged_outer_schema;
use super::super::super::core::CombinedExpressionEvaluator;
use crate::errors::ExecutorError;

impl CombinedExpressionEvaluator<'_> {
    /// Evaluate EXISTS predicate: EXISTS (SELECT ...)
    /// SQL:1999 Section 8.7: EXISTS predicate
    /// Returns TRUE if subquery returns at least one row
    /// Returns FALSE if subquery returns zero rows
    /// Never returns NULL (unlike most predicates)
    pub(in crate::evaluator::combined) fn eval_exists(
        &self,
        subquery: &vibesql_ast::SelectStmt,
        negated: bool,
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
            "EXISTS requires database reference".to_string(),
        ))?;

        // Execute the subquery with outer context and propagate depth
        // Build merged schema outside if-else to ensure it lives long enough (fix for #2463)
        let merged_schema = if !self.schema.table_schemas.is_empty() {
            Some(build_merged_outer_schema(self.schema, self.outer_schema))
        } else {
            None
        };

        let select_executor = if let Some(ref schema) = merged_schema {
            crate::select::SelectExecutor::new_with_outer_context_and_depth(
                database,
                row,
                schema,
                self.depth,
            )
        } else {
            crate::select::SelectExecutor::new(database)
        };
        let rows = select_executor.execute(subquery)?;

        // Delegate to shared logic
        Ok(crate::evaluator::subqueries_shared::eval_exists_core(!rows.is_empty(), negated))
    }
}
