//! Execution Context
//!
//! This module provides the `ExecutionContext` struct that bundles all the context
//! needed for query execution, eliminating the need for multiple evaluator constructor
//! variants throughout the codebase.

use std::collections::HashMap;

use crate::{
    evaluator::CombinedExpressionEvaluator, procedural, schema::CombinedSchema,
    select::cte::CteResult,
};

/// Execution context that bundles all information needed for query execution.
///
/// This struct consolidates the various optional contexts (outer row, procedural,
/// CTE, windows) that were previously passed separately, reducing the number of
/// evaluator constructor variants needed.
///
/// # Example
///
/// ```text
/// let ctx = ExecutionContext::new(schema, database)
///     .with_cte_context(cte_results)
///     .with_outer_context(outer_row, outer_schema);
///
/// let evaluator = ctx.create_evaluator();
/// ```
pub struct ExecutionContext<'a> {
    /// The combined schema for column resolution
    pub schema: &'a CombinedSchema,
    /// Database reference for table access and subqueries
    pub database: &'a vibesql_storage::Database,
    /// Optional outer row for correlated subqueries
    pub outer_row: Option<&'a vibesql_storage::Row>,
    /// Optional outer schema for correlated subqueries
    pub outer_schema: Option<&'a CombinedSchema>,
    /// Optional procedural context for stored procedures/functions
    pub procedural_context: Option<&'a procedural::ExecutionContext>,
    /// Optional CTE context for WITH clause results
    pub cte_context: Option<&'a HashMap<String, CteResult>>,
    /// Optional window function mapping
    pub window_mapping: Option<&'a HashMap<crate::select::WindowFunctionKey, usize>>,
}

impl<'a> ExecutionContext<'a> {
    /// Create a new execution context with required parameters.
    ///
    /// # Arguments
    /// * `schema` - The combined schema for column resolution
    /// * `database` - Database reference for table access
    pub fn new(schema: &'a CombinedSchema, database: &'a vibesql_storage::Database) -> Self {
        Self {
            schema,
            database,
            outer_row: None,
            outer_schema: None,
            procedural_context: None,
            cte_context: None,
            window_mapping: None,
        }
    }

    /// Add outer context for correlated subqueries.
    ///
    /// # Arguments
    /// * `outer_row` - The current row from the outer query
    /// * `outer_schema` - The schema of the outer query
    #[must_use]
    pub fn with_outer_context(
        mut self,
        outer_row: &'a vibesql_storage::Row,
        outer_schema: &'a CombinedSchema,
    ) -> Self {
        self.outer_row = Some(outer_row);
        self.outer_schema = Some(outer_schema);
        self
    }

    /// Add procedural context for stored procedures/functions.
    ///
    /// # Arguments
    /// * `proc_ctx` - The procedural execution context
    #[must_use]
    pub fn with_procedural_context(mut self, proc_ctx: &'a procedural::ExecutionContext) -> Self {
        self.procedural_context = Some(proc_ctx);
        self
    }

    /// Add CTE context for WITH clause results.
    ///
    /// # Arguments
    /// * `cte_ctx` - Map of CTE names to their results
    #[must_use]
    pub fn with_cte_context(mut self, cte_ctx: &'a HashMap<String, CteResult>) -> Self {
        self.cte_context = Some(cte_ctx);
        self
    }

    /// Add window function mapping.
    ///
    /// # Arguments
    /// * `window_mapping` - Map of window function keys to result column indices
    #[must_use]
    pub fn with_window_mapping(
        mut self,
        window_mapping: &'a HashMap<crate::select::WindowFunctionKey, usize>,
    ) -> Self {
        self.window_mapping = Some(window_mapping);
        self
    }

    /// Create a CombinedExpressionEvaluator with all the context from this struct.
    ///
    /// This replaces the many constructor variants like:
    /// - `with_database()`
    /// - `with_database_and_cte()`
    /// - `with_database_and_outer_context()`
    /// - `with_database_and_outer_context_and_cte()`
    /// - `with_database_and_procedural_context()`
    /// - `with_database_and_procedural_context_and_cte()`
    /// - `with_database_and_windows()`
    /// - `with_database_and_windows_and_cte()`
    ///
    /// By using a builder pattern, we consolidate all 8+ variants into a single method.
    pub fn create_evaluator(&self) -> CombinedExpressionEvaluator<'a> {
        // Use the most complete constructor and set optional fields
        // We match on the combination of optional fields to call the right constructor
        // This is temporary until we refactor CombinedExpressionEvaluator itself
        match (
            self.outer_row,
            self.outer_schema,
            self.procedural_context,
            self.cte_context,
            self.window_mapping,
        ) {
            // With outer context and CTE
            (Some(outer_row), Some(outer_schema), None, Some(cte_ctx), None) => {
                CombinedExpressionEvaluator::with_database_and_outer_context_and_cte(
                    self.schema,
                    self.database,
                    outer_row,
                    outer_schema,
                    cte_ctx,
                )
            }
            // With outer context only
            (Some(outer_row), Some(outer_schema), None, None, None) => {
                CombinedExpressionEvaluator::with_database_and_outer_context(
                    self.schema,
                    self.database,
                    outer_row,
                    outer_schema,
                )
            }
            // With procedural context and CTE
            (None, None, Some(proc_ctx), Some(cte_ctx), None) => {
                CombinedExpressionEvaluator::with_database_and_procedural_context_and_cte(
                    self.schema,
                    self.database,
                    proc_ctx,
                    cte_ctx,
                )
            }
            // With procedural context only
            (None, None, Some(proc_ctx), None, None) => {
                CombinedExpressionEvaluator::with_database_and_procedural_context(
                    self.schema,
                    self.database,
                    proc_ctx,
                )
            }
            // With CTE context only
            (None, None, None, Some(cte_ctx), None) => {
                CombinedExpressionEvaluator::with_database_and_cte(
                    self.schema,
                    self.database,
                    cte_ctx,
                )
            }
            // With window mapping and CTE
            (None, None, None, Some(cte_ctx), Some(window_map)) => {
                CombinedExpressionEvaluator::with_database_and_windows_and_cte(
                    self.schema,
                    self.database,
                    window_map,
                    cte_ctx,
                )
            }
            // With window mapping only
            (None, None, None, None, Some(window_map)) => {
                CombinedExpressionEvaluator::with_database_and_windows(
                    self.schema,
                    self.database,
                    window_map,
                )
            }
            // Base case: just database
            (None, None, None, None, None) => {
                CombinedExpressionEvaluator::with_database(self.schema, self.database)
            }
            // Unsupported combinations fall back to base
            // Note: outer + procedural is not a valid combination in current usage
            _ => CombinedExpressionEvaluator::with_database(self.schema, self.database),
        }
    }

    /// Check if this context has outer context (for correlated subqueries).
    #[inline]
    pub fn has_outer_context(&self) -> bool {
        self.outer_row.is_some() && self.outer_schema.is_some()
    }

    /// Check if this context has CTE context.
    #[inline]
    pub fn has_cte_context(&self) -> bool {
        self.cte_context.is_some()
    }

    /// Check if this context has procedural context.
    #[inline]
    pub fn has_procedural_context(&self) -> bool {
        self.procedural_context.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::CombinedSchema;
    use vibesql_catalog::TableSchema;

    /// Builder for creating ExecutionContext from SelectExecutor fields.
    ///
    /// This helper extracts common context-building logic that's repeated
    /// across multiple execution methods.
    struct ExecutionContextBuilder<'a> {
        schema: &'a CombinedSchema,
        database: &'a vibesql_storage::Database,
        outer_row: Option<&'a vibesql_storage::Row>,
        outer_schema: Option<&'a CombinedSchema>,
        procedural_context: Option<&'a procedural::ExecutionContext>,
        cte_context: Option<&'a HashMap<String, CteResult>>,
        executor_cte_context: Option<&'a HashMap<String, CteResult>>,
    }

    impl<'a> ExecutionContextBuilder<'a> {
        /// Create a new builder with required parameters.
        fn new(schema: &'a CombinedSchema, database: &'a vibesql_storage::Database) -> Self {
            Self {
                schema,
                database,
                outer_row: None,
                outer_schema: None,
                procedural_context: None,
                cte_context: None,
                executor_cte_context: None,
            }
        }

        /// Set CTE context from query-level CTEs.
        #[must_use]
        fn cte_from_query(mut self, cte_results: &'a HashMap<String, CteResult>) -> Self {
            if !cte_results.is_empty() {
                self.cte_context = Some(cte_results);
            }
            self
        }

        /// Build the ExecutionContext.
        ///
        /// CTE context priority: query-level CTEs take precedence over executor-level.
        fn build(self) -> ExecutionContext<'a> {
            let mut ctx = ExecutionContext::new(self.schema, self.database);

            // Set outer context if both row and schema are present
            if let (Some(outer_row), Some(outer_schema)) = (self.outer_row, self.outer_schema) {
                ctx = ctx.with_outer_context(outer_row, outer_schema);
            }

            // Set procedural context if present
            if let Some(proc_ctx) = self.procedural_context {
                ctx = ctx.with_procedural_context(proc_ctx);
            }

            // Set CTE context (query-level takes precedence)
            if let Some(cte_ctx) = self.cte_context {
                ctx = ctx.with_cte_context(cte_ctx);
            } else if let Some(executor_cte) = self.executor_cte_context {
                ctx = ctx.with_cte_context(executor_cte);
            }

            ctx
        }
    }

    fn create_test_schema() -> CombinedSchema {
        let table_schema = TableSchema::new("test".to_string(), vec![]);
        CombinedSchema::from_table("test".to_string(), table_schema)
    }

    #[test]
    fn test_execution_context_builder_basic() {
        let schema = create_test_schema();
        let database = vibesql_storage::Database::new();

        let ctx = ExecutionContext::new(&schema, &database);

        assert!(!ctx.has_outer_context());
        assert!(!ctx.has_cte_context());
        assert!(!ctx.has_procedural_context());
    }

    #[test]
    fn test_execution_context_with_cte() {
        let schema = create_test_schema();
        let database = vibesql_storage::Database::new();
        let cte_results: HashMap<String, CteResult> = HashMap::new();

        let ctx = ExecutionContext::new(&schema, &database).with_cte_context(&cte_results);

        assert!(ctx.has_cte_context());
    }

    #[test]
    fn test_execution_context_builder_chain() {
        let schema = create_test_schema();
        let database = vibesql_storage::Database::new();
        let cte_results: HashMap<String, CteResult> = HashMap::new();

        let ctx =
            ExecutionContextBuilder::new(&schema, &database).cte_from_query(&cte_results).build();

        // Empty CTE map should not set context
        assert!(!ctx.has_cte_context());
    }
}
