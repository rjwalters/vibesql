//! Single-table expression evaluator
//!
//! This module provides the ExpressionEvaluator for evaluating expressions
//! in the context of a single table schema.

use crate::errors::ExecutorError;
use lru::LruCache;
use std::{cell::RefCell, rc::Rc};

/// Evaluates expressions in the context of a row
pub struct ExpressionEvaluator<'a> {
    pub(super) schema: &'a vibesql_catalog::TableSchema,
    pub(super) outer_row: Option<&'a vibesql_storage::Row>,
    pub(super) outer_schema: Option<&'a vibesql_catalog::TableSchema>,
    pub(super) database: Option<&'a vibesql_storage::Database>,
    /// Trigger context for OLD/NEW pseudo-variable resolution
    pub(super) trigger_context: Option<&'a crate::trigger_execution::TriggerContext<'a>>,
    /// Procedural context for stored procedure/function variable resolution
    pub(super) procedural_context: Option<&'a crate::procedural::ExecutionContext>,
    /// Current depth in expression tree (for preventing stack overflow)
    pub(super) depth: usize,
    /// CSE cache for common sub-expression elimination with LRU eviction (shared via Rc across depth levels)
    pub(super) cse_cache: Rc<RefCell<LruCache<u64, vibesql_types::SqlValue>>>,
    /// Whether CSE is enabled (can be disabled for debugging)
    pub(super) enable_cse: bool,
    /// Cache for non-correlated subquery results with LRU eviction (key = subquery hash, value = result rows)
    /// Shared via Rc across child evaluators within a single statement execution.
    pub(super) subquery_cache: Rc<RefCell<LruCache<u64, Vec<vibesql_storage::Row>>>>,
}

impl<'a> ExpressionEvaluator<'a> {
    /// Create a new expression evaluator for a given schema
    pub fn new(schema: &'a vibesql_catalog::TableSchema) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: None,
            outer_schema: None,
            database: None,
            trigger_context: None,
            procedural_context: None,
            depth: 0,
            cse_cache: Rc::new(RefCell::new(super::caching::create_cse_cache())),
            enable_cse: super::caching::is_cse_enabled(),
            subquery_cache: Rc::new(RefCell::new(super::caching::create_subquery_cache())),
        }
    }

    /// Create a new expression evaluator with outer query context for correlated subqueries
    pub fn with_outer_context(
        schema: &'a vibesql_catalog::TableSchema,
        outer_row: &'a vibesql_storage::Row,
        outer_schema: &'a vibesql_catalog::TableSchema,
    ) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
            database: None,
            trigger_context: None,
            procedural_context: None,
            depth: 0,
            cse_cache: Rc::new(RefCell::new(super::caching::create_cse_cache())),
            enable_cse: super::caching::is_cse_enabled(),
            subquery_cache: Rc::new(RefCell::new(super::caching::create_subquery_cache())),
        }
    }

    /// Create a new expression evaluator with database reference for subqueries
    pub fn with_database(
        schema: &'a vibesql_catalog::TableSchema,
        database: &'a vibesql_storage::Database,
    ) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: None,
            outer_schema: None,
            database: Some(database),
            trigger_context: None,
            procedural_context: None,
            depth: 0,
            cse_cache: Rc::new(RefCell::new(super::caching::create_cse_cache())),
            enable_cse: super::caching::is_cse_enabled(),
            subquery_cache: Rc::new(RefCell::new(super::caching::create_subquery_cache())),
        }
    }

    /// Create a new expression evaluator with trigger context for OLD/NEW pseudo-variables
    pub fn with_trigger_context(
        schema: &'a vibesql_catalog::TableSchema,
        database: &'a vibesql_storage::Database,
        trigger_context: &'a crate::trigger_execution::TriggerContext<'a>,
    ) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: None,
            outer_schema: None,
            database: Some(database),
            trigger_context: Some(trigger_context),
            procedural_context: None,
            depth: 0,
            cse_cache: Rc::new(RefCell::new(super::caching::create_cse_cache())),
            enable_cse: super::caching::is_cse_enabled(),
            subquery_cache: Rc::new(RefCell::new(super::caching::create_subquery_cache())),
        }
    }

    /// Create a new expression evaluator with database and outer context (for correlated
    /// subqueries)
    pub fn with_database_and_outer_context(
        schema: &'a vibesql_catalog::TableSchema,
        database: &'a vibesql_storage::Database,
        outer_row: &'a vibesql_storage::Row,
        outer_schema: &'a vibesql_catalog::TableSchema,
    ) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
            database: Some(database),
            trigger_context: None,
            procedural_context: None,
            depth: 0,
            cse_cache: Rc::new(RefCell::new(super::caching::create_cse_cache())),
            enable_cse: super::caching::is_cse_enabled(),
            subquery_cache: Rc::new(RefCell::new(super::caching::create_subquery_cache())),
        }
    }

    /// Create a new expression evaluator with procedural context for stored procedure/function execution
    pub fn with_procedural_context(
        schema: &'a vibesql_catalog::TableSchema,
        database: &'a vibesql_storage::Database,
        procedural_context: &'a crate::procedural::ExecutionContext,
    ) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: None,
            outer_schema: None,
            database: Some(database),
            trigger_context: None,
            procedural_context: Some(procedural_context),
            depth: 0,
            cse_cache: Rc::new(RefCell::new(super::caching::create_cse_cache())),
            enable_cse: super::caching::is_cse_enabled(),
            subquery_cache: Rc::new(RefCell::new(super::caching::create_subquery_cache())),
        }
    }

    /// Evaluate a binary operation
    pub(crate) fn eval_binary_op(
        &self,
        left: &vibesql_types::SqlValue,
        op: &vibesql_ast::BinaryOperator,
        right: &vibesql_types::SqlValue,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Extract SQL mode from database, default to MySQL if not available
        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();

        super::core::eval_binary_op_static(left, op, right, sql_mode)
    }

    /// Clear the CSE cache
    /// Should be called before evaluating expressions for a new row in multi-row contexts
    pub fn clear_cse_cache(&self) {
        self.cse_cache.borrow_mut().clear();
    }

    /// Static version of eval_binary_op for shared logic
    ///
    /// Delegates to the core module's implementation.
    pub(crate) fn eval_binary_op_static(
        left: &vibesql_types::SqlValue,
        op: &vibesql_ast::BinaryOperator,
        right: &vibesql_types::SqlValue,
        sql_mode: vibesql_types::SqlMode,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        super::core::eval_binary_op_static(left, op, right, sql_mode)
    }

    /// Static version of eval_between for constant folding during optimization
    ///
    /// Delegates to the core module's implementation.
    pub(crate) fn eval_between_static(
        expr_val: &vibesql_types::SqlValue,
        low_val: &vibesql_types::SqlValue,
        high_val: &vibesql_types::SqlValue,
        negated: bool,
        symmetric: bool,
        sql_mode: vibesql_types::SqlMode,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        super::core::eval_between_static(expr_val, low_val, high_val, negated, symmetric, sql_mode)
    }

    /// Compare two SQL values for equality in simple CASE expressions
    ///
    /// Delegates to the core module's implementation.
    pub(crate) fn values_are_equal(
        left: &vibesql_types::SqlValue,
        right: &vibesql_types::SqlValue,
    ) -> bool {
        super::core::values_are_equal(left, right)
    }

    /// Helper to execute a closure with incremented depth
    pub(super) fn with_incremented_depth<F, T>(&self, f: F) -> Result<T, ExecutorError>
    where
        F: FnOnce(&Self) -> Result<T, ExecutorError>,
    {
        // Create a new evaluator with incremented depth
        // Share the CSE cache and subquery cache across depth levels for consistent caching
        let evaluator = ExpressionEvaluator {
            schema: self.schema,
            outer_row: self.outer_row,
            outer_schema: self.outer_schema,
            database: self.database,
            trigger_context: self.trigger_context,
            procedural_context: self.procedural_context,
            depth: self.depth + 1,
            cse_cache: self.cse_cache.clone(),
            enable_cse: self.enable_cse,
            subquery_cache: self.subquery_cache.clone(),
        };
        f(&evaluator)
    }
}
