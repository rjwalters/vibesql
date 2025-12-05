//! Combined schema expression evaluator
//!
//! This module provides the CombinedExpressionEvaluator for evaluating expressions
//! in the context of combined schemas (e.g., JOINs with multiple tables).

use crate::{errors::ExecutorError, schema::CombinedSchema, select::WindowFunctionKey};
use ahash::AHasher;
use lru::LruCache;
use std::{
    cell::RefCell,
    collections::HashMap,
    hash::{Hash, Hasher},
    rc::Rc,
};

/// Evaluates expressions with combined schema (for JOINs)
pub struct CombinedExpressionEvaluator<'a> {
    pub(super) schema: &'a CombinedSchema,
    pub(super) database: Option<&'a vibesql_storage::Database>,
    pub(super) outer_row: Option<&'a vibesql_storage::Row>,
    pub(super) outer_schema: Option<&'a CombinedSchema>,
    pub(super) window_mapping: Option<&'a HashMap<WindowFunctionKey, usize>>,
    /// Procedural context for stored procedure/function variable resolution
    pub(super) procedural_context: Option<&'a crate::procedural::ExecutionContext>,
    /// CTE (Common Table Expression) context for accessing WITH clause results
    pub(super) cte_context: Option<&'a HashMap<String, crate::select::cte::CteResult>>,
    /// Cache for column lookups to avoid repeated schema traversals
    /// Uses pre-computed hash of (table, column) as key to avoid string allocations
    column_cache: RefCell<HashMap<u64, usize>>,
    /// Cache for non-correlated subquery results with LRU eviction (key = subquery hash, value = result rows)
    /// Shared via Rc across child evaluators within a single statement execution.
    /// Cache lifetime is tied to the evaluator instance - each new evaluator gets a fresh cache.
    pub(super) subquery_cache: Rc<RefCell<LruCache<u64, Vec<vibesql_storage::Row>>>>,
    /// Current depth in expression tree (for preventing stack overflow)
    pub(super) depth: usize,
    /// CSE cache for common sub-expression elimination with LRU eviction (shared via Rc across depth levels)
    pub(super) cse_cache: Rc<RefCell<LruCache<u64, vibesql_types::SqlValue>>>,
    /// Whether CSE is enabled (can be disabled for debugging)
    pub(super) enable_cse: bool,
}

impl<'a> CombinedExpressionEvaluator<'a> {
    /// Create a new combined expression evaluator
    /// Note: Currently unused as all callers use with_database(), but kept for API completeness
    #[allow(dead_code)]
    pub(crate) fn new(schema: &'a CombinedSchema) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: None,
            outer_row: None,
            outer_schema: None,
            window_mapping: None,
            procedural_context: None,
            cte_context: None,
            column_cache: RefCell::new(HashMap::new()),
            subquery_cache: Rc::new(RefCell::new(super::caching::create_subquery_cache())),
            depth: 0,
            cse_cache: Rc::new(RefCell::new(super::caching::create_cse_cache())),
            enable_cse: super::caching::is_cse_enabled(),
        }
    }

    /// Create a new combined expression evaluator with database reference
    pub(crate) fn with_database(
        schema: &'a CombinedSchema,
        database: &'a vibesql_storage::Database,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: Some(database),
            outer_row: None,
            outer_schema: None,
            window_mapping: None,
            procedural_context: None,
            cte_context: None,
            column_cache: RefCell::new(HashMap::new()),
            subquery_cache: Rc::new(RefCell::new(super::caching::create_subquery_cache())),
            depth: 0,
            cse_cache: Rc::new(RefCell::new(super::caching::create_cse_cache())),
            enable_cse: super::caching::is_cse_enabled(),
        }
    }

    /// Create a new combined expression evaluator with database and outer context for correlated
    /// subqueries
    pub(crate) fn with_database_and_outer_context(
        schema: &'a CombinedSchema,
        database: &'a vibesql_storage::Database,
        outer_row: &'a vibesql_storage::Row,
        outer_schema: &'a CombinedSchema,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: Some(database),
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
            window_mapping: None,
            procedural_context: None,
            cte_context: None,
            column_cache: RefCell::new(HashMap::new()),
            subquery_cache: Rc::new(RefCell::new(super::caching::create_subquery_cache())),
            depth: 0,
            cse_cache: Rc::new(RefCell::new(super::caching::create_cse_cache())),
            enable_cse: super::caching::is_cse_enabled(),
        }
    }

    /// Create a new combined expression evaluator with database and window mapping
    pub(crate) fn with_database_and_windows(
        schema: &'a CombinedSchema,
        database: &'a vibesql_storage::Database,
        window_mapping: &'a HashMap<WindowFunctionKey, usize>,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: Some(database),
            outer_row: None,
            outer_schema: None,
            window_mapping: Some(window_mapping),
            procedural_context: None,
            cte_context: None,
            column_cache: RefCell::new(HashMap::new()),
            subquery_cache: Rc::new(RefCell::new(super::caching::create_subquery_cache())),
            depth: 0,
            cse_cache: Rc::new(RefCell::new(super::caching::create_cse_cache())),
            enable_cse: super::caching::is_cse_enabled(),
        }
    }

    /// Create a new combined expression evaluator with database, window mapping, and CTE context
    pub(crate) fn with_database_and_windows_and_cte(
        schema: &'a CombinedSchema,
        database: &'a vibesql_storage::Database,
        window_mapping: &'a HashMap<WindowFunctionKey, usize>,
        cte_context: &'a HashMap<String, crate::select::cte::CteResult>,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: Some(database),
            outer_row: None,
            outer_schema: None,
            window_mapping: Some(window_mapping),
            procedural_context: None,
            cte_context: Some(cte_context),
            column_cache: RefCell::new(HashMap::new()),
            subquery_cache: Rc::new(RefCell::new(super::caching::create_subquery_cache())),
            depth: 0,
            cse_cache: Rc::new(RefCell::new(super::caching::create_cse_cache())),
            enable_cse: super::caching::is_cse_enabled(),
        }
    }

    /// Create a new combined expression evaluator with database and procedural context
    pub(crate) fn with_database_and_procedural_context(
        schema: &'a CombinedSchema,
        database: &'a vibesql_storage::Database,
        procedural_context: &'a crate::procedural::ExecutionContext,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: Some(database),
            outer_row: None,
            outer_schema: None,
            window_mapping: None,
            procedural_context: Some(procedural_context),
            cte_context: None,
            column_cache: RefCell::new(HashMap::new()),
            subquery_cache: Rc::new(RefCell::new(super::caching::create_subquery_cache())),
            depth: 0,
            cse_cache: Rc::new(RefCell::new(super::caching::create_cse_cache())),
            enable_cse: super::caching::is_cse_enabled(),
        }
    }

    /// Create a new combined expression evaluator with database and CTE context
    pub(crate) fn with_database_and_cte(
        schema: &'a CombinedSchema,
        database: &'a vibesql_storage::Database,
        cte_context: &'a HashMap<String, crate::select::cte::CteResult>,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: Some(database),
            outer_row: None,
            outer_schema: None,
            window_mapping: None,
            procedural_context: None,
            cte_context: Some(cte_context),
            column_cache: RefCell::new(HashMap::new()),
            subquery_cache: Rc::new(RefCell::new(super::caching::create_subquery_cache())),
            depth: 0,
            cse_cache: Rc::new(RefCell::new(super::caching::create_cse_cache())),
            enable_cse: super::caching::is_cse_enabled(),
        }
    }

    /// Create a new combined expression evaluator with database, outer context, and CTE context
    pub(crate) fn with_database_and_outer_context_and_cte(
        schema: &'a CombinedSchema,
        database: &'a vibesql_storage::Database,
        outer_row: &'a vibesql_storage::Row,
        outer_schema: &'a CombinedSchema,
        cte_context: &'a HashMap<String, crate::select::cte::CteResult>,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: Some(database),
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
            window_mapping: None,
            procedural_context: None,
            cte_context: Some(cte_context),
            column_cache: RefCell::new(HashMap::new()),
            subquery_cache: Rc::new(RefCell::new(super::caching::create_subquery_cache())),
            depth: 0,
            cse_cache: Rc::new(RefCell::new(super::caching::create_cse_cache())),
            enable_cse: super::caching::is_cse_enabled(),
        }
    }

    /// Create a new combined expression evaluator with database, procedural context, and CTE context
    pub(crate) fn with_database_and_procedural_context_and_cte(
        schema: &'a CombinedSchema,
        database: &'a vibesql_storage::Database,
        procedural_context: &'a crate::procedural::ExecutionContext,
        cte_context: &'a HashMap<String, crate::select::cte::CteResult>,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: Some(database),
            outer_row: None,
            outer_schema: None,
            window_mapping: None,
            procedural_context: Some(procedural_context),
            cte_context: Some(cte_context),
            column_cache: RefCell::new(HashMap::new()),
            subquery_cache: Rc::new(RefCell::new(super::caching::create_subquery_cache())),
            depth: 0,
            cse_cache: Rc::new(RefCell::new(super::caching::create_cse_cache())),
            enable_cse: super::caching::is_cse_enabled(),
        }
    }

    /// Clear the CSE cache
    /// Should be called before evaluating expressions for a new row in multi-row contexts
    pub(crate) fn clear_cse_cache(&self) {
        self.cse_cache.borrow_mut().clear();
    }

    /// Compute hash key for column cache without allocating strings
    #[inline]
    fn column_cache_key(table: Option<&str>, column: &str) -> u64 {
        let mut hasher = AHasher::default();
        table.hash(&mut hasher);
        column.hash(&mut hasher);
        hasher.finish()
    }

    /// Get column index with caching to avoid repeated schema lookups
    pub(crate) fn get_column_index_cached(
        &self,
        table: Option<&str>,
        column: &str,
    ) -> Option<usize> {
        let key = Self::column_cache_key(table, column);

        // Check cache first
        if let Some(&idx) = self.column_cache.borrow().get(&key) {
            return Some(idx);
        }

        // Cache miss: lookup and store
        if let Some(idx) = self.schema.get_column_index(table, column) {
            self.column_cache.borrow_mut().insert(key, idx);
            Some(idx)
        } else {
            None
        }
    }

    /// Helper to execute a closure with incremented depth
    pub(super) fn with_incremented_depth<F, T>(&self, f: F) -> Result<T, ExecutorError>
    where
        F: FnOnce(&Self) -> Result<T, ExecutorError>,
    {
        // Create a new evaluator with incremented depth
        // Share caches between parent and child evaluators
        let evaluator = CombinedExpressionEvaluator {
            schema: self.schema,
            database: self.database,
            outer_row: self.outer_row,
            outer_schema: self.outer_schema,
            window_mapping: self.window_mapping,
            procedural_context: self.procedural_context,
            cte_context: self.cte_context,
            // Share the column cache between parent and child evaluators
            column_cache: RefCell::new(self.column_cache.borrow().clone()),
            // Share the subquery cache - subqueries can be reused across depths
            subquery_cache: self.subquery_cache.clone(),
            depth: self.depth + 1,
            cse_cache: self.cse_cache.clone(),
            enable_cse: self.enable_cse,
        };
        f(&evaluator)
    }

    /// Clone the evaluator for evaluating a different expression
    ///
    /// Shares the subquery cache (safe because non-correlated subqueries produce
    /// the same results regardless of the current row) but creates a fresh CSE cache
    /// (necessary because CSE results depend on row values).
    pub fn clone_for_new_expression(&self) -> Self {
        CombinedExpressionEvaluator {
            schema: self.schema,
            database: self.database,
            outer_row: self.outer_row,
            outer_schema: self.outer_schema,
            window_mapping: self.window_mapping,
            procedural_context: self.procedural_context,
            cte_context: self.cte_context,
            column_cache: RefCell::new(HashMap::new()),
            subquery_cache: self.subquery_cache.clone(),
            depth: self.depth,
            cse_cache: Rc::new(RefCell::new(super::caching::create_cse_cache())),
            enable_cse: self.enable_cse,
        }
    }

    /// Get the combined schema for this evaluator
    pub(crate) fn schema(&self) -> &'a CombinedSchema {
        self.schema
    }

    /// Get the database reference (if available)
    pub(crate) fn database(&self) -> Option<&'a vibesql_storage::Database> {
        self.database
    }

    /// Get evaluator components for parallel execution
    /// Returns (schema, database, outer_row, outer_schema, window_mapping, cte_context, enable_cse)
    ///
    /// Issue #3562: Now includes cte_context to enable IN subqueries referencing CTEs
    /// during parallel predicate evaluation.
    #[cfg(feature = "parallel")]
    pub(crate) fn get_parallel_components(&self) -> super::parallel::ParallelComponents<'a> {
        (
            self.schema,
            self.database,
            self.outer_row,
            self.outer_schema,
            self.window_mapping,
            self.cte_context,
            self.enable_cse,
        )
    }

    /// Create evaluator from parallel components
    /// Creates a fresh evaluator with independent caches for thread-safe parallel execution
    ///
    /// Issue #3562: Now accepts cte_context to enable IN subqueries referencing CTEs
    /// during parallel predicate evaluation.
    #[cfg(feature = "parallel")]
    pub(crate) fn from_parallel_components(
        schema: &'a CombinedSchema,
        database: Option<&'a vibesql_storage::Database>,
        outer_row: Option<&'a vibesql_storage::Row>,
        outer_schema: Option<&'a CombinedSchema>,
        window_mapping: Option<&'a HashMap<WindowFunctionKey, usize>>,
        cte_context: Option<
            &'a std::collections::HashMap<String, super::super::select::cte::CteResult>,
        >,
        enable_cse: bool,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database,
            outer_row,
            outer_schema,
            window_mapping,
            procedural_context: None,
            cte_context,
            column_cache: RefCell::new(HashMap::new()),
            subquery_cache: Rc::new(RefCell::new(super::caching::create_subquery_cache())),
            depth: 0,
            cse_cache: Rc::new(RefCell::new(super::caching::create_cse_cache())),
            enable_cse,
        }
    }
}
