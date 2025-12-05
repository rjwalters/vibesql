//! SelectExecutor construction and initialization

use instant::Instant;
use std::{
    cell::{Cell, OnceCell, RefCell},
    collections::HashMap,
};

use crate::{
    errors::ExecutorError,
    evaluator::compiled_pivot::PivotAggregateGroup,
    limits::{MAX_MEMORY_BYTES, MEMORY_WARNING_BYTES},
    memory::QueryArena,
};

/// Executes SELECT queries
pub struct SelectExecutor<'a> {
    pub(super) database: &'a vibesql_storage::Database,
    pub(super) outer_row: Option<&'a vibesql_storage::Row>,
    pub(super) outer_schema: Option<&'a crate::schema::CombinedSchema>,
    /// Procedural context for stored procedure/function variable resolution
    pub(super) procedural_context: Option<&'a crate::procedural::ExecutionContext>,
    /// CTE (Common Table Expression) context for accessing WITH clause results
    /// Enables scalar subqueries to reference CTEs defined in the outer query
    pub(super) cte_context: Option<&'a HashMap<String, super::super::cte::CteResult>>,
    /// Subquery nesting depth (for preventing stack overflow)
    pub(super) subquery_depth: usize,
    /// Memory used by this query execution (in bytes)
    pub(super) memory_used_bytes: Cell<usize>,
    /// Flag to prevent logging the same warning multiple times
    pub(super) memory_warning_logged: Cell<bool>,
    /// Query start time (for timeout enforcement)
    pub(crate) start_time: Instant,
    /// Timeout in seconds (defaults to MAX_QUERY_EXECUTION_SECONDS)
    pub timeout_seconds: u64,
    /// Cache for aggregate results within a single group
    /// Key: Hash of the aggregate expression (format: "{name}:{distinct}:{arg_debug}")
    /// Value: Cached aggregate result
    /// Scope: Per-group evaluation (cleared between groups)
    /// Lazily initialized - only created when first aggregate is evaluated
    pub(super) aggregate_cache: OnceCell<RefCell<HashMap<String, vibesql_types::SqlValue>>>,
    /// Arena allocator for query-scoped allocations
    /// Eliminates malloc/free overhead by using bump-pointer allocation
    /// All allocations are freed when query completes
    /// Lazily initialized - only created when first allocation is needed
    pub(super) arena: OnceCell<RefCell<QueryArena>>,
    /// Pivot aggregate group for batched SUM(CASE...) optimization
    /// Detected once per query, executed once per group
    /// Stores results directly in aggregate_cache
    pub(super) pivot_group: RefCell<Option<PivotAggregateGroup>>,
}

impl<'a> SelectExecutor<'a> {
    /// Create a new SELECT executor
    ///
    /// # Performance
    ///
    /// This constructor is optimized for OLTP workloads:
    /// - Arena is lazily initialized (10MB allocation deferred until needed)
    /// - Aggregate cache is lazily initialized (HashMap allocation deferred)
    /// - Simple queries that don't use aggregates or complex allocations skip these costs
    pub fn new(database: &'a vibesql_storage::Database) -> Self {
        SelectExecutor {
            database,
            outer_row: None,
            outer_schema: None,
            procedural_context: None,
            cte_context: None,
            subquery_depth: 0,
            memory_used_bytes: Cell::new(0),
            memory_warning_logged: Cell::new(false),
            start_time: Instant::now(),
            timeout_seconds: crate::limits::MAX_QUERY_EXECUTION_SECONDS,
            aggregate_cache: OnceCell::new(),
            arena: OnceCell::new(),
            pivot_group: RefCell::new(None),
        }
    }

    /// Create a new SELECT executor with outer context for correlated subqueries
    pub fn new_with_outer_context(
        database: &'a vibesql_storage::Database,
        outer_row: &'a vibesql_storage::Row,
        outer_schema: &'a crate::schema::CombinedSchema,
    ) -> Self {
        SelectExecutor {
            database,
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
            procedural_context: None,
            cte_context: None,
            subquery_depth: 0,
            memory_used_bytes: Cell::new(0),
            memory_warning_logged: Cell::new(false),
            start_time: Instant::now(),
            timeout_seconds: crate::limits::MAX_QUERY_EXECUTION_SECONDS,
            aggregate_cache: OnceCell::new(),
            arena: OnceCell::new(),
            pivot_group: RefCell::new(None),
        }
    }

    /// Create a new SELECT executor with explicit depth tracking
    /// Used for non-correlated subqueries to propagate depth limit enforcement
    pub fn new_with_depth(database: &'a vibesql_storage::Database, parent_depth: usize) -> Self {
        SelectExecutor {
            database,
            outer_row: None,
            outer_schema: None,
            procedural_context: None,
            cte_context: None,
            subquery_depth: parent_depth + 1,
            memory_used_bytes: Cell::new(0),
            memory_warning_logged: Cell::new(false),
            start_time: Instant::now(),
            timeout_seconds: crate::limits::MAX_QUERY_EXECUTION_SECONDS,
            aggregate_cache: OnceCell::new(),
            arena: OnceCell::new(),
            pivot_group: RefCell::new(None),
        }
    }

    /// Create a new SELECT executor with outer context and explicit depth
    /// Used when creating subquery executors to track nesting depth
    ///
    /// # Note on Timeout Inheritance
    ///
    /// Currently subqueries get their own 60s timeout rather than sharing parent's timeout.
    /// This means a query with N subqueries could run for up to N*60s instead of 60s total.
    ///
    /// However, this is acceptable for the initial fix because:
    /// 1. The main regression (100% timeout) was caused by ZERO timeout enforcement
    /// 2. Having per-subquery timeouts still prevents infinite loops (the core issue)
    /// 3. Most problematic queries cause recursive subquery execution, which IS caught
    /// 4. Threading timeout through evaluators requires extensive refactoring
    ///
    /// Future improvement: Add timeout fields to ExpressionEvaluator and pass through
    /// See: <https://github.com/rjwalters/vibesql/issues/1012#subquery-timeout>
    pub fn new_with_outer_context_and_depth(
        database: &'a vibesql_storage::Database,
        outer_row: &'a vibesql_storage::Row,
        outer_schema: &'a crate::schema::CombinedSchema,
        parent_depth: usize,
    ) -> Self {
        SelectExecutor {
            database,
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
            procedural_context: None,
            cte_context: None,
            subquery_depth: parent_depth + 1,
            memory_used_bytes: Cell::new(0),
            memory_warning_logged: Cell::new(false),
            start_time: Instant::now(),
            timeout_seconds: crate::limits::MAX_QUERY_EXECUTION_SECONDS,
            aggregate_cache: OnceCell::new(),
            arena: OnceCell::new(),
            pivot_group: RefCell::new(None),
        }
    }

    /// Create a new SELECT executor with procedural context for stored procedures/functions
    pub fn new_with_procedural_context(
        database: &'a vibesql_storage::Database,
        procedural_context: &'a crate::procedural::ExecutionContext,
    ) -> Self {
        SelectExecutor {
            database,
            outer_row: None,
            outer_schema: None,
            procedural_context: Some(procedural_context),
            cte_context: None,
            subquery_depth: 0,
            memory_used_bytes: Cell::new(0),
            memory_warning_logged: Cell::new(false),
            start_time: Instant::now(),
            timeout_seconds: crate::limits::MAX_QUERY_EXECUTION_SECONDS,
            aggregate_cache: OnceCell::new(),
            arena: OnceCell::new(),
            pivot_group: RefCell::new(None),
        }
    }

    /// Create a new SELECT executor with CTE context and depth tracking
    /// Used for non-correlated subqueries that need access to parent CTEs
    pub fn new_with_cte_and_depth(
        database: &'a vibesql_storage::Database,
        cte_context: &'a HashMap<String, super::super::cte::CteResult>,
        parent_depth: usize,
    ) -> Self {
        SelectExecutor {
            database,
            outer_row: None,
            outer_schema: None,
            procedural_context: None,
            cte_context: Some(cte_context),
            subquery_depth: parent_depth + 1,
            memory_used_bytes: Cell::new(0),
            memory_warning_logged: Cell::new(false),
            start_time: Instant::now(),
            timeout_seconds: crate::limits::MAX_QUERY_EXECUTION_SECONDS,
            aggregate_cache: OnceCell::new(),
            arena: OnceCell::new(),
            pivot_group: RefCell::new(None),
        }
    }

    /// Create a new SELECT executor with outer context, CTE context, and depth tracking
    /// Used for correlated subqueries that need access to both outer row and parent CTEs
    pub fn new_with_outer_and_cte_and_depth(
        database: &'a vibesql_storage::Database,
        outer_row: &'a vibesql_storage::Row,
        outer_schema: &'a crate::schema::CombinedSchema,
        cte_context: &'a HashMap<String, super::super::cte::CteResult>,
        parent_depth: usize,
    ) -> Self {
        SelectExecutor {
            database,
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
            procedural_context: None,
            cte_context: Some(cte_context),
            subquery_depth: parent_depth + 1,
            memory_used_bytes: Cell::new(0),
            memory_warning_logged: Cell::new(false),
            start_time: Instant::now(),
            timeout_seconds: crate::limits::MAX_QUERY_EXECUTION_SECONDS,
            aggregate_cache: OnceCell::new(),
            arena: OnceCell::new(),
            pivot_group: RefCell::new(None),
        }
    }

    /// Track memory allocation
    pub(super) fn track_memory_allocation(&self, bytes: usize) -> Result<(), ExecutorError> {
        let mut current = self.memory_used_bytes.get();
        current += bytes;
        self.memory_used_bytes.set(current);

        // Log warning at threshold
        if !self.memory_warning_logged.get() && current > MEMORY_WARNING_BYTES {
            eprintln!(
                "⚠️  Query memory usage: {:.2} GB",
                current as f64 / 1024.0 / 1024.0 / 1024.0
            );
            self.memory_warning_logged.set(true);
        }

        // Hard limit
        if current > MAX_MEMORY_BYTES {
            return Err(ExecutorError::MemoryLimitExceeded {
                used_bytes: current,
                max_bytes: MAX_MEMORY_BYTES,
            });
        }

        Ok(())
    }

    /// Track memory deallocation
    #[cfg(test)]
    pub(super) fn track_memory_deallocation(&self, bytes: usize) {
        let current = self.memory_used_bytes.get();
        self.memory_used_bytes.set(current.saturating_sub(bytes));
    }

    /// Override default timeout for this query (useful for testing)
    pub fn with_timeout(mut self, seconds: u64) -> Self {
        self.timeout_seconds = seconds;
        self
    }

    /// Clear aggregate cache (should be called between group evaluations)
    /// No-op if the cache has not been initialized (lazy initialization)
    pub(super) fn clear_aggregate_cache(&self) {
        if let Some(cache) = self.aggregate_cache.get() {
            cache.borrow_mut().clear();
        }
    }

    /// Get access to the aggregate cache, initializing it lazily if needed
    pub(super) fn get_aggregate_cache(&self) -> &RefCell<HashMap<String, vibesql_types::SqlValue>> {
        self.aggregate_cache.get_or_init(|| RefCell::new(HashMap::new()))
    }

    /// Get access to the query buffer pool for reducing allocations
    pub(crate) fn query_buffer_pool(&self) -> &vibesql_storage::QueryBufferPool {
        self.database.query_buffer_pool()
    }

    /// Check if query has exceeded timeout
    /// Call this in hot loops to prevent infinite execution
    pub fn check_timeout(&self) -> Result<(), crate::errors::ExecutorError> {
        let elapsed = self.start_time.elapsed().as_secs();
        if elapsed >= self.timeout_seconds {
            return Err(crate::errors::ExecutorError::QueryTimeoutExceeded {
                elapsed_seconds: elapsed,
                max_seconds: self.timeout_seconds,
            });
        }
        Ok(())
    }

    /// Get access to the query arena for allocations
    /// The arena is lazily initialized on first access
    #[allow(dead_code)]
    pub(crate) fn arena(&self) -> &RefCell<QueryArena> {
        self.arena.get_or_init(|| RefCell::new(QueryArena::new()))
    }

    /// Reset the arena for query reuse
    /// Called at the start of each query execution
    /// No-op if the arena has not been initialized (lazy initialization)
    pub(super) fn reset_arena(&self) {
        if let Some(arena) = self.arena.get() {
            arena.borrow_mut().reset();
        }
    }

    /// Reset the executor for reuse between queries
    ///
    /// This method prepares the executor for a new query execution by:
    /// - Resetting the start time to now
    /// - Clearing memory tracking counters
    /// - Resetting the arena (if initialized)
    /// - Clearing the aggregate cache (if initialized)
    ///
    /// # Performance
    ///
    /// Call this method to reuse an executor instead of creating a new one.
    /// This avoids the allocation overhead of creating new HashMap and arena instances.
    pub fn reset_for_reuse(&mut self) {
        self.start_time = Instant::now();
        self.memory_used_bytes.set(0);
        self.memory_warning_logged.set(false);
        self.subquery_depth = 0;
        self.outer_row = None;
        self.outer_schema = None;
        self.procedural_context = None;
        self.cte_context = None;

        // Reset arena if it was initialized (clears offset, keeps buffer allocation)
        if let Some(arena) = self.arena.get() {
            arena.borrow_mut().reset();
        }

        // Clear aggregate cache if it was initialized (clears entries, keeps HashMap allocation)
        if let Some(cache) = self.aggregate_cache.get() {
            cache.borrow_mut().clear();
        }

        // Clear pivot group
        *self.pivot_group.borrow_mut() = None;
    }

    /// Set the pivot aggregate group for this query
    ///
    /// Called once during query planning when a pivot pattern is detected.
    /// The pivot group is then executed once per group in aggregation.
    pub(super) fn set_pivot_group(&self, group: PivotAggregateGroup) {
        *self.pivot_group.borrow_mut() = Some(group);
    }

    /// Execute pivot aggregates for the current group and cache results
    ///
    /// This executes all pivot aggregates in a single pass over the rows,
    /// storing results in the aggregate cache. Subsequent calls to evaluate
    /// individual pivot aggregates will hit the cache.
    pub(super) fn execute_pivot_aggregates(
        &self,
        group_rows: &[vibesql_storage::Row],
    ) -> Result<(), ExecutorError> {
        let pivot_group = self.pivot_group.borrow();
        if let Some(ref pivot) = *pivot_group {
            let results = pivot.execute(group_rows)?;

            // Store all pivot results in the aggregate cache
            let cache = self.get_aggregate_cache();
            let mut cache_mut = cache.borrow_mut();
            for (cache_key, value) in results {
                cache_mut.insert(cache_key, value);
            }
        }
        Ok(())
    }

    /// Check if a pivot group is set for this query
    pub(super) fn has_pivot_group(&self) -> bool {
        self.pivot_group.borrow().is_some()
    }
}
