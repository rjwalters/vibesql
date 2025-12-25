//! SelectExecutor construction and initialization

use std::{
    cell::{Cell, OnceCell, RefCell},
    collections::HashMap,
};

use instant::Instant;

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
    /// Index of the "representative row" for aggregate context subqueries.
    /// When evaluating correlated subqueries in aggregate SELECT lists, SQLite uses
    /// the row that corresponds to the aggregate result (e.g., the row where the column
    /// has its MAX value for MAX(col)). This field stores that row's index.
    /// See issue #4683 for details.
    pub(super) aggregate_representative_row_idx: RefCell<Option<usize>>,
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
            aggregate_representative_row_idx: RefCell::new(None),
        }
    }

    /// Create a new SELECT executor with CTE context
    /// Used for INSERT ... SELECT with CTEs (WITH clause on INSERT statement)
    pub fn new_with_cte(
        database: &'a vibesql_storage::Database,
        cte_context: &'a HashMap<String, super::super::cte::CteResult>,
    ) -> Self {
        Self::new_with_cte_and_depth(database, cte_context, 0)
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
            aggregate_representative_row_idx: RefCell::new(None),
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
            aggregate_representative_row_idx: RefCell::new(None),
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
            aggregate_representative_row_idx: RefCell::new(None),
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
            aggregate_representative_row_idx: RefCell::new(None),
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
            aggregate_representative_row_idx: RefCell::new(None),
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
            aggregate_representative_row_idx: RefCell::new(None),
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

        // Clear representative row index
        *self.aggregate_representative_row_idx.borrow_mut() = None;
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

    /// Set the representative row index for aggregate context subquery evaluation.
    ///
    /// This is used to implement SQLite's behavior where correlated subqueries in
    /// aggregate SELECT lists use the row that corresponds to the aggregate result.
    /// For example, in `SELECT max(a), (SELECT d FROM t2 WHERE a=c) FROM t1`,
    /// the subquery uses `a` from the row where `a` has its maximum value.
    ///
    /// # Arguments
    /// * `idx` - The index of the representative row in the current group's rows
    pub(super) fn set_aggregate_representative_row(&self, idx: Option<usize>) {
        *self.aggregate_representative_row_idx.borrow_mut() = idx;
    }

    /// Get the representative row index for aggregate context subquery evaluation.
    /// Returns None if no representative row has been set.
    pub(super) fn get_aggregate_representative_row(&self) -> Option<usize> {
        *self.aggregate_representative_row_idx.borrow()
    }

    /// Find the representative row based on aggregates in the SELECT list.
    ///
    /// For MAX(col) aggregates, returns the index of the row where col has its maximum value.
    /// For MIN(col) aggregates, returns the index of the row where col has its minimum value.
    /// For other aggregates or no aggregates, returns None (fallback to first row behavior).
    ///
    /// This implements SQLite's behavior where bare column references in aggregate queries
    /// use values from the row that contributed to the aggregate result.
    ///
    /// # Arguments
    /// * `select_list` - The expanded SELECT list to scan for aggregates
    /// * `group_rows` - The rows in the current group
    /// * `evaluator` - Expression evaluator for computing column values
    pub(super) fn find_representative_row_index(
        &self,
        select_list: &[vibesql_ast::SelectItem],
        group_rows: &[vibesql_storage::Row],
        evaluator: &crate::evaluator::CombinedExpressionEvaluator,
    ) -> Option<usize> {
        use crate::select::grouping::compare_sql_values;
        use vibesql_types::SqlValue;

        if group_rows.is_empty() {
            return None;
        }

        // Scan SELECT list for MAX or MIN aggregates on a column
        for item in select_list {
            if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
                if let vibesql_ast::Expression::AggregateFunction { name, args, .. } = expr {
                    let name_upper = name.to_uppercase();

                    // We only care about MAX/MIN aggregates on columns
                    if (name_upper == "MAX" || name_upper == "MIN") && args.len() == 1 {
                        // Find the row where the column has its max/min value
                        let mut best_idx = 0;
                        let mut best_val: Option<SqlValue> = None;

                        for (idx, row) in group_rows.iter().enumerate() {
                            if let Ok(val) = evaluator.eval(&args[0], row) {
                                // Skip NULL values
                                if matches!(val, SqlValue::Null) {
                                    continue;
                                }

                                let is_better = match &best_val {
                                    None => true,
                                    Some(best) => {
                                        let cmp = compare_sql_values(&val, best);
                                        if name_upper == "MAX" {
                                            cmp == std::cmp::Ordering::Greater
                                        } else {
                                            cmp == std::cmp::Ordering::Less
                                        }
                                    }
                                };

                                if is_better {
                                    best_idx = idx;
                                    best_val = Some(val);
                                }
                            }
                        }

                        // If we found a non-NULL value, use that row
                        if best_val.is_some() {
                            return Some(best_idx);
                        }
                    }
                }
            }
        }

        // No suitable aggregate found, return None (will fall back to first row)
        None
    }
}
