//! Configuration for join order search

#![allow(clippy::unnecessary_literal_unwrap)]

/// Configuration for parallel join order search
#[derive(Debug, Clone)]
pub struct ParallelSearchConfig {
    /// Enable parallel BFS search (vs sequential DFS)
    pub enabled: bool,
    /// Maximum depth to explore with parallel BFS (tables with >max_depth use DFS)
    pub max_depth: usize,
    /// Maximum states per layer before pruning
    pub max_states_per_layer: usize,
    /// Prune states with cost > best * threshold
    pub pruning_threshold: f64,
    /// Maximum time budget for join order search (milliseconds)
    /// Default: 1000ms for OLAP workloads
    pub time_budget_ms: u64,
    /// Whether to use time-bounded search (vs table-count cutoff)
    pub use_time_budget: bool,
    /// Enable verbose logging of search statistics
    pub verbose: bool,
}

impl Default for ParallelSearchConfig {
    fn default() -> Self {
        Self::with_table_count(4) // Default assumes 4 tables
    }
}

impl ParallelSearchConfig {
    /// Create a config with adaptive time budget based on table count
    ///
    /// The time budget increases with query complexity:
    /// - 1-3 tables: 500ms (simple queries)
    /// - 4-5 tables: 1000ms (typical OLAP queries)
    /// - 6-7 tables: 1500ms (complex multi-way joins like Q7)
    /// - 8+ tables: 2000ms (very complex queries like Q21)
    ///
    /// This adaptive approach gives more time to complex queries that need it
    /// while keeping simple queries fast.
    pub fn with_table_count(num_tables: usize) -> Self {
        // Read time budget from environment variable if set (overrides adaptive)
        let time_budget_ms = std::env::var("JOIN_REORDER_TIME_BUDGET_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or({
                // Adaptive budget based on table count
                match num_tables {
                    0..=3 => 500,  // Simple queries
                    4..=5 => 1000, // Typical OLAP
                    6..=7 => 1500, // Complex multi-way joins (Q7)
                    _ => 2000,     // Very complex (Q21)
                }
            });

        let verbose = std::env::var("JOIN_REORDER_VERBOSE").is_ok();

        Self {
            enabled: true,
            max_depth: 8, // Support 8-way joins like TPC-H Q8
            max_states_per_layer: 1000,
            pruning_threshold: 1.5,
            time_budget_ms,
            use_time_budget: true, // New: prefer time-bounded over table-count
            verbose,
        }
    }
}
