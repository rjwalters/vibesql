//! Configuration for join order search

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
        // Read time budget from environment variable if set
        let time_budget_ms = std::env::var("JOIN_REORDER_TIME_BUDGET_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);

        let verbose = std::env::var("JOIN_REORDER_VERBOSE").is_ok();

        Self {
            enabled: true,
            max_depth: 8, // Support 8-way joins like TPC-H Q8
            max_states_per_layer: 1000,
            pruning_threshold: 1.5,
            time_budget_ms,
            use_time_budget: true,  // New: prefer time-bounded over table-count
            verbose,
        }
    }
}
