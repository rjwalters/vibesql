//! Public API for join order optimization

use super::config::ParallelSearchConfig;
use super::context::JoinOrderContext;
use super::reorder::JoinOrderAnalyzer;

/// Performs join order optimization via exhaustive search
pub struct JoinOrderSearch {
    pub(super) context: JoinOrderContext,
}

impl JoinOrderSearch {
    /// Create a new join order search from an analyzer with real table statistics
    pub fn from_analyzer(
        analyzer: &JoinOrderAnalyzer,
        database: &vibesql_storage::Database,
    ) -> Self {
        Self::from_analyzer_with_predicates(analyzer, database, &std::collections::HashMap::new())
    }

    /// Create a new join order search with WHERE clause selectivity applied
    ///
    /// This version accounts for table-local predicates when estimating cardinalities,
    /// which helps choose better join orders for queries like TPC-H Q3 where filter
    /// predicates significantly reduce table sizes before joining.
    pub fn from_analyzer_with_predicates(
        analyzer: &JoinOrderAnalyzer,
        database: &vibesql_storage::Database,
        table_local_predicates: &std::collections::HashMap<String, Vec<vibesql_ast::Expression>>,
    ) -> Self {
        let edges = analyzer.edges().to_vec();
        let edge_selectivities = JoinOrderContext::compute_edge_selectivities(&edges, database);

        let num_tables = analyzer.tables().len();
        let context = JoinOrderContext {
            all_tables: analyzer.tables().clone(),
            edges,
            table_cardinalities: JoinOrderContext::extract_cardinalities_with_selectivity(
                analyzer,
                database,
                table_local_predicates,
            ),
            edge_selectivities,
            config: ParallelSearchConfig::with_table_count(num_tables),
        };

        Self { context }
    }

    /// Find optimal join order by exploring search space
    ///
    /// Returns list of table names in the order they should be joined.
    ///
    /// When time-bounded search is enabled (default), uses parallel BFS for all
    /// multi-table queries with a configurable time budget. This allows optimization
    /// of large queries (9+ tables) while preventing excessive search time.
    ///
    /// When time-bounded search is disabled, uses legacy behavior: parallel BFS for
    /// 3-6 table queries with highly connected join graphs, DFS for others.
    pub fn find_optimal_order(&self) -> Vec<String> {
        if self.context.all_tables.is_empty() {
            return Vec::new();
        }

        // Use time-bounded BFS for all multi-table queries when enabled
        if self.context.config.use_time_budget {
            // Time-bounded BFS handles all query sizes with time budget protection
            self.context.find_optimal_order_parallel()
        } else {
            // Legacy behavior: table-count based decision
            if self.should_use_parallel_search() {
                self.context.find_optimal_order_parallel()
            } else {
                self.context.find_optimal_order_dfs()
            }
        }
    }

    /// Determine whether to use parallel BFS or sequential DFS
    pub(super) fn should_use_parallel_search(&self) -> bool {
        // Don't parallelize if disabled
        if !self.context.config.enabled {
            return false;
        }

        let num_tables = self.context.all_tables.len();

        // Don't parallelize small queries (< 3 tables)
        if num_tables < 3 {
            return false;
        }

        // Don't parallelize beyond depth limit (memory constraints)
        if num_tables > self.context.config.max_depth {
            return false;
        }

        // Parallel BFS beneficial for highly connected graphs
        // Calculate edge density: edges per table
        let edge_density = self.context.edges.len() as f64 / num_tables as f64;

        // High edge density suggests complex join graph → parallel beneficial
        // Threshold of 1.5 means we need at least 1-2 edges per table
        edge_density >= 1.5
    }
}
