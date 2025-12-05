//! Public API for join order optimization

use super::config::ParallelSearchConfig;
use super::context::JoinOrderContext;
use super::reorder::JoinOrderAnalyzer;
use crate::optimizer::aggregate_analysis::AggregateAnalysis;

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
        Self::from_analyzer_with_predicates(
            analyzer,
            database,
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
        )
    }

    /// Create a new join order search with WHERE clause selectivity applied
    ///
    /// This version accounts for table-local predicates when estimating cardinalities,
    /// which helps choose better join orders for queries like TPC-H Q3 where filter
    /// predicates significantly reduce table sizes before joining.
    ///
    /// # Parameters
    /// - `alias_to_table`: Maps table aliases (e.g., "n1", "n2") to actual table names (e.g., "nation").
    ///   This is critical for queries with self-joins (like TPC-H Q7 with two nation aliases)
    ///   to correctly look up table cardinalities and statistics.
    pub fn from_analyzer_with_predicates(
        analyzer: &JoinOrderAnalyzer,
        database: &vibesql_storage::Database,
        table_local_predicates: &std::collections::HashMap<String, Vec<vibesql_ast::Expression>>,
        alias_to_table: &std::collections::HashMap<String, String>,
    ) -> Self {
        let edges = analyzer.edges().to_vec();
        let edge_selectivities =
            JoinOrderContext::compute_edge_selectivities(&edges, database, alias_to_table);

        let num_tables = analyzer.tables().len();

        // Extract base cardinalities (before filters) for cascading filter tracking
        let table_base_cardinalities =
            JoinOrderContext::extract_base_cardinalities(analyzer, database, alias_to_table);

        let context = JoinOrderContext {
            all_tables: analyzer.tables().clone(),
            edges,
            table_cardinalities: JoinOrderContext::extract_cardinalities_with_selectivity(
                analyzer,
                database,
                table_local_predicates,
                alias_to_table,
            ),
            table_base_cardinalities,
            edge_selectivities,
            config: ParallelSearchConfig::with_table_count(num_tables),
            aggregate_analysis: None,
        };

        Self { context }
    }

    /// Create a new join order search with aggregate-aware optimization
    ///
    /// This version accepts aggregate analysis results and can adjust join order
    /// decisions based on GROUP BY/HAVING clauses. When HAVING filters are selective,
    /// the optimizer may prefer different join orders that enable early aggregation.
    ///
    /// # Parameters
    /// - `alias_to_table`: Maps table aliases to actual table names for database lookups.
    pub fn from_analyzer_with_aggregates(
        analyzer: &JoinOrderAnalyzer,
        database: &vibesql_storage::Database,
        table_local_predicates: &std::collections::HashMap<String, Vec<vibesql_ast::Expression>>,
        alias_to_table: &std::collections::HashMap<String, String>,
        aggregate_analysis: AggregateAnalysis,
    ) -> Self {
        let edges = analyzer.edges().to_vec();
        let edge_selectivities =
            JoinOrderContext::compute_edge_selectivities(&edges, database, alias_to_table);

        let num_tables = analyzer.tables().len();

        // Extract base cardinalities (before filters) for cascading filter tracking
        let table_base_cardinalities =
            JoinOrderContext::extract_base_cardinalities(analyzer, database, alias_to_table);

        let context = JoinOrderContext {
            all_tables: analyzer.tables().clone(),
            edges,
            table_cardinalities: JoinOrderContext::extract_cardinalities_with_selectivity(
                analyzer,
                database,
                table_local_predicates,
                alias_to_table,
            ),
            table_base_cardinalities,
            edge_selectivities,
            config: ParallelSearchConfig::with_table_count(num_tables),
            aggregate_analysis: Some(aggregate_analysis),
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
