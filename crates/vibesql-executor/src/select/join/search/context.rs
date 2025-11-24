//! Context for join order search operations

use std::collections::BTreeSet;

use super::config::ParallelSearchConfig;
use super::reorder::JoinEdge;
use crate::optimizer::aggregate_analysis::AggregateAnalysis;

/// Context for join order search operations
///
/// Holds shared state and configuration used by all search strategies.
/// This internal struct encapsulates data needed by cost estimation,
/// DFS, BFS, and greedy algorithms.
pub(super) struct JoinOrderContext {
    /// All tables in the query
    pub all_tables: BTreeSet<String>,
    /// Join edges (which tables connect)
    pub edges: Vec<JoinEdge>,
    /// Estimated rows for each table after local filters
    pub table_cardinalities: std::collections::HashMap<String, usize>,
    /// Selectivity for each join edge based on column NDV (number of distinct values)
    /// Key is (left_table, right_table) normalized to lowercase
    pub edge_selectivities: std::collections::HashMap<(String, String), f64>,
    /// Configuration for parallel search
    pub config: ParallelSearchConfig,
    /// Aggregate analysis for GROUP BY/HAVING optimization
    /// When present, cardinalities may be adjusted to account for post-aggregate filtering
    pub aggregate_analysis: Option<AggregateAnalysis>,
}
