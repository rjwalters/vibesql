//! Context for join order search operations

use std::collections::BTreeSet;

use super::config::ParallelSearchConfig;
use super::reorder::JoinEdge;
use super::state::CascadingFilterState;
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
    /// Base rows for each table before local filters (used for filter tracking)
    pub table_base_cardinalities: std::collections::HashMap<String, usize>,
    /// Selectivity for each join edge based on column NDV (number of distinct values)
    /// Key is (left_table, right_table) normalized to lowercase
    pub edge_selectivities: std::collections::HashMap<(String, String), f64>,
    /// Configuration for parallel search
    pub config: ParallelSearchConfig,
    /// Aggregate analysis for GROUP BY/HAVING optimization
    /// When present, cardinalities may be adjusted to account for post-aggregate filtering
    #[allow(dead_code)]
    pub aggregate_analysis: Option<AggregateAnalysis>,
}

impl JoinOrderContext {
    /// Create an initial filter state based on table cardinalities
    ///
    /// Tables that have local predicates applied will have lower cardinalities
    /// than their base cardinalities. We detect this and record it.
    pub fn create_initial_filter_state(&self) -> CascadingFilterState {
        let mut state = CascadingFilterState::new();

        for table in &self.all_tables {
            let filtered_card = self.table_cardinalities.get(table).copied().unwrap_or(10000);
            let base_card =
                self.table_base_cardinalities.get(table).copied().unwrap_or(filtered_card);

            // If the filtered cardinality is less than base, a filter was applied
            if filtered_card < base_card && base_card > 0 {
                let selectivity = filtered_card as f64 / base_card as f64;
                state.add_filtered_table(table.clone(), selectivity);

                if self.config.verbose {
                    eprintln!(
                        "[CASCADE_FILTER] Table {} marked as filtered: {} -> {} (selectivity: {:.4})",
                        table, base_card, filtered_card, selectivity
                    );
                }
            }
        }

        state
    }
}
