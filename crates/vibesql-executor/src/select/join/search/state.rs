//! State types for join order search

use std::collections::{BTreeMap, BTreeSet};

/// Represents the cost of joining a set of tables
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct JoinCost {
    /// Estimated number of intermediate rows
    pub cardinality: usize,
    /// Estimated comparison operations
    pub operations: u64,
}

impl JoinCost {
    pub fn new(cardinality: usize, operations: u64) -> Self {
        Self { cardinality, operations }
    }

    /// Estimate total cost as a comparable value
    /// Prioritizes reducing intermediate row count (cardinality)
    /// then comparison operations
    pub fn total(&self) -> u64 {
        // Weight cardinality heavily since it affects downstream joins
        // 1 additional row impacts all future joins
        // Use saturating arithmetic to prevent overflow with large intermediate results
        (self.cardinality as u64).saturating_mul(1000).saturating_add(self.operations)
    }
}

/// Tracks cascading filter information for accurate cardinality estimation
///
/// When a table has local predicates applied (e.g., `o_orderdate < '1995-03-15'`),
/// this affects downstream joins. Rows that survive the filter have correlated
/// characteristics, so joins through filtered tables should not assume independent
/// selectivity.
#[derive(Debug, Clone, Default)]
pub struct CascadingFilterState {
    /// Tables that have had local predicates applied
    /// Maps table name -> cumulative selectivity factor applied to that table
    pub filtered_tables: BTreeMap<String, f64>,

    /// Cumulative correlation factor for the current join chain
    /// Starts at 1.0, decreases as we join through more filtered tables
    /// This represents how much the intermediate result is "tighter" than
    /// independent selectivity would suggest
    pub correlation_factor: f64,
}

impl CascadingFilterState {
    pub fn new() -> Self {
        Self { filtered_tables: BTreeMap::new(), correlation_factor: 1.0 }
    }

    /// Record that a table has had filters applied with given selectivity
    pub fn add_filtered_table(&mut self, table: String, selectivity: f64) {
        self.filtered_tables.insert(table, selectivity);
    }

    /// Check if a table has been filtered
    pub fn is_filtered(&self, table: &str) -> bool {
        self.filtered_tables.contains_key(table)
    }

    /// Get the selectivity applied to a table (1.0 if not filtered)
    pub fn get_table_selectivity(&self, table: &str) -> f64 {
        self.filtered_tables.get(table).copied().unwrap_or(1.0)
    }

    /// Apply correlation adjustment when joining through filtered tables
    ///
    /// When joining table B to a set containing filtered table A, the output
    /// cardinality is reduced because:
    /// 1. The rows in A that survived the filter are not randomly distributed
    /// 2. The join key values in the filtered A may correlate with the filter predicate
    ///
    /// We use a conservative factor: if a table was filtered, joins to it
    /// produce fewer rows than independent selectivity would suggest.
    pub fn apply_correlation_for_join(&mut self, joined_tables: &BTreeSet<String>) {
        // Count how many filtered tables we're joining through
        let filtered_count = joined_tables.iter().filter(|t| self.is_filtered(t)).count();

        if filtered_count > 0 {
            // Each filtered table in the join chain reduces the correlation factor
            // Use a conservative reduction: 0.85 per filtered table
            // This means joins through filtered tables produce ~15% fewer rows
            // than independent selectivity would predict
            self.correlation_factor *= 0.85_f64.powi(filtered_count as i32);
        }
    }
}

/// State during join order search
#[derive(Debug, Clone)]
pub(super) struct SearchState {
    /// Tables already joined
    pub joined_tables: BTreeSet<String>,
    /// Cumulative cost so far
    pub cost_so_far: JoinCost,
    /// Ordering of tables
    pub order: Vec<String>,
    /// Current intermediate result size (rows after all joins so far)
    pub current_cardinality: usize,
    /// Cascading filter state for accurate cardinality estimation
    pub filter_state: CascadingFilterState,
}
