//! State types for join order search

use std::collections::BTreeSet;

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
}
