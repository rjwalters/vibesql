//! Greedy heuristic for large queries
//!
//! This module implements a polynomial-time approximation algorithm for join
//! order optimization when exhaustive search is impractical (8+ tables).

use std::collections::BTreeSet;

use super::context::JoinOrderContext;
use super::state::JoinCost;

impl JoinOrderContext {
    /// Find optimal order using greedy heuristic
    ///
    /// This is a polynomial-time approximation algorithm for large queries where
    /// exhaustive search is impractical. It uses a greedy strategy:
    ///
    /// 1. Start with the smallest table (by row count)
    /// 2. At each step, choose the next table that:
    ///    a) Has a join condition with already-joined tables (if possible)
    ///    b) Produces the smallest intermediate result
    /// 3. If no joinable tables remain, pick the smallest unjoined table (Cartesian product)
    ///
    /// Time complexity: O(n²) where n = number of tables
    /// Space complexity: O(n)
    ///
    /// This produces good (though not necessarily optimal) join orders for large queries,
    /// avoiding the factorial explosion of exhaustive search.
    pub(super) fn find_optimal_order_greedy(&self) -> Vec<String> {
        if self.all_tables.is_empty() {
            return Vec::new();
        }

        let mut joined_tables = BTreeSet::new();
        let mut remaining_tables: BTreeSet<String> = self.all_tables.clone();
        let mut join_order = Vec::new();
        let mut current_cardinality: usize;

        // Create initial filter state based on table cardinalities
        let mut filter_state = self.create_initial_filter_state();

        // Step 1: Start with the smallest table (lowest cardinality)
        let first_table = remaining_tables
            .iter()
            .min_by_key(|table| self.table_cardinalities.get(*table).copied().unwrap_or(10000))
            .unwrap()
            .clone();

        current_cardinality = self.table_cardinalities.get(&first_table).copied().unwrap_or(10000);
        joined_tables.insert(first_table.clone());
        remaining_tables.remove(&first_table);
        join_order.push(first_table);

        // Step 2: Greedily add tables one at a time
        while !remaining_tables.is_empty() {
            let mut best_table: Option<String> = None;
            let mut best_cost = JoinCost::new(usize::MAX, u64::MAX);

            // Try each remaining table that has a join edge to already-joined tables
            // This enforces connected subgraph enumeration (no CROSS JOINs)
            for candidate in &remaining_tables {
                // Only consider tables with join edges to already-joined tables
                if !self.has_join_edge(&joined_tables, candidate) {
                    continue;
                }

                let cost = self.estimate_join_cost_with_filters(
                    current_cardinality,
                    &joined_tables,
                    candidate,
                    &filter_state,
                );

                if best_table.is_none() || cost.total() < best_cost.total() {
                    best_table = Some(candidate.clone());
                    best_cost = cost;
                }
            }

            // Add the best table to the join order
            if let Some(table) = best_table {
                joined_tables.insert(table.clone());
                remaining_tables.remove(&table);
                join_order.push(table);
                // Update current cardinality to the result of this join
                current_cardinality = best_cost.cardinality;
                // Apply correlation adjustment for the new join
                filter_state.apply_correlation_for_join(&joined_tables);
            } else {
                // No connected tables remain - this is a disconnected join graph
                // This is common in SQLLogicTest where queries have multiple table-local
                // predicates but no join conditions (Cartesian products)
                // Pick smallest remaining table to minimize intermediate result size
                if let Some(table) = remaining_tables
                    .iter()
                    .min_by_key(|t| self.table_cardinalities.get(*t).copied().unwrap_or(10000))
                    .cloned()
                {
                    let cost = self.estimate_join_cost_with_filters(
                        current_cardinality,
                        &joined_tables,
                        &table,
                        &filter_state,
                    );
                    joined_tables.insert(table.clone());
                    remaining_tables.remove(&table);
                    join_order.push(table);
                    current_cardinality = cost.cardinality;
                    // Apply correlation adjustment for the new join
                    filter_state.apply_correlation_for_join(&joined_tables);
                } else {
                    break;
                }
            }
        }

        join_order
    }
}
