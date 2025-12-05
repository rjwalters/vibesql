//! Sequential depth-first search strategy
//!
//! This module implements the sequential DFS strategy for join order optimization.
//! It uses recursive backtracking with branch-and-bound pruning to explore the
//! search space efficiently.

use std::collections::BTreeSet;

use super::context::JoinOrderContext;
use super::state::{JoinCost, SearchState};

impl JoinOrderContext {
    /// Find optimal order using sequential DFS (original algorithm)
    pub(super) fn find_optimal_order_dfs(&self) -> Vec<String> {
        let num_tables = self.all_tables.len();

        // For large queries (>= 8 tables), use greedy heuristic instead of exhaustive search
        // Exhaustive search becomes prohibitively expensive:
        // 8 tables: 40,320 permutations, 10 tables: 3,628,800, 18 tables: 6.4e15
        // Even with pruning, we hit iteration limits and get poor results
        if num_tables >= 8 {
            return self.find_optimal_order_greedy();
        }

        // Create initial filter state based on table cardinalities
        let initial_filter_state = self.create_initial_filter_state();

        let initial_state = SearchState {
            joined_tables: BTreeSet::new(),
            cost_so_far: JoinCost::new(0, 0),
            order: Vec::new(),
            current_cardinality: 0,
            filter_state: initial_filter_state,
        };

        let mut best_cost = u64::MAX;
        let mut best_order = vec![];
        let mut iterations = 0;

        // Maximum iterations to prevent pathological cases
        // For n tables, factorial complexity means:
        // 3 tables: 6 iterations, 4 tables: 24, 5 tables: 120, 6 tables: 720, 7 tables: 5,040
        // Cap at 10000 to allow more exploration for 7-table queries
        let max_iterations = 10000;

        self.search_recursive(
            initial_state,
            &mut best_cost,
            &mut best_order,
            &mut iterations,
            max_iterations,
        );

        // If we hit iteration limit without finding a complete ordering, fall back to greedy
        if best_order.is_empty() {
            return self.find_optimal_order_greedy();
        }

        best_order
    }

    /// Recursive depth-first search with pruning
    fn search_recursive(
        &self,
        state: SearchState,
        best_cost: &mut u64,
        best_order: &mut Vec<String>,
        iterations: &mut u32,
        max_iterations: u32,
    ) {
        // Early termination: check iteration limit
        *iterations += 1;
        if *iterations > max_iterations {
            return;
        }

        // Base case: all tables joined
        if state.joined_tables.len() == self.all_tables.len() {
            let total_cost = state.cost_so_far.total();
            if total_cost < *best_cost {
                *best_cost = total_cost;
                *best_order = state.order.clone();
            }
            return;
        }

        // Pruning: if current cost exceeds best, don't explore further
        if state.cost_so_far.total() >= *best_cost {
            return;
        }

        // Try adding each unjoined table that can be joined to already-joined tables
        // For the first table, any table is valid
        // For subsequent tables, prefer tables with join edges but allow disconnected tables
        // when the join graph is not fully connected (e.g., Cartesian products)
        let candidates: Vec<&String> = if state.joined_tables.is_empty() {
            // First table: any unjoined table
            self.all_tables.iter().filter(|t| !state.joined_tables.contains(*t)).collect()
        } else {
            // Try to find tables with join edges first (connected subgraph)
            let connected: Vec<&String> = self
                .all_tables
                .iter()
                .filter(|t| !state.joined_tables.contains(*t))
                .filter(|t| self.has_join_edge(&state.joined_tables, t))
                .collect();

            if !connected.is_empty() {
                // Prefer connected tables to avoid Cartesian products
                connected
            } else {
                // No connected tables remain - this is a disconnected join graph
                // Allow ANY remaining table (Cartesian product is unavoidable)
                // For Cartesian products, order by cardinality (smallest first) to minimize
                // intermediate result sizes
                let mut unjoined: Vec<&String> =
                    self.all_tables.iter().filter(|t| !state.joined_tables.contains(*t)).collect();

                // Sort by cardinality (smallest first) to minimize intermediate sizes
                unjoined
                    .sort_by_key(|t| self.table_cardinalities.get(*t).copied().unwrap_or(10000));

                unjoined
            }
        };

        for next_table in candidates {
            if state.joined_tables.contains(next_table) {
                continue;
            }

            // Estimate cost of joining this table with cascading filter awareness
            let join_cost = self.estimate_join_cost_with_filters(
                state.current_cardinality,
                &state.joined_tables,
                next_table,
                &state.filter_state,
            );

            // Create new state with this table added and updated filter state
            let mut next_state = state.clone();
            next_state.joined_tables.insert(next_table.clone());
            next_state.cost_so_far = JoinCost::new(
                state.cost_so_far.cardinality + join_cost.cardinality,
                state.cost_so_far.operations + join_cost.operations,
            );
            next_state.order.push(next_table.clone());
            // Update current cardinality to the result of this join
            next_state.current_cardinality = join_cost.cardinality;
            // Apply correlation adjustment for the new join
            next_state.filter_state.apply_correlation_for_join(&next_state.joined_tables);

            // Recursively search from this state
            self.search_recursive(next_state, best_cost, best_order, iterations, max_iterations);
        }
    }
}
