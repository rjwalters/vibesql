//! Join cost estimation
//!
//! This module implements cost estimation for join operations. Cost estimates
//! guide the search algorithm in selecting optimal join orders by predicting
//! the expense of different join sequences.

#![allow(clippy::doc_lazy_continuation)]

use std::collections::{BTreeSet, HashMap};

use super::context::JoinOrderContext;
use super::state::JoinCost;

impl JoinOrderContext {
    /// Extract table cardinalities from actual table statistics, adjusted by WHERE clause selectivity
    ///
    /// Uses real row counts from database tables and applies selectivity estimation
    /// for WHERE clause predicates that filter specific tables.
    ///
    /// # Parameters
    /// - `alias_to_table`: Maps table aliases (e.g., "n1", "n2") to actual table names (e.g., "nation")
    pub(super) fn extract_cardinalities_with_selectivity(
        analyzer: &crate::select::join::reorder::JoinOrderAnalyzer,
        database: &vibesql_storage::Database,
        table_local_predicates: &HashMap<String, Vec<vibesql_ast::Expression>>,
        alias_to_table: &HashMap<String, String>,
    ) -> std::collections::HashMap<String, usize> {
        let mut cardinalities = std::collections::HashMap::new();

        for table_name in analyzer.tables() {
            // Resolve alias to actual table name for database lookup
            let actual_table_name = alias_to_table
                .get(&table_name.to_lowercase())
                .cloned()
                .unwrap_or_else(|| table_name.clone());

            // Get actual table row count from database using the resolved table name
            let base_rows = database
                .get_table(&actual_table_name)
                .map(|t| t.row_count())
                .unwrap_or(10000); // Fallback for CTEs/subqueries

            // Apply selectivity estimation for local predicates on this table
            let estimated_rows = if let Some(predicates) = table_local_predicates.get(&table_name.to_lowercase()) {
                // Get table statistics for selectivity estimation (using actual table name)
                let stats = database
                    .get_table(&actual_table_name)
                    .and_then(|t| t.get_statistics());

                if let Some(stats) = stats {
                    // Estimate combined selectivity of all local predicates
                    let mut selectivity = 1.0;
                    for pred in predicates {
                        let pred_sel = crate::optimizer::selectivity::estimate_selectivity(pred, stats);
                        selectivity *= pred_sel;
                    }
                    // Apply selectivity to base row count
                    std::cmp::max(1, (base_rows as f64 * selectivity) as usize)
                } else {
                    // No stats available, use heuristic based on predicate analysis
                    // This is better than flat 30% per predicate
                    let mut selectivity = 1.0;
                    for pred in predicates {
                        let pred_sel = estimate_predicate_selectivity_heuristic(pred);
                        selectivity *= pred_sel;
                    }
                    std::cmp::max(1, (base_rows as f64 * selectivity) as usize)
                }
            } else {
                base_rows
            };

            // Debug logging
            if std::env::var("JOIN_REORDER_VERBOSE").is_ok() && base_rows != estimated_rows {
                let selectivity = estimated_rows as f64 / base_rows as f64;
                eprintln!(
                    "[JOIN_REORDER] Table {} cardinality: {} -> {} (selectivity: {:.4}, {} predicates)",
                    table_name, base_rows, estimated_rows, selectivity,
                    table_local_predicates.get(&table_name.to_lowercase()).map(|p| p.len()).unwrap_or(0)
                );
            }

            cardinalities.insert(table_name.clone(), estimated_rows);
        }

        cardinalities
    }


    /// Compute join selectivities for each edge based on column NDV (number of distinct values)
    ///
    /// For equijoin A.x = B.y, selectivity = 1 / max(NDV(A.x), NDV(B.y))
    ///
    /// **Important**: For composite join keys (multiple edges between same table pair),
    /// this function multiplies all individual selectivities together. This prevents
    /// memory explosions in queries like TPC-H Q9 where partsupp has TWO join conditions:
    /// - ps_suppkey = l_suppkey (selectivity ~0.01)
    /// - ps_partkey = l_partkey (selectivity ~0.05)
    ///
    /// Combined: 0.01 × 0.05 = 0.0005 (much more selective!)
    ///
    /// # Parameters
    /// - `alias_to_table`: Maps table aliases (e.g., "n1", "n2") to actual table names (e.g., "nation")
    pub(super) fn compute_edge_selectivities(
        edges: &[super::super::reorder::JoinEdge],
        database: &vibesql_storage::Database,
        alias_to_table: &HashMap<String, String>,
    ) -> HashMap<(String, String), f64> {
        // First, compute individual edge selectivities
        let mut individual_selectivities = Vec::new();

        for edge in edges {
            let left_table = edge.left_table.to_lowercase();
            let right_table = edge.right_table.to_lowercase();

            // Resolve aliases to actual table names for database lookups
            let actual_left_table = alias_to_table
                .get(&left_table)
                .cloned()
                .unwrap_or_else(|| edge.left_table.clone());
            let actual_right_table = alias_to_table
                .get(&right_table)
                .cloned()
                .unwrap_or_else(|| edge.right_table.clone());

            // Get NDV for left column (using actual table name)
            let left_ndv = database
                .get_table(&actual_left_table)
                .and_then(|t| t.get_statistics())
                .and_then(|stats| {
                    // Try exact match, uppercase, lowercase
                    stats.columns.get(&edge.left_column)
                        .or_else(|| stats.columns.get(&edge.left_column.to_uppercase()))
                        .or_else(|| stats.columns.get(&edge.left_column.to_lowercase()))
                })
                .map(|cs| cs.n_distinct)
                .unwrap_or(1000); // Fallback

            // Get NDV for right column (using actual table name)
            let right_ndv = database
                .get_table(&actual_right_table)
                .and_then(|t| t.get_statistics())
                .and_then(|stats| {
                    stats.columns.get(&edge.right_column)
                        .or_else(|| stats.columns.get(&edge.right_column.to_uppercase()))
                        .or_else(|| stats.columns.get(&edge.right_column.to_lowercase()))
                })
                .map(|cs| cs.n_distinct)
                .unwrap_or(1000); // Fallback

            // Join selectivity = 1 / max(NDV_left, NDV_right)
            let max_ndv = std::cmp::max(left_ndv, right_ndv).max(1);
            let selectivity = 1.0 / max_ndv as f64;

            // Debug logging
            if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                eprintln!(
                    "[JOIN_REORDER] Edge {}.{} = {}.{}: NDV({}, {}) -> selectivity {:.6}",
                    edge.left_table, edge.left_column,
                    edge.right_table, edge.right_column,
                    left_ndv, right_ndv, selectivity
                );
            }

            individual_selectivities.push(((left_table, right_table), selectivity));
        }

        // Now, combine selectivities for table pairs with multiple edges
        // Group by (table1, table2) and multiply selectivities
        let mut combined_selectivities = HashMap::new();

        for ((left_table, right_table), selectivity) in individual_selectivities {
            // Update forward direction
            let forward_key = (left_table.clone(), right_table.clone());
            let current = combined_selectivities.get(&forward_key).copied().unwrap_or(1.0);
            combined_selectivities.insert(forward_key.clone(), current * selectivity);

            // Update reverse direction
            let reverse_key = (right_table.clone(), left_table.clone());
            let current = combined_selectivities.get(&reverse_key).copied().unwrap_or(1.0);
            combined_selectivities.insert(reverse_key, current * selectivity);

            // Debug logging for composite keys
            if std::env::var("JOIN_REORDER_VERBOSE").is_ok() && current != 1.0 {
                eprintln!(
                    "[JOIN_REORDER] Composite key detected: {}-{} combined selectivity: {:.6} -> {:.6}",
                    left_table, right_table, current, current * selectivity
                );
            }
        }

        combined_selectivities
    }

    /// Estimate cost of joining next_table to already-joined tables
    ///
    /// # Parameters
    /// - `current_cardinality`: Size of intermediate result after all previous joins
    /// - `joined_tables`: Set of tables already joined (used to check for join edges)
    /// - `next_table`: Table being added to the join
    pub(super) fn estimate_join_cost(
        &self,
        current_cardinality: usize,
        joined_tables: &BTreeSet<String>,
        next_table: &str,
    ) -> JoinCost {
        if joined_tables.is_empty() {
            // First table: just a scan with selectivity
            let cardinality = self.table_cardinalities.get(next_table).copied().unwrap_or(10000);
            return JoinCost::new(cardinality, 0);
        }

        // Use current intermediate result size as left side of join
        let left_cardinality = current_cardinality;

        let right_cardinality = self.table_cardinalities.get(next_table).copied().unwrap_or(10000);

        // Get selectivity from pre-computed edge selectivities (NDV-based)
        // Find the best (most selective) edge connecting joined_tables to next_table
        let next_table_lower = next_table.to_lowercase();
        let selectivity = self.get_edge_selectivity(joined_tables, &next_table_lower);

        // Get join type to determine cardinality calculation
        let join_type = self.get_join_type(joined_tables, &next_table_lower);

        // Estimate output cardinality based on join type
        let output_cardinality = match join_type {
            vibesql_ast::JoinType::Semi | vibesql_ast::JoinType::Anti => {
                // SEMI/ANTI joins: output is at most left_cardinality (existence check)
                // For SEMI: each left row appears at most once (1 if match exists, 0 otherwise)
                // For ANTI: each left row appears at most once (1 if no match, 0 otherwise)
                // The selectivity represents the fraction of left rows that match (SEMI) or don't match (ANTI)
                std::cmp::max(
                    1,
                    std::cmp::min(
                        left_cardinality,
                        (left_cardinality as f64 * selectivity) as usize
                    )
                )
            }
            _ => {
                // INNER/LEFT/etc: use cross-product × selectivity
                std::cmp::max(
                    1,
                    (left_cardinality as f64 * right_cardinality as f64 * selectivity) as usize,
                )
            }
        };

        // Estimate operations: For hash join (our primary strategy), cost includes:
        // - Build hash table from left: O(left_cardinality) with overhead
        // - Probe with right: O(right_cardinality)
        //
        // Hash table build is more expensive than simple scan due to:
        // - Memory allocation
        // - Hash computation
        // - Collision resolution
        //
        // We model this with a 2x multiplier on the build side to account for overhead.
        // This encourages the optimizer to prefer smaller build sides.
        let operations = if self.has_join_edge(joined_tables, next_table) {
            // Hash join: build cost (2x) + probe cost (1x)
            // This reflects that building a hash table is more expensive than probing
            let build_cost = (left_cardinality as u64) * 2;
            let probe_cost = right_cardinality as u64;
            build_cost + probe_cost
        } else {
            // Cross join: quadratic cost (nested loop)
            (left_cardinality as u64) * (right_cardinality as u64)
        };

        // Verbose logging for debugging join order decisions
        if self.config.verbose {
            let left_desc = if joined_tables.is_empty() {
                "(start)".to_string()
            } else {
                format!("{{{}}}({} rows)", joined_tables.iter().cloned().collect::<Vec<_>>().join(","), left_cardinality)
            };
            let right_desc = format!("{}({} rows)", next_table, right_cardinality);
            eprintln!(
                "[JOIN_COST] {} + {} -> output={}, ops={}, selectivity={:.6}, type={:?}",
                left_desc,
                right_desc,
                output_cardinality,
                operations,
                selectivity,
                join_type
            );
        }

        JoinCost::new(output_cardinality, operations)
    }

    /// Get the best (most selective) edge for joining next_table to any of the joined_tables
    ///
    /// Note: Composite join keys (multiple edges between same table pair) are already
    /// handled in compute_edge_selectivities, so the selectivities here are combined.
    fn get_edge_selectivity(&self, joined_tables: &BTreeSet<String>, next_table: &str) -> f64 {
        let mut best_selectivity = 1.0; // Default for cross join (no filtering)

        for joined_table in joined_tables {
            let joined_lower = joined_table.to_lowercase();
            let next_lower = next_table.to_lowercase();

            // Selectivity is pre-computed with composite keys already multiplied
            if let Some(&sel) = self.edge_selectivities.get(&(joined_lower, next_lower)) {
                if sel < best_selectivity {
                    best_selectivity = sel;
                }
            }
        }

        best_selectivity
    }

    /// Find the join type for joining next_table to any of the joined_tables
    ///
    /// If multiple edges exist with different join types, returns the "most restrictive" type.
    /// Priority: SEMI > ANTI > INNER (SEMI/ANTI are more selective)
    fn get_join_type(&self, joined_tables: &BTreeSet<String>, next_table: &str) -> vibesql_ast::JoinType {
        use vibesql_ast::JoinType;

        let mut found_type = JoinType::Inner; // Default

        for edge in &self.edges {
            if edge.involves_table(next_table) {
                for joined_table in joined_tables {
                    if edge.involves_table(joined_table) {
                        // Found an edge connecting joined_tables to next_table
                        match (&found_type, &edge.join_type) {
                            (_, JoinType::Semi) => found_type = JoinType::Semi,
                            (JoinType::Inner, JoinType::Anti) => found_type = JoinType::Anti,
                            (JoinType::Inner, t) => found_type = t.clone(),
                            _ => {}
                        }
                    }
                }
            }
        }

        found_type
    }

    /// Check if there's a join edge connecting the joined tables and next table
    pub(super) fn has_join_edge(&self, joined_tables: &BTreeSet<String>, next_table: &str) -> bool {
        for edge in &self.edges {
            if edge.involves_table(next_table) {
                for joined_table in joined_tables {
                    if edge.involves_table(joined_table) {
                        return true;
                    }
                }
            }
        }
        false
    }
}

/// Estimate predicate selectivity without statistics using heuristics
///
/// This function analyzes the predicate structure to provide better estimates
/// than a flat 30% per predicate. It considers:
/// - Equality predicates: more selective (10%)
/// - Range predicates: less selective (25-33%)
/// - IN lists: depends on number of values
/// - LIKE patterns: depends on wildcards
/// - Complex expressions: conservative (50%)
fn estimate_predicate_selectivity_heuristic(pred: &vibesql_ast::Expression) -> f64 {
    use vibesql_ast::{BinaryOperator, Expression};

    match pred {
        // AND: multiply selectivities
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            let left_sel = estimate_predicate_selectivity_heuristic(left);
            let right_sel = estimate_predicate_selectivity_heuristic(right);
            left_sel * right_sel
        }

        // OR: 1 - ((1 - s1) * (1 - s2))
        Expression::BinaryOp { op: BinaryOperator::Or, left, right } => {
            let left_sel = estimate_predicate_selectivity_heuristic(left);
            let right_sel = estimate_predicate_selectivity_heuristic(right);
            1.0 - ((1.0 - left_sel) * (1.0 - right_sel))
        }

        // Equality: highly selective (10%)
        Expression::BinaryOp { op: BinaryOperator::Equal, .. } => 0.10,

        // Inequality: less selective (90%)
        Expression::BinaryOp { op: BinaryOperator::NotEqual, .. } => 0.90,

        // Range comparisons: moderately selective (25%)
        Expression::BinaryOp {
            op: BinaryOperator::LessThan |
                BinaryOperator::LessThanOrEqual |
                BinaryOperator::GreaterThan |
                BinaryOperator::GreaterThanOrEqual,
            ..
        } => 0.25,

        // BETWEEN: similar to range (33%)
        Expression::Between { .. } => 0.33,

        // IN list: depends on number of values (estimate 5% per value, cap at 50%)
        Expression::InList { values, negated: false, .. } => {
            (values.len() as f64 * 0.05).min(0.50)
        }
        Expression::InList { values, negated: true, .. } => {
            1.0 - (values.len() as f64 * 0.05).min(0.50)
        }

        // LIKE: depends on pattern
        Expression::Like { pattern, .. } => {
            // Try to extract pattern string
            if let Expression::Literal(vibesql_types::SqlValue::Varchar(s)) = pattern.as_ref() {
                if s.starts_with('%') && s.ends_with('%') {
                    0.10 // %pattern% - substring search
                } else if s.starts_with('%') || s.ends_with('%') {
                    0.15 // prefix or suffix search
                } else {
                    0.10 // exact match
                }
            } else {
                0.15 // unknown pattern
            }
        }

        // IS NULL / IS NOT NULL: assume 10% nulls
        Expression::IsNull { negated: false, .. } => 0.10,
        Expression::IsNull { negated: true, .. } => 0.90,

        // NOT: inverse
        Expression::UnaryOp { op: vibesql_ast::UnaryOperator::Not, expr } => {
            1.0 - estimate_predicate_selectivity_heuristic(expr)
        }

        // Complex expressions: conservative estimate
        _ => 0.50,
    }
}
