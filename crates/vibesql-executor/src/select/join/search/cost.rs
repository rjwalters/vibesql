//! Join cost estimation
//!
//! This module implements cost estimation for join operations. Cost estimates
//! guide the search algorithm in selecting optimal join orders by predicting
//! the expense of different join sequences.

#![allow(clippy::doc_lazy_continuation)]

use std::collections::{BTreeSet, HashMap};

use super::context::JoinOrderContext;
use super::state::{CascadingFilterState, JoinCost};

impl JoinOrderContext {
    /// Extract base table cardinalities (before any filters applied)
    ///
    /// This is used for cascading filter awareness - we need to know both
    /// the original table size and the filtered size to compute filter selectivity.
    ///
    /// # Parameters
    /// - `alias_to_table`: Maps table aliases (e.g., "n1", "n2") to actual table names (e.g., "nation")
    pub(super) fn extract_base_cardinalities(
        analyzer: &crate::select::join::reorder::JoinOrderAnalyzer,
        database: &vibesql_storage::Database,
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
            let base_rows =
                database.get_table(&actual_table_name).map(|t| t.row_count()).unwrap_or(10000); // Fallback for CTEs/subqueries

            cardinalities.insert(table_name.clone(), base_rows);
        }

        cardinalities
    }

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
            let base_rows =
                database.get_table(&actual_table_name).map(|t| t.row_count()).unwrap_or(10000); // Fallback for CTEs/subqueries

            // Apply selectivity estimation for local predicates on this table
            let estimated_rows = if let Some(predicates) =
                table_local_predicates.get(&table_name.to_lowercase())
            {
                // Get table statistics for selectivity estimation (using actual table name)
                let stats = database.get_table(&actual_table_name).and_then(|t| t.get_statistics());

                if let Some(stats) = stats {
                    // Estimate combined selectivity of all local predicates
                    let mut selectivity = 1.0;
                    for pred in predicates {
                        let pred_sel =
                            crate::optimizer::selectivity::estimate_selectivity(pred, stats);
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
    /// **FK Detection Enhancement**: When a join involves a primary key column (NDV == row_count)
    /// joining to a foreign key column (NDV < row_count), we use improved selectivity estimation
    /// based on the FK cardinality ratio. This handles star schema patterns better:
    /// - `customer.c_custkey (PK) = orders.o_custkey (FK)`: ~15 orders per customer
    /// - Selectivity accounts for the FK multiplicity rather than assuming uniform distribution
    ///
    /// **Important**: For composite join keys (multiple edges between same table pair),
    /// this function uses the MAXIMUM selectivity instead of multiplying them together.
    /// Multiplying assumes column independence, which is incorrect for composite FK-PK
    /// relationships like TPC-H Q9's partsupp-lineitem join on (partkey, suppkey).
    /// Using MAX prevents catastrophic underestimation of result cardinality.
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
            let actual_left_table =
                alias_to_table.get(&left_table).cloned().unwrap_or_else(|| edge.left_table.clone());
            let actual_right_table = alias_to_table
                .get(&right_table)
                .cloned()
                .unwrap_or_else(|| edge.right_table.clone());

            // Get table statistics for FK detection
            let left_stats = database
                .get_table(&actual_left_table)
                .and_then(|t| Some((t.row_count(), t.get_statistics()?)));
            let right_stats = database
                .get_table(&actual_right_table)
                .and_then(|t| Some((t.row_count(), t.get_statistics()?)));

            // Get NDV for left column (using actual table name)
            let left_ndv = left_stats
                .as_ref()
                .and_then(|(_, stats)| {
                    stats
                        .columns
                        .get(&edge.left_column)
                        .or_else(|| stats.columns.get(&edge.left_column.to_uppercase()))
                        .or_else(|| stats.columns.get(&edge.left_column.to_lowercase()))
                })
                .map(|cs| cs.n_distinct)
                .unwrap_or(1000); // Fallback

            // Get NDV for right column (using actual table name)
            let right_ndv = right_stats
                .as_ref()
                .and_then(|(_, stats)| {
                    stats
                        .columns
                        .get(&edge.right_column)
                        .or_else(|| stats.columns.get(&edge.right_column.to_uppercase()))
                        .or_else(|| stats.columns.get(&edge.right_column.to_lowercase()))
                })
                .map(|cs| cs.n_distinct)
                .unwrap_or(1000); // Fallback

            // Get row counts for FK detection
            let left_row_count = left_stats.as_ref().map(|(rc, _)| *rc).unwrap_or(10000);
            let right_row_count = right_stats.as_ref().map(|(rc, _)| *rc).unwrap_or(10000);

            // Detect PK-FK relationships for improved selectivity estimation
            // A column is likely a PK if NDV == row_count (all unique values)
            let left_is_pk = is_likely_primary_key(left_ndv, left_row_count);
            let right_is_pk = is_likely_primary_key(right_ndv, right_row_count);

            let selectivity = compute_pk_fk_selectivity(
                left_ndv,
                right_ndv,
                left_row_count,
                right_row_count,
                left_is_pk,
                right_is_pk,
            );

            // Debug logging
            if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                let pk_fk_info = if left_is_pk && !right_is_pk {
                    format!(
                        " [PK-FK: left is PK, ratio={:.1}]",
                        right_row_count as f64 / right_ndv.max(1) as f64
                    )
                } else if right_is_pk && !left_is_pk {
                    format!(
                        " [PK-FK: right is PK, ratio={:.1}]",
                        left_row_count as f64 / left_ndv.max(1) as f64
                    )
                } else if left_is_pk && right_is_pk {
                    " [PK-PK join]".to_string()
                } else {
                    String::new()
                };
                eprintln!(
                    "[JOIN_REORDER] Edge {}.{} = {}.{}: NDV({}, {}), rows({}, {}) -> selectivity {:.6}{}",
                    edge.left_table, edge.left_column,
                    edge.right_table, edge.right_column,
                    left_ndv, right_ndv,
                    left_row_count, right_row_count,
                    selectivity,
                    pk_fk_info
                );
            }

            individual_selectivities.push(((left_table, right_table), selectivity));
        }

        // Now, combine selectivities for table pairs with multiple edges
        //
        // IMPORTANT: For composite keys (multiple edges between same table pair), we use
        // MAX selectivity instead of multiplying them. Multiplying assumes column independence,
        // which is incorrect for composite FK-PK relationships:
        //
        // Example: partsupp-lineitem join on (partkey, suppkey):
        // - partsupp has composite PK (ps_partkey, ps_suppkey) = 80K unique combinations
        // - lineitem has FK (l_partkey, l_suppkey) referencing partsupp
        // - Each lineitem matches exactly ONE partsupp row
        //
        // With independence (WRONG):
        //   selectivity = 0.000126 * 0.001 = 0.000000126
        //   result = 80K * 600K * 0.000000126 = 6 rows (catastrophically wrong!)
        //
        // With MAX selectivity (CORRECT):
        //   selectivity = max(0.000126, 0.001) = 0.001
        //   result = 80K * 600K * 0.001 = 48M rows (still overestimates, but safe)
        //
        // The overestimate is acceptable because:
        // 1. It prevents memory explosions from underestimation
        // 2. Cost model will still prefer filtered tables earlier in join order
        // 3. Downstream joins will reduce cardinality as filters apply
        let mut combined_selectivities = HashMap::new();

        for ((left_table, right_table), selectivity) in individual_selectivities {
            // Update forward direction - use MAX for composite keys
            let forward_key = (left_table.clone(), right_table.clone());
            let current: f64 = combined_selectivities.get(&forward_key).copied().unwrap_or(0.0);

            // For first edge, just use the selectivity
            // For subsequent edges (composite key), use MAX instead of product
            let new_selectivity = if current == 0.0 {
                selectivity
            } else {
                // Composite key detected: use MAX to avoid catastrophic underestimation
                // The max selectivity is the most conservative (least selective) estimate
                current.max(selectivity)
            };
            combined_selectivities.insert(forward_key.clone(), new_selectivity);

            // Update reverse direction
            let reverse_key = (right_table.clone(), left_table.clone());
            let current_rev: f64 = combined_selectivities.get(&reverse_key).copied().unwrap_or(0.0);
            let new_selectivity_rev = if current_rev == 0.0 {
                selectivity
            } else {
                current_rev.max(selectivity)
            };
            combined_selectivities.insert(reverse_key, new_selectivity_rev);

            // Debug logging for composite keys
            if std::env::var("JOIN_REORDER_VERBOSE").is_ok() && current != 0.0 {
                eprintln!(
                    "[JOIN_REORDER] Composite key detected: {}-{} combined selectivity: {:.6} -> {:.6} (using MAX, not product)",
                    left_table, right_table, current, new_selectivity
                );
            }
        }

        combined_selectivities
    }

    /// Estimate cost of joining next_table to already-joined tables
    ///
    /// NOTE: This function is kept for reference/debugging but is superseded by
    /// `estimate_join_cost_with_filters` which accounts for cascading filter correlation.
    ///
    /// # Parameters
    /// - `current_cardinality`: Size of intermediate result after all previous joins
    /// - `joined_tables`: Set of tables already joined (used to check for join edges)
    /// - `next_table`: Table being added to the join
    #[allow(dead_code)]
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
                        (left_cardinality as f64 * selectivity) as usize,
                    ),
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
                format!(
                    "{{{}}}({} rows)",
                    joined_tables.iter().cloned().collect::<Vec<_>>().join(","),
                    left_cardinality
                )
            };
            let right_desc = format!("{}({} rows)", next_table, right_cardinality);
            eprintln!(
                "[JOIN_COST] {} + {} -> output={}, ops={}, selectivity={:.6}, type={:?}",
                left_desc, right_desc, output_cardinality, operations, selectivity, join_type
            );
        }

        JoinCost::new(output_cardinality, operations)
    }

    /// Estimate cost of joining next_table to already-joined tables with cascading filter awareness
    ///
    /// This version accounts for filter correlation - when tables have local predicates applied,
    /// the rows that survive the filter are not randomly distributed. Joins through filtered
    /// tables produce fewer rows than independent selectivity would suggest.
    ///
    /// # Parameters
    /// - `current_cardinality`: Size of intermediate result after all previous joins
    /// - `joined_tables`: Set of tables already joined (used to check for join edges)
    /// - `next_table`: Table being added to the join
    /// - `filter_state`: Tracks which tables have been filtered and their correlation factor
    pub(super) fn estimate_join_cost_with_filters(
        &self,
        current_cardinality: usize,
        joined_tables: &BTreeSet<String>,
        next_table: &str,
        filter_state: &CascadingFilterState,
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

        // Apply cascading filter correlation adjustment
        //
        // Key insight: When joining through filtered tables, the intermediate result
        // is "tighter" than independent selectivity would suggest. For example:
        //
        // Q3: customer (filtered by c_mktsegment) -> orders (filtered by o_orderdate)
        //
        // Independent selectivity would predict:
        //   customer: 150K * 0.2 = 30K (20% match segment)
        //   orders: 1.5M * 0.5 = 750K (50% match date filter)
        //   join: 30K * 750K * (1/150K) = 150K rows
        //
        // But in reality, customers matching the segment filter may have DIFFERENT
        // order patterns than the general population. The join selectivity is correlated
        // with the filter predicates.
        //
        // We apply a correlation factor that reduces the estimated cardinality
        // when joining through filtered tables.
        let correlation_adjustment = filter_state.correlation_factor;

        // Check if the next table has a filter applied - if so, that filter
        // further reduces the correlation factor for this specific join
        let next_table_selectivity = filter_state.get_table_selectivity(&next_table_lower);
        let effective_correlation = if next_table_selectivity < 1.0 {
            // The next table is also filtered, apply additional correlation reduction
            // This captures the compound effect of multiple filters in the join chain
            correlation_adjustment * 0.9 // Additional 10% reduction per filtered table
        } else {
            correlation_adjustment
        };

        // Estimate output cardinality based on join type with correlation adjustment
        let output_cardinality = match join_type {
            vibesql_ast::JoinType::Semi | vibesql_ast::JoinType::Anti => {
                // SEMI/ANTI joins: output is at most left_cardinality (existence check)
                let base_estimate = (left_cardinality as f64 * selectivity) as usize;
                std::cmp::max(
                    1,
                    std::cmp::min(
                        left_cardinality,
                        (base_estimate as f64 * effective_correlation) as usize,
                    ),
                )
            }
            _ => {
                // INNER/LEFT/etc: use cross-product × selectivity × correlation
                let base_estimate =
                    left_cardinality as f64 * right_cardinality as f64 * selectivity;
                let correlated_estimate = base_estimate * effective_correlation;
                std::cmp::max(1, correlated_estimate as usize)
            }
        };

        // Estimate operations (same as before - correlation doesn't affect operation count)
        let operations = if self.has_join_edge(joined_tables, next_table) {
            // Hash join: build cost (2x) + probe cost (1x)
            let build_cost = (left_cardinality as u64) * 2;
            let probe_cost = right_cardinality as u64;
            build_cost + probe_cost
        } else {
            // Cross join: quadratic cost (nested loop)
            (left_cardinality as u64) * (right_cardinality as u64)
        };

        // Verbose logging for debugging join order decisions with filter info
        if self.config.verbose {
            let left_desc = format!(
                "{{{}}}({} rows)",
                joined_tables.iter().cloned().collect::<Vec<_>>().join(","),
                left_cardinality
            );
            let right_desc = format!("{}({} rows)", next_table, right_cardinality);
            let filter_info = if effective_correlation < 1.0 {
                format!(", corr_factor={:.4}", effective_correlation)
            } else {
                String::new()
            };
            eprintln!(
                "[JOIN_COST_FILTERED] {} + {} -> output={}, ops={}, selectivity={:.6}, type={:?}{}",
                left_desc,
                right_desc,
                output_cardinality,
                operations,
                selectivity,
                join_type,
                filter_info
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
    fn get_join_type(
        &self,
        joined_tables: &BTreeSet<String>,
        next_table: &str,
    ) -> vibesql_ast::JoinType {
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
            op:
                BinaryOperator::LessThan
                | BinaryOperator::LessThanOrEqual
                | BinaryOperator::GreaterThan
                | BinaryOperator::GreaterThanOrEqual,
            ..
        } => 0.25,

        // BETWEEN: similar to range (33%)
        Expression::Between { .. } => 0.33,

        // IN list: depends on number of values (estimate 5% per value, cap at 50%)
        Expression::InList { values, negated: false, .. } => (values.len() as f64 * 0.05).min(0.50),
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

/// Detect if a column is likely a primary key based on statistics
///
/// A column is considered a likely primary key if:
/// - NDV (number of distinct values) equals row count (all values unique)
/// - Or NDV is very close to row count (allowing for sampling variance)
///
/// This heuristic works well for:
/// - Auto-increment IDs
/// - UUID columns
/// - Composite keys (when NDV is computed on the combined key)
fn is_likely_primary_key(ndv: usize, row_count: usize) -> bool {
    if row_count == 0 {
        return false;
    }

    // Allow 1% tolerance for sampling variance
    // For small tables, require exact match
    if row_count < 100 {
        ndv >= row_count
    } else {
        let ratio = ndv as f64 / row_count as f64;
        ratio >= 0.99
    }
}

/// Compute selectivity for a join considering PK-FK relationships
///
/// For PK-FK joins (dimension table to fact table), the selectivity model is:
/// - Each FK value matches exactly one PK value
/// - The FK side may have multiple rows per distinct FK value (cardinality ratio)
///
/// Formula for PK-FK join where left is PK:
///   selectivity = 1 / left_ndv (each FK row finds exactly one match)
///
/// For FK-PK join where right is PK:
///   selectivity = 1 / right_ndv
///
/// For non-PK-FK joins (both sides have duplicates):
///   selectivity = 1 / max(left_ndv, right_ndv) (traditional formula)
///
/// For PK-PK joins (1:1 relationship):
///   selectivity = 1 / max(left_ndv, right_ndv)
fn compute_pk_fk_selectivity(
    left_ndv: usize,
    right_ndv: usize,
    _left_row_count: usize,
    _right_row_count: usize,
    left_is_pk: bool,
    right_is_pk: bool,
) -> f64 {
    // Ensure at least 1 to avoid division by zero
    let left_ndv = left_ndv.max(1);
    let right_ndv = right_ndv.max(1);

    match (left_is_pk, right_is_pk) {
        // PK-FK join: left is PK (dimension), right is FK (fact)
        // Each FK value matches at most one PK value
        // Selectivity based on PK uniqueness
        (true, false) => 1.0 / left_ndv as f64,

        // FK-PK join: left is FK (fact), right is PK (dimension)
        // Each FK value matches at most one PK value
        (false, true) => 1.0 / right_ndv as f64,

        // PK-PK join (1:1 relationship) or non-PK join
        // Use traditional formula: 1 / max(NDV)
        _ => {
            let max_ndv = std::cmp::max(left_ndv, right_ndv);
            1.0 / max_ndv as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_likely_primary_key() {
        // Exact match - definitely PK
        assert!(is_likely_primary_key(1000, 1000));

        // Small table - require exact match
        assert!(is_likely_primary_key(50, 50));
        assert!(!is_likely_primary_key(49, 50));

        // Large table - allow 1% variance
        assert!(is_likely_primary_key(990, 1000)); // 99% unique
        assert!(!is_likely_primary_key(980, 1000)); // 98% unique

        // Edge cases
        assert!(!is_likely_primary_key(0, 0));
        assert!(!is_likely_primary_key(100, 0));
    }

    #[test]
    fn test_compute_pk_fk_selectivity() {
        // PK-FK: left is PK (customer), right is FK (orders)
        // customer.c_custkey (PK, 150K unique) = orders.o_custkey (FK, 100K unique out of 1.5M rows)
        let selectivity = compute_pk_fk_selectivity(
            150_000,   // left_ndv (customer PK)
            100_000,   // right_ndv (orders FK)
            150_000,   // left_row_count (customer)
            1_500_000, // right_row_count (orders)
            true,      // left is PK
            false,     // right is FK
        );
        // Should be 1/150K based on PK uniqueness
        assert!((selectivity - 1.0 / 150_000.0).abs() < 1e-10);

        // FK-PK: left is FK (orders), right is PK (customer)
        let selectivity = compute_pk_fk_selectivity(
            100_000,   // left_ndv (orders FK)
            150_000,   // right_ndv (customer PK)
            1_500_000, // left_row_count (orders)
            150_000,   // right_row_count (customer)
            false,     // left is FK
            true,      // right is PK
        );
        // Should be 1/150K based on PK uniqueness
        assert!((selectivity - 1.0 / 150_000.0).abs() < 1e-10);

        // Non-PK join: both sides have duplicates
        let selectivity = compute_pk_fk_selectivity(
            1000,  // left_ndv
            2000,  // right_ndv
            5000,  // left_row_count
            8000,  // right_row_count
            false, // not PK
            false, // not PK
        );
        // Should be 1/max(1000, 2000) = 1/2000
        assert!((selectivity - 1.0 / 2000.0).abs() < 1e-10);

        // PK-PK join: 1:1 relationship
        let selectivity = compute_pk_fk_selectivity(
            1000, // left_ndv
            1000, // right_ndv
            1000, // left_row_count
            1000, // right_row_count
            true, // both PK
            true, // both PK
        );
        // Should be 1/max(1000, 1000) = 1/1000
        assert!((selectivity - 1.0 / 1000.0).abs() < 1e-10);
    }
}
