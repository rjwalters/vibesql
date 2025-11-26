//! Join execution with optional reordering support
//!
//! This module handles the "Decorator Pattern" for join execution:
//! - Normal execution: Process join tree recursively as-is
//! - Reordered execution: Flatten the join tree and execute in specified order

#![allow(dead_code)]

use std::collections::HashMap;

use vibesql_ast::FromClause;

use super::{
    cte::CteResult,
    join::{nested_loop_join, FromResult},
};
use crate::{errors::ExecutorError, optimizer::PredicateDecomposition, timeout::TimeoutContext};

/// Specification for reordered join execution
#[derive(Debug, Clone)]
pub struct JoinReorderingSpec {
    /// Tables in the optimal join order
    pub table_order: Vec<String>,
    /// Enable reordering application
    pub enabled: bool,
}

impl JoinReorderingSpec {
    /// Create a new reordering specification
    pub fn new(table_order: Vec<String>) -> Self {
        JoinReorderingSpec { table_order, enabled: true }
    }

    /// Check if reordering should be applied
    pub fn should_apply(&self) -> bool {
        self.enabled && !self.table_order.is_empty()
    }
}

/// Flattened join information for reordered execution
#[derive(Debug)]
struct FlattenedJoinTree {
    /// List of (table_name, from_clause) pairs in left-to-right order
    tables: Vec<(String, FromClause)>,
    /// Map of table name to its index in the flattened list
    table_index: HashMap<String, usize>,
}

/// Flatten a join tree into a list of tables and joins
///
/// Returns None if the tree cannot be flattened (contains non-join operations)
fn flatten_join_tree(from: &FromClause) -> Option<FlattenedJoinTree> {
    let mut tables = Vec::new();
    let mut table_index = HashMap::new();

    fn flatten_recursive(
        from: &FromClause,
        tables: &mut Vec<(String, FromClause)>,
        table_index: &mut HashMap<String, usize>,
    ) -> Option<()> {
        match from {
            FromClause::Table { name, alias } => {
                let effective_name = alias.clone().unwrap_or_else(|| name.clone());
                table_index.insert(effective_name.clone(), tables.len());
                tables.push((effective_name, from.clone()));
                Some(())
            }
            FromClause::Subquery { query: _, alias } => {
                // For now, treat subqueries as single units that can't be reordered
                table_index.insert(alias.clone(), tables.len());
                tables.push((alias.clone(), from.clone()));
                Some(())
            }
            FromClause::Join { left, right, .. } => {
                flatten_recursive(left, tables, table_index)?;
                flatten_recursive(right, tables, table_index)?;
                Some(())
            }
        }
    }

    flatten_recursive(from, &mut tables, &mut table_index)?;

    if tables.is_empty() {
        return None;
    }

    Some(FlattenedJoinTree { tables, table_index })
}

/// Extract join conditions from a join tree
///
/// Returns a list of (left_table_name, right_table_name, join_condition) tuples
fn extract_join_conditions(from: &FromClause) -> Vec<(String, String, Option<vibesql_ast::Expression>)> {
    let mut conditions = Vec::new();

    fn extract_recursive(
        from: &FromClause,
        conditions: &mut Vec<(String, String, Option<vibesql_ast::Expression>)>,
    ) -> Option<Vec<String>> {
        match from {
            FromClause::Table { name, alias } => {
                let table_name = alias.clone().unwrap_or_else(|| name.clone());
                Some(vec![table_name])
            }
            FromClause::Subquery { alias, .. } => Some(vec![alias.clone()]),
            FromClause::Join { left, right, condition, .. } => {
                let left_tables = extract_recursive(left, conditions)?;
                let right_tables = extract_recursive(right, conditions)?;

                // Record this join condition
                if let Some(left_table) = left_tables.first() {
                    if let Some(right_table) = right_tables.first() {
                        conditions.push((
                            left_table.clone(),
                            right_table.clone(),
                            condition.clone(),
                        ));
                    }
                }

                // Return all tables involved in this subtree
                let mut all_tables = left_tables;
                all_tables.extend(right_tables);
                Some(all_tables)
            }
        }
    }

    extract_recursive(from, &mut conditions);
    conditions
}

/// Attempt to execute a join tree in reordered sequence
///
/// This function returns Some(Ok(...)) if reordering was applied,
/// Some(Err(...)) if an error occurred during reordered execution,
/// and None if reordering should not be applied (fall back to standard).
///
/// Implementation:
/// - Flattens the join tree into individual tables
/// - Executes tables in the specified order via execute_from_fn
/// - Joins them using extracted join conditions
/// - For each new table, searches for a join condition with ANY previously-joined table (not just
///   the immediately preceding one, which is critical for star joins)
pub(super) fn execute_reordered_join<F>(
    from: &FromClause,
    _cte_results: &HashMap<String, CteResult>,
    database: &vibesql_storage::Database,
    execute_from_fn: F,
    predicates: Option<&PredicateDecomposition>,
    reorder_spec: &JoinReorderingSpec,
) -> Option<Result<FromResult, ExecutorError>>
where
    F: Fn(&FromClause, Option<&PredicateDecomposition>) -> Result<FromResult, ExecutorError> + Copy,
{
    if !reorder_spec.should_apply() {
        return None;
    }

    // Only attempt reordering on Join nodes
    if !matches!(from, FromClause::Join { .. }) {
        return None;
    }

    // Flatten the join tree
    let flattened = flatten_join_tree(from)?;

    // Check that all tables in reorder_spec exist in the flattened tree
    for table_name in &reorder_spec.table_order {
        if !flattened.table_index.contains_key(table_name) {
            return None; // Invalid reordering, fall back to standard
        }
    }

    // Extract all join conditions from the original tree
    let all_conditions = extract_join_conditions(from);

    // Build a map of (left_table, right_table) -> condition for quick lookup
    let mut condition_map: HashMap<(String, String), Option<vibesql_ast::Expression>> = HashMap::new();
    for (left, right, cond) in all_conditions {
        condition_map.insert((left.clone(), right.clone()), cond.clone());
        // Also add reverse for flexibility (both directions for flexibility in matching)
        if let Some(ref c) = cond {
            condition_map.insert((right, left), Some(c.clone()));
        } else {
            condition_map.insert((right, left), None);
        }
    }

    // Execute the first table
    if reorder_spec.table_order.is_empty() {
        return None;
    }

    let first_table_name = reorder_spec.table_order[0].clone();
    let first_idx = *flattened.table_index.get(&first_table_name)?;
    let (_, first_from) = &flattened.tables[first_idx];

    let mut result = match execute_from_fn(first_from, predicates) {
        Ok(r) => r,
        Err(e) => return Some(Err(e)),
    };

    // Join with remaining tables in specified order
    for i in 1..reorder_spec.table_order.len() {
        let next_table_name = &reorder_spec.table_order[i];

        let next_idx = *flattened.table_index.get(next_table_name)?;
        let (_, next_from) = &flattened.tables[next_idx];

        let next_result = match execute_from_fn(next_from, predicates) {
            Ok(r) => r,
            Err(e) => return Some(Err(e)),
        };

        // Find the join condition between next_table and ANY already-joined table
        // This is critical for star joins and complex join graphs where tables
        // don't form a simple chain (e.g., t1 joins to t2, t3, t4 but not t2-t3)
        let mut join_condition = None;
        for j in 0..i {
            let already_joined = &reorder_spec.table_order[j];

            // Try both directions: (already_joined, next) and (next, already_joined)
            if let Some(cond) = condition_map
                .get(&(already_joined.clone(), next_table_name.clone()))
                .or_else(|| condition_map.get(&(next_table_name.clone(), already_joined.clone())))
                .cloned()
                .flatten()
            {
                join_condition = Some(cond);
                break; // Found a condition, use it
            }
        }

        // For now, use INNER JOIN (could be configurable)
        let join_type = vibesql_ast::JoinType::Inner;

        // Phase 3: Extract WHERE clause equijoins that apply to this specific join
        // Filter equijoins to only those connecting left (accumulated) and right (next_table) sides
        let applicable_equijoins: Vec<vibesql_ast::Expression> = if let Some(preds) = predicates {
            let left_schema_tables: std::collections::HashSet<_> =
                result.schema.table_schemas.keys().cloned().collect();
            let right_schema_tables: std::collections::HashSet<_> =
                next_result.schema.table_schemas.keys().cloned().collect();

            preds
                .equijoin_conditions
                .iter()
                .filter_map(|(left_table, _left_col, right_table, _right_col, expr)| {
                    // Check if this equijoin connects tables from left and right
                    let left_in_left = left_schema_tables.contains(left_table);
                    let right_in_right = right_schema_tables.contains(right_table);
                    let right_in_left = left_schema_tables.contains(right_table);
                    let left_in_right = right_schema_tables.contains(left_table);

                    if (left_in_left && right_in_right) || (right_in_left && left_in_right) {
                        Some(expr.clone())
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        // nested_loop_join with WHERE clause equijoins for hash join optimization (Phase 3)
        // Note: Using default timeout context - proper timeout propagation is a future improvement
        let timeout_ctx = TimeoutContext::new_default();
        result = match nested_loop_join(
            result,
            next_result,
            &join_type,
            &join_condition,
            false,
            database,
            &applicable_equijoins,
            &timeout_ctx,
        ) {
            Ok(r) => r,
            Err(e) => return Some(Err(e)),
        };
    }

    Some(Ok(result))
}
