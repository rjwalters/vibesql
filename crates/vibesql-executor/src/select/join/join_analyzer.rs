//! Join condition analyzer for detecting equi-join opportunities
//!
//! This module analyzes join conditions to identify equi-joins (equality-based joins)
//! which can be optimized using hash join algorithms instead of nested loop joins.
//!
//! ## Multi-Column Join Keys
//!
//! For queries with multiple equi-join conditions between the same table pair
//! (e.g., `A.x = B.x AND A.y = B.y`), this module extracts ALL conditions to enable
//! composite-key hash joins. This dramatically improves performance for:
//! - TPC-H Q3, Q7, Q10 with multi-way joins
//! - Queries with composite foreign keys
//! - Any join with multiple equality conditions

use vibesql_ast::{BinaryOperator, Expression};

use crate::schema::CombinedSchema;

/// Information about an equi-join condition (single column)
#[derive(Debug, Clone)]
pub struct EquiJoinInfo {
    /// Column index in the left table
    pub left_col_idx: usize,
    /// Column index in the right table
    pub right_col_idx: usize,
}

/// Information about an arithmetic equi-join condition
///
/// Used for conditions like `col1 = col2 + constant` or `col1 = col2 - constant`
/// which can be optimized using hash join with offset transformation.
///
/// The transformation works by applying the offset to the build side key:
/// - For `left_col = right_col - offset`: hash(right_col - offset), probe(left_col)
/// - For `left_col = right_col + offset`: hash(right_col + offset), probe(left_col)
#[derive(Debug, Clone)]
pub struct ArithmeticEquiJoinInfo {
    /// Column index in the left table (probe side)
    pub left_col_idx: usize,
    /// Column index in the right table (build side, before offset)
    pub right_col_idx: usize,
    /// Offset to apply to the right column value during build
    /// Positive means add, negative means subtract
    /// For `left = right - 53`, this is -53 (subtract 53 from right)
    /// For `left = right + 53`, this is +53 (add 53 to right)
    pub offset: i64,
}

/// Information about a multi-column equi-join (composite key)
///
/// Used when there are multiple equality conditions between the same table pair,
/// enabling composite-key hash join for better performance.
#[derive(Debug, Clone)]
pub struct MultiColumnEquiJoinInfo {
    /// Column indices in the left table (in order)
    pub left_col_indices: Vec<usize>,
    /// Column indices in the right table (in order, matching left)
    pub right_col_indices: Vec<usize>,
}

/// Analyze a join condition to detect if it's a simple equi-join
///
/// Returns Some(EquiJoinInfo) if the condition is a simple equi-join like:
/// - `t1.col = t2.col`
/// - `t2.col = t1.col`
///
/// Returns None if:
/// - The condition is more complex (AND/OR of multiple conditions)
/// - The condition uses operators other than equality
/// - The condition doesn't reference columns from both sides
pub fn analyze_equi_join(
    condition: &Expression,
    schema: &CombinedSchema,
    left_column_count: usize,
) -> Option<EquiJoinInfo> {
    analyze_single_equi_join(condition, schema, left_column_count)
}

/// Analyze a single equality expression for equi-join
fn analyze_single_equi_join(
    condition: &Expression,
    schema: &CombinedSchema,
    left_column_count: usize,
) -> Option<EquiJoinInfo> {
    match condition {
        Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
            // Try to extract column references from both sides
            if let (Some(left_idx), Some(right_idx)) =
                (extract_column_index(left, schema), extract_column_index(right, schema))
            {
                // Check if one is from left table and one is from right table
                if left_idx < left_column_count && right_idx >= left_column_count {
                    // Left column from left table, right column from right table
                    return Some(EquiJoinInfo {
                        left_col_idx: left_idx,
                        right_col_idx: right_idx - left_column_count,
                    });
                } else if right_idx < left_column_count && left_idx >= left_column_count {
                    // Left column from right table, right column from left table (swapped)
                    return Some(EquiJoinInfo {
                        left_col_idx: right_idx,
                        right_col_idx: left_idx - left_column_count,
                    });
                }
            }
            None
        }
        _ => None,
    }
}

/// Result of analyzing a compound condition for equi-join opportunities
#[derive(Debug)]
pub struct CompoundEquiJoinResult {
    /// The equi-join info for the hash join
    pub equi_join: EquiJoinInfo,
    /// Remaining conditions to apply as post-join filter
    pub remaining_conditions: Vec<Expression>,
}

/// Result of analyzing a compound condition for multi-column equi-join opportunities
#[derive(Debug)]
pub struct MultiColumnEquiJoinResult {
    /// All equi-join column pairs found (for composite key hash join)
    pub equi_joins: MultiColumnEquiJoinInfo,
    /// Remaining conditions to apply as post-join filter
    pub remaining_conditions: Vec<Expression>,
}

/// Analyze a compound condition to extract ALL equi-join conditions for multi-column hash join
///
/// For compound conditions like `a.x = b.x AND a.y = b.y AND a.z > 5`, this will:
/// 1. Extract ALL equi-join conditions (`a.x = b.x`, `a.y = b.y`) for composite key hash join
/// 2. Return remaining conditions (`a.z > 5`) as post-join filters
///
/// This enables composite-key hash join for queries with multiple join columns,
/// providing significant performance improvements for TPC-H Q3, Q7, Q10 and similar queries.
///
/// Returns None if no equi-join conditions are found.
pub fn analyze_multi_column_equi_join(
    condition: &Expression,
    schema: &CombinedSchema,
    left_column_count: usize,
) -> Option<MultiColumnEquiJoinResult> {
    // First try as a simple equi-join
    if let Some(equi_join) = analyze_single_equi_join(condition, schema, left_column_count) {
        return Some(MultiColumnEquiJoinResult {
            equi_joins: MultiColumnEquiJoinInfo {
                left_col_indices: vec![equi_join.left_col_idx],
                right_col_indices: vec![equi_join.right_col_idx],
            },
            remaining_conditions: vec![],
        });
    }

    // Try to extract from AND conditions
    match condition {
        Expression::BinaryOp { op: BinaryOperator::And, .. } => {
            // Flatten all AND conditions
            let mut conditions = Vec::new();
            flatten_and_conditions(condition, &mut conditions);

            // Collect ALL equi-join conditions
            let mut left_indices = Vec::new();
            let mut right_indices = Vec::new();
            let mut remaining = Vec::new();

            for cond in conditions {
                if let Some(equi_join) = analyze_single_equi_join(cond, schema, left_column_count) {
                    left_indices.push(equi_join.left_col_idx);
                    right_indices.push(equi_join.right_col_idx);
                } else {
                    remaining.push(cond.clone());
                }
            }

            // Return if we found at least one equi-join
            if !left_indices.is_empty() {
                return Some(MultiColumnEquiJoinResult {
                    equi_joins: MultiColumnEquiJoinInfo {
                        left_col_indices: left_indices,
                        right_col_indices: right_indices,
                    },
                    remaining_conditions: remaining,
                });
            }

            None
        }
        _ => None,
    }
}

/// Analyze a potentially compound (AND) condition to extract equi-join opportunities
///
/// For compound conditions like `a.x = b.x AND a.y > 5 AND b.z = 10`, this will:
/// 1. Extract the first equi-join condition (`a.x = b.x`) for hash join
/// 2. Return remaining conditions (`a.y > 5 AND b.z = 10`) as post-join filters
///
/// This enables hash join optimization for complex WHERE clauses in queries like TPC-H Q3.
pub fn analyze_compound_equi_join(
    condition: &Expression,
    schema: &CombinedSchema,
    left_column_count: usize,
) -> Option<CompoundEquiJoinResult> {
    // First try as a simple equi-join
    if let Some(equi_join) = analyze_single_equi_join(condition, schema, left_column_count) {
        return Some(CompoundEquiJoinResult { equi_join, remaining_conditions: vec![] });
    }

    // Try to extract from AND conditions
    match condition {
        Expression::BinaryOp { op: BinaryOperator::And, left: _, right: _ } => {
            // Flatten all AND conditions
            let mut conditions = Vec::new();
            flatten_and_conditions(condition, &mut conditions);

            // Find the first equi-join condition
            for (i, cond) in conditions.iter().enumerate() {
                if let Some(equi_join) = analyze_single_equi_join(cond, schema, left_column_count) {
                    // Build remaining conditions
                    let remaining: Vec<Expression> = conditions
                        .iter()
                        .enumerate()
                        .filter(|(j, _)| *j != i)
                        .map(|(_, c)| (*c).clone())
                        .collect();

                    return Some(CompoundEquiJoinResult {
                        equi_join,
                        remaining_conditions: remaining,
                    });
                }
            }

            // No equi-join found in AND conditions
            None
        }
        _ => None,
    }
}

/// Flatten nested AND conditions into a vector
fn flatten_and_conditions<'a>(expr: &'a Expression, out: &mut Vec<&'a Expression>) {
    match expr {
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            flatten_and_conditions(left, out);
            flatten_and_conditions(right, out);
        }
        _ => out.push(expr),
    }
}

/// Extract column index from an expression if it's a simple column reference
fn extract_column_index(expr: &Expression, schema: &CombinedSchema) -> Option<usize> {
    match expr {
        Expression::ColumnRef { table, column } => {
            // Resolve column to index in combined schema
            schema.get_column_index(table.as_deref(), column)
        }
        _ => None,
    }
}

/// Flatten nested OR conditions into a vector
fn flatten_or_conditions<'a>(expr: &'a Expression, out: &mut Vec<&'a Expression>) {
    match expr {
        Expression::BinaryOp { op: BinaryOperator::Or, left, right } => {
            flatten_or_conditions(left, out);
            flatten_or_conditions(right, out);
        }
        _ => out.push(expr),
    }
}

/// Analyze an OR expression to extract common equi-join predicates
///
/// For expressions like `(a.x = b.x AND ...) OR (a.x = b.x AND ...) OR (a.x = b.x AND ...)`,
/// this will extract the common equi-join `a.x = b.x` that appears in ALL branches.
///
/// This enables hash join optimization for TPC-H Q19 and similar queries with complex OR conditions.
///
/// Returns Some(CompoundEquiJoinResult) if:
/// - All OR branches contain the same equi-join condition
/// - The equi-join can be used for hash join
///
/// Returns None if:
/// - Branches have different equi-joins
/// - No common equi-join found
pub fn analyze_or_equi_join(
    condition: &Expression,
    schema: &CombinedSchema,
    left_column_count: usize,
) -> Option<CompoundEquiJoinResult> {
    // Only process OR expressions
    if !matches!(condition, Expression::BinaryOp { op: BinaryOperator::Or, .. }) {
        return None;
    }

    // Flatten all OR branches
    let mut or_branches = Vec::new();
    flatten_or_conditions(condition, &mut or_branches);

    if or_branches.is_empty() {
        return None;
    }

    // For each branch, try to extract ALL equi-join conditions
    // We need to find equi-joins that appear in EVERY branch
    let mut branch_equijoins: Vec<Vec<EquiJoinInfo>> = Vec::new();

    for branch in &or_branches {
        let mut branch_joins = Vec::new();

        // A branch might be a single equijoin or an AND of multiple conditions
        match branch {
            Expression::BinaryOp { op: BinaryOperator::Equal, .. } => {
                // Single equality - check if it's an equi-join
                if let Some(equi_join) = analyze_single_equi_join(branch, schema, left_column_count)
                {
                    branch_joins.push(equi_join);
                }
            }
            Expression::BinaryOp { op: BinaryOperator::And, .. } => {
                // AND expression - extract all equi-joins from it
                let mut and_conditions = Vec::new();
                flatten_and_conditions(branch, &mut and_conditions);

                for cond in and_conditions {
                    if let Some(equi_join) =
                        analyze_single_equi_join(cond, schema, left_column_count)
                    {
                        branch_joins.push(equi_join);
                    }
                }
            }
            _ => {
                // Branch contains no equi-joins, can't optimize
                return None;
            }
        }

        // If this branch has no equi-joins, we can't find a common one
        if branch_joins.is_empty() {
            return None;
        }

        branch_equijoins.push(branch_joins);
    }

    // Now find equi-joins that appear in ALL branches
    // We'll compare by (left_col_idx, right_col_idx)
    if branch_equijoins.is_empty() {
        return None;
    }

    // Check each equi-join from the first branch
    for first_equijoin in &branch_equijoins[0] {
        let mut found_in_all = true;

        // Check if this equi-join appears in all other branches
        for other_branch in &branch_equijoins[1..] {
            let mut found_in_this_branch = false;
            for other_equijoin in other_branch {
                if first_equijoin.left_col_idx == other_equijoin.left_col_idx
                    && first_equijoin.right_col_idx == other_equijoin.right_col_idx
                {
                    found_in_this_branch = true;
                    break;
                }
            }

            if !found_in_this_branch {
                found_in_all = false;
                break;
            }
        }

        if found_in_all {
            // Found a common equi-join! Return it with the original OR as remaining condition
            return Some(CompoundEquiJoinResult {
                equi_join: first_equijoin.clone(),
                remaining_conditions: vec![condition.clone()],
            });
        }
    }

    // No common equi-join found
    None
}

/// Analyze a join condition to detect if it's an arithmetic equi-join
///
/// Detects patterns like:
/// - `col1 = col2 - constant` → ArithmeticEquiJoinInfo with negative offset
/// - `col1 = col2 + constant` → ArithmeticEquiJoinInfo with positive offset
/// - `col2 - constant = col1` → same, just reordered
/// - `col2 + constant = col1` → same, just reordered
///
/// This enables hash join optimization for TPC-DS Q2 which has:
/// `d_week_seq1 = d_week_seq2 - 53`
///
/// Returns None if the condition is not a simple arithmetic equi-join.
pub fn analyze_arithmetic_equi_join(
    condition: &Expression,
    schema: &CombinedSchema,
    left_column_count: usize,
) -> Option<ArithmeticEquiJoinInfo> {
    match condition {
        Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
            // Pattern 1: col1 = col2 +/- constant
            if let Some(info) = try_extract_arithmetic_join(left, right, schema, left_column_count)
            {
                return Some(info);
            }
            // Pattern 2: col2 +/- constant = col1 (swapped)
            if let Some(info) = try_extract_arithmetic_join(right, left, schema, left_column_count)
            {
                return Some(info);
            }
            None
        }
        _ => None,
    }
}

/// Try to extract arithmetic equi-join from pattern: simple_col = col +/- constant
fn try_extract_arithmetic_join(
    simple_side: &Expression,
    arithmetic_side: &Expression,
    schema: &CombinedSchema,
    left_column_count: usize,
) -> Option<ArithmeticEquiJoinInfo> {
    // simple_side must be a column reference
    let simple_col_idx = extract_column_index(simple_side, schema)?;

    // arithmetic_side must be (column +/- integer_literal)
    match arithmetic_side {
        Expression::BinaryOp { op, left: arith_left, right: arith_right } => {
            // Check for Plus or Minus
            let is_plus = matches!(op, BinaryOperator::Plus);
            let is_minus = matches!(op, BinaryOperator::Minus);

            if !is_plus && !is_minus {
                return None;
            }

            // Left side of arithmetic should be a column
            let arith_col_idx = extract_column_index(arith_left, schema)?;

            // Right side should be an integer literal
            let offset_value = match arith_right.as_ref() {
                Expression::Literal(vibesql_types::SqlValue::Integer(n)) => *n,
                _ => return None,
            };

            // Compute the offset to apply during build phase
            // For `simple_col = arith_col - 53`: offset = -53 (subtract from arith_col)
            // For `simple_col = arith_col + 53`: offset = +53 (add to arith_col)
            let offset = if is_plus { offset_value } else { -offset_value };

            // Determine which is left (probe) and right (build) table
            // simple_col is the probe side (lookup key)
            // arith_col is the build side (key needs offset applied)
            if simple_col_idx < left_column_count && arith_col_idx >= left_column_count {
                // simple_col from left table (probe), arith_col from right table (build)
                Some(ArithmeticEquiJoinInfo {
                    left_col_idx: simple_col_idx,
                    right_col_idx: arith_col_idx - left_column_count,
                    offset,
                })
            } else if arith_col_idx < left_column_count && simple_col_idx >= left_column_count {
                // arith_col from left table (build), simple_col from right table (probe)
                // Need to swap roles: left becomes build, right becomes probe
                // But our hash join assumes right is build, so we negate the offset
                Some(ArithmeticEquiJoinInfo {
                    left_col_idx: arith_col_idx,
                    right_col_idx: simple_col_idx - left_column_count,
                    offset: -offset, // Negate because tables are swapped
                })
            } else {
                // Both columns from same table - not a valid join
                None
            }
        }
        _ => None,
    }
}
