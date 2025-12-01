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

/// Adjustment to apply during hash join for expressions like `col = col +/- constant`
#[derive(Debug, Clone)]
pub enum KeyAdjustment {
    /// No adjustment needed (simple `col = col`)
    None,
    /// Add a constant to the key (for `col = other_col - N`, we add N to other_col)
    AddInteger(i64),
    /// Subtract a constant from the key (for `col = other_col + N`, we subtract N from other_col)
    SubtractInteger(i64),
}

/// Information about an adjusted equi-join condition
///
/// Extends EquiJoinInfo to support expressions like:
/// - `col1 = col2 + 53` (adjust by subtracting 53 from col2)
/// - `col1 = col2 - 53` (adjust by adding 53 to col2)
///
/// This enables hash join for TPC-DS Q2 and similar queries.
#[derive(Debug, Clone)]
pub struct AdjustedEquiJoinInfo {
    /// Column index in the left table
    pub left_col_idx: usize,
    /// Column index in the right table
    pub right_col_idx: usize,
    /// Adjustment to apply to the right column value during hash table build
    pub right_adjustment: KeyAdjustment,
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

/// Analyze an equality expression for adjusted equi-join (col = col +/- constant)
///
/// Handles patterns like:
/// - `col1 = col2 + 53` → adjust by subtracting 53 from col2
/// - `col1 = col2 - 53` → adjust by adding 53 to col2
///
/// This enables hash join for TPC-DS Q2 and similar queries with temporal offsets.
pub fn analyze_adjusted_equi_join(
    condition: &Expression,
    schema: &CombinedSchema,
    left_column_count: usize,
) -> Option<AdjustedEquiJoinInfo> {
    let debug = std::env::var("ADJ_JOIN_DEBUG").is_ok();
    if debug {
        eprintln!("[ADJ_JOIN_DEBUG] Analyzing condition: {:?}", condition);
        eprintln!("[ADJ_JOIN_DEBUG] Schema tables: {:?}", schema.table_schemas.keys().collect::<Vec<_>>());
        eprintln!("[ADJ_JOIN_DEBUG] Left column count: {}", left_column_count);
    }

    // First try simple equi-join (no adjustment)
    if let Some(simple) = analyze_single_equi_join(condition, schema, left_column_count) {
        if debug {
            eprintln!("[ADJ_JOIN_DEBUG] Found simple equi-join: left={}, right={}", simple.left_col_idx, simple.right_col_idx);
        }
        return Some(AdjustedEquiJoinInfo {
            left_col_idx: simple.left_col_idx,
            right_col_idx: simple.right_col_idx,
            right_adjustment: KeyAdjustment::None,
        });
    }

    // Try to match patterns like `col = col +/- constant`
    match condition {
        Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
            if debug {
                eprintln!("[ADJ_JOIN_DEBUG] Trying adjusted equi-join: left={:?}, right={:?}", left, right);
            }
            // Try both orderings: col = expr and expr = col
            if let Some(result) = try_adjusted_equi_join(left, right, schema, left_column_count) {
                if debug {
                    eprintln!("[ADJ_JOIN_DEBUG] SUCCESS with left-right ordering: {:?}", result);
                }
                return Some(result);
            }
            if let Some(result) = try_adjusted_equi_join(right, left, schema, left_column_count) {
                if debug {
                    eprintln!("[ADJ_JOIN_DEBUG] SUCCESS with right-left ordering: {:?}", result);
                }
                return Some(result);
            }
            if debug {
                eprintln!("[ADJ_JOIN_DEBUG] No adjusted equi-join found");
            }
            None
        }
        _ => {
            if debug {
                eprintln!("[ADJ_JOIN_DEBUG] Not a BinaryOp::Equal expression");
            }
            None
        }
    }
}

/// Helper to try matching `simple_col = col +/- constant`
fn try_adjusted_equi_join(
    simple_side: &Expression,
    adjusted_side: &Expression,
    schema: &CombinedSchema,
    left_column_count: usize,
) -> Option<AdjustedEquiJoinInfo> {
    let debug = std::env::var("ADJ_JOIN_DEBUG").is_ok();

    // Check if simple_side is a plain column reference
    let simple_col_idx = extract_column_index(simple_side, schema);
    if debug {
        eprintln!("[ADJ_JOIN_DEBUG] try_adjusted: simple_side={:?}, simple_col_idx={:?}", simple_side, simple_col_idx);
        eprintln!("[ADJ_JOIN_DEBUG]   Schema columns: {:?}",
            schema.table_schemas.iter()
                .flat_map(|(t, (_, s))| s.columns.iter().map(move |c| format!("{}.{}", t, c.name)))
                .collect::<Vec<_>>());
    }
    let simple_col_idx = simple_col_idx?;

    // Check if adjusted_side is `col +/- constant`
    let (adj_col_idx, adjustment) = extract_column_with_constant_adjustment(adjusted_side, schema)?;

    // Check if one is from left table and one is from right table
    if simple_col_idx < left_column_count && adj_col_idx >= left_column_count {
        // Simple column from left, adjusted column from right
        Some(AdjustedEquiJoinInfo {
            left_col_idx: simple_col_idx,
            right_col_idx: adj_col_idx - left_column_count,
            right_adjustment: adjustment,
        })
    } else if adj_col_idx < left_column_count && simple_col_idx >= left_column_count {
        // Adjusted column from left, simple column from right
        // We need to swap and invert the adjustment
        // If left = right + N, then we build on (right + N) and probe with left
        // But our structure assumes adjustment on right, so we need to handle differently
        // For simplicity, we only support adjustment on the right side for now
        None
    } else {
        None
    }
}

/// Extract a column reference with an integer constant adjustment
///
/// Matches patterns like:
/// - `col + 53` → returns (col_idx, SubtractInteger(53))
/// - `col - 53` → returns (col_idx, AddInteger(53))
///
/// The adjustment is inverted because to make `left_col = right_col - 53` work with hash join,
/// we need to compute `right_col - 53` during build (which equals adding 53 in the opposite direction).
fn extract_column_with_constant_adjustment(
    expr: &Expression,
    schema: &CombinedSchema,
) -> Option<(usize, KeyAdjustment)> {
    match expr {
        Expression::BinaryOp { op, left, right } => {
            // Try col +/- constant
            if let (Some(col_idx), Some(constant)) = (
                extract_column_index(left, schema),
                extract_integer_literal(right),
            ) {
                match op {
                    BinaryOperator::Plus => {
                        // col + N: to match, we subtract N from col
                        Some((col_idx, KeyAdjustment::SubtractInteger(constant)))
                    }
                    BinaryOperator::Minus => {
                        // col - N: to match, we add N to col
                        Some((col_idx, KeyAdjustment::AddInteger(constant)))
                    }
                    _ => None,
                }
            } else if let (Some(constant), Some(col_idx)) = (
                extract_integer_literal(left),
                extract_column_index(right, schema),
            ) {
                // constant +/- col (less common but supported)
                match op {
                    BinaryOperator::Plus => {
                        // N + col: same as col + N
                        Some((col_idx, KeyAdjustment::SubtractInteger(constant)))
                    }
                    BinaryOperator::Minus => {
                        // N - col: this would require negation, skip for now
                        None
                    }
                    _ => None,
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Extract an integer literal from an expression
fn extract_integer_literal(expr: &Expression) -> Option<i64> {
    use vibesql_types::SqlValue;
    match expr {
        Expression::Literal(SqlValue::Integer(n)) => Some(*n),
        Expression::Literal(SqlValue::Bigint(n)) => Some(*n),
        Expression::UnaryOp { op: vibesql_ast::UnaryOperator::Minus, expr } => {
            match expr.as_ref() {
                Expression::Literal(SqlValue::Integer(n)) => Some(-n),
                Expression::Literal(SqlValue::Bigint(n)) => Some(-n),
                _ => None,
            }
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
        return Some(CompoundEquiJoinResult {
            equi_join,
            remaining_conditions: vec![],
        });
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
                if let Some(equi_join) = analyze_single_equi_join(branch, schema, left_column_count) {
                    branch_joins.push(equi_join);
                }
            }
            Expression::BinaryOp { op: BinaryOperator::And, .. } => {
                // AND expression - extract all equi-joins from it
                let mut and_conditions = Vec::new();
                flatten_and_conditions(branch, &mut and_conditions);

                for cond in and_conditions {
                    if let Some(equi_join) = analyze_single_equi_join(cond, schema, left_column_count) {
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
