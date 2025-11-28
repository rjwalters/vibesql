//! ROLLUP, CUBE, and GROUPING SETS expansion
//!
//! SQL:1999 OLAP extensions for multi-dimensional aggregation.
//! This module handles expanding these constructs into concrete grouping sets.

use vibesql_ast::{Expression, GroupByClause, GroupingElement, GroupingSet, MixedGroupingItem, WindowFunctionSpec, WindowSpec};

/// A resolved grouping set - a set of expressions to group by
/// along with information about which base expressions are "rolled up" (aggregated)
#[derive(Debug, Clone)]
pub struct ResolvedGroupingSet {
    /// The expressions to GROUP BY for this grouping set
    pub group_by_exprs: Vec<Expression>,
    /// For each base expression (in the order they appear in ROLLUP/CUBE/GROUPING SETS),
    /// whether it's rolled up (true = aggregated/NULL, false = present in grouping)
    pub rolled_up: Vec<bool>,
}

/// Context for GROUPING() function evaluation
/// Tracks which columns are rolled up in the current grouping set
#[derive(Debug, Clone, Default)]
pub struct GroupingContext {
    /// The base expressions from the GROUP BY clause
    pub base_expressions: Vec<Expression>,
    /// For each base expression, whether it's rolled up in current grouping set
    pub rolled_up: Vec<bool>,
}

impl GroupingContext {
    /// Check if a specific expression is rolled up (aggregated/NULL)
    /// Returns 1 if rolled up, 0 if present
    pub fn is_rolled_up(&self, expr: &Expression) -> i32 {
        for (i, base_expr) in self.base_expressions.iter().enumerate() {
            if expressions_equal(expr, base_expr) {
                return if self.rolled_up.get(i).copied().unwrap_or(false) { 1 } else { 0 };
            }
        }
        // Expression not found in base expressions - return 0
        0
    }

    /// Compute GROUPING_ID for multiple expressions
    ///
    /// Returns a bitmap integer where:
    /// - Leftmost argument = most significant bit
    /// - 1 = column is rolled up (aggregated)
    /// - 0 = column is present in grouping
    ///
    /// Formula: GROUPING_ID(c1, c2, ..., cn) = GROUPING(c1) * 2^(n-1) + GROUPING(c2) * 2^(n-2) + ... + GROUPING(cn)
    ///
    /// Example with 3 columns (d_year, i_category, i_brand):
    /// - All present: 0 (binary 000)
    /// - i_brand rolled up: 1 (binary 001)
    /// - i_category, i_brand rolled up: 3 (binary 011)
    /// - All rolled up: 7 (binary 111)
    pub fn grouping_id(&self, exprs: &[Expression]) -> i64 {
        let n = exprs.len();
        let mut result: i64 = 0;

        for (i, expr) in exprs.iter().enumerate() {
            let is_rolled_up = self.is_rolled_up(expr);
            if is_rolled_up == 1 {
                // Leftmost argument = most significant bit
                // Position i (0-based from left) -> bit position (n-1-i)
                result |= 1i64 << (n - 1 - i);
            }
        }

        result
    }
}

/// Expand a GROUP BY clause into a list of resolved grouping sets
pub fn expand_group_by_clause(clause: &GroupByClause) -> Vec<ResolvedGroupingSet> {
    match clause {
        GroupByClause::Simple(exprs) => {
            // Simple GROUP BY - single grouping set with all expressions
            vec![ResolvedGroupingSet {
                group_by_exprs: exprs.clone(),
                rolled_up: vec![false; exprs.len()],
            }]
        }
        GroupByClause::Rollup(elements) => expand_rollup(elements),
        GroupByClause::Cube(elements) => expand_cube(elements),
        GroupByClause::GroupingSets(sets) => expand_grouping_sets(sets),
        GroupByClause::Mixed(items) => expand_mixed(items),
    }
}

/// Get the base expressions from a GROUP BY clause (for GROUPING() function)
pub fn get_base_expressions(clause: &GroupByClause) -> Vec<Expression> {
    match clause {
        GroupByClause::Simple(exprs) => exprs.clone(),
        GroupByClause::Rollup(elements) | GroupByClause::Cube(elements) => {
            elements.iter().flat_map(element_to_expressions).collect()
        }
        GroupByClause::GroupingSets(sets) => {
            // Use the first non-empty set's columns as base expressions
            // (all sets should have the same columns conceptually)
            sets.iter()
                .find(|s| !s.columns.is_empty())
                .map(|s| s.columns.clone())
                .unwrap_or_default()
        }
        GroupByClause::Mixed(items) => {
            // Collect all expressions from all items
            items
                .iter()
                .flat_map(|item| match item {
                    MixedGroupingItem::Simple(expr) => vec![expr.clone()],
                    MixedGroupingItem::Rollup(elements) | MixedGroupingItem::Cube(elements) => {
                        elements.iter().flat_map(element_to_expressions).collect()
                    }
                    MixedGroupingItem::GroupingSets(sets) => {
                        sets.iter().flat_map(|s| s.columns.clone()).collect()
                    }
                })
                .collect()
        }
    }
}

/// Expand ROLLUP into grouping sets
///
/// ROLLUP(a, b, c) produces:
/// - (a, b, c)
/// - (a, b)
/// - (a)
/// - ()
fn expand_rollup(elements: &[GroupingElement]) -> Vec<ResolvedGroupingSet> {
    // Flatten elements to expressions for base expressions list
    let base_exprs: Vec<Expression> =
        elements.iter().flat_map(element_to_expressions).collect();

    let mut result = Vec::with_capacity(elements.len() + 1);

    // Generate sets from full set down to empty set
    for prefix_len in (0..=elements.len()).rev() {
        let group_by_exprs: Vec<Expression> = elements[0..prefix_len]
            .iter()
            .flat_map(element_to_expressions)
            .collect();

        // Build rolled_up flags - elements beyond prefix_len are rolled up
        let mut rolled_up = Vec::with_capacity(base_exprs.len());
        for (i, element) in elements.iter().enumerate() {
            let element_expr_count = match element {
                GroupingElement::Single(_) => 1,
                GroupingElement::Composite(exprs) => exprs.len(),
            };
            for _ in 0..element_expr_count {
                rolled_up.push(i >= prefix_len);
            }
        }

        result.push(ResolvedGroupingSet { group_by_exprs, rolled_up });
    }

    result
}

/// Expand CUBE into grouping sets
///
/// CUBE(a, b) produces all 2^n combinations:
/// - (a, b)
/// - (a)
/// - (b)
/// - ()
fn expand_cube(elements: &[GroupingElement]) -> Vec<ResolvedGroupingSet> {
    let base_exprs: Vec<Expression> =
        elements.iter().flat_map(element_to_expressions).collect();

    let n = elements.len();
    let num_sets = 1 << n; // 2^n combinations

    let mut result = Vec::with_capacity(num_sets);

    // Generate all 2^n combinations using bit mask
    // Start from all bits set (full set) and go down to 0 (empty set)
    for mask in (0..num_sets).rev() {
        let mut group_by_exprs = Vec::new();
        let mut rolled_up = Vec::with_capacity(base_exprs.len());

        for (i, element) in elements.iter().enumerate() {
            let include = (mask & (1 << (n - 1 - i))) != 0;
            let element_exprs = element_to_expressions(element);

            for expr in &element_exprs {
                if include {
                    group_by_exprs.push(expr.clone());
                }
                rolled_up.push(!include);
            }
        }

        result.push(ResolvedGroupingSet { group_by_exprs, rolled_up });
    }

    result
}

/// Expand GROUPING SETS into resolved grouping sets
fn expand_grouping_sets(sets: &[GroupingSet]) -> Vec<ResolvedGroupingSet> {
    // Find the "universe" of all expressions used across all sets
    let all_exprs: Vec<Expression> =
        sets.iter().flat_map(|s| s.columns.clone()).collect();

    // Deduplicate while preserving order (use first occurrence)
    let mut base_exprs: Vec<Expression> = Vec::new();
    for expr in &all_exprs {
        if !base_exprs.iter().any(|e| expressions_equal(e, expr)) {
            base_exprs.push(expr.clone());
        }
    }

    sets.iter()
        .map(|set| {
            // Build rolled_up flags - expression is rolled up if NOT in this set
            let rolled_up: Vec<bool> = base_exprs
                .iter()
                .map(|base| !set.columns.iter().any(|e| expressions_equal(e, base)))
                .collect();

            ResolvedGroupingSet { group_by_exprs: set.columns.clone(), rolled_up }
        })
        .collect()
}

/// Expand mixed GROUP BY clause into resolved grouping sets
///
/// Mixed syntax like `GROUP BY a, ROLLUP(b, c)` means:
/// - Simple expressions (a) appear in ALL grouping sets
/// - ROLLUP/CUBE/GROUPING SETS are expanded normally
/// - If there are multiple ROLLUP/CUBE/GROUPING SETS, take their cross-product
///
/// Example: `GROUP BY a, ROLLUP(b, c)` produces:
/// - (a, b, c)  - full set
/// - (a, b)    - c rolled up
/// - (a)       - b and c rolled up (a always present)
fn expand_mixed(items: &[MixedGroupingItem]) -> Vec<ResolvedGroupingSet> {
    // Collect simple expressions (always present in all grouping sets)
    let mut simple_exprs: Vec<Expression> = Vec::new();
    // Collect expansions from ROLLUP/CUBE/GROUPING SETS items
    let mut item_expansions: Vec<Vec<ResolvedGroupingSet>> = Vec::new();

    for item in items {
        match item {
            MixedGroupingItem::Simple(expr) => {
                simple_exprs.push(expr.clone());
            }
            MixedGroupingItem::Rollup(elements) => {
                item_expansions.push(expand_rollup(elements));
            }
            MixedGroupingItem::Cube(elements) => {
                item_expansions.push(expand_cube(elements));
            }
            MixedGroupingItem::GroupingSets(sets) => {
                item_expansions.push(expand_grouping_sets(sets));
            }
        }
    }

    // If no ROLLUP/CUBE/GROUPING SETS, it's effectively a simple GROUP BY
    if item_expansions.is_empty() {
        return vec![ResolvedGroupingSet {
            group_by_exprs: simple_exprs.clone(),
            rolled_up: vec![false; simple_exprs.len()],
        }];
    }

    // Take cross-product of all item expansions
    let combined = cross_product_grouping_sets(item_expansions);

    // Prepend simple expressions to each resulting grouping set
    combined
        .into_iter()
        .map(|set| {
            let mut group_by_exprs = simple_exprs.clone();
            group_by_exprs.extend(set.group_by_exprs);

            // Simple expressions are never rolled up (always present)
            let mut rolled_up = vec![false; simple_exprs.len()];
            rolled_up.extend(set.rolled_up);

            ResolvedGroupingSet { group_by_exprs, rolled_up }
        })
        .collect()
}

/// Compute cross-product of multiple grouping set expansions
///
/// For example, if we have ROLLUP(a) and ROLLUP(b):
/// - ROLLUP(a) -> [(a), ()]
/// - ROLLUP(b) -> [(b), ()]
/// - Cross product -> [(a,b), (a), (b), ()]
fn cross_product_grouping_sets(
    expansions: Vec<Vec<ResolvedGroupingSet>>,
) -> Vec<ResolvedGroupingSet> {
    if expansions.is_empty() {
        return vec![ResolvedGroupingSet {
            group_by_exprs: vec![],
            rolled_up: vec![],
        }];
    }

    let mut result = expansions[0].clone();

    for expansion in expansions.into_iter().skip(1) {
        let mut new_result = Vec::with_capacity(result.len() * expansion.len());

        for left in &result {
            for right in &expansion {
                // Combine the two grouping sets
                let mut combined_exprs = left.group_by_exprs.clone();
                combined_exprs.extend(right.group_by_exprs.clone());

                let mut combined_rolled_up = left.rolled_up.clone();
                combined_rolled_up.extend(right.rolled_up.clone());

                new_result.push(ResolvedGroupingSet {
                    group_by_exprs: combined_exprs,
                    rolled_up: combined_rolled_up,
                });
            }
        }

        result = new_result;
    }

    result
}

/// Convert a grouping element to its expressions
fn element_to_expressions(element: &GroupingElement) -> Vec<Expression> {
    match element {
        GroupingElement::Single(expr) => vec![expr.clone()],
        GroupingElement::Composite(exprs) => exprs.clone(),
    }
}

/// Check if two expressions are semantically equal (for matching GROUPING() arguments)
/// Uses case-insensitive comparison for column names, structural equality for others.
fn expressions_equal(a: &Expression, b: &Expression) -> bool {
    match (a, b) {
        // ColumnRef: case-insensitive comparison for identifiers
        (
            Expression::ColumnRef { table: t1, column: c1 },
            Expression::ColumnRef { table: t2, column: c2 },
        ) => {
            let columns_equal = c1.eq_ignore_ascii_case(c2);
            let tables_equal = match (t1, t2) {
                (Some(tb1), Some(tb2)) => tb1.eq_ignore_ascii_case(tb2),
                (None, None) => true,
                // If one has a qualifier and the other doesn't, they could still be equal
                _ => true,
            };
            columns_equal && tables_equal
        }

        // Literal: use derived PartialEq
        (Expression::Literal(v1), Expression::Literal(v2)) => v1 == v2,

        // BinaryOp: recurse into operands
        (
            Expression::BinaryOp { op: op1, left: l1, right: r1 },
            Expression::BinaryOp { op: op2, left: l2, right: r2 },
        ) => op1 == op2 && expressions_equal(l1, l2) && expressions_equal(r1, r2),

        // UnaryOp: recurse into operand
        (Expression::UnaryOp { op: op1, expr: e1 }, Expression::UnaryOp { op: op2, expr: e2 }) => {
            op1 == op2 && expressions_equal(e1, e2)
        }

        // Function: case-insensitive name, recurse into args
        (
            Expression::Function { name: n1, args: a1, character_unit: cu1 },
            Expression::Function { name: n2, args: a2, character_unit: cu2 },
        ) => {
            n1.eq_ignore_ascii_case(n2)
                && cu1 == cu2
                && a1.len() == a2.len()
                && a1.iter().zip(a2).all(|(x, y)| expressions_equal(x, y))
        }

        // AggregateFunction: case-insensitive name, check distinct, recurse into args
        (
            Expression::AggregateFunction { name: n1, distinct: d1, args: a1 },
            Expression::AggregateFunction { name: n2, distinct: d2, args: a2 },
        ) => {
            n1.eq_ignore_ascii_case(n2)
                && d1 == d2
                && a1.len() == a2.len()
                && a1.iter().zip(a2).all(|(x, y)| expressions_equal(x, y))
        }

        // IsNull: recurse into expression
        (Expression::IsNull { expr: e1, negated: n1 }, Expression::IsNull { expr: e2, negated: n2 }) => {
            n1 == n2 && expressions_equal(e1, e2)
        }

        // Wildcard
        (Expression::Wildcard, Expression::Wildcard) => true,

        // Case: recurse into operand, when_clauses, else_result
        (
            Expression::Case { operand: op1, when_clauses: w1, else_result: e1 },
            Expression::Case { operand: op2, when_clauses: w2, else_result: e2 },
        ) => {
            let operands_equal = match (op1, op2) {
                (Some(o1), Some(o2)) => expressions_equal(o1, o2),
                (None, None) => true,
                _ => false,
            };
            let whens_equal = w1.len() == w2.len()
                && w1.iter().zip(w2).all(|(wc1, wc2)| {
                    wc1.conditions.len() == wc2.conditions.len()
                        && wc1
                            .conditions
                            .iter()
                            .zip(&wc2.conditions)
                            .all(|(c1, c2)| expressions_equal(c1, c2))
                        && expressions_equal(&wc1.result, &wc2.result)
                });
            let else_equal = match (e1, e2) {
                (Some(el1), Some(el2)) => expressions_equal(el1, el2),
                (None, None) => true,
                _ => false,
            };
            operands_equal && whens_equal && else_equal
        }

        // ScalarSubquery: use reference equality (subqueries are unlikely to be semantically equal)
        (Expression::ScalarSubquery(_), Expression::ScalarSubquery(_)) => false,

        // In subquery: subqueries are not comparable for GROUPING() purposes
        (Expression::In { .. }, Expression::In { .. }) => false,

        // InList: recurse into expression and values
        (
            Expression::InList { expr: e1, values: v1, negated: n1 },
            Expression::InList { expr: e2, values: v2, negated: n2 },
        ) => {
            n1 == n2
                && expressions_equal(e1, e2)
                && v1.len() == v2.len()
                && v1.iter().zip(v2).all(|(x, y)| expressions_equal(x, y))
        }

        // Between: recurse into expressions
        (
            Expression::Between { expr: e1, low: l1, high: h1, negated: n1, symmetric: s1 },
            Expression::Between { expr: e2, low: l2, high: h2, negated: n2, symmetric: s2 },
        ) => {
            n1 == n2
                && s1 == s2
                && expressions_equal(e1, e2)
                && expressions_equal(l1, l2)
                && expressions_equal(h1, h2)
        }

        // Cast: recurse into expression, check data type
        (
            Expression::Cast { expr: e1, data_type: dt1 },
            Expression::Cast { expr: e2, data_type: dt2 },
        ) => dt1 == dt2 && expressions_equal(e1, e2),

        // Position: recurse into expressions
        (
            Expression::Position { substring: s1, string: st1, character_unit: cu1 },
            Expression::Position { substring: s2, string: st2, character_unit: cu2 },
        ) => cu1 == cu2 && expressions_equal(s1, s2) && expressions_equal(st1, st2),

        // Trim: check position, recurse into expressions
        (
            Expression::Trim { position: p1, removal_char: r1, string: s1 },
            Expression::Trim { position: p2, removal_char: r2, string: s2 },
        ) => {
            let removal_equal = match (r1, r2) {
                (Some(rc1), Some(rc2)) => expressions_equal(rc1, rc2),
                (None, None) => true,
                _ => false,
            };
            p1 == p2 && removal_equal && expressions_equal(s1, s2)
        }

        // Like: recurse into expressions
        (
            Expression::Like { expr: e1, pattern: p1, negated: n1 },
            Expression::Like { expr: e2, pattern: p2, negated: n2 },
        ) => n1 == n2 && expressions_equal(e1, e2) && expressions_equal(p1, p2),

        // Exists: subqueries are not comparable for GROUPING() purposes
        (Expression::Exists { .. }, Expression::Exists { .. }) => false,

        // QuantifiedComparison: subqueries are not comparable for GROUPING() purposes
        (Expression::QuantifiedComparison { .. }, Expression::QuantifiedComparison { .. }) => false,

        // Current date/time functions
        (Expression::CurrentDate, Expression::CurrentDate) => true,
        (Expression::CurrentTime { precision: p1 }, Expression::CurrentTime { precision: p2 }) => {
            p1 == p2
        }
        (
            Expression::CurrentTimestamp { precision: p1 },
            Expression::CurrentTimestamp { precision: p2 },
        ) => p1 == p2,

        // Interval: recurse into value expression
        (
            Expression::Interval {
                value: v1,
                unit: u1,
                leading_precision: lp1,
                fractional_precision: fp1,
            },
            Expression::Interval {
                value: v2,
                unit: u2,
                leading_precision: lp2,
                fractional_precision: fp2,
            },
        ) => u1 == u2 && lp1 == lp2 && fp1 == fp2 && expressions_equal(v1, v2),

        // Default
        (Expression::Default, Expression::Default) => true,

        // DuplicateKeyValue: case-insensitive column comparison
        (Expression::DuplicateKeyValue { column: c1 }, Expression::DuplicateKeyValue { column: c2 }) => {
            c1.eq_ignore_ascii_case(c2)
        }

        // WindowFunction: recurse into function spec and over clause
        (
            Expression::WindowFunction { function: f1, over: o1 },
            Expression::WindowFunction { function: f2, over: o2 },
        ) => window_function_equal(f1, f2) && window_spec_equal(o1, o2),

        // NextValue: case-insensitive sequence name
        (Expression::NextValue { sequence_name: s1 }, Expression::NextValue { sequence_name: s2 }) => {
            s1.eq_ignore_ascii_case(s2)
        }

        // MatchAgainst: case-insensitive column names, recurse into search modifier
        (
            Expression::MatchAgainst { columns: c1, search_modifier: s1, mode: m1 },
            Expression::MatchAgainst { columns: c2, search_modifier: s2, mode: m2 },
        ) => {
            m1 == m2
                && c1.len() == c2.len()
                && c1.iter().zip(c2).all(|(a, b)| a.eq_ignore_ascii_case(b))
                && expressions_equal(s1, s2)
        }

        // PseudoVariable: check pseudo table and case-insensitive column
        (
            Expression::PseudoVariable { pseudo_table: pt1, column: c1 },
            Expression::PseudoVariable { pseudo_table: pt2, column: c2 },
        ) => pt1 == pt2 && c1.eq_ignore_ascii_case(c2),

        // SessionVariable: case-insensitive name
        (Expression::SessionVariable { name: n1 }, Expression::SessionVariable { name: n2 }) => {
            n1.eq_ignore_ascii_case(n2)
        }

        // Different variants are not equal
        _ => false,
    }
}

/// Check if two window function specs are equal
fn window_function_equal(a: &WindowFunctionSpec, b: &WindowFunctionSpec) -> bool {
    match (a, b) {
        (
            WindowFunctionSpec::Aggregate { name: n1, args: a1 },
            WindowFunctionSpec::Aggregate { name: n2, args: a2 },
        )
        | (
            WindowFunctionSpec::Ranking { name: n1, args: a1 },
            WindowFunctionSpec::Ranking { name: n2, args: a2 },
        )
        | (
            WindowFunctionSpec::Value { name: n1, args: a1 },
            WindowFunctionSpec::Value { name: n2, args: a2 },
        ) => {
            n1.eq_ignore_ascii_case(n2)
                && a1.len() == a2.len()
                && a1.iter().zip(a2).all(|(x, y)| expressions_equal(x, y))
        }
        _ => false,
    }
}

/// Check if two window specs are equal
fn window_spec_equal(a: &WindowSpec, b: &WindowSpec) -> bool {
    let partition_equal = match (&a.partition_by, &b.partition_by) {
        (Some(p1), Some(p2)) => {
            p1.len() == p2.len() && p1.iter().zip(p2).all(|(x, y)| expressions_equal(x, y))
        }
        (None, None) => true,
        _ => false,
    };

    // For order_by and frame, use derived PartialEq (they don't contain identifiers
    // that need case-insensitive comparison at the top level)
    let order_equal = a.order_by == b.order_by;
    let frame_equal = a.frame == b.frame;

    partition_equal && order_equal && frame_equal
}

/// Resolve GROUP BY expression that might be a SELECT list alias or column position
///
/// Similar to ORDER BY alias resolution, handles three cases:
/// 1. Numeric literal (e.g., GROUP BY 1, 2, 3) - returns the expression from that position in SELECT list
/// 2. Simple column reference that matches a SELECT list alias - returns the SELECT list expression
/// 3. Otherwise - returns the original GROUP BY expression
///
/// This enables standard SQL behavior where GROUP BY can reference SELECT aliases:
/// ```sql
/// SELECT n_name as nation, COUNT(*) FROM ... GROUP BY nation
/// ```
pub fn resolve_group_by_alias(
    group_expr: &Expression,
    select_list: &[vibesql_ast::SelectItem],
) -> Expression {
    // Check for numeric column position (GROUP BY 1, 2, 3, etc.)
    if let Expression::Literal(vibesql_types::SqlValue::Integer(pos)) = group_expr {
        if *pos > 0 && (*pos as usize) <= select_list.len() {
            // Valid column position, return the expression at that position
            let idx = (*pos as usize) - 1;
            if let vibesql_ast::SelectItem::Expression { expr, .. } = &select_list[idx] {
                return expr.clone();
            }
        }
    }

    // Check if GROUP BY expression is a simple column reference (no table qualifier)
    if let Expression::ColumnRef { table: None, column } = group_expr {
        // Search for matching alias in SELECT list (case-insensitive)
        for item in select_list {
            if let vibesql_ast::SelectItem::Expression { expr, alias: Some(alias_name) } = item {
                if alias_name.eq_ignore_ascii_case(column) {
                    // Found matching alias, use the SELECT list expression
                    return expr.clone();
                }
            }
        }
    }

    // Not an alias or column position, use the original expression
    group_expr.clone()
}

/// Resolve all aliases in a ResolvedGroupingSet against SELECT list
///
/// Processes each GROUP BY expression, resolving any aliases to their
/// underlying expressions from the SELECT list.
pub fn resolve_grouping_set_aliases(
    set: &ResolvedGroupingSet,
    select_list: &[vibesql_ast::SelectItem],
) -> ResolvedGroupingSet {
    ResolvedGroupingSet {
        group_by_exprs: set.group_by_exprs.iter()
            .map(|expr| resolve_group_by_alias(expr, select_list))
            .collect(),
        rolled_up: set.rolled_up.clone(),
    }
}

/// Resolve aliases in base expressions for GroupingContext
///
/// Used to ensure GROUPING() function can properly match aliased columns
pub fn resolve_base_expressions_aliases(
    base_exprs: &[Expression],
    select_list: &[vibesql_ast::SelectItem],
) -> Vec<Expression> {
    base_exprs.iter()
        .map(|expr| resolve_group_by_alias(expr, select_list))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str) -> Expression {
        Expression::ColumnRef { table: None, column: name.to_string() }
    }

    #[test]
    fn test_expand_rollup() {
        let elements = vec![
            GroupingElement::Single(col("a")),
            GroupingElement::Single(col("b")),
            GroupingElement::Single(col("c")),
        ];

        let sets = expand_rollup(&elements);
        assert_eq!(sets.len(), 4); // (a,b,c), (a,b), (a), ()

        // Full set
        assert_eq!(sets[0].group_by_exprs.len(), 3);
        assert_eq!(sets[0].rolled_up, vec![false, false, false]);

        // (a, b)
        assert_eq!(sets[1].group_by_exprs.len(), 2);
        assert_eq!(sets[1].rolled_up, vec![false, false, true]);

        // (a)
        assert_eq!(sets[2].group_by_exprs.len(), 1);
        assert_eq!(sets[2].rolled_up, vec![false, true, true]);

        // ()
        assert_eq!(sets[3].group_by_exprs.len(), 0);
        assert_eq!(sets[3].rolled_up, vec![true, true, true]);
    }

    #[test]
    fn test_expand_cube() {
        let elements =
            vec![GroupingElement::Single(col("a")), GroupingElement::Single(col("b"))];

        let sets = expand_cube(&elements);
        assert_eq!(sets.len(), 4); // (a,b), (a), (b), ()

        // Verify all combinations are present
        let set_sizes: Vec<usize> = sets.iter().map(|s| s.group_by_exprs.len()).collect();
        assert!(set_sizes.contains(&2)); // (a, b)
        assert!(set_sizes.iter().filter(|&&s| s == 1).count() == 2); // (a) and (b)
        assert!(set_sizes.contains(&0)); // ()
    }

    #[test]
    fn test_expand_grouping_sets() {
        let sets = vec![
            GroupingSet { columns: vec![col("a"), col("b")] },
            GroupingSet { columns: vec![col("a")] },
            GroupingSet { columns: vec![] },
        ];

        let resolved = expand_grouping_sets(&sets);
        assert_eq!(resolved.len(), 3);
    }

    #[test]
    fn test_grouping_context() {
        let ctx = GroupingContext {
            base_expressions: vec![col("a"), col("b"), col("c")],
            rolled_up: vec![false, true, true],
        };

        assert_eq!(ctx.is_rolled_up(&col("a")), 0); // Not rolled up
        assert_eq!(ctx.is_rolled_up(&col("b")), 1); // Rolled up
        assert_eq!(ctx.is_rolled_up(&col("c")), 1); // Rolled up
        assert_eq!(ctx.is_rolled_up(&col("d")), 0); // Unknown, default to 0
    }

    #[test]
    fn test_grouping_id() {
        // Test GROUPING_ID with 3 columns: a, b, c
        // Bit mapping: a is MSB, c is LSB
        let ctx = GroupingContext {
            base_expressions: vec![col("a"), col("b"), col("c")],
            rolled_up: vec![false, false, false], // All present
        };
        // GROUPING_ID(a, b, c) = 0*4 + 0*2 + 0*1 = 0 (binary 000)
        assert_eq!(ctx.grouping_id(&[col("a"), col("b"), col("c")]), 0);

        let ctx = GroupingContext {
            base_expressions: vec![col("a"), col("b"), col("c")],
            rolled_up: vec![false, false, true], // c rolled up
        };
        // GROUPING_ID(a, b, c) = 0*4 + 0*2 + 1*1 = 1 (binary 001)
        assert_eq!(ctx.grouping_id(&[col("a"), col("b"), col("c")]), 1);

        let ctx = GroupingContext {
            base_expressions: vec![col("a"), col("b"), col("c")],
            rolled_up: vec![false, true, true], // b, c rolled up
        };
        // GROUPING_ID(a, b, c) = 0*4 + 1*2 + 1*1 = 3 (binary 011)
        assert_eq!(ctx.grouping_id(&[col("a"), col("b"), col("c")]), 3);

        let ctx = GroupingContext {
            base_expressions: vec![col("a"), col("b"), col("c")],
            rolled_up: vec![true, true, true], // All rolled up
        };
        // GROUPING_ID(a, b, c) = 1*4 + 1*2 + 1*1 = 7 (binary 111)
        assert_eq!(ctx.grouping_id(&[col("a"), col("b"), col("c")]), 7);

        // Test with subset of columns
        let ctx = GroupingContext {
            base_expressions: vec![col("a"), col("b"), col("c")],
            rolled_up: vec![true, false, true], // a, c rolled up
        };
        // GROUPING_ID(a, c) = 1*2 + 1*1 = 3 (binary 11)
        assert_eq!(ctx.grouping_id(&[col("a"), col("c")]), 3);
        // GROUPING_ID(b, c) = 0*2 + 1*1 = 1 (binary 01)
        assert_eq!(ctx.grouping_id(&[col("b"), col("c")]), 1);
    }

    #[test]
    fn test_grouping_id_single_column() {
        // Single column - should be same as GROUPING()
        let ctx = GroupingContext {
            base_expressions: vec![col("a")],
            rolled_up: vec![false],
        };
        assert_eq!(ctx.grouping_id(&[col("a")]), 0);

        let ctx = GroupingContext {
            base_expressions: vec![col("a")],
            rolled_up: vec![true],
        };
        assert_eq!(ctx.grouping_id(&[col("a")]), 1);
    }

    #[test]
    fn test_expand_mixed_simple_with_rollup() {
        // GROUP BY a, ROLLUP(b, c)
        // Should produce:
        // - (a, b, c) - full set
        // - (a, b)    - c rolled up
        // - (a)       - b, c rolled up (a always present)
        let items = vec![
            MixedGroupingItem::Simple(col("a")),
            MixedGroupingItem::Rollup(vec![
                GroupingElement::Single(col("b")),
                GroupingElement::Single(col("c")),
            ]),
        ];

        let sets = expand_mixed(&items);

        // ROLLUP(b, c) produces 3 sets: (b,c), (b), ()
        // Combined with a: (a,b,c), (a,b), (a)
        assert_eq!(sets.len(), 3);

        // (a, b, c) - all present
        assert_eq!(sets[0].group_by_exprs.len(), 3);
        assert_eq!(sets[0].rolled_up, vec![false, false, false]); // a, b, c all present

        // (a, b) - c rolled up
        assert_eq!(sets[1].group_by_exprs.len(), 2);
        assert_eq!(sets[1].rolled_up, vec![false, false, true]); // a, b present; c rolled up

        // (a) - b, c rolled up
        assert_eq!(sets[2].group_by_exprs.len(), 1);
        assert_eq!(sets[2].rolled_up, vec![false, true, true]); // a present; b, c rolled up
    }

    #[test]
    fn test_expand_mixed_simple_with_cube() {
        // GROUP BY a, CUBE(b)
        // Should produce:
        // - (a, b) - full set
        // - (a)    - b rolled up (a always present)
        let items = vec![
            MixedGroupingItem::Simple(col("a")),
            MixedGroupingItem::Cube(vec![GroupingElement::Single(col("b"))]),
        ];

        let sets = expand_mixed(&items);

        // CUBE(b) produces 2 sets: (b), ()
        // Combined with a: (a,b), (a)
        assert_eq!(sets.len(), 2);

        // (a, b) - all present
        assert_eq!(sets[0].group_by_exprs.len(), 2);
        assert_eq!(sets[0].rolled_up, vec![false, false]);

        // (a) - b rolled up
        assert_eq!(sets[1].group_by_exprs.len(), 1);
        assert_eq!(sets[1].rolled_up, vec![false, true]);
    }

    #[test]
    fn test_expand_mixed_multiple_simple() {
        // GROUP BY a, b, ROLLUP(c)
        // Should produce:
        // - (a, b, c) - full set
        // - (a, b)    - c rolled up
        let items = vec![
            MixedGroupingItem::Simple(col("a")),
            MixedGroupingItem::Simple(col("b")),
            MixedGroupingItem::Rollup(vec![GroupingElement::Single(col("c"))]),
        ];

        let sets = expand_mixed(&items);

        // ROLLUP(c) produces 2 sets: (c), ()
        // Combined with a, b: (a,b,c), (a,b)
        assert_eq!(sets.len(), 2);

        // (a, b, c) - all present
        assert_eq!(sets[0].group_by_exprs.len(), 3);
        assert_eq!(sets[0].rolled_up, vec![false, false, false]);

        // (a, b) - c rolled up
        assert_eq!(sets[1].group_by_exprs.len(), 2);
        assert_eq!(sets[1].rolled_up, vec![false, false, true]);
    }

    #[test]
    fn test_expand_mixed_cross_product_of_rollups() {
        // GROUP BY ROLLUP(a), ROLLUP(b)
        // ROLLUP(a) produces: (a), ()
        // ROLLUP(b) produces: (b), ()
        // Cross product: (a,b), (a), (b), ()
        let items = vec![
            MixedGroupingItem::Rollup(vec![GroupingElement::Single(col("a"))]),
            MixedGroupingItem::Rollup(vec![GroupingElement::Single(col("b"))]),
        ];

        let sets = expand_mixed(&items);

        // 2 * 2 = 4 sets
        assert_eq!(sets.len(), 4);

        // Verify the sets (order may vary based on expansion)
        let set_sizes: Vec<usize> = sets.iter().map(|s| s.group_by_exprs.len()).collect();
        assert!(set_sizes.contains(&2)); // (a, b)
        assert!(set_sizes.iter().filter(|&&s| s == 1).count() == 2); // (a), (b)
        assert!(set_sizes.contains(&0)); // ()
    }

    #[test]
    fn test_expand_mixed_only_simple() {
        // GROUP BY a, b (only simple expressions - but stored as Mixed)
        // Should behave like Simple
        let items = vec![
            MixedGroupingItem::Simple(col("a")),
            MixedGroupingItem::Simple(col("b")),
        ];

        let sets = expand_mixed(&items);

        // Only one grouping set with both columns
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].group_by_exprs.len(), 2);
        assert_eq!(sets[0].rolled_up, vec![false, false]);
    }

    // ============================================================
    // Unit tests for expressions_equal() - issue #2768
    // ============================================================

    fn qualified_col(table: &str, column: &str) -> Expression {
        Expression::ColumnRef { table: Some(table.to_string()), column: column.to_string() }
    }

    fn lit_int(n: i64) -> Expression {
        Expression::Literal(vibesql_types::SqlValue::Integer(n))
    }

    fn lit_str(s: &str) -> Expression {
        Expression::Literal(vibesql_types::SqlValue::Varchar(s.to_string()))
    }

    fn binary_op(op: vibesql_ast::BinaryOperator, left: Expression, right: Expression) -> Expression {
        Expression::BinaryOp { op, left: Box::new(left), right: Box::new(right) }
    }

    fn unary_op(op: vibesql_ast::UnaryOperator, expr: Expression) -> Expression {
        Expression::UnaryOp { op, expr: Box::new(expr) }
    }

    fn func(name: &str, args: Vec<Expression>) -> Expression {
        Expression::Function { name: name.to_string(), args, character_unit: None }
    }

    fn agg(name: &str, args: Vec<Expression>, distinct: bool) -> Expression {
        Expression::AggregateFunction { name: name.to_string(), distinct, args }
    }

    // --- ColumnRef tests ---

    #[test]
    fn test_expressions_equal_column_ref_same() {
        assert!(expressions_equal(&col("name"), &col("name")));
    }

    #[test]
    fn test_expressions_equal_column_ref_case_insensitive() {
        assert!(expressions_equal(&col("Name"), &col("name")));
        assert!(expressions_equal(&col("NAME"), &col("name")));
        assert!(expressions_equal(&col("NaMe"), &col("nAmE")));
    }

    #[test]
    fn test_expressions_equal_column_ref_different() {
        assert!(!expressions_equal(&col("name"), &col("id")));
    }

    #[test]
    fn test_expressions_equal_column_ref_qualified() {
        // Same table and column
        assert!(expressions_equal(
            &qualified_col("users", "name"),
            &qualified_col("users", "name")
        ));
        // Case-insensitive table
        assert!(expressions_equal(
            &qualified_col("USERS", "name"),
            &qualified_col("users", "name")
        ));
        // Different table
        assert!(!expressions_equal(
            &qualified_col("users", "name"),
            &qualified_col("orders", "name")
        ));
    }

    #[test]
    fn test_expressions_equal_column_ref_mixed_qualification() {
        // One qualified, one unqualified - allowed to be equal (conservative)
        assert!(expressions_equal(&col("name"), &qualified_col("users", "name")));
        assert!(expressions_equal(&qualified_col("users", "name"), &col("name")));
    }

    // --- Literal tests ---

    #[test]
    fn test_expressions_equal_literal_same() {
        assert!(expressions_equal(&lit_int(42), &lit_int(42)));
        assert!(expressions_equal(&lit_str("hello"), &lit_str("hello")));
    }

    #[test]
    fn test_expressions_equal_literal_different() {
        assert!(!expressions_equal(&lit_int(42), &lit_int(43)));
        assert!(!expressions_equal(&lit_str("hello"), &lit_str("world")));
        assert!(!expressions_equal(&lit_int(42), &lit_str("42")));
    }

    // --- BinaryOp tests ---

    #[test]
    fn test_expressions_equal_binary_op() {
        use vibesql_ast::BinaryOperator;

        // Same operation
        assert!(expressions_equal(
            &binary_op(BinaryOperator::Plus, col("a"), lit_int(1)),
            &binary_op(BinaryOperator::Plus, col("a"), lit_int(1))
        ));

        // Case-insensitive column in binary op
        assert!(expressions_equal(
            &binary_op(BinaryOperator::Plus, col("A"), lit_int(1)),
            &binary_op(BinaryOperator::Plus, col("a"), lit_int(1))
        ));

        // Different operator
        assert!(!expressions_equal(
            &binary_op(BinaryOperator::Plus, col("a"), lit_int(1)),
            &binary_op(BinaryOperator::Minus, col("a"), lit_int(1))
        ));

        // Different operands
        assert!(!expressions_equal(
            &binary_op(BinaryOperator::Plus, col("a"), lit_int(1)),
            &binary_op(BinaryOperator::Plus, col("b"), lit_int(1))
        ));
    }

    // --- UnaryOp tests ---

    #[test]
    fn test_expressions_equal_unary_op() {
        use vibesql_ast::UnaryOperator;

        assert!(expressions_equal(
            &unary_op(UnaryOperator::Minus, col("x")),
            &unary_op(UnaryOperator::Minus, col("x"))
        ));

        assert!(expressions_equal(
            &unary_op(UnaryOperator::Minus, col("X")),
            &unary_op(UnaryOperator::Minus, col("x"))
        ));

        assert!(!expressions_equal(
            &unary_op(UnaryOperator::Minus, col("x")),
            &unary_op(UnaryOperator::Not, col("x"))
        ));
    }

    // --- Function tests ---

    #[test]
    fn test_expressions_equal_function() {
        // Same function
        assert!(expressions_equal(
            &func("UPPER", vec![col("name")]),
            &func("UPPER", vec![col("name")])
        ));

        // Case-insensitive function name
        assert!(expressions_equal(
            &func("upper", vec![col("name")]),
            &func("UPPER", vec![col("name")])
        ));

        // Case-insensitive arg column
        assert!(expressions_equal(
            &func("UPPER", vec![col("NAME")]),
            &func("UPPER", vec![col("name")])
        ));

        // Different function
        assert!(!expressions_equal(
            &func("UPPER", vec![col("name")]),
            &func("LOWER", vec![col("name")])
        ));

        // Different args
        assert!(!expressions_equal(
            &func("UPPER", vec![col("name")]),
            &func("UPPER", vec![col("id")])
        ));

        // Different arg count
        assert!(!expressions_equal(
            &func("COALESCE", vec![col("a"), lit_int(0)]),
            &func("COALESCE", vec![col("a")])
        ));
    }

    // --- AggregateFunction tests ---

    #[test]
    fn test_expressions_equal_aggregate() {
        // Same aggregate
        assert!(expressions_equal(
            &agg("SUM", vec![col("amount")], false),
            &agg("SUM", vec![col("amount")], false)
        ));

        // Case-insensitive
        assert!(expressions_equal(
            &agg("sum", vec![col("amount")], false),
            &agg("SUM", vec![col("amount")], false)
        ));

        // Different distinct flag
        assert!(!expressions_equal(
            &agg("COUNT", vec![col("id")], true),
            &agg("COUNT", vec![col("id")], false)
        ));
    }

    // --- IsNull tests ---

    #[test]
    fn test_expressions_equal_is_null() {
        let is_null = |e: Expression, negated: bool| Expression::IsNull {
            expr: Box::new(e),
            negated,
        };

        assert!(expressions_equal(&is_null(col("a"), false), &is_null(col("a"), false)));
        assert!(expressions_equal(&is_null(col("A"), false), &is_null(col("a"), false)));
        assert!(!expressions_equal(&is_null(col("a"), false), &is_null(col("a"), true)));
        assert!(!expressions_equal(&is_null(col("a"), false), &is_null(col("b"), false)));
    }

    // --- Cast tests ---

    #[test]
    fn test_expressions_equal_cast() {
        use vibesql_types::DataType;

        let cast = |e: Expression, dt: DataType| Expression::Cast {
            expr: Box::new(e),
            data_type: dt,
        };

        assert!(expressions_equal(
            &cast(col("a"), DataType::Integer),
            &cast(col("a"), DataType::Integer)
        ));

        assert!(expressions_equal(
            &cast(col("A"), DataType::Integer),
            &cast(col("a"), DataType::Integer)
        ));

        assert!(!expressions_equal(
            &cast(col("a"), DataType::Integer),
            &cast(col("a"), DataType::Varchar { max_length: None })
        ));
    }

    // --- InList tests ---

    #[test]
    fn test_expressions_equal_in_list() {
        let in_list = |e: Expression, vals: Vec<Expression>, negated: bool| Expression::InList {
            expr: Box::new(e),
            values: vals,
            negated,
        };

        assert!(expressions_equal(
            &in_list(col("status"), vec![lit_str("a"), lit_str("b")], false),
            &in_list(col("status"), vec![lit_str("a"), lit_str("b")], false)
        ));

        // Case-insensitive column
        assert!(expressions_equal(
            &in_list(col("STATUS"), vec![lit_str("a"), lit_str("b")], false),
            &in_list(col("status"), vec![lit_str("a"), lit_str("b")], false)
        ));

        // Different values
        assert!(!expressions_equal(
            &in_list(col("status"), vec![lit_str("a"), lit_str("b")], false),
            &in_list(col("status"), vec![lit_str("a"), lit_str("c")], false)
        ));

        // Different negation
        assert!(!expressions_equal(
            &in_list(col("status"), vec![lit_str("a")], false),
            &in_list(col("status"), vec![lit_str("a")], true)
        ));
    }

    // --- Between tests ---

    #[test]
    fn test_expressions_equal_between() {
        let between =
            |e: Expression, lo: Expression, hi: Expression, negated: bool| Expression::Between {
                expr: Box::new(e),
                low: Box::new(lo),
                high: Box::new(hi),
                negated,
                symmetric: false,
            };

        assert!(expressions_equal(
            &between(col("age"), lit_int(18), lit_int(65), false),
            &between(col("age"), lit_int(18), lit_int(65), false)
        ));

        assert!(expressions_equal(
            &between(col("AGE"), lit_int(18), lit_int(65), false),
            &between(col("age"), lit_int(18), lit_int(65), false)
        ));

        assert!(!expressions_equal(
            &between(col("age"), lit_int(18), lit_int(65), false),
            &between(col("age"), lit_int(18), lit_int(65), true)
        ));
    }

    // --- Like tests ---

    #[test]
    fn test_expressions_equal_like() {
        let like = |e: Expression, p: Expression, negated: bool| Expression::Like {
            expr: Box::new(e),
            pattern: Box::new(p),
            negated,
        };

        assert!(expressions_equal(
            &like(col("name"), lit_str("%john%"), false),
            &like(col("name"), lit_str("%john%"), false)
        ));

        assert!(expressions_equal(
            &like(col("NAME"), lit_str("%john%"), false),
            &like(col("name"), lit_str("%john%"), false)
        ));

        assert!(!expressions_equal(
            &like(col("name"), lit_str("%john%"), false),
            &like(col("name"), lit_str("%jane%"), false)
        ));
    }

    // --- Subquery tests (always return false) ---

    #[test]
    fn test_expressions_equal_scalar_subquery() {
        use vibesql_ast::SelectStmt;
        // Create minimal SelectStmt instances
        let subq1 = Expression::ScalarSubquery(Box::new(SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        }));
        let subq2 = Expression::ScalarSubquery(Box::new(SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        }));

        // Subqueries always compare as false (reference inequality)
        assert!(!expressions_equal(&subq1, &subq2));
    }

    // --- Different variants ---

    #[test]
    fn test_expressions_equal_different_variants() {
        assert!(!expressions_equal(&col("a"), &lit_int(1)));
        assert!(!expressions_equal(&lit_int(1), &Expression::Wildcard));
        assert!(!expressions_equal(&col("a"), &func("UPPER", vec![col("a")])));
    }

    // --- Wildcard ---

    #[test]
    fn test_expressions_equal_wildcard() {
        assert!(expressions_equal(&Expression::Wildcard, &Expression::Wildcard));
    }

    // --- Current date/time ---

    #[test]
    fn test_expressions_equal_current_date() {
        assert!(expressions_equal(&Expression::CurrentDate, &Expression::CurrentDate));
        assert!(expressions_equal(
            &Expression::CurrentTime { precision: Some(3) },
            &Expression::CurrentTime { precision: Some(3) }
        ));
        assert!(!expressions_equal(
            &Expression::CurrentTime { precision: Some(3) },
            &Expression::CurrentTime { precision: Some(6) }
        ));
        assert!(!expressions_equal(
            &Expression::CurrentDate,
            &Expression::CurrentTime { precision: None }
        ));
    }

    // --- Nested expressions ---

    #[test]
    fn test_expressions_equal_nested() {
        use vibesql_ast::BinaryOperator;

        // (a + b) * c == (A + B) * C
        let expr1 = binary_op(
            BinaryOperator::Multiply,
            binary_op(BinaryOperator::Plus, col("a"), col("b")),
            col("c"),
        );
        let expr2 = binary_op(
            BinaryOperator::Multiply,
            binary_op(BinaryOperator::Plus, col("A"), col("B")),
            col("C"),
        );
        assert!(expressions_equal(&expr1, &expr2));

        // UPPER(LOWER(name)) == UPPER(LOWER(NAME))
        let expr3 = func("UPPER", vec![func("LOWER", vec![col("name")])]);
        let expr4 = func("UPPER", vec![func("LOWER", vec![col("NAME")])]);
        assert!(expressions_equal(&expr3, &expr4));
    }

    // --- GroupingContext with complex expressions ---

    #[test]
    fn test_grouping_context_with_expressions() {
        use vibesql_ast::BinaryOperator;

        let expr = binary_op(BinaryOperator::Plus, col("a"), lit_int(1));
        let ctx = GroupingContext {
            base_expressions: vec![col("a"), expr.clone()],
            rolled_up: vec![false, true],
        };

        // Simple column lookup
        assert_eq!(ctx.is_rolled_up(&col("a")), 0);
        assert_eq!(ctx.is_rolled_up(&col("A")), 0); // case insensitive

        // Complex expression lookup
        assert_eq!(ctx.is_rolled_up(&expr), 1);

        // Same expression with different case
        let expr_upper = binary_op(BinaryOperator::Plus, col("A"), lit_int(1));
        assert_eq!(ctx.is_rolled_up(&expr_upper), 1);
    }

    // ============================================================
    // Unit tests for GROUP BY alias resolution - issue #2910
    // ============================================================

    fn select_item(expr: Expression, alias: Option<&str>) -> vibesql_ast::SelectItem {
        vibesql_ast::SelectItem::Expression {
            expr,
            alias: alias.map(|s| s.to_string()),
        }
    }

    #[test]
    fn test_resolve_group_by_alias_simple() {
        // SELECT n_name AS nation, COUNT(*) FROM ... GROUP BY nation
        let select_list = vec![
            select_item(col("n_name"), Some("nation")),
            select_item(
                Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![Expression::Wildcard],
                },
                None,
            ),
        ];

        // GROUP BY nation should resolve to n_name
        let group_expr = col("nation");
        let resolved = resolve_group_by_alias(&group_expr, &select_list);

        assert!(matches!(resolved, Expression::ColumnRef { table: None, column } if column == "n_name"));
    }

    #[test]
    fn test_resolve_group_by_alias_case_insensitive() {
        // Alias matching should be case-insensitive
        let select_list = vec![select_item(col("n_name"), Some("NATION"))];

        // GROUP BY nation (lowercase) should match NATION (uppercase)
        let group_expr = col("nation");
        let resolved = resolve_group_by_alias(&group_expr, &select_list);

        assert!(matches!(resolved, Expression::ColumnRef { table: None, column } if column == "n_name"));
    }

    #[test]
    fn test_resolve_group_by_alias_numeric_position() {
        // GROUP BY 1 should return the first SELECT item
        let select_list = vec![
            select_item(col("n_name"), Some("nation")),
            select_item(col("amount"), None),
        ];

        let group_expr = Expression::Literal(vibesql_types::SqlValue::Integer(1));
        let resolved = resolve_group_by_alias(&group_expr, &select_list);

        assert!(matches!(resolved, Expression::ColumnRef { table: None, column } if column == "n_name"));

        // GROUP BY 2 should return the second SELECT item
        let group_expr2 = Expression::Literal(vibesql_types::SqlValue::Integer(2));
        let resolved2 = resolve_group_by_alias(&group_expr2, &select_list);

        assert!(matches!(resolved2, Expression::ColumnRef { table: None, column } if column == "amount"));
    }

    #[test]
    fn test_resolve_group_by_alias_no_match() {
        // GROUP BY expression that doesn't match any alias should remain unchanged
        let select_list = vec![select_item(col("n_name"), Some("nation"))];

        // GROUP BY something_else should not resolve to anything
        let group_expr = col("something_else");
        let resolved = resolve_group_by_alias(&group_expr, &select_list);

        assert!(matches!(resolved, Expression::ColumnRef { table: None, column } if column == "something_else"));
    }

    #[test]
    fn test_resolve_group_by_alias_qualified_column() {
        // Qualified column references (e.g., t.col) should NOT resolve aliases
        let select_list = vec![select_item(col("n_name"), Some("nation"))];

        // GROUP BY t.nation should NOT resolve to n_name (it's table-qualified)
        let group_expr = Expression::ColumnRef {
            table: Some("t".to_string()),
            column: "nation".to_string(),
        };
        let resolved = resolve_group_by_alias(&group_expr, &select_list);

        // Should remain unchanged
        assert!(matches!(resolved, Expression::ColumnRef { table: Some(t), column } if t == "t" && column == "nation"));
    }

    #[test]
    fn test_resolve_group_by_alias_expression_alias() {
        // SELECT SUBSTR(o_orderdate, 1, 4) AS o_year ... GROUP BY o_year
        let substr_expr = Expression::Function {
            name: "SUBSTR".to_string(),
            args: vec![
                col("o_orderdate"),
                Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                Expression::Literal(vibesql_types::SqlValue::Integer(4)),
            ],
            character_unit: None,
        };
        let select_list = vec![select_item(substr_expr.clone(), Some("o_year"))];

        // GROUP BY o_year should resolve to SUBSTR(o_orderdate, 1, 4)
        let group_expr = col("o_year");
        let resolved = resolve_group_by_alias(&group_expr, &select_list);

        // Should be the SUBSTR expression
        assert!(matches!(resolved, Expression::Function { name, .. } if name == "SUBSTR"));
    }

    #[test]
    fn test_resolve_grouping_set_aliases() {
        // Test resolving aliases in a full grouping set
        let select_list = vec![
            select_item(col("n1_n_name"), Some("supp_nation")),
            select_item(col("n2_n_name"), Some("cust_nation")),
        ];

        let original_set = ResolvedGroupingSet {
            group_by_exprs: vec![col("supp_nation"), col("cust_nation")],
            rolled_up: vec![false, false],
        };

        let resolved_set = resolve_grouping_set_aliases(&original_set, &select_list);

        // Both should resolve to the actual column names
        assert_eq!(resolved_set.group_by_exprs.len(), 2);
        assert!(matches!(&resolved_set.group_by_exprs[0], Expression::ColumnRef { table: None, column } if column == "n1_n_name"));
        assert!(matches!(&resolved_set.group_by_exprs[1], Expression::ColumnRef { table: None, column } if column == "n2_n_name"));
        // rolled_up should be preserved
        assert_eq!(resolved_set.rolled_up, vec![false, false]);
    }

    #[test]
    fn test_resolve_base_expressions_aliases() {
        let select_list = vec![
            select_item(col("a_col"), Some("a")),
            select_item(col("b_col"), Some("b")),
        ];

        let base_exprs = vec![col("a"), col("b")];
        let resolved = resolve_base_expressions_aliases(&base_exprs, &select_list);

        assert_eq!(resolved.len(), 2);
        assert!(matches!(&resolved[0], Expression::ColumnRef { table: None, column } if column == "a_col"));
        assert!(matches!(&resolved[1], Expression::ColumnRef { table: None, column } if column == "b_col"));
    }
}
