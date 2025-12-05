//! GROUP BY clause expansion for ROLLUP, CUBE, and GROUPING SETS
//!
//! This module handles expanding SQL:1999 OLAP grouping constructs
//! into concrete sets of GROUP BY expressions.

use super::expression_utils::expressions_equal;
use super::ResolvedGroupingSet;
use vibesql_ast::{Expression, GroupByClause, GroupingElement, GroupingSet, MixedGroupingItem};

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
    let base_exprs: Vec<Expression> = elements.iter().flat_map(element_to_expressions).collect();

    let mut result = Vec::with_capacity(elements.len() + 1);

    // Generate sets from full set down to empty set
    for prefix_len in (0..=elements.len()).rev() {
        let group_by_exprs: Vec<Expression> =
            elements[0..prefix_len].iter().flat_map(element_to_expressions).collect();

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
    let base_exprs: Vec<Expression> = elements.iter().flat_map(element_to_expressions).collect();

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
    let all_exprs: Vec<Expression> = sets.iter().flat_map(|s| s.columns.clone()).collect();

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
        return vec![ResolvedGroupingSet { group_by_exprs: vec![], rolled_up: vec![] }];
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
        let elements = vec![GroupingElement::Single(col("a")), GroupingElement::Single(col("b"))];

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
        let items = vec![MixedGroupingItem::Simple(col("a")), MixedGroupingItem::Simple(col("b"))];

        let sets = expand_mixed(&items);

        // Only one grouping set with both columns
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].group_by_exprs.len(), 2);
        assert_eq!(sets[0].rolled_up, vec![false, false]);
    }
}
