//! ROLLUP, CUBE, and GROUPING SETS expansion
//!
//! SQL:1999 OLAP extensions for multi-dimensional aggregation.
//! This module handles expanding these constructs into concrete grouping sets.

use vibesql_ast::{Expression, GroupByClause, GroupingElement, GroupingSet};

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
    }
}

/// Get the base expressions from a GROUP BY clause (for GROUPING() function)
pub fn get_base_expressions(clause: &GroupByClause) -> Vec<Expression> {
    match clause {
        GroupByClause::Simple(exprs) => exprs.clone(),
        GroupByClause::Rollup(elements) | GroupByClause::Cube(elements) => {
            elements.iter().flat_map(|e| element_to_expressions(e)).collect()
        }
        GroupByClause::GroupingSets(sets) => {
            // Use the first non-empty set's columns as base expressions
            // (all sets should have the same columns conceptually)
            sets.iter()
                .find(|s| !s.columns.is_empty())
                .map(|s| s.columns.clone())
                .unwrap_or_default()
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
        elements.iter().flat_map(|e| element_to_expressions(e)).collect();

    let mut result = Vec::with_capacity(elements.len() + 1);

    // Generate sets from full set down to empty set
    for prefix_len in (0..=elements.len()).rev() {
        let group_by_exprs: Vec<Expression> = elements[0..prefix_len]
            .iter()
            .flat_map(|e| element_to_expressions(e))
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
        elements.iter().flat_map(|e| element_to_expressions(e)).collect();

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

/// Convert a grouping element to its expressions
fn element_to_expressions(element: &GroupingElement) -> Vec<Expression> {
    match element {
        GroupingElement::Single(expr) => vec![expr.clone()],
        GroupingElement::Composite(exprs) => exprs.clone(),
    }
}

/// Check if two expressions are equal (for matching GROUPING() arguments)
/// This is a simplified equality check that handles common cases
fn expressions_equal(a: &Expression, b: &Expression) -> bool {
    match (a, b) {
        (
            Expression::ColumnRef { table: t1, column: c1 },
            Expression::ColumnRef { table: t2, column: c2 },
        ) => {
            // Case-insensitive column name comparison
            let columns_equal = c1.eq_ignore_ascii_case(c2);
            let tables_equal = match (t1, t2) {
                (Some(tb1), Some(tb2)) => tb1.eq_ignore_ascii_case(tb2),
                (None, None) => true,
                // If one has a qualifier and the other doesn't, they could still be equal
                // but we'll be conservative
                _ => true,
            };
            columns_equal && tables_equal
        }
        (Expression::Literal(v1), Expression::Literal(v2)) => v1 == v2,
        // For other expressions, use Debug representation equality as fallback
        _ => format!("{:?}", a) == format!("{:?}", b),
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
}
