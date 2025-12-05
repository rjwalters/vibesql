//! ROLLUP, CUBE, and GROUPING SETS expansion
//!
//! SQL:1999 OLAP extensions for multi-dimensional aggregation.
//! This module handles expanding these constructs into concrete grouping sets.

mod alias_resolution;
mod expansion;
mod expression_utils;

use vibesql_ast::Expression;

// Re-export public API
pub use alias_resolution::{resolve_base_expressions_aliases, resolve_grouping_set_aliases};
pub use expansion::{expand_group_by_clause, get_base_expressions};
pub use expression_utils::expressions_equal;

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

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::BinaryOperator;

    fn col(name: &str) -> Expression {
        Expression::ColumnRef { table: None, column: name.to_string() }
    }

    fn lit_int(n: i64) -> Expression {
        Expression::Literal(vibesql_types::SqlValue::Integer(n))
    }

    fn binary_op(op: BinaryOperator, left: Expression, right: Expression) -> Expression {
        Expression::BinaryOp { op, left: Box::new(left), right: Box::new(right) }
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
        let ctx = GroupingContext { base_expressions: vec![col("a")], rolled_up: vec![false] };
        assert_eq!(ctx.grouping_id(&[col("a")]), 0);

        let ctx = GroupingContext { base_expressions: vec![col("a")], rolled_up: vec![true] };
        assert_eq!(ctx.grouping_id(&[col("a")]), 1);
    }

    #[test]
    fn test_grouping_context_with_expressions() {
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
}
