//! Predicate pattern recognition for fast-path optimization
//!
//! This module detects common predicate patterns to enable specialized evaluation
//! that avoids SqlValue enum matching overhead.

use vibesql_ast::{BinaryOperator, Expression};
use vibesql_types::SqlValue;

use crate::schema::CombinedSchema;

/// Detected predicate patterns that can use specialized fast paths
#[derive(Debug, Clone)]
pub(crate) enum PredicatePattern {
    /// Numeric comparison: column_idx op constant (e.g., "price < 100.0")
    /// Optimized for f64 comparisons without enum matching
    NumericComparison { col_idx: usize, op: ComparisonOp, constant: f64 },

    /// Date range check: column >= min_date AND column < max_date
    /// Common in TPC-H queries (e.g., l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01')
    DateRange { col_idx: usize, min_date: vibesql_types::Date, max_date: vibesql_types::Date },

    /// BETWEEN for numeric values: column BETWEEN min AND max
    /// Optimized for contiguous range checks
    BetweenNumeric { col_idx: usize, min: f64, max: f64 },

    /// Integer comparison: column_idx op constant (e.g., "quantity > 10")
    /// Optimized for i64 comparisons without enum matching
    IntegerComparison { col_idx: usize, op: ComparisonOp, constant: i64 },

    /// General predicate that doesn't match any fast path
    /// Falls back to standard SqlValue enum matching
    #[allow(dead_code)]
    General(Expression),
}

/// Comparison operators supported by fast paths
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ComparisonOp {
    Lt,    // <
    Le,    // <=
    Gt,    // >
    Ge,    // >=
    Eq,    // =
    NotEq, // <>
}

impl ComparisonOp {
    /// Convert AST BinaryOperator to ComparisonOp
    fn from_binary_op(op: &BinaryOperator) -> Option<Self> {
        match op {
            BinaryOperator::LessThan => Some(ComparisonOp::Lt),
            BinaryOperator::LessThanOrEqual => Some(ComparisonOp::Le),
            BinaryOperator::GreaterThan => Some(ComparisonOp::Gt),
            BinaryOperator::GreaterThanOrEqual => Some(ComparisonOp::Ge),
            BinaryOperator::Equal => Some(ComparisonOp::Eq),
            BinaryOperator::NotEqual => Some(ComparisonOp::NotEq),
            _ => None,
        }
    }
}

impl PredicatePattern {
    /// Analyze WHERE clause expression and detect optimization opportunities
    pub(crate) fn from_where_clause(where_expr: &Expression, schema: &CombinedSchema) -> Self {
        // Try to detect date range pattern first (most specific)
        if let Some(pattern) = Self::detect_date_range(where_expr, schema) {
            return pattern;
        }

        // Try to detect numeric BETWEEN pattern
        if let Expression::Between { expr, low, high, negated: false, symmetric: false } =
            where_expr
        {
            if let Some(pattern) = Self::detect_between_numeric(expr, low, high, schema) {
                return pattern;
            }
        }

        // Try to detect simple comparison patterns
        if let Expression::BinaryOp { left, op, right } = where_expr {
            if let Some(comp_op) = ComparisonOp::from_binary_op(op) {
                // Check for: column op constant
                if let Some(pattern) =
                    Self::detect_column_constant_comparison(left, comp_op, right, schema)
                {
                    return pattern;
                }

                // Check for: constant op column (reversed)
                if let Some(pattern) =
                    Self::detect_constant_column_comparison(left, comp_op, right, schema)
                {
                    return pattern;
                }
            }
        }

        // No pattern matched - use general evaluation
        PredicatePattern::General(where_expr.clone())
    }

    /// Detect pattern: date_column >= min AND date_column < max
    fn detect_date_range(expr: &Expression, schema: &CombinedSchema) -> Option<PredicatePattern> {
        // Match: AND expression
        if let Expression::BinaryOp { left, op: BinaryOperator::And, right } = expr {
            // Check if both sides are comparisons on the same column
            let (left_col_idx, left_op, left_date) = Self::extract_date_comparison(left, schema)?;
            let (right_col_idx, right_op, right_date) =
                Self::extract_date_comparison(right, schema)?;

            // Must be the same column
            if left_col_idx != right_col_idx {
                return None;
            }

            // Check for pattern: col >= min_date AND col < max_date
            if left_op == ComparisonOp::Ge && right_op == ComparisonOp::Lt {
                return Some(PredicatePattern::DateRange {
                    col_idx: left_col_idx,
                    min_date: left_date,
                    max_date: right_date,
                });
            }

            // Check for reversed pattern: col < max_date AND col >= min_date
            if left_op == ComparisonOp::Lt && right_op == ComparisonOp::Ge {
                return Some(PredicatePattern::DateRange {
                    col_idx: left_col_idx,
                    min_date: right_date,
                    max_date: left_date,
                });
            }
        }

        None
    }

    /// Extract date comparison components: (column_index, op, date_value)
    fn extract_date_comparison(
        expr: &Expression,
        schema: &CombinedSchema,
    ) -> Option<(usize, ComparisonOp, vibesql_types::Date)> {
        if let Expression::BinaryOp { left, op, right } = expr {
            let comp_op = ComparisonOp::from_binary_op(op)?;

            // Pattern: column op date_literal
            if let Expression::ColumnRef { table, column } = left.as_ref() {
                let col_idx = schema.get_column_index(table.as_deref(), column)?;
                if let Expression::Literal(SqlValue::Date(date)) = right.as_ref() {
                    return Some((col_idx, comp_op, *date));
                }
            }

            // Pattern: date_literal op column (need to reverse operator)
            if let Expression::Literal(SqlValue::Date(date)) = left.as_ref() {
                if let Expression::ColumnRef { table, column } = right.as_ref() {
                    let col_idx = schema.get_column_index(table.as_deref(), column)?;
                    let reversed_op = match comp_op {
                        ComparisonOp::Lt => ComparisonOp::Gt,
                        ComparisonOp::Le => ComparisonOp::Ge,
                        ComparisonOp::Gt => ComparisonOp::Lt,
                        ComparisonOp::Ge => ComparisonOp::Le,
                        ComparisonOp::Eq => ComparisonOp::Eq,
                        ComparisonOp::NotEq => ComparisonOp::NotEq,
                    };
                    return Some((col_idx, reversed_op, *date));
                }
            }
        }

        None
    }

    /// Detect BETWEEN pattern for numeric values
    fn detect_between_numeric(
        expr: &Expression,
        low: &Expression,
        high: &Expression,
        schema: &CombinedSchema,
    ) -> Option<PredicatePattern> {
        // Extract column reference
        if let Expression::ColumnRef { table, column } = expr {
            let col_idx = schema.get_column_index(table.as_deref(), column)?;

            // Extract numeric bounds
            let min_val = Self::extract_f64_literal(low)?;
            let max_val = Self::extract_f64_literal(high)?;

            return Some(PredicatePattern::BetweenNumeric { col_idx, min: min_val, max: max_val });
        }

        None
    }

    /// Detect pattern: column op constant
    fn detect_column_constant_comparison(
        left: &Expression,
        op: ComparisonOp,
        right: &Expression,
        schema: &CombinedSchema,
    ) -> Option<PredicatePattern> {
        // Extract column index
        if let Expression::ColumnRef { table, column } = left {
            let col_idx = schema.get_column_index(table.as_deref(), column)?;

            // Try to extract integer constant FIRST (before f64)
            // This ensures Integer literals are detected as IntegerComparison
            if let Some(val) = Self::extract_i64_literal(right) {
                return Some(PredicatePattern::IntegerComparison { col_idx, op, constant: val });
            }

            // Try to extract numeric constant (f64)
            if let Some(val) = Self::extract_f64_literal(right) {
                return Some(PredicatePattern::NumericComparison { col_idx, op, constant: val });
            }
        }

        None
    }

    /// Detect pattern: constant op column (need to reverse operator)
    fn detect_constant_column_comparison(
        left: &Expression,
        op: ComparisonOp,
        right: &Expression,
        schema: &CombinedSchema,
    ) -> Option<PredicatePattern> {
        // Extract column index from right side
        if let Expression::ColumnRef { table, column } = right {
            let col_idx = schema.get_column_index(table.as_deref(), column)?;

            // Reverse the operator since constant is on left
            let reversed_op = match op {
                ComparisonOp::Lt => ComparisonOp::Gt,
                ComparisonOp::Le => ComparisonOp::Ge,
                ComparisonOp::Gt => ComparisonOp::Lt,
                ComparisonOp::Ge => ComparisonOp::Le,
                ComparisonOp::Eq => ComparisonOp::Eq,
                ComparisonOp::NotEq => ComparisonOp::NotEq,
            };

            // Try to extract integer constant FIRST (before f64)
            if let Some(val) = Self::extract_i64_literal(left) {
                return Some(PredicatePattern::IntegerComparison {
                    col_idx,
                    op: reversed_op,
                    constant: val,
                });
            }

            // Try to extract numeric constant (f64)
            if let Some(val) = Self::extract_f64_literal(left) {
                return Some(PredicatePattern::NumericComparison {
                    col_idx,
                    op: reversed_op,
                    constant: val,
                });
            }
        }

        None
    }

    /// Extract f64 value from literal expression
    fn extract_f64_literal(expr: &Expression) -> Option<f64> {
        if let Expression::Literal(val) = expr {
            match val {
                SqlValue::Double(f) => Some(*f),
                SqlValue::Float(f) => Some(*f as f64),
                SqlValue::Real(f) => Some(*f as f64),
                SqlValue::Numeric(f) => Some(*f),
                SqlValue::Integer(i) => Some(*i as f64),
                SqlValue::Smallint(i) => Some(*i as f64),
                SqlValue::Bigint(i) => Some(*i as f64),
                _ => None,
            }
        } else {
            None
        }
    }

    /// Extract i64 value from literal expression
    fn extract_i64_literal(expr: &Expression) -> Option<i64> {
        if let Expression::Literal(val) = expr {
            match val {
                SqlValue::Integer(i) => Some(*i),
                SqlValue::Smallint(i) => Some(*i as i64),
                SqlValue::Bigint(i) => Some(*i),
                _ => None,
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    fn create_test_schema() -> CombinedSchema {
        let columns = vec![
            ColumnSchema {
                name: "price".to_string(),
                data_type: DataType::Decimal { precision: 15, scale: 2 },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "quantity".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "shipdate".to_string(),
                data_type: DataType::Date,
                nullable: false,
                default_value: None,
            },
        ];

        let schema = TableSchema::new("lineitem".to_string(), columns);

        CombinedSchema::from_table("lineitem".to_string(), schema)
    }

    #[test]
    fn test_numeric_comparison_detection() {
        let schema = create_test_schema();

        // Test: price < 100.0
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: Some("lineitem".to_string()),
                column: "price".to_string(),
            }),
            op: BinaryOperator::LessThan,
            right: Box::new(Expression::Literal(SqlValue::Double(100.0))),
        };

        let pattern = PredicatePattern::from_where_clause(&expr, &schema);

        match pattern {
            PredicatePattern::NumericComparison { col_idx, op, constant } => {
                assert_eq!(col_idx, 0);
                assert_eq!(op, ComparisonOp::Lt);
                assert_eq!(constant, 100.0);
            }
            _ => panic!("Expected NumericComparison pattern"),
        }
    }

    #[test]
    fn test_integer_comparison_detection() {
        let schema = create_test_schema();

        // Test: quantity > 10
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: Some("lineitem".to_string()),
                column: "quantity".to_string(),
            }),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expression::Literal(SqlValue::Integer(10))),
        };

        let pattern = PredicatePattern::from_where_clause(&expr, &schema);

        match pattern {
            PredicatePattern::IntegerComparison { col_idx, op, constant } => {
                assert_eq!(col_idx, 1);
                assert_eq!(op, ComparisonOp::Gt);
                assert_eq!(constant, 10);
            }
            _ => panic!("Expected IntegerComparison pattern"),
        }
    }

    #[test]
    fn test_date_range_detection() {
        let schema = create_test_schema();

        // Test: shipdate >= '1994-01-01' AND shipdate < '1995-01-01'
        let min_date: vibesql_types::Date = "1994-01-01".parse().unwrap();
        let max_date: vibesql_types::Date = "1995-01-01".parse().unwrap();

        let left_comparison = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: Some("lineitem".to_string()),
                column: "shipdate".to_string(),
            }),
            op: BinaryOperator::GreaterThanOrEqual,
            right: Box::new(Expression::Literal(SqlValue::Date(min_date))),
        };

        let right_comparison = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: Some("lineitem".to_string()),
                column: "shipdate".to_string(),
            }),
            op: BinaryOperator::LessThan,
            right: Box::new(Expression::Literal(SqlValue::Date(max_date))),
        };

        let expr = Expression::BinaryOp {
            left: Box::new(left_comparison),
            op: BinaryOperator::And,
            right: Box::new(right_comparison),
        };

        let pattern = PredicatePattern::from_where_clause(&expr, &schema);

        match pattern {
            PredicatePattern::DateRange {
                col_idx,
                min_date: detected_min,
                max_date: detected_max,
            } => {
                assert_eq!(col_idx, 2);
                assert_eq!(detected_min, min_date);
                assert_eq!(detected_max, max_date);
            }
            _ => panic!("Expected DateRange pattern"),
        }
    }
}
