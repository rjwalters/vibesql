//! Index strategy selection for predicates
//!
//! This module determines how to use indexes to evaluate predicates efficiently.
//! It replaces the scattered index predicate extraction logic with a unified
//! approach that works with the typed `Predicate` enum.

use vibesql_types::SqlValue;

use super::predicate::Predicate;
use vibesql_ast::{BinaryOperator, Expression};

/// Strategy for using an index to evaluate a predicate
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum IndexStrategy {
    /// Exact key lookup (equality predicate)
    ///
    /// Used for predicates like `col = value`
    ExactLookup { column: String, value: SqlValue },

    /// Multiple exact lookups (IN clause)
    ///
    /// Used for predicates like `col IN (1, 2, 3)`
    MultiLookup { column: String, values: Vec<SqlValue> },

    /// Prefix lookup on multi-column index
    ///
    /// Used when matching the first N columns of a composite index
    PrefixLookup {
        columns: Vec<String>,
        prefix_values: Vec<SqlValue>,
    },

    /// Range scan with optional bounds
    ///
    /// Used for predicates like `col > 10`, `col BETWEEN 1 AND 100`
    RangeScan {
        column: String,
        lower_bound: Option<SqlValue>,
        upper_bound: Option<SqlValue>,
        include_lower: bool,
        include_upper: bool,
    },

    /// Full index scan (no filtering, just ordering)
    ///
    /// Used when index can provide ordering but not filtering
    FullScan { columns: Vec<String> },
}

/// Metadata about an available index on a table
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct IndexMetadata {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[allow(dead_code)]
impl IndexStrategy {
    /// Extract index strategy from a table-local predicate
    ///
    /// This analyzes a predicate that references a single table and determines
    /// if an index can be used to evaluate it efficiently.
    ///
    /// Returns `None` if the predicate cannot be evaluated using the index.
    pub fn from_predicate(predicate: &Predicate, index: &IndexMetadata) -> Option<Self> {
        match predicate {
            Predicate::TableLocal { expression, .. } => {
                // For single-column indexes, try to extract strategy from expression
                if index.columns.len() == 1 {
                    let column = &index.columns[0];
                    extract_single_column_strategy(expression, column)
                } else {
                    // For multi-column indexes, try to match prefix
                    extract_prefix_strategy(expression, &index.columns)
                }
            }
            _ => None, // Only table-local predicates can use indexes
        }
    }

    /// Check if this strategy fully satisfies the predicate
    ///
    /// Returns `true` if using this index eliminates the need for additional
    /// filtering on the predicate. This is important for determining if we can
    /// skip the filter step after index scan.
    pub fn fully_satisfies(&self, predicate: &Predicate) -> bool {
        match (self, predicate) {
            (IndexStrategy::ExactLookup { .. }, Predicate::TableLocal { .. }) => true,
            (IndexStrategy::MultiLookup { .. }, Predicate::TableLocal { .. }) => true,
            (IndexStrategy::RangeScan { .. }, Predicate::TableLocal { expression, .. }) => {
                // Range scan fully satisfies if it matches the entire predicate
                // For now, conservatively return false for complex expressions
                matches!(
                    expression,
                    Expression::BinaryOp {
                        op: BinaryOperator::Equal
                            | BinaryOperator::GreaterThan
                            | BinaryOperator::GreaterThanOrEqual
                            | BinaryOperator::LessThan
                            | BinaryOperator::LessThanOrEqual,
                        ..
                    } | Expression::Between { .. }
                )
            }
            _ => false,
        }
    }

    /// Get the columns used by this index strategy
    pub fn columns(&self) -> Vec<&str> {
        match self {
            IndexStrategy::ExactLookup { column, .. } => vec![column.as_str()],
            IndexStrategy::MultiLookup { column, .. } => vec![column.as_str()],
            IndexStrategy::PrefixLookup { columns, .. } => {
                columns.iter().map(|s| s.as_str()).collect()
            }
            IndexStrategy::RangeScan { column, .. } => vec![column.as_str()],
            IndexStrategy::FullScan { columns } => columns.iter().map(|s| s.as_str()).collect(),
        }
    }
}

/// Extract index strategy for a single-column index
#[allow(dead_code)]
fn extract_single_column_strategy(expr: &Expression, column_name: &str) -> Option<IndexStrategy> {
    match expr {
        Expression::BinaryOp { left, op, right } => match op {
            BinaryOperator::Equal => {
                // Check if left is our column and right is a literal
                if is_column_ref(left, column_name) {
                    if let Expression::Literal(value) = right.as_ref() {
                        if matches!(value, SqlValue::Null) {
                            return None; // NULL equality can't use index
                        }
                        return Some(IndexStrategy::ExactLookup {
                            column: column_name.to_string(),
                            value: value.clone(),
                        });
                    }
                }
                // Also handle reverse: value = col
                if is_column_ref(right, column_name) {
                    if let Expression::Literal(value) = left.as_ref() {
                        if matches!(value, SqlValue::Null) {
                            return None;
                        }
                        return Some(IndexStrategy::ExactLookup {
                            column: column_name.to_string(),
                            value: value.clone(),
                        });
                    }
                }
                None
            }
            BinaryOperator::GreaterThan
            | BinaryOperator::GreaterThanOrEqual
            | BinaryOperator::LessThan
            | BinaryOperator::LessThanOrEqual => {
                // Check if left is our column and right is a literal
                if is_column_ref(left, column_name) {
                    if let Expression::Literal(value) = right.as_ref() {
                        if matches!(value, SqlValue::Null) {
                            return None;
                        }
                        return Some(match op {
                            BinaryOperator::GreaterThan => IndexStrategy::RangeScan {
                                column: column_name.to_string(),
                                lower_bound: Some(value.clone()),
                                upper_bound: None,
                                include_lower: false,
                                include_upper: false,
                            },
                            BinaryOperator::GreaterThanOrEqual => IndexStrategy::RangeScan {
                                column: column_name.to_string(),
                                lower_bound: Some(value.clone()),
                                upper_bound: None,
                                include_lower: true,
                                include_upper: false,
                            },
                            BinaryOperator::LessThan => IndexStrategy::RangeScan {
                                column: column_name.to_string(),
                                lower_bound: None,
                                upper_bound: Some(value.clone()),
                                include_lower: false,
                                include_upper: false,
                            },
                            BinaryOperator::LessThanOrEqual => IndexStrategy::RangeScan {
                                column: column_name.to_string(),
                                lower_bound: None,
                                upper_bound: Some(value.clone()),
                                include_lower: false,
                                include_upper: true,
                            },
                            _ => unreachable!(),
                        });
                    }
                }
                None
            }
            _ => None,
        },
        Expression::Between { expr, low, high, .. } => {
            // Check if expr is our column and bounds are literals
            if is_column_ref(expr, column_name) {
                if let (Expression::Literal(low_val), Expression::Literal(high_val)) =
                    (low.as_ref(), high.as_ref())
                {
                    if matches!(low_val, SqlValue::Null) || matches!(high_val, SqlValue::Null) {
                        return None;
                    }
                    return Some(IndexStrategy::RangeScan {
                        column: column_name.to_string(),
                        lower_bound: Some(low_val.clone()),
                        upper_bound: Some(high_val.clone()),
                        include_lower: true,
                        include_upper: true,
                    });
                }
            }
            None
        }
        Expression::InList { expr, values, .. } => {
            // Check if expr is our column and all values are literals
            if is_column_ref(expr, column_name) {
                let mut literal_values = Vec::new();
                for val in values {
                    if let Expression::Literal(lit) = val {
                        if matches!(lit, SqlValue::Null) {
                            return None; // Can't use index for NULL
                        }
                        literal_values.push(lit.clone());
                    } else {
                        return None; // All values must be literals
                    }
                }
                return Some(IndexStrategy::MultiLookup {
                    column: column_name.to_string(),
                    values: literal_values,
                });
            }
            None
        }
        _ => None,
    }
}

/// Extract index strategy for a multi-column index prefix
#[allow(dead_code)]
fn extract_prefix_strategy(expr: &Expression, index_columns: &[String]) -> Option<IndexStrategy> {
    // For now, only support exact matching on the first column
    // Full prefix matching would require more sophisticated analysis
    if index_columns.is_empty() {
        return None;
    }

    extract_single_column_strategy(expr, &index_columns[0])
}

/// Check if an expression is a column reference to a specific column
#[allow(dead_code)]
fn is_column_ref(expr: &Expression, column_name: &str) -> bool {
    match expr {
        Expression::ColumnRef {
            table: _,
            column: col,
        } => col.to_lowercase() == column_name.to_lowercase(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(dead_code)]
    fn make_index(columns: Vec<String>) -> IndexMetadata {
        IndexMetadata {
            name: "test_idx".to_string(),
            columns,
            unique: false,
        }
    }

    #[test]
    fn test_exact_lookup_strategy() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "id".to_string(),
            }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(42))),
        };

        let strategy = extract_single_column_strategy(&expr, "id");
        assert!(matches!(strategy, Some(IndexStrategy::ExactLookup { .. })));
    }

    #[test]
    fn test_range_scan_strategy() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "age".to_string(),
            }),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expression::Literal(SqlValue::Integer(18))),
        };

        let strategy = extract_single_column_strategy(&expr, "age");
        assert!(matches!(strategy, Some(IndexStrategy::RangeScan { .. })));
    }

    #[test]
    fn test_multi_lookup_strategy() {
        let expr = Expression::InList {
            expr: Box::new(Expression::ColumnRef {
                table: None,
                column: "status".to_string(),
            }),
            values: vec![
                Expression::Literal(SqlValue::Varchar("active".to_string())),
                Expression::Literal(SqlValue::Varchar("pending".to_string())),
            ],
            negated: false,
        };

        let strategy = extract_single_column_strategy(&expr, "status");
        assert!(matches!(strategy, Some(IndexStrategy::MultiLookup { .. })));
    }

    #[test]
    fn test_between_strategy() {
        let expr = Expression::Between {
            expr: Box::new(Expression::ColumnRef {
                table: None,
                column: "price".to_string(),
            }),
            low: Box::new(Expression::Literal(SqlValue::Integer(10))),
            high: Box::new(Expression::Literal(SqlValue::Integer(100))),
            negated: false,
            symmetric: false,
        };

        let strategy = extract_single_column_strategy(&expr, "price");
        assert!(matches!(strategy, Some(IndexStrategy::RangeScan { .. })));
    }

    #[test]
    fn test_null_equality_returns_none() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "id".to_string(),
            }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Null)),
        };

        let strategy = extract_single_column_strategy(&expr, "id");
        assert!(strategy.is_none());
    }

    #[test]
    fn test_is_column_ref() {
        let expr = Expression::ColumnRef {
            table: None,
            column: "name".to_string(),
        };
        assert!(is_column_ref(&expr, "name"));
        assert!(is_column_ref(&expr, "NAME")); // Case insensitive
        assert!(!is_column_ref(&expr, "other"));
    }

    #[test]
    fn test_strategy_columns() {
        let strategy = IndexStrategy::ExactLookup {
            column: "id".to_string(),
            value: SqlValue::Integer(1),
        };
        assert_eq!(strategy.columns(), vec!["id"]);

        let strategy = IndexStrategy::MultiLookup {
            column: "status".to_string(),
            values: vec![],
        };
        assert_eq!(strategy.columns(), vec!["status"]);
    }
}
