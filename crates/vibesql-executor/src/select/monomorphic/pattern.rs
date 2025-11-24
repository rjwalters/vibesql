//! AST-based query pattern matching for monomorphic execution paths
//!
//! This module provides a robust pattern matching system based on AST structure
//! rather than string matching. This allows matching semantically equivalent queries
//! regardless of formatting, whitespace, or clause ordering.

use vibesql_ast::{BinaryOperator, Expression, FromClause, SelectItem, SelectStmt};

use crate::schema::CombinedSchema;

/// Trait for matching query patterns based on AST structure
///
/// Implementations should match based on semantic structure (e.g., "single table
/// with aggregation") rather than exact syntax or formatting.
pub trait QueryPattern {
    /// Check if this pattern matches the given SELECT statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The SELECT statement AST to match against
    /// * `schema` - Schema information for resolving column references
    ///
    /// # Returns
    ///
    /// `true` if the query matches this pattern, `false` otherwise
    fn matches(&self, stmt: &SelectStmt, schema: &CombinedSchema) -> bool;

    /// Get a description of this pattern for debugging
    #[allow(dead_code)]
    fn description(&self) -> &str;
}

// =============================================================================
// Helper Functions for Pattern Matching
// =============================================================================

/// Check if the FROM clause references a single table with the given name
///
/// Returns `true` if the FROM clause is a simple table reference (not a join or subquery)
/// and the table name matches the expected name (case-insensitive).
pub fn is_single_table(from_clause: &Option<FromClause>, expected_name: &str) -> bool {
    match from_clause {
        Some(FromClause::Table { name, .. }) => name.eq_ignore_ascii_case(expected_name),
        _ => false,
    }
}

/// Check if the SELECT list contains an aggregate function with the given name
///
/// This function recursively searches through SELECT items and expressions to find
/// aggregate function calls.
pub fn has_aggregate_function(select_list: &[SelectItem], function_name: &str) -> bool {
    select_list.iter().any(|item| match item {
        SelectItem::Expression { expr, .. } => contains_aggregate(expr, function_name),
        _ => false,
    })
}

/// Recursively check if an expression contains an aggregate function with the given name
fn contains_aggregate(expr: &Expression, function_name: &str) -> bool {
    match expr {
        Expression::AggregateFunction { name, .. } => name.eq_ignore_ascii_case(function_name),
        Expression::BinaryOp { left, right, .. } => {
            contains_aggregate(left, function_name) || contains_aggregate(right, function_name)
        }
        Expression::UnaryOp { expr, .. } => contains_aggregate(expr, function_name),
        Expression::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            operand
                .as_ref()
                .map(|e| contains_aggregate(e, function_name))
                .unwrap_or(false)
                || when_clauses.iter().any(|wc| {
                    wc.conditions
                        .iter()
                        .any(|c| contains_aggregate(c, function_name))
                        || contains_aggregate(&wc.result, function_name)
                })
                || else_result
                    .as_ref()
                    .map(|e| contains_aggregate(e, function_name))
                    .unwrap_or(false)
        }
        _ => false,
    }
}

/// Count the total number of aggregate function calls in the SELECT list
///
/// This function counts ALL aggregate functions (SUM, COUNT, AVG, etc.) across
/// all SELECT items, regardless of their names.
pub fn count_aggregate_functions(select_list: &[SelectItem]) -> usize {
    select_list.iter().map(|item| match item {
        SelectItem::Expression { expr, .. } => count_aggregates_in_expr(expr),
        _ => 0,
    }).sum()
}

/// Recursively count aggregate functions in an expression
fn count_aggregates_in_expr(expr: &Expression) -> usize {
    match expr {
        Expression::AggregateFunction { .. } => 1,
        Expression::BinaryOp { left, right, .. } => {
            count_aggregates_in_expr(left) + count_aggregates_in_expr(right)
        }
        Expression::UnaryOp { expr, .. } => count_aggregates_in_expr(expr),
        Expression::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            operand
                .as_ref()
                .map(|e| count_aggregates_in_expr(e))
                .unwrap_or(0)
                + when_clauses.iter().map(|wc| {
                    wc.conditions
                        .iter()
                        .map(count_aggregates_in_expr)
                        .sum::<usize>()
                        + count_aggregates_in_expr(&wc.result)
                }).sum::<usize>()
                + else_result
                    .as_ref()
                    .map(|e| count_aggregates_in_expr(e))
                    .unwrap_or(0)
        }
        _ => 0,
    }
}

/// Check if the expression contains a multiplication of two specific columns
///
/// This matches expressions like `col1 * col2` regardless of operand order (case-insensitive).
pub fn contains_column_multiply(expr: &Expression, col1: &str, col2: &str) -> bool {
    match expr {
        Expression::BinaryOp {
            op: BinaryOperator::Multiply,
            left,
            right,
        } => {
            let (left_col, right_col) = match (left.as_ref(), right.as_ref()) {
                (Expression::ColumnRef { column: c1, .. }, Expression::ColumnRef { column: c2, .. }) => {
                    (c1.as_str(), c2.as_str())
                }
                _ => return false,
            };

            // Check both orderings (case-insensitive)
            (left_col.eq_ignore_ascii_case(col1) && right_col.eq_ignore_ascii_case(col2)) ||
            (left_col.eq_ignore_ascii_case(col2) && right_col.eq_ignore_ascii_case(col1))
        }
        Expression::AggregateFunction { args, .. } => {
            // Recursively check aggregate arguments
            args.iter().any(|arg| contains_column_multiply(arg, col1, col2))
        }
        Expression::BinaryOp { left, right, .. } => {
            contains_column_multiply(left, col1, col2)
                || contains_column_multiply(right, col1, col2)
        }
        Expression::UnaryOp { expr, .. } => contains_column_multiply(expr, col1, col2),
        _ => false,
    }
}

/// Check if a WHERE clause contains a reference to a specific column
///
/// This recursively searches through the expression tree to find column references.
pub fn where_references_column(where_clause: &Option<Expression>, column_name: &str) -> bool {
    match where_clause {
        Some(expr) => expression_references_column(expr, column_name),
        None => false,
    }
}

/// Recursively check if an expression references a specific column (case-insensitive)
fn expression_references_column(expr: &Expression, column_name: &str) -> bool {
    match expr {
        Expression::ColumnRef { column, .. } => column.eq_ignore_ascii_case(column_name),
        Expression::BinaryOp { left, right, .. } => {
            expression_references_column(left, column_name)
                || expression_references_column(right, column_name)
        }
        Expression::UnaryOp { expr, .. } => expression_references_column(expr, column_name),
        Expression::Between { expr, low, high, .. } => {
            expression_references_column(expr, column_name)
                || expression_references_column(low, column_name)
                || expression_references_column(high, column_name)
        }
        Expression::IsNull { expr, .. } => expression_references_column(expr, column_name),
        Expression::InList { expr, values, .. } => {
            expression_references_column(expr, column_name)
                || values
                    .iter()
                    .any(|v| expression_references_column(v, column_name))
        }
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            args.iter()
                .any(|arg| expression_references_column(arg, column_name))
        }
        Expression::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            operand
                .as_ref()
                .map(|e| expression_references_column(e, column_name))
                .unwrap_or(false)
                || when_clauses.iter().any(|wc| {
                    wc.conditions
                        .iter()
                        .any(|c| expression_references_column(c, column_name))
                        || expression_references_column(&wc.result, column_name)
                })
                || else_result
                    .as_ref()
                    .map(|e| expression_references_column(e, column_name))
                    .unwrap_or(false)
        }
        _ => false,
    }
}

/// Check if the WHERE clause contains a BETWEEN predicate on a specific column
pub fn has_between_predicate(where_clause: &Option<Expression>, column_name: &str) -> bool {
    match where_clause {
        Some(expr) => find_between_predicate(expr, column_name),
        None => false,
    }
}

/// Recursively search for a BETWEEN predicate on a specific column (case-insensitive)
fn find_between_predicate(expr: &Expression, column_name: &str) -> bool {
    match expr {
        Expression::Between {
            expr: inner_expr,
            negated: false,
            ..
        } => {
            if let Expression::ColumnRef { column, .. } = inner_expr.as_ref() {
                column.eq_ignore_ascii_case(column_name)
            } else {
                false
            }
        }
        Expression::BinaryOp {
            op: BinaryOperator::And,
            left,
            right,
        } => find_between_predicate(left, column_name) || find_between_predicate(right, column_name),
        _ => false,
    }
}

/// Check if the query has no joins (single table query)
pub fn has_no_joins(from_clause: &Option<FromClause>) -> bool {
    match from_clause {
        Some(FromClause::Table { .. }) => true,
        Some(FromClause::Join { .. }) => false,
        Some(FromClause::Subquery { .. }) => false,
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::{Expression, SelectItem};

    #[test]
    fn test_is_single_table() {
        let from_table = Some(FromClause::Table {
            name: "lineitem".to_string(),
            alias: None,
        });
        assert!(is_single_table(&from_table, "lineitem"));
        assert!(!is_single_table(&from_table, "orders"));

        let from_join = Some(FromClause::Join {
            left: Box::new(FromClause::Table {
                name: "lineitem".to_string(),
                alias: None,
            }),
            right: Box::new(FromClause::Table {
                name: "orders".to_string(),
                alias: None,
            }),
            join_type: vibesql_ast::JoinType::Inner,
            condition: None,
            natural: false,
        });
        assert!(!is_single_table(&from_join, "lineitem"));
    }

    #[test]
    fn test_has_aggregate_function() {
        let select_with_sum = vec![SelectItem::Expression {
            expr: Expression::AggregateFunction {
                name: "SUM".to_string(),
                distinct: false,
                args: vec![Expression::ColumnRef {
                    table: None,
                    column: "price".to_string(),
                }],
            },
            alias: Some("total".to_string()),
        }];
        assert!(has_aggregate_function(&select_with_sum, "SUM"));
        assert!(has_aggregate_function(&select_with_sum, "sum")); // case insensitive
        assert!(!has_aggregate_function(&select_with_sum, "COUNT"));

        let select_no_agg = vec![SelectItem::Expression {
            expr: Expression::ColumnRef {
                table: None,
                column: "price".to_string(),
            },
            alias: None,
        }];
        assert!(!has_aggregate_function(&select_no_agg, "SUM"));
    }

    #[test]
    fn test_contains_column_multiply() {
        let multiply_expr = Expression::BinaryOp {
            op: BinaryOperator::Multiply,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "price".to_string(),
            }),
            right: Box::new(Expression::ColumnRef {
                table: None,
                column: "discount".to_string(),
            }),
        };

        assert!(contains_column_multiply(&multiply_expr, "price", "discount"));
        assert!(contains_column_multiply(&multiply_expr, "discount", "price")); // order independent
        assert!(!contains_column_multiply(&multiply_expr, "price", "quantity"));

        // Test nested in aggregate
        let agg_expr = Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: false,
            args: vec![multiply_expr],
        };
        assert!(contains_column_multiply(&agg_expr, "price", "discount"));
    }

    #[test]
    fn test_where_references_column() {
        let where_expr = Some(Expression::BinaryOp {
            op: BinaryOperator::LessThan,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "quantity".to_string(),
            }),
            right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(24))),
        });

        assert!(where_references_column(&where_expr, "quantity"));
        assert!(!where_references_column(&where_expr, "price"));
        assert!(!where_references_column(&None, "quantity"));
    }

    #[test]
    fn test_has_between_predicate() {
        let between_expr = Some(Expression::Between {
            expr: Box::new(Expression::ColumnRef {
                table: None,
                column: "discount".to_string(),
            }),
            low: Box::new(Expression::Literal(vibesql_types::SqlValue::Double(0.05))),
            high: Box::new(Expression::Literal(vibesql_types::SqlValue::Double(0.07))),
            negated: false,
            symmetric: false,
        });

        assert!(has_between_predicate(&between_expr, "discount"));
        assert!(!has_between_predicate(&between_expr, "quantity"));
    }
}
