//! Adaptive Execution Model Selection
//!
//! Automatically chooses between row-oriented and columnar execution based on
//! query patterns to ensure optimal performance for both OLTP and analytical workloads.
//!
//! ## Execution Models
//!
//! - **RowOriented**: Traditional tuple-at-a-time processing, optimal for:
//!   - Point lookups (WHERE id = 123)
//!   - Small result sets (<1000 rows)
//!   - Wide projections (SELECT *)
//!   - Complex joins with many predicates
//!   - OLTP transactions
//!
//! - **Columnar**: Vectorized columnar processing with SIMD, optimal for:
//!   - Analytical queries (GROUP BY, aggregations)
//!   - Heavy arithmetic expressions
//!   - Large table scans with filtering
//!   - Few columns projected
//!   - TPC-H style queries
//!
//! ## Heuristics
//!
//! The execution model is selected based on:
//! 1. Query hints (`/* COLUMNAR */` or `/* ROW_ORIENTED */`)
//! 2. Aggregation presence (GROUP BY, aggregate functions)
//! 3. Arithmetic expression complexity
//! 4. Join pattern simplicity (equijoins only)
//! 5. Projection selectivity (column count)
//!
//! ## Example
//!
//! ```text
//! use vibesql_executor::optimizer::adaptive::{ExecutionModel, choose_execution_model};
//! use vibesql_ast::SelectStmt;
//!
//! let query: SelectStmt = // ... parse query
//! let model = choose_execution_model(&query);
//!
//! match model {
//!     ExecutionModel::RowOriented => {
//!         // Use traditional row-by-row execution
//!     }
//!     ExecutionModel::Columnar => {
//!         // Use columnar execution with SIMD
//!     }
//! }
//! ```

use vibesql_ast::SelectStmt;

mod expression;
mod hints;
mod patterns;
mod query;
pub mod strategy;

use hints::extract_query_hint;
use patterns::has_analytical_pattern;

// Re-export strategy types for external use
pub use strategy::{choose_execution_strategy, ExecutionStrategy, StrategyContext};

/// Execution model for query processing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionModel {
    /// Traditional row-oriented tuple-at-a-time processing
    ///
    /// Best for:
    /// - OLTP-style queries (point lookups, small updates)
    /// - Queries with complex predicates or joins
    /// - Wide projections (many columns)
    /// - Small result sets
    RowOriented,

    /// Vectorized columnar processing with SIMD support
    ///
    /// Best for:
    /// - Analytical queries (GROUP BY, aggregations)
    /// - Arithmetic-heavy queries
    /// - Large table scans with filtering
    /// - Narrow projections (few columns)
    Columnar,
}

/// Choose the optimal execution model for a query
///
/// Uses heuristics to determine whether row-oriented or columnar execution
/// will perform better for the given query pattern.
///
/// # Arguments
/// * `query` - The SELECT statement to analyze
///
/// # Returns
/// The recommended execution model (RowOriented or Columnar)
///
/// # Example
///
/// ```text
/// // Analytical query → Columnar
/// let query = parse("SELECT SUM(price * quantity) FROM orders GROUP BY region");
/// assert_eq!(choose_execution_model(&query), ExecutionModel::Columnar);
///
/// // Point lookup → RowOriented
/// let query = parse("SELECT * FROM users WHERE id = 123");
/// assert_eq!(choose_execution_model(&query), ExecutionModel::RowOriented);
/// ```
pub fn choose_execution_model(query: &SelectStmt) -> ExecutionModel {
    // Check for query hints first (manual override)
    if let Some(hint) = extract_query_hint(query) {
        return hint;
    }

    // Apply heuristics to detect analytical patterns
    if has_analytical_pattern(query) {
        ExecutionModel::Columnar
    } else {
        ExecutionModel::RowOriented
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::{
        BinaryOperator, Expression, FromClause, GroupByClause, JoinType, SelectItem, SelectStmt,
    };
    use vibesql_types::SqlValue;

    #[test]
    fn test_row_oriented_for_point_lookup() {
        // SELECT * FROM users WHERE id = 123
        let query = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "users".to_string(),
                alias: None,
                column_aliases: None,
            }),
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(123))),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        // Should use row-oriented (wildcard projection, no aggregation)
        assert_eq!(choose_execution_model(&query), ExecutionModel::RowOriented);
    }

    #[test]
    fn test_columnar_for_group_by() {
        // SELECT region, SUM(price * quantity) FROM orders GROUP BY region
        // Phase 6: GROUP BY with aggregation is now supported in columnar execution
        let query = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![
                SelectItem::Expression {
                    expr: Expression::ColumnRef { table: None, column: "region".to_string() },
                    alias: None,
                },
                SelectItem::Expression {
                    expr: Expression::AggregateFunction {
                        name: "SUM".to_string(),
                        distinct: false,
                        args: vec![Expression::BinaryOp {
                            left: Box::new(Expression::ColumnRef {
                                table: None,
                                column: "price".to_string(),
                            }),
                            op: BinaryOperator::Multiply,
                            right: Box::new(Expression::ColumnRef {
                                table: None,
                                column: "quantity".to_string(),
                            }),
                        }],
                    },
                    alias: None,
                },
            ],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "orders".to_string(),
                alias: None,
                column_aliases: None,
            }),
            where_clause: None,
            group_by: Some(GroupByClause::Simple(vec![Expression::ColumnRef {
                table: None,
                column: "region".to_string(),
            }])),
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        // Should use columnar (GROUP BY with aggregation is now supported in Phase 6)
        assert_eq!(choose_execution_model(&query), ExecutionModel::Columnar);
    }

    #[test]
    fn test_columnar_for_aggregation_without_group_by() {
        // SELECT SUM(price * quantity) FROM orders
        // Phase 5 supports aggregation WITHOUT GROUP BY
        let query = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::AggregateFunction {
                    name: "SUM".to_string(),
                    distinct: false,
                    args: vec![Expression::BinaryOp {
                        left: Box::new(Expression::ColumnRef {
                            table: None,
                            column: "price".to_string(),
                        }),
                        op: BinaryOperator::Multiply,
                        right: Box::new(Expression::ColumnRef {
                            table: None,
                            column: "quantity".to_string(),
                        }),
                    }],
                },
                alias: Some("total".to_string()),
            }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "orders".to_string(),
                alias: None,
                column_aliases: None,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        // Should use columnar (aggregation + arithmetic, no GROUP BY)
        assert_eq!(choose_execution_model(&query), ExecutionModel::Columnar);
    }

    #[test]
    fn test_row_oriented_for_many_joins() {
        // SELECT * FROM t1 JOIN t2 JOIN t3 JOIN t4 (4 tables)
        let query = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Join {
                left: Box::new(FromClause::Join {
                    left: Box::new(FromClause::Join {
                        left: Box::new(FromClause::Table {
                            name: "t1".to_string(),
                            alias: None,
                            column_aliases: None,
                        }),
                        right: Box::new(FromClause::Table {
                            name: "t2".to_string(),
                            alias: None,
                            column_aliases: None,
                        }),
                        join_type: JoinType::Inner,
                        condition: None,
                        natural: false,
                    }),
                    right: Box::new(FromClause::Table {
                        name: "t3".to_string(),
                        alias: None,
                        column_aliases: None,
                    }),
                    join_type: JoinType::Inner,
                    condition: None,
                    natural: false,
                }),
                right: Box::new(FromClause::Table {
                    name: "t4".to_string(),
                    alias: None,
                    column_aliases: None,
                }),
                join_type: JoinType::Inner,
                condition: None,
                natural: false,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        // Should use row-oriented (4 tables > threshold, wildcard)
        assert_eq!(choose_execution_model(&query), ExecutionModel::RowOriented);
    }

    #[test]
    fn test_has_aggregate_functions() {
        let query_with_count = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![Expression::Wildcard],
                },
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "orders".to_string(),
                alias: None,
                column_aliases: None,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        assert!(query::has_aggregate_functions(&query_with_count));
    }

    #[test]
    fn test_has_arithmetic_expressions() {
        let query_with_arithmetic = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "price".to_string(),
                    }),
                    op: BinaryOperator::Multiply,
                    right: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "quantity".to_string(),
                    }),
                },
                alias: Some("total".to_string()),
            }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "orders".to_string(),
                alias: None,
                column_aliases: None,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        assert!(query::has_arithmetic_expressions(&query_with_arithmetic));
    }

    #[test]
    fn test_selective_projection() {
        // SELECT id, name (2 columns)
        let selective = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![
                SelectItem::Expression {
                    expr: Expression::ColumnRef { table: None, column: "id".to_string() },
                    alias: None,
                },
                SelectItem::Expression {
                    expr: Expression::ColumnRef { table: None, column: "name".to_string() },
                    alias: None,
                },
            ],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "users".to_string(),
                alias: None,
                column_aliases: None,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        assert!(query::has_selective_projection(&selective));

        // SELECT * (wildcard)
        let non_selective = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "users".to_string(),
                alias: None,
                column_aliases: None,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        assert!(!query::has_selective_projection(&non_selective));
    }
}
