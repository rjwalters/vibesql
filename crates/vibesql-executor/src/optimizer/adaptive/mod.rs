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
//! 1. Aggregation presence (GROUP BY, aggregate functions)
//! 2. Arithmetic expression complexity
//! 3. Join pattern simplicity (equijoins only)
//! 4. Projection selectivity (column count)
//!
//! Query-comment hints (`/* COLUMNAR */` / `/* ROW_ORIENTED */`) override
//! the heuristics below when present. The lexer captures a recognized hint
//! comment and the parser attaches it to `SelectStmt::hints` when it
//! appears immediately after a leading `SELECT` keyword (see
//! `vibesql_ast::QueryHint` for the exact recognized syntax and
//! scope/precedence rules — multiple hints use last-one-wins). See
//! issue #6534 (original stub removal) and #6547 (this plumbing).
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

use vibesql_ast::{QueryHint, SelectStmt};

mod expression;
mod patterns;
mod query;
pub mod strategy;

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
    // An explicit query-comment hint overrides the heuristics entirely.
    // Multiple hints use last-one-wins precedence (see `QueryHint` docs).
    if let Some(hint) = query.hints.last() {
        return match hint {
            QueryHint::Columnar => ExecutionModel::Columnar,
            QueryHint::RowOriented => ExecutionModel::RowOriented,
        };
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
    use vibesql_ast::{
        BinaryOperator, Expression, FromClause, GroupByClause, JoinType, QueryHint, SelectItem,
        SelectStmt,
    };
    use vibesql_types::SqlValue;

    use super::*;

    #[test]
    fn test_row_oriented_for_point_lookup() {
        // SELECT * FROM users WHERE id = 123
        let query = SelectStmt {
            hints: Vec::new(),
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                index_hint: None,
                name: "users".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "id", false,
                ))),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(123))),
            }),
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        // Should use row-oriented (wildcard projection, no aggregation)
        assert_eq!(choose_execution_model(&query), ExecutionModel::RowOriented);
    }

    #[test]
    fn test_columnar_for_group_by() {
        // SELECT region, SUM(price * quantity) FROM orders GROUP BY region
        // Phase 6: GROUP BY with aggregation is now supported in columnar execution
        let query = SelectStmt {
            hints: Vec::new(),
            with_clause: None,
            distinct: false,
            select_list: vec![
                SelectItem::Expression {
                    expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                        "region", false,
                    )),
                    alias: None,
                    source_text: None,
                },
                SelectItem::Expression {
                    expr: Expression::AggregateFunction {
                        name: vibesql_ast::FunctionIdentifier::new("SUM"),
                        distinct: false,
                        args: vec![Expression::BinaryOp {
                            left: Box::new(Expression::ColumnRef(
                                vibesql_ast::ColumnIdentifier::simple("price", false),
                            )),
                            op: BinaryOperator::Multiply,
                            right: Box::new(Expression::ColumnRef(
                                vibesql_ast::ColumnIdentifier::simple("quantity", false),
                            )),
                        }],
                        order_by: None,
                        filter: None,
                    },
                    alias: None,
                    source_text: None,
                },
            ],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                index_hint: None,
                name: "orders".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: None,
            group_by: Some(GroupByClause::Simple(vec![Expression::ColumnRef(
                vibesql_ast::ColumnIdentifier::simple("region", false),
            )])),
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        // Should use columnar (GROUP BY with aggregation is now supported in Phase 6)
        assert_eq!(choose_execution_model(&query), ExecutionModel::Columnar);
    }

    #[test]
    fn test_columnar_for_aggregation_without_group_by() {
        // SELECT SUM(price * quantity) FROM orders
        // Phase 5 supports aggregation WITHOUT GROUP BY
        let query = SelectStmt {
            hints: Vec::new(),
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::AggregateFunction {
                    name: vibesql_ast::FunctionIdentifier::new("SUM"),
                    distinct: false,
                    args: vec![Expression::BinaryOp {
                        left: Box::new(Expression::ColumnRef(
                            vibesql_ast::ColumnIdentifier::simple("price", false),
                        )),
                        op: BinaryOperator::Multiply,
                        right: Box::new(Expression::ColumnRef(
                            vibesql_ast::ColumnIdentifier::simple("quantity", false),
                        )),
                    }],
                    order_by: None,
                    filter: None,
                },
                alias: Some("total".to_string()),
                source_text: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                index_hint: None,
                name: "orders".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        // Should use columnar (aggregation + arithmetic, no GROUP BY)
        assert_eq!(choose_execution_model(&query), ExecutionModel::Columnar);
    }

    #[test]
    fn test_row_oriented_for_many_joins() {
        // SELECT * FROM t1 JOIN t2 JOIN t3 JOIN t4 (4 tables)
        let query = SelectStmt {
            hints: Vec::new(),
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Join {
                left: Box::new(FromClause::Join {
                    left: Box::new(FromClause::Join {
                        left: Box::new(FromClause::Table {
                            index_hint: None,
                            name: "t1".to_string(),
                            alias: None,
                            column_aliases: None,
                            quoted: false,
                        }),
                        right: Box::new(FromClause::Table {
                            index_hint: None,
                            name: "t2".to_string(),
                            alias: None,
                            column_aliases: None,
                            quoted: false,
                        }),
                        join_type: JoinType::Inner,
                        condition: None,
                        using_columns: None,
                        natural: false,
                        alias: None,
                    }),
                    right: Box::new(FromClause::Table {
                        index_hint: None,
                        name: "t3".to_string(),
                        alias: None,
                        column_aliases: None,
                        quoted: false,
                    }),
                    join_type: JoinType::Inner,
                    condition: None,
                    using_columns: None,
                    natural: false,
                    alias: None,
                }),
                right: Box::new(FromClause::Table {
                    index_hint: None,
                    name: "t4".to_string(),
                    alias: None,
                    column_aliases: None,
                    quoted: false,
                }),
                join_type: JoinType::Inner,
                condition: None,
                using_columns: None,
                natural: false,
                alias: None,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        // Should use row-oriented (4 tables > threshold, wildcard)
        assert_eq!(choose_execution_model(&query), ExecutionModel::RowOriented);
    }

    #[test]
    fn test_has_aggregate_functions() {
        let query_with_count = SelectStmt {
            hints: Vec::new(),
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::AggregateFunction {
                    name: vibesql_ast::FunctionIdentifier::new("COUNT"),
                    distinct: false,
                    args: vec![Expression::Wildcard],
                    order_by: None,
                    filter: None,
                },
                alias: None,
                source_text: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                index_hint: None,
                name: "orders".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        assert!(query::has_aggregate_functions(&query_with_count));
    }

    #[test]
    fn test_has_arithmetic_expressions() {
        let query_with_arithmetic = SelectStmt {
            hints: Vec::new(),
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                        "price", false,
                    ))),
                    op: BinaryOperator::Multiply,
                    right: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                        "quantity", false,
                    ))),
                },
                alias: Some("total".to_string()),
                source_text: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                index_hint: None,
                name: "orders".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        assert!(query::has_arithmetic_expressions(&query_with_arithmetic));
    }

    #[test]
    fn test_selective_projection() {
        // SELECT id, name (2 columns)
        let selective = SelectStmt {
            hints: Vec::new(),
            with_clause: None,
            distinct: false,
            select_list: vec![
                SelectItem::Expression {
                    expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("id", false)),
                    alias: None,
                    source_text: None,
                },
                SelectItem::Expression {
                    expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                        "name", false,
                    )),
                    alias: None,
                    source_text: None,
                },
            ],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                index_hint: None,
                name: "users".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        assert!(query::has_selective_projection(&selective));

        // SELECT * (wildcard)
        let non_selective = SelectStmt {
            hints: Vec::new(),
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                index_hint: None,
                name: "users".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };

        assert!(!query::has_selective_projection(&non_selective));
    }

    /// A minimal point-lookup query that the heuristics alone would select
    /// `RowOriented` for (mirrors `test_row_oriented_for_point_lookup`).
    fn point_lookup_query(hints: Vec<QueryHint>) -> SelectStmt {
        SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                index_hint: None,
                name: "users".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "id", false,
                ))),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(123))),
            }),
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
            hints,
        }
    }

    #[test]
    fn test_columnar_hint_overrides_row_oriented_heuristic() {
        // Heuristics alone would pick RowOriented (point lookup), but an
        // explicit /* COLUMNAR */ hint forces Columnar.
        let query = point_lookup_query(vec![QueryHint::Columnar]);
        assert_eq!(choose_execution_model(&query), ExecutionModel::Columnar);
    }

    #[test]
    fn test_row_oriented_hint_overrides_columnar_heuristic() {
        // Heuristics alone would pick Columnar (GROUP BY aggregation), but
        // an explicit /* ROW_ORIENTED */ hint forces RowOriented.
        let mut query = point_lookup_query(vec![QueryHint::RowOriented]);
        query.where_clause = None;
        query.select_list = vec![SelectItem::Expression {
            expr: Expression::AggregateFunction {
                name: vibesql_ast::FunctionIdentifier::new("SUM"),
                distinct: false,
                args: vec![Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "price", false,
                ))],
                order_by: None,
                filter: None,
            },
            alias: None,
            source_text: None,
        }];
        query.group_by = Some(GroupByClause::Simple(vec![Expression::ColumnRef(
            vibesql_ast::ColumnIdentifier::simple("region", false),
        )]));

        assert_eq!(choose_execution_model(&query), ExecutionModel::RowOriented);
    }

    #[test]
    fn test_conflicting_hints_last_one_wins() {
        // Multiple hints: the last one in source order takes precedence.
        let query = point_lookup_query(vec![QueryHint::RowOriented, QueryHint::Columnar]);
        assert_eq!(choose_execution_model(&query), ExecutionModel::Columnar);

        let query = point_lookup_query(vec![QueryHint::Columnar, QueryHint::RowOriented]);
        assert_eq!(choose_execution_model(&query), ExecutionModel::RowOriented);
    }

    #[test]
    fn test_no_hint_falls_back_to_heuristic() {
        // No hints present: behaves exactly as before (heuristic-driven).
        let query = point_lookup_query(vec![]);
        assert_eq!(choose_execution_model(&query), ExecutionModel::RowOriented);
    }
}
