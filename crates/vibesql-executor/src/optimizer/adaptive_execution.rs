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
//! ```rust,ignore
//! use vibesql_executor::optimizer::adaptive_execution::{ExecutionModel, choose_execution_model};
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

use vibesql_ast::{BinaryOperator, Expression, FromClause, JoinType, SelectItem, SelectStmt};

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
/// ```rust,ignore
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

/// Extract query execution hint from comment
///
/// Supports:
/// - `/* COLUMNAR */` - Force columnar execution
/// - `/* ROW_ORIENTED */` - Force row-oriented execution
///
/// Note: Currently returns None as hint parsing from comments
/// would require parser changes. This is a placeholder for future enhancement.
fn extract_query_hint(_query: &SelectStmt) -> Option<ExecutionModel> {
    // TODO: Extract hints from query comments when parser support is added
    // For now, hints are not supported
    None
}

/// Detect if a query has analytical patterns suitable for columnar execution
///
/// Returns true if the query benefits from columnar execution based on:
/// - Has aggregation (GROUP BY or aggregate functions like SUM, AVG, COUNT)
/// - Has arithmetic expressions (price * quantity, price * (1 - discount))
/// - Simple equijoins only (no complex nested joins or non-equijoins)
/// - Selective projection (few columns, not SELECT *)
///
/// # Phase 5 Limitations (Current Columnar Execution Support)
///
/// The current columnar execution implementation (Phase 5) has these constraints:
/// - **No GROUP BY**: Only simple aggregates without grouping (e.g., SUM, AVG, COUNT)
/// - **Single table only**: No JOIN support yet (planned for Phase 6)
/// - **No DISTINCT**: Not yet implemented in columnar path
/// - **Simple predicates**: AND-only predicates (no OR/IN/complex expressions)
///
/// These heuristics will evolve as columnar execution gains more capabilities.
///
/// # Rationale
///
/// Columnar execution excels at:
/// - Aggregations: SIMD can process multiple values per instruction
/// - Arithmetic: Vectorized operations on packed column data
/// - Scans: Better cache locality when accessing few columns
///
/// Row-oriented is better for:
/// - Point lookups: Single row access patterns
/// - Wide projections: Need all columns anyway
/// - Complex joins: Tuple-at-a-time processing more flexible
fn has_analytical_pattern(query: &SelectStmt) -> bool {
    // Phase 5 limitation: No GROUP BY support yet
    // Columnar execution only supports simple aggregates without grouping
    if has_group_by(query) {
        return false;
    }

    // Phase 5 limitation: Single table only (no JOINs)
    // count_tables() will return 0 for no FROM, 1 for single table, 2+ for joins
    let is_single_table = match &query.from {
        None => false,                        // No FROM clause -> not suitable
        Some(from) => count_tables(from) == 1, // Exactly one table
    };

    if !is_single_table {
        return false;
    }

    // Phase 5 limitation: No DISTINCT support
    if query.distinct {
        return false;
    }

    // Must have aggregate functions (not just GROUP BY)
    let has_aggregation = has_aggregate_functions(query);
    if !has_aggregation {
        return false;
    }

    // Beneficial if query has arithmetic expressions OR selective projection
    // - Arithmetic: Vectorized operations (e.g., SUM(price * quantity))
    // - Selective projection: Avoid conversion overhead for wide rows
    let has_arithmetic = has_arithmetic_expressions(query);
    let selective_projection = has_selective_projection(query);

    // Columnar execution is beneficial if:
    // - Has aggregation (required)
    // - Single table (required by Phase 5)
    // - Either arithmetic expressions OR selective projection
    has_arithmetic || selective_projection
}

/// Check if query has GROUP BY clause
fn has_group_by(query: &SelectStmt) -> bool {
    query.group_by.is_some()
}

/// Check if query contains aggregate functions (SUM, AVG, MIN, MAX, COUNT)
fn has_aggregate_functions(query: &SelectStmt) -> bool {
    query.select_list.iter().any(|item| match item {
        SelectItem::Expression { expr, .. } => contains_aggregate(expr),
        SelectItem::Wildcard { .. } => false,
        SelectItem::QualifiedWildcard { .. } => false,
    })
}

/// Recursively check if an expression contains aggregate functions
fn contains_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::AggregateFunction { .. } => true,
        Expression::Function { args, .. } => {
            // Check arguments for nested aggregates
            args.iter().any(contains_aggregate)
        }
        Expression::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        Expression::UnaryOp { expr, .. } => contains_aggregate(expr),
        Expression::Case {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            operand.as_ref().map_or(false, |e| contains_aggregate(e))
                || when_clauses.iter().any(|clause| {
                    clause.conditions.iter().any(contains_aggregate)
                        || contains_aggregate(&clause.result)
                })
                || else_result
                    .as_ref()
                    .map_or(false, |e| contains_aggregate(e))
        }
        Expression::InList { expr, values, .. } => {
            contains_aggregate(expr) || values.iter().any(contains_aggregate)
        }
        Expression::Between {
            expr, low, high, ..
        } => contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high),
        Expression::ScalarSubquery(_) | Expression::In { .. } => {
            // Conservative: assume subqueries may contain aggregates
            true
        }
        _ => false,
    }
}

/// Check if query contains arithmetic expressions in SELECT list or WHERE clause
///
/// Arithmetic expressions like `price * (1 - discount)` benefit from SIMD
/// vectorization in columnar execution.
fn has_arithmetic_expressions(query: &SelectStmt) -> bool {
    // Check SELECT list for arithmetic
    let select_has_arithmetic = query.select_list.iter().any(|item| match item {
        SelectItem::Expression { expr, .. } => contains_arithmetic(expr),
        _ => false,
    });

    // Check WHERE clause for arithmetic
    let where_has_arithmetic = query
        .where_clause
        .as_ref()
        .map_or(false, contains_arithmetic);

    select_has_arithmetic || where_has_arithmetic
}

/// Recursively check if an expression contains arithmetic operations
fn contains_arithmetic(expr: &Expression) -> bool {
    match expr {
        Expression::BinaryOp { op, left, right } => {
            let is_arithmetic = matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo
                    | BinaryOperator::IntegerDivide
                    | BinaryOperator::Concat
            );

            is_arithmetic || contains_arithmetic(left) || contains_arithmetic(right)
        }
        Expression::UnaryOp { expr, .. } => contains_arithmetic(expr),
        Expression::Case {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            operand
                .as_ref()
                .map_or(false, |e| contains_arithmetic(e))
                || when_clauses.iter().any(|clause| {
                    clause.conditions.iter().any(contains_arithmetic)
                        || contains_arithmetic(&clause.result)
                })
                || else_result
                    .as_ref()
                    .map_or(false, |e| contains_arithmetic(e))
        }
        Expression::InList { expr, values, .. } => {
            contains_arithmetic(expr) || values.iter().any(contains_arithmetic)
        }
        Expression::Between {
            expr, low, high, ..
        } => contains_arithmetic(expr) || contains_arithmetic(low) || contains_arithmetic(high),
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            args.iter().any(contains_arithmetic)
        }
        _ => false,
    }
}

/// Check if all joins in the query are simple (equijoins with single tables)
///
/// Complex joins with many tables, non-equijoin conditions, or nested subqueries
/// are better suited for row-oriented execution.
///
/// Returns true if:
/// - No joins (single table), or
/// - All joins are simple equijoins with <= 3 tables
fn all_joins_are_simple(query: &SelectStmt) -> bool {
    match &query.from {
        None => true, // No FROM clause (e.g., SELECT 1)
        Some(from) => {
            // Count tables in FROM clause
            let table_count = count_tables(from);

            // Simple if <= 3 tables (allows TPC-H style queries with 2-3 joins)
            // Complex joins with many tables benefit less from columnar execution
            table_count <= 3
        }
    }
}

/// Count the number of tables in a FROM clause (including joins)
fn count_tables(from: &FromClause) -> usize {
    match from {
        FromClause::Table { .. } => 1,
        FromClause::Subquery { .. } => {
            // Treat subqueries as complex (conservative approach)
            // Could be optimized later to analyze subquery patterns
            4 // > 3, forces row-oriented execution
        }
        FromClause::Join { left, right, .. } => count_tables(left) + count_tables(right),
    }
}

/// Check if query has selective projection (few columns, not SELECT *)
///
/// Selective projections benefit more from columnar execution because:
/// - Only needed columns are loaded (projection pushdown)
/// - Less conversion overhead between row/columnar formats
///
/// Returns true if:
/// - Not SELECT * (wildcard)
/// - Projected columns <= 10 (arbitrary threshold)
fn has_selective_projection(query: &SelectStmt) -> bool {
    // Count non-wildcard items
    let non_wildcard_count = query
        .select_list
        .iter()
        .filter(|item| {
            !matches!(
                item,
                SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. }
            )
        })
        .count();

    // If there are any wildcards, projection is NOT selective
    let has_wildcard = query.select_list.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. }
        )
    });

    if has_wildcard {
        return false;
    }

    // Selective if <= 10 columns
    // This threshold allows for moderate projections like TPC-H queries
    // while avoiding columnar overhead for very wide projections
    non_wildcard_count > 0 && non_wildcard_count <= 10
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::{BinaryOperator, FromClause, JoinType, SelectItem, SelectStmt};
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
            }),
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
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
    fn test_columnar_for_aggregation() {
        // SELECT SUM(price * quantity) FROM orders
        // Phase 5: No GROUP BY (simple aggregation with arithmetic expression)
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
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "orders".to_string(),
                alias: None,
            }),
            where_clause: None,
            group_by: None, // Phase 5: No GROUP BY support yet
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        // Should use columnar (aggregation + arithmetic expression)
        // Phase 5: Single table, no GROUP BY, has arithmetic (price * quantity)
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
                        }),
                        right: Box::new(FromClause::Table {
                            name: "t2".to_string(),
                            alias: None,
                        }),
                        join_type: JoinType::Inner,
                        condition: None,
                        natural: false,
                    }),
                    right: Box::new(FromClause::Table {
                        name: "t3".to_string(),
                        alias: None,
                    }),
                    join_type: JoinType::Inner,
                    condition: None,
                    natural: false,
                }),
                right: Box::new(FromClause::Table {
                    name: "t4".to_string(),
                    alias: None,
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
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        assert!(has_aggregate_functions(&query_with_count));
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
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        assert!(has_arithmetic_expressions(&query_with_arithmetic));
    }

    #[test]
    fn test_selective_projection() {
        // SELECT id, name (2 columns)
        let selective = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![
                SelectItem::Expression {
                    expr: Expression::ColumnRef {
                        table: None,
                        column: "id".to_string(),
                    },
                    alias: None,
                },
                SelectItem::Expression {
                    expr: Expression::ColumnRef {
                        table: None,
                        column: "name".to_string(),
                    },
                    alias: None,
                },
            ],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "users".to_string(),
                alias: None,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        assert!(has_selective_projection(&selective));

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
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        assert!(!has_selective_projection(&non_selective));
    }
}
