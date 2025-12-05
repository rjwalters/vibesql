//! Tests for aggregate functions without FROM clause (issue #937)
//!
//! SQL standard: SELECT without FROM operates over a single implicit row.
//! This allows aggregate functions like COUNT(*) and MAX() to work correctly.

use super::super::*;

#[test]
fn test_max_constant_without_from() {
    // SELECT MAX(100) should return 100
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::AggregateFunction {
                name: "MAX".to_string(),
                distinct: false,
                args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(100))],
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get(0), Some(&vibesql_types::SqlValue::Integer(100)));
}

#[test]
fn test_count_star_without_from() {
    // SELECT COUNT(*) should return 1 (one implicit row)
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::AggregateFunction {
                name: "COUNT".to_string(),
                distinct: false,
                args: vec![vibesql_ast::Expression::Wildcard],
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get(0), Some(&vibesql_types::SqlValue::Integer(1)));
}

#[test]
fn test_aggregate_in_expression_without_from() {
    // SELECT MAX(5) + 10 should return 15
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::AggregateFunction {
                    name: "MAX".to_string(),
                    distinct: false,
                    args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                        5,
                    ))],
                }),
                op: vibesql_ast::BinaryOperator::Plus,
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(10),
                )),
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get(0), Some(&vibesql_types::SqlValue::Integer(15)));
}

#[test]
fn test_count_distinct_without_from() {
    // SELECT COUNT(DISTINCT 65) should return 1
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::AggregateFunction {
                name: "COUNT".to_string(),
                distinct: true,
                args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(65))],
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get(0), Some(&vibesql_types::SqlValue::Integer(1)));
}

#[test]
fn test_multiple_aggregates_without_from() {
    // SELECT MAX(5), MIN(10), COUNT(*) should return 5, 10, 1
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::AggregateFunction {
                    name: "MAX".to_string(),
                    distinct: false,
                    args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                        5,
                    ))],
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::AggregateFunction {
                    name: "MIN".to_string(),
                    distinct: false,
                    args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                        10,
                    ))],
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![vibesql_ast::Expression::Wildcard],
                },
                alias: None,
            },
        ],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get(0), Some(&vibesql_types::SqlValue::Integer(5)));
    assert_eq!(result[0].get(1), Some(&vibesql_types::SqlValue::Integer(10)));
    assert_eq!(result[0].get(2), Some(&vibesql_types::SqlValue::Integer(1)));
}

#[test]
fn test_sum_avg_without_from() {
    // SELECT SUM(100), AVG(50) should return 100, 50
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::AggregateFunction {
                    name: "SUM".to_string(),
                    distinct: false,
                    args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                        100,
                    ))],
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::AggregateFunction {
                    name: "AVG".to_string(),
                    distinct: false,
                    args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                        50,
                    ))],
                },
                alias: None,
            },
        ],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get(0), Some(&vibesql_types::SqlValue::Integer(100)));
    assert_eq!(result[0].get(1), Some(&vibesql_types::SqlValue::Numeric(50.0)));
}
