//! VALUES clause tests
//!
//! Tests for VALUES table constructor functionality in FROM clauses.

use super::super::*;

/// Test basic VALUES with single row
#[test]
fn test_values_single_row() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Values {
            rows: vec![vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
            ]],
            alias: "t".to_string(),
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_ok(), "VALUES query failed: {:?}", result.err());

    let result = result.unwrap();
    assert_eq!(result.len(), 1, "Expected 1 row");
    assert_eq!(result[0].values.len(), 3, "Expected 3 columns");
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[0].values[2], vibesql_types::SqlValue::Integer(3));
}

/// Test VALUES with multiple rows
#[test]
fn test_values_multiple_rows() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Values {
            rows: vec![
                vec![
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                        arcstr::ArcStr::from("a"),
                    )),
                ],
                vec![
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                        arcstr::ArcStr::from("b"),
                    )),
                ],
                vec![
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                        arcstr::ArcStr::from("c"),
                    )),
                ],
            ],
            alias: "t".to_string(),
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_ok(), "VALUES query failed: {:?}", result.err());

    let result = result.unwrap();
    assert_eq!(result.len(), 3, "Expected 3 rows");

    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(
        result[0].values[1],
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("a"))
    );

    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(2));
    assert_eq!(
        result[1].values[1],
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("b"))
    );

    assert_eq!(result[2].values[0], vibesql_types::SqlValue::Integer(3));
    assert_eq!(
        result[2].values[1],
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("c"))
    );
}

/// Test VALUES with column aliases
#[test]
fn test_values_with_column_aliases() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Values {
            rows: vec![vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
            ]],
            alias: "t".to_string(),
            column_aliases: Some(vec!["x".to_string(), "y".to_string()]),
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_ok(), "VALUES query failed: {:?}", result.err());

    let result = result.unwrap();
    assert_eq!(result.len(), 1, "Expected 1 row");
    // Just verify the query executed successfully - column names are in schema
    assert_eq!(result[0].values.len(), 2, "Expected 2 columns");
}

/// Test VALUES with NULL values
#[test]
fn test_values_with_nulls() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Values {
            rows: vec![
                vec![
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
                ],
                vec![
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
                ],
            ],
            alias: "t".to_string(),
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_ok(), "VALUES query failed: {:?}", result.err());

    let result = result.unwrap();
    assert_eq!(result.len(), 2, "Expected 2 rows");
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Null);
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Null);
}

/// Test VALUES with negative numbers (unary minus)
#[test]
fn test_values_with_negative_numbers() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Values {
            rows: vec![vec![
                vibesql_ast::Expression::UnaryOp {
                    op: vibesql_ast::UnaryOperator::Minus,
                    expr: Box::new(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Integer(42),
                    )),
                },
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(100)),
            ]],
            alias: "t".to_string(),
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_ok(), "VALUES query failed: {:?}", result.err());

    let result = result.unwrap();
    assert_eq!(result.len(), 1, "Expected 1 row");
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(-42));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(100));
}
