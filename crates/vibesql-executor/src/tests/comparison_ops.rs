//! Comparison operator tests
//!
//! Tests for comparison operators in expressions.

use super::super::*;

#[test]
fn test_greater_than_comparison() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "nums".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "val".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("nums", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(10)]))
        .unwrap();
    db.insert_row("nums", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(20)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "nums".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "val".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::GreaterThan,
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(15))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(20));
}

#[test]
fn test_less_than_comparison() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "nums".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "val".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("nums", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(10)]))
        .unwrap();
    db.insert_row("nums", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(20)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "nums".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "val".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::LessThan,
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(15))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(10));
}

#[test]
fn test_not_equal_comparison() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "nums".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "val".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("nums", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(10)]))
        .unwrap();
    db.insert_row("nums", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(20)]))
        .unwrap();
    db.insert_row("nums", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(30)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "nums".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "val".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::NotEqual,
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(20))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(10));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(30));
}

#[test]
fn test_less_than_or_equal_comparison() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "nums".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "val".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("nums", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(10)]))
        .unwrap();
    db.insert_row("nums", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(20)]))
        .unwrap();
    db.insert_row("nums", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(30)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "nums".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "val".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::LessThanOrEqual,
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(20))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(10));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(20));
}
