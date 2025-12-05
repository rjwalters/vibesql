//! Tests for NOT operator in WHERE clauses (issue #2010)
//!
//! This module tests the NOT unary operator in various contexts:
//! - SELECT WHERE with NOT
//! - DELETE WHERE with NOT
//! - NOT with complex expressions

use super::super::*;

#[test]
fn test_not_in_select_where() {
    // Test: SELECT pk FROM tab0 WHERE NOT (col0 < 542)
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "tab0".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "pk".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "col0".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "tab0",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "tab0",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(542),
        ]),
    )
    .unwrap();
    db.insert_row(
        "tab0",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(600),
        ]),
    )
    .unwrap();

    let executor = select::SelectExecutor::new(&db);

    // SELECT pk FROM tab0 WHERE NOT (col0 < 542)
    // Should return rows where col0 >= 542 (pk=2 and pk=3)
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "pk".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "tab0".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::UnaryOp {
            op: vibesql_ast::UnaryOperator::Not,
            expr: Box::new(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "col0".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::LessThan,
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(542),
                )),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2); // Should return 2 rows (pk=2 and pk=3)
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(3));
}

#[test]
fn test_not_with_equality() {
    // Test: SELECT pk FROM tab0 WHERE NOT col0 = 100
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "tab0".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "pk".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "col0".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "tab0",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "tab0",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();

    let executor = select::SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "pk".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "tab0".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::UnaryOp {
            op: vibesql_ast::UnaryOperator::Not,
            expr: Box::new(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "col0".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(100),
                )),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1); // Should return 1 row (pk=2)
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(2));
}

#[test]
fn test_not_in_delete_where() {
    // Test: DELETE FROM tab0 WHERE NOT (col0 < 542)
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "tab0".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "pk".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "col0".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "tab0",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "tab0",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(542),
        ]),
    )
    .unwrap();
    db.insert_row(
        "tab0",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(600),
        ]),
    )
    .unwrap();

    // DELETE FROM tab0 WHERE NOT (col0 < 542)
    let stmt = vibesql_ast::DeleteStmt {
        only: false,
        table_name: "tab0".to_string(),
        where_clause: Some(vibesql_ast::WhereClause::Condition(vibesql_ast::Expression::UnaryOp {
            op: vibesql_ast::UnaryOperator::Not,
            expr: Box::new(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "col0".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::LessThan,
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(542),
                )),
            }),
        })),
    };

    let deleted_count = delete::DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted_count, 2); // Should delete 2 rows (pk=2 and pk=3)

    // Verify only row with pk=1 remains by selecting all rows
    let executor = select::SelectExecutor::new(&db);
    let verify_stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "tab0".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };
    let remaining_rows = executor.execute(&verify_stmt).unwrap();
    assert_eq!(remaining_rows.len(), 1);
    assert_eq!(remaining_rows[0].values[0], vibesql_types::SqlValue::Integer(1)); // pk=1
    assert_eq!(remaining_rows[0].values[1], vibesql_types::SqlValue::Integer(100));
    // col0=100
}

#[test]
fn test_not_with_null() {
    // Test NOT with NULL value (should return NULL per SQL three-valued logic)
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "tab0".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "pk".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "col0".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ), // nullable
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "tab0",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Null,
        ]),
    )
    .unwrap();
    db.insert_row(
        "tab0",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();

    let executor = select::SelectExecutor::new(&db);

    // SELECT pk FROM tab0 WHERE NOT (col0 < 542)
    // Row with NULL should be filtered out (NULL propagates)
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "pk".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "tab0".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::UnaryOp {
            op: vibesql_ast::UnaryOperator::Not,
            expr: Box::new(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "col0".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::LessThan,
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(542),
                )),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 0); // NOT (NULL < 542) = NOT NULL = NULL, which filters out the row
}
