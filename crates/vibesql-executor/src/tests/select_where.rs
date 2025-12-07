//! WHERE clause tests
//!
//! Tests for SELECT with WHERE clause filtering.

use super::super::*;

#[test]
fn test_select_with_where() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "age".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(25),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(17),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(30),
        ]),
    )
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
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "age".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::GreaterThanOrEqual,
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(18))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(3));
}

#[test]
fn test_select_with_and_condition() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "products".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "price".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "stock".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(100),
            vibesql_types::SqlValue::Integer(5),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(200),
            vibesql_types::SqlValue::Integer(0),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(150),
            vibesql_types::SqlValue::Integer(10),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    // WHERE price > 50 AND stock > 0
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "products".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "price".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::GreaterThan,
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(50),
                )),
            }),
            op: vibesql_ast::BinaryOperator::And,
            right: Box::new(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "stock".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::GreaterThan,
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(0),
                )),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2); // Products 1 and 3
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(3));
}

#[test]
fn test_select_with_or_condition() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "items".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "category".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "items",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("electronics")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "items",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("food")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "items",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("books")),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    // WHERE category = 'electronics' OR category = 'books'
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "items".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "category".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("electronics")),
                )),
            }),
            op: vibesql_ast::BinaryOperator::Or,
            right: Box::new(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "category".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("books")),
                )),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2); // Items 1 and 3
}

#[test]
fn test_select_with_null_in_where() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "data".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "value".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "data",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "data",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Null,
        ]),
    )
    .unwrap();
    db.insert_row(
        "data",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    // WHERE value > 50 - should filter out NULL (NULL comparisons are unknown)
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "data".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "value".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::GreaterThan,
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(50))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    // NULL > 50 is unknown (not true), so row 2 is filtered out
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(3));
}
