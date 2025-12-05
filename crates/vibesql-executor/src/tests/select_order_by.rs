//! Basic SELECT ORDER BY tests
//!
//! Tests for ORDER BY functionality including single and multiple column ordering.

use super::super::*;

/// Test ORDER BY single column ascending
#[test]
fn test_order_by_single_column_asc() {
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
            vibesql_types::SqlValue::Integer(30),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(20),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(25),
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
        where_clause: None,
        group_by: None,
        having: None,
        order_by: Some(vec![vibesql_ast::OrderByItem {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "age".to_string() },
            direction: vibesql_ast::OrderDirection::Asc,
        }]),
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(20));
    assert_eq!(result[1].values[1], vibesql_types::SqlValue::Integer(25));
    assert_eq!(result[2].values[1], vibesql_types::SqlValue::Integer(30));
}

/// Test ORDER BY multiple columns with different directions
#[test]
fn test_order_by_multiple_columns() {
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
                "dept".to_string(),
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
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(35),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(30),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(20),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(4),
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(25),
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
        where_clause: None,
        group_by: None,
        having: None,
        order_by: Some(vec![
            vibesql_ast::OrderByItem {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "dept".to_string(),
                },
                direction: vibesql_ast::OrderDirection::Asc,
            },
            vibesql_ast::OrderByItem {
                expr: vibesql_ast::Expression::ColumnRef { table: None, column: "age".to_string() },
                direction: vibesql_ast::OrderDirection::Desc,
            },
        ]),
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 4);
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[0].values[2], vibesql_types::SqlValue::Integer(30));
    assert_eq!(result[1].values[1], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[1].values[2], vibesql_types::SqlValue::Integer(25));
    assert_eq!(result[2].values[1], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[2].values[2], vibesql_types::SqlValue::Integer(35));
    assert_eq!(result[3].values[1], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[3].values[2], vibesql_types::SqlValue::Integer(20));
}
