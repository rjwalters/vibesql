//! DISTINCT tests
//!
//! Tests for SELECT DISTINCT functionality to remove duplicate rows.

use super::super::*;

#[test]
fn test_distinct_removes_duplicate_rows() {
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
                "category".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert multiple rows with duplicate categories
    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Electronics")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Books")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Electronics")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(4),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Books")),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT DISTINCT category FROM products
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: true,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "category".to_string(),
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "products".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should only return 2 unique categories
    assert_eq!(result.len(), 2);

    // Extract category values
    let categories: Vec<arcstr::ArcStr> = result
        .iter()
        .map(|row| match &row.values[0] {
            vibesql_types::SqlValue::Varchar(s) => s.clone(),
            _ => panic!("Expected varchar"),
        })
        .collect();

    // Both categories should be present
    assert!(categories.iter().any(|c| c.as_str() == "Electronics"));
    assert!(categories.iter().any(|c| c.as_str() == "Books"));
}

#[test]
fn test_distinct_with_multiple_columns() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "orders".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "customer_id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "status".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(20) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert rows with various combinations
    db.insert_row(
        "orders",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(100),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("pending")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "orders",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(100),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("shipped")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "orders",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(100),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("pending")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "orders",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(4),
            vibesql_types::SqlValue::Integer(200),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("pending")),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT DISTINCT customer_id, status FROM orders
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: true,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "customer_id".to_string(),
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "status".to_string(),
                },
                alias: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
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
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return 3 unique combinations: (100, pending), (100, shipped), (200, pending)
    assert_eq!(result.len(), 3);
}

#[test]
fn test_distinct_with_null_values() {
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
                "description".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true, // nullable
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert rows with NULL values
    db.insert_row(
        "items",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Item A")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "items",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Null,
        ]),
    )
    .unwrap();
    db.insert_row(
        "items",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Null,
        ]),
    )
    .unwrap();
    db.insert_row(
        "items",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(4),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Item A")),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT DISTINCT description FROM items
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: true,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "description".to_string(),
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "items".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return 2 unique values: "Item A" and NULL (NULLs are considered equal for DISTINCT)
    assert_eq!(result.len(), 2);
}

#[test]
fn test_distinct_false_preserves_duplicates() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "products".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "category".to_string(),
            vibesql_types::DataType::Varchar { max_length: Some(50) },
            false,
        )],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Electronics"))]),
    )
    .unwrap();
    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Electronics"))]),
    )
    .unwrap();
    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Books"))]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT category FROM products (without DISTINCT)
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
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return all 3 rows including duplicates
    assert_eq!(result.len(), 3);
}

#[test]
fn test_distinct_with_where_clause() {
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
                "role".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(20) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("admin")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("user")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("admin")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(4),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("admin")),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT DISTINCT role FROM users WHERE id > 1
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: true,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "role".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "id".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::GreaterThan,
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should filter first (id > 1), then apply DISTINCT
    // Remaining rows: id=2 (user), id=3 (admin), id=4 (admin)
    // After DISTINCT: user, admin
    assert_eq!(result.len(), 2);
}

#[test]
fn test_distinct_with_order_by() {
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
                "category".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Books")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Electronics")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Books")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(4),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Electronics")),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT DISTINCT category FROM products ORDER BY category
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: true,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "category".to_string(),
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "products".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: Some(vec![vibesql_ast::OrderByItem {
            expr: vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "category".to_string(),
            },
            direction: vibesql_ast::OrderDirection::Asc,
        }]),
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return 2 unique categories, sorted
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Books")));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Electronics")));
}
