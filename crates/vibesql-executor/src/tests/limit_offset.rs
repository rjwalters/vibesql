//! LIMIT/OFFSET pagination tests
//!
//! Tests for LIMIT and OFFSET clause handling in SELECT statements.

use super::super::*;
fn make_pagination_stmt(limit: Option<usize>, offset: Option<usize>) -> vibesql_ast::SelectStmt {
    vibesql_ast::SelectStmt {
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
        order_by: None,
        limit,
        offset,
    }
}

fn make_users_table() -> vibesql_storage::Database {
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
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Carol")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(4),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Dave")),
        ]),
    )
    .unwrap();
    db
}

#[test]
fn test_limit_basic() {
    let db = make_users_table();
    let executor = SelectExecutor::new(&db);
    let stmt = make_pagination_stmt(Some(2), None);

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(2));
}

#[test]
fn test_offset_basic() {
    let db = make_users_table();
    let executor = SelectExecutor::new(&db);
    let stmt = make_pagination_stmt(None, Some(2));

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(3));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(4));
}

#[test]
fn test_limit_and_offset() {
    let db = make_users_table();
    let executor = SelectExecutor::new(&db);
    let stmt = make_pagination_stmt(Some(2), Some(1));

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(3));
}

#[test]
fn test_offset_beyond_result_set() {
    let db = make_users_table();
    let executor = SelectExecutor::new(&db);
    let stmt = make_pagination_stmt(None, Some(10));

    let result = executor.execute(&stmt).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_limit_greater_than_result_set() {
    let db = make_users_table();
    let executor = SelectExecutor::new(&db);
    let stmt = make_pagination_stmt(Some(10), None);

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 4);
}

/// Test LIMIT/OFFSET with INNER JOIN (#3776)
///
/// This tests that the columnar join execution path correctly applies
/// LIMIT and OFFSET to join results.
#[test]
fn test_limit_with_inner_join() {
    let mut db = vibesql_storage::Database::new();

    // Create users table
    let users_schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(users_schema).unwrap();
    for i in 1..=4 {
        db.insert_row(
            "users",
            vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(i),
                vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(format!("User{}", i))),
            ]),
        )
        .unwrap();
    }

    // Create orders table - each user has 2 orders
    let orders_schema = vibesql_catalog::TableSchema::new(
        "orders".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "user_id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "amount".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(orders_schema).unwrap();
    let mut order_id = 1;
    for user_id in 1..=4 {
        for _ in 0..2 {
            db.insert_row(
                "orders",
                vibesql_storage::Row::new(vec![
                    vibesql_types::SqlValue::Integer(order_id),
                    vibesql_types::SqlValue::Integer(user_id),
                    vibesql_types::SqlValue::Integer(order_id * 10),
                ]),
            )
            .unwrap();
            order_id += 1;
        }
    }

    // INNER JOIN produces 8 rows (4 users * 2 orders each)
    // Test with LIMIT 3
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                name: "users".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                name: "orders".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Inner,
            condition: Some(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("users".to_string()),
                    column: "id".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("orders".to_string()),
                    column: "user_id".to_string(),
                }),
            }),
            natural: false,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: Some(3),
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 3, "LIMIT 3 should return exactly 3 rows");
}

/// Test OFFSET with INNER JOIN (#3776)
#[test]
fn test_offset_with_inner_join() {
    let mut db = vibesql_storage::Database::new();

    // Create users table
    let users_schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(users_schema).unwrap();
    for i in 1..=4 {
        db.insert_row(
            "users",
            vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(i),
                vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(format!("User{}", i))),
            ]),
        )
        .unwrap();
    }

    // Create orders table - each user has 2 orders (8 total)
    let orders_schema = vibesql_catalog::TableSchema::new(
        "orders".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "user_id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(orders_schema).unwrap();
    let mut order_id = 1;
    for user_id in 1..=4 {
        for _ in 0..2 {
            db.insert_row(
                "orders",
                vibesql_storage::Row::new(vec![
                    vibesql_types::SqlValue::Integer(order_id),
                    vibesql_types::SqlValue::Integer(user_id),
                ]),
            )
            .unwrap();
            order_id += 1;
        }
    }

    // INNER JOIN produces 8 rows, OFFSET 5 should return 3 rows
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                name: "users".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                name: "orders".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Inner,
            condition: Some(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("users".to_string()),
                    column: "id".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("orders".to_string()),
                    column: "user_id".to_string(),
                }),
            }),
            natural: false,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: Some(5),
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 3, "OFFSET 5 from 8 rows should return 3 rows");
}

/// Test LIMIT and OFFSET together with INNER JOIN (#3776)
#[test]
fn test_limit_offset_with_inner_join() {
    let mut db = vibesql_storage::Database::new();

    // Create users table
    let users_schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(users_schema).unwrap();
    for i in 1..=5 {
        db.insert_row(
            "users",
            vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(i),
                vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(format!("User{}", i))),
            ]),
        )
        .unwrap();
    }

    // Create orders table - each user has 2 orders (10 total)
    let orders_schema = vibesql_catalog::TableSchema::new(
        "orders".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "user_id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(orders_schema).unwrap();
    let mut order_id = 1;
    for user_id in 1..=5 {
        for _ in 0..2 {
            db.insert_row(
                "orders",
                vibesql_storage::Row::new(vec![
                    vibesql_types::SqlValue::Integer(order_id),
                    vibesql_types::SqlValue::Integer(user_id),
                ]),
            )
            .unwrap();
            order_id += 1;
        }
    }

    // INNER JOIN produces 10 rows, LIMIT 3 OFFSET 2 should return rows 3-5
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                name: "users".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                name: "orders".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Inner,
            condition: Some(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("users".to_string()),
                    column: "id".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("orders".to_string()),
                    column: "user_id".to_string(),
                }),
            }),
            natural: false,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: Some(3),
        offset: Some(2),
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 3, "LIMIT 3 OFFSET 2 should return exactly 3 rows");
}
