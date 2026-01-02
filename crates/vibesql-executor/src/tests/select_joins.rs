//! JOIN tests
//!
//! Tests for INNER JOIN operations.

use super::super::*;

#[test]
fn test_inner_join_two_tables() {
    let mut db = vibesql_storage::Database::new();

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
    db.insert_row(
        "orders",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(50),
        ]),
    )
    .unwrap();
    db.insert_row(
        "orders",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(75),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "users".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "orders".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Inner,
            condition: Some(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef(
                    vibesql_ast::ColumnIdentifier::qualified("users", false, "id", false),
                )),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::ColumnRef(
                    vibesql_ast::ColumnIdentifier::qualified("orders", false, "user_id", false),
                )),
            }),
            using_columns: None,
            natural: false, alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values.len(), 5); // users (2 cols) + orders (3 cols)
}

#[test]
fn test_right_outer_join() {
    let mut db = vibesql_storage::Database::new();

    // Create users table with 2 users
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

    // Create orders table with order for user 2 and user 999 (no matching user)
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
    db.insert_row(
        "orders",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(75),
        ]),
    )
    .unwrap();
    db.insert_row(
        "orders",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(999), // No matching user
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();

    // RIGHT OUTER JOIN should include all orders, with NULLs for missing users
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "users".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "orders".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::RightOuter,
            condition: Some(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef(
                    vibesql_ast::ColumnIdentifier::qualified("users", false, "id", false),
                )),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::ColumnRef(
                    vibesql_ast::ColumnIdentifier::qualified("orders", false, "user_id", false),
                )),
            }),
            using_columns: None,
            natural: false, alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2); // Both orders should appear

    // One row should have NULLs for user columns (order for user 999)
    let null_count =
        result.iter().filter(|row| row.values[0] == vibesql_types::SqlValue::Null).count();
    assert_eq!(null_count, 1, "Should have one row with NULL user");
}

#[test]
fn test_full_outer_join() {
    let mut db = vibesql_storage::Database::new();

    // Create users table with users 1, 2
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

    // Create orders: one for user 2, one for user 999 (no match)
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
    db.insert_row(
        "orders",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(75),
        ]),
    )
    .unwrap();
    db.insert_row(
        "orders",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(999),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();

    // FULL OUTER JOIN should include:
    // - User 1 with NULL order
    // - User 2 with order 1
    // - Order 2 with NULL user
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "users".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "orders".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::FullOuter,
            condition: Some(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef(
                    vibesql_ast::ColumnIdentifier::qualified("users", false, "id", false),
                )),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::ColumnRef(
                    vibesql_ast::ColumnIdentifier::qualified("orders", false, "user_id", false),
                )),
            }),
            using_columns: None,
            natural: false, alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 3); // Alice (no order), Bob+order, order (no user)

    // Count rows with NULLs in user columns (order with no user)
    let null_user_count =
        result.iter().filter(|row| row.values[0] == vibesql_types::SqlValue::Null).count();
    assert_eq!(null_user_count, 1, "Should have one unmatched order");

    // Count rows with NULLs in order columns (user with no order)
    let null_order_count =
        result.iter().filter(|row| row.values[2] == vibesql_types::SqlValue::Null).count();
    assert_eq!(null_order_count, 1, "Should have one unmatched user");
}

#[test]
fn test_cross_join() {
    let mut db = vibesql_storage::Database::new();

    // Create users table with 2 users
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

    // Create products table with 3 products
    let products_schema = vibesql_catalog::TableSchema::new(
        "products".to_string(),
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
    db.create_table(products_schema).unwrap();
    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Widget")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Gadget")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Doohickey")),
        ]),
    )
    .unwrap();

    // CROSS JOIN should produce cartesian product: 2 * 3 = 6 rows
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "users".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "products".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Cross,
            condition: None, // CROSS JOIN has no condition
            using_columns: None,
            natural: false, alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 6); // 2 users * 3 products = 6 combinations
    assert_eq!(result[0].values.len(), 4); // users (2 cols) + products (2 cols)
}

/// Test that CROSS JOIN with ON condition works like INNER JOIN (SQLite compatibility)
///
/// SQLite accepts CROSS JOIN with ON clause and treats it semantically as an INNER JOIN.
/// The ON condition filters the Cartesian product, producing the same results as INNER JOIN.
#[test]
fn test_cross_join_with_condition_works_like_inner_join() {
    let mut db = vibesql_storage::Database::new();

    let users_schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "id".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(users_schema).unwrap();
    db.insert_row("users", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("users", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(2)]))
        .unwrap();

    let products_schema = vibesql_catalog::TableSchema::new(
        "products".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "id".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(products_schema).unwrap();
    db.insert_row("products", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("products", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(3)]))
        .unwrap();

    // CROSS JOIN with ON condition should work like INNER JOIN (SQLite compatibility)
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "users".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "products".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Cross,
            condition: Some(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef(
                    vibesql_ast::ColumnIdentifier::qualified("users", false, "id", false),
                )),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::ColumnRef(
                    vibesql_ast::ColumnIdentifier::qualified("products", false, "id", false),
                )),
            }),
            using_columns: None,
            natural: false, alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    // Only 1 matching row (users.id=1 matches products.id=1)
    assert_eq!(result.len(), 1);
    // Result has 2 columns (users.id, products.id)
    assert_eq!(result[0].values.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(1));
}

/// Test that NULL values in join columns do NOT match each other (issue #1877)
/// According to SQL semantics, NULL = NULL evaluates to NULL (not TRUE),
/// so rows with NULL in join columns should be excluded from INNER JOIN results.
#[test]
fn test_inner_join_null_values_dont_match() {
    let mut db = vibesql_storage::Database::new();

    // Create t1 with one matching row and one NULL
    let t1_schema = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "x".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(t1_schema).unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("one")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Null,
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("null-left")),
        ]),
    )
    .unwrap();

    // Create t2 with one matching row and one NULL
    let t2_schema = vibesql_catalog::TableSchema::new(
        "t2".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "y".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
            vibesql_catalog::ColumnSchema::new(
                "value".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table(t2_schema).unwrap();
    db.insert_row(
        "t2",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("matched")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "t2",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Null,
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("null-right")),
        ]),
    )
    .unwrap();

    // INNER JOIN t1.x = t2.y should only match on x=1, y=1
    // NULL values should NOT match (NULL = NULL is NULL, not TRUE)
    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                    "t1", false, "name", false,
                )),
                alias: None,
                source_text: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                    "t2", false, "value", false,
                )),
                alias: None,
                source_text: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Join {
            left: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "t1".to_string(),
                alias: None,
                column_aliases: None,
            }),
            right: Box::new(vibesql_ast::FromClause::Table {
                quoted: false,
                name: "t2".to_string(),
                alias: None,
                column_aliases: None,
            }),
            join_type: vibesql_ast::JoinType::Inner,
            condition: Some(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef(
                    vibesql_ast::ColumnIdentifier::qualified("t1", false, "x", false),
                )),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::ColumnRef(
                    vibesql_ast::ColumnIdentifier::qualified("t2", false, "y", false),
                )),
            }),
            using_columns: None,
            natural: false, alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should only have ONE row: the match between (1, "one") and (1, "matched")
    // NULL rows should NOT match each other
    assert_eq!(result.len(), 1, "INNER JOIN should only match non-NULL values");
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("one")));
    assert_eq!(
        result[0].values[1],
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("matched"))
    );
}
