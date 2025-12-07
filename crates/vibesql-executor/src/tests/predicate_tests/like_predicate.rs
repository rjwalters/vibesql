//! LIKE and NOT LIKE pattern matching tests
//!
//! Tests for SQL LIKE predicates covering:
//! - Wildcard patterns (% for any sequence, _ for single char)
//! - NOT LIKE negation
//! - NULL pattern and value handling
//! - Pattern matching edge cases

use super::super::super::*;

#[test]
fn test_like_wildcard_percent() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "name".to_string(),
            vibesql_types::DataType::Varchar { max_length: Some(50) },
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "test",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
    )
    .unwrap();
    db.insert_row(
        "test",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
    )
    .unwrap();
    db.insert_row(
        "test",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alex"))]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE name LIKE 'Al%'
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "test".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::Like {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "name".to_string(),
            }),
            pattern: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Al%")))),
            negated: false,
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
    assert_eq!(result.len(), 2); // Alice and Alex
}

#[test]
fn test_like_wildcard_underscore() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "name".to_string(),
            vibesql_types::DataType::Varchar { max_length: Some(50) },
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "test",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("cat"))]),
    )
    .unwrap();
    db.insert_row(
        "test",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("bat"))]),
    )
    .unwrap();
    db.insert_row(
        "test",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("cart"))]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE name LIKE '_at'
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "test".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::Like {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "name".to_string(),
            }),
            pattern: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("_at")))),
            negated: false,
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
    assert_eq!(result.len(), 2); // cat and bat (cart is 4 chars)
}

#[test]
fn test_not_like() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "name".to_string(),
            vibesql_types::DataType::Varchar { max_length: Some(50) },
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "test",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
    )
    .unwrap();
    db.insert_row(
        "test",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
    )
    .unwrap();
    db.insert_row(
        "test",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alex"))]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE name NOT LIKE 'Al%'
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "test".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::Like {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "name".to_string(),
            }),
            pattern: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Al%")))),
            negated: true,
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
    assert_eq!(result.len(), 1); // Only Bob
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")));
}

#[test]
fn test_like_null_pattern() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "name".to_string(),
            vibesql_types::DataType::Varchar { max_length: Some(50) },
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "test",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE name LIKE NULL
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "test".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::Like {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "name".to_string(),
            }),
            pattern: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
            negated: false,
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
    assert_eq!(result.len(), 0); // NULL pattern matches nothing
}

#[test]
fn test_like_null_value() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "name".to_string(),
            vibesql_types::DataType::Varchar { max_length: Some(50) },
            true,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Null])).unwrap();
    db.insert_row(
        "test",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE name LIKE 'Al%'
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "test".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::Like {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "name".to_string(),
            }),
            pattern: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Al%")))),
            negated: false,
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
    assert_eq!(result.len(), 1); // NULL value doesn't match, only Alice
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
}
