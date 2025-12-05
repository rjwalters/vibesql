//! BETWEEN and NOT BETWEEN range tests
//!
//! Tests for SQL BETWEEN predicates covering:
//! - Basic range inclusion (inclusive boundaries)
//! - NOT BETWEEN range exclusion
//! - NULL expression handling
//! - Boundary value edge cases

use super::super::super::*;

#[test]
fn test_between_with_null_expr() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "val".to_string(),
            vibesql_types::DataType::Integer,
            true,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Null])).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(5)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val BETWEEN 1 AND 10
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
        where_clause: Some(vibesql_ast::Expression::Between {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "val".to_string(),
            }),
            low: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1))),
            high: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10))),
            negated: false,
            symmetric: false,
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
    assert_eq!(result.len(), 1); // NULL doesn't match, only 5
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(5));
}

#[test]
fn test_not_between() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "val".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(5)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(15)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(25)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val NOT BETWEEN 10 AND 20
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
        where_clause: Some(vibesql_ast::Expression::Between {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "val".to_string(),
            }),
            low: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10))),
            high: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(20))),
            negated: true,
            symmetric: false,
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
    assert_eq!(result.len(), 2); // 5 and 25
}

#[test]
fn test_between_boundary_values() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "val".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(10)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(15)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(20)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val BETWEEN 10 AND 20
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
        where_clause: Some(vibesql_ast::Expression::Between {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "val".to_string(),
            }),
            low: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10))),
            high: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(20))),
            negated: false,
            symmetric: false,
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
    assert_eq!(result.len(), 3); // All values including boundaries
}

#[test]
fn test_not_between_with_null_bound() {
    // Tests NOT BETWEEN with NULL upper bound (SQL:1999 standard behavior)
    // SQL standard: val NOT BETWEEN low AND NULL → NULL, WHERE NULL excludes all rows
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "val".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(5)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(15)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val NOT BETWEEN 10 AND NULL
    // Three-valued logic: (val < 10) OR (val > NULL)
    // For val=5: (5 < 10) OR (5 > NULL) = TRUE OR NULL = TRUE (included)
    // For val=15: (15 < 10) OR (15 > NULL) = FALSE OR NULL = NULL (filtered)
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
        where_clause: Some(vibesql_ast::Expression::Between {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "val".to_string(),
            }),
            low: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10))),
            high: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
            negated: true,
            symmetric: false,
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
    assert_eq!(result.len(), 1); // Three-valued logic: 5 < 10 = TRUE, TRUE OR NULL = TRUE
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(5));
}

#[test]
fn test_between_with_null_bound() {
    // Tests that BETWEEN with NULL bound returns NULL (SQL:1999 standard behavior)
    // WHERE NULL is treated as false, so it excludes all rows
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "val".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(5)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(15)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val BETWEEN NULL AND 20
    // SQL standard: BETWEEN returns NULL, WHERE NULL excludes all rows (0 rows)
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
        where_clause: Some(vibesql_ast::Expression::Between {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "val".to_string(),
            }),
            low: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
            high: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(20))),
            negated: false,
            symmetric: false,
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
    assert_eq!(result.len(), 0); // SQL standard: NULL (excludes all rows)
}

#[test]
fn test_not_between_with_null_lower_bound() {
    // Tests NOT BETWEEN with NULL lower bound (SQL:1999 standard behavior)
    // SQL standard: val NOT BETWEEN NULL AND high → NULL, WHERE NULL excludes all rows
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "val".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(5)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(15)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(25)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val NOT BETWEEN NULL AND 20
    // Three-valued logic: (val < NULL) OR (val > 20)
    // For val=25: (25 < NULL) OR (25 > 20) = NULL OR TRUE = TRUE (included)
    // For val=5,15: (val < NULL) OR (val > 20) = NULL OR FALSE = NULL (filtered)
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
        where_clause: Some(vibesql_ast::Expression::Between {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "val".to_string(),
            }),
            low: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
            high: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(20))),
            negated: true,
            symmetric: false,
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
    assert_eq!(result.len(), 1); // Three-valued logic: 25 > 20 = TRUE, NULL OR TRUE = TRUE
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(25));
}

#[test]
fn test_not_negative_literal_between_null_bounds() {
    // Tests issue #1840: NOT -78 BETWEEN NULL AND 25
    // This is a regression test for the specific query from index/random/10/slt_good_0.test
    // According to SQLite behavior:
    // - Since low bound is NULL and negated=true, should return: -78 > 25 = FALSE    // - So this should filter ALL rows (return 0 rows)
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "tab0".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "col0".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("tab0", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(97)]))
        .unwrap();
    db.insert_row("tab0", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(75)]))
        .unwrap();
    db.insert_row("tab0", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(61)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM tab0 WHERE NOT - 78 BETWEEN NULL AND ( 25 )
    // SQLite behavior: returns 0 rows
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "tab0".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::Between {
            expr: Box::new(vibesql_ast::Expression::UnaryOp {
                op: vibesql_ast::UnaryOperator::Minus,
                expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                    78,
                ))),
            }),
            low: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
            high: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(25))),
            negated: true,
            symmetric: false,
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
    assert_eq!(result.len(), 0); // Should return 0 rows
}
