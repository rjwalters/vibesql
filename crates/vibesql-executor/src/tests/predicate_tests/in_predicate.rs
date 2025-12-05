//! IN and NOT IN predicate tests
//!
//! Tests for SQL IN/NOT IN predicates covering:
//! - Basic IN list matching
//! - NOT IN list exclusion
//! - NULL value handling (both in expression and list)
//! - Three-valued logic compliance (SQL:1999)

use super::super::super::*;

#[test]
fn test_in_list_basic() {
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
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(2)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(5)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val IN (1, 3, 5)
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
        where_clause: Some(vibesql_ast::Expression::InList {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "val".to_string(),
            }),
            values: vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5)),
            ],
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
    assert_eq!(result.len(), 2); // 1 and 5 match
}

#[test]
fn test_not_in_list() {
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
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(2)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(5)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val NOT IN (1, 3, 5)
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
        where_clause: Some(vibesql_ast::Expression::InList {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "val".to_string(),
            }),
            values: vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5)),
            ],
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
    assert_eq!(result.len(), 1); // Only 2 doesn't match
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(2));
}

#[test]
fn test_in_list_with_null_value() {
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
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Null])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val IN (1, 3, 5)
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
        where_clause: Some(vibesql_ast::Expression::InList {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "val".to_string(),
            }),
            values: vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5)),
            ],
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
    assert_eq!(result.len(), 1); // NULL doesn't match, only 1 matches
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
}

#[test]
fn test_in_list_with_null_in_list() {
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
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(2)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val IN (1, NULL, 5)
    // Should return no rows because 2 doesn't match 1 or 5, and NULL comparison is unknown
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
        where_clause: Some(vibesql_ast::Expression::InList {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "val".to_string(),
            }),
            values: vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5)),
            ],
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
    assert_eq!(result.len(), 0); // NULL in list causes unknown result for non-matching value
}

#[test]
fn test_empty_in_list() {
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
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(2)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val IN ()
    // Should return no rows (empty IN always returns false)
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
        where_clause: Some(vibesql_ast::Expression::InList {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "val".to_string(),
            }),
            values: vec![],
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
    assert_eq!(result.len(), 0); // Empty IN list always returns false
}

#[test]
fn test_empty_not_in_list() {
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
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(2)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val NOT IN ()
    // Should return all rows (empty NOT IN always returns true)
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
        where_clause: Some(vibesql_ast::Expression::InList {
            expr: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "val".to_string(),
            }),
            values: vec![],
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
    assert_eq!(result.len(), 2); // Empty NOT IN list always returns true
}

#[test]
fn test_null_in_empty_subquery() {
    let mut db = vibesql_storage::Database::new();

    // Create empty table
    let schema = vibesql_catalog::TableSchema::new(
        "empty_table".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "x".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT NULL IN (SELECT * FROM empty_table)
    // Expected: 0 (FALSE), per SQLite behavior
    // NULL IN empty set returns FALSE, not NULL
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::In {
                expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
                subquery: Box::new(vibesql_ast::SelectStmt {
                    with_clause: None,
                    set_operation: None,
                    distinct: false,
                    select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
                    from: Some(vibesql_ast::FromClause::Table {
                        name: "empty_table".to_string(),
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
                }),
                negated: false,
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Boolean(false));
}

#[test]
fn test_null_not_in_empty_subquery() {
    let mut db = vibesql_storage::Database::new();

    // Create empty table
    let schema = vibesql_catalog::TableSchema::new(
        "empty_table".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "x".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT NULL NOT IN (SELECT * FROM empty_table)
    // Expected: 1 (TRUE), per SQLite behavior
    // NULL NOT IN empty set returns TRUE, not NULL
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::In {
                expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
                subquery: Box::new(vibesql_ast::SelectStmt {
                    with_clause: None,
                    set_operation: None,
                    distinct: false,
                    select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
                    from: Some(vibesql_ast::FromClause::Table {
                        name: "empty_table".to_string(),
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
                }),
                negated: true,
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Boolean(true));
}

#[test]
fn test_value_in_empty_subquery() {
    let mut db = vibesql_storage::Database::new();

    // Create empty table
    let schema = vibesql_catalog::TableSchema::new(
        "empty_table".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "x".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT 1 IN (SELECT * FROM empty_table)
    // Expected: 0 (FALSE) - should already work, this is a regression test
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::In {
                expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                    1,
                ))),
                subquery: Box::new(vibesql_ast::SelectStmt {
                    with_clause: None,
                    set_operation: None,
                    distinct: false,
                    select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
                    from: Some(vibesql_ast::FromClause::Table {
                        name: "empty_table".to_string(),
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
                }),
                negated: false,
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Boolean(false));
}
