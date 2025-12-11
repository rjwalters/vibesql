//! ROWID pseudo-column tests (SQLite compatibility)
//!
//! Tests for the ROWID, _rowid_, and oid pseudo-columns that return
//! the internal row identifier for SQLite compatibility.
//!
//! Issue #4247

use super::super::*;

/// Test SELECT rowid from a table
#[test]
fn test_select_rowid() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "a".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "b".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(10),
            vibesql_types::SqlValue::Integer(20),
        ]),
    )
    .unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(30),
            vibesql_types::SqlValue::Integer(40),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT rowid, a, b FROM t1
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
            values: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "rowid".to_string(),
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "a".to_string(),
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "b".to_string(),
                },
                alias: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t1".to_string(),
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
    assert_eq!(result.len(), 2);

    // First row should have rowid 0
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Bigint(0));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(10));
    assert_eq!(result[0].values[2], vibesql_types::SqlValue::Integer(20));

    // Second row should have rowid 1
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Bigint(1));
    assert_eq!(result[1].values[1], vibesql_types::SqlValue::Integer(30));
    assert_eq!(result[1].values[2], vibesql_types::SqlValue::Integer(40));
}

/// Test that _rowid_ alias works the same as rowid
#[test]
fn test_select_underscore_rowid() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "x".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(100)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT _rowid_ FROM t1
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
            values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "_rowid_".to_string(),
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t1".to_string(),
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
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Bigint(0));
}

/// Test that oid alias works the same as rowid
#[test]
fn test_select_oid() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "x".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(100)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT oid FROM t1
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
            values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "oid".to_string(),
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t1".to_string(),
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
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Bigint(0));
}

/// Test that ROWID is case-insensitive
#[test]
fn test_rowid_case_insensitive() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "x".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(100)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT ROWID (uppercase) FROM t1
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
            values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "ROWID".to_string(),
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t1".to_string(),
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
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Bigint(0));
}

/// Test that a real column named 'rowid' takes precedence over the pseudo-column
#[test]
fn test_real_rowid_column_takes_precedence() {
    let mut db = vibesql_storage::Database::new();

    // Create a table with an actual column named 'rowid'
    let schema = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "rowid".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "data".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert a row with rowid = 999 (different from physical index 0)
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(999),
            vibesql_types::SqlValue::Integer(42),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT rowid FROM t1 - should return 999 (the column value), not 0 (the row index)
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
            values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "rowid".to_string(),
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t1".to_string(),
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
    assert_eq!(result.len(), 1);
    // Should return the actual column value (999), not the physical row index (0)
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(999));
}

/// Test ROWID with table alias qualification
#[test]
fn test_rowid_with_table_alias() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "x".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(100)]),
    )
    .unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(200)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT t.rowid FROM t1 AS t
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
            values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef {
                table: Some("t".to_string()),
                column: "rowid".to_string(),
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t1".to_string(),
            alias: Some("t".to_string()),
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
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Bigint(0));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Bigint(1));
}
