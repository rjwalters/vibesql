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
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("rowid", false)),
                alias: None, source_text: None },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("a", false)),
                alias: None, source_text: None },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("b", false)),
                alias: None, source_text: None },
        ],
        from: Some(vibesql_ast::FromClause::Table { quoted: false,
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

    // First row should have rowid 1 (SQLite ROWIDs are 1-indexed)
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Bigint(1));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(10));
    assert_eq!(result[0].values[2], vibesql_types::SqlValue::Integer(20));

    // Second row should have rowid 2
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Bigint(2));
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
            expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("_rowid_", false)),
            alias: None,
            source_text: None,
        }],
        from: Some(vibesql_ast::FromClause::Table { quoted: false,
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
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Bigint(1));
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
            expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("oid", false)),
            alias: None,
            source_text: None,
        }],
        from: Some(vibesql_ast::FromClause::Table { quoted: false,
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
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Bigint(1));
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
            expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("ROWID", false)),
            alias: None,
            source_text: None,
        }],
        from: Some(vibesql_ast::FromClause::Table { quoted: false,
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
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Bigint(1));
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
            expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("rowid", false)),
            alias: None,
            source_text: None,
        }],
        from: Some(vibesql_ast::FromClause::Table { quoted: false,
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
            expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified("t", false, "rowid", false)),
            alias: None,
            source_text: None,
        }],
        from: Some(vibesql_ast::FromClause::Table { quoted: false,
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
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Bigint(1));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Bigint(2));
}

/// Test that explicit row_id is preserved when using Row::with_row_id()
#[test]
fn test_explicit_rowid_preserved() {
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

    // Insert with explicit rowid = 5
    db.insert_row(
        "t1",
        vibesql_storage::Row::with_row_id(
            vec![vibesql_types::SqlValue::Integer(100)],
            5,
        ),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT rowid, x FROM t1
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("rowid", false)),
                alias: None,
                source_text: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("x", false)),
                alias: None,
                source_text: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            quoted: false,
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
    // Rowid should be 5 (the explicit value), not 1 (physical index)
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Bigint(5));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(100));
}

/// Test that mixed explicit and auto-assigned rowids work correctly
#[test]
fn test_mixed_explicit_and_auto_rowid() {
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

    // Insert with explicit rowid = 10
    db.insert_row(
        "t1",
        vibesql_storage::Row::with_row_id(
            vec![vibesql_types::SqlValue::Integer(100)],
            10,
        ),
    )
    .unwrap();

    // Insert without explicit rowid (should get auto-assigned based on physical index)
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(200)]),
    )
    .unwrap();

    // Insert with explicit rowid = 20
    db.insert_row(
        "t1",
        vibesql_storage::Row::with_row_id(
            vec![vibesql_types::SqlValue::Integer(300)],
            20,
        ),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT rowid, x FROM t1
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("rowid", false)),
                alias: None,
                source_text: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("x", false)),
                alias: None,
                source_text: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            quoted: false,
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
    assert_eq!(result.len(), 3);
    // First row: explicit rowid 10
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Bigint(10));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(100));
    // Second row: auto-assigned rowid 2 (physical index 1 + 1)
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Bigint(2));
    assert_eq!(result[1].values[1], vibesql_types::SqlValue::Integer(200));
    // Third row: explicit rowid 20
    assert_eq!(result[2].values[0], vibesql_types::SqlValue::Bigint(20));
    assert_eq!(result[2].values[1], vibesql_types::SqlValue::Integer(300));
}

/// Test ORDER BY rowid (issue #4573)
#[test]
fn test_order_by_rowid() {
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

    // Insert rows with explicit rowids in non-sequential order
    db.insert_row(
        "t1",
        vibesql_storage::Row::with_row_id(vec![vibesql_types::SqlValue::Integer(30)], 3),
    )
    .unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::with_row_id(vec![vibesql_types::SqlValue::Integer(10)], 1),
    )
    .unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::with_row_id(vec![vibesql_types::SqlValue::Integer(20)], 2),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT x FROM t1 ORDER BY rowid ASC
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "x", false,
            )),
            alias: None,
            source_text: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            quoted: false,
            name: "t1".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: Some(vec![vibesql_ast::OrderByItem {
            expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "rowid", false,
            )),
            direction: vibesql_ast::OrderDirection::Asc,
            nulls_order: None,
        }]),
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 3);
    // Should be sorted by rowid: 1, 2, 3 -> x values: 10, 20, 30
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(10));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(20));
    assert_eq!(result[2].values[0], vibesql_types::SqlValue::Integer(30));
}

/// Test ORDER BY rowid DESC (issue #4573)
#[test]
fn test_order_by_rowid_desc() {
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

    // Insert rows
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(10)]),
    )
    .unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(20)]),
    )
    .unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(30)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT x FROM t1 ORDER BY _rowid_ DESC
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "x", false,
            )),
            alias: None,
            source_text: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            quoted: false,
            name: "t1".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: Some(vec![vibesql_ast::OrderByItem {
            expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "_rowid_", false,
            )),
            direction: vibesql_ast::OrderDirection::Desc,
            nulls_order: None,
        }]),
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 3);
    // Should be sorted by rowid DESC: 3, 2, 1 -> x values: 30, 20, 10
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(30));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(20));
    assert_eq!(result[2].values[0], vibesql_types::SqlValue::Integer(10));
}
