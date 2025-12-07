//! MIN and MAX aggregate function tests
//!
//! Tests for MIN/MAX functions on integers and strings.

use super::super::*;

#[test]
fn test_min_function() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "temps".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "temp".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "temps",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(72),
        ]),
    )
    .unwrap();
    db.insert_row(
        "temps",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(65),
        ]),
    )
    .unwrap();
    db.insert_row(
        "temps",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(78),
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
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Function {
                name: "MIN".to_string(),
                args: vec![vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "temp".to_string(),
                }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "temps".to_string(),
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
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(65));
}

#[test]
fn test_max_function() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "temps".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "temp".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "temps",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(72),
        ]),
    )
    .unwrap();
    db.insert_row(
        "temps",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(65),
        ]),
    )
    .unwrap();
    db.insert_row(
        "temps",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(78),
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
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Function {
                name: "MAX".to_string(),
                args: vec![vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "temp".to_string(),
                }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "temps".to_string(),
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
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(78));
}

#[test]
fn test_min_max_on_strings() {
    // Edge case: MIN/MAX on VARCHAR values should use lexicographic ordering
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "names".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "names",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Zebra")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "names",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Apple")),
        ]),
    )
    .unwrap();
    db.insert_row(
        "names",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Mango")),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Test MIN
    let stmt_min = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Function {
                name: "MIN".to_string(),
                args: vec![vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "name".to_string(),
                }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "names".to_string(),
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

    let result = executor.execute(&stmt_min).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Apple")));

    // Test MAX
    let stmt_max = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Function {
                name: "MAX".to_string(),
                args: vec![vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "name".to_string(),
                }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "names".to_string(),
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

    let result = executor.execute(&stmt_max).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Zebra")));
}
