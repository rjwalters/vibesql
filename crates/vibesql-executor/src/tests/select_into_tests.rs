//! SELECT INTO tests - SQL:1999 Feature E111

use crate::{CreateTableExecutor, ExecutorError, SelectExecutor, SelectIntoExecutor};

#[test]
fn test_select_into_single_row() {
    let mut db = vibesql_storage::Database::new();

    // Create source table
    let create_stmt = vibesql_ast::CreateTableStmt {
        if_not_exists: false,
        table_name: "source".to_string(),
        columns: vec![
            vibesql_ast::ColumnDef {
                name: "id".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            vibesql_ast::ColumnDef {
                name: "name".to_string(),
                data_type: vibesql_types::DataType::Varchar { max_length: Some(50) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

    // Insert one row
    db.insert_row(
        "source",
        vibesql_storage::Row::from_vec(vec![
                vibesql_types::SqlValue::Integer(1),
                vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            ]),
    )
    .unwrap();

    // Execute SELECT INTO
    let select_stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef { table: None, column: "id".to_string() },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "name".to_string(),
                },
                alias: None,
            },
        ],
        into_table: Some("target".to_string()),
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "source".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };

    let result = SelectIntoExecutor::execute(&select_stmt, "target", &mut db);
    assert!(result.is_ok(), "SELECT INTO should succeed: {:?}", result.err());

    // Verify table was created
    assert!(db.catalog.table_exists("target"));

    // Verify row was inserted
    let executor = SelectExecutor::new(&db);
    let query = vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "target".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };
    let rows = executor.execute(&query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(rows[0].values[1], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
}

#[test]
fn test_select_into_no_rows_error() {
    let mut db = vibesql_storage::Database::new();

    // Create source table
    let create_stmt = vibesql_ast::CreateTableStmt {
        if_not_exists: false,
        table_name: "source".to_string(),
        columns: vec![vibesql_ast::ColumnDef {
            name: "id".to_string(),
            data_type: vibesql_types::DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

    // No rows inserted - table is empty

    // Execute SELECT INTO (should fail - no rows)
    let select_stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "id".to_string() },
            alias: None,
        }],
        into_table: Some("target".to_string()),
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "source".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };

    let result = SelectIntoExecutor::execute(&select_stmt, "target", &mut db);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::UnsupportedFeature(msg)) => {
            assert!(msg.contains("no rows"));
        }
        _ => panic!("Expected UnsupportedFeature error for no rows"),
    }
}

#[test]
fn test_select_into_multiple_rows_error() {
    let mut db = vibesql_storage::Database::new();

    // Create source table
    let create_stmt = vibesql_ast::CreateTableStmt {
        if_not_exists: false,
        table_name: "source".to_string(),
        columns: vec![vibesql_ast::ColumnDef {
            name: "id".to_string(),
            data_type: vibesql_types::DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

    // Insert multiple rows
    db.insert_row(
        "source",
        vibesql_storage::Row::from_vec(vec![vibesql_types::SqlValue::Integer(1)]),
    )
    .unwrap();
    db.insert_row(
        "source",
        vibesql_storage::Row::from_vec(vec![vibesql_types::SqlValue::Integer(2)]),
    )
    .unwrap();

    // Execute SELECT INTO (should fail - multiple rows)
    let select_stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "id".to_string() },
            alias: None,
        }],
        into_table: Some("target".to_string()),
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "source".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };

    let result = SelectIntoExecutor::execute(&select_stmt, "target", &mut db);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::UnsupportedFeature(msg)) => {
            assert!(msg.contains("2 rows"));
        }
        _ => panic!("Expected UnsupportedFeature error for multiple rows"),
    }
}

#[test]
fn test_select_into_with_expressions() {
    let mut db = vibesql_storage::Database::new();

    // Create source table
    let create_stmt = vibesql_ast::CreateTableStmt {
        if_not_exists: false,
        table_name: "source".to_string(),
        columns: vec![vibesql_ast::ColumnDef {
            name: "x".to_string(),
            data_type: vibesql_types::DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

    // Insert one row
    db.insert_row(
        "source",
        vibesql_storage::Row::from_vec(vec![vibesql_types::SqlValue::Integer(10)]),
    )
    .unwrap();

    // Execute SELECT INTO with expression and alias
    let select_stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::Plus,
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "x".to_string(),
                }),
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(5),
                )),
            },
            alias: Some("y".to_string()),
        }],
        into_table: Some("target".to_string()),
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "source".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };

    let result = SelectIntoExecutor::execute(&select_stmt, "target", &mut db);
    assert!(result.is_ok(), "SELECT INTO with expression should succeed: {:?}", result.err());

    // Verify table was created with correct column name
    let executor = SelectExecutor::new(&db);
    let query = vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "target".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };
    let rows = executor.execute(&query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Integer(15));
}
