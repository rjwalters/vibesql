mod common;

use common::setup_users_table as setup_test_table;
use vibesql_executor::{ExecutorError, InsertExecutor};

#[test]
fn test_insert_from_select_basic() {
    let mut db = vibesql_storage::Database::new();
    setup_test_table(&mut db);

    // First insert some data to select from
    let insert_stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Create another table to insert into
    let schema = vibesql_catalog::TableSchema::new(
        "users_backup".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // INSERT INTO users_backup SELECT * FROM users
    let select_stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "users".to_string(),
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

    let insert_select_stmt = vibesql_ast::InsertStmt {
        table_name: "users_backup".to_string(),
        columns: vec![], // No explicit columns, use all
        source: vibesql_ast::InsertSource::Select(Box::new(select_stmt)),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &insert_select_stmt).unwrap();
    assert_eq!(rows, 1);

    let table = db.get_table("users_backup").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_insert_from_select_with_where() {
    let mut db = vibesql_storage::Database::new();
    setup_test_table(&mut db);

    // Insert multiple users
    let insert_stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob"))),
            ],
        ]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Create backup table
    let schema = vibesql_catalog::TableSchema::new(
        "active_users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // INSERT INTO active_users SELECT * FROM users WHERE id = 1
    let select_stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
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
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };

    let insert_select_stmt = vibesql_ast::InsertStmt {
        table_name: "active_users".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Select(Box::new(select_stmt)),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &insert_select_stmt).unwrap();
    assert_eq!(rows, 1);

    let table = db.get_table("active_users").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_insert_from_select_column_mismatch() {
    let mut db = vibesql_storage::Database::new();
    setup_test_table(&mut db);

    // Insert some data
    let insert_stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Create a table with different column count
    let schema = vibesql_catalog::TableSchema::new(
        "wrong_table".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
            vibesql_catalog::ColumnSchema::new(
                "extra".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Try to INSERT with wrong column count
    let select_stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "users".to_string(),
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

    let insert_select_stmt = vibesql_ast::InsertStmt {
        table_name: "wrong_table".to_string(),
        columns: vec![], // Should match all columns
        source: vibesql_ast::InsertSource::Select(Box::new(select_stmt)),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let result = InsertExecutor::execute(&mut db, &insert_select_stmt);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::UnsupportedExpression(_)));
}

#[test]
fn test_insert_from_select_with_aggregates() {
    let mut db = vibesql_storage::Database::new();

    // Create sales table
    let schema = vibesql_catalog::TableSchema::new(
        "sales".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
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
    db.create_table(schema).unwrap();

    // Insert sales data
    let insert_stmt = vibesql_ast::InsertStmt {
        table_name: "sales".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(100)),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(200)),
            ],
        ]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Create summary table
    let summary_schema = vibesql_catalog::TableSchema::new(
        "summary".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "total".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "count".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(summary_schema).unwrap();

    // INSERT INTO summary SELECT SUM(amount), COUNT(*) FROM sales
    let select_stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::Function {
                    name: "SUM".to_string(),
                    args: vec![vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "amount".to_string(),
                    }],
                    character_unit: None,
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::Function {
                    name: "COUNT".to_string(),
                    args: vec![vibesql_ast::Expression::Wildcard],
                    character_unit: None,
                },
                alias: None,
            },
        ],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "sales".to_string(),
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

    let insert_select_stmt = vibesql_ast::InsertStmt {
        table_name: "summary".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Select(Box::new(select_stmt)),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    let rows = InsertExecutor::execute(&mut db, &insert_select_stmt).unwrap();
    assert_eq!(rows, 1);

    // Verify aggregated data was inserted
    let table = db.get_table("summary").unwrap();
    assert_eq!(table.row_count(), 1);
}
