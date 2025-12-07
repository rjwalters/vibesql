mod common;

use common::setup_users_table as setup_test_table;
use vibesql_executor::{ExecutorError, InsertExecutor};

#[test]
fn test_basic_insert() {
    let mut db = vibesql_storage::Database::new();
    setup_test_table(&mut db);

    // INSERT INTO users VALUES (1, 'Alice')
    let stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![], // No columns specified
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows, 1);

    // Verify row was inserted
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_multi_row_insert() {
    let mut db = vibesql_storage::Database::new();
    setup_test_table(&mut db);

    // INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')
    let stmt = vibesql_ast::InsertStmt {
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

    let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows, 2);

    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 2);
}

#[test]
fn test_insert_with_column_list() {
    let mut db = vibesql_storage::Database::new();
    setup_test_table(&mut db);

    // INSERT INTO users (name, id) VALUES ('Alice', 1)
    let stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec!["name".to_string(), "id".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows, 1);

    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_insert_null_value() {
    let mut db = vibesql_storage::Database::new();

    // CREATE TABLE users (id INT, name VARCHAR(50))
    // name is nullable
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
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                true, // nullable
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // INSERT INTO users VALUES (1, NULL)
    let stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows, 1);
}

#[test]
fn test_insert_type_mismatch() {
    let mut db = vibesql_storage::Database::new();
    setup_test_table(&mut db);

    // INSERT INTO users VALUES ('not_a_number', 'Alice')
    let stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("not_a_number"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let result = InsertExecutor::execute(&mut db, &stmt);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::UnsupportedExpression(_)));
}

#[test]
fn test_insert_column_count_mismatch() {
    let mut db = vibesql_storage::Database::new();
    setup_test_table(&mut db);

    // INSERT INTO users VALUES (1)  -- Missing name column
    let stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![vibesql_ast::Expression::Literal(
            vibesql_types::SqlValue::Integer(1),
        )]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let result = InsertExecutor::execute(&mut db, &stmt);
    assert!(result.is_err());
}

#[test]
fn test_insert_table_not_found() {
    let mut db = vibesql_storage::Database::new();

    // INSERT INTO nonexistent VALUES (1, 'Alice')
    let stmt = vibesql_ast::InsertStmt {
        table_name: "nonexistent".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let result = InsertExecutor::execute(&mut db, &stmt);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::TableNotFound(_)));
}

#[test]
fn test_insert_column_not_found() {
    let mut db = vibesql_storage::Database::new();
    setup_test_table(&mut db);

    // INSERT INTO users (id, invalid_col) VALUES (1, 'Alice')
    let stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec!["id".to_string(), "invalid_col".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let result = InsertExecutor::execute(&mut db, &stmt);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::ColumnNotFound { .. }));
}

#[test]
fn test_insert_not_null_constraint_violation() {
    let mut db = vibesql_storage::Database::new();
    setup_test_table(&mut db);

    // INSERT INTO users VALUES (NULL, 'Alice')
    // id column is NOT NULL, so this should fail
    let stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let result = InsertExecutor::execute(&mut db, &stmt);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("NOT NULL constraint violation"));
            assert!(msg.contains("column 'id'"));
            assert!(msg.contains("table 'users'"));
            assert!(msg.contains("cannot be NULL"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}
