mod common;

use common::setup_users_table as setup_test_table;
use vibesql_executor::{ExecutorError, InsertExecutor};

#[test]
fn test_multi_row_insert_atomic_success() {
    let mut db = vibesql_storage::Database::new();
    setup_test_table(&mut db);

    // INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')
    // All should succeed
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
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))),
            ],
        ]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows, 3);

    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 3);
}

#[test]
fn test_multi_row_insert_atomic_failure() {
    let mut db = vibesql_storage::Database::new();
    setup_test_table(&mut db);

    // INSERT INTO users VALUES (1, 'Alice'), (NULL, 'Bob'), (3, 'Charlie')
    // Second row violates NOT NULL constraint on id, should fail atomically
    let stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null), // id = NULL (violates NOT NULL)
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob"))),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))),
            ],
        ]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let result = InsertExecutor::execute(&mut db, &stmt);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::ConstraintViolation(_)));

    // No rows should be inserted due to atomicity
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 0);
}

#[test]
fn test_multi_row_insert_with_column_list() {
    let mut db = vibesql_storage::Database::new();
    setup_test_table(&mut db);

    // INSERT INTO users (name, id) VALUES ('Alice', 1), ('Bob', 2)
    let stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec!["name".to_string(), "id".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob"))),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
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
fn test_multi_row_insert_type_mismatch() {
    let mut db = vibesql_storage::Database::new();
    setup_test_table(&mut db);

    // INSERT INTO users VALUES (1, 'Alice'), ('not_a_number', 'Bob')
    // Second row has type mismatch, should fail atomically
    let stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("not_a_number"))), /* Wrong type for id */
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob"))),
            ],
        ]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let result = InsertExecutor::execute(&mut db, &stmt);
    assert!(result.is_err());

    // No rows should be inserted due to atomicity
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 0);
}

#[test]
fn test_multi_row_insert_various_data_types() {
    let mut db = vibesql_storage::Database::new();

    // CREATE TABLE test_types (id INT NOT NULL, name VARCHAR(50), active BOOLEAN, score FLOAT)
    let schema = vibesql_catalog::TableSchema::new(
        "test_types".to_string(),
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
                "active".to_string(),
                vibesql_types::DataType::Boolean,
                true,
            ),
            vibesql_catalog::ColumnSchema::new(
                "score".to_string(),
                vibesql_types::DataType::Float { precision: 53 },
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // INSERT INTO test_types VALUES
    //   (1, 'Alice', TRUE, 95.5),
    //   (2, 'Bob', FALSE, 87.2),
    //   (3, NULL, NULL, NULL)
    let stmt = vibesql_ast::InsertStmt {
        table_name: "test_types".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Float(95.5)),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob"))),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(false)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Float(87.2)),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            ],
        ]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows, 3);

    let table = db.get_table("test_types").unwrap();
    assert_eq!(table.row_count(), 3);
}

#[test]
fn test_multi_row_insert_primary_key_violation() {
    let mut db = vibesql_storage::Database::new();

    // CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))
    let schema = vibesql_catalog::TableSchema::with_primary_key(
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
                true,
            ),
        ],
        vec!["id".to_string()],
    );
    db.create_table(schema).unwrap();

    // INSERT INTO users VALUES (1, 'Alice'), (1, 'Bob')
    // Second row violates PRIMARY KEY, should fail atomically
    let stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
            ],
            vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)), // Duplicate primary key
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob"))),
            ],
        ]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let result = InsertExecutor::execute(&mut db, &stmt);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::ConstraintViolation(_)));

    // No rows should be inserted due to atomicity
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 0);
}

#[test]
fn test_single_row_insert_no_transaction() {
    let mut db = vibesql_storage::Database::new();
    setup_test_table(&mut db);

    // Single row INSERT should work without implicit transaction
    let stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows, 1);

    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);
}
