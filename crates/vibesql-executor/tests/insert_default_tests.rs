mod common;

use vibesql_executor::InsertExecutor;

#[test]
fn test_character_varying_column_with_length() {
    let mut db = vibesql_storage::Database::new();

    // CREATE TABLE test_cv (id INT, description CHARACTER VARYING(100))
    let schema = vibesql_catalog::TableSchema::new(
        "test_cv".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "description".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // INSERT INTO test_cv VALUES (1, 'Test description')
    let stmt = vibesql_ast::InsertStmt {
        table_name: "test_cv".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Test description"))),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows, 1);

    // Verify data was inserted correctly
    let table = db.get_table("test_cv").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_character_varying_column_without_length() {
    let mut db = vibesql_storage::Database::new();

    // CREATE TABLE test_cv_nolen (id INT, text CHARACTER VARYING)
    let schema = vibesql_catalog::TableSchema::new(
        "test_cv_nolen".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "text".to_string(),
                vibesql_types::DataType::Varchar { max_length: None },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // INSERT INTO test_cv_nolen VALUES (1, 'Unlimited length text')
    let stmt = vibesql_ast::InsertStmt {
        table_name: "test_cv_nolen".to_string(),
        columns: vec![],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Unlimited length text"))),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows, 1);

    // Verify data was inserted correctly
    let table = db.get_table("test_cv_nolen").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_insert_with_default_value() {
    let mut db = vibesql_storage::Database::new();

    // CREATE TABLE users (id INT DEFAULT 999, name VARCHAR(50))
    let mut id_column = vibesql_catalog::ColumnSchema::new(
        "id".to_string(),
        vibesql_types::DataType::Integer,
        false,
    );
    id_column.default_value =
        Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(999)));

    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            id_column,
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // INSERT INTO users (id, name) VALUES (DEFAULT, 'Alice')
    let stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec!["id".to_string(), "name".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Default,
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows, 1);

    // Verify default value was used
    let table = db.get_table("users").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(0), Some(&vibesql_types::SqlValue::Integer(999)));
    assert_eq!(row.get(1), Some(&vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))));
}

#[test]
fn test_insert_default_no_default_value_defined() {
    let mut db = vibesql_storage::Database::new();

    // CREATE TABLE users (id INT, name VARCHAR(50)) -- no default for id
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ), /* nullable */
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // INSERT INTO users (id, name) VALUES (DEFAULT, 'Alice')
    let stmt = vibesql_ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec!["id".to_string(), "name".to_string()],
        source: vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Default,
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };

    let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
    assert_eq!(rows, 1);

    // Verify NULL was used when no default is defined
    let table = db.get_table("users").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(0), Some(&vibesql_types::SqlValue::Null));
    assert_eq!(row.get(1), Some(&vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice"))));
}
