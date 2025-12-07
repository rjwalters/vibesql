//! End-to-end integration tests for basic CRUD operations.
//!
//! Tests basic SELECT queries with WHERE clauses and column selection.

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

/// Execute a SELECT query end-to-end: parse SQL → execute → return results.
fn execute_select(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

fn create_users_schema() -> TableSchema {
    TableSchema::new(
        "USERS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
            ColumnSchema::new("AGE".to_string(), DataType::Integer, false),
        ],
    )
}

fn insert_sample_users(db: &mut Database) {
    let rows = vec![
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(std::sync::Arc::from("Alice")),
            SqlValue::Integer(25),
        ]),
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(std::sync::Arc::from("Bob")),
            SqlValue::Integer(17),
        ]),
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(std::sync::Arc::from("Charlie")),
            SqlValue::Integer(30),
        ]),
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar(std::sync::Arc::from("Diana")),
            SqlValue::Integer(22),
        ]),
        Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar(std::sync::Arc::from("Eve")),
            SqlValue::Integer(35),
        ]),
    ];

    for row in rows {
        db.insert_row("USERS", row).unwrap();
    }
}

#[test]
fn test_e2e_select_star() {
    let schema = create_users_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_users(&mut db);

    let results = execute_select(&db, "SELECT * FROM users").unwrap();
    assert_eq!(results.len(), 5);

    // Verify first row
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
    assert_eq!(results[0].values[1], SqlValue::Varchar(std::sync::Arc::from("Alice")));
    assert_eq!(results[0].values[2], SqlValue::Integer(25));
}

#[test]
fn test_e2e_select_specific_columns() {
    let schema = create_users_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_users(&mut db);

    let results = execute_select(&db, "SELECT name, age FROM users").unwrap();
    assert_eq!(results.len(), 5);

    // Verify structure: should have 2 columns (name, age)
    assert_eq!(results[0].values.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("Alice")));
    assert_eq!(results[0].values[1], SqlValue::Integer(25));
}

#[test]
fn test_e2e_select_with_where() {
    let schema = create_users_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_users(&mut db);

    let results = execute_select(&db, "SELECT name FROM users WHERE age > 25").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("Charlie")));
    assert_eq!(results[1].values[0], SqlValue::Varchar(std::sync::Arc::from("Eve")));
}

#[test]
fn test_e2e_select_with_complex_where() {
    let schema = create_users_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_users(&mut db);

    let results =
        execute_select(&db, "SELECT name FROM users WHERE age > 20 AND age < 30").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Varchar(std::sync::Arc::from("Alice")));
    assert_eq!(results[1].values[0], SqlValue::Varchar(std::sync::Arc::from("Diana")));
}
