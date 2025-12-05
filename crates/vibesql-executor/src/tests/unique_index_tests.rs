//! Integration tests for UNIQUE index constraint enforcement
//!
//! These tests verify that the executor correctly enforces uniqueness
//! for user-defined indexes created with CREATE UNIQUE INDEX.
//!
//! Tests cover:
//! - Basic uniqueness enforcement on INSERT
//! - Composite unique indexes
//! - NULL handling (multiple NULLs allowed)
//! - UPDATE constraint validation
//! - Case sensitivity

use vibesql_ast::{IndexColumn, OrderDirection};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use crate::errors::ExecutorError;

/// Create a test database with users table
fn create_test_db() -> Database {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    // Create users table with nullable email and phone columns
    let users_schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "email".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true, // nullable for NULL tests
            ),
            ColumnSchema::new(
                "first_name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
            ColumnSchema::new(
                "last_name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
            ColumnSchema::new(
                "phone".to_string(),
                DataType::Varchar { max_length: Some(20) },
                true,
            ),
        ],
    );

    db.create_table(users_schema).unwrap();
    db
}

#[test]
fn test_unique_index_basic_insert_enforcement() {
    let mut db = create_test_db();

    // Create unique index on email
    db.create_index(
        "idx_users_email".to_string(),
        "users".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "email".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    // First insert should succeed
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("alice@example.com".to_string()),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Varchar("Smith".to_string()),
            SqlValue::Null,
        ]),
    )
    .unwrap();

    // Second insert with same email should fail
    let result = db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("alice@example.com".to_string()),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Varchar("Jones".to_string()),
            SqlValue::Null,
        ]),
    );

    assert!(result.is_err());
    match result.unwrap_err() {
        vibesql_storage::StorageError::UniqueConstraintViolation(msg) => {
            assert!(msg.contains("UNIQUE constraint"));
            assert!(msg.contains("IDX_USERS_EMAIL")); // Index names are normalized to uppercase
        }
        e => panic!("Expected UniqueConstraintViolation, got {:?}", e),
    }
}

#[test]
fn test_unique_index_null_values_allowed() {
    let mut db = create_test_db();

    // Create unique index on email
    db.create_index(
        "idx_users_email".to_string(),
        "users".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "email".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    // Insert rows with NULL email - all should succeed
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Null, // NULL email
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Varchar("Smith".to_string()),
            SqlValue::Null,
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Null, // NULL email again - should succeed
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Varchar("Jones".to_string()),
            SqlValue::Null,
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Null, // Another NULL email - should succeed
            SqlValue::Varchar("Charlie".to_string()),
            SqlValue::Varchar("Brown".to_string()),
            SqlValue::Null,
        ]),
    )
    .unwrap();

    // All three inserts should succeed (NULL != NULL in SQL)
    let table = db.get_table("users").unwrap();
    assert_eq!(table.scan().len(), 3);
}

#[test]
fn test_unique_index_composite_key() {
    let mut db = create_test_db();

    // Create composite unique index on (first_name, last_name)
    db.create_index(
        "idx_users_name".to_string(),
        "users".to_string(),
        true, // unique
        vec![
            IndexColumn {
                column_name: "first_name".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
            IndexColumn {
                column_name: "last_name".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
        ],
    )
    .unwrap();

    // First insert: John Doe
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("john@example.com".to_string()),
            SqlValue::Varchar("John".to_string()),
            SqlValue::Varchar("Doe".to_string()),
            SqlValue::Null,
        ]),
    )
    .unwrap();

    // Second insert: John Smith (different last name) - should succeed
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("johnsmith@example.com".to_string()),
            SqlValue::Varchar("John".to_string()),
            SqlValue::Varchar("Smith".to_string()),
            SqlValue::Null,
        ]),
    )
    .unwrap();

    // Third insert: Jane Doe (different first name) - should succeed
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("jane@example.com".to_string()),
            SqlValue::Varchar("Jane".to_string()),
            SqlValue::Varchar("Doe".to_string()),
            SqlValue::Null,
        ]),
    )
    .unwrap();

    // Fourth insert: John Doe again - should fail
    let result = db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar("johndoe2@example.com".to_string()),
            SqlValue::Varchar("John".to_string()),
            SqlValue::Varchar("Doe".to_string()),
            SqlValue::Null,
        ]),
    );

    assert!(result.is_err());
    match result.unwrap_err() {
        vibesql_storage::StorageError::UniqueConstraintViolation(msg) => {
            assert!(msg.contains("UNIQUE constraint"));
            assert!(msg.contains("IDX_USERS_NAME")); // Index names are normalized to uppercase
        }
        e => panic!("Expected UniqueConstraintViolation, got {:?}", e),
    }
}

#[test]
fn test_unique_index_composite_with_null() {
    let mut db = create_test_db();

    // Create composite unique index on (first_name, last_name)
    db.create_index(
        "idx_users_name".to_string(),
        "users".to_string(),
        true, // unique
        vec![
            IndexColumn {
                column_name: "first_name".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
            IndexColumn {
                column_name: "last_name".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            },
        ],
    )
    .unwrap();

    // Insert with NULL in first_name
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("test1@example.com".to_string()),
            SqlValue::Null,
            SqlValue::Varchar("Doe".to_string()),
            SqlValue::Null,
        ]),
    )
    .unwrap();

    // Insert another row with NULL in first_name and same last_name - should succeed
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("test2@example.com".to_string()),
            SqlValue::Null,
            SqlValue::Varchar("Doe".to_string()),
            SqlValue::Null,
        ]),
    )
    .unwrap();

    // Both inserts should succeed because NULL is present
    let table = db.get_table("users").unwrap();
    assert_eq!(table.scan().len(), 2);
}

#[test]
fn test_unique_index_update_enforcement() {
    let mut db = create_test_db();

    // Create unique index on email
    db.create_index(
        "idx_users_email".to_string(),
        "users".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "email".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    // Insert two different users
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("alice@example.com".to_string()),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Varchar("Smith".to_string()),
            SqlValue::Null,
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("bob@example.com".to_string()),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Varchar("Jones".to_string()),
            SqlValue::Null,
        ]),
    )
    .unwrap();

    // Try to UPDATE bob's email to alice's email - should fail
    let parsed =
        Parser::parse_sql("UPDATE users SET email = 'alice@example.com' WHERE id = 2").unwrap();
    let stmt = match parsed {
        vibesql_ast::Statement::Update(stmt) => stmt,
        _ => panic!("Expected UPDATE statement"),
    };

    let result = crate::update::UpdateExecutor::execute(&stmt, &mut db);

    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            eprintln!("Error message: {}", msg);
            assert!(msg.contains("UNIQUE constraint"), "Message: {}", msg);
            assert!(
                msg.contains("IDX_USERS_EMAIL") || msg.contains("idx_users_email"),
                "Message: {}",
                msg
            ); // Index names should be normalized to uppercase
        }
        e => panic!("Expected ConstraintViolation, got {:?}", e),
    }
}

#[test]
fn test_unique_index_update_same_value_allowed() {
    let mut db = create_test_db();

    // Create unique index on email
    db.create_index(
        "idx_users_email".to_string(),
        "users".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "email".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    // Insert a user
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("alice@example.com".to_string()),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Varchar("Smith".to_string()),
            SqlValue::Null,
        ]),
    )
    .unwrap();

    // UPDATE with same email value should succeed
    let parsed = Parser::parse_sql(
        "UPDATE users SET email = 'alice@example.com', first_name = 'Alicia' WHERE id = 1",
    )
    .unwrap();
    let stmt = match parsed {
        vibesql_ast::Statement::Update(stmt) => stmt,
        _ => panic!("Expected UPDATE statement"),
    };

    let result = crate::update::UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1); // One row updated
}

#[test]
fn test_unique_index_update_to_different_value() {
    let mut db = create_test_db();

    // Create unique index on email
    db.create_index(
        "idx_users_email".to_string(),
        "users".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "email".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    // Insert a user
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("alice@example.com".to_string()),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Varchar("Smith".to_string()),
            SqlValue::Null,
        ]),
    )
    .unwrap();

    // UPDATE to different email value should succeed
    let parsed =
        Parser::parse_sql("UPDATE users SET email = 'newemail@example.com' WHERE id = 1").unwrap();
    let stmt = match parsed {
        vibesql_ast::Statement::Update(stmt) => stmt,
        _ => panic!("Expected UPDATE statement"),
    };

    let result = crate::update::UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1); // One row updated

    // Verify the email was actually updated
    let table = db.get_table("users").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.values[1], SqlValue::Varchar("newemail@example.com".to_string()));
}

#[test]
fn test_unique_index_update_to_null() {
    let mut db = create_test_db();

    // Create unique index on email
    db.create_index(
        "idx_users_email".to_string(),
        "users".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "email".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    // Insert a user with non-null email
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("alice@example.com".to_string()),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Varchar("Smith".to_string()),
            SqlValue::Null,
        ]),
    )
    .unwrap();

    // UPDATE email to NULL should succeed
    let parsed = Parser::parse_sql("UPDATE users SET email = NULL WHERE id = 1").unwrap();
    let stmt = match parsed {
        vibesql_ast::Statement::Update(stmt) => stmt,
        _ => panic!("Expected UPDATE statement"),
    };

    let result = crate::update::UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);

    // Verify the email was set to NULL
    let table = db.get_table("users").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.values[1], SqlValue::Null);
}

#[test]
fn test_unique_index_multiple_indexes_on_table() {
    let mut db = create_test_db();

    // Create two unique indexes on different columns
    db.create_index(
        "idx_users_email".to_string(),
        "users".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "email".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_users_phone".to_string(),
        "users".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "phone".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    // Insert first user
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("alice@example.com".to_string()),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Varchar("Smith".to_string()),
            SqlValue::Varchar("555-0001".to_string()),
        ]),
    )
    .unwrap();

    // Try to insert with duplicate email - should fail
    let result1 = db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("alice@example.com".to_string()),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Varchar("Jones".to_string()),
            SqlValue::Varchar("555-0002".to_string()),
        ]),
    );
    assert!(result1.is_err());

    // Try to insert with duplicate phone - should fail
    let result2 = db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("charlie@example.com".to_string()),
            SqlValue::Varchar("Charlie".to_string()),
            SqlValue::Varchar("Brown".to_string()),
            SqlValue::Varchar("555-0001".to_string()),
        ]),
    );
    assert!(result2.is_err());

    // Insert with unique email AND phone - should succeed
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar("diana@example.com".to_string()),
            SqlValue::Varchar("Diana".to_string()),
            SqlValue::Varchar("Prince".to_string()),
            SqlValue::Varchar("555-0003".to_string()),
        ]),
    )
    .unwrap();

    let table = db.get_table("users").unwrap();
    assert_eq!(table.scan().len(), 2); // Only first and last insert succeeded
}
