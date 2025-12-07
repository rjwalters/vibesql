//! Tests for indexed column prefix functionality (MySQL/SQLite compatibility)
//!
//! Tests prefix indexing feature where indexes can be created on the first N
//! characters of a string column, e.g., UNIQUE (email(50))

use vibesql_ast::{IndexColumn, OrderDirection};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

fn create_test_db() -> Database {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    let users_schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "email".to_string(),
                DataType::Varchar { max_length: Some(255) },
                false,
            ),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );

    db.create_table(users_schema).unwrap();
    db
}

#[test]
fn test_create_index_with_prefix_length() {
    let mut db = create_test_db();

    // Create index on first 50 characters of email
    db.create_index(
        "idx_email_prefix".to_string(),
        "users".to_string(),
        false, // not unique
        vec![IndexColumn {
            column_name: "email".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: Some(50),
        }],
    )
    .unwrap();

    assert!(db.index_exists("idx_email_prefix"));
}

#[test]
fn test_create_unique_index_with_prefix_length() {
    let mut db = create_test_db();

    // Create UNIQUE index on first 50 characters of email
    db.create_index(
        "idx_email_unique_prefix".to_string(),
        "users".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "email".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: Some(50),
        }],
    )
    .unwrap();

    assert!(db.index_exists("idx_email_unique_prefix"));
}

#[test]
fn test_unique_prefix_index_enforces_prefix_uniqueness() {
    let mut db = create_test_db();

    // Create UNIQUE index on first 10 characters
    db.create_index(
        "idx_email_prefix10".to_string(),
        "users".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "email".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: Some(10),
        }],
    )
    .unwrap();

    // Insert first row with email starting with "testuser12"
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("testuser123@example.com")),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]),
    )
    .expect("First insert should succeed");

    // Try to insert second row with email that has same 10-char prefix (should fail)
    let result = db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("testuser124@example.com")),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
        ]),
    );

    assert!(result.is_err(), "Should reject duplicate prefix");
    assert!(result.unwrap_err().to_string().contains("UNIQUE constraint"));
}

#[test]
fn test_unique_prefix_index_allows_different_prefixes() {
    let mut db = create_test_db();

    // Create UNIQUE index on first 10 characters
    db.create_index(
        "idx_email_prefix10".to_string(),
        "users".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "email".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: Some(10),
        }],
    )
    .unwrap();

    // Insert first row
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]),
    )
    .expect("First insert should succeed");

    // Insert second row with different 10-char prefix (should succeed)
    let result = db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("bob123456@example.com")),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
        ]),
    );

    assert!(result.is_ok(), "Should allow different prefixes: {:?}", result.err());
}

#[test]
fn test_prefix_index_with_short_strings() {
    let mut db = create_test_db();

    // Create UNIQUE index on first 50 characters (longer than some test strings)
    db.create_index(
        "idx_email_prefix50".to_string(),
        "users".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "email".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: Some(50),
        }],
    )
    .unwrap();

    // Insert row with short email (< 50 chars)
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("short@test.com")),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]),
    )
    .expect("First insert should succeed");

    // Try to insert duplicate short string (should fail)
    let result = db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("short@test.com")),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
        ]),
    );

    assert!(result.is_err(), "Should reject duplicate short string");
    assert!(result.unwrap_err().to_string().contains("UNIQUE constraint"));
}

#[test]
fn test_composite_prefix_index() {
    let mut db = create_test_db();

    // Create composite UNIQUE index with prefix on both email and name
    db.create_index(
        "idx_composite_prefix".to_string(),
        "users".to_string(),
        true, // unique
        vec![
            IndexColumn {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: Some(10),
            },
            IndexColumn {
                column_name: "name".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: Some(5),
            },
        ],
    )
    .unwrap();

    // Insert first row with email="alice@exam..." (10 chars) and name="Alice..." (5 chars)
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")), // email: "alice@exam" prefix
            SqlValue::Varchar(arcstr::ArcStr::from("Alice Smith")),       // name: "Alice" prefix
        ]),
    )
    .expect("First insert should succeed");

    // Try to insert row with same email(10) + name(5) prefix combination (should fail)
    let result = db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("alice@example.org")), // Same "alice@exam" prefix
            SqlValue::Varchar(arcstr::ArcStr::from("Alice Jones")),       // Same "Alice" prefix
        ]),
    );

    assert!(result.is_err(), "Should reject duplicate composite prefix: {:?}", result);
    assert!(result.unwrap_err().to_string().contains("UNIQUE constraint"));

    // Insert row with different email prefix (should succeed)
    let result = db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("bob@example.com")), // Different "bob@exampl" prefix
            SqlValue::Varchar(arcstr::ArcStr::from("Alice Smith")),     // Same "Alice" prefix is OK
        ]),
    );

    assert!(result.is_ok(), "Should allow different email prefix: {:?}", result.err());
}

#[test]
fn test_prefix_index_with_utf8_strings() {
    let mut db = create_test_db();

    // Create UNIQUE index on first 6 characters (UTF-8 aware)
    db.create_index(
        "idx_email_utf8".to_string(),
        "users".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "email".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: Some(6),
        }],
    )
    .unwrap();

    // Insert with UTF-8 characters - "josé@e" is the 6-char prefix
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("josé@example.com")),
            SqlValue::Varchar(arcstr::ArcStr::from("José")),
        ]),
    )
    .expect("First insert should succeed");

    // Try to insert with same UTF-8 prefix "josé@e" (should fail)
    let result = db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("josé@email.com")),
            SqlValue::Varchar(arcstr::ArcStr::from("José Jr.")),
        ]),
    );

    assert!(result.is_err(), "Should reject duplicate UTF-8 prefix: {:?}", result);
    assert!(result.unwrap_err().to_string().contains("UNIQUE constraint"));

    // Insert with different UTF-8 prefix (should succeed)
    let result = db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("maria@example.com")),
            SqlValue::Varchar(arcstr::ArcStr::from("Maria")),
        ]),
    );

    assert!(result.is_ok(), "Should allow different UTF-8 prefix: {:?}", result.err());
}

#[test]
fn test_non_unique_prefix_index_allows_duplicates() {
    let mut db = create_test_db();

    // Create non-unique index on first 10 characters
    db.create_index(
        "idx_email_prefix10".to_string(),
        "users".to_string(),
        false, // NOT unique
        vec![IndexColumn {
            column_name: "email".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: Some(10),
        }],
    )
    .unwrap();

    // Insert first row
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("testuser123@example.com")),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]),
    )
    .expect("First insert should succeed");

    // Insert second row with same 10-char prefix (should succeed for non-unique index)
    let result = db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("testuser124@example.com")),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
        ]),
    );

    assert!(result.is_ok(), "Non-unique index should allow duplicate prefixes: {:?}", result.err());
}
