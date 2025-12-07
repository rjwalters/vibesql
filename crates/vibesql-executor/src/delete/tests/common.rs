//! Common test utilities for delete tests

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

/// Sets up a users test table with id, name, and active columns with test data.
/// This table is used across delete test files.
#[allow(dead_code)] // Test helper - available for all test modules
pub fn setup_users_table_with_active(db: &mut Database) {
    // CREATE TABLE users (id INT, name VARCHAR(50), active BOOLEAN)
    let schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("active".to_string(), DataType::Boolean, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            SqlValue::Boolean(true),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            SqlValue::Boolean(false),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
            SqlValue::Boolean(true),
        ]),
    )
    .unwrap();
}
