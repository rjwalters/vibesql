//! Tests for the changes() function
//!
//! The changes() function returns the number of rows modified by the most recent
//! INSERT, UPDATE, or DELETE statement.
//!
//! This tests the core Database API for tracking changes count.

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

fn create_test_table(db: &mut Database) {
    let schema = TableSchema::new(
        "test".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("value".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(schema).expect("Failed to create test table");
}

#[test]
fn test_last_changes_count_initial_value() {
    let db = Database::new();

    // Before any DML operations, last_changes_count should return 0
    assert_eq!(db.last_changes_count(), 0);
}

#[test]
fn test_set_and_get_last_changes_count() {
    let mut db = Database::new();

    // Test setting and getting changes count
    db.set_last_changes_count(5);
    assert_eq!(db.last_changes_count(), 5);

    db.set_last_changes_count(10);
    assert_eq!(db.last_changes_count(), 10);

    db.set_last_changes_count(0);
    assert_eq!(db.last_changes_count(), 0);
}

#[test]
fn test_last_changes_count_after_insert_single_row() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // Insert a single row
    let row = Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100)]);
    db.insert_row("test", row).expect("Failed to insert");

    // Simulate what the executor does
    db.set_last_changes_count(1);

    assert_eq!(db.last_changes_count(), 1);
}

#[test]
fn test_last_changes_count_after_multiple_inserts() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // Insert 3 rows
    for i in 1..=3 {
        let row = Row::new(vec![SqlValue::Integer(i), SqlValue::Integer(i * 100)]);
        db.insert_row("test", row).expect("Failed to insert");
    }

    // Simulate what the executor does for a 3-row insert
    db.set_last_changes_count(3);

    assert_eq!(db.last_changes_count(), 3);
}

#[test]
fn test_last_changes_count_persists_across_reads() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // Set changes count
    db.set_last_changes_count(5);

    // Read from table (SELECT operations should not reset changes count)
    let table = db.get_table("test").expect("Table should exist");
    let _rows = table.scan();

    // Changes count should still be 5
    assert_eq!(db.last_changes_count(), 5);
}

#[test]
fn test_last_changes_count_zero_operations() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // Insert some rows first
    for i in 1..=3 {
        let row = Row::new(vec![SqlValue::Integer(i), SqlValue::Integer(100)]);
        db.insert_row("test", row).expect("Failed to insert");
    }

    // Simulate an update that affects 0 rows
    db.set_last_changes_count(0);

    assert_eq!(db.last_changes_count(), 0);
}

#[test]
fn test_last_changes_count_sequential_operations() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // Simulate: INSERT 3 rows
    db.set_last_changes_count(3);
    assert_eq!(db.last_changes_count(), 3);

    // Simulate: UPDATE 1 row
    db.set_last_changes_count(1);
    assert_eq!(db.last_changes_count(), 1);

    // Simulate: DELETE 2 rows
    db.set_last_changes_count(2);
    assert_eq!(db.last_changes_count(), 2);
}
