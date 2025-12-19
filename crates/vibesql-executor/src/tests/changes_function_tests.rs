//! Tests for the changes() and total_changes() functions
//!
//! The changes() function returns the number of rows modified by the most recent
//! INSERT, UPDATE, or DELETE statement.
//!
//! The total_changes() function returns the cumulative number of rows modified
//! since the database connection was opened.
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

// ============================================================================
// total_changes() Tests
// ============================================================================

#[test]
fn test_total_changes_count_initial_value() {
    let db = Database::new();

    // Before any DML operations, total_changes_count should return 0
    assert_eq!(db.total_changes_count(), 0);
}

#[test]
fn test_increment_total_changes_count() {
    let mut db = Database::new();

    // Test incrementing total changes count
    db.increment_total_changes_count(5);
    assert_eq!(db.total_changes_count(), 5);

    db.increment_total_changes_count(3);
    assert_eq!(db.total_changes_count(), 8);

    db.increment_total_changes_count(2);
    assert_eq!(db.total_changes_count(), 10);
}

#[test]
fn test_total_changes_accumulates_across_operations() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // Simulate: INSERT 3 rows
    db.set_last_changes_count(3);
    db.increment_total_changes_count(3);
    assert_eq!(db.last_changes_count(), 3);
    assert_eq!(db.total_changes_count(), 3);

    // Simulate: UPDATE 2 rows
    db.set_last_changes_count(2);
    db.increment_total_changes_count(2);
    assert_eq!(db.last_changes_count(), 2);
    assert_eq!(db.total_changes_count(), 5); // 3 + 2

    // Simulate: DELETE 1 row
    db.set_last_changes_count(1);
    db.increment_total_changes_count(1);
    assert_eq!(db.last_changes_count(), 1);
    assert_eq!(db.total_changes_count(), 6); // 3 + 2 + 1
}

#[test]
fn test_changes_resets_but_total_changes_accumulates() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // First operation: INSERT 5 rows
    db.set_last_changes_count(5);
    db.increment_total_changes_count(5);
    assert_eq!(db.last_changes_count(), 5);
    assert_eq!(db.total_changes_count(), 5);

    // Second operation: INSERT 3 rows
    // last_changes should be replaced, total_changes should accumulate
    db.set_last_changes_count(3);
    db.increment_total_changes_count(3);
    assert_eq!(db.last_changes_count(), 3); // Reset to 3
    assert_eq!(db.total_changes_count(), 8); // 5 + 3

    // Third operation: DELETE 2 rows
    db.set_last_changes_count(2);
    db.increment_total_changes_count(2);
    assert_eq!(db.last_changes_count(), 2); // Reset to 2
    assert_eq!(db.total_changes_count(), 10); // 5 + 3 + 2
}

#[test]
fn test_total_changes_zero_increment() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // First operation with some rows affected
    db.set_last_changes_count(5);
    db.increment_total_changes_count(5);
    assert_eq!(db.total_changes_count(), 5);

    // Operation that affects 0 rows (e.g., UPDATE with no matching WHERE)
    db.set_last_changes_count(0);
    db.increment_total_changes_count(0);
    assert_eq!(db.last_changes_count(), 0);
    assert_eq!(db.total_changes_count(), 5); // Should remain unchanged
}

#[test]
fn test_total_changes_persists_across_reads() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // Set up some changes
    db.set_last_changes_count(5);
    db.increment_total_changes_count(5);

    // Read from table (SELECT operations should not affect total_changes count)
    let table = db.get_table("test").expect("Table should exist");
    let _rows = table.scan();

    // Both counts should remain unchanged
    assert_eq!(db.last_changes_count(), 5);
    assert_eq!(db.total_changes_count(), 5);
}

#[test]
fn test_cloned_database_resets_total_changes() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // Build up some change counts
    db.set_last_changes_count(10);
    db.increment_total_changes_count(10);
    assert_eq!(db.total_changes_count(), 10);

    // Clone the database - should reset counts per SQLite semantics
    // (new connection = new tracking)
    let cloned = db.clone();
    assert_eq!(cloned.last_changes_count(), 0);
    assert_eq!(cloned.total_changes_count(), 0);
}
