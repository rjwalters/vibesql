// Test to verify hash indexes work for UPDATE constraint validation
use std::time::Instant;

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

#[test]
fn test_update_hash_indexes() {
    println!("Testing hash indexes for UPDATE constraint validation...");

    let mut db = Database::new();

    // Create a table with primary key and unique constraint
    let schema = TableSchema::with_all_constraints(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "email".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new(
                "username".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
        Some(vec!["id".to_string()]),
        vec![vec!["email".to_string()], vec!["username".to_string()]],
    );

    db.create_table(schema).unwrap();

    // Insert many rows
    println!("Inserting 1000 rows...");
    let start = Instant::now();
    for i in 0..1000 {
        let row = Row::new(vec![
            SqlValue::Integer(i),
            SqlValue::Varchar(format!("user{}@example.com", i)),
            SqlValue::Varchar(format!("user{}", i)),
        ]);
        db.insert_row("users", row).unwrap();
    }
    let insert_time = start.elapsed();
    println!("Insert time: {:?}", insert_time);

    // Verify that hash indexes exist and are populated
    let table = db.get_table("users").unwrap();
    assert!(table.primary_key_index().is_some(), "Primary key index should exist");

    let pk_index = table.primary_key_index().unwrap();
    assert_eq!(pk_index.len(), 1000, "Primary key index should contain 1000 entries");

    let unique_indexes = table.unique_indexes();
    assert_eq!(unique_indexes.len(), 2, "Should have 2 unique indexes");
    assert_eq!(unique_indexes[0].len(), 1000, "Email unique index should contain 1000 entries");
    assert_eq!(unique_indexes[1].len(), 1000, "Username unique index should contain 1000 entries");

    // Test that constraint checking is fast (should be O(1) with hash indexes)
    // We simulate the constraint check logic from the UPDATE executor
    println!("Testing hash index lookup performance...");

    let schema = db.catalog.get_table("users").unwrap();

    // Test primary key constraint check - should be O(1)
    let start = Instant::now();
    let pk_indices = schema.get_primary_key_indices().unwrap();
    let test_pk_values: Vec<vibesql_types::SqlValue> =
        pk_indices.iter().map(|&idx| table.scan()[0].values[idx].clone()).collect();

    let constraint_violated = if let Some(pk_index) = table.primary_key_index() {
        // This simulates the constraint check: look up if this PK already exists
        pk_index.contains_key(&test_pk_values)
    } else {
        false
    };
    let lookup_time = start.elapsed();

    println!("Primary key constraint check time: {:?}", lookup_time);
    assert!(constraint_violated, "Should detect existing primary key");
    // Hash lookup should be very fast (< 1ms for 1000 entries)
    assert!(lookup_time < std::time::Duration::from_millis(1), "Hash lookup should be fast");

    // Test unique constraint check - should be O(1)
    let start = Instant::now();
    let unique_constraint_indices = schema.get_unique_constraint_indices();
    let unique_indices = &unique_constraint_indices[0]; // email constraint
    let test_unique_values: Vec<vibesql_types::SqlValue> =
        unique_indices.iter().map(|&idx| table.scan()[0].values[idx].clone()).collect();

    let constraint_violated =
        if !test_unique_values.iter().any(|v| *v == vibesql_types::SqlValue::Null) {
            unique_indexes[0].contains_key(&test_unique_values)
        } else {
            false
        };
    let lookup_time = start.elapsed();

    println!("Unique constraint check time: {:?}", lookup_time);
    assert!(constraint_violated, "Should detect existing unique value");
    // Hash lookup should be very fast (< 1ms for 1000 entries)
    assert!(lookup_time < std::time::Duration::from_millis(1), "Hash lookup should be fast");

    println!("Hash indexes working correctly for UPDATE operations!");
}
