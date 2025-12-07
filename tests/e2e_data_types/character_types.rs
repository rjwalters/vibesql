//! End-to-end tests for character SQL data types.

use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

use super::fixtures::*;

#[test]
fn test_e2e_char_type() {
    // Test CHAR fixed-length type with space padding behavior
    let schema = create_codes_schema();

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert CHAR values - should pad short strings with spaces
    db.insert_row(
        "CODES",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Character(std::sync::Arc::from("ABC")), // Will be padded to "ABC  " (5 chars)
            SqlValue::Character(std::sync::Arc::from("Hello")), // Will be padded to "Hello     " (10 chars)
        ]),
    )
    .unwrap();

    db.insert_row(
        "CODES",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Character(std::sync::Arc::from("12345")), // Exact length (5 chars)
            SqlValue::Character(std::sync::Arc::from("World")), // Will be padded to "World     " (10 chars)
        ]),
    )
    .unwrap();

    db.insert_row(
        "CODES",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Character(std::sync::Arc::from("TOOLONG")), // Will be truncated to "TOOLO" (5 chars)
            SqlValue::Character(std::sync::Arc::from("VeryLongName")), /* Will be truncated to
                                                         * "VeryLongNa" (10 chars) */
        ]),
    )
    .unwrap();

    // SELECT all rows
    let results = execute_select(&db, "SELECT id, code, name FROM codes").unwrap();
    assert_eq!(results.len(), 3);

    // Row 1: Check padding
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
    assert_eq!(results[0].values[1], SqlValue::Character(std::sync::Arc::from("ABC  ")));
    assert_eq!(results[0].values[2], SqlValue::Character(std::sync::Arc::from("Hello     ")));

    // Row 2: Check exact length
    assert_eq!(results[1].values[0], SqlValue::Integer(2));
    assert_eq!(results[1].values[1], SqlValue::Character(std::sync::Arc::from("12345")));
    assert_eq!(results[1].values[2], SqlValue::Character(std::sync::Arc::from("World     ")));

    // Row 3: Check truncation
    assert_eq!(results[2].values[0], SqlValue::Integer(3));
    assert_eq!(results[2].values[1], SqlValue::Character(std::sync::Arc::from("TOOLO")));
    assert_eq!(results[2].values[2], SqlValue::Character(std::sync::Arc::from("VeryLongNa")));

    // Test WHERE clause with CHAR comparison
    let results = execute_select(&db, "SELECT id FROM codes WHERE code = 'ABC  '").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
}
