// ============================================================================
// Binary Persistence Tests
// ============================================================================
//
// Tests for binary format serialization/deserialization, including:
// - TableIdentifier quoted flag persistence (SQL:1999 case-sensitivity)
// - Backward compatibility with older format versions

use vibesql_catalog::{ColumnSchema, TableIdentifier, TableSchema};
use vibesql_types::{DataType, SqlValue};

use crate::Database;

/// Test that the quoted flag for TableIdentifier is persisted correctly
/// through a save/load roundtrip.
#[test]
fn test_quoted_table_identifier_roundtrip() {
    let mut db = Database::new();

    // Create a table with a quoted identifier (case-sensitive)
    let schema = TableSchema::new(
        "MyTable".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    let identifier = TableIdentifier::quoted("MyTable");
    db.create_table_with_identifier(schema, identifier).unwrap();

    // Verify the identifier is marked as quoted
    let original_identifier = db.catalog.get_table_identifier("MyTable").unwrap();
    assert!(original_identifier.is_quoted(), "Original table should be quoted");
    assert_eq!(original_identifier.canonical(), "MyTable");

    // Save and load using binary format
    let path = "/tmp/test_quoted_identifier.vbsql";
    db.save_binary(path).unwrap();

    let loaded_db = Database::load_binary(path).unwrap();

    // Verify the quoted flag was preserved
    let loaded_identifier = loaded_db.catalog.get_table_identifier("MyTable").unwrap();
    assert!(
        loaded_identifier.is_quoted(),
        "Loaded table should still be quoted after roundtrip"
    );
    assert_eq!(loaded_identifier.canonical(), "MyTable");

    // Cleanup
    std::fs::remove_file(path).ok();
}

/// Test that unquoted tables remain unquoted through roundtrip
#[test]
fn test_unquoted_table_identifier_roundtrip() {
    let mut db = Database::new();

    // Create a table with an unquoted identifier (case-insensitive)
    let schema = TableSchema::new(
        "users".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    let identifier = TableIdentifier::unquoted("users");
    db.create_table_with_identifier(schema, identifier).unwrap();

    // Verify the identifier is NOT quoted
    let original_identifier = db.catalog.get_table_identifier("users").unwrap();
    assert!(
        !original_identifier.is_quoted(),
        "Original table should be unquoted"
    );
    assert_eq!(original_identifier.canonical(), "users");

    // Save and load using binary format
    let path = "/tmp/test_unquoted_identifier.vbsql";
    db.save_binary(path).unwrap();

    let loaded_db = Database::load_binary(path).unwrap();

    // Verify the unquoted flag was preserved
    let loaded_identifier = loaded_db.catalog.get_table_identifier("users").unwrap();
    assert!(
        !loaded_identifier.is_quoted(),
        "Loaded table should still be unquoted after roundtrip"
    );
    assert_eq!(loaded_identifier.canonical(), "users");

    // Cleanup
    std::fs::remove_file(path).ok();
}

/// Test that both quoted and unquoted tables can coexist and be persisted correctly
#[test]
fn test_mixed_quoted_unquoted_tables_roundtrip() {
    let mut db = Database::new();

    // Create an unquoted table (case-insensitive)
    let schema1 = TableSchema::new(
        "users".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table_with_identifier(schema1, TableIdentifier::unquoted("users"))
        .unwrap();

    // Create a quoted table (case-sensitive)
    let schema2 = TableSchema::new(
        "UserProfiles".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table_with_identifier(schema2, TableIdentifier::quoted("UserProfiles"))
        .unwrap();

    // Save and load
    let path = "/tmp/test_mixed_identifiers.vbsql";
    db.save_binary(path).unwrap();

    let loaded_db = Database::load_binary(path).unwrap();

    // Verify both tables preserved their quoted flags
    let users_id = loaded_db.catalog.get_table_identifier("users").unwrap();
    assert!(!users_id.is_quoted(), "users should remain unquoted");

    let profiles_id = loaded_db.catalog.get_table_identifier("UserProfiles").unwrap();
    assert!(profiles_id.is_quoted(), "UserProfiles should remain quoted");

    // Cleanup
    std::fs::remove_file(path).ok();
}

/// Test that tables with data preserve quoted flag
#[test]
fn test_quoted_table_with_data_roundtrip() {
    let mut db = Database::new();

    // Create a quoted table with data
    let schema = TableSchema::new(
        "CaseSensitiveTable".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    db.create_table_with_identifier(schema, TableIdentifier::quoted("CaseSensitiveTable"))
        .unwrap();

    // Insert data
    let table = db.get_table_mut("CaseSensitiveTable").unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]))
        .unwrap();
    table
        .insert(crate::Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
        ]))
        .unwrap();

    // Save and load
    let path = "/tmp/test_quoted_with_data.vbsql";
    db.save_binary(path).unwrap();

    let loaded_db = Database::load_binary(path).unwrap();

    // Verify quoted flag preserved
    let identifier = loaded_db
        .catalog
        .get_table_identifier("CaseSensitiveTable")
        .unwrap();
    assert!(identifier.is_quoted());

    // Verify data preserved
    let loaded_table = loaded_db.get_table("CaseSensitiveTable").unwrap();
    assert_eq!(loaded_table.row_count(), 2);

    // Cleanup
    std::fs::remove_file(path).ok();
}

/// Test legacy behavior: tables created without identifier use default (unquoted)
#[test]
fn test_legacy_table_creation_defaults_to_unquoted() {
    let mut db = Database::new();

    // Create table using legacy method (no explicit identifier)
    let schema = TableSchema::new(
        "legacy_table".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Should default to unquoted
    let identifier = db.catalog.get_table_identifier("legacy_table").unwrap();
    assert!(!identifier.is_quoted(), "Legacy tables should default to unquoted");

    // Save and load
    let path = "/tmp/test_legacy_table.vbsql";
    db.save_binary(path).unwrap();

    let loaded_db = Database::load_binary(path).unwrap();

    // Verify still unquoted after load
    let loaded_id = loaded_db.catalog.get_table_identifier("legacy_table").unwrap();
    assert!(!loaded_id.is_quoted());

    // Cleanup
    std::fs::remove_file(path).ok();
}
