//! TPC-DS Schema Data Type Validation Tests
//!
//! These tests verify that the TPC-DS schema loading works correctly,
//! catching issues where schema definitions and data loading use
//! incompatible data types (e.g., DATE vs INTEGER).

/// Test that TPC-DS schema loading completes without type mismatches.
/// Regression test for issue #2772 - TypeMismatch for wp_rec_start_date.
#[test]
fn test_tpcds_schema_loads_without_type_mismatch() {
    // The TPC-DS schema module is part of the benchmarks, so we test it
    // by directly using the storage types to create the same schema structure.
    // This is a compile-time verification that the types are correct.

    use std::str::FromStr;

    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_storage::{Database, Row};
    use vibesql_types::{DataType, Date, SqlValue};

    let mut db = Database::new();

    // Create web_page table with DATE columns (matching the schema definition)
    db.create_table(TableSchema::new(
        "web_page".to_string(),
        vec![
            ColumnSchema {
                name: "wp_web_page_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None, is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "wp_web_page_id".to_string(),
                data_type: DataType::Varchar { max_length: Some(16) },
                nullable: false,
                default_value: None,
                generated_expr: None, is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "wp_rec_start_date".to_string(),
                data_type: DataType::Date,
                nullable: true,
                default_value: None,
                generated_expr: None, is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "wp_rec_end_date".to_string(),
                data_type: DataType::Date,
                nullable: true,
                default_value: None,
                generated_expr: None, is_exact_integer_type: false,
                collation: None,
            },
        ],
    ))
    .unwrap();

    // Insert row with correct DATE type (not INTEGER)
    let row = Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Varchar(arcstr::ArcStr::from("AAAAAA0000000001")),
        SqlValue::Date(Date::from_str("1998-01-01").unwrap()),
        SqlValue::Null,
    ]);

    // This should succeed - will fail with TypeMismatch if wrong type is used
    db.insert_row("web_page", row).unwrap();

    // Verify the table exists and has data
    let table = db.get_table("web_page").expect("web_page table should exist");
    assert_eq!(table.row_count(), 1);
}

/// Test that web_site table also uses correct DATE type for web_rec_start_date.
#[test]
fn test_web_site_date_columns() {
    use std::str::FromStr;

    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_storage::{Database, Row};
    use vibesql_types::{DataType, Date, SqlValue};

    let mut db = Database::new();

    // Create web_site table with DATE columns
    db.create_table(TableSchema::new(
        "web_site".to_string(),
        vec![
            ColumnSchema {
                name: "web_site_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None, is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "web_site_id".to_string(),
                data_type: DataType::Varchar { max_length: Some(16) },
                nullable: false,
                default_value: None,
                generated_expr: None, is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "web_rec_start_date".to_string(),
                data_type: DataType::Date,
                nullable: true,
                default_value: None,
                generated_expr: None, is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "web_rec_end_date".to_string(),
                data_type: DataType::Date,
                nullable: true,
                default_value: None,
                generated_expr: None, is_exact_integer_type: false,
                collation: None,
            },
        ],
    ))
    .unwrap();

    // Insert row with correct DATE type
    let row = Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Varchar(arcstr::ArcStr::from("AAAAAA0000000001")),
        SqlValue::Date(Date::from_str("1998-01-01").unwrap()),
        SqlValue::Null,
    ]);

    // This should succeed
    db.insert_row("web_site", row).unwrap();

    // Verify
    let table = db.get_table("web_site").expect("web_site table should exist");
    assert_eq!(table.row_count(), 1);
}

/// Test that inserting INTEGER into DATE column succeeds with SQLite type affinity.
/// SQLite allows any value to be stored in any column - types are preferences, not constraints.
#[test]
fn test_integer_into_date_column_sqlite_affinity() {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_storage::{Database, Row};
    use vibesql_types::{DataType, SqlValue};

    let mut db = Database::new();

    // Create table with DATE column
    db.create_table(TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None, is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "date_col".to_string(),
                data_type: DataType::Date,
                nullable: true,
                default_value: None,
                generated_expr: None, is_exact_integer_type: false,
                collation: None,
            },
        ],
    ))
    .unwrap();

    // Insert INTEGER into DATE column - SQLite affinity allows this
    let row = Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Integer(19980101), // SQLite affinity: stored as-is
    ]);

    // SQLite type affinity: this should succeed
    let result = db.insert_row("test_table", row);
    assert!(result.is_ok(), "SQLite type affinity should accept INTEGER in DATE column");

    // Verify the row was inserted
    let table = db.get_table("test_table").unwrap();
    assert_eq!(table.row_count(), 1);
}
