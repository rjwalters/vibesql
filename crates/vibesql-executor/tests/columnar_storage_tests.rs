//! Integration tests for native columnar storage tables
//!
//! Tests the CREATE TABLE ... WITH (storage = 'columnar') functionality.

use vibesql_ast::Statement;
use vibesql_catalog::{ColumnSchema, StorageFormat, TableSchema};
use vibesql_executor::CreateTableExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::DataType;

/// Helper to create a columnar table using SQL
fn create_columnar_table(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        Statement::CreateTable(create_stmt) => CreateTableExecutor::execute(&create_stmt, db)
            .map_err(|e| format!("Execution error: {:?}", e)),
        other => Err(format!("Expected CREATE TABLE statement, got {:?}", other)),
    }
}

/// Helper to create a columnar table directly (bypasses executor schema handling)
fn create_columnar_table_direct(db: &mut Database, name: &str, columns: Vec<ColumnSchema>) {
    let schema =
        TableSchema::with_storage_format(name.to_string(), columns, StorageFormat::Columnar);
    db.create_table(schema).unwrap();
}

/// Helper to create a row table directly
fn create_row_table_direct(db: &mut Database, name: &str, columns: Vec<ColumnSchema>) {
    let schema = TableSchema::with_storage_format(name.to_string(), columns, StorageFormat::Row);
    db.create_table(schema).unwrap();
}

// ============================================================================
// Basic Columnar Table Creation Tests
// ============================================================================

#[test]
fn test_create_columnar_table_basic() {
    let mut db = Database::new();

    let result = create_columnar_table(
        &mut db,
        "CREATE TABLE analytics (id INT, value DOUBLE) STORAGE COLUMNAR",
    );
    assert!(result.is_ok(), "Should create columnar table: {:?}", result);

    // Verify table exists and has correct storage format
    let table = db.get_table("analytics").expect("Table should exist");
    assert!(table.schema.is_columnar(), "Table should be columnar");
    assert_eq!(table.schema.storage_format, StorageFormat::Columnar);
}

#[test]
fn test_create_row_table_explicit() {
    let mut db = Database::new();

    let result = create_columnar_table(
        &mut db,
        "CREATE TABLE oltp_data (id INT, name VARCHAR(50)) STORAGE ROW",
    );
    assert!(result.is_ok(), "Should create row table: {:?}", result);

    let table = db.get_table("oltp_data").expect("Table should exist");
    assert!(!table.schema.is_columnar(), "Table should be row-oriented");
    assert_eq!(table.schema.storage_format, StorageFormat::Row);
}

#[test]
fn test_create_table_default_is_row() {
    let mut db = Database::new();

    let result =
        create_columnar_table(&mut db, "CREATE TABLE default_table (id INT, name VARCHAR(50))");
    assert!(result.is_ok(), "Should create table with default storage");

    let table = db.get_table("default_table").expect("Table should exist");
    assert!(!table.schema.is_columnar(), "Default should be row-oriented");
    assert_eq!(table.schema.storage_format, StorageFormat::Row);
}

// ============================================================================
// Columnar Table Data Operations Tests
// ============================================================================

#[test]
fn test_columnar_table_insert_and_scan() {
    let mut db = Database::new();

    // Create columnar table directly
    create_columnar_table_direct(
        &mut db,
        "metrics",
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("value".to_string(), DataType::DoublePrecision, false),
        ],
    );

    // Insert rows
    db.insert_row(
        "metrics",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Double(3.5),
        ]),
    )
    .unwrap();

    db.insert_row(
        "metrics",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Double(2.5),
        ]),
    )
    .unwrap();

    // Verify row count
    let table = db.get_table("metrics").unwrap();
    assert_eq!(table.row_count(), 2);

    // Verify rows can be scanned
    let rows = table.scan();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_columnar_table_scan_columnar_returns_native_data() {
    let mut db = Database::new();

    // Create columnar table directly
    create_columnar_table_direct(
        &mut db,
        "native_scan",
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("amount".to_string(), DataType::Bigint, false),
        ],
    );

    // Insert data
    for i in 1..=10 {
        db.insert_row(
            "native_scan",
            vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(i),
                vibesql_types::SqlValue::Bigint(i * 100),
            ]),
        )
        .unwrap();
    }

    // Get columnar scan - this should be zero-copy for native columnar tables
    let table = db.get_table("native_scan").unwrap();
    let columnar = table.scan_columnar().expect("Columnar scan should succeed");

    // Verify columnar data
    assert_eq!(columnar.row_count(), 10);
    assert_eq!(columnar.column_count(), 2);

    // Verify column names
    assert!(columnar.get_column("id").is_some(), "Should have 'id' column");
    assert!(columnar.get_column("amount").is_some(), "Should have 'amount' column");
}

#[test]
fn test_columnar_table_is_native_columnar() {
    let mut db = Database::new();

    // Create columnar table directly
    create_columnar_table_direct(
        &mut db,
        "col_table",
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );

    // Create row table directly
    create_row_table_direct(
        &mut db,
        "row_table",
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );

    let col_table = db.get_table("col_table").unwrap();
    let row_table = db.get_table("row_table").unwrap();

    assert!(
        col_table.is_native_columnar(),
        "Columnar table should return true for is_native_columnar()"
    );
    assert!(
        !row_table.is_native_columnar(),
        "Row table should return false for is_native_columnar()"
    );
}

// ============================================================================
// Columnar Table with Various Data Types
// ============================================================================

#[test]
fn test_columnar_table_with_multiple_types() {
    let mut db = Database::new();

    // Create columnar table directly with multiple types
    create_columnar_table_direct(
        &mut db,
        "typed_data",
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("price".to_string(), DataType::DoublePrecision, false),
            ColumnSchema::new("quantity".to_string(), DataType::Bigint, false),
            ColumnSchema::new("rate".to_string(), DataType::DoublePrecision, false),
            ColumnSchema::new("active".to_string(), DataType::Boolean, false),
        ],
    );

    // Insert a row with various types
    db.insert_row(
        "typed_data",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Test Product")),
            vibesql_types::SqlValue::Double(19.99),
            vibesql_types::SqlValue::Bigint(100),
            vibesql_types::SqlValue::Double(0.15),
            vibesql_types::SqlValue::Boolean(true),
        ]),
    )
    .unwrap();

    let table = db.get_table("typed_data").unwrap();
    assert_eq!(table.row_count(), 1);

    // Verify columnar scan works with multiple types
    let columnar = table.scan_columnar().unwrap();
    assert_eq!(columnar.column_count(), 6);
}

#[test]
fn test_columnar_table_with_nulls() {
    let mut db = Database::new();

    // Create columnar table directly
    create_columnar_table_direct(
        &mut db,
        "nullable_data",
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("optional_value".to_string(), DataType::Integer, true),
        ],
    );

    // Insert rows with NULL values
    db.insert_row(
        "nullable_data",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Null,
        ]),
    )
    .unwrap();

    db.insert_row(
        "nullable_data",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(42),
        ]),
    )
    .unwrap();

    let table = db.get_table("nullable_data").unwrap();
    let columnar = table.scan_columnar().unwrap();
    assert_eq!(columnar.row_count(), 2);
}

// ============================================================================
// Row Table Still Uses Cache (Regression Test)
// ============================================================================

#[test]
fn test_row_table_columnar_scan_uses_conversion() {
    let mut db = Database::new();

    // Create a standard row table directly
    create_row_table_direct(
        &mut db,
        "row_only",
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("value".to_string(), DataType::Integer, false),
        ],
    );

    // Insert data
    db.insert_row(
        "row_only",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();

    let table = db.get_table("row_only").unwrap();

    // Row tables should not be native columnar
    assert!(!table.is_native_columnar(), "Row table should not be native columnar");

    // But columnar scan should still work (via conversion/cache)
    let columnar = table.scan_columnar().expect("Columnar scan should work for row tables too");
    assert_eq!(columnar.row_count(), 1);
}

// ============================================================================
// Storage Format in Catalog Tests
// ============================================================================

#[test]
fn test_storage_format_preserved_in_schema() {
    let mut db = Database::new();

    // Create columnar table directly
    create_columnar_table_direct(
        &mut db,
        "catalog_test",
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );

    // Access via catalog
    let schema = db.catalog.get_table("catalog_test").expect("Table should be in catalog");
    assert_eq!(schema.storage_format, StorageFormat::Columnar);
    assert!(schema.is_columnar());
}

#[test]
fn test_storage_format_with_table_constraints() {
    let mut db = Database::new();

    // Columnar table with PRIMARY KEY
    let result = create_columnar_table(
        &mut db,
        "CREATE TABLE constrained (
            id INT PRIMARY KEY,
            name VARCHAR(50)
        ) STORAGE COLUMNAR",
    );
    assert!(result.is_ok(), "Should create columnar table with constraints: {:?}", result);

    let table = db.get_table("constrained").unwrap();
    assert!(table.schema.is_columnar());
    assert!(table.schema.primary_key.is_some(), "Should have primary key");
}
