//! Schema introspection tests for WASM API
//!
//! Tests schema inspection functionality: list tables, describe tables, column metadata

use super::helpers::execute_sql;

#[test]
fn test_schema_list_tables() {
    let mut db = vibesql_storage::Database::new();

    // Create multiple tables
    execute_sql(&mut db, "CREATE TABLE users (id INTEGER)").unwrap();
    execute_sql(&mut db, "CREATE TABLE products (id INTEGER)").unwrap();
    execute_sql(&mut db, "CREATE TABLE orders (id INTEGER)").unwrap();

    // List tables
    let tables = db.list_tables();

    // Table names are normalized to lowercase per SQL:1999 (unquoted identifiers)
    assert_eq!(tables.len(), 3);
    assert!(tables.contains(&"users".to_string()));
    assert!(tables.contains(&"products".to_string()));
    assert!(tables.contains(&"orders".to_string()));
}

#[test]
fn test_schema_describe_table() {
    let mut db = vibesql_storage::Database::new();

    // Create table with multiple columns and types
    execute_sql(
        &mut db,
        "CREATE TABLE employees (id INTEGER, name VARCHAR(100), salary DOUBLE, active BOOLEAN)",
    )
    .unwrap();

    // Get table schema (table names are normalized to lowercase per SQL:1999)
    let table = db.get_table("employees").expect("Table not found");
    let schema = &table.schema;

    // Column names are normalized to lowercase per SQL:1999 (unquoted identifiers)
    assert_eq!(schema.columns.len(), 4);
    assert_eq!(schema.columns[0].name, "id");
    assert_eq!(schema.columns[1].name, "name");
    assert_eq!(schema.columns[2].name, "salary");
    assert_eq!(schema.columns[3].name, "active");
}

#[test]
fn test_schema_table_not_found() {
    let db = vibesql_storage::Database::new();

    // Try to get non-existent table
    let result = db.get_table("nonexistent");

    assert!(result.is_none());
}

#[test]
fn test_schema_column_metadata() {
    let mut db = vibesql_storage::Database::new();

    // Create table
    execute_sql(&mut db, "CREATE TABLE test (id INTEGER, description VARCHAR(255))").unwrap();

    // Table and column names are normalized to lowercase per SQL:1999
    let table = db.get_table("test").expect("Table not found");
    let schema = &table.schema;

    // Verify column metadata
    assert_eq!(schema.columns[0].name, "id");
    assert!(matches!(schema.columns[0].data_type, vibesql_types::DataType::Integer));

    assert_eq!(schema.columns[1].name, "description");
    assert!(matches!(schema.columns[1].data_type, vibesql_types::DataType::Varchar { .. }));
}
