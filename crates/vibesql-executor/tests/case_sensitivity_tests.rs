//! Comprehensive tests for case sensitivity identifier lookups
//!
//! Tests the case_sensitive_identifiers setting in the catalog which controls
//! whether table and view lookups are case-sensitive or case-insensitive.
//!
//! Default behavior (case_sensitive_identifiers = true, SQL:1999 compliant):
//! - Lookups are case-sensitive (SQL standard)
//! - The parser normalizes unquoted identifiers to uppercase
//! - Delimited identifiers preserve their exact case
//! - "users" and "USERS" are different tables
//!
//! When case_sensitive_identifiers = false (MySQL compatible mode):
//! - Lookups are case-insensitive
//! - "users", "USERS", "Users" all refer to the same table

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::Database;
use vibesql_types::DataType;

/// Test basic table creation and lookup with different cases (case-insensitive mode)
#[test]
fn test_table_lookup_case_insensitive_when_enabled() {
    let mut db = Database::new();

    // Default is case-sensitive (SQL:1999 compliant)
    assert!(db.catalog.is_case_sensitive_identifiers());

    // Enable case-insensitive mode for this test
    db.catalog.set_case_sensitive_identifiers(false);
    assert!(!db.catalog.is_case_sensitive_identifiers());

    // Create table with lowercase name
    let schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Should find table with different case variations
    assert!(db.catalog.get_table("users").is_some(), "Should find 'users'");
    assert!(db.catalog.get_table("USERS").is_some(), "Should find 'USERS'");
    assert!(db.catalog.get_table("Users").is_some(), "Should find 'Users'");
    assert!(db.catalog.get_table("uSeRs").is_some(), "Should find 'uSeRs'");
}

/// Test table creation with uppercase and lookup with lowercase (case-insensitive mode)
#[test]
fn test_table_lookup_created_uppercase_when_case_insensitive() {
    let mut db = Database::new();

    // Enable case-insensitive mode for this test
    db.catalog.set_case_sensitive_identifiers(false);

    // Create table with uppercase name
    let schema = TableSchema::new(
        "PRODUCTS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Should find with different case variations
    assert!(db.catalog.get_table("PRODUCTS").is_some());
    assert!(db.catalog.get_table("products").is_some());
    assert!(db.catalog.get_table("Products").is_some());
}

/// Test mixed case table creation (case-insensitive mode)
#[test]
fn test_table_lookup_mixed_case_when_case_insensitive() {
    let mut db = Database::new();

    // Enable case-insensitive mode for this test
    db.catalog.set_case_sensitive_identifiers(false);

    // Create table with mixed case name
    let schema = TableSchema::new(
        "MyTable".to_string(),
        vec![ColumnSchema::new("MyColumn".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Should find with different case variations
    assert!(db.catalog.get_table("MyTable").is_some());
    assert!(db.catalog.get_table("MYTABLE").is_some());
    assert!(db.catalog.get_table("mytable").is_some());
    assert!(db.catalog.get_table("myTABLE").is_some());
}

/// Test DROP TABLE with different cases (case-insensitive mode)
#[test]
fn test_drop_table_case_insensitive_when_enabled() {
    let mut db = Database::new();

    // Enable case-insensitive mode for this test
    db.catalog.set_case_sensitive_identifiers(false);

    // Create table with lowercase
    let schema = TableSchema::new(
        "orders".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Drop with uppercase should work
    assert!(db.catalog.drop_table("ORDERS").is_ok());

    // Table should be gone
    assert!(db.catalog.get_table("orders").is_none());
    assert!(db.catalog.get_table("ORDERS").is_none());
}

/// Test DROP TABLE with mixed case (case-insensitive mode)
#[test]
fn test_drop_table_mixed_case_when_case_insensitive() {
    let mut db = Database::new();

    // Enable case-insensitive mode for this test
    db.catalog.set_case_sensitive_identifiers(false);

    // Create with mixed case
    let schema = TableSchema::new(
        "CustomerOrders".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Drop with different case
    assert!(db.catalog.drop_table("customerorders").is_ok());

    // Verify it's dropped
    assert!(db.catalog.get_table("CustomerOrders").is_none());
}

/// Test case-sensitive mode behavior
#[test]
fn test_case_sensitive_mode() {
    let mut db = Database::new();

    // Enable case-sensitive mode
    db.catalog.set_case_sensitive_identifiers(true);
    assert!(db.catalog.is_case_sensitive_identifiers());

    // Create table with lowercase
    let schema1 = TableSchema::new(
        "users".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema1).unwrap();

    // Should find with exact case only
    assert!(db.catalog.get_table("users").is_some());
    assert!(db.catalog.get_table("USERS").is_none());
    assert!(db.catalog.get_table("Users").is_none());

    // Create another table with uppercase (should work since case-sensitive)
    let schema2 = TableSchema::new(
        "USERS".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema2).unwrap();

    // Now both should exist separately
    assert!(db.catalog.get_table("users").is_some());
    assert!(db.catalog.get_table("USERS").is_some());
}

/// Test toggling case sensitivity setting
#[test]
fn test_toggle_case_sensitivity() {
    let mut db = Database::new();

    // Default is case-sensitive (SQL:1999 compliant)
    assert!(db.catalog.is_case_sensitive_identifiers());

    // Start in case-insensitive mode for this test
    db.catalog.set_case_sensitive_identifiers(false);
    assert!(!db.catalog.is_case_sensitive_identifiers());

    // Create table in case-insensitive mode with uppercase name
    // This will ensure it's stored as "ORDERS" after normalization
    let schema = TableSchema::new(
        "ORDERS".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Should find with different cases in case-insensitive mode
    assert!(db.catalog.get_table("ORDERS").is_some());
    assert!(db.catalog.get_table("orders").is_some());

    // Switch to case-sensitive
    db.catalog.set_case_sensitive_identifiers(true);
    assert!(db.catalog.is_case_sensitive_identifiers());

    // Now lookups should be case-sensitive
    // The table is stored as "ORDERS" (since it was created with uppercase)
    assert!(db.catalog.get_table("orders").is_none());
    assert!(db.catalog.get_table("ORDERS").is_some());

    // Switch back to case-insensitive
    db.catalog.set_case_sensitive_identifiers(false);

    // Should work again
    assert!(db.catalog.get_table("orders").is_some());
    assert!(db.catalog.get_table("ORDERS").is_some());
}

/// Test view lookups with case insensitivity (case-insensitive mode)
#[test]
fn test_view_lookup_case_insensitive_when_enabled() {
    use vibesql_ast;
    use vibesql_catalog::ViewDefinition;

    let mut db = Database::new();

    // Enable case-insensitive mode for this test
    db.catalog.set_case_sensitive_identifiers(false);

    // Create a base table
    let schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Create a view with lowercase
    let select_stmt = vibesql_ast::SelectStmt {
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef { table: None, column: "id".to_string() },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "name".to_string(),
                },
                alias: None,
            },
        ],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
        with_clause: None,
    };

    let view = ViewDefinition {
        name: "active_users".to_string(),
        query: select_stmt,
        columns: None,
        with_check_option: false,
        sql_definition: None,
    };
    db.catalog.create_view(view).unwrap();

    // Should find view with different cases (case-insensitive mode enabled)
    assert!(db.catalog.get_view("active_users").is_some());
    assert!(db.catalog.get_view("ACTIVE_USERS").is_some());
    assert!(db.catalog.get_view("Active_Users").is_some());
}

/// Test DROP VIEW with case insensitivity (case-insensitive mode)
#[test]
fn test_drop_view_case_insensitive_when_enabled() {
    use vibesql_ast;
    use vibesql_catalog::ViewDefinition;

    let mut db = Database::new();

    // Enable case-insensitive mode for this test
    db.catalog.set_case_sensitive_identifiers(false);

    // Create a base table
    let schema = TableSchema::new(
        "products".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Create view
    let select_stmt = vibesql_ast::SelectStmt {
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "id".to_string() },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "products".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
        with_clause: None,
    };

    let view = ViewDefinition {
        name: "product_view".to_string(),
        query: select_stmt,
        columns: None,
        with_check_option: false,
        sql_definition: None,
    };
    db.catalog.create_view(view).unwrap();

    // Drop with uppercase
    assert!(db.catalog.drop_view("PRODUCT_VIEW", false).is_ok());

    // Should be gone
    assert!(db.catalog.get_view("product_view").is_none());
    assert!(db.catalog.get_view("PRODUCT_VIEW").is_none());
}

/// Test view in case-sensitive mode
#[test]
fn test_view_case_sensitive_mode() {
    use vibesql_ast;
    use vibesql_catalog::ViewDefinition;

    let mut db = Database::new();

    // Enable case-sensitive mode
    db.catalog.set_case_sensitive_identifiers(true);

    // Create base table
    let schema = TableSchema::new(
        "users".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Create view with lowercase
    let select_stmt = vibesql_ast::SelectStmt {
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "id".to_string() },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "users".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
        with_clause: None,
    };

    let view = ViewDefinition {
        name: "user_view".to_string(),
        query: select_stmt,
        columns: None,
        with_check_option: false,
        sql_definition: None,
    };
    db.catalog.create_view(view).unwrap();

    // Should only find with exact case
    assert!(db.catalog.get_view("user_view").is_some());
    assert!(db.catalog.get_view("USER_VIEW").is_none());
    assert!(db.catalog.get_view("User_View").is_none());
}

/// Test that table not found errors work correctly
#[test]
fn test_table_not_found_case_variations() {
    let mut db = Database::new();

    // Create table
    let schema = TableSchema::new(
        "existing_table".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Non-existent table should return None regardless of case
    assert!(db.catalog.get_table("nonexistent").is_none());
    assert!(db.catalog.get_table("NONEXISTENT").is_none());
    assert!(db.catalog.get_table("NonExistent").is_none());
}

/// Test DROP of non-existent table with case variations
#[test]
fn test_drop_nonexistent_table_case_variations() {
    let mut db = Database::new();

    // Dropping non-existent table should fail regardless of case
    assert!(db.catalog.drop_table("nonexistent").is_err());
    assert!(db.catalog.drop_table("NONEXISTENT").is_err());
    assert!(db.catalog.drop_table("NonExistent").is_err());
}
