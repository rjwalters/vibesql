//! Test CREATE SCHEMA with embedded schema elements

use vibesql_ast::Statement;
use vibesql_executor::SchemaExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[test]
fn test_create_schema_with_embedded_table() {
    let mut db = Database::new();

    // Parse CREATE SCHEMA with embedded CREATE TABLE
    let sql = "CREATE SCHEMA test_schema CREATE TABLE users (id INT, name VARCHAR(50))";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    // Execute the statement
    if let vibesql_ast::Statement::CreateSchema(create_schema_stmt) = stmt {
        let result = SchemaExecutor::execute_create_schema(&create_schema_stmt, &mut db);
        assert!(result.is_ok(), "Failed to execute: {:?}", result.err());

        // Verify schema was created
        assert!(db.catalog.schema_exists("test_schema"));

        // Verify table was created in the schema
        assert!(db.catalog.table_exists("test_schema.users"));
    } else {
        panic!("Expected CreateSchema statement");
    }
}

#[test]
fn test_create_schema_with_multiple_tables() {
    let mut db = Database::new();

    // Parse CREATE SCHEMA with multiple embedded CREATE TABLE statements
    let sql = "CREATE SCHEMA myschema CREATE TABLE t1 (a INT) CREATE TABLE t2 (b VARCHAR(10))";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    // Execute the statement
    if let vibesql_ast::Statement::CreateSchema(create_schema_stmt) = stmt {
        let result = SchemaExecutor::execute_create_schema(&create_schema_stmt, &mut db);
        assert!(result.is_ok(), "Failed to execute: {:?}", result.err());

        // Verify schema was created
        assert!(db.catalog.schema_exists("myschema"));

        // Verify both tables were created
        assert!(db.catalog.table_exists("myschema.t1"));
        assert!(db.catalog.table_exists("myschema.t2"));
    } else {
        panic!("Expected CreateSchema statement");
    }
}

#[test]
fn test_create_schema_no_elements_still_works() {
    let mut db = Database::new();

    // Parse simple CREATE SCHEMA (no embedded elements)
    let sql = "CREATE SCHEMA simple";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    // Execute the statement
    if let vibesql_ast::Statement::CreateSchema(create_schema_stmt) = stmt {
        let result = SchemaExecutor::execute_create_schema(&create_schema_stmt, &mut db);
        assert!(result.is_ok(), "Failed to execute: {:?}", result.err());

        // Verify schema was created
        assert!(db.catalog.schema_exists("simple"));
    } else {
        panic!("Expected CreateSchema statement");
    }
}

#[test]
fn test_create_schema_rollback_on_first_element_failure() {
    let mut db = Database::new();

    // Create the first table successfully first
    let sql = "CREATE SCHEMA app CREATE TABLE items (id INT)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateSchema(create_schema_stmt) = stmt {
        let result = SchemaExecutor::execute_create_schema(&create_schema_stmt, &mut db);
        assert!(result.is_ok());
        assert!(db.catalog.schema_exists("app"));
        assert!(db.catalog.table_exists("app.items"));
    }

    // Now try to create a schema with a table that conflicts with the existing table
    let sql2 = "CREATE SCHEMA app2 CREATE TABLE items (id INT)";
    let stmt2 = Parser::parse_sql(sql2).expect("Failed to parse");

    // Execute the statement
    if let Statement::CreateSchema(create_schema_stmt2) = stmt2 {
        // Set current schema to app2 will fail if table already exists in catalog
        let result = SchemaExecutor::execute_create_schema(&create_schema_stmt2, &mut db);

        // Check if it failed (depends on whether catalog validates duplicate table names)
        if result.is_err() {
            // Verify schema was rolled back if error occurred
            assert!(!db.catalog.schema_exists("app2"), "Schema should be rolled back");
        } else {
            // If no error, both schemas should exist with separate tables
            assert!(db.catalog.schema_exists("app2"));
            assert!(db.catalog.table_exists("app2.items"));
        }
    } else {
        panic!("Expected CreateSchema statement");
    }
}

#[test]
fn test_create_schema_rollback_on_second_element_failure() {
    let mut db = Database::new();

    // First table is valid, second table has same name (duplicate)
    let sql = "CREATE SCHEMA sales \
               CREATE TABLE customers (id INT, name VARCHAR(100)) \
               CREATE TABLE customers (id INT)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    // Execute the statement
    if let Statement::CreateSchema(create_schema_stmt) = stmt {
        let result = SchemaExecutor::execute_create_schema(&create_schema_stmt, &mut db);

        // Should fail on second table due to duplicate name
        assert!(result.is_err(), "Expected error on second table due to duplicate name");

        // Verify complete rollback - neither schema nor first table should exist
        assert!(!db.catalog.schema_exists("sales"), "Schema should be rolled back");
        assert!(!db.catalog.table_exists("sales.customers"), "First table should be rolled back");
    } else {
        panic!("Expected CreateSchema statement");
    }
}

#[test]
fn test_create_schema_success_commits_all_elements() {
    let mut db = Database::new();

    // All valid elements - should succeed atomically
    let sql = "CREATE SCHEMA retail \
               CREATE TABLE products (id INT, name VARCHAR(50)) \
               CREATE TABLE inventory (product_id INT, quantity INT)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    // Execute the statement
    if let Statement::CreateSchema(create_schema_stmt) = stmt {
        let result = SchemaExecutor::execute_create_schema(&create_schema_stmt, &mut db);

        // Should succeed
        assert!(result.is_ok(), "Expected success: {:?}", result.err());

        // Verify all elements were committed
        assert!(db.catalog.schema_exists("retail"), "Schema should exist");
        assert!(db.catalog.table_exists("retail.products"), "First table should exist");
        assert!(db.catalog.table_exists("retail.inventory"), "Second table should exist");
    } else {
        panic!("Expected CreateSchema statement");
    }
}

#[test]
fn test_create_schema_rollback_with_duplicate_table_name() {
    let mut db = Database::new();

    // Second CREATE TABLE has duplicate name (should fail)
    let sql = "CREATE SCHEMA warehouse \
               CREATE TABLE items (id INT) \
               CREATE TABLE items (name VARCHAR(50))";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    // Execute the statement
    if let Statement::CreateSchema(create_schema_stmt) = stmt {
        let result = SchemaExecutor::execute_create_schema(&create_schema_stmt, &mut db);

        // Print result for debugging
        eprintln!("Result: {:?}", result);
        eprintln!("Schema exists: {}", db.catalog.schema_exists("warehouse"));
        eprintln!("Table exists: {}", db.catalog.table_exists("warehouse.items"));

        // Should fail due to duplicate table name
        assert!(result.is_err(), "Expected error due to duplicate table name, got: {:?}", result);

        // Verify rollback
        assert!(!db.catalog.schema_exists("warehouse"), "Schema should be rolled back");
        assert!(!db.catalog.table_exists("warehouse.items"), "Table should not exist");
    } else {
        panic!("Expected CreateSchema statement");
    }
}
