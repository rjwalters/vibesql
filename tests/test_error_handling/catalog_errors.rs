//! Catalog error tests
//!
//! Tests for catalog-level errors including table, column, schema, and view operations.

use vibesql_ast::Statement;
use vibesql_executor::{
    advanced_objects::{execute_create_view, execute_drop_view},
    AlterTableExecutor, CreateTableExecutor, ExecutorError, SchemaExecutor, SelectExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[test]
fn test_table_already_exists_error() {
    let mut db = Database::new();

    // Create table first time
    let sql = "CREATE TABLE users (id INTEGER)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("First create should succeed");
    }

    // Try to create same table again
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        let result = CreateTableExecutor::execute(&create_stmt, &mut db);
        assert!(result.is_err(), "Should fail with TableAlreadyExists");

        if let Err(ExecutorError::TableAlreadyExists(name)) = result {
            assert!(
                name == "USERS" || name == "public.USERS",
                "Expected USERS or public.USERS, got: {}",
                name
            );
            let error_msg = format!("{}", ExecutorError::TableAlreadyExists(name));
            assert!(error_msg.contains("already exists"));
        } else {
            panic!("Expected TableAlreadyExists error, got: {:?}", result);
        }
    }
}

#[test]
fn test_table_not_found_error() {
    let db = Database::new();

    // Try to select from non-existent table
    let sql = "SELECT * FROM nonexistent_table";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Select(select_stmt) = stmt {
        let result = SelectExecutor::new(&db).execute(&select_stmt);
        assert!(result.is_err(), "Should fail with TableNotFound");

        if let Err(ExecutorError::TableNotFound(name)) = result {
            assert_eq!(name, "NONEXISTENT_TABLE");
            let error_msg = format!("{}", ExecutorError::TableNotFound(name));
            assert!(error_msg.contains("not found"));
        } else {
            panic!("Expected TableNotFound error, got: {:?}", result);
        }
    }
}

#[test]
fn test_column_not_found_error() {
    let mut db = Database::new();

    // Create table
    let sql = "CREATE TABLE products (id INTEGER, name VARCHAR(50))";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create should succeed");
    }

    // Try to select non-existent column
    let sql = "SELECT invalid_column FROM products";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::Select(select_stmt) = stmt {
        let result = SelectExecutor::new(&db).execute(&select_stmt);
        assert!(result.is_err(), "Should fail with ColumnNotFound");

        match result {
            Err(ExecutorError::ColumnNotFound {
                column_name,
                table_name,
                searched_tables,
                available_columns,
            }) => {
                let error_msg = format!(
                    "{}",
                    ExecutorError::ColumnNotFound {
                        column_name: column_name.clone(),
                        table_name: table_name.clone(),
                        searched_tables: searched_tables.clone(),
                        available_columns: available_columns.clone(),
                    }
                );
                assert!(error_msg.contains("not found"));
            }
            other => panic!("Expected ColumnNotFound error, got: {:?}", other),
        }
    }
}

#[test]
fn test_column_already_exists_error() {
    let mut db = Database::new();

    // Create table
    let sql = "CREATE TABLE items (id INTEGER, name VARCHAR(50))";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create should succeed");
    }

    // Try to add column that already exists
    let sql = "ALTER TABLE items ADD COLUMN id INTEGER";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::AlterTable(alter_stmt) = stmt {
        let result = AlterTableExecutor::execute(&alter_stmt, &mut db);
        assert!(result.is_err(), "Should fail with ColumnAlreadyExists");

        match result {
            Err(ExecutorError::ColumnAlreadyExists(name)) => {
                let error_msg = format!("{}", ExecutorError::ColumnAlreadyExists(name.clone()));
                assert!(error_msg.contains("already exists"));
            }
            other => panic!("Expected ColumnAlreadyExists error, got: {:?}", other),
        }
    }
}

#[test]
fn test_schema_already_exists_error() {
    let mut db = Database::new();

    // Create schema first time
    let sql = "CREATE SCHEMA myschema";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateSchema(create_stmt) = stmt {
        SchemaExecutor::execute_create_schema(&create_stmt, &mut db)
            .expect("First create should succeed");
    }

    // Try to create same schema again
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateSchema(create_stmt) = stmt {
        let result = SchemaExecutor::execute_create_schema(&create_stmt, &mut db);
        assert!(result.is_err(), "Should fail with SchemaAlreadyExists");

        match result {
            Err(ExecutorError::SchemaAlreadyExists(name)) => {
                let error_msg = format!("{}", ExecutorError::SchemaAlreadyExists(name.clone()));
                assert!(error_msg.contains("already exists"));
            }
            Err(ExecutorError::StorageError(msg)) if msg.contains("SchemaAlreadyExists") => {
                assert!(msg.contains("MYSCHEMA"));
            }
            other => panic!("Expected SchemaAlreadyExists error, got: {:?}", other),
        }
    }
}

#[test]
fn test_schema_not_found_error() {
    let mut db = Database::new();

    // Try to drop non-existent schema
    let sql = "DROP SCHEMA nonexistent_schema";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::DropSchema(drop_stmt) = stmt {
        let result = SchemaExecutor::execute_drop_schema(&drop_stmt, &mut db);
        assert!(result.is_err(), "Should fail with SchemaNotFound");

        match result {
            Err(ExecutorError::SchemaNotFound(name)) => {
                let error_msg = format!("{}", ExecutorError::SchemaNotFound(name.clone()));
                assert!(error_msg.contains("not found"));
            }
            Err(ExecutorError::StorageError(msg)) if msg.contains("SchemaNotFound") => {
                assert!(msg.contains("NONEXISTENT_SCHEMA"));
            }
            other => panic!("Expected SchemaNotFound error, got: {:?}", other),
        }
    }
}

#[test]
fn test_schema_not_empty_error() {
    let mut db = Database::new();

    // Create schema
    let sql = "CREATE SCHEMA testschema";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateSchema(create_stmt) = stmt {
        SchemaExecutor::execute_create_schema(&create_stmt, &mut db)
            .expect("Create should succeed");
    }

    // Create table in schema
    let sql = "CREATE TABLE testschema.items (id INTEGER)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create table should succeed");
    }

    // Try to drop non-empty schema
    let sql = "DROP SCHEMA testschema";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::DropSchema(drop_stmt) = stmt {
        let result = SchemaExecutor::execute_drop_schema(&drop_stmt, &mut db);
        assert!(result.is_err(), "Should fail with SchemaNotEmpty");

        match result {
            Err(ExecutorError::SchemaNotEmpty(name)) => {
                let error_msg = format!("{}", ExecutorError::SchemaNotEmpty(name.clone()));
                assert!(error_msg.contains("not empty"));
            }
            Err(ExecutorError::StorageError(msg)) if msg.contains("SchemaNotEmpty") => {
                assert!(msg.contains("TESTSCHEMA"));
            }
            other => panic!("Expected SchemaNotEmpty error, got: {:?}", other),
        }
    }
}

#[test]
fn test_view_already_exists_error() {
    let mut db = Database::new();

    // Create table for view
    let sql = "CREATE TABLE base_table (id INTEGER)";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, &mut db).expect("Create should succeed");
    }

    // Create view first time
    let sql = "CREATE VIEW myview AS SELECT * FROM base_table";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateView(create_stmt) = stmt {
        execute_create_view(&create_stmt, &mut db).expect("First create should succeed");
    }

    // Try to create same view again
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::CreateView(create_stmt) = stmt {
        let result = execute_create_view(&create_stmt, &mut db);
        assert!(result.is_err(), "Should fail with ViewAlreadyExists");

        // Error is wrapped in ExecutorError::Other
        match result {
            Err(ExecutorError::Other(msg)) if msg.contains("already exists") => {
                assert!(msg.contains("View"));
            }
            other => panic!("Expected ViewAlreadyExists error, got: {:?}", other),
        }
    }
}

#[test]
fn test_view_not_found_error() {
    let mut db = Database::new();

    // Try to drop non-existent view
    let sql = "DROP VIEW nonexistent_view";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    if let Statement::DropView(drop_stmt) = stmt {
        let result = execute_drop_view(&drop_stmt, &mut db);
        assert!(result.is_err(), "Should fail with ViewNotFound");

        match result {
            Err(ExecutorError::Other(msg)) if msg.contains("not found") => {
                assert!(msg.contains("View"));
            }
            other => panic!("Expected ViewNotFound error, got: {:?}", other),
        }
    }
}
