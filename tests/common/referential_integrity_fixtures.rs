//! Shared fixtures and utilities for referential integrity (foreign key) testing
//!
//! This module provides reusable functions for:
//! - Creating tables with referential integrity constraints
//! - Inserting test data with foreign key relationships
//! - Executing DELETE statements
//! - Asserting constraint violation behaviors

#![allow(dead_code)]

use vibesql_ast::Expression;
use vibesql_catalog::{ColumnSchema, ForeignKeyConstraint, ReferentialAction, TableSchema};
use vibesql_executor::{DeleteExecutor, UpdateExecutor};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

// ========================================================================
// Table Creation Helpers
// ========================================================================

/// Create a standard parent table with a primary key
///
/// Creates a table with columns: ID (INTEGER, PK), NAME (VARCHAR(50), nullable)
pub fn create_parent_table(db: &mut Database, table_name: &str) {
    let schema = TableSchema::with_primary_key(
        table_name.to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("NAME".to_string(), DataType::Varchar { max_length: Some(50) }, true),
        ],
        vec!["ID".to_string()],
    );
    db.create_table(schema).unwrap();
}

/// Create a child table with a foreign key constraint to a parent table
///
/// Creates a table with columns: ID (INTEGER, PK), PARENT_ID (INTEGER, nullable, FK), DATA
/// (VARCHAR(50), nullable)
pub fn create_child_table(
    db: &mut Database,
    table_name: &str,
    parent_table: &str,
    on_delete: ReferentialAction,
    on_update: ReferentialAction,
) {
    let columns = vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("PARENT_ID".to_string(), DataType::Integer, true),
        ColumnSchema::new("DATA".to_string(), DataType::Varchar { max_length: Some(50) }, true),
    ];

    let fk = ForeignKeyConstraint {
        name: Some(format!("FK_{}_{}", table_name, parent_table)),
        column_names: vec!["PARENT_ID".to_string()],
        column_indices: vec![1],
        parent_table: parent_table.to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: on_delete.clone(),
        on_update: on_update.clone(),
    };

    let mut schema =
        TableSchema::with_primary_key(table_name.to_string(), columns, vec!["ID".to_string()]);
    schema.foreign_keys.push(fk);

    db.create_table(schema).unwrap();
}

/// Create a child table with a foreign key constraint and a default value for the FK column
///
/// Creates a table with columns: ID (INTEGER, PK), PARENT_ID (INTEGER, nullable, FK with default), DATA
/// (VARCHAR(50), nullable)
///
/// This is specifically for testing SET DEFAULT referential action.
pub fn create_child_table_with_default(
    db: &mut Database,
    table_name: &str,
    parent_table: &str,
    on_delete: ReferentialAction,
    on_update: ReferentialAction,
    default_value: i64,
) {
    let mut parent_id_column = ColumnSchema::new("PARENT_ID".to_string(), DataType::Integer, true);
    parent_id_column.set_default(Expression::Literal(SqlValue::Integer(default_value)));

    let columns = vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        parent_id_column,
        ColumnSchema::new("DATA".to_string(), DataType::Varchar { max_length: Some(50) }, true),
    ];

    let fk = ForeignKeyConstraint {
        name: Some(format!("FK_{}_{}", table_name, parent_table)),
        column_names: vec!["PARENT_ID".to_string()],
        column_indices: vec![1],
        parent_table: parent_table.to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: on_delete.clone(),
        on_update: on_update.clone(),
    };

    let mut schema =
        TableSchema::with_primary_key(table_name.to_string(), columns, vec!["ID".to_string()]);
    schema.foreign_keys.push(fk);

    db.create_table(schema).unwrap();
}

/// Create a self-referential employee table (employees with manager references)
///
/// Creates a table with columns: ID (INTEGER, PK), MANAGER_ID (INTEGER, nullable, self-FK), NAME
/// (VARCHAR(50))
pub fn create_self_referential_employee_table(db: &mut Database) {
    let columns = vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("MANAGER_ID".to_string(), DataType::Integer, true),
        ColumnSchema::new("NAME".to_string(), DataType::Varchar { max_length: Some(50) }, false),
    ];

    let fk = ForeignKeyConstraint {
        name: Some("FK_EMPLOYEE_MANAGER".to_string()),
        column_names: vec!["MANAGER_ID".to_string()],
        column_indices: vec![1],
        parent_table: "EMPLOYEE".to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
    };

    let mut schema =
        TableSchema::with_primary_key("EMPLOYEE".to_string(), columns, vec!["ID".to_string()]);
    schema.foreign_keys.push(fk);
    db.create_table(schema).unwrap();
}

/// Create parent and child tables for multi-column foreign key testing
///
/// Parent: DEPT_ID (INTEGER), EMP_ID (INTEGER), NAME (VARCHAR), composite PK on (DEPT_ID, EMP_ID)
/// Child: ID (INTEGER, PK), PARENT_DEPT_ID (INTEGER), PARENT_EMP_ID (INTEGER), multi-column FK
pub fn create_multi_column_parent_child_tables(db: &mut Database) {
    // Create parent with composite primary key
    let parent_schema = TableSchema::with_primary_key(
        "PARENT".to_string(),
        vec![
            ColumnSchema::new("DEPT_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("EMP_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
        vec!["DEPT_ID".to_string(), "EMP_ID".to_string()],
    );
    db.create_table(parent_schema).unwrap();

    // Create child with multi-column foreign key
    let columns = vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("PARENT_DEPT_ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("PARENT_EMP_ID".to_string(), DataType::Integer, false),
    ];

    let fk = ForeignKeyConstraint {
        name: Some("FK_MULTI".to_string()),
        column_names: vec!["PARENT_DEPT_ID".to_string(), "PARENT_EMP_ID".to_string()],
        column_indices: vec![1, 2],
        parent_table: "PARENT".to_string(),
        parent_column_names: vec!["DEPT_ID".to_string(), "EMP_ID".to_string()],
        parent_column_indices: vec![0, 1],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
    };

    let mut schema =
        TableSchema::with_primary_key("CHILD".to_string(), columns, vec!["ID".to_string()]);
    schema.foreign_keys.push(fk);
    db.create_table(schema).unwrap();
}

/// Create setup for testing shared child with multiple parent tables
///
/// Creates: PARENT1, PARENT2 (standard parent tables), and CHILD with FKs to both
pub fn create_multiple_parent_child_tables(db: &mut Database) {
    // Create two parent tables
    create_parent_table(db, "PARENT1");
    create_parent_table(db, "PARENT2");

    // Create child table with FKs to both parents
    let columns = vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("PARENT1_ID".to_string(), DataType::Integer, true),
        ColumnSchema::new("PARENT2_ID".to_string(), DataType::Integer, true),
    ];

    let fk1 = ForeignKeyConstraint {
        name: Some("FK_CHILD_PARENT1".to_string()),
        column_names: vec!["PARENT1_ID".to_string()],
        column_indices: vec![1],
        parent_table: "PARENT1".to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
    };

    let fk2 = ForeignKeyConstraint {
        name: Some("FK_CHILD_PARENT2".to_string()),
        column_names: vec!["PARENT2_ID".to_string()],
        column_indices: vec![2],
        parent_table: "PARENT2".to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
    };

    let mut schema =
        TableSchema::with_primary_key("CHILD".to_string(), columns, vec!["ID".to_string()]);
    schema.foreign_keys.push(fk1);
    schema.foreign_keys.push(fk2);
    db.create_table(schema).unwrap();
}

// ========================================================================
// Data Insertion Helpers
// ========================================================================

/// Insert a standard parent row (ID, NAME)
pub fn insert_parent_row(db: &mut Database, table: &str, id: i64, name: &str) {
    db.insert_row(
        table,
        Row::new(vec![SqlValue::Integer(id), SqlValue::Varchar(name.to_string())]),
    )
    .unwrap();
}

/// Insert a standard child row (ID, PARENT_ID, DATA)
pub fn insert_child_row(db: &mut Database, table: &str, id: i64, parent_id: i64, data: &str) {
    db.insert_row(
        table,
        Row::new(vec![
            SqlValue::Integer(id),
            SqlValue::Integer(parent_id),
            SqlValue::Varchar(data.to_string()),
        ]),
    )
    .unwrap();
}

/// Insert a child row with NULL foreign key (ID, NULL, DATA)
pub fn insert_child_row_null_fk(db: &mut Database, table: &str, id: i64, data: &str) {
    db.insert_row(
        table,
        Row::new(vec![SqlValue::Integer(id), SqlValue::Null, SqlValue::Varchar(data.to_string())]),
    )
    .unwrap();
}

// ========================================================================
// Execution and Assertion Helpers
// ========================================================================

/// Execute a DELETE statement and return the number of rows deleted (or error)
pub fn execute_delete(db: &mut Database, sql: &str) -> Result<usize, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        vibesql_ast::Statement::Delete(delete_stmt) => DeleteExecutor::execute(&delete_stmt, db)
            .map_err(|e| format!("Execution error: {:?}", e)),
        other => Err(format!("Expected DELETE statement, got {:?}", other)),
    }
}

/// Execute an UPDATE statement and return the number of rows updated (or error)
pub fn execute_update(db: &mut Database, sql: &str) -> Result<usize, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        vibesql_ast::Statement::Update(update_stmt) => UpdateExecutor::execute(&update_stmt, db)
            .map_err(|e| format!("Execution error: {:?}", e)),
        other => Err(format!("Expected UPDATE statement, got {:?}", other)),
    }
}

/// Assert that a DELETE operation failed with a foreign key constraint violation
pub fn assert_fk_violation(result: Result<usize, String>, expected_error_contains: &str) {
    assert!(result.is_err(), "Expected FK constraint violation, but DELETE succeeded");
    let err = result.unwrap_err();
    assert!(
        err.contains("FOREIGN KEY constraint violation"),
        "Error message should contain 'FOREIGN KEY constraint violation', got: {}",
        err
    );
    if !expected_error_contains.is_empty() {
        assert!(
            err.contains(expected_error_contains),
            "Error message should contain '{}', got: {}",
            expected_error_contains,
            err
        );
    }
}

/// Assert that a DELETE operation succeeded and deleted the expected number of rows
pub fn assert_successful_delete(result: Result<usize, String>, expected_count: usize) {
    match result {
        Ok(count) => assert_eq!(
            count, expected_count,
            "Expected to delete {} rows, deleted {}",
            expected_count, count
        ),
        Err(e) => panic!("DELETE should have succeeded but failed with: {}", e),
    }
}

/// Assert that an UPDATE operation succeeded and updated the expected number of rows
pub fn assert_successful_update(result: Result<usize, String>, expected_count: usize) {
    match result {
        Ok(count) => assert_eq!(
            count, expected_count,
            "Expected to update {} rows, updated {}",
            expected_count, count
        ),
        Err(e) => panic!("UPDATE should have succeeded but failed with: {}", e),
    }
}
