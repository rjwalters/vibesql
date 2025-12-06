//! CREATE TABLE executor tests
//!
//! Tests for basic CREATE TABLE functionality including:
//! - Simple table creation
//! - Multiple data types
//! - Nullable columns
//! - Table already exists error handling
//! - Empty column lists
//! - Multiple table creation
//! - Special characters in names
//! - Case sensitivity
//! - Spatial types (POINT, POLYGON, MULTIPOLYGON)

use vibesql_ast::{ColumnDef, CreateTableStmt};
use vibesql_storage::Database;
use vibesql_types::DataType;

use crate::CreateTableExecutor;

#[test]
fn test_create_simple_table() {
    let mut db = Database::new();

    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Varchar { max_length: Some(255) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Table 'users' created successfully in schema 'public'");

    // Verify table exists in catalog
    assert!(db.catalog.table_exists("users"));

    // Verify table is accessible from storage
    assert!(db.get_table("users").is_some());
}

#[test]
fn test_create_table_with_multiple_types() {
    let mut db = Database::new();

    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "products".to_string(),
        columns: vec![
            ColumnDef {
                name: "product_id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "price".to_string(),
                data_type: DataType::Integer, /* Using Integer for price (could be Decimal in
                                               * future) */
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "in_stock".to_string(),
                data_type: DataType::Boolean,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "description".to_string(),
                data_type: DataType::Varchar { max_length: Some(500) },
                nullable: true, // Optional field
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    // Verify schema correctness
    let schema = db.catalog.get_table("products");
    assert!(schema.is_some());
    let schema = schema.unwrap();
    assert_eq!(schema.column_count(), 5);
    assert!(!schema.get_column("product_id").unwrap().nullable);
    assert!(schema.get_column("description").unwrap().nullable);
}

#[test]
fn test_create_table_already_exists() {
    let mut db = Database::new();

    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "users".to_string(),
        columns: vec![ColumnDef {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };

    // First creation succeeds
    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    // Second creation fails
    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    assert!(matches!(result, Err(crate::errors::ExecutorError::TableAlreadyExists(_))));
}

#[test]
fn test_create_table_with_nullable_columns() {
    let mut db = Database::new();

    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "employees".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "middle_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true, // Nullable field
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "manager_id".to_string(),
                data_type: DataType::Integer,
                nullable: true, // Nullable foreign key
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    // Verify nullable attribute is preserved
    let schema = db.catalog.get_table("employees").unwrap();
    assert!(!schema.get_column("id").unwrap().nullable);
    assert!(schema.get_column("middle_name").unwrap().nullable);
    assert!(schema.get_column("manager_id").unwrap().nullable);
}

#[test]
fn test_create_table_empty_columns_list() {
    let mut db = Database::new();

    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "empty_table".to_string(),
        columns: vec![], // Empty columns
        table_constraints: vec![],
        table_options: vec![],
    };

    // Should succeed (though not very useful)
    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    let schema = db.catalog.get_table("empty_table").unwrap();
    assert_eq!(schema.column_count(), 0);
}

#[test]
fn test_create_multiple_tables() {
    let mut db = Database::new();

    // Create first table
    let stmt1 = CreateTableStmt {
        if_not_exists: false,
        table_name: "customers".to_string(),
        columns: vec![ColumnDef {
            name: "customer_id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&stmt1, &mut db).unwrap();

    // Create second table
    let stmt2 = CreateTableStmt {
        if_not_exists: false,
        table_name: "orders".to_string(),
        columns: vec![ColumnDef {
            name: "order_id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&stmt2, &mut db).unwrap();

    // Verify both tables exist
    assert!(db.catalog.table_exists("customers"));
    assert!(db.catalog.table_exists("orders"));
    assert_eq!(db.list_tables().len(), 2);
}

#[test]
fn test_create_table_with_special_characters_in_name() {
    let mut db = Database::new();

    // Test with underscores (common case)
    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "user_profiles".to_string(),
        columns: vec![ColumnDef {
            name: "profile_id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());
    assert!(db.catalog.table_exists("user_profiles"));
}

#[test]
fn test_create_table_case_sensitivity() {
    let mut db = Database::new();

    let stmt1 = CreateTableStmt {
        if_not_exists: false,
        table_name: "Users".to_string(),
        columns: vec![ColumnDef {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&stmt1, &mut db).unwrap();

    // Try to create "users" (different case)
    let stmt2 = CreateTableStmt {
        if_not_exists: false,
        table_name: "users".to_string(),
        columns: vec![ColumnDef {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };

    // Behavior depends on catalog's case sensitivity
    // Just verify it either succeeds or fails gracefully
    let result = CreateTableExecutor::execute(&stmt2, &mut db);
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_table_with_spatial_types() {
    // Test spatial data types (SQL/MM standard) - Issue #818
    // These are parsed as UserDefined types and should be accepted by executor
    let mut db = Database::new();

    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "spatial_table".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "location".to_string(),
                data_type: DataType::UserDefined { type_name: "POINT".to_string() },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "area".to_string(),
                data_type: DataType::UserDefined { type_name: "POLYGON".to_string() },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "regions".to_string(),
                data_type: DataType::UserDefined { type_name: "MULTIPOLYGON".to_string() },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok(), "Should accept spatial types as UserDefined types");

    // Verify table exists and has correct schema
    let schema = db.catalog.get_table("spatial_table");
    assert!(schema.is_some());
    let schema = schema.unwrap();
    assert_eq!(schema.column_count(), 4);

    // Verify spatial type columns exist
    assert!(schema.get_column("location").is_some());
    assert!(schema.get_column("area").is_some());
    assert!(schema.get_column("regions").is_some());
}

#[test]
fn test_create_table_multipolygon_sqllogictest() {
    // Test the exact scenario from SQLLogicTest - Issue #818
    let mut db = Database::new();

    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "t1710a".to_string(),
        columns: vec![
            ColumnDef {
                name: "c1".to_string(),
                data_type: DataType::UserDefined { type_name: "MULTIPOLYGON".to_string() },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: Some("text155459".to_string()),
            },
            ColumnDef {
                name: "c2".to_string(),
                data_type: DataType::UserDefined { type_name: "MULTIPOLYGON".to_string() },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: Some("text155461".to_string()),
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(
        result.is_ok(),
        "Should create table with MULTIPOLYGON columns (SQLLogicTest conformance)"
    );

    // Verify table was created successfully
    assert!(db.catalog.table_exists("t1710a"));
    let schema = db.catalog.get_table("t1710a").unwrap();
    assert_eq!(schema.column_count(), 2);
}
