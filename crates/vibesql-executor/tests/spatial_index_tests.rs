//! Integration tests for spatial indexes

use vibesql_ast::{ColumnDef, CreateIndexStmt, CreateTableStmt, IndexColumn, OrderDirection};
use vibesql_executor::{CreateIndexExecutor, CreateTableExecutor, DropIndexExecutor};
use vibesql_storage::Database;
use vibesql_types::DataType;

#[test]
fn test_create_spatial_index_basic() {
    let mut db = Database::new();

    // Create table with geometry column
    let create_table_stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "places".to_string(),
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
                data_type: DataType::Varchar { max_length: Some(1000) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };

    CreateTableExecutor::execute(&create_table_stmt, &mut db).unwrap();

    // Create spatial index
    let create_index_stmt = CreateIndexStmt {
        index_name: "idx_location".to_string(),
        if_not_exists: false,
        table_name: "places".to_string(),
        index_type: vibesql_ast::IndexType::Spatial,
        columns: vec![IndexColumn {
            column_name: "location".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    };

    let result = CreateIndexExecutor::execute(&create_index_stmt, &mut db);
    assert!(result.is_ok(), "Failed to create spatial index: {:?}", result.err());
    assert!(db.spatial_index_exists("idx_location"));
}

#[test]
fn test_spatial_index_multiple_columns_error() {
    let mut db = Database::new();

    // Create table
    let create_table_stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "places".to_string(),
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
                name: "location1".to_string(),
                data_type: DataType::Varchar { max_length: Some(1000) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "location2".to_string(),
                data_type: DataType::Varchar { max_length: Some(1000) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };

    CreateTableExecutor::execute(&create_table_stmt, &mut db).unwrap();

    // Try to create spatial index on multiple columns (should fail)
    let create_index_stmt = CreateIndexStmt {
        index_name: "idx_location".to_string(),
        if_not_exists: false,
        table_name: "places".to_string(),
        index_type: vibesql_ast::IndexType::Spatial,
        columns: vec![
            IndexColumn {
                column_name: "location1".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "location2".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    };

    let result = CreateIndexExecutor::execute(&create_index_stmt, &mut db);
    assert!(result.is_err(), "Expected error for multiple columns");
}

#[test]
fn test_drop_spatial_index() {
    let mut db = Database::new();

    // Create table with geometry column
    let create_table_stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "places".to_string(),
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
                data_type: DataType::Varchar { max_length: Some(1000) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };

    CreateTableExecutor::execute(&create_table_stmt, &mut db).unwrap();

    // Create spatial index
    let create_index_stmt = CreateIndexStmt {
        index_name: "idx_location".to_string(),
        if_not_exists: false,
        table_name: "places".to_string(),
        index_type: vibesql_ast::IndexType::Spatial,
        columns: vec![IndexColumn {
            column_name: "location".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    };

    CreateIndexExecutor::execute(&create_index_stmt, &mut db).unwrap();
    assert!(db.spatial_index_exists("idx_location"));

    // Drop spatial index
    let drop_stmt =
        vibesql_ast::DropIndexStmt { index_name: "idx_location".to_string(), if_exists: false };

    let result = DropIndexExecutor::execute(&drop_stmt, &mut db);
    assert!(result.is_ok(), "Failed to drop spatial index: {:?}", result.err());
    assert!(!db.spatial_index_exists("idx_location"));
}

#[test]
fn test_spatial_index_if_not_exists() {
    let mut db = Database::new();

    // Create table
    let create_table_stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "places".to_string(),
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
                data_type: DataType::Varchar { max_length: Some(1000) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };

    CreateTableExecutor::execute(&create_table_stmt, &mut db).unwrap();

    // Create spatial index
    let create_index_stmt = CreateIndexStmt {
        index_name: "idx_location".to_string(),
        if_not_exists: false,
        table_name: "places".to_string(),
        index_type: vibesql_ast::IndexType::Spatial,
        columns: vec![IndexColumn {
            column_name: "location".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    };

    CreateIndexExecutor::execute(&create_index_stmt, &mut db).unwrap();

    // Try to create again with IF NOT EXISTS (should succeed)
    let create_index_stmt2 = CreateIndexStmt {
        index_name: "idx_location".to_string(),
        if_not_exists: true,
        table_name: "places".to_string(),
        index_type: vibesql_ast::IndexType::Spatial,
        columns: vec![IndexColumn {
            column_name: "location".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    };

    let result = CreateIndexExecutor::execute(&create_index_stmt2, &mut db);
    assert!(result.is_ok(), "IF NOT EXISTS should succeed");
}
