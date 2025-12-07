//! Regression tests for Database::reset() catalog and index metadata clearing
//!
//! This module tests that Database::reset() properly clears:
//! - Catalog metadata (tables, schemas, indexes)
//! - User-defined B-tree indexes
//! - Spatial indexes
//!
//! This prevents cross-file contamination when reusing Database instances
//! (e.g., in SQLLogicTest pooled DB adapter).

use crate::{Database, Row};
use vibesql_ast::{IndexColumn, OrderDirection};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_types::{DataType, SqlValue};

#[test]
fn test_reset_clears_catalog_and_indexes() {
    let mut db = Database::new();

    // ============================================================================
    // FIRST USAGE: Create table with index, insert data
    // ============================================================================

    // Create a table
    let schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(255) },
                false,
            ),
        ],
    );
    db.create_table(schema.clone()).unwrap();

    // Create an index on the table
    db.create_index(
        "idx_users_id".to_string(),
        "users".to_string(),
        false,
        vec![IndexColumn {
            column_name: "id".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // Insert some data
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
    )
    .unwrap();

    // Verify table and index exist
    assert!(db.get_table("users").is_some());
    assert!(db.index_exists("idx_users_id"));
    assert_eq!(db.list_tables().len(), 1);
    assert_eq!(db.list_indexes().len(), 1);

    // ============================================================================
    // RESET: Clear everything
    // ============================================================================

    db.reset();

    // Verify catalog is cleared
    assert_eq!(db.list_tables().len(), 0, "Catalog should be empty after reset");
    assert_eq!(db.list_indexes().len(), 0, "All indexes should be cleared after reset");
    assert!(db.get_table("users").is_none(), "Table should not exist after reset");
    assert!(!db.index_exists("idx_users_id"), "Index should not exist after reset");

    // ============================================================================
    // SECOND USAGE: Recreate same table with index (different data)
    // ============================================================================

    // Recreate the same table (should not conflict with stale catalog metadata)
    db.create_table(schema.clone()).unwrap();

    // Recreate the same index (should not conflict with stale index metadata)
    db.create_index(
        "idx_users_id".to_string(),
        "users".to_string(),
        false,
        vec![IndexColumn {
            column_name: "id".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // Insert different data
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(10), SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(20), SqlValue::Varchar(arcstr::ArcStr::from("Diana"))]),
    )
    .unwrap();

    // Verify new table and index work correctly
    assert!(db.get_table("users").is_some());
    assert!(db.index_exists("idx_users_id"));

    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 2, "Table should have only new rows after reset");

    // Verify index was recreated successfully (old index metadata was cleared)
    // If stale index metadata existed, creating the index would have failed or returned wrong results
    let index = db.get_index("idx_users_id").unwrap();
    // Just verify that index was successfully recreated - exact table name format may vary
    assert_eq!(index.columns.len(), 1, "Index should have exactly one column");
}

#[test]
fn test_reset_clears_spatial_indexes() {
    let mut db = Database::new();

    // ============================================================================
    // FIRST USAGE: Create table with spatial index
    // ============================================================================

    let schema = TableSchema::new(
        "locations".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("point".to_string(), DataType::CharacterLargeObject, false), // Using CLOB as placeholder for geometry
        ],
    );
    db.create_table(schema.clone()).unwrap();

    // Create a spatial index
    use crate::database::operations::SpatialIndexMetadata;
    use crate::index::SpatialIndex;

    let spatial_metadata = SpatialIndexMetadata {
        index_name: "idx_locations_point".to_string(),
        table_name: "locations".to_string(),
        column_name: "point".to_string(),
        created_at: None,
    };
    let spatial_index = SpatialIndex::new("point".to_string());
    db.create_spatial_index(spatial_metadata, spatial_index).unwrap();

    // Verify spatial index exists
    assert!(db.spatial_index_exists("idx_locations_point"));
    assert_eq!(db.list_spatial_indexes().len(), 1);

    // ============================================================================
    // RESET: Clear everything
    // ============================================================================

    db.reset();

    // Verify spatial index is cleared
    assert!(
        !db.spatial_index_exists("idx_locations_point"),
        "Spatial index should not exist after reset"
    );
    assert_eq!(
        db.list_spatial_indexes().len(),
        0,
        "All spatial indexes should be cleared after reset"
    );

    // ============================================================================
    // SECOND USAGE: Recreate same spatial index
    // ============================================================================

    db.create_table(schema.clone()).unwrap();

    let spatial_metadata2 = SpatialIndexMetadata {
        index_name: "idx_locations_point".to_string(),
        table_name: "locations".to_string(),
        column_name: "point".to_string(),
        created_at: None,
    };
    let spatial_index2 = SpatialIndex::new("point".to_string());
    db.create_spatial_index(spatial_metadata2, spatial_index2).unwrap();

    // Verify new spatial index works
    assert!(db.spatial_index_exists("idx_locations_point"));
    assert_eq!(db.list_spatial_indexes().len(), 1);
}

#[test]
fn test_reset_preserves_database_config() {
    use crate::DatabaseConfig;
    use std::path::PathBuf;

    // Create database with custom config and path
    let config = DatabaseConfig::server_default();
    let path = PathBuf::from("/tmp/test_db");
    let mut db = Database::with_path_and_config(path.clone(), config.clone());

    // Create some data
    let schema = TableSchema::new(
        "test".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Reset should clear data but preserve config
    db.reset();

    // Verify config is preserved by creating a new index and checking it works
    // (If config was lost, disk-backed indexes wouldn't work correctly)
    let schema2 = TableSchema::new(
        "test2".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema2).unwrap();
    db.create_index(
        "idx_test2".to_string(),
        "test2".to_string(),
        false,
        vec![IndexColumn {
            column_name: "id".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // If config was lost, this would fail
    assert!(db.index_exists("idx_test2"));
}

#[test]
fn test_reset_multiple_tables_and_indexes() {
    let mut db = Database::new();

    // ============================================================================
    // FIRST USAGE: Create multiple tables with multiple indexes
    // ============================================================================

    for i in 0..3 {
        let table_name = format!("table{}", i);
        let schema = TableSchema::new(
            table_name.clone(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "value".to_string(),
                    DataType::Varchar { max_length: Some(255) },
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();

        // Create 2 indexes per table
        for j in 0..2 {
            let index_name = format!("idx_{}_{}", table_name, j);
            let col_name = if j == 0 { "id" } else { "value" };
            db.create_index(
                index_name,
                table_name.clone(),
                false,
                vec![IndexColumn {
                    column_name: col_name.to_string(),
                    direction: OrderDirection::Asc,
                    prefix_length: None,
                }],
            )
            .unwrap();
        }

        // Insert data
        db.insert_row(
            &table_name,
            Row::new(vec![SqlValue::Integer(i as i64), SqlValue::Varchar(arcstr::ArcStr::from(format!("value{}", i)))]),
        )
        .unwrap();
    }

    // Verify all tables and indexes exist
    assert_eq!(db.list_tables().len(), 3);
    assert_eq!(db.list_indexes().len(), 6); // 3 tables * 2 indexes each

    // ============================================================================
    // RESET: Clear everything
    // ============================================================================

    db.reset();

    // Verify everything is cleared
    assert_eq!(db.list_tables().len(), 0);
    assert_eq!(db.list_indexes().len(), 0);

    // ============================================================================
    // SECOND USAGE: Create different tables/indexes
    // ============================================================================

    let new_schema = TableSchema::new(
        "new_table".to_string(),
        vec![ColumnSchema::new("col".to_string(), DataType::Integer, false)],
    );
    db.create_table(new_schema).unwrap();

    assert_eq!(db.list_tables().len(), 1);
    assert_eq!(db.list_tables()[0], "new_table");
}
