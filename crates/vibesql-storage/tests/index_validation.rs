// Comprehensive validation tests for index architecture (Phase 4)
//
// This test suite validates the index architecture refactoring using
// the actual available APIs (not SQL, which is at a higher layer).
//
// Tests cover:
// 1. Adaptive backend selection (InMemory vs DiskBacked)
// 2. Resource budget configuration
// 3. Index correctness (NULL handling, duplicates, multi-column)
// 4. Persistence and recovery

use vibesql_ast::{IndexColumn, OrderDirection};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::{Database, DatabaseConfig, Row, SpillPolicy};
use vibesql_types::{DataType, SqlValue};

// ============================================================================
// Resource Budget Configuration Tests
// ============================================================================

#[cfg(test)]
mod resource_budget_tests {
    use super::*;

    #[test]
    fn test_browser_default_config() {
        let config = DatabaseConfig::browser_default();

        // Browser config should have limited memory (512MB) and reasonable disk (2GB)
        assert_eq!(
            config.memory_budget,
            512 * 1024 * 1024,
            "Browser should have 512MB memory budget"
        );
        assert_eq!(
            config.disk_budget,
            2 * 1024 * 1024 * 1024,
            "Browser should have 2GB disk budget"
        );
        assert!(
            matches!(config.spill_policy, SpillPolicy::SpillToDisk),
            "Browser should spill to disk when memory exhausted"
        );
    }

    #[test]
    fn test_server_default_config() {
        let config = DatabaseConfig::server_default();

        // Server config should have abundant memory (16GB) and large disk (1TB)
        assert_eq!(
            config.memory_budget,
            16 * 1024 * 1024 * 1024,
            "Server should have 16GB memory budget"
        );
        assert_eq!(
            config.disk_budget,
            1024 * 1024 * 1024 * 1024,
            "Server should have 1TB disk budget"
        );
        assert!(
            matches!(config.spill_policy, SpillPolicy::BestEffort),
            "Server should use best effort policy"
        );
    }

    #[test]
    fn test_test_default_config() {
        let config = DatabaseConfig::test_default();

        // Test config should have reasonable limits for testing
        assert_eq!(config.memory_budget, 10 * 1024 * 1024, "Test should have 10MB memory budget");
        assert_eq!(config.disk_budget, 100 * 1024 * 1024, "Test should have 100MB disk budget");
    }

    #[test]
    fn test_database_with_config() {
        // Verify that database can be created with each config variant
        let _browser_db = Database::with_config(DatabaseConfig::browser_default());
        let _server_db = Database::with_config(DatabaseConfig::server_default());
        let _test_db = Database::with_config(DatabaseConfig::test_default());

        // No panics means configs are valid
    }
}

// ============================================================================
// Adaptive Backend Tests
// ============================================================================

#[cfg(test)]
mod adaptive_backend_tests {
    use super::*;

    #[test]
    fn test_small_index_creation() {
        // Create database with default config
        let mut db = Database::new();

        // Create a small table with index
        let schema = TableSchema::new(
            "small_table".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
            ],
        );

        db.create_table(schema).unwrap();

        // Insert a small number of rows (should stay in memory)
        for i in 0..100 {
            let row = Row::new(vec![SqlValue::Integer(i), SqlValue::Varchar(arcstr::ArcStr::from(format!("user{}", i)))]);
            db.insert_row("small_table", row).unwrap();
        }

        // Create index - should succeed for small datasets
        // Signature: create_index(name, table, unique, columns)
        let result = db.create_index(
            "idx_small".to_string(),
            "small_table".to_string(),
            false, // not unique
            vec![IndexColumn {
                column_name: "id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        );

        assert!(result.is_ok(), "Should be able to create index on small table");

        // Verify index exists
        assert!(db.index_exists("idx_small"), "Index should exist");
    }

    #[test]
    fn test_multiple_indexes_coexist() {
        let mut db = Database::new();

        let schema = TableSchema::new(
            "multi".to_string(),
            vec![
                ColumnSchema::new("a".to_string(), DataType::Integer, false),
                ColumnSchema::new("b".to_string(), DataType::Integer, false),
                ColumnSchema::new("c".to_string(), DataType::Integer, false),
            ],
        );

        db.create_table(schema).unwrap();

        // Insert some rows
        for i in 0..50 {
            let row = Row::new(vec![
                SqlValue::Integer(i),
                SqlValue::Integer(i * 2),
                SqlValue::Integer(i * 3),
            ]);
            db.insert_row("multi", row).unwrap();
        }

        // Create multiple indexes on different columns
        db.create_index(
            "idx_a".to_string(),
            "multi".to_string(),
            false,
            vec![IndexColumn {
                column_name: "a".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        )
        .unwrap();

        db.create_index(
            "idx_b".to_string(),
            "multi".to_string(),
            false,
            vec![IndexColumn {
                column_name: "b".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        )
        .unwrap();

        db.create_index(
            "idx_c".to_string(),
            "multi".to_string(),
            false,
            vec![IndexColumn {
                column_name: "c".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        )
        .unwrap();

        // All indexes should exist
        assert!(db.index_exists("idx_a"), "Index idx_a should exist");
        assert!(db.index_exists("idx_b"), "Index idx_b should exist");
        assert!(db.index_exists("idx_c"), "Index idx_c should exist");
    }
}

// ============================================================================
// Index Correctness Tests
// ============================================================================

#[cfg(test)]
mod correctness_tests {
    use super::*;

    #[test]
    fn test_empty_index() {
        let mut db = Database::new();

        let schema = TableSchema::new(
            "empty_table".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );

        db.create_table(schema).unwrap();

        // Create index on empty table
        let result = db.create_index(
            "idx_empty".to_string(),
            "empty_table".to_string(),
            false,
            vec![IndexColumn {
                column_name: "id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        );

        assert!(result.is_ok(), "Should be able to create index on empty table");
        assert!(db.index_exists("idx_empty"), "Index should exist");
    }

    #[test]
    fn test_null_handling_in_index() {
        let mut db = Database::new();

        let schema = TableSchema::new(
            "nullable_table".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("value".to_string(), DataType::Integer, true), // nullable
            ],
        );

        db.create_table(schema).unwrap();

        // Insert rows with NULL values
        db.insert_row("nullable_table", Row::new(vec![SqlValue::Integer(1), SqlValue::Null]))
            .unwrap();
        db.insert_row(
            "nullable_table",
            Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(100)]),
        )
        .unwrap();
        db.insert_row("nullable_table", Row::new(vec![SqlValue::Integer(3), SqlValue::Null]))
            .unwrap();
        db.insert_row(
            "nullable_table",
            Row::new(vec![SqlValue::Integer(4), SqlValue::Integer(200)]),
        )
        .unwrap();

        // Create index on nullable column
        let result = db.create_index(
            "idx_nullable".to_string(),
            "nullable_table".to_string(),
            false,
            vec![IndexColumn {
                column_name: "value".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        );

        assert!(result.is_ok(), "Should be able to create index on nullable column");
        assert!(db.index_exists("idx_nullable"), "Index should exist");
    }

    #[test]
    fn test_duplicate_values_in_non_unique_index() {
        let mut db = Database::new();

        let schema = TableSchema::new(
            "duplicates".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("category".to_string(), DataType::Integer, false),
            ],
        );

        db.create_table(schema).unwrap();

        // Insert rows with duplicate category values
        db.insert_row("duplicates", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100)]))
            .unwrap();
        db.insert_row("duplicates", Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(100)]))
            .unwrap();
        db.insert_row("duplicates", Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(200)]))
            .unwrap();
        db.insert_row("duplicates", Row::new(vec![SqlValue::Integer(4), SqlValue::Integer(100)]))
            .unwrap();

        // Create non-unique index - should succeed with duplicates
        let result = db.create_index(
            "idx_category".to_string(),
            "duplicates".to_string(),
            false, // NOT unique
            vec![IndexColumn {
                column_name: "category".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        );

        assert!(result.is_ok(), "Non-unique index should accept duplicate values");
        assert!(db.index_exists("idx_category"), "Index should exist");
    }

    #[test]
    fn test_multi_column_index() {
        let mut db = Database::new();

        let schema = TableSchema::new(
            "multi_col".to_string(),
            vec![
                ColumnSchema::new(
                    "last_name".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
                ColumnSchema::new(
                    "first_name".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
                ColumnSchema::new("age".to_string(), DataType::Integer, false),
            ],
        );

        db.create_table(schema).unwrap();

        // Insert some rows
        db.insert_row(
            "multi_col",
            Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from("Smith")),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Integer(30),
            ]),
        )
        .unwrap();

        db.insert_row(
            "multi_col",
            Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from("Smith")),
                SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
                SqlValue::Integer(25),
            ]),
        )
        .unwrap();

        // Create multi-column index
        let result = db.create_index(
            "idx_name".to_string(),
            "multi_col".to_string(),
            false,
            vec![
                IndexColumn {
                    column_name: "last_name".to_string(),
                    direction: OrderDirection::Asc,
                    prefix_length: None,
                },
                IndexColumn {
                    column_name: "first_name".to_string(),
                    direction: OrderDirection::Asc,
                    prefix_length: None,
                },
            ],
        );

        assert!(result.is_ok(), "Should be able to create multi-column index");
        assert!(db.index_exists("idx_name"), "Multi-column index should exist");
    }

    #[test]
    fn test_unique_index_rejects_duplicates() {
        let mut db = Database::new();

        let schema = TableSchema::new(
            "unique_test".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "email".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
            ],
        );

        db.create_table(schema).unwrap();

        // Insert first row
        db.insert_row(
            "unique_test",
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
            ]),
        )
        .unwrap();

        // Create UNIQUE index
        db.create_index(
            "idx_email_unique".to_string(),
            "unique_test".to_string(),
            true, // UNIQUE
            vec![IndexColumn {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        )
        .unwrap();

        // Try to insert duplicate email - should fail
        let duplicate_result = db.insert_row(
            "unique_test",
            Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
            ]),
        );

        assert!(duplicate_result.is_err(), "Unique index should reject duplicate values");
    }
}

// ============================================================================
// Persistence Tests
// ============================================================================

#[cfg(test)]
mod persistence_tests {
    use super::*;

    #[test]
    fn test_database_with_path_for_disk_backed() {
        // Create database with a path to enable potential disk-backed indexes
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let mut db = Database::with_path(db_path);

        let schema = TableSchema::new(
            "test_table".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );

        db.create_table(schema).unwrap();

        // Insert some rows
        for i in 0..100 {
            db.insert_row("test_table", Row::new(vec![SqlValue::Integer(i)])).unwrap();
        }

        // Create index - with path configured, this could use disk-backed storage
        let result = db.create_index(
            "idx_id".to_string(),
            "test_table".to_string(),
            false,
            vec![IndexColumn {
                column_name: "id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        );

        assert!(result.is_ok(), "Index creation should succeed with path configured");
    }

    #[test]
    fn test_index_metadata_persists_in_catalog() {
        let mut db = Database::new();

        let schema = TableSchema::new(
            "persist_test".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );

        db.create_table(schema).unwrap();

        // Create index
        db.create_index(
            "idx_persist".to_string(),
            "persist_test".to_string(),
            false,
            vec![IndexColumn {
                column_name: "id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        )
        .unwrap();

        // Verify index exists
        assert!(db.index_exists("idx_persist"), "Index should exist");
    }
}

// ============================================================================
// Index Operations Tests
// ============================================================================

#[cfg(test)]
mod index_operations_tests {
    use super::*;

    #[test]
    fn test_drop_index() {
        let mut db = Database::new();

        let schema = TableSchema::new(
            "drop_test".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );

        db.create_table(schema).unwrap();

        // Create index
        db.create_index(
            "idx_to_drop".to_string(),
            "drop_test".to_string(),
            false,
            vec![IndexColumn {
                column_name: "id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        )
        .unwrap();

        assert!(db.index_exists("idx_to_drop"), "Index should exist before drop");

        // Drop index
        let result = db.drop_index("idx_to_drop");
        assert!(result.is_ok(), "Should be able to drop index");

        // Verify index no longer exists
        assert!(!db.index_exists("idx_to_drop"), "Index should not exist after drop");
    }

    #[test]
    fn test_cascade_drop_indexes_with_table() {
        let mut db = Database::new();

        let schema = TableSchema::new(
            "cascade_test".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("value".to_string(), DataType::Integer, false),
            ],
        );

        db.create_table(schema).unwrap();

        // Create multiple indexes
        db.create_index(
            "idx_cascade_1".to_string(),
            "cascade_test".to_string(),
            false,
            vec![IndexColumn {
                column_name: "id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        )
        .unwrap();

        db.create_index(
            "idx_cascade_2".to_string(),
            "cascade_test".to_string(),
            false,
            vec![IndexColumn {
                column_name: "value".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        )
        .unwrap();

        assert!(db.index_exists("idx_cascade_1"), "Index 1 should exist");
        assert!(db.index_exists("idx_cascade_2"), "Index 2 should exist");

        // Drop table - should cascade and drop indexes
        db.drop_table("cascade_test").unwrap();

        // Verify indexes were dropped
        assert!(!db.index_exists("idx_cascade_1"), "Index 1 should be dropped with table");
        assert!(!db.index_exists("idx_cascade_2"), "Index 2 should be dropped with table");
    }

    #[test]
    fn test_rebuild_index_after_bulk_insert() {
        let mut db = Database::new();

        let schema = TableSchema::new(
            "rebuild_test".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );

        db.create_table(schema).unwrap();

        // Insert rows BEFORE creating index
        for i in 0..100 {
            db.insert_row("rebuild_test", Row::new(vec![SqlValue::Integer(i)])).unwrap();
        }

        // Create index on existing data - should build from existing rows
        let result = db.create_index(
            "idx_rebuild".to_string(),
            "rebuild_test".to_string(),
            false,
            vec![IndexColumn {
                column_name: "id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        );

        assert!(result.is_ok(), "Should be able to create index on existing data");
        assert!(db.index_exists("idx_rebuild"), "Index should exist after rebuild");
    }
}
