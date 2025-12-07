//! Storage - In-Memory Data Storage
//!
//! This crate provides in-memory storage for database tables and rows.

// Allow approximate constants in tests (e.g., 3.14 for PI) as they are test data values
#![cfg_attr(test, allow(clippy::approx_constant))]

pub mod backend;
pub mod blob;
pub mod btree;
pub mod buffer;
pub mod change_events;
pub mod columnar;
pub mod columnar_cache;
pub mod database;
pub mod error;
pub mod index;
pub mod page;
pub mod persistence;
pub mod progress;
pub mod query_buffer_pool;
pub mod row;
pub mod statistics;
pub mod table;
pub mod wal;

pub use backend::{StorageBackend, StorageFile};
pub use blob::{BlobId, BlobMetadata, BlobStorageConfig, BlobStorageService};
pub use buffer::{BufferPool, BufferPoolStats};
pub use change_events::{
    channel as change_event_channel, ChangeEvent, ChangeEventReceiver, ChangeEventSender,
    RecvError as ChangeEventRecvError, DEFAULT_CHANNEL_CAPACITY,
};
pub use columnar::{ColumnData, ColumnarTable};
pub use columnar_cache::{CacheStats, ColumnarCache};
pub use database::{
    print_delete_profile_summary, reset_delete_profile_stats, Database, DatabaseConfig,
    DeleteProfileStats, IndexData, IndexManager, IndexMetadata, OwnedStreamingRangeScan,
    SpatialIndexMetadata, SpillPolicy, TransactionState, DELETE_PROFILE_STATS,
};
pub use error::{StorageError, StorageResult};
pub use index::{extract_mbr_from_sql_value, SpatialIndex, SpatialIndexEntry};
pub use persistence::load::{parse_sql_statements, read_sql_dump};
pub use query_buffer_pool::{
    QueryBufferPool, QueryBufferPoolStats, RowBufferGuard, ValueBufferGuard,
};
pub use row::{Row, RowValues, ROW_INLINE_CAPACITY};
pub use statistics::{ColumnStatistics, TableIndexInfo, TableStatistics};
pub use table::{DeleteResult, Table};
pub use wal::{
    DurabilityConfig, DurabilityMode, Lsn, PersistenceConfig, PersistenceEngine, PersistenceStats,
    TransactionDurability, WalEntry, WalOp, WalOpTag,
};

// Platform-specific exports
#[cfg(not(target_arch = "wasm32"))]
pub use backend::{NativeFile, NativeStorage};

#[cfg(target_arch = "wasm32")]
pub use backend::{OpfsFile, OpfsStorage};

#[cfg(test)]
mod tests {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    use super::*;
    use crate::Row;

    #[test]
    fn test_hash_indexes_primary_key() {
        let schema = TableSchema::with_primary_key(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
            ],
            vec!["id".to_string()],
        );

        let mut table = Table::new(schema);

        // Insert some rows
        for i in 0..10 {
            let row =
                Row::new(vec![SqlValue::Integer(i), SqlValue::Varchar(arcstr::ArcStr::from(format!("User {}", i)))]);
            table.insert(row).unwrap();
        }

        // Check that primary key index exists and has entries
        assert!(table.primary_key_index().is_some());
        assert_eq!(table.primary_key_index().as_ref().unwrap().len(), 10);

        // Try to insert duplicate - should work at table level (constraint check is in executor)
        let duplicate_row =
            Row::new(vec![SqlValue::Integer(0), SqlValue::Varchar(arcstr::ArcStr::from("Duplicate User"))]);
        table.insert(duplicate_row).unwrap(); // This succeeds because constraint checking is in
                                              // executor
    }

    #[test]
    fn test_hash_indexes_unique_constraints() {
        let schema = TableSchema::with_unique_constraints(
            "products".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "sku".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
            ],
            vec![vec!["sku".to_string()]], // Unique constraint on sku
        );

        let mut table = Table::new(schema);

        // Insert some rows
        for i in 0..5 {
            let row = Row::new(vec![SqlValue::Integer(i), SqlValue::Varchar(arcstr::ArcStr::from(format!("SKU{}", i)))]);
            table.insert(row).unwrap();
        }

        // Check that unique index exists and has entries
        assert_eq!(table.unique_indexes().len(), 1);
        assert_eq!(table.unique_indexes()[0].len(), 5);
    }

    #[test]
    fn test_update_row_selective_non_indexed_column() {
        // Create table with primary key and unique constraint
        let schema = TableSchema::with_all_constraints(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "email".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
            ],
            Some(vec!["id".to_string()]),
            vec![vec!["email".to_string()]],
        );
        let mut table = Table::new(schema);

        // Insert initial row
        let row1 = Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]);
        table.insert(row1).unwrap();

        // Update only the 'name' column (non-indexed)
        let updated_row = Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice Smith")),
        ]);
        let mut changed_columns = std::collections::HashSet::new();
        changed_columns.insert(2); // 'name' column index

        table.update_row_selective(0, updated_row, &changed_columns).unwrap();

        // Verify row was updated
        let row = table.scan().iter().next().unwrap();
        assert_eq!(row.get(2), Some(&SqlValue::Varchar(arcstr::ArcStr::from("Alice Smith"))));
    }

    #[test]
    fn test_update_row_selective_primary_key_column() {
        // Create table with primary key
        let schema = TableSchema::with_primary_key(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
            ],
            vec!["id".to_string()],
        );
        let mut table = Table::new(schema);

        // Insert initial rows
        table
            .insert(Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]))
            .unwrap();
        table
            .insert(Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]))
            .unwrap();

        // Update primary key column
        let updated_row = Row::new(vec![
            SqlValue::Integer(10), // Changed PK
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]);
        let mut changed_columns = std::collections::HashSet::new();
        changed_columns.insert(0); // 'id' column index

        table.update_row_selective(0, updated_row, &changed_columns).unwrap();

        // Verify primary key index was updated
        assert_eq!(table.row_count(), 2);
        let row = table.scan().iter().next().unwrap();
        assert_eq!(row.get(0), Some(&SqlValue::Integer(10)));
    }

    #[test]
    fn test_update_row_selective_unique_constraint_column() {
        // Create table with unique constraint
        let schema = TableSchema::with_unique_constraints(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "email".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
            ],
            vec![vec!["email".to_string()]],
        );
        let mut table = Table::new(schema);

        // Insert initial rows
        table
            .insert(Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            ]))
            .unwrap();

        // Update unique constraint column
        let updated_row = Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("alice.smith@example.com")), // Changed email
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]);
        let mut changed_columns = std::collections::HashSet::new();
        changed_columns.insert(1); // 'email' column index

        table.update_row_selective(0, updated_row, &changed_columns).unwrap();

        // Verify unique index was updated
        let row = table.scan().iter().next().unwrap();
        assert_eq!(row.get(1), Some(&SqlValue::Varchar(arcstr::ArcStr::from("alice.smith@example.com"))));
    }

    #[test]
    fn test_update_row_selective_vs_full_correctness() {
        // Verify both methods produce the same result
        let schema1 = TableSchema::with_all_constraints(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "email".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
            ],
            Some(vec!["id".to_string()]),
            vec![vec!["email".to_string()]],
        );
        let mut table1 = Table::new(schema1.clone());

        let schema2 = schema1.clone();
        let mut table2 = Table::new(schema2);

        // Insert same initial row into both tables
        let initial_row = Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]);
        table1.insert(initial_row.clone()).unwrap();
        table2.insert(initial_row).unwrap();

        // Update with selective method
        let updated_row1 = Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("alice@example.com")),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice Smith")),
        ]);
        let mut changed_columns = std::collections::HashSet::new();
        changed_columns.insert(2); // 'name' column
        table1.update_row_selective(0, updated_row1.clone(), &changed_columns).unwrap();

        // Update with full method
        table2.update_row(0, updated_row1).unwrap();

        // Both tables should be identical
        let row1 = table1.scan().iter().next().unwrap();
        let row2 = table2.scan().iter().next().unwrap();
        assert_eq!(row1.get(0), row2.get(0));
        assert_eq!(row1.get(1), row2.get(1));
        assert_eq!(row1.get(2), row2.get(2));
    }
}
