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
// Inter-process advisory locking (flock/LockFileEx via std file locks) is
// unavailable on wasm32 — cfg-gated so the WASM build keeps compiling.
#[cfg(not(target_arch = "wasm32"))]
pub mod lock;
pub mod mvcc;
pub mod page;
pub mod persistence;
pub mod progress;
pub mod query_buffer_pool;
pub mod row;
pub mod statistics;
pub mod table;
pub mod wal;

// Platform-specific exports
#[cfg(not(target_arch = "wasm32"))]
pub use backend::{NativeFile, NativeStorage};
#[cfg(target_arch = "wasm32")]
pub use backend::{OpfsFile, OpfsStorage};
pub use backend::{StorageBackend, StorageFile};
pub use blob::{BlobId, BlobMetadata, BlobStorageConfig, BlobStorageService};
pub use buffer::{BufferPool, BufferPoolStats};
pub use change_events::{
    channel as change_event_channel, ChangeEvent, ChangeEventPk, ChangeEventReceiver,
    ChangeEventSender, RecvError as ChangeEventRecvError, DEFAULT_CHANNEL_CAPACITY,
};
pub use columnar::{ColumnData, ColumnarTable};
pub use columnar_cache::{CacheStats, ColumnarCache};
pub use database::{
    print_delete_profile_summary, reset_delete_profile_stats, AccessSignalSnapshot, Database,
    DatabaseConfig, DeferredFkViolation, DeferredFkViolationKind, DeleteProfileStats, IndexData,
    IndexManager, IndexMetadata, OwnedStreamingRangeScan, SpatialIndexMetadata, SpillPolicy,
    TransactionState, DELETE_PROFILE_STATS,
};
pub use error::{StorageError, StorageResult};
pub use index::{extract_mbr_from_sql_value, SpatialIndex, SpatialIndexEntry};
#[cfg(not(target_arch = "wasm32"))]
pub use lock::{acquire_exclusive, cleanup_stale_temp_files, DatabaseLock};
pub use mvcc::{mvcc_enabled, stamp_xmax_for_write, stamp_xmin_for_write, TxnSnapshot};
pub use persistence::load::{parse_sql_statements, read_sql_dump};
pub use query_buffer_pool::{
    QueryBufferPool, QueryBufferPoolStats, RowBufferGuard, ValueBufferGuard,
};
pub use row::{Row, RowValues, TxnId, PRE_MVCC_TXN_ID, ROW_INLINE_CAPACITY};
pub use statistics::{ColumnStatistics, TableIndexInfo, TableStatistics};
pub use table::{DeleteResult, RowidExhausted, Table};
pub use wal::{
    DurabilityConfig, DurabilityMode, Lsn, PersistenceConfig, PersistenceEngine, PersistenceStats,
    TransactionDurability, WalEntry, WalOp, WalOpTag,
};

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
            let row = Row::new(vec![
                SqlValue::Integer(i),
                SqlValue::Varchar(arcstr::ArcStr::from(format!("User {}", i))),
            ]);
            table.insert(row).unwrap();
        }

        // Check that primary key index exists and has entries
        assert!(table.primary_key_index().is_some());
        assert_eq!(table.primary_key_index().as_ref().unwrap().len(), 10);

        // Try to insert duplicate - should work at table level (constraint check is in executor)
        let duplicate_row = Row::new(vec![
            SqlValue::Integer(0),
            SqlValue::Varchar(arcstr::ArcStr::from("Duplicate User")),
        ]);
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
            let row = Row::new(vec![
                SqlValue::Integer(i),
                SqlValue::Varchar(arcstr::ArcStr::from(format!("SKU{}", i))),
            ]);
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
            .insert(Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            ]))
            .unwrap();
        table
            .insert(Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            ]))
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
        assert_eq!(
            row.get(1),
            Some(&SqlValue::Varchar(arcstr::ArcStr::from("alice.smith@example.com")))
        );
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

    // -----------------------------------------------------------------------
    // Rowid stability (issue #5835)
    // -----------------------------------------------------------------------

    fn rowid_test_table() -> Table {
        let schema = TableSchema::new(
            "t".to_string(),
            vec![ColumnSchema::new("x".to_string(), DataType::Integer, true)],
        );
        Table::new(schema)
    }

    /// `next_rowid` covers both the physical row count (implicit rowids) and
    /// the largest explicit rowid ever assigned, so allocation never collides
    /// after explicit rowids (e.g. reloaded from a v13 snapshot) exceed the
    /// physical count.
    #[test]
    fn test_next_rowid_tracks_explicit_rowids() {
        let mut table = rowid_test_table();

        // Empty table: next rowid is 1.
        assert_eq!(table.next_rowid(), 1);

        // Implicit rows: next rowid stays physical_count + 1.
        table.insert(Row::new(vec![SqlValue::Integer(10)])).unwrap();
        assert_eq!(table.next_rowid(), 2);

        // Explicit rowid larger than the physical count (the reload shape:
        // live rows persisted with rowids 1 and 7).
        table.insert(Row::with_row_id(vec![SqlValue::Integer(20)], 7)).unwrap();
        assert_eq!(
            table.next_rowid(),
            8,
            "must exceed the explicit rowid 7, not physical count 2"
        );

        // Deletes never decrease it (monotone).
        assert!(table.mark_deleted_inplace(1));
        assert_eq!(table.next_rowid(), 8);
    }

    /// Compaction physically shifts row positions; surviving implicit rowids
    /// must be materialized before the shift so `WHERE rowid=N` still targets
    /// the same row afterwards.
    #[test]
    fn test_compact_materializes_implicit_rowids() {
        let mut table = rowid_test_table();
        for i in 1..=10i64 {
            table.insert(Row::new(vec![SqlValue::Integer(i)])).unwrap();
        }

        // Tombstone rows at physical indices 0..=5 (implicit rowids 1..=6) —
        // over 50%, so compact_if_needed compacts.
        for idx in 0..6 {
            assert!(table.mark_deleted_inplace(idx));
        }
        assert!(table.compact_if_needed(), "expected compaction with 60% deleted");

        // Survivors were at physical indices 6..=9 → implicit rowids 7..=10.
        let rowids: Vec<u64> = table
            .scan_live()
            .map(|(_, row)| row.row_id.expect("materialized rowid"))
            .collect();
        assert_eq!(rowids, vec![7, 8, 9, 10]);

        // Allocation continues past the materialized max, not at the
        // (shrunken) physical count + 1 (which would collide with rowid 7).
        assert_eq!(table.next_rowid(), 11);
    }

    /// Rowids are signed (SQLite model): a negative explicit rowid is stored
    /// as its two's-complement u64 bit pattern and must never poison
    /// allocation. Judge-review regression for PR #5891: previously
    /// `INSERT INTO t(rowid,x) VALUES(-1,5)` set `max_assigned_rowid` to
    /// `u64::MAX`, so the next implicit insert computed `u64::MAX + 1` —
    /// panic in debug builds, duplicate rowid 0 in release builds.
    ///
    /// sqlite3-verified allocation: next implicit rowid = signed max + 1
    /// (only -1 present → 0; only -5 present → -4), or 1 for an empty table.
    #[test]
    fn test_negative_explicit_rowid_does_not_poison_allocation() {
        let mut table = rowid_test_table();

        // Explicit rowid -1 (stored as u64::MAX bit pattern).
        table.insert(Row::with_row_id(vec![SqlValue::Integer(5)], (-1i64) as u64)).unwrap();
        assert_eq!(table.max_rowid_signed(), Some(-1));
        assert_eq!(table.next_rowid_signed(), 0, "sqlite3: after rowid -1, next is 0");
        assert_eq!(table.next_rowid(), 0u64);

        // The implicit insert that previously panicked: stamp the allocated
        // rowid (as the executor does) and continue allocating past it.
        let next = table.next_rowid();
        table.insert(Row::with_row_id(vec![SqlValue::Integer(6)], next)).unwrap();
        assert_eq!(table.max_rowid_signed(), Some(0));
        assert_eq!(table.next_rowid_signed(), 1);

        // Deeply negative maxima allocate signed max + 1 (sqlite3: -5 → -4).
        let mut table2 = rowid_test_table();
        table2.insert(Row::with_row_id(vec![SqlValue::Integer(1)], (-5i64) as u64)).unwrap();
        assert_eq!(table2.next_rowid_signed(), -4, "sqlite3: after rowid -5, next is -4");

        // A positive rowid still dominates a negative one (signed max).
        table2.insert(Row::with_row_id(vec![SqlValue::Integer(2)], 3)).unwrap();
        assert_eq!(table2.next_rowid_signed(), 4, "sqlite3: max(-5, 3) + 1 = 4");
    }

    /// `next_rowid_signed` is the infallible *peek* helper: it saturates at
    /// `i64::MAX` rather than overflowing. The fallible sqlite3-parity allocator
    /// [`Table::allocate_rowid`] is what insert paths use for stored rowids
    /// (see the tests below); this only guards the peek's saturation.
    #[test]
    fn test_next_rowid_saturates_at_i64_max() {
        let mut table = rowid_test_table();
        table.insert(Row::with_row_id(vec![SqlValue::Integer(1)], i64::MAX as u64)).unwrap();
        assert_eq!(table.next_rowid_signed(), i64::MAX);
    }

    /// `allocate_rowid` matches sqlite3's `max(rowid) + 1` in the ordinary
    /// (non-saturated) range, including negative maxima — the same values the
    /// plain-rowid and INTEGER PRIMARY KEY NULL-assign paths must agree on
    /// (issue #5894).
    #[test]
    fn test_allocate_rowid_matches_sqlite_normal_range() {
        // Empty table: first allocation is 1.
        let table = rowid_test_table();
        assert_eq!(table.allocate_rowid(), Ok(1));

        // Only negative rowids present: next is signed max + 1, NOT 1
        // (sqlite3: after rowid -5, next is -4; after -1, next is 0).
        let mut neg = rowid_test_table();
        neg.insert(Row::with_row_id(vec![SqlValue::Integer(1)], (-5i64) as u64)).unwrap();
        assert_eq!(neg.allocate_rowid(), Ok(-4));

        let mut neg1 = rowid_test_table();
        neg1.insert(Row::with_row_id(vec![SqlValue::Integer(1)], (-1i64) as u64)).unwrap();
        assert_eq!(neg1.allocate_rowid(), Ok(0));

        // Mixed negative/positive: the positive max dominates.
        neg.insert(Row::with_row_id(vec![SqlValue::Integer(2)], 10)).unwrap();
        assert_eq!(neg.allocate_rowid(), Ok(11));

        // A table one below the ceiling allocates i64::MAX itself (no error).
        let mut near = rowid_test_table();
        near.insert(Row::with_row_id(vec![SqlValue::Integer(1)], (i64::MAX - 1) as u64)).unwrap();
        assert_eq!(near.allocate_rowid(), Ok(i64::MAX));
    }

    /// At `i64::MAX`, sqlite3 does NOT reuse the max (a silent duplicate) or
    /// overflow — it probes a random *unused* rowid. `allocate_rowid` must
    /// return a fresh, in-range, not-in-use rowid (issue #5894). The value is
    /// nondeterministic, so we assert uniqueness and validity, not a specific
    /// number.
    #[test]
    fn test_allocate_rowid_at_i64_max_probes_unused() {
        let mut table = rowid_test_table();
        table.insert(Row::with_row_id(vec![SqlValue::Integer(1)], i64::MAX as u64)).unwrap();

        let in_use: std::collections::HashSet<i64> =
            table.scan_live().map(|(_, r)| r.row_id.unwrap() as i64).collect();

        // 20 draws must all be positive, in range, and unused — never i64::MAX.
        for _ in 0..20 {
            let allocated = table.allocate_rowid().expect("random probe should find a free rowid");
            assert!(allocated > 0, "probed rowid must be positive: {allocated}");
            assert!(allocated < i64::MAX, "probed rowid must be below the ceiling");
            assert!(!in_use.contains(&allocated), "probed rowid must be unused: {allocated}");
        }
    }

    /// The exhaustion marker mirrors sqlite3's SQLITE_FULL text so callers can
    /// surface it verbatim (issue #5894).
    #[test]
    fn test_rowid_exhausted_display() {
        assert_eq!(crate::RowidExhausted.to_string(), "database or disk is full");
    }
}
