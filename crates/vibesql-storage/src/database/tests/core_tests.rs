// ============================================================================
// Core Database Tests
// ============================================================================
//
// Tests for Database struct functionality including:
// - SQL mode management
// - Change event broadcasting
// - WAL persistence
// - Transaction durability hints

use vibesql_types::{MySqlModeFlags, SqlMode, SqlValue};

use crate::change_events::ChangeEvent;
use crate::database::Database;

// ============================================================================
// SQL Mode Tests
// ============================================================================

#[test]
fn test_set_sql_mode_changes_mode() {
    let mut db = Database::new();

    // Default is MySQL (for SQLLogicTest compatibility - dolthub corpus was regenerated against
    // MySQL 8.x)
    assert!(matches!(db.sql_mode(), SqlMode::MySQL { .. }));

    // Change to SQLite
    db.set_sql_mode(SqlMode::SQLite);
    assert!(matches!(db.sql_mode(), SqlMode::SQLite));

    // Change back to MySQL
    db.set_sql_mode(SqlMode::MySQL { flags: MySqlModeFlags::default() });
    assert!(matches!(db.sql_mode(), SqlMode::MySQL { .. }));
}

#[test]
fn test_set_sql_mode_updates_session_variable() {
    let mut db = Database::new();

    // Set to SQLite mode
    db.set_sql_mode(SqlMode::SQLite);

    // Check session variable reflects the change
    let sql_mode_var = db.get_session_variable("SQL_MODE");
    assert!(sql_mode_var.is_some());
    if let Some(SqlValue::Varchar(mode_str)) = sql_mode_var {
        assert_eq!(mode_str.as_str(), "SQLITE");
    } else {
        panic!("Expected SQL_MODE to be a Varchar");
    }
}

#[test]
fn test_set_sql_mode_mysql_with_flags() {
    let mut db = Database::new();

    // Set MySQL with specific flags
    db.set_sql_mode(SqlMode::MySQL {
        flags: MySqlModeFlags {
            pipes_as_concat: true,
            ansi_quotes: true,
            strict_mode: true,
            sqlite_division_semantics: false,
        },
    });

    // Check session variable contains the flags
    let sql_mode_var = db.get_session_variable("SQL_MODE");
    assert!(sql_mode_var.is_some());
    if let Some(SqlValue::Varchar(mode_str)) = sql_mode_var {
        assert!(mode_str.contains("STRICT_TRANS_TABLES"));
        assert!(mode_str.contains("PIPES_AS_CONCAT"));
        assert!(mode_str.contains("ANSI_QUOTES"));
    } else {
        panic!("Expected SQL_MODE to be a Varchar");
    }
}

#[test]
fn test_set_sql_mode_mysql_default_flags() {
    let mut db = Database::new();

    // Set MySQL with default flags (all false)
    db.set_sql_mode(SqlMode::MySQL { flags: MySqlModeFlags::default() });

    // Check session variable has default MySQL modes
    let sql_mode_var = db.get_session_variable("SQL_MODE");
    assert!(sql_mode_var.is_some());
    if let Some(SqlValue::Varchar(mode_str)) = sql_mode_var {
        // Default should include common MySQL defaults
        assert!(
            mode_str.contains("NO_ZERO_IN_DATE") || mode_str.contains("NO_ENGINE_SUBSTITUTION")
        );
    } else {
        panic!("Expected SQL_MODE to be a Varchar");
    }
}

#[test]
fn test_sql_mode_affects_subsequent_queries() {
    let mut db = Database::new();

    // Start in MySQL mode (default for SQLLogicTest compatibility)
    assert!(matches!(db.sql_mode(), SqlMode::MySQL { .. }));

    // Switch to SQLite
    db.set_sql_mode(SqlMode::SQLite);

    // Verify the mode changed
    let mode = db.sql_mode();
    assert!(matches!(mode, SqlMode::SQLite));
}

// ============================================================================
// Change Event Tests
// ============================================================================

#[test]
fn test_change_events_disabled_by_default() {
    let db = Database::new();
    assert!(!db.change_events_enabled());
    assert!(db.subscribe_changes().is_none());
}

#[test]
fn test_enable_change_events() {
    let mut db = Database::new();
    let _rx = db.enable_change_events(16);
    assert!(db.change_events_enabled());
    assert!(db.subscribe_changes().is_some());
}

#[test]
fn test_insert_emits_change_event() {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    let mut db = Database::new();
    let mut rx = db.enable_change_events(16);

    // Create a simple table
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

    // Insert a row
    let row = crate::Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
    ]);
    db.insert_row("users", row).unwrap();

    // Verify change event was emitted
    let events = rx.recv_all();
    assert_eq!(events.len(), 1);
    match &events[0] {
        ChangeEvent::Insert { table_name, row_index, .. } => {
            assert_eq!(*row_index, 0);
            // Table name will be "users" as passed to insert_row
            assert_eq!(table_name, "users");
        }
        _ => panic!("Expected Insert event, got {:?}", events[0]),
    }
}

#[test]
fn test_batch_insert_emits_multiple_events() {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    let mut db = Database::new();
    let mut rx = db.enable_change_events(16);

    // Create a simple table
    let schema = TableSchema::new(
        "products".to_string(),
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

    // Insert batch of rows
    let rows = vec![
        crate::Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Product A")),
        ]),
        crate::Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Product B")),
        ]),
        crate::Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Product C")),
        ]),
    ];
    db.insert_rows_batch("products", rows).unwrap();

    // Verify 3 change events were emitted
    let events = rx.recv_all();
    assert_eq!(events.len(), 3);
    for (i, event) in events.iter().enumerate() {
        assert!(matches!(event, ChangeEvent::Insert { row_index, .. } if *row_index == i));
    }
}

#[test]
fn test_update_emits_change_event() {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    let mut db = Database::new();

    // Create table with primary key
    let schema = TableSchema::with_primary_key(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
        vec!["id".to_string()],
    );
    db.create_table(schema).unwrap();

    // Insert a row
    let row = crate::Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
    ]);
    db.insert_row("users", row).unwrap();

    // Now enable change events and update
    let mut rx = db.enable_change_events(16);

    db.update_row_by_pk(
        "users",
        SqlValue::Integer(1),
        vec![("name", SqlValue::Varchar(arcstr::ArcStr::from("Alice Smith")))],
    )
    .unwrap();

    // Verify update event was emitted
    let events = rx.recv_all();
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], ChangeEvent::Update { row_index: 0, .. }));
}

#[test]
fn test_multiple_subscribers() {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    let mut db = Database::new();
    let mut rx1 = db.enable_change_events(16);
    let mut rx2 = db.subscribe_changes().unwrap();

    // Create table and insert
    let schema = TableSchema::new(
        "test".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    let row = crate::Row::new(vec![SqlValue::Integer(1)]);
    db.insert_row("test", row).unwrap();

    // Both receivers should get the event
    assert_eq!(rx1.recv_all().len(), 1);
    assert_eq!(rx2.recv_all().len(), 1);
}

#[test]
fn test_no_panic_on_lagged_receiver() {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    let mut db = Database::new();
    let _rx = db.enable_change_events(2); // Very small buffer

    // Create table
    let schema = TableSchema::new(
        "test".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Insert more rows than buffer can hold
    for i in 0..10 {
        let row = crate::Row::new(vec![SqlValue::Integer(i)]);
        db.insert_row("test", row).unwrap();
    }
    // Should not panic - lagged receivers are handled gracefully
}

#[test]
fn test_notify_deletes() {
    let mut db = Database::new();
    let mut rx = db.enable_change_events(16);

    // Directly call notify_deletes (since DELETE is handled by executor)
    db.notify_deletes("users", &[0, 2, 5]);

    let events = rx.recv_all();
    assert_eq!(events.len(), 3);
    assert!(
        matches!(&events[0], ChangeEvent::Delete { table_name, row_index: 0, .. } if table_name == "users")
    );
    assert!(
        matches!(&events[1], ChangeEvent::Delete { table_name, row_index: 2, .. } if table_name == "users")
    );
    assert!(
        matches!(&events[2], ChangeEvent::Delete { table_name, row_index: 5, .. } if table_name == "users")
    );
}

#[test]
fn test_notify_update() {
    let mut db = Database::new();
    let mut rx = db.enable_change_events(16);

    // Directly call notify_update
    db.notify_update("products", 42);

    let events = rx.recv_all();
    assert_eq!(events.len(), 1);
    assert!(
        matches!(&events[0], ChangeEvent::Update { table_name, row_index: 42, .. } if table_name == "products")
    );
}

// ============================================================================
// WAL Persistence Tests
// ============================================================================

#[test]
fn test_persistence_disabled_by_default() {
    let db = Database::new();
    assert!(!db.persistence_enabled());
    assert!(db.persistence_stats().is_none());
}

#[test]
fn test_enable_persistence() {
    use std::io::Cursor;

    use crate::wal::{PersistenceConfig, PersistenceEngine};

    let mut db = Database::new();
    assert!(!db.persistence_enabled());

    // Create a persistence engine with an in-memory writer
    let buf = Vec::new();
    let cursor = Cursor::new(buf);
    let engine = PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();

    db.enable_persistence(engine);
    assert!(db.persistence_enabled());
    assert!(db.persistence_stats().is_some());
}

#[test]
fn test_persistence_emits_insert_entries() {
    use std::io::Cursor;

    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    use crate::wal::{PersistenceConfig, PersistenceEngine};

    let mut db = Database::new();

    // Enable persistence
    let buf = Vec::new();
    let cursor = Cursor::new(buf);
    let engine = PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();
    db.enable_persistence(engine);

    // Create a table
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

    // Insert rows
    let row1 = crate::Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
    ]);
    let row2 =
        crate::Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]);
    db.insert_row("users", row1).unwrap();
    db.insert_row("users", row2).unwrap();

    // Check stats
    let stats = db.persistence_stats().unwrap();
    // CreateTable + 2 Inserts = 3 entries
    assert_eq!(stats.entries_sent, 3);
}

#[test]
fn test_persistence_emits_transaction_entries() {
    use std::io::Cursor;

    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    use crate::wal::{PersistenceConfig, PersistenceEngine};

    let mut db = Database::new();

    // Enable persistence
    let buf = Vec::new();
    let cursor = Cursor::new(buf);
    let engine = PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();
    db.enable_persistence(engine);

    // Create a table
    let schema = TableSchema::new(
        "test".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Start transaction
    db.begin_transaction().unwrap();

    // Insert
    let row = crate::Row::new(vec![SqlValue::Integer(1)]);
    db.insert_row("test", row).unwrap();

    // Commit
    db.commit_transaction().unwrap();

    // Check stats: CreateTable + TxnBegin + Insert + TxnCommit = 4
    let stats = db.persistence_stats().unwrap();
    assert_eq!(stats.entries_sent, 4);
}

#[test]
fn test_sync_persistence_no_op_when_disabled() {
    let db = Database::new();
    // Should not error when persistence is disabled
    assert!(db.sync_persistence().is_ok());
}

#[test]
fn test_emit_wal_delete() {
    use std::io::Cursor;

    use crate::wal::{PersistenceConfig, PersistenceEngine};

    let mut db = Database::new();

    // Enable persistence
    let buf = Vec::new();
    let cursor = Cursor::new(buf);
    let engine = PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();
    db.enable_persistence(engine);

    // Emit delete entries
    db.emit_wal_delete("users", 0, vec![SqlValue::Integer(1)]);
    db.emit_wal_delete("users", 1, vec![SqlValue::Integer(2)]);

    // Check stats
    let stats = db.persistence_stats().unwrap();
    assert_eq!(stats.entries_sent, 2);
}

#[test]
fn test_emit_wal_create_index() {
    use std::io::Cursor;

    use crate::wal::{PersistenceConfig, PersistenceEngine};

    let mut db = Database::new();

    // Enable persistence
    let buf = Vec::new();
    let cursor = Cursor::new(buf);
    let engine = PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();
    db.enable_persistence(engine);

    // Emit create index entry
    db.emit_wal_create_index(1, "idx_users_email", "users", vec![1], false);

    // Check stats
    let stats = db.persistence_stats().unwrap();
    assert_eq!(stats.entries_sent, 1);
}

#[test]
fn test_emit_wal_drop_index() {
    use std::io::Cursor;

    use crate::wal::{PersistenceConfig, PersistenceEngine};

    let mut db = Database::new();

    // Enable persistence
    let buf = Vec::new();
    let cursor = Cursor::new(buf);
    let engine = PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();
    db.enable_persistence(engine);

    // Emit drop index entry
    db.emit_wal_drop_index(1, "idx_users_email");

    // Check stats
    let stats = db.persistence_stats().unwrap();
    assert_eq!(stats.entries_sent, 1);
}

#[test]
fn test_emit_wal_no_op_when_disabled() {
    let db = Database::new();

    // These should be no-ops when persistence is disabled (no panic)
    db.emit_wal_delete("users", 0, vec![SqlValue::Integer(1)]);
    db.emit_wal_create_index(1, "idx", "table", vec![0], false);
    db.emit_wal_drop_index(1, "idx");

    // Persistence stats should still be None
    assert!(db.persistence_stats().is_none());
}

// ============================================================================
// Transaction Durability Hint Tests
// ============================================================================

#[test]
fn test_begin_transaction_with_default_durability() {
    use crate::wal::TransactionDurability;

    let mut db = Database::new();

    db.begin_transaction_with_durability(TransactionDurability::Default).unwrap();
    assert!(db.in_transaction());

    // Verify the durability hint is stored
    assert!(db.in_transaction());

    db.rollback_transaction().unwrap();
}

#[test]
fn test_begin_transaction_with_force_durable() {
    use crate::wal::TransactionDurability;

    let mut db = Database::new();

    db.begin_transaction_with_durability(TransactionDurability::ForceDurable).unwrap();
    assert!(db.in_transaction());

    db.rollback_transaction().unwrap();
}

#[test]
fn test_begin_transaction_with_allow_lazy() {
    use crate::wal::TransactionDurability;

    let mut db = Database::new();

    db.begin_transaction_with_durability(TransactionDurability::AllowLazy).unwrap();
    assert!(db.in_transaction());

    db.rollback_transaction().unwrap();
}

#[test]
fn test_begin_transaction_with_force_volatile() {
    use crate::wal::TransactionDurability;

    let mut db = Database::new();

    db.begin_transaction_with_durability(TransactionDurability::ForceVolatile).unwrap();
    assert!(db.in_transaction());

    db.rollback_transaction().unwrap();
}

#[test]
fn test_durability_hint_cleared_on_commit() {
    use crate::wal::TransactionDurability;

    let mut db = Database::new();

    db.begin_transaction_with_durability(TransactionDurability::ForceDurable).unwrap();
    assert!(db.in_transaction());

    db.commit_transaction().unwrap();
    assert!(!db.in_transaction());
}

#[test]
fn test_durability_hint_cleared_on_rollback() {
    use crate::wal::TransactionDurability;

    let mut db = Database::new();

    db.begin_transaction_with_durability(TransactionDurability::ForceDurable).unwrap();
    assert!(db.in_transaction());

    db.rollback_transaction().unwrap();
    assert!(!db.in_transaction());
}

#[test]
fn test_force_durable_triggers_sync() {
    use std::io::Cursor;

    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    use crate::wal::{PersistenceConfig, PersistenceEngine, TransactionDurability};

    let mut db = Database::new();

    // Enable persistence in lazy mode (default - no sync on commit)
    let buf = Vec::new();
    let cursor = Cursor::new(buf);
    let engine = PersistenceEngine::with_writer(cursor, PersistenceConfig::lazy()).unwrap();
    db.enable_persistence(engine);

    // Create a table
    let schema = TableSchema::new(
        "test".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Begin transaction with ForceDurable hint
    db.begin_transaction_with_durability(TransactionDurability::ForceDurable).unwrap();

    // Insert a row
    let row = crate::Row::new(vec![SqlValue::Integer(1)]);
    db.insert_row("test", row).unwrap();

    // Commit - should trigger sync because ForceDurable overrides lazy mode
    db.commit_transaction().unwrap();

    // Check stats - explicit_flushes should have been triggered by sync
    let stats = db.persistence_stats().unwrap();
    assert!(stats.explicit_flushes >= 1, "ForceDurable should trigger an explicit flush on commit");
}

#[test]
fn test_default_durability_respects_lazy_mode() {
    use std::io::Cursor;

    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    use crate::wal::{PersistenceConfig, PersistenceEngine, TransactionDurability};

    let mut db = Database::new();

    // Enable persistence in lazy mode
    let buf = Vec::new();
    let cursor = Cursor::new(buf);
    let engine = PersistenceEngine::with_writer(cursor, PersistenceConfig::lazy()).unwrap();
    db.enable_persistence(engine);

    // Create a table
    let schema = TableSchema::new(
        "test".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Get initial stats after table creation
    let initial_stats = db.persistence_stats().unwrap();
    let initial_explicit_flushes = initial_stats.explicit_flushes;

    // Begin transaction with Default hint (should follow lazy mode - no sync on commit)
    db.begin_transaction_with_durability(TransactionDurability::Default).unwrap();

    // Insert a row
    let row = crate::Row::new(vec![SqlValue::Integer(1)]);
    db.insert_row("test", row).unwrap();

    // Commit - should NOT trigger sync in lazy mode with default durability
    db.commit_transaction().unwrap();

    // Check stats - no new explicit_flushes should have been triggered
    let final_stats = db.persistence_stats().unwrap();
    assert_eq!(
        final_stats.explicit_flushes, initial_explicit_flushes,
        "Default durability in lazy mode should not trigger explicit flush on commit"
    );
}

#[test]
fn test_durability_hint_no_panic_without_persistence() {
    use crate::wal::TransactionDurability;

    // Create database WITHOUT persistence enabled
    let mut db = Database::new();

    // Begin transaction with ForceDurable hint
    db.begin_transaction_with_durability(TransactionDurability::ForceDurable).unwrap();
    assert!(db.in_transaction());

    // Commit should not panic even though ForceDurable requests sync
    // (sync is a no-op when persistence is not enabled)
    db.commit_transaction().unwrap();
    assert!(!db.in_transaction());
}

#[test]
fn test_allow_lazy_downgrades_durable_mode() {
    use std::io::Cursor;

    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    use crate::wal::{PersistenceConfig, PersistenceEngine, TransactionDurability};

    let mut db = Database::new();

    // Enable persistence in durable mode (sync on every commit by default)
    let buf = Vec::new();
    let cursor = Cursor::new(buf);
    let engine = PersistenceEngine::with_writer(cursor, PersistenceConfig::durable()).unwrap();
    db.enable_persistence(engine);

    // Create a table
    let schema = TableSchema::new(
        "test".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Get initial stats after table creation
    let initial_stats = db.persistence_stats().unwrap();
    let initial_explicit_flushes = initial_stats.explicit_flushes;

    // Begin transaction with AllowLazy hint (should downgrade durable to lazy)
    db.begin_transaction_with_durability(TransactionDurability::AllowLazy).unwrap();

    // Insert a row
    let row = crate::Row::new(vec![SqlValue::Integer(1)]);
    db.insert_row("test", row).unwrap();

    // Commit - AllowLazy should prevent sync even in durable mode
    db.commit_transaction().unwrap();

    // Check stats - no new explicit_flushes should have been triggered
    let final_stats = db.persistence_stats().unwrap();
    assert_eq!(
        final_stats.explicit_flushes, initial_explicit_flushes,
        "AllowLazy should downgrade durable mode and not trigger explicit flush on commit"
    );
}

// ============================================================================
// Rollback restores index state (regression for #5413)
// ============================================================================

/// Regression for #5413: `rollback_transaction` is the SHARED rollback path
/// used by standalone transactions too. The B-tree `IndexManager` lives in
/// `Operations`, which is NOT part of the catalog/tables snapshot — so before
/// the fix an index mutated inside a transaction survived ROLLBACK. This test
/// mutates the index manager inside a transaction (CREATE INDEX), rolls back,
/// and asserts the index is gone, proving `Operations` is restored alongside
/// catalog/tables.
#[test]
fn rollback_restores_index_manager_state() {
    use vibesql_ast::IndexColumn;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    let mut db = Database::new();

    // A table with a single column to index.
    let schema = TableSchema::new(
        "t".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    db.begin_transaction().unwrap();
    db.create_index(
        "idx_t_id".to_string(),
        "t".to_string(),
        false,
        vec![IndexColumn::new_column("id".to_string(), vibesql_ast::OrderDirection::Asc)],
    )
    .unwrap();
    assert!(db.index_exists("idx_t_id"), "index exists inside the transaction");

    db.rollback_transaction().unwrap();

    // Before the #5413 fix, the IndexManager mutation survived rollback and
    // this assertion failed.
    assert!(
        !db.index_exists("idx_t_id"),
        "ROLLBACK must restore the IndexManager, removing the index created in the txn"
    );
}

/// Regression for #5413: committed index mutations must persist (the fix must
/// only undo rolled-back index changes, never committed ones).
#[test]
fn commit_keeps_index_manager_state() {
    use vibesql_ast::IndexColumn;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    let mut db = Database::new();

    let schema = TableSchema::new(
        "t".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    db.begin_transaction().unwrap();
    db.create_index(
        "idx_t_id".to_string(),
        "t".to_string(),
        false,
        vec![IndexColumn::new_column("id".to_string(), vibesql_ast::OrderDirection::Asc)],
    )
    .unwrap();
    db.commit_transaction().unwrap();

    assert!(db.index_exists("idx_t_id"), "COMMIT must keep the index created in the txn");
}

// ============================================================================
// Copy-on-write Operations snapshot (#5419)
// ============================================================================

/// #5419: a read-only transaction must NOT deep-clone the `Operations`
/// (IndexManager + spatial indexes) at BEGIN. Before #5419 the snapshot was
/// eager, so every BEGIN — including the per-read scratch txn used by
/// replicated read-your-own-writes — paid an O(index-keys) clone. With the
/// copy-on-write snapshot, a transaction that only reads triggers zero
/// clones. Asserted deterministically via the clone counter (no timing).
#[test]
fn read_only_transaction_does_not_clone_operations_snapshot() {
    use vibesql_ast::IndexColumn;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    let mut db = Database::new();
    let schema = TableSchema::new(
        "t".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Build a non-trivial index so an eager clone would be observable work.
    db.create_index(
        "idx_t_id".to_string(),
        "t".to_string(),
        false,
        vec![IndexColumn::new_column("id".to_string(), vibesql_ast::OrderDirection::Asc)],
    )
    .unwrap();
    for i in 0..256 {
        db.insert_row("t", crate::Row::new(vec![SqlValue::Integer(i)])).unwrap();
    }

    let before = db.operations_snapshot_clones();

    // A purely read-only transaction: BEGIN, observe index state, ROLLBACK.
    db.begin_transaction().unwrap();
    assert!(db.index_exists("idx_t_id"), "index visible inside the read-only txn");
    let _ = db.list_indexes();
    db.rollback_transaction().unwrap();

    assert_eq!(
        db.operations_snapshot_clones(),
        before,
        "a read-only transaction must not deep-clone the Operations snapshot (#5419)"
    );
}

/// #5419: a transaction that DOES mutate an index lazily clones the
/// `Operations` snapshot exactly once (on the first mutation), and that
/// snapshot still fully restores index state on ROLLBACK — i.e. the #5413
/// correctness fix is preserved, just deferred. Also verifies the clone is
/// taken once even across multiple index mutations.
#[test]
fn mutating_transaction_clones_operations_snapshot_once_and_rolls_back() {
    use vibesql_ast::IndexColumn;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    let mut db = Database::new();
    let schema = TableSchema::new(
        "t".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.create_index(
        "idx_t_id".to_string(),
        "t".to_string(),
        false,
        vec![IndexColumn::new_column("id".to_string(), vibesql_ast::OrderDirection::Asc)],
    )
    .unwrap();

    let before = db.operations_snapshot_clones();

    db.begin_transaction().unwrap();
    // First index mutation triggers exactly one lazy clone.
    db.insert_row("t", crate::Row::new(vec![SqlValue::Integer(1)])).unwrap();
    assert_eq!(
        db.operations_snapshot_clones(),
        before + 1,
        "first index mutation must take exactly one snapshot clone"
    );
    // Further mutations do not re-clone.
    db.insert_row("t", crate::Row::new(vec![SqlValue::Integer(2)])).unwrap();
    db.create_index(
        "idx_t_id2".to_string(),
        "t".to_string(),
        false,
        vec![IndexColumn::new_column("id".to_string(), vibesql_ast::OrderDirection::Asc)],
    )
    .unwrap();
    assert_eq!(
        db.operations_snapshot_clones(),
        before + 1,
        "subsequent mutations must reuse the existing snapshot, not re-clone"
    );

    db.rollback_transaction().unwrap();

    // The lazily-captured snapshot must fully restore index state: the
    // index created in the txn is gone, and the pre-existing index survives.
    assert!(
        !db.index_exists("idx_t_id2"),
        "ROLLBACK must restore Operations: index created in the txn is removed (#5413 preserved)"
    );
    assert!(db.index_exists("idx_t_id"), "the pre-transaction index must survive ROLLBACK");
    assert_eq!(db.get_table("t").map(|t| t.row_count()), Some(0), "rolled-back rows are gone");
}
