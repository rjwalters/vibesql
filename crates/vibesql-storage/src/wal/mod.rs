// ============================================================================
// Write-Ahead Log (WAL)
// ============================================================================
//
// This module provides Write-Ahead Log infrastructure for capturing database
// changes for async persistence. The WAL enables:
//
// 1. Durability: Changes can be recovered after a crash
// 2. Async persistence: Changes are logged quickly, persisted in background
// 3. Replication: WAL can be shipped to replicas (future)
//
// ## Architecture
//
// The WAL is an append-only log file with the following structure:
//
// ```text
// ┌────────────────────────────────────────┐
// │ WAL Header (32 bytes)                  │
// │ - Magic: "VWAL" (4 bytes)              │
// │ - Version: u32                         │
// │ - Created: u64 timestamp               │
// │ - Reserved: 16 bytes                   │
// ├────────────────────────────────────────┤
// │ Entry 1: [len:u32][crc:u32][data:...]  │
// ├────────────────────────────────────────┤
// │ Entry 2: [len:u32][crc:u32][data:...]  │
// ├────────────────────────────────────────┤
// │ ...                                    │
// └────────────────────────────────────────┘
// ```
//
// Each entry contains:
// - Length prefix (4 bytes): Size of the serialized entry data
// - CRC32 checksum (4 bytes): For corruption detection
// - Entry data: Serialized WalEntry
//
// ## Usage
//
// ```text
// use vibesql_storage::wal::{WalWriter, WalReader, WalEntry, WalOp};
// use std::io::Cursor;
//
// // Create a new WAL file
// let buf = Vec::new();
// let cursor = Cursor::new(buf);
// let mut writer = WalWriter::create(cursor).unwrap();
//
// // Append an operation
// let lsn = writer.append_op(WalOp::Insert {
//     table_id: 1,
//     row_id: 100,
//     values: vec![SqlValue::Integer(42)],
// }).unwrap();
//
// // Read entries back
// let cursor = Cursor::new(writer.writer.into_inner());
// let mut reader = WalReader::open(cursor).unwrap();
// let entries = reader.read_all().unwrap();
// ```
//
// ## Corruption Handling
//
// The WAL reader detects corruption through:
// 1. CRC32 checksums on each entry
// 2. Length prefixes to detect truncation
//
// When corruption is detected, recovery can truncate the WAL at the last
// valid entry using `find_recovery_point()`.

pub mod checkpoint;
pub mod durability;
pub mod engine;
pub mod entry;
pub mod format;
pub mod reader;
pub mod recovery;
pub mod scheduler;
pub mod truncate;
pub mod writer;

// Re-export main types
pub use checkpoint::{
    read_checkpoint_data, CheckpointHeader, CheckpointInfo, CheckpointWriter,
    CHECKPOINT_HEADER_SIZE, CHECKPOINT_MAGIC, CHECKPOINT_VERSION,
};
pub use durability::{DurabilityConfig, DurabilityMode, TransactionDurability};
pub use engine::{
    FlushNotifier, PersistenceConfig, PersistenceEngine, PersistenceStats, WalMessage,
    DEFAULT_CHANNEL_CAPACITY, DEFAULT_FLUSH_COUNT, DEFAULT_FLUSH_INTERVAL_MS,
};
pub use entry::{Lsn, WalEntry, WalOp, WalOpTag};
pub use format::{WalHeader, WAL_HEADER_SIZE, WAL_MAGIC, WAL_VERSION};
pub use reader::{find_recovery_point, ReadResult, RecoveryInfo, WalIterator, WalReader};
pub use scheduler::{
    CheckpointConfig, CheckpointScheduler, CheckpointStats, CheckpointTrigger,
    CheckpointTriggerState, DEFAULT_CHECKPOINT_INTERVAL_SECS, DEFAULT_KEEP_CHECKPOINTS,
    DEFAULT_WAL_SIZE_THRESHOLD,
};
pub use recovery::{needs_recovery, recover, RecoveryConfig, RecoveryManager, RecoveryStats};
pub use truncate::{truncate_wal, TruncateResult};
pub use writer::{verify_checksum, WalWriter};
