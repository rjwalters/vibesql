// ============================================================================
// WAL Entry Types
// ============================================================================
//
// Defines the Write-Ahead Log entry structure and operation types for
// capturing database changes for async persistence.

use std::io::{Read, Write};

use vibesql_types::SqlValue;

use crate::{
    persistence::binary::{
        io::{read_bool, read_u32, read_u64, write_bool, write_u32, write_u64},
        value::{read_sql_value, write_sql_value},
    },
    StorageError,
};

/// Log Sequence Number - monotonically increasing identifier for WAL entries
pub type Lsn = u64;

/// WAL entry representing a single operation to be persisted
#[derive(Debug, Clone, PartialEq)]
pub struct WalEntry {
    /// Log sequence number - unique, monotonically increasing
    pub lsn: Lsn,
    /// Timestamp when the entry was created (milliseconds since epoch)
    pub timestamp_ms: u64,
    /// The operation to perform
    pub op: WalOp,
}

/// Operations that can be recorded in the WAL
#[derive(Debug, Clone, PartialEq)]
pub enum WalOp {
    // DML Operations
    //
    // As of WAL format version 2, DML ops carry the fully-qualified
    // `table_name` so recovery can route the mutation back to the correct
    // table during replay. The numeric `table_id` is a hash of the name and is
    // retained for diagnostics/compat, but it is NOT sufficient to resolve a
    // table during recovery: it lives in a different id space than the
    // monotonic `table_id` stored in `CreateTable`.
    /// Insert a row into a table
    ///
    /// `row_id` is the row's physical index at emit time (diagnostic only —
    /// replay appends rows in LSN order, reproducing physical positions).
    /// `rowid` (WAL format v3+, issue #5835) is the row's effective SQLite
    /// rowid: the explicit `Row::row_id` when present, else the implicit
    /// physical position + 1. Recovery stamps it back onto the replayed row
    /// so rowid semantics survive a crash. `None` only when parsed from a
    /// v2-or-earlier log.
    Insert {
        table_id: u32,
        table_name: String,
        row_id: u64,
        values: Vec<SqlValue>,
        rowid: Option<u64>,
    },
    /// Update a row in a table
    Update {
        table_id: u32,
        table_name: String,
        row_id: u64,
        old_values: Vec<SqlValue>,
        new_values: Vec<SqlValue>,
    },
    /// Delete a row from a table
    Delete { table_id: u32, table_name: String, row_id: u64, old_values: Vec<SqlValue> },

    // DDL Operations
    /// Create a new table
    CreateTable {
        table_id: u32,
        table_name: String,
        /// Serialized schema (using existing binary format)
        schema_data: Vec<u8>,
    },
    /// Drop a table
    DropTable { table_id: u32, table_name: String },
    /// Create an index
    CreateIndex {
        index_id: u32,
        index_name: String,
        table_id: u32,
        column_indices: Vec<u32>,
        is_unique: bool,
    },
    /// Drop an index
    DropIndex { index_id: u32, index_name: String },

    // Transaction Operations
    /// Begin a transaction
    TxnBegin { txn_id: u64 },
    /// Commit a transaction
    TxnCommit { txn_id: u64 },
    /// Rollback a transaction
    TxnRollback { txn_id: u64 },

    // Savepoint Operations (WAL format v4, issue #6170)
    //
    // A `SAVEPOINT`/`ROLLBACK TO SAVEPOINT` pair inside an open transaction is
    // recorded so crash recovery can reproduce the same in-memory undo the live
    // engine performs (`Database::rollback_to_savepoint`, #6278's wholesale
    // catalog/tables/operations snapshot restore). Without these markers,
    // recovery's buffer-until-commit replay (`TransactionTracker`) had no way
    // to know that operations logged between a `SAVEPOINT` and its later
    // `ROLLBACK TO` were undone before the transaction ultimately committed —
    // it replayed every buffered DML op in the transaction unconditionally, so
    // a row inserted and then rolled back via `ROLLBACK TO SAVEPOINT` (with the
    // transaction going on to commit some *other* way, e.g. `RELEASE` of the
    // outermost savepoint) reappeared after a fresh process re-opened the
    // database from WAL (fkey2-2.60).
    /// Mark a named savepoint at the current position in the transaction's
    /// operation stream.
    Savepoint { name: String },
    /// Roll back to a previously marked savepoint: recovery discards every
    /// buffered operation recorded after the matching `Savepoint` marker (the
    /// marker itself, and everything before it, survives — mirroring
    /// `Database::rollback_to_savepoint`'s "the named savepoint itself
    /// survives" semantics).
    RollbackToSavepoint { name: String },

    // Checkpoint Operations
    /// Begin a checkpoint
    CheckpointBegin { checkpoint_id: u64 },
    /// Complete a checkpoint (all data up to this LSN is persisted)
    CheckpointComplete { checkpoint_id: u64, lsn: Lsn },
}

/// Operation type tags for binary serialization
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalOpTag {
    Insert = 0x01,
    Update = 0x02,
    Delete = 0x03,
    CreateTable = 0x10,
    DropTable = 0x11,
    CreateIndex = 0x12,
    DropIndex = 0x13,
    TxnBegin = 0x20,
    TxnCommit = 0x21,
    TxnRollback = 0x22,
    Savepoint = 0x23,
    RollbackToSavepoint = 0x24,
    CheckpointBegin = 0x30,
    CheckpointComplete = 0x31,
}

impl WalOpTag {
    pub fn from_u8(tag: u8) -> Result<Self, StorageError> {
        match tag {
            0x01 => Ok(WalOpTag::Insert),
            0x02 => Ok(WalOpTag::Update),
            0x03 => Ok(WalOpTag::Delete),
            0x10 => Ok(WalOpTag::CreateTable),
            0x11 => Ok(WalOpTag::DropTable),
            0x12 => Ok(WalOpTag::CreateIndex),
            0x13 => Ok(WalOpTag::DropIndex),
            0x20 => Ok(WalOpTag::TxnBegin),
            0x21 => Ok(WalOpTag::TxnCommit),
            0x22 => Ok(WalOpTag::TxnRollback),
            0x23 => Ok(WalOpTag::Savepoint),
            0x24 => Ok(WalOpTag::RollbackToSavepoint),
            0x30 => Ok(WalOpTag::CheckpointBegin),
            0x31 => Ok(WalOpTag::CheckpointComplete),
            _ => Err(StorageError::IoError(format!("Unknown WAL op tag: 0x{:02X}", tag))),
        }
    }
}

impl WalEntry {
    /// Create a new WAL entry
    pub fn new(lsn: Lsn, timestamp_ms: u64, op: WalOp) -> Self {
        Self { lsn, timestamp_ms, op }
    }

    /// Serialize the entry to bytes
    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), StorageError> {
        write_u64(writer, self.lsn)?;
        write_u64(writer, self.timestamp_ms)?;
        self.op.serialize(writer)?;
        Ok(())
    }

    /// Deserialize an entry from bytes (current WAL format version).
    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, StorageError> {
        Self::deserialize_versioned(reader, crate::wal::format::WAL_VERSION)
    }

    /// Deserialize an entry from bytes for a specific WAL format `version`.
    ///
    /// DML ops gained an inline `table_name` in version 2; pass the version
    /// read from the WAL header so older logs (version 1) are parsed with the
    /// legacy layout.
    pub fn deserialize_versioned<R: Read>(
        reader: &mut R,
        version: u32,
    ) -> Result<Self, StorageError> {
        let lsn = read_u64(reader)?;
        let timestamp_ms = read_u64(reader)?;
        let op = WalOp::deserialize_versioned(reader, version)?;
        Ok(Self { lsn, timestamp_ms, op })
    }
}

impl WalOp {
    /// Serialize the operation to bytes
    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), StorageError> {
        match self {
            WalOp::Insert { table_id, table_name, row_id, values, rowid } => {
                writer
                    .write_all(&[WalOpTag::Insert as u8])
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
                write_u32(writer, *table_id)?;
                write_string(writer, table_name)?;
                write_u64(writer, *row_id)?;
                write_sql_values(writer, values)?;
                // WAL format v3 (issue #5835): effective rowid trailer.
                match rowid {
                    Some(r) => {
                        write_bool(writer, true)?;
                        write_u64(writer, *r)?;
                    }
                    None => write_bool(writer, false)?,
                }
            }
            WalOp::Update { table_id, table_name, row_id, old_values, new_values } => {
                writer
                    .write_all(&[WalOpTag::Update as u8])
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
                write_u32(writer, *table_id)?;
                write_string(writer, table_name)?;
                write_u64(writer, *row_id)?;
                write_sql_values(writer, old_values)?;
                write_sql_values(writer, new_values)?;
            }
            WalOp::Delete { table_id, table_name, row_id, old_values } => {
                writer
                    .write_all(&[WalOpTag::Delete as u8])
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
                write_u32(writer, *table_id)?;
                write_string(writer, table_name)?;
                write_u64(writer, *row_id)?;
                write_sql_values(writer, old_values)?;
            }
            WalOp::CreateTable { table_id, table_name, schema_data } => {
                writer
                    .write_all(&[WalOpTag::CreateTable as u8])
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
                write_u32(writer, *table_id)?;
                write_string(writer, table_name)?;
                write_bytes(writer, schema_data)?;
            }
            WalOp::DropTable { table_id, table_name } => {
                writer
                    .write_all(&[WalOpTag::DropTable as u8])
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
                write_u32(writer, *table_id)?;
                write_string(writer, table_name)?;
            }
            WalOp::CreateIndex { index_id, index_name, table_id, column_indices, is_unique } => {
                writer
                    .write_all(&[WalOpTag::CreateIndex as u8])
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
                write_u32(writer, *index_id)?;
                write_string(writer, index_name)?;
                write_u32(writer, *table_id)?;
                write_u32(writer, column_indices.len() as u32)?;
                for &idx in column_indices {
                    write_u32(writer, idx)?;
                }
                write_bool(writer, *is_unique)?;
            }
            WalOp::DropIndex { index_id, index_name } => {
                writer
                    .write_all(&[WalOpTag::DropIndex as u8])
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
                write_u32(writer, *index_id)?;
                write_string(writer, index_name)?;
            }
            WalOp::TxnBegin { txn_id } => {
                writer
                    .write_all(&[WalOpTag::TxnBegin as u8])
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
                write_u64(writer, *txn_id)?;
            }
            WalOp::TxnCommit { txn_id } => {
                writer
                    .write_all(&[WalOpTag::TxnCommit as u8])
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
                write_u64(writer, *txn_id)?;
            }
            WalOp::TxnRollback { txn_id } => {
                writer
                    .write_all(&[WalOpTag::TxnRollback as u8])
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
                write_u64(writer, *txn_id)?;
            }
            WalOp::Savepoint { name } => {
                writer
                    .write_all(&[WalOpTag::Savepoint as u8])
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
                write_string(writer, name)?;
            }
            WalOp::RollbackToSavepoint { name } => {
                writer
                    .write_all(&[WalOpTag::RollbackToSavepoint as u8])
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
                write_string(writer, name)?;
            }
            WalOp::CheckpointBegin { checkpoint_id } => {
                writer
                    .write_all(&[WalOpTag::CheckpointBegin as u8])
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
                write_u64(writer, *checkpoint_id)?;
            }
            WalOp::CheckpointComplete { checkpoint_id, lsn } => {
                writer
                    .write_all(&[WalOpTag::CheckpointComplete as u8])
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
                write_u64(writer, *checkpoint_id)?;
                write_u64(writer, *lsn)?;
            }
        }
        Ok(())
    }

    /// Deserialize an operation from bytes (current WAL format version).
    pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self, StorageError> {
        Self::deserialize_versioned(reader, crate::wal::format::WAL_VERSION)
    }

    /// Deserialize an operation from bytes for a specific WAL format `version`.
    ///
    /// Version 2 added an inline `table_name` to the Insert/Update/Delete DML
    /// ops. For version 1 logs the field is absent on disk, so we synthesize an
    /// empty name (recovery treats an empty DML table name as unroutable and
    /// skips it — version-1 DML replay was never functional anyway).
    pub fn deserialize_versioned<R: Read>(
        reader: &mut R,
        version: u32,
    ) -> Result<Self, StorageError> {
        let mut tag_buf = [0u8; 1];
        reader.read_exact(&mut tag_buf).map_err(|e| StorageError::IoError(e.to_string()))?;
        let tag = WalOpTag::from_u8(tag_buf[0])?;

        // DML ops carry an inline table_name starting at WAL format version 2.
        let dml_has_table_name = version >= 2;

        match tag {
            WalOpTag::Insert => {
                let table_id = read_u32(reader)?;
                let table_name =
                    if dml_has_table_name { read_string(reader)? } else { String::new() };
                let row_id = read_u64(reader)?;
                let values = read_sql_values(reader)?;
                // WAL format v3 (issue #5835): effective rowid trailer.
                // Absent in v2-and-earlier logs — replay falls back to the
                // legacy physical renumbering for such entries.
                let rowid = if version >= 3 {
                    if read_bool(reader)? {
                        Some(read_u64(reader)?)
                    } else {
                        None
                    }
                } else {
                    None
                };
                Ok(WalOp::Insert { table_id, table_name, row_id, values, rowid })
            }
            WalOpTag::Update => {
                let table_id = read_u32(reader)?;
                let table_name =
                    if dml_has_table_name { read_string(reader)? } else { String::new() };
                let row_id = read_u64(reader)?;
                let old_values = read_sql_values(reader)?;
                let new_values = read_sql_values(reader)?;
                Ok(WalOp::Update { table_id, table_name, row_id, old_values, new_values })
            }
            WalOpTag::Delete => {
                let table_id = read_u32(reader)?;
                let table_name =
                    if dml_has_table_name { read_string(reader)? } else { String::new() };
                let row_id = read_u64(reader)?;
                let old_values = read_sql_values(reader)?;
                Ok(WalOp::Delete { table_id, table_name, row_id, old_values })
            }
            WalOpTag::CreateTable => {
                let table_id = read_u32(reader)?;
                let table_name = read_string(reader)?;
                let schema_data = read_bytes(reader)?;
                Ok(WalOp::CreateTable { table_id, table_name, schema_data })
            }
            WalOpTag::DropTable => {
                let table_id = read_u32(reader)?;
                let table_name = read_string(reader)?;
                Ok(WalOp::DropTable { table_id, table_name })
            }
            WalOpTag::CreateIndex => {
                let index_id = read_u32(reader)?;
                let index_name = read_string(reader)?;
                let table_id = read_u32(reader)?;
                let num_columns = read_u32(reader)? as usize;
                let mut column_indices = Vec::with_capacity(num_columns);
                for _ in 0..num_columns {
                    column_indices.push(read_u32(reader)?);
                }
                let is_unique = read_bool(reader)?;
                Ok(WalOp::CreateIndex { index_id, index_name, table_id, column_indices, is_unique })
            }
            WalOpTag::DropIndex => {
                let index_id = read_u32(reader)?;
                let index_name = read_string(reader)?;
                Ok(WalOp::DropIndex { index_id, index_name })
            }
            WalOpTag::TxnBegin => {
                let txn_id = read_u64(reader)?;
                Ok(WalOp::TxnBegin { txn_id })
            }
            WalOpTag::TxnCommit => {
                let txn_id = read_u64(reader)?;
                Ok(WalOp::TxnCommit { txn_id })
            }
            WalOpTag::TxnRollback => {
                let txn_id = read_u64(reader)?;
                Ok(WalOp::TxnRollback { txn_id })
            }
            WalOpTag::Savepoint => {
                let name = read_string(reader)?;
                Ok(WalOp::Savepoint { name })
            }
            WalOpTag::RollbackToSavepoint => {
                let name = read_string(reader)?;
                Ok(WalOp::RollbackToSavepoint { name })
            }
            WalOpTag::CheckpointBegin => {
                let checkpoint_id = read_u64(reader)?;
                Ok(WalOp::CheckpointBegin { checkpoint_id })
            }
            WalOpTag::CheckpointComplete => {
                let checkpoint_id = read_u64(reader)?;
                let lsn = read_u64(reader)?;
                Ok(WalOp::CheckpointComplete { checkpoint_id, lsn })
            }
        }
    }
}

// Helper functions for serialization

fn write_sql_values<W: Write>(writer: &mut W, values: &[SqlValue]) -> Result<(), StorageError> {
    write_u32(writer, values.len() as u32)?;
    for value in values {
        write_sql_value(writer, value)?;
    }
    Ok(())
}

fn read_sql_values<R: Read>(reader: &mut R) -> Result<Vec<SqlValue>, StorageError> {
    let len = read_u32(reader)? as usize;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(read_sql_value(reader)?);
    }
    Ok(values)
}

fn write_string<W: Write>(writer: &mut W, s: &str) -> Result<(), StorageError> {
    let bytes = s.as_bytes();
    write_u32(writer, bytes.len() as u32)?;
    writer.write_all(bytes).map_err(|e| StorageError::IoError(e.to_string()))
}

fn read_string<R: Read>(reader: &mut R) -> Result<String, StorageError> {
    let len = read_u32(reader)? as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).map_err(|e| StorageError::IoError(e.to_string()))?;
    String::from_utf8(buf).map_err(|e| StorageError::IoError(format!("Invalid UTF-8: {}", e)))
}

fn write_bytes<W: Write>(writer: &mut W, data: &[u8]) -> Result<(), StorageError> {
    write_u32(writer, data.len() as u32)?;
    writer.write_all(data).map_err(|e| StorageError::IoError(e.to_string()))
}

fn read_bytes<R: Read>(reader: &mut R) -> Result<Vec<u8>, StorageError> {
    let len = read_u32(reader)? as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).map_err(|e| StorageError::IoError(e.to_string()))?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_entry_roundtrip_insert() {
        let entry = WalEntry::new(
            1,
            1234567890,
            WalOp::Insert {
                table_id: 42,
                table_name: "main.users".to_string(),
                row_id: 100,
                values: vec![
                    SqlValue::Integer(1),
                    SqlValue::Varchar(arcstr::ArcStr::from("test")),
                    SqlValue::Boolean(true),
                ],
                rowid: Some(101),
            },
        );

        let mut buf = Vec::new();
        entry.serialize(&mut buf).unwrap();

        let mut reader = &buf[..];
        let decoded = WalEntry::deserialize(&mut reader).unwrap();

        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_wal_entry_roundtrip_update() {
        let entry = WalEntry::new(
            2,
            1234567891,
            WalOp::Update {
                table_id: 42,
                table_name: "main.users".to_string(),
                row_id: 100,
                old_values: vec![
                    SqlValue::Integer(1),
                    SqlValue::Varchar(arcstr::ArcStr::from("old")),
                ],
                new_values: vec![
                    SqlValue::Integer(2),
                    SqlValue::Varchar(arcstr::ArcStr::from("new")),
                ],
            },
        );

        let mut buf = Vec::new();
        entry.serialize(&mut buf).unwrap();

        let mut reader = &buf[..];
        let decoded = WalEntry::deserialize(&mut reader).unwrap();

        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_wal_entry_roundtrip_delete() {
        let entry = WalEntry::new(
            3,
            1234567892,
            WalOp::Delete {
                table_id: 42,
                table_name: "main.users".to_string(),
                row_id: 100,
                old_values: vec![SqlValue::Integer(1), SqlValue::Null],
            },
        );

        let mut buf = Vec::new();
        entry.serialize(&mut buf).unwrap();

        let mut reader = &buf[..];
        let decoded = WalEntry::deserialize(&mut reader).unwrap();

        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_wal_entry_roundtrip_create_table() {
        let entry = WalEntry::new(
            4,
            1234567893,
            WalOp::CreateTable {
                table_id: 1,
                table_name: "users".to_string(),
                schema_data: vec![0x01, 0x02, 0x03, 0x04],
            },
        );

        let mut buf = Vec::new();
        entry.serialize(&mut buf).unwrap();

        let mut reader = &buf[..];
        let decoded = WalEntry::deserialize(&mut reader).unwrap();

        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_wal_entry_roundtrip_create_index() {
        let entry = WalEntry::new(
            5,
            1234567894,
            WalOp::CreateIndex {
                index_id: 10,
                index_name: "idx_users_email".to_string(),
                table_id: 1,
                column_indices: vec![2, 3],
                is_unique: true,
            },
        );

        let mut buf = Vec::new();
        entry.serialize(&mut buf).unwrap();

        let mut reader = &buf[..];
        let decoded = WalEntry::deserialize(&mut reader).unwrap();

        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_wal_entry_roundtrip_transaction_ops() {
        let entries = vec![
            WalEntry::new(6, 1234567895, WalOp::TxnBegin { txn_id: 1000 }),
            WalEntry::new(7, 1234567896, WalOp::TxnCommit { txn_id: 1000 }),
            WalEntry::new(8, 1234567897, WalOp::TxnRollback { txn_id: 1001 }),
        ];

        for entry in entries {
            let mut buf = Vec::new();
            entry.serialize(&mut buf).unwrap();

            let mut reader = &buf[..];
            let decoded = WalEntry::deserialize(&mut reader).unwrap();

            assert_eq!(entry, decoded);
        }
    }

    #[test]
    fn test_wal_entry_roundtrip_checkpoint() {
        let entries = vec![
            WalEntry::new(9, 1234567898, WalOp::CheckpointBegin { checkpoint_id: 1 }),
            WalEntry::new(10, 1234567899, WalOp::CheckpointComplete { checkpoint_id: 1, lsn: 8 }),
        ];

        for entry in entries {
            let mut buf = Vec::new();
            entry.serialize(&mut buf).unwrap();

            let mut reader = &buf[..];
            let decoded = WalEntry::deserialize(&mut reader).unwrap();

            assert_eq!(entry, decoded);
        }
    }

    #[test]
    fn test_wal_entry_roundtrip_savepoint_ops() {
        let entries = vec![
            WalEntry::new(11, 1234567900, WalOp::Savepoint { name: "outer".to_string() }),
            WalEntry::new(12, 1234567901, WalOp::RollbackToSavepoint { name: "outer".to_string() }),
        ];

        for entry in entries {
            let mut buf = Vec::new();
            entry.serialize(&mut buf).unwrap();

            let mut reader = &buf[..];
            let decoded = WalEntry::deserialize(&mut reader).unwrap();

            assert_eq!(entry, decoded);
        }
    }

    #[test]
    fn test_wal_op_tag_from_u8() {
        assert_eq!(WalOpTag::from_u8(0x01).unwrap(), WalOpTag::Insert);
        assert_eq!(WalOpTag::from_u8(0x02).unwrap(), WalOpTag::Update);
        assert_eq!(WalOpTag::from_u8(0x03).unwrap(), WalOpTag::Delete);
        assert_eq!(WalOpTag::from_u8(0x10).unwrap(), WalOpTag::CreateTable);
        assert!(WalOpTag::from_u8(0xFF).is_err());
    }
}
