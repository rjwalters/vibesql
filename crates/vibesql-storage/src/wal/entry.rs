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
        io::{read_bool, read_u32, read_u64, read_u8, write_bool, write_u32, write_u64, write_u8},
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
    ///
    /// `definition` (WAL format v6+, issue #6741) carries the full index
    /// definition — owning schema, key parts (columns *and* expressions, with
    /// per-part direction/collation), the partial-index `WHERE` predicate, and
    /// the verbatim `CREATE INDEX` text — so crash recovery can faithfully
    /// rebuild the index. It is `None` when parsed from a v5-or-earlier log
    /// (the thin `column_indices` payload alone cannot reconstruct an index,
    /// so such entries keep their historical log-only replay behavior) and for
    /// thin ops from callers that cannot describe the index. As of v7 (issue
    /// #6758) spatial / IVFFlat / HNSW indexes carry a definition too (see
    /// [`WalIndexKind`]).
    CreateIndex {
        index_id: u32,
        index_name: String,
        table_id: u32,
        column_indices: Vec<u32>,
        is_unique: bool,
        definition: Option<WalIndexDefinition>,
    },
    /// Drop an index
    ///
    /// `owner` (WAL format v6+, issue #6741) identifies the owning schema and
    /// table so recovery drops exactly the index that was dropped live (a
    /// same-named index can exist in another schema). `None` when parsed from
    /// a v5-or-earlier log, in which case replay keeps its historical
    /// log-only behavior.
    DropIndex { index_id: u32, index_name: String, owner: Option<WalIndexOwner> },

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

    // Implicit statement-level savepoint (WAL format v5, issue #6438 — sibling
    // of #6170's named-savepoint fix above).
    //
    // SQLite (and `Database::arm_statement_savepoint`/`rollback_statement_savepoint`,
    // #5417) wraps every top-level statement inside an open transaction in an
    // implicit savepoint so a `RAISE(ABORT)` (or an ordinary constraint
    // violation) can undo just that statement's partial changes without
    // rolling back the whole transaction. This marker pair is the unnamed,
    // single-slot analogue of `Savepoint`/`RollbackToSavepoint`: unlike named
    // savepoints there is no stack and no name to disambiguate, since at most
    // one implicit statement savepoint is ever armed at a time (the caller
    // arms exactly one per top-level statement and releases or rolls it back
    // before the next). Without these markers, recovery's buffer-until-commit
    // replay had no way to know that DML ops logged by a statement that
    // aborted partway through (e.g. row 3 of a multi-row `UPDATE` trips
    // `RAISE(ABORT)` after rows 1-2 already wrote) were undone in memory
    // before the enclosing transaction went on to commit — it replayed every
    // buffered op unconditionally, resurrecting the already-rolled-back rows.
    /// Mark the current position in the transaction's operation stream as the
    /// start of a newly-armed implicit statement savepoint. A second
    /// `StatementSavepoint` before any `RollbackStatementSavepoint`/release
    /// simply overwrites the mark, mirroring the live engine's "at most one
    /// armed at a time" model.
    StatementSavepoint,
    /// Roll back to the most recently marked implicit statement savepoint:
    /// recovery discards every buffered operation recorded after the matching
    /// `StatementSavepoint` marker (the marker itself, and everything before
    /// it, survives).
    RollbackStatementSavepoint,

    // Checkpoint Operations
    /// Begin a checkpoint
    CheckpointBegin { checkpoint_id: u64 },
    /// Complete a checkpoint (all data up to this LSN is persisted)
    CheckpointComplete { checkpoint_id: u64, lsn: Lsn },
}

/// Full definition of a B-tree index, carried by [`WalOp::CreateIndex`] as of
/// WAL format v6 (issue #6741) so crash recovery can rebuild the index exactly
/// as the live `CREATE INDEX` created it.
///
/// Mirrors the per-index record of the binary checkpoint catalog
/// (`persistence/binary/catalog.rs`): key-part expressions and the partial
/// `WHERE` predicate are serialized as SQL text and re-parsed with the full
/// main-parser grammar on decode.
#[derive(Debug, Clone, PartialEq)]
pub struct WalIndexDefinition {
    /// The index's stored table identity (usually the bare table name — see
    /// `Database::create_index_for_table` for why it is kept bare).
    pub table_name: String,
    /// The schema-qualified name the index body was built against (e.g.
    /// `main.t`), used to resolve the physical table during replay.
    pub qualified_table_name: String,
    /// The schema that owns the index (e.g. `main`).
    pub schema: String,
    /// Key parts: plain columns (with direction, prefix length, collation and
    /// quoting) and/or expressions (with direction).
    pub columns: Vec<vibesql_ast::IndexColumn>,
    /// Partial-index predicate (`CREATE INDEX ... WHERE expr`), if any.
    pub where_clause: Option<vibesql_ast::Expression>,
    /// Verbatim `CREATE INDEX` text for `sqlite_master.sql` (issue #6734).
    pub sql_source: Option<String>,
    /// Which kind of index this is, plus its kind-specific parameters (WAL
    /// format v7, issue #6758). Logs older than v7 only ever carried B-tree
    /// definitions, so they decode as [`WalIndexKind::BTree`].
    pub kind: WalIndexKind,
}

/// Kind of index described by a [`WalIndexDefinition`] (WAL format v7, issue
/// #6758), with the per-kind build parameters recovery needs to rebuild it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalIndexKind {
    /// Ordinary B-tree index (also every definition from a v6 log).
    BTree,
    /// Spatial (R-tree) index over one geometry column.
    Spatial,
    /// IVFFlat vector index.
    IVFFlat { metric: vibesql_ast::VectorDistanceMetric, lists: u32 },
    /// HNSW vector index.
    Hnsw { metric: vibesql_ast::VectorDistanceMetric, m: u32, ef_construction: u32 },
}

/// Owning schema + table of a dropped index, carried by [`WalOp::DropIndex`]
/// as of WAL format v6 (issue #6741).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalIndexOwner {
    /// The schema that owned the index (e.g. `main`).
    pub schema: String,
    /// The index's stored table identity.
    pub table_name: String,
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
    StatementSavepoint = 0x25,
    RollbackStatementSavepoint = 0x26,
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
            0x25 => Ok(WalOpTag::StatementSavepoint),
            0x26 => Ok(WalOpTag::RollbackStatementSavepoint),
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
            WalOp::CreateIndex {
                index_id,
                index_name,
                table_id,
                column_indices,
                is_unique,
                definition,
            } => {
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
                // WAL format v6 (issue #6741): full index definition trailer.
                match definition {
                    Some(def) => {
                        write_bool(writer, true)?;
                        write_index_definition(writer, def)?;
                    }
                    None => write_bool(writer, false)?,
                }
            }
            WalOp::DropIndex { index_id, index_name, owner } => {
                writer
                    .write_all(&[WalOpTag::DropIndex as u8])
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
                write_u32(writer, *index_id)?;
                write_string(writer, index_name)?;
                // WAL format v6 (issue #6741): owning schema + table trailer.
                match owner {
                    Some(owner) => {
                        write_bool(writer, true)?;
                        write_string(writer, &owner.schema)?;
                        write_string(writer, &owner.table_name)?;
                    }
                    None => write_bool(writer, false)?,
                }
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
            WalOp::StatementSavepoint => {
                writer
                    .write_all(&[WalOpTag::StatementSavepoint as u8])
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
            }
            WalOp::RollbackStatementSavepoint => {
                writer
                    .write_all(&[WalOpTag::RollbackStatementSavepoint as u8])
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
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
                // WAL format v6 (issue #6741): full index definition trailer.
                // Absent in v5-and-earlier logs — such entries decode as the
                // thin op and keep their historical (log-only) replay.
                let definition = if version >= 6 && read_bool(reader)? {
                    Some(read_index_definition(reader, version)?)
                } else {
                    None
                };
                Ok(WalOp::CreateIndex {
                    index_id,
                    index_name,
                    table_id,
                    column_indices,
                    is_unique,
                    definition,
                })
            }
            WalOpTag::DropIndex => {
                let index_id = read_u32(reader)?;
                let index_name = read_string(reader)?;
                // WAL format v6 (issue #6741): owning schema + table trailer.
                let owner = if version >= 6 && read_bool(reader)? {
                    let schema = read_string(reader)?;
                    let table_name = read_string(reader)?;
                    Some(WalIndexOwner { schema, table_name })
                } else {
                    None
                };
                Ok(WalOp::DropIndex { index_id, index_name, owner })
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
            WalOpTag::StatementSavepoint => Ok(WalOp::StatementSavepoint),
            WalOpTag::RollbackStatementSavepoint => Ok(WalOp::RollbackStatementSavepoint),
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

fn write_optional_string<W: Write>(writer: &mut W, s: Option<&str>) -> Result<(), StorageError> {
    match s {
        Some(s) => {
            write_bool(writer, true)?;
            write_string(writer, s)
        }
        None => write_bool(writer, false),
    }
}

fn read_optional_string<R: Read>(reader: &mut R) -> Result<Option<String>, StorageError> {
    if read_bool(reader)? {
        Ok(Some(read_string(reader)?))
    } else {
        Ok(None)
    }
}

fn write_direction<W: Write>(
    writer: &mut W,
    direction: &vibesql_ast::OrderDirection,
) -> Result<(), StorageError> {
    let byte = match direction {
        vibesql_ast::OrderDirection::Asc => 0u8,
        vibesql_ast::OrderDirection::Desc => 1u8,
    };
    write_u8(writer, byte)
}

fn read_direction<R: Read>(reader: &mut R) -> Result<vibesql_ast::OrderDirection, StorageError> {
    match read_u8(reader)? {
        0 => Ok(vibesql_ast::OrderDirection::Asc),
        1 => Ok(vibesql_ast::OrderDirection::Desc),
        other => Err(StorageError::IoError(format!(
            "Invalid index key-part direction in WAL CreateIndex: {}",
            other
        ))),
    }
}

/// Parse SQL expression text persisted by [`write_index_definition`]. Uses the
/// full main-parser grammar, exactly like the binary checkpoint load path
/// (issue #5833), so every form accepted at CREATE INDEX time round-trips.
fn parse_persisted_expression(
    sql: &str,
    what: &str,
) -> Result<vibesql_ast::Expression, StorageError> {
    vibesql_parser::Parser::parse_expression_sql(sql).map_err(|e| {
        StorageError::IoError(format!("Failed to parse WAL CreateIndex {} '{}': {}", what, sql, e))
    })
}

/// Serialize a [`WalIndexDefinition`] (WAL format v6+).
///
/// Layout: table_name, qualified_table_name, schema, key-part count, then per
/// key part a type byte (0 = column, 1 = expression) followed by
/// - column: name, direction, prefix-length flag (+u64), collation flag (+string), quoted flag
/// - expression: SQL text, direction
///
/// then the optional `WHERE` predicate SQL and the optional verbatim
/// `CREATE INDEX` text.
fn write_index_definition<W: Write>(
    writer: &mut W,
    def: &WalIndexDefinition,
) -> Result<(), StorageError> {
    use vibesql_ast::pretty_print::ToSql;

    write_string(writer, &def.table_name)?;
    write_string(writer, &def.qualified_table_name)?;
    write_string(writer, &def.schema)?;
    write_u32(writer, def.columns.len() as u32)?;
    for col in &def.columns {
        match col {
            vibesql_ast::IndexColumn::Column {
                column_name,
                direction,
                prefix_length,
                collation,
                is_quoted,
            } => {
                write_u8(writer, 0)?;
                write_string(writer, column_name)?;
                write_direction(writer, direction)?;
                match prefix_length {
                    Some(len) => {
                        write_bool(writer, true)?;
                        write_u64(writer, *len)?;
                    }
                    None => write_bool(writer, false)?,
                }
                write_optional_string(writer, collation.as_deref())?;
                write_bool(writer, *is_quoted)?;
            }
            vibesql_ast::IndexColumn::Expression { expr, direction } => {
                write_u8(writer, 1)?;
                write_string(writer, &expr.to_sql())?;
                write_direction(writer, direction)?;
            }
        }
    }
    let where_sql = def.where_clause.as_ref().map(|expr| expr.to_sql());
    write_optional_string(writer, where_sql.as_deref())?;
    write_optional_string(writer, def.sql_source.as_deref())?;
    // WAL format v7 (issue #6758): index-kind discriminator + parameters.
    match def.kind {
        WalIndexKind::BTree => write_u8(writer, 0)?,
        WalIndexKind::Spatial => write_u8(writer, 1)?,
        WalIndexKind::IVFFlat { metric, lists } => {
            write_u8(writer, 2)?;
            write_metric(writer, metric)?;
            write_u32(writer, lists)?;
        }
        WalIndexKind::Hnsw { metric, m, ef_construction } => {
            write_u8(writer, 3)?;
            write_metric(writer, metric)?;
            write_u32(writer, m)?;
            write_u32(writer, ef_construction)?;
        }
    }
    Ok(())
}

fn write_metric<W: Write>(
    writer: &mut W,
    metric: vibesql_ast::VectorDistanceMetric,
) -> Result<(), StorageError> {
    write_u8(
        writer,
        match metric {
            vibesql_ast::VectorDistanceMetric::L2 => 0,
            vibesql_ast::VectorDistanceMetric::Cosine => 1,
            vibesql_ast::VectorDistanceMetric::InnerProduct => 2,
        },
    )
}

fn read_metric<R: Read>(reader: &mut R) -> Result<vibesql_ast::VectorDistanceMetric, StorageError> {
    match read_u8(reader)? {
        0 => Ok(vibesql_ast::VectorDistanceMetric::L2),
        1 => Ok(vibesql_ast::VectorDistanceMetric::Cosine),
        2 => Ok(vibesql_ast::VectorDistanceMetric::InnerProduct),
        other => Err(StorageError::IoError(format!(
            "Invalid vector distance metric in WAL CreateIndex: {}",
            other
        ))),
    }
}

/// Deserialize a [`WalIndexDefinition`] written by [`write_index_definition`].
fn read_index_definition<R: Read>(
    reader: &mut R,
    version: u32,
) -> Result<WalIndexDefinition, StorageError> {
    let table_name = read_string(reader)?;
    let qualified_table_name = read_string(reader)?;
    let schema = read_string(reader)?;
    let column_count = read_u32(reader)? as usize;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let column = match read_u8(reader)? {
            0 => {
                let column_name = read_string(reader)?;
                let direction = read_direction(reader)?;
                let prefix_length = if read_bool(reader)? { Some(read_u64(reader)?) } else { None };
                let collation = read_optional_string(reader)?;
                let is_quoted = read_bool(reader)?;
                vibesql_ast::IndexColumn::Column {
                    column_name,
                    direction,
                    prefix_length,
                    collation,
                    is_quoted,
                }
            }
            1 => {
                let sql = read_string(reader)?;
                let direction = read_direction(reader)?;
                let expr = parse_persisted_expression(&sql, "key expression")?;
                vibesql_ast::IndexColumn::Expression { expr: Box::new(expr), direction }
            }
            other => {
                return Err(StorageError::IoError(format!(
                    "Invalid index key-part type in WAL CreateIndex: {}",
                    other
                )))
            }
        };
        columns.push(column);
    }
    let where_clause = match read_optional_string(reader)? {
        Some(sql) => Some(parse_persisted_expression(&sql, "partial-index WHERE predicate")?),
        None => None,
    };
    let sql_source = read_optional_string(reader)?;
    // v6 logs carry only B-tree definitions; v7+ adds the kind discriminator.
    let kind = if version >= 7 {
        match read_u8(reader)? {
            0 => WalIndexKind::BTree,
            1 => WalIndexKind::Spatial,
            2 => {
                let metric = read_metric(reader)?;
                WalIndexKind::IVFFlat { metric, lists: read_u32(reader)? }
            }
            3 => {
                let metric = read_metric(reader)?;
                let m = read_u32(reader)?;
                WalIndexKind::Hnsw { metric, m, ef_construction: read_u32(reader)? }
            }
            other => {
                return Err(StorageError::IoError(format!(
                    "Invalid index kind in WAL CreateIndex: {}",
                    other
                )))
            }
        }
    } else {
        WalIndexKind::BTree
    };
    Ok(WalIndexDefinition {
        table_name,
        qualified_table_name,
        schema,
        columns,
        where_clause,
        sql_source,
        kind,
    })
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
                definition: None,
            },
        );

        let mut buf = Vec::new();
        entry.serialize(&mut buf).unwrap();

        let mut reader = &buf[..];
        let decoded = WalEntry::deserialize(&mut reader).unwrap();

        assert_eq!(entry, decoded);
    }

    fn parse_expr(sql: &str) -> vibesql_ast::Expression {
        vibesql_parser::Parser::parse_expression_sql(sql).unwrap()
    }

    /// A v6 `CreateIndex` carrying the full definition (plain column with
    /// collation/quoting/DESC, an expression key part, a partial WHERE
    /// predicate, the owning schema, and the verbatim CREATE INDEX text)
    /// round-trips losslessly (issue #6741).
    #[test]
    fn test_wal_entry_roundtrip_create_index_with_definition() {
        let definition = WalIndexDefinition {
            table_name: "users".to_string(),
            qualified_table_name: "main.users".to_string(),
            schema: "main".to_string(),
            columns: vec![
                vibesql_ast::IndexColumn::Column {
                    column_name: "Email".to_string(),
                    direction: vibesql_ast::OrderDirection::Desc,
                    prefix_length: Some(8),
                    collation: Some("NOCASE".to_string()),
                    is_quoted: true,
                },
                vibesql_ast::IndexColumn::Expression {
                    expr: Box::new(parse_expr("lower(name) || 'x'")),
                    direction: vibesql_ast::OrderDirection::Asc,
                },
            ],
            where_clause: Some(parse_expr("age > 18 AND name IS NOT NULL")),
            sql_source: Some(
                "CREATE UNIQUE INDEX  idx_users ON users(\"Email\" COLLATE NOCASE DESC, \
                 lower(name) || 'x') WHERE age > 18 AND name IS NOT NULL"
                    .to_string(),
            ),
            kind: WalIndexKind::BTree,
        };
        let entry = WalEntry::new(
            6,
            1234567895,
            WalOp::CreateIndex {
                index_id: 11,
                index_name: "idx_users".to_string(),
                table_id: 1,
                column_indices: vec![2, 0xFFFF_FFFF],
                is_unique: true,
                definition: Some(definition),
            },
        );

        let mut buf = Vec::new();
        entry.serialize(&mut buf).unwrap();

        let mut reader = &buf[..];
        let decoded = WalEntry::deserialize(&mut reader).unwrap();

        assert_eq!(entry, decoded);
        assert!(reader.is_empty(), "the whole entry must be consumed");
    }

    /// A `DropIndex` carrying its owning schema/table round-trips (v6+).
    #[test]
    fn test_wal_entry_roundtrip_drop_index_with_owner() {
        for owner in [
            None,
            Some(WalIndexOwner { schema: "main".to_string(), table_name: "users".to_string() }),
        ] {
            let entry = WalEntry::new(
                7,
                1234567896,
                WalOp::DropIndex { index_id: 11, index_name: "idx_users".to_string(), owner },
            );

            let mut buf = Vec::new();
            entry.serialize(&mut buf).unwrap();

            let mut reader = &buf[..];
            let decoded = WalEntry::deserialize(&mut reader).unwrap();

            assert_eq!(entry, decoded);
            assert!(reader.is_empty(), "the whole entry must be consumed");
        }
    }

    /// Backward compatibility (issue #6741): a `CreateIndex`/`DropIndex` entry
    /// written by a v5 binary has no definition/owner trailer. Decoding it at
    /// version 5 must consume exactly the v5 layout and yield the thin op
    /// (`definition`/`owner` = `None`), so the entry that follows still
    /// decodes correctly.
    #[test]
    fn test_v5_create_and_drop_index_decode_as_thin_ops() {
        // Hand-build the v5 on-disk layout for two consecutive entries.
        let mut buf = Vec::new();
        // Entry 1: CreateIndex (v5 layout: no trailer).
        write_u64(&mut buf, 1).unwrap();
        write_u64(&mut buf, 100).unwrap();
        buf.push(WalOpTag::CreateIndex as u8);
        write_u32(&mut buf, 42).unwrap();
        write_string(&mut buf, "idx_v5").unwrap();
        write_u32(&mut buf, 7).unwrap();
        write_u32(&mut buf, 1).unwrap();
        write_u32(&mut buf, 0).unwrap();
        write_bool(&mut buf, false).unwrap();
        // Entry 2: DropIndex (v5 layout: no trailer).
        write_u64(&mut buf, 2).unwrap();
        write_u64(&mut buf, 101).unwrap();
        buf.push(WalOpTag::DropIndex as u8);
        write_u32(&mut buf, 42).unwrap();
        write_string(&mut buf, "idx_v5").unwrap();
        // Entry 3: a TxnCommit, to prove the stream stayed aligned.
        write_u64(&mut buf, 3).unwrap();
        write_u64(&mut buf, 102).unwrap();
        buf.push(WalOpTag::TxnCommit as u8);
        write_u64(&mut buf, 9).unwrap();

        let mut reader = &buf[..];
        let e1 = WalEntry::deserialize_versioned(&mut reader, 5).unwrap();
        assert_eq!(
            e1.op,
            WalOp::CreateIndex {
                index_id: 42,
                index_name: "idx_v5".to_string(),
                table_id: 7,
                column_indices: vec![0],
                is_unique: false,
                definition: None,
            }
        );
        let e2 = WalEntry::deserialize_versioned(&mut reader, 5).unwrap();
        assert_eq!(
            e2.op,
            WalOp::DropIndex { index_id: 42, index_name: "idx_v5".to_string(), owner: None }
        );
        let e3 = WalEntry::deserialize_versioned(&mut reader, 5).unwrap();
        assert_eq!(e3.op, WalOp::TxnCommit { txn_id: 9 });
        assert!(reader.is_empty());
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
    fn test_wal_entry_roundtrip_statement_savepoint_ops() {
        let entries = vec![
            WalEntry::new(13, 1234567902, WalOp::StatementSavepoint),
            WalEntry::new(14, 1234567903, WalOp::RollbackStatementSavepoint),
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
    fn kind_entry(kind: WalIndexKind) -> WalEntry {
        WalEntry::new(
            7,
            1234567896,
            WalOp::CreateIndex {
                index_id: 12,
                index_name: "idx_v".to_string(),
                table_id: 1,
                column_indices: vec![1],
                is_unique: false,
                definition: Some(WalIndexDefinition {
                    table_name: "docs".to_string(),
                    qualified_table_name: "main.docs".to_string(),
                    schema: "main".to_string(),
                    columns: vec![vibesql_ast::IndexColumn::Column {
                        column_name: "emb".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                        collation: None,
                        is_quoted: false,
                    }],
                    where_clause: None,
                    sql_source: None,
                    kind,
                }),
            },
        )
    }

    /// Spatial / IVFFlat / HNSW definitions round-trip with their parameters
    /// (WAL format v7, issue #6758).
    #[test]
    fn test_wal_entry_roundtrip_spatial_and_vector_index_kinds() {
        use vibesql_ast::VectorDistanceMetric as M;
        for kind in [
            WalIndexKind::Spatial,
            WalIndexKind::IVFFlat { metric: M::Cosine, lists: 7 },
            WalIndexKind::Hnsw { metric: M::InnerProduct, m: 12, ef_construction: 80 },
        ] {
            let entry = kind_entry(kind);
            let mut buf = Vec::new();
            entry.serialize(&mut buf).unwrap();
            let decoded = WalEntry::deserialize(&mut &buf[..]).unwrap();
            assert_eq!(entry, decoded);
        }
    }

    /// A v6 log's definition has no kind trailer: it must still decode, as a
    /// B-tree (issue #6758 backward compatibility).
    #[test]
    fn test_v6_create_index_definition_decodes_as_btree() {
        let entry = kind_entry(WalIndexKind::BTree);
        let mut buf = Vec::new();
        entry.serialize(&mut buf).unwrap();
        // Strip the v7 trailer (a single kind byte for B-tree) to produce the
        // exact v6 layout.
        buf.pop();
        let decoded = WalEntry::deserialize_versioned(&mut &buf[..], 6).unwrap();
        assert_eq!(entry, decoded);
    }
}
