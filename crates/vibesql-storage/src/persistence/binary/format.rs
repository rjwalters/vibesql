// ============================================================================
// Binary File Format: Header and Type Tags
// ============================================================================
//
// Handles file format constants, version management, and type tag definitions.

use std::io::{Read, Write};

use crate::StorageError;

/// Magic number for vibesql binary format: "VBSQL" in ASCII
pub const MAGIC: &[u8; 5] = b"VBSQL";

/// Current format version
/// Version history:
/// - v1: Initial binary format
/// - v2: Added sequence persistence and column default_value expressions
/// - v3: Added primary key and unique constraints persistence
/// - v4: Added quoted flag for TableIdentifier (SQL:1999 case-sensitivity)
/// - v5: Added column-level collation persistence
/// - v6: Added expression index support (type_byte before each index column)
/// - v7: Added MVCC version fields (`xmin: u64` + `xmax: Option<u64>`) per row.
///       Phase 1a of #5136 / parent #4460. v6 files remain readable: the v6
///       row layout has no MVCC prefix, so the read path treats absent fields
///       as `xmin = PRE_MVCC_TXN_ID (= 0), xmax = None` — meaning "always
///       committed, visible to every snapshot." Write path always emits v7.
/// - v8: Added partial-index WHERE clause persistence per index. v7 files
///       remain readable: the read path treats absent partial-index data as
///       "no WHERE clause" (full index). Issue #5181.
/// - v9: Added verbatim `TableSchema::sql_source` persistence per table (the
///       byte-for-byte original `CREATE TABLE` text shown in
///       `sqlite_master.sql`). v8 and earlier files remain readable: the read
///       path treats the absent field as `sql_source = None`, falling back to
///       the reconstructed CREATE TABLE text. Issue #5619.
/// - v10: Added view persistence. The catalog serializer now emits a views
///        section (name, schema, optional column list, with_check_option, the
///        defining SELECT as SQL text, and the verbatim `sql_definition`) just
///        before the triggers section, so view-dependent INSTEAD OF triggers
///        resolve during recovery. v9 and earlier files remain readable: the
///        read path is gated on `version >= 10` and treats absence as "zero
///        views." A v10 file opened by a v9 binary is rejected cleanly by
///        `read_header` (version > VERSION). Issue #5771.
/// - v11: Added generated-column expression persistence per column
///        (`ColumnSchema::generated_expr`, the `c AS (a+b)` in `CREATE TABLE`).
///        Generated values are materialized at INSERT time, so rows written
///        before a save always reload correctly — but without the expression
///        the reloaded schema is amnesiac and post-reload INSERTs store NULL
///        for the generated column. Encoded like `default_value`:
///        present-flag bool + expression, after the collation field. v10 and
///        earlier files remain readable: the read path is gated on
///        `version >= 11` and treats absence as `generated_expr = None`
///        (prior behavior). Issue #5794.
/// - v12: Added `TableSchema::without_rowid` persistence per table (one bool
///        after `sql_source`). Without it, a reloaded WITHOUT ROWID table
///        forgets it is rowid-less, so `sqlite_master` wrongly lists its
///        implicit PRIMARY KEY autoindex (SQLite never lists a WITHOUT ROWID
///        PK autoindex — the PK *is* the table b-tree; alterdropcol 7.2).
///        v11 and earlier files remain readable: the read path is gated on
///        `version >= 12` and treats absence as `without_rowid = false`
///        (prior behavior). Issue #5796.
/// - v13: Added per-row rowid persistence (one u64 per row, after the MVCC
///        version prefix and before the column values). Without it, every
///        reloaded row lost its SQLite rowid and was silently renumbered by
///        physical position, so `WHERE rowid=N` / `DELETE ... WHERE rowid=N`
///        targeted different rows before and after a process restart, and
///        new inserts could collide with reloaded explicit rowids. The write
///        path materializes each live row's effective rowid (the rowid-alias
///        INTEGER PRIMARY KEY value when the table has one, else the row's
///        explicit rowid, else its implicit physical position + 1). v12 and
///        earlier files remain readable: the read path is gated on
///        `version >= 13` and treats absence as "no explicit rowid" (prior
///        renumbering behavior). Issue #5835.
/// - v14: Added `TriggerDefinition::schema` persistence per trigger (one
///        present-flag bool + optional string, appended after the trigger's
///        triggered_action). Without it, every reloaded trigger lost its schema
///        tag and became a main-schema trigger, so a trigger that SQLite binds
///        to a temp-table namesake was silently rebound to the main table after
///        a checkpoint. The same change filters temp views and temp triggers
///        (`is_temp()`) out of the catalog write entirely, since session-scoped
///        temp objects must not survive a checkpoint at all. v13 and earlier
///        files remain readable: the read path is gated on `version >= 14` and
///        treats absence as `schema = None` (prior behavior). Issue #5940.
/// - v15: Added per-index-key-part explicit collation persistence
///        (`IndexColumn::Column::collation`, the `COLLATE nocase` in
///        `CREATE UNIQUE INDEX xyz1 ON xyz(d, c, b COLLATE nocase)`). Encoded as
///        a present-flag bool + optional collation name, appended after the
///        column's direction byte, and only for column-type index parts
///        (type byte 0); expression parts carry any collation inside their SQL
///        text. Without it, a reloaded index forgot its key-part collation, so
///        an upsert conflict target such as `ON CONFLICT(b COLLATE nocase, ...)`
///        stopped matching after a checkpoint (upsert4 2.x.2.1). v14 and earlier
///        files remain readable: the read path is gated on `version >= 15` and
///        treats absence as `collation = None` (prior behavior). Issue #5921.
/// - v16: Added `TriggerDefinition::sql_definition` persistence per trigger (one
///        present-flag bool + optional string, appended after the trigger's
///        schema field). Without it, every reloaded trigger lost its verbatim
///        `CREATE TRIGGER` text and `sqlite_master.sql` fell back to an
///        AST-reconstructed form (injected `BEFORE`/`FOR EACH ROW`, normalized
///        spacing), so a trigger's `sql` column changed after a checkpoint
///        (altercol.test 9.x). Mirrors the view `sql_definition` field. v15 and
///        earlier files remain readable: the read path is gated on
///        `version >= 16` and treats absence as `sql_definition = None` (prior
///        AST-reconstruction behavior). Issue #6174.
pub const VERSION: u8 = 16;

/// Type tags for binary serialization
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeTag {
    Null = 0x00,
    Smallint = 0x01,
    Integer = 0x02,
    Bigint = 0x03,
    Unsigned = 0x04,
    Numeric = 0x05,
    Float = 0x06,
    Real = 0x07,
    Double = 0x08,
    Character = 0x10,
    Varchar = 0x11,
    Boolean = 0x20,
    Date = 0x30,
    Time = 0x31,
    Timestamp = 0x32,
    Interval = 0x33,
    Vector = 0x40,
    Blob = 0x50,
}

impl TypeTag {
    pub fn from_u8(tag: u8) -> Result<Self, StorageError> {
        match tag {
            0x00 => Ok(TypeTag::Null),
            0x01 => Ok(TypeTag::Smallint),
            0x02 => Ok(TypeTag::Integer),
            0x03 => Ok(TypeTag::Bigint),
            0x04 => Ok(TypeTag::Unsigned),
            0x05 => Ok(TypeTag::Numeric),
            0x06 => Ok(TypeTag::Float),
            0x07 => Ok(TypeTag::Real),
            0x08 => Ok(TypeTag::Double),
            0x10 => Ok(TypeTag::Character),
            0x11 => Ok(TypeTag::Varchar),
            0x20 => Ok(TypeTag::Boolean),
            0x30 => Ok(TypeTag::Date),
            0x31 => Ok(TypeTag::Time),
            0x32 => Ok(TypeTag::Timestamp),
            0x33 => Ok(TypeTag::Interval),
            0x40 => Ok(TypeTag::Vector),
            0x50 => Ok(TypeTag::Blob),
            _ => Err(StorageError::NotImplemented(format!("Unknown type tag: 0x{:02X}", tag))),
        }
    }
}

/// Write binary file header
///
/// Format:
/// - Magic number (5 bytes)
/// - Version (1 byte)
/// - Flags (1 byte) - reserved for future use
/// - Reserved (9 bytes)
///
/// Total: 16 bytes
pub fn write_header<W: Write>(writer: &mut W) -> Result<(), StorageError> {
    // Magic number (5 bytes)
    writer
        .write_all(MAGIC)
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

    // Version (1 byte)
    writer
        .write_all(&[VERSION])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

    // Flags (1 byte) - reserved for future use (compression, encryption, etc.)
    writer
        .write_all(&[0])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

    // Reserved (9 bytes)
    writer
        .write_all(&[0; 9])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

    Ok(())
}

/// Read and validate binary file header
/// Returns the format version for backward compatibility handling
pub fn read_header<R: Read>(reader: &mut R) -> Result<u8, StorageError> {
    // Read and validate magic number
    let mut magic = [0u8; 5];
    reader
        .read_exact(&mut magic)
        .map_err(|e| StorageError::NotImplemented(format!("Failed to read header: {}", e)))?;

    if &magic != MAGIC {
        return Err(StorageError::NotImplemented(format!(
            "Invalid file format: expected VBSQL magic number, got {:?}",
            String::from_utf8_lossy(&magic)
        )));
    }

    // Read and validate version
    let mut version = [0u8; 1];
    reader
        .read_exact(&mut version)
        .map_err(|e| StorageError::NotImplemented(format!("Failed to read version: {}", e)))?;

    if version[0] > VERSION {
        // Typed forward-version error (issue #5807): callers (checkpoint
        // recovery, CLI open) must treat this as fatal — never fall back to an
        // older checkpoint or an empty database, and never mask it behind a
        // SQL-dump reparse attempt.
        return Err(StorageError::UnsupportedFormatVersion {
            found: version[0],
            supported: VERSION,
        });
    }

    // Read flags (reserved for future use)
    let mut flags = [0u8; 1];
    reader
        .read_exact(&mut flags)
        .map_err(|e| StorageError::NotImplemented(format!("Failed to read flags: {}", e)))?;

    // Skip reserved bytes
    let mut reserved = [0u8; 9];
    reader.read_exact(&mut reserved).map_err(|e| {
        StorageError::NotImplemented(format!("Failed to read reserved bytes: {}", e))
    })?;

    Ok(version[0])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let mut buf = Vec::new();
        write_header(&mut buf).unwrap();

        assert_eq!(buf.len(), 16);
        assert_eq!(&buf[0..5], MAGIC);
        assert_eq!(buf[5], VERSION);

        let mut reader = &buf[..];
        let version = read_header(&mut reader).unwrap();
        assert_eq!(version, VERSION);
    }

    /// Issue #5807: a header claiming a format version newer than this binary
    /// must surface as the typed `UnsupportedFormatVersion` error so open
    /// paths can hard-error instead of silently falling back.
    #[test]
    fn test_read_header_forward_version_is_typed_error() {
        let mut buf = Vec::new();
        write_header(&mut buf).unwrap();
        buf[5] = VERSION + 1;

        let mut reader = &buf[..];
        let err = read_header(&mut reader).unwrap_err();
        assert_eq!(
            err,
            StorageError::UnsupportedFormatVersion { found: VERSION + 1, supported: VERSION }
        );
        let msg = err.to_string();
        assert!(msg.contains("newer version of VibeSQL"), "message was: {msg}");
    }
}
