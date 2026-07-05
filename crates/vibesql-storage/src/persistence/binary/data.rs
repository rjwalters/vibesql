// ============================================================================
// Table Data Serialization
// ============================================================================
//
// Handles reading and writing table row data.
//
// # MVCC version fields (Phase 1a of #5136)
//
// As of `format::VERSION = 7`, every row on disk carries two MVCC version
// fields written *before* its column values:
//
//   ┌──────────────────────────┬─────────────────────────────────────┐
//   │ xmin: u64                │ creator txn id                      │
//   ├──────────────────────────┼─────────────────────────────────────┤
//   │ xmax_present: u8 (0/1)   │ 0 → no xmax (live row)              │
//   │                          │ 1 → xmax present, read u64 next     │
//   ├──────────────────────────┼─────────────────────────────────────┤
//   │ xmax: u64 (optional)     │ deleter txn id, only if present == 1│
//   ├──────────────────────────┼─────────────────────────────────────┤
//   │ values...                │ column values, one per schema col   │
//   └──────────────────────────┴─────────────────────────────────────┘
//
// **Backwards compatibility:** v6 files predate this prefix and store rows
// as bare value sequences. `read_data` accepts the format `version` from the
// header and dispatches: v6 reads no MVCC prefix and stamps the resulting
// rows with the pre-MVCC sentinel (`xmin = PRE_MVCC_TXN_ID, xmax = None`),
// matching the semantics of "this row was committed before MVCC existed and
// is visible to every snapshot." v7 reads the prefix as documented above.

use std::io::{Read, Write};

use super::{
    io::*,
    value::{read_sql_value, write_sql_value},
};
use crate::{row::PRE_MVCC_TXN_ID, Database, Row, StorageError};

/// Tag byte for `Option<TxnId>::None` in the v7 row prefix.
const XMAX_TAG_NONE: u8 = 0;
/// Tag byte for `Option<TxnId>::Some(_)` in the v7 row prefix.
const XMAX_TAG_SOME: u8 = 1;

/// Write a single row's MVCC version prefix in v7 format.
///
/// Layout: `xmin (u64 LE) | xmax_tag (u8) | [xmax (u64 LE) if tag == 1]`.
fn write_row_version_prefix<W: Write>(writer: &mut W, row: &Row) -> Result<(), StorageError> {
    write_u64(writer, row.xmin)?;
    match row.xmax {
        None => write_u8(writer, XMAX_TAG_NONE)?,
        Some(xmax) => {
            write_u8(writer, XMAX_TAG_SOME)?;
            write_u64(writer, xmax)?;
        }
    }
    Ok(())
}

/// Read a single row's MVCC version prefix from v7 format.
///
/// Returns `(xmin, xmax)`. Errors on an unknown xmax tag.
fn read_row_version_prefix<R: Read>(
    reader: &mut R,
) -> Result<(u64, Option<u64>), StorageError> {
    let xmin = read_u64(reader)?;
    let tag = read_u8(reader)?;
    let xmax = match tag {
        XMAX_TAG_NONE => None,
        XMAX_TAG_SOME => Some(read_u64(reader)?),
        other => {
            return Err(StorageError::IoError(format!(
                "Invalid xmax tag in row version prefix: 0x{:02X} (expected 0x00 or 0x01)",
                other
            )))
        }
    };
    Ok((xmin, xmax))
}

pub fn write_data<W: Write>(writer: &mut W, db: &Database) -> Result<(), StorageError> {
    let table_names = db.catalog.list_tables();

    for table_name in table_names {
        if let Some(table) = db.get_table(&table_name) {
            // Write table name
            write_string(writer, &table_name)?;

            // Write row count
            write_u64(writer, table.row_count() as u64)?;

            // Rowid-alias column (INTEGER PRIMARY KEY), if any: for aliased
            // tables the column value IS the rowid, so persist that (v13).
            let alias_idx = table.schema.rowid_alias_column;

            // Write each row (only live/non-deleted rows)
            // Using scan_live() to skip rows marked as deleted in the deletion bitmap.
            // This fixes a bug where deleted rows were being persisted to disk.
            //
            // v13 layout: per-row MVCC prefix (xmin + xmax), then the row's
            // effective rowid (u64), then column values. The effective rowid
            // is materialized at write time (issue #5835): the rowid-alias
            // IPK value when the table has one, else the row's explicit
            // rowid, else the implicit physical position + 1. Persisting it
            // keeps `WHERE rowid=N` stable across reloads even when live
            // rows shift physical positions (tombstones are dropped here).
            for (idx, row) in table.scan_live() {
                write_row_version_prefix(writer, row)?;
                let effective_rowid = alias_idx
                    .and_then(|i| match row.values.get(i) {
                        Some(vibesql_types::SqlValue::Integer(v)) => Some(*v as u64),
                        _ => None,
                    })
                    .or(row.row_id)
                    .unwrap_or((idx + 1) as u64);
                write_u64(writer, effective_rowid)?;
                for value in &row.values {
                    write_sql_value(writer, value)?;
                }
            }
        }
    }

    Ok(())
}

/// Read row data with version-aware MVCC prefix handling.
///
/// - For `version <= 6` (legacy `.vbsql` files): no MVCC prefix on disk; rows
///   are reconstructed with `xmin = PRE_MVCC_TXN_ID, xmax = None`. This is
///   the "always committed, visible to all snapshots" sentinel.
/// - For `version >= 7`: each row begins with the MVCC version prefix
///   documented in the module-level doc comment.
/// - For `version >= 13`: each row additionally carries its effective rowid
///   (u64) between the MVCC prefix and the column values (issue #5835).
///   Older files reconstruct rows with `row_id = None` (the pre-fix
///   physical-position renumbering).
pub fn read_data<R: Read>(
    reader: &mut R,
    db: &mut Database,
    version: u8,
) -> Result<(), StorageError> {
    let table_count = db.catalog.list_tables().len();

    // Track tables that were loaded so we can rebuild their indexes
    let mut loaded_tables = Vec::with_capacity(table_count);

    let has_mvcc_prefix = version >= 7;
    let has_rowid = version >= 13;

    for _ in 0..table_count {
        // Read table name from file (don't rely on list_tables() ordering)
        let table_name = read_string(reader)?;
        let row_count = read_u64(reader)?;

        // Get column count first
        let column_count = db
            .get_table(&table_name)
            .map(|t| t.schema.columns.len())
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

        // Now get mutable reference and insert rows
        if let Some(table) = db.get_table_mut(&table_name) {
            for _ in 0..row_count {
                // Read MVCC version prefix on v7+; on v6 use sentinel defaults.
                let (xmin, xmax) = if has_mvcc_prefix {
                    read_row_version_prefix(reader)?
                } else {
                    (PRE_MVCC_TXN_ID, None)
                };

                // Read the persisted effective rowid on v13+ (issue #5835).
                let row_id = if has_rowid { Some(read_u64(reader)?) } else { None };

                let mut values = Vec::with_capacity(column_count);
                for _ in 0..column_count {
                    values.push(read_sql_value(reader)?);
                }

                let mut row = Row::from_vec(values);
                row.xmin = xmin;
                row.xmax = xmax;
                row.row_id = row_id;

                table.insert(row).map_err(|e| {
                    StorageError::NotImplemented(format!("Failed to insert row: {}", e))
                })?;
            }
        }

        loaded_tables.push(table_name);
    }

    // Rebuild database-level indexes for all loaded tables.
    // This is necessary because indexes are created during catalog loading when tables
    // are empty, so the index data structures have no entries. After loading rows,
    // we need to populate the indexes with the actual data.
    // This fixes the bug where ORDER BY on indexed columns returns empty results
    // after loading a database from disk (see issue #3602).
    for table_name in loaded_tables {
        db.rebuild_indexes(&table_name);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use vibesql_types::SqlValue;

    use super::*;

    #[test]
    fn test_row_version_prefix_roundtrip_live_row() {
        let row = {
            let mut r = Row::from_vec(vec![SqlValue::Integer(42)]);
            r.xmin = 1234;
            r.xmax = None;
            r
        };

        let mut buf = Vec::new();
        write_row_version_prefix(&mut buf, &row).unwrap();

        let mut reader = Cursor::new(&buf);
        let (xmin, xmax) = read_row_version_prefix(&mut reader).unwrap();
        assert_eq!(xmin, 1234);
        assert_eq!(xmax, None);
    }

    #[test]
    fn test_row_version_prefix_roundtrip_deleted_row() {
        let row = {
            let mut r = Row::from_vec(vec![SqlValue::Integer(42)]);
            r.xmin = 100;
            r.xmax = Some(200);
            r
        };

        let mut buf = Vec::new();
        write_row_version_prefix(&mut buf, &row).unwrap();

        let mut reader = Cursor::new(&buf);
        let (xmin, xmax) = read_row_version_prefix(&mut reader).unwrap();
        assert_eq!(xmin, 100);
        assert_eq!(xmax, Some(200));
    }

    #[test]
    fn test_row_version_prefix_invalid_tag() {
        // Hand-craft a buffer with a bogus xmax tag.
        let mut buf = Vec::new();
        write_u64(&mut buf, 1).unwrap(); // xmin
        write_u8(&mut buf, 0xFF).unwrap(); // invalid tag

        let mut reader = Cursor::new(&buf);
        let result = read_row_version_prefix(&mut reader);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid xmax tag"));
    }

    #[test]
    fn test_row_version_prefix_byte_layout_live() {
        // Live row prefix is 9 bytes: 8 (xmin) + 1 (xmax tag = 0).
        let row = {
            let mut r = Row::from_vec(vec![]);
            r.xmin = 0xDEAD_BEEF_CAFE_F00D;
            r.xmax = None;
            r
        };

        let mut buf = Vec::new();
        write_row_version_prefix(&mut buf, &row).unwrap();
        assert_eq!(buf.len(), 9);
        assert_eq!(buf[8], XMAX_TAG_NONE);
    }

    #[test]
    fn test_row_version_prefix_byte_layout_deleted() {
        // Deleted row prefix is 17 bytes: 8 (xmin) + 1 (xmax tag = 1) + 8 (xmax).
        let row = {
            let mut r = Row::from_vec(vec![]);
            r.xmin = 1;
            r.xmax = Some(2);
            r
        };

        let mut buf = Vec::new();
        write_row_version_prefix(&mut buf, &row).unwrap();
        assert_eq!(buf.len(), 17);
        assert_eq!(buf[8], XMAX_TAG_SOME);
    }
}
