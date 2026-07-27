//! AUTOINCREMENT bookkeeping via a real `sqlite_sequence` table (issue #6173).
//!
//! SQLite's `AUTOINCREMENT` keyword (only legal on a single-column `INTEGER
//! PRIMARY KEY`, i.e. the rowid alias) guarantees a NULL-inserted rowid is
//! always larger than any rowid the table has EVER held — even across a full
//! `DELETE FROM <table>` and a process restart. A plain (non-AUTOINCREMENT)
//! `INTEGER PRIMARY KEY` has no such guarantee: once every row is deleted, the
//! next NULL insert reuses rowid 1.
//!
//! SQLite implements this by lazily creating a real table, `sqlite_sequence
//! (name, seq)`, the first time any `AUTOINCREMENT` table is created, and
//! maintaining one row per AUTOINCREMENT table: `seq` is the high-water mark
//! (the largest rowid the table has ever held, explicit or auto-generated).
//! VibeSQL mirrors this design exactly — `sqlite_sequence` is a genuine
//! catalog table (not a virtual/computed one), so ordinary `SELECT` / `UPDATE`
//! / `DELETE` against it flow through the normal executor pipeline for free,
//! and its row data persists via the same WAL/snapshot machinery as any user
//! table. Only the lazy creation, the per-INSERT bookkeeping, and the
//! DROP-TABLE cleanup are special-cased here.
//!
//! Reference: <https://sqlite.org/autoinc.html>

use vibesql_catalog::{ColumnSchema, TableIdentifier, TableSchema};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use crate::errors::ExecutorError;

/// The reserved system table name (SQLite's own spelling, always lowercase).
pub const SQLITE_SEQUENCE_TABLE: &str = "sqlite_sequence";

/// Whether `name` refers to the `sqlite_sequence` system table (case-insensitive,
/// matching SQLite's identifier folding).
pub fn is_sqlite_sequence_table(name: &str) -> bool {
    name.eq_ignore_ascii_case(SQLITE_SEQUENCE_TABLE)
}

/// Schema for the lazily-created `sqlite_sequence` table.
///
/// SQLite declares both columns with no type at all (`CREATE TABLE
/// sqlite_sequence(name,seq)`), which gives them BLOB/NONE affinity — no
/// coercion is applied to inserted/updated values (verified against sqlite3
/// 3.51.0: `UPDATE sqlite_sequence SET seq='a-string'` stores the literal
/// string, not an error or a coerced 0). VibeSQL's `DataType::BinaryLargeObject`
/// is exactly this "no affinity" passthrough type (used for any column
/// declared without a type, e.g. `CREATE TABLE t(a)` — see
/// `vibesql-parser/src/parser/create/table.rs`), so it round-trips arbitrary
/// values through a normal `UPDATE sqlite_sequence` unchanged.
fn sqlite_sequence_table_schema() -> TableSchema {
    let mut schema = TableSchema::new(
        SQLITE_SEQUENCE_TABLE.to_string(),
        vec![
            ColumnSchema::new("name".to_string(), DataType::BinaryLargeObject, true),
            ColumnSchema::new("seq".to_string(), DataType::BinaryLargeObject, true),
        ],
    );
    schema.set_sql_source(format!("CREATE TABLE {}(name,seq)", SQLITE_SEQUENCE_TABLE));
    schema
}

/// Lazily create the `sqlite_sequence` table in the CURRENT schema if it does
/// not already exist. Safe to call unconditionally — a no-op when the table
/// is already present (e.g. a second `CREATE TABLE ... AUTOINCREMENT` in the
/// same schema, or a table reloaded from a snapshot that already created it).
///
/// Uses [`Database::create_table_with_identifier`] directly, bypassing the
/// user-facing `sqlite_`-prefix reservation guard (`is_reserved_object_name`)
/// that would otherwise reject this name — this is engine-internal DDL, not
/// user-issued. Because that low-level API emits its own WAL `CreateTable` op
/// and inserts into both the catalog and storage, the table persists exactly
/// like any user table (issue #6173's core design point).
pub fn ensure_sqlite_sequence_table(database: &mut Database) -> Result<(), ExecutorError> {
    if database.catalog.get_table(SQLITE_SEQUENCE_TABLE).is_some() {
        return Ok(());
    }
    database
        .create_table_with_identifier(
            sqlite_sequence_table_schema(),
            TableIdentifier::new(SQLITE_SEQUENCE_TABLE, false),
        )
        .map_err(|e| ExecutorError::StorageError(e.to_string()))
}

/// SQLite's effective "read `seq` as an integer" semantics for the fallback
/// path used when computing the next AUTOINCREMENT rowid: a valid integer (or
/// an integer-valued real) parses as itself; NULL, a non-numeric TEXT value,
/// or a value that doesn't fit in `i64` (verified against sqlite3 3.51.0:
/// `UPDATE sqlite_sequence SET seq='-12345678901234567890'` — 20 digits,
/// outside `i64` range — behaves exactly like an unparseable value) all
/// return `None`, signaling "ignore me, fall back to the table's actual max
/// rowid" to the caller.
fn parse_seq_value(value: &SqlValue) -> Option<i64> {
    match value {
        SqlValue::Integer(v) | SqlValue::Bigint(v) => Some(*v),
        SqlValue::Smallint(v) => Some(*v as i64),
        SqlValue::Unsigned(v) => i64::try_from(*v).ok(),
        SqlValue::Real(f) | SqlValue::Double(f) | SqlValue::Numeric(f) => {
            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                Some(*f as i64)
            } else {
                None
            }
        }
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

/// Look up the `sqlite_sequence` row for `table_display_name` (exact,
/// case-sensitive match on `name` — SQLite compares with the default BINARY
/// collation). Returns `Some(parsed_seq)` when a row exists (`parsed_seq` is
/// `None` when the stored value is NULL/non-numeric/out-of-range — see
/// [`parse_seq_value`]), or `None` when no row exists for this table yet.
fn lookup_sequence_row(database: &Database, table_display_name: &str) -> Option<Option<i64>> {
    let table = database.get_table(SQLITE_SEQUENCE_TABLE)?;
    table.scan_live().find_map(|(_, row)| match row.values.first() {
        Some(SqlValue::Varchar(s)) if s.as_str() == table_display_name => {
            Some(row.values.get(1).and_then(parse_seq_value))
        }
        _ => None,
    })
}

/// The `sqlite_sequence.seq` high-water mark for `table_display_name`, or
/// `None` when no row exists yet, or the stored value is NULL/non-numeric/
/// out-of-range (see [`parse_seq_value`]) — either way, "nothing to combine
/// with the table's own max rowid".
///
/// Used both by [`compute_next_autoincrement_rowid`] (the INTEGER PRIMARY KEY
/// -named-column NULL-fill path in `insert/defaults.rs`) and by the
/// `rowid`/`_rowid_`/`oid` pseudo-column auto-allocation path in
/// `insert/execution.rs` — both paths auto-generate the SAME logical value
/// (the IPK column value IS the rowid) and must agree, or the row's *stored*
/// column value and its *tracked* high-water mark (`Table::max_rowid_signed`,
/// which future allocations read from) silently diverge.
pub fn sequence_high_water_mark(database: &Database, table_display_name: &str) -> Option<i64> {
    lookup_sequence_row(database, table_display_name).and_then(|v| v)
}

/// Compute the rowid a NULL/omitted INTEGER PRIMARY KEY insert into an
/// AUTOINCREMENT table must receive.
///
/// SQLite semantics (verified against sqlite3 3.51.0, autoinc.test): the new
/// rowid is `max(table's actual current max rowid, sqlite_sequence.seq) + 1`
/// — NOT simply `sqlite_sequence.seq + 1`. This is what makes a manually
/// lowered `UPDATE sqlite_sequence SET seq=5` a no-op when the table's real
/// max rowid is already higher, while a manually RAISED value (`seq=1234`
/// when the table's max is 124) IS honored. An invalid stored `seq` (NULL, a
/// non-numeric string, or one that doesn't fit `i64`) is ignored — the
/// computation falls back to the table's actual max rowid alone.
pub fn compute_next_autoincrement_rowid(
    database: &Database,
    storage_table_name: &str,
    table_display_name: &str,
) -> Result<i64, ExecutorError> {
    let table_max = database.get_table(storage_table_name).and_then(|t| t.max_rowid_signed());
    let seq_val = sequence_high_water_mark(database, table_display_name);
    let combined = match (table_max, seq_val) {
        (Some(a), Some(b)) => a.max(b),
        (Some(a), None) => a,
        (None, Some(b)) => b,
        (None, None) => 0,
    };
    combined
        .checked_add(1)
        .ok_or_else(|| ExecutorError::SqliteCompatError("database or disk is full".to_string()))
}

/// Bump (or lazily create) the `sqlite_sequence` entry for `table_display_name`
/// after a successfully-completed INSERT statement against an AUTOINCREMENT
/// table.
///
/// `min_value` is the highest rowid this statement considered assigning to a
/// row of the target table — explicit or auto-generated, and INCLUDING rows
/// later discarded by `OR IGNORE` / `ON CONFLICT DO NOTHING` (SQLite still
/// bumps the sequence for those, verified against sqlite3 3.51.0), or `0` when
/// the statement processed no candidate rows at all (e.g. an `INSERT ...
/// SELECT` whose SELECT yields zero rows still creates a `(table, 0)`
/// sqlite_sequence row on its first successful run — sqlite3 3.51.0,
/// autoinc-9.1). A statement that fails outright (no `OR IGNORE`/`ON CONFLICT`
/// rescue) never reaches this call, so its bookkeeping never happens —
/// matching SQLite's whole-statement rollback.
///
/// The stored value is always `max(existing stored value, min_value)` — a
/// smaller explicit/auto rowid never lowers the tracked high-water mark.
pub fn bump_sequence_after_insert(
    database: &mut Database,
    table_display_name: &str,
    min_value: i64,
) -> Result<(), ExecutorError> {
    ensure_sqlite_sequence_table(database)?;

    let existing = lookup_sequence_row(database, table_display_name);
    let existing_valid = existing.and_then(|v| v);
    let new_val = existing_valid.unwrap_or(i64::MIN).max(min_value);
    let row_exists = existing.is_some();

    if row_exists {
        let name = table_display_name.to_string();
        database
            .update_row_matching(
                SQLITE_SEQUENCE_TABLE,
                move |row| matches!(row.values.first(), Some(SqlValue::Varchar(s)) if s.as_str() == name),
                vec![("seq", SqlValue::Integer(new_val))],
            )
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
    } else {
        database
            .insert_row(
                SQLITE_SEQUENCE_TABLE,
                Row::new(vec![
                    SqlValue::Varchar(arcstr::ArcStr::from(table_display_name)),
                    SqlValue::Integer(new_val),
                ]),
            )
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
    }
    Ok(())
}

/// Remove the `sqlite_sequence` entry for a dropped AUTOINCREMENT table
/// (SQLite: `DROP TABLE` on an AUTOINCREMENT table removes its
/// `sqlite_sequence` row, but the `sqlite_sequence` table itself stays
/// behind). A no-op if `sqlite_sequence` doesn't exist (or has no row for this
/// table) — dropping a table before its first successful INSERT is normal.
pub fn remove_sequence_entry(
    database: &mut Database,
    table_display_name: &str,
) -> Result<(), ExecutorError> {
    if database.get_table(SQLITE_SEQUENCE_TABLE).is_none() {
        return Ok(());
    }
    let name = table_display_name.to_string();
    database
        .delete_row_matching(SQLITE_SEQUENCE_TABLE, move |row| {
            matches!(row.values.first(), Some(SqlValue::Varchar(s)) if s.as_str() == name)
        })
        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_sqlite_sequence_table() {
        assert!(is_sqlite_sequence_table("sqlite_sequence"));
        assert!(is_sqlite_sequence_table("SQLITE_SEQUENCE"));
        assert!(is_sqlite_sequence_table("Sqlite_Sequence"));
        assert!(!is_sqlite_sequence_table("sqlite_master"));
        assert!(!is_sqlite_sequence_table("t1"));
    }

    #[test]
    fn test_parse_seq_value() {
        assert_eq!(parse_seq_value(&SqlValue::Integer(1234)), Some(1234));
        assert_eq!(parse_seq_value(&SqlValue::Null), None);
        assert_eq!(parse_seq_value(&SqlValue::Varchar(arcstr::ArcStr::from("a-string"))), None);
        assert_eq!(
            parse_seq_value(&SqlValue::Varchar(arcstr::ArcStr::from("-12345678901234567890"))),
            None
        );
        assert_eq!(parse_seq_value(&SqlValue::Varchar(arcstr::ArcStr::from("42"))), Some(42));
    }

    #[test]
    fn test_ensure_sqlite_sequence_table_idempotent() {
        let mut db = Database::new();
        assert!(db.catalog.get_table(SQLITE_SEQUENCE_TABLE).is_none());
        ensure_sqlite_sequence_table(&mut db).unwrap();
        assert!(db.catalog.get_table(SQLITE_SEQUENCE_TABLE).is_some());
        // Calling again must not error (idempotent).
        ensure_sqlite_sequence_table(&mut db).unwrap();
    }

    #[test]
    fn test_bump_sequence_creates_and_upserts() {
        let mut db = Database::new();
        bump_sequence_after_insert(&mut db, "t1", 12).unwrap();
        assert_eq!(lookup_sequence_row(&db, "t1"), Some(Some(12)));

        // A smaller explicit value never lowers the tracked max.
        bump_sequence_after_insert(&mut db, "t1", 1).unwrap();
        assert_eq!(lookup_sequence_row(&db, "t1"), Some(Some(12)));

        // A larger value raises it.
        bump_sequence_after_insert(&mut db, "t1", 123).unwrap();
        assert_eq!(lookup_sequence_row(&db, "t1"), Some(Some(123)));

        // A second table is tracked independently.
        bump_sequence_after_insert(&mut db, "t2", 1).unwrap();
        assert_eq!(lookup_sequence_row(&db, "t2"), Some(Some(1)));
        assert_eq!(lookup_sequence_row(&db, "t1"), Some(Some(123)));
    }

    #[test]
    fn test_remove_sequence_entry() {
        let mut db = Database::new();
        bump_sequence_after_insert(&mut db, "t1", 5).unwrap();
        bump_sequence_after_insert(&mut db, "t2", 7).unwrap();
        remove_sequence_entry(&mut db, "t1").unwrap();
        assert_eq!(lookup_sequence_row(&db, "t1"), None);
        assert_eq!(lookup_sequence_row(&db, "t2"), Some(Some(7)));
    }
}
