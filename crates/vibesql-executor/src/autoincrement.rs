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

/// The schema-qualified physical name of the `sqlite_sequence` table living in
/// `owning_schema` (e.g. `main.sqlite_sequence` or `temp_7.sqlite_sequence`).
///
/// Each database — `main` plus every session's temp schema — keeps its OWN
/// `sqlite_sequence` table, exactly like SQLite (autoinc-4.x). The owning
/// schema is supplied EXPLICITLY by the caller (resolved from the original,
/// possibly-qualified statement text) rather than re-derived here from the
/// bare table name: a bare-name re-resolution re-applies temp-shadows-main and
/// therefore misroutes an explicitly-qualified `INSERT INTO main.t1` to
/// `temp.sqlite_sequence` whenever a same-named table exists in the temp
/// schema (issue #6350). This mirrors [`remove_sequence_entry`]'s API shape,
/// which `drop_table.rs`/`truncate` already use correctly.
fn sequence_table_in(owning_schema: &str) -> String {
    format!("{}.{}", owning_schema, SQLITE_SEQUENCE_TABLE)
}

/// Lazily create the `sqlite_sequence` table in `owning_schema` if it does not
/// already exist there. Safe to call unconditionally — a no-op when the table
/// is already present (e.g. a second `CREATE TABLE ... AUTOINCREMENT` in the
/// same schema, or a table reloaded from a snapshot that already created it).
///
/// `owning_schema` is the schema (`main` or a `temp_*` schema) that owns the
/// AUTOINCREMENT table, resolved by the caller from the original (possibly
/// schema-qualified) statement — NOT re-derived from the bare table name here
/// (issue #6350; see [`sequence_table_in`]). A `CREATE TEMP TABLE ...
/// AUTOINCREMENT` issued while the current schema is `main` thus still gets
/// its `sqlite_sequence` in the temp schema (autoinc-4.x).
///
/// Uses [`Database::create_table_with_identifier`] directly, bypassing the
/// user-facing `sqlite_`-prefix reservation guard (`is_reserved_object_name`)
/// that would otherwise reject this name — this is engine-internal DDL, not
/// user-issued. Because that low-level API emits its own WAL `CreateTable` op
/// and inserts into both the catalog and storage, the table persists exactly
/// like any user table (issue #6173's core design point).
pub fn ensure_sqlite_sequence_table(
    database: &mut Database,
    owning_schema: &str,
) -> Result<(), ExecutorError> {
    let qualified = sequence_table_in(owning_schema);
    if database.get_table(&qualified).is_some() {
        return Ok(());
    }
    database
        .create_table_with_identifier(
            sqlite_sequence_table_schema(),
            TableIdentifier::qualified(owning_schema, false, SQLITE_SEQUENCE_TABLE, false),
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
fn lookup_sequence_row(
    database: &Database,
    seq_table: &str,
    table_display_name: &str,
) -> Option<Option<i64>> {
    let table = database.get_table(seq_table)?;
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
/// `owning_schema` is the schema the statement's target table actually
/// resolved to (honoring any explicit `main.`/`temp.` qualifier — issue
/// #6350; see [`sequence_table_in`]).
///
/// Used both by [`compute_next_autoincrement_rowid`] (the INTEGER PRIMARY KEY
/// -named-column NULL-fill path in `insert/defaults.rs`) and by the
/// `rowid`/`_rowid_`/`oid` pseudo-column auto-allocation path in
/// `insert/execution.rs` — both paths auto-generate the SAME logical value
/// (the IPK column value IS the rowid) and must agree, or the row's *stored*
/// column value and its *tracked* high-water mark (`Table::max_rowid_signed`,
/// which future allocations read from) silently diverge.
pub fn sequence_high_water_mark(
    database: &Database,
    table_display_name: &str,
    owning_schema: &str,
) -> Option<i64> {
    let seq_table = sequence_table_in(owning_schema);
    lookup_sequence_row(database, &seq_table, table_display_name).and_then(|v| v)
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
///
/// `owning_schema` is the schema the statement's target table actually
/// resolved to (honoring any explicit `main.`/`temp.` qualifier — issue
/// #6350; see [`sequence_table_in`]).
pub fn compute_next_autoincrement_rowid(
    database: &Database,
    storage_table_name: &str,
    table_display_name: &str,
    owning_schema: &str,
) -> Result<i64, ExecutorError> {
    let table_max = database.get_table(storage_table_name).and_then(|t| t.max_rowid_signed());
    let seq_val = sequence_high_water_mark(database, table_display_name, owning_schema);
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
///
/// `owning_schema` is the schema the statement's target table actually
/// resolved to (honoring any explicit `main.`/`temp.` qualifier — issue
/// #6350; see [`sequence_table_in`]).
pub fn bump_sequence_after_insert(
    database: &mut Database,
    table_display_name: &str,
    owning_schema: &str,
    min_value: i64,
) -> Result<(), ExecutorError> {
    ensure_sqlite_sequence_table(database, owning_schema)?;

    // Target the `sqlite_sequence` in the SAME database as the table being
    // inserted into, so a main table's counter never lands in
    // `temp.sqlite_sequence` and vice-versa (autoinc-4.x, issues #6173/#6350).
    let seq_table = sequence_table_in(owning_schema);
    let existing = lookup_sequence_row(database, &seq_table, table_display_name);
    let existing_valid = existing.and_then(|v| v);
    let new_val = existing_valid.unwrap_or(i64::MIN).max(min_value);
    let row_exists = existing.is_some();

    if row_exists {
        let name = table_display_name.to_string();
        database
            .update_row_matching(
                &seq_table,
                move |row| matches!(row.values.first(), Some(SqlValue::Varchar(s)) if s.as_str() == name),
                vec![("seq", SqlValue::Integer(new_val))],
            )
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
    } else {
        database
            .insert_row(
                &seq_table,
                Row::new(vec![
                    SqlValue::Varchar(arcstr::ArcStr::from(table_display_name)),
                    SqlValue::Integer(new_val),
                ]),
            )
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
    }
    Ok(())
}

/// Remove the `sqlite_sequence` entry for a dropped or truncated AUTOINCREMENT
/// table (SQLite: `DROP TABLE` on an AUTOINCREMENT table removes its
/// `sqlite_sequence` row, but the `sqlite_sequence` table itself stays
/// behind). A no-op if the owning schema's `sqlite_sequence` doesn't exist (or
/// has no row for this table) — dropping a table before its first successful
/// INSERT is normal.
///
/// `owning_schema` is the schema (`main` or a `temp_*` schema) that held the
/// table, so the row is removed from the correct database's `sqlite_sequence`
/// (autoinc-4.x). It MUST be captured by the caller BEFORE the table is dropped
/// from the catalog, since it can no longer be resolved from the table name
/// afterwards.
pub fn remove_sequence_entry(
    database: &mut Database,
    table_display_name: &str,
    owning_schema: &str,
) -> Result<(), ExecutorError> {
    let seq_table = sequence_table_in(owning_schema);
    if database.get_table(&seq_table).is_none() {
        return Ok(());
    }
    let name = table_display_name.to_string();
    database
        .delete_row_matching(&seq_table, move |row| {
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
        assert_eq!(
            parse_seq_value(&SqlValue::Varchar(arcstr::ArcStr::from("a-string"))),
            None
        );
        assert_eq!(
            parse_seq_value(&SqlValue::Varchar(arcstr::ArcStr::from(
                "-12345678901234567890"
            ))),
            None
        );
        assert_eq!(
            parse_seq_value(&SqlValue::Varchar(arcstr::ArcStr::from("42"))),
            Some(42)
        );
    }

    /// Helper: read a `sqlite_sequence` row for `name` from `owning_schema`'s
    /// `sqlite_sequence` table, exactly as the production paths do.
    fn lookup_in(db: &Database, owning_schema: &str, name: &str) -> Option<Option<i64>> {
        let seq_table = sequence_table_in(owning_schema);
        lookup_sequence_row(db, &seq_table, name)
    }

    #[test]
    fn test_ensure_sqlite_sequence_table_idempotent() {
        let mut db = Database::new();
        assert!(db.catalog.get_table(SQLITE_SEQUENCE_TABLE).is_none());
        ensure_sqlite_sequence_table(&mut db, "main").unwrap();
        assert!(db.catalog.get_table(SQLITE_SEQUENCE_TABLE).is_some());
        // Calling again must not error (idempotent).
        ensure_sqlite_sequence_table(&mut db, "main").unwrap();
    }

    #[test]
    fn test_bump_sequence_creates_and_upserts() {
        let mut db = Database::new();
        bump_sequence_after_insert(&mut db, "t1", "main", 12).unwrap();
        assert_eq!(lookup_in(&db, "main", "t1"), Some(Some(12)));

        // A smaller explicit value never lowers the tracked max.
        bump_sequence_after_insert(&mut db, "t1", "main", 1).unwrap();
        assert_eq!(lookup_in(&db, "main", "t1"), Some(Some(12)));

        // A larger value raises it.
        bump_sequence_after_insert(&mut db, "t1", "main", 123).unwrap();
        assert_eq!(lookup_in(&db, "main", "t1"), Some(Some(123)));

        // A second table is tracked independently.
        bump_sequence_after_insert(&mut db, "t2", "main", 1).unwrap();
        assert_eq!(lookup_in(&db, "main", "t2"), Some(Some(1)));
        assert_eq!(lookup_in(&db, "main", "t1"), Some(Some(123)));
    }

    #[test]
    fn test_remove_sequence_entry() {
        let mut db = Database::new();
        bump_sequence_after_insert(&mut db, "t1", "main", 5).unwrap();
        bump_sequence_after_insert(&mut db, "t2", "main", 7).unwrap();
        remove_sequence_entry(&mut db, "t1", "main").unwrap();
        assert_eq!(lookup_in(&db, "main", "t1"), None);
        assert_eq!(lookup_in(&db, "main", "t2"), Some(Some(7)));
    }

    #[test]
    fn test_bump_sequence_routes_to_explicit_owning_schema() {
        // Same display name tracked independently per owning schema — the
        // explicit `owning_schema` parameter, not any bare-name re-resolution,
        // decides which `sqlite_sequence` receives the bookkeeping (#6350).
        let mut db = Database::new();
        // The session temp schema is created eagerly at connection open.
        let temp_schema = db.catalog.temp_schema_name().to_string();

        bump_sequence_after_insert(&mut db, "t1", "main", 10).unwrap();
        bump_sequence_after_insert(&mut db, "t1", &temp_schema, 20).unwrap();

        assert_eq!(lookup_in(&db, "main", "t1"), Some(Some(10)));
        assert_eq!(lookup_in(&db, &temp_schema, "t1"), Some(Some(20)));

        // Reads are schema-scoped too.
        assert_eq!(sequence_high_water_mark(&db, "t1", "main"), Some(10));
        assert_eq!(sequence_high_water_mark(&db, "t1", &temp_schema), Some(20));
    }
}
