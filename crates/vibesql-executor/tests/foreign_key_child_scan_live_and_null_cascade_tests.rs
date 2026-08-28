//! Regression tests for two `update/foreign_keys.rs::check_no_child_references`
//! bugs found while re-measuring `e_fkey.test` for issue #6170.
//!
//! ## Bug 1: stale (bitmap-deleted, not yet compacted) child rows counted as
//! still referencing a parent
//!
//! `check_no_child_references` (used to decide whether an UPDATE that changes
//! a parent key is safe, or must CASCADE/SET NULL/SET DEFAULT/RESTRICT/NO
//! ACTION) scanned the child table via the raw, non-MVCC `Table::scan()`,
//! which — per its own doc comment — "may include deleted rows" that have
//! been bitmap-marked but not yet physically compacted.
//! `Table::compact_if_needed()` only compacts once *more than* half of a
//! table's rows are deleted, so a table with (say) one deleted row out of two
//! leaves that deleted row sitting in `scan()`'s output. A DELETE that
//! removes the *only* child row referencing a parent key, followed by an
//! UPDATE of that parent key in the same statement batch/transaction, then
//! spuriously found the just-deleted child row still "referencing" the
//! parent and raised a false-positive "cannot update a parent row"
//! violation — but only when the child table also held >= 1 other live row
//! for a *different* key (otherwise the delete emptied the table entirely
//! and crossed the 50% compaction threshold, hiding the bug). This exact
//! shape reproduces SQLite's `e_fkey-14.4`.
//!
//! ## Bug 2: NULL child FK values swept into an `ON UPDATE CASCADE` whose
//! parent's OLD key value is also NULL
//!
//! Per EVIDENCE-OF R-23980-48859, a NULL foreign-key column never
//! references any parent row — not even a parent row whose key is itself
//! NULL. `check_no_child_references`'s child-matching predicate called
//! `fk_values_equal(child, parent, ..)`, whose `child == parent` fast path
//! treats `Null == Null` as a match. Every *other* caller of
//! `fk_values_equal` already filters NULL FK values out before calling it,
//! but this one didn't, so a child row with a NULL FK column was
//! incorrectly cascaded whenever a parent row's key was updated away from
//! NULL. This reproduces SQLite's `e_fkey-47.4`.
//!
//! See issue #6170.

use vibesql_ast::Statement;
use vibesql_executor::{CreateTableExecutor, DeleteExecutor, InsertExecutor, UpdateExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn run(db: &mut Database, sql: &str) -> Result<(), String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("parse error: {:?}", e))?;
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).map(|_| ()).map_err(|e| e.to_string())
        }
        Statement::Update(s) => {
            UpdateExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        Statement::Delete(s) => {
            DeleteExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        other => Err(format!("unsupported statement type in test helper: {:?}", other)),
    }
}

/// DELETE removes the only child row referencing a parent key; a subsequent
/// UPDATE of that parent key in the same transaction must succeed as long as
/// no *other* child row still references it — even when an unrelated live
/// child row (referencing a *different* parent key) is present and keeps the
/// child table's deleted-row ratio below the 50% compaction threshold.
#[test]
fn update_parent_key_after_deleting_sole_referencing_child_succeeds() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    run(&mut db, "CREATE TABLE artist(artistid INTEGER PRIMARY KEY, artistname TEXT)").unwrap();
    run(
        &mut db,
        "CREATE TABLE track(trackid INTEGER, trackname TEXT, trackartist INTEGER, \
         FOREIGN KEY(trackartist) REFERENCES artist(artistid))",
    )
    .unwrap();
    run(&mut db, "INSERT INTO artist VALUES(1, 'A')").unwrap();
    run(&mut db, "INSERT INTO artist VALUES(3, 'B')").unwrap();
    run(&mut db, "INSERT INTO track VALUES(11, 'x', 1)").unwrap();
    // Unrelated live row referencing a *different* parent key. Without it,
    // deleting track 11 alone would empty the table and cross the 50%
    // compaction threshold, masking the bug.
    run(&mut db, "INSERT INTO track VALUES(14, 'y', 3)").unwrap();

    run(&mut db, "DELETE FROM track WHERE trackid = 11").unwrap();
    run(&mut db, "UPDATE artist SET artistid = 4 WHERE artistid = 1").unwrap_or_else(|e| {
        panic!(
            "updating a parent key must succeed once its only referencing child row \
             was deleted earlier in the same transaction, got: {e}"
        )
    });

    let artist = db.get_table("artist").unwrap();
    let ids: Vec<i64> = artist
        .scan_live()
        .map(|(_, row)| match &row.values[0] {
            SqlValue::Integer(i) => *i,
            other => panic!("unexpected artistid value: {other:?}"),
        })
        .collect();
    assert!(ids.contains(&4), "expected artist row updated to id 4, got {ids:?}");
    assert!(!ids.contains(&1), "old artist id 1 should no longer exist, got {ids:?}");

    // Guard against over-relaxing: a parent key that IS still referenced by a
    // live child row must still be rejected (NO ACTION is the default).
    let err = run(&mut db, "UPDATE artist SET artistid = 99 WHERE artistid = 3")
        .expect_err("updating a still-referenced parent key must fail");
    assert!(
        err.to_ascii_lowercase().contains("foreign key"),
        "expected a FOREIGN KEY violation, got: {err}"
    );
}

/// A child row whose FK column is NULL never references any parent row —
/// not even a parent row whose own key is NULL. `ON UPDATE CASCADE` moving a
/// parent row's key away from NULL must not touch a NULL-valued child FK
/// column.
#[test]
fn on_update_cascade_does_not_touch_null_child_fk_when_parent_key_was_null() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    run(&mut db, "CREATE TABLE p1(a, b UNIQUE)").unwrap();
    run(&mut db, "CREATE TABLE c1(c REFERENCES p1(b) ON UPDATE CASCADE, d)").unwrap();
    run(&mut db, "INSERT INTO p1 VALUES(NULL, NULL)").unwrap();
    run(&mut db, "INSERT INTO c1 VALUES(NULL, NULL)").unwrap();

    run(&mut db, "UPDATE p1 SET b = 6 WHERE b IS NULL")
        .unwrap_or_else(|e| panic!("updating a NULL parent key must succeed: {e}"));

    let c1 = db.get_table("c1").unwrap();
    let rows: Vec<&SqlValue> = c1.scan_live().map(|(_, row)| &row.values[0]).collect();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].is_null(),
        "NULL child FK value must not be cascaded when the parent's OLD key was also NULL, got {:?}",
        rows[0]
    );
}
