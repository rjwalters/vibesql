//! Unit tests for `PRAGMA legacy_alter_table`'s gating of `ALTER TABLE ...
//! RENAME TO`'s dependent-object rewrite (trigger `ON`-target, view body, and
//! foreign key `REFERENCES` clauses — both a self-referential FK on the
//! renamed table and a FK on a separate child table), matching SQLite's
//! `alter.c` (`sqlite3AlterRenameTable`) semantics exactly. Issue #6634 / PR
//! #6640 fixed `execute_rename_table`
//! (`crate::alter::table_options`) to gate the rewrite:
//!
//! - trigger rewrite fires only when `legacy_alter_table=OFF`
//! - view rewrite fires only when `legacy_alter_table=OFF`
//! - FK rewrite fires when `legacy_alter_table=OFF` **or** `foreign_keys=ON` — the one documented
//!   exception SQLite's `alter.c` codes explicitly: FK rewriting still fires under
//!   `legacy_alter_table=ON` when foreign key enforcement is active.
//!
//! Both fixes shipped in PR #6640 with only manual/TCL verification and no
//! fast unit-test regression coverage — this file closes that gap (issue
//! #6641).
//!
//! Mirrors the sibling PRAGMA-gating test
//! `alter_writable_schema_precheck_suppression.rs`: the PRAGMA-equivalent
//! state is set directly via `Database::set_legacy_alter_table` /
//! `set_foreign_keys_enabled`, not by round-tripping through `PRAGMA` SQL
//! parsing.

use vibesql_ast::Statement;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

fn exec(db: &mut Database, sql: &str) -> Result<String, ExecutorError> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).expect("parse");
    match stmt {
        Statement::CreateTable(s) => {
            crate::CreateTableExecutor::execute_with_source(&s, db, Some(sql))
        }
        Statement::CreateTrigger(s) => {
            crate::TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql))
        }
        Statement::CreateView(mut s) => {
            s.sql_definition = Some(sql.to_string());
            crate::ViewExecutor::execute_create_view(&s, db)
        }
        Statement::AlterTable(s) => {
            crate::alter::AlterTableExecutor::execute_with_source(&s, db, Some(sql))
        }
        other => panic!("unexpected statement: {:?}", other),
    }
}

/// Shared fixture: `t1` has a self-referential FK column (`pid REFERENCES
/// t1(a)`), a trigger `tr1` defined `ON t1`, a view `v1` selecting from `t1`,
/// and a separate child table `c1` with a FK to `t1`. `ALTER TABLE t1 RENAME
/// TO e` exercises all three dependent-object rewrite paths at once.
fn build_fixture(db: &mut Database) {
    exec(db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, pid INTEGER REFERENCES t1(a))").unwrap();
    exec(db, "CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN SELECT 1; END").unwrap();
    exec(db, "CREATE VIEW v1 AS SELECT a FROM t1").unwrap();
    exec(db, "CREATE TABLE c1(x INTEGER REFERENCES t1(a))").unwrap();
}

/// `legacy_alter_table=OFF` (the default): trigger, view, and both the
/// self-referential and child-table FK are all rewritten to the new name.
#[test]
fn legacy_off_default_rewrites_trigger_view_and_fk() {
    let mut db = Database::new();
    assert!(!db.legacy_alter_table(), "legacy_alter_table must default to OFF");
    build_fixture(&mut db);

    exec(&mut db, "ALTER TABLE t1 RENAME TO e").unwrap();

    let trigger = db.catalog.get_trigger("tr1").expect("trigger tr1 must survive the rename");
    assert_eq!(
        trigger.table_name.to_ascii_lowercase(),
        "e",
        "trigger ON-target must be rewritten to e"
    );
    assert_eq!(
        trigger.sql_definition.as_deref(),
        Some("CREATE TRIGGER tr1 AFTER INSERT ON \"e\" BEGIN SELECT 1; END")
    );

    let view = db.catalog.get_view("v1").expect("view v1 must survive the rename");
    assert_eq!(view.sql_definition.as_deref(), Some("CREATE VIEW v1 AS SELECT a FROM \"e\""));

    let e_schema = db.catalog.get_table("e").expect("renamed table e must exist");
    assert_eq!(e_schema.foreign_keys.len(), 1);
    assert_eq!(
        e_schema.foreign_keys[0].parent_table, "e",
        "self-referential FK on the renamed table must follow the rename"
    );

    let c1_schema = db.catalog.get_table("c1").expect("c1 must exist");
    assert_eq!(
        c1_schema.foreign_keys[0].parent_table, "e",
        "child table c1's FK must follow the rename"
    );
}

/// `legacy_alter_table=ON`, `foreign_keys=OFF`: trigger, view, and both FKs
/// are all suppressed — every dependent object keeps naming the table `t1`
/// verbatim (SQLite's legacy pre-3.25.0 `ALTER TABLE RENAME` behavior).
#[test]
fn legacy_on_foreign_keys_off_suppresses_trigger_view_and_fk() {
    let mut db = Database::new();
    build_fixture(&mut db);
    db.set_legacy_alter_table(true);
    assert!(!db.foreign_keys_enabled(), "foreign_keys must default to OFF");

    exec(&mut db, "ALTER TABLE t1 RENAME TO e").unwrap();

    let trigger = db.catalog.get_trigger("tr1").expect("trigger tr1 must survive the rename");
    assert_eq!(
        trigger.table_name.to_ascii_lowercase(),
        "t1",
        "trigger ON-target must be left naming the old table"
    );
    assert_eq!(
        trigger.sql_definition.as_deref(),
        Some("CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN SELECT 1; END")
    );

    let view = db.catalog.get_view("v1").expect("view v1 must survive the rename");
    assert_eq!(
        view.sql_definition.as_deref(),
        Some("CREATE VIEW v1 AS SELECT a FROM t1"),
        "view body must be left naming the old table"
    );

    let e_schema = db.catalog.get_table("e").expect("renamed table e must exist");
    assert_eq!(
        e_schema.foreign_keys[0].parent_table, "t1",
        "self-referential FK must keep naming the old table"
    );

    let c1_schema = db.catalog.get_table("c1").expect("c1 must exist");
    assert_eq!(
        c1_schema.foreign_keys[0].parent_table, "t1",
        "child table c1's FK must keep naming the old table"
    );
}

/// `legacy_alter_table=ON`, `foreign_keys=ON`: trigger and view rewrite stay
/// suppressed, but FK rewrite still fires — the one exception SQLite's
/// `alter.c` codes explicitly (`legacy_alter_table==OFF || foreign_keys==ON`).
#[test]
fn legacy_on_foreign_keys_on_fk_rewrite_still_fires() {
    let mut db = Database::new();
    build_fixture(&mut db);
    db.set_legacy_alter_table(true);
    db.set_foreign_keys_enabled(true);

    exec(&mut db, "ALTER TABLE t1 RENAME TO e").unwrap();

    // Trigger/view rewrite has no foreign_keys exception: still suppressed.
    let trigger = db.catalog.get_trigger("tr1").expect("trigger tr1 must survive the rename");
    assert_eq!(
        trigger.table_name.to_ascii_lowercase(),
        "t1",
        "trigger ON-target must still be suppressed under foreign_keys=ON"
    );

    let view = db.catalog.get_view("v1").expect("view v1 must survive the rename");
    assert_eq!(
        view.sql_definition.as_deref(),
        Some("CREATE VIEW v1 AS SELECT a FROM t1"),
        "view rewrite must still be suppressed under foreign_keys=ON"
    );

    // FK rewrite fires despite legacy_alter_table=ON, because foreign_keys=ON.
    let e_schema = db.catalog.get_table("e").expect("renamed table e must exist");
    assert_eq!(
        e_schema.foreign_keys[0].parent_table, "e",
        "self-referential FK must still be rewritten when foreign_keys=ON"
    );

    let c1_schema = db.catalog.get_table("c1").expect("c1 must exist");
    assert_eq!(
        c1_schema.foreign_keys[0].parent_table, "e",
        "child table c1's FK must still be rewritten when foreign_keys=ON"
    );
}

// ---------------------------------------------------------------------------
// `legacy_alter_table` gating of the whole-schema *precheck*
// (`precheck_schema_objects`), not just the dependent-object rewrite above.
//
// PR #6663 wired `precheck_schema_objects` into `execute_rename_table` so an
// already-broken trigger/view anywhere in the schema aborts a RENAME TO
// (altertab3.test 4.1.2/4.2.1, issue #6174). `RENAME TO` is the one ALTER form
// where SQLite skips that whole schema reparse under `PRAGMA
// legacy_alter_table=ON`, so the precheck needs the same gate the rewrite pass
// already has. Verified against sqlite3 3.51.0 (PR #6663 review):
//
// ```
// sqlite> PRAGMA legacy_alter_table=ON;
// sqlite> CREATE TABLE t1(a, b);
// sqlite> CREATE TABLE t3(e, f);
// sqlite> CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN INSERT INTO t2 VALUES(new.a, new.b); END;
// sqlite> ALTER TABLE t3 RENAME TO t4;   -- succeeds, no error
// ```
//
// (`RENAME COLUMN`/`DROP COLUMN`, the other two `precheck_schema_objects` call
// sites, still abort under `legacy_alter_table=ON` in real SQLite — only the
// message degrades — so they stay ungated.)
// ---------------------------------------------------------------------------

/// Shared fixture: an *already-broken* trigger (`tr1 ON t1` inserts into `t2`,
/// which was never created) plus a completely unrelated table `t3` whose
/// RENAME TO is what the gating tests exercise.
fn build_broken_trigger_fixture(db: &mut Database) {
    exec(db, "CREATE TABLE t1(a, b)").unwrap();
    exec(db, "CREATE TABLE t3(e, f)").unwrap();
    exec(
        db,
        "CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN INSERT INTO t2 VALUES(new.a, new.b); END",
    )
    .unwrap();
}

/// `legacy_alter_table=ON`: the whole-schema precheck is suppressed, so an
/// already-broken *unrelated* trigger does not block renaming `t3` — matching
/// real sqlite3 3.51.0 under the same pragma.
#[test]
fn legacy_on_broken_unrelated_trigger_does_not_block_rename_to() {
    let mut db = Database::new();
    build_broken_trigger_fixture(&mut db);
    db.set_legacy_alter_table(true);

    exec(&mut db, "ALTER TABLE t3 RENAME TO t4")
        .expect("legacy_alter_table=ON must suppress the whole-schema reparse on RENAME TO");

    assert!(db.get_table("t3").is_none(), "t3 must have been renamed away");
    assert!(db.get_table("t4").is_some(), "t4 must exist after the rename");
    assert!(
        db.catalog.get_trigger("tr1").is_some(),
        "the broken trigger is untouched by the rename, not dropped"
    );
}

/// Control for the test above: with `legacy_alter_table=OFF` (the default) the
/// same fixture still aborts the RENAME TO with SQLite's schema-reparse error,
/// so the gate suppresses the precheck only under legacy mode.
#[test]
fn legacy_off_broken_unrelated_trigger_still_blocks_rename_to() {
    let mut db = Database::new();
    build_broken_trigger_fixture(&mut db);
    assert!(!db.legacy_alter_table(), "legacy_alter_table must default to OFF");

    let err = exec(&mut db, "ALTER TABLE t3 RENAME TO t4")
        .expect_err("legacy_alter_table=OFF must still reject on a broken schema object");
    assert_eq!(err.to_string(), "error in trigger tr1: no such table: main.t2");
    assert!(db.get_table("t3").is_some(), "a rejected ALTER leaves t3 in place");
    assert!(db.get_table("t4").is_none(), "a rejected ALTER must not create t4");
}

/// Same gating for an already-broken *view* (`c99` is not a column of `t4`):
/// under `legacy_alter_table=ON` it does not block renaming the unrelated
/// table `t3`.
#[test]
fn legacy_on_broken_unrelated_view_does_not_block_rename_to() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t4(id INTEGER PRIMARY KEY, c1 INT)").unwrap();
    exec(&mut db, "CREATE VIEW t4v1 AS SELECT id, c1, c99 FROM t4").unwrap();
    exec(&mut db, "CREATE TABLE t3(e, f)").unwrap();
    db.set_legacy_alter_table(true);

    exec(&mut db, "ALTER TABLE t3 RENAME TO t5")
        .expect("legacy_alter_table=ON must suppress the whole-schema reparse on RENAME TO");

    assert!(db.get_table("t3").is_none(), "t3 must have been renamed away");
    assert!(db.get_table("t5").is_some(), "t5 must exist after the rename");
    let view = db.catalog.get_view("t4v1").expect("the broken view must survive");
    assert_eq!(
        view.sql_definition.as_deref(),
        Some("CREATE VIEW t4v1 AS SELECT id, c1, c99 FROM t4"),
        "the unrelated broken view is left exactly as it was"
    );
}

/// Control for the view case: `legacy_alter_table=OFF` still rejects.
#[test]
fn legacy_off_broken_unrelated_view_still_blocks_rename_to() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t4(id INTEGER PRIMARY KEY, c1 INT)").unwrap();
    exec(&mut db, "CREATE VIEW t4v1 AS SELECT id, c1, c99 FROM t4").unwrap();
    exec(&mut db, "CREATE TABLE t3(e, f)").unwrap();

    let err = exec(&mut db, "ALTER TABLE t3 RENAME TO t5")
        .expect_err("legacy_alter_table=OFF must still reject on a broken schema object");
    assert_eq!(err.to_string(), "error in view t4v1: no such column: c99");
    assert!(db.get_table("t3").is_some(), "a rejected ALTER leaves t3 in place");
    assert!(db.get_table("t5").is_none(), "a rejected ALTER must not create t5");
}
