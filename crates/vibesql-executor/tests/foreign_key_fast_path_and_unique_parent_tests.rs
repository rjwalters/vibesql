//! Regression tests for two silent "wrong answer" FK-enforcement bugs fixed
//! under issue #6170. Both are the kind of correctness regression that is easy
//! to reintroduce via an unrelated refactor, so they are pinned here in the
//! path that gates every PR (`cargo test -p vibesql-executor`), independent of
//! the (dispatch-only) TCL conformance job.
//!
//! Bug #1 — single-row UPDATE super-fast path bypassed FK validation.
//!   `try_super_fast_path` (in-place column writes, no row cloning) checked
//!   that an assigned column was not the PK / not UNIQUE / not independently
//!   indexed, but never checked whether it was a FOREIGN KEY *child-side*
//!   column. `UPDATE child SET fk_col=<literal> WHERE pk=<literal>` therefore
//!   silently skipped referential-integrity enforcement whenever `fk_col`
//!   carried no index of its own — a dangling reference was written and the
//!   UPDATE returned success. These tests deliberately shape the UPDATE to hit
//!   that path (`WHERE <ipk> = <literal>` → single row, `fk_col` unindexed).
//!
//! Bug #2 — UPDATE-triggered cascade checked the wrong parent key.
//!   `ForeignKeyValidator::check_no_child_references` hardcoded the parent
//!   table's PRIMARY KEY as "the key that changed". An FK whose parent key is a
//!   UNIQUE constraint/index rather than the PK was silently skipped by every
//!   CASCADE/SET NULL/SET DEFAULT/RESTRICT/NO ACTION check whenever the PK was
//!   left untouched. These tests change only the UNIQUE parent key and assert
//!   the configured referential action actually fires.

use vibesql_ast::Statement;
use vibesql_executor::{CreateIndexExecutor, CreateTableExecutor, InsertExecutor, UpdateExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn run(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("parse error: {:?}", e))?;
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).map(|_| String::new()).map_err(|e| e.to_string())
        }
        Statement::CreateIndex(s) => {
            CreateIndexExecutor::execute(&s, db).map(|_| String::new()).map_err(|e| e.to_string())
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).map(|_| String::new()).map_err(|e| e.to_string())
        }
        Statement::Update(s) => {
            UpdateExecutor::execute(&s, db).map(|_| String::new()).map_err(|e| e.to_string())
        }
        other => Err(format!("unsupported statement type in test helper: {:?}", other)),
    }
}

/// All rows of a table as raw value vectors, in physical scan order.
fn rows(db: &Database, table: &str) -> Vec<Vec<SqlValue>> {
    let t = db.get_table(table).expect("table not found");
    t.scan().iter().map(|r| r.values.to_vec()).collect()
}

// ---------------------------------------------------------------------------
// Bug #1: single-row super-fast-path FK bypass.
// ---------------------------------------------------------------------------

/// `UPDATE child SET fk_col=<literal> WHERE id=<literal>` where `fk_col` has no
/// index of its own and points at a non-existent parent row must be rejected —
/// not silently applied by the in-place fast path.
#[test]
fn fast_path_update_of_fk_column_to_missing_parent_is_rejected() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    // `id INTEGER PRIMARY KEY` gives a rowid so `WHERE id = 1` resolves a single
    // row (the trigger for the super-fast path). `fk_col` is a plain FK column
    // with no UNIQUE/PK/index of its own — exactly the shape that used to slip
    // past FK validation on the in-place path.
    run(&mut db, "CREATE TABLE parent(a INTEGER PRIMARY KEY, b)").unwrap();
    run(
        &mut db,
        "CREATE TABLE child(id INTEGER PRIMARY KEY, fk_col REFERENCES parent(a), payload)",
    )
    .unwrap();
    run(&mut db, "INSERT INTO parent VALUES(1, 10)").unwrap();
    run(&mut db, "INSERT INTO child VALUES(1, 1, 100)").unwrap();

    // 999 has no matching parent row: this must raise an FK violation.
    let res = run(&mut db, "UPDATE child SET fk_col = 999 WHERE id = 1");
    assert!(
        res.is_err(),
        "single-row fast-path UPDATE of an FK column to a missing parent must be rejected, got {res:?}"
    );

    // And it must not have been silently applied: the row still references 1.
    let child = rows(&db, "child");
    assert_eq!(child.len(), 1, "child row count changed unexpectedly");
    assert_eq!(
        child[0][1],
        SqlValue::Integer(1),
        "FK column was silently mutated to a dangling value despite the violation: {child:?}"
    );
}

/// Companion guard: the same fast-path UPDATE to a *valid* parent key must
/// still succeed (the fix must not over-reject legitimate FK-column updates).
#[test]
fn fast_path_update_of_fk_column_to_existing_parent_succeeds() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    run(&mut db, "CREATE TABLE parent(a INTEGER PRIMARY KEY, b)").unwrap();
    run(
        &mut db,
        "CREATE TABLE child(id INTEGER PRIMARY KEY, fk_col REFERENCES parent(a), payload)",
    )
    .unwrap();
    run(&mut db, "INSERT INTO parent VALUES(1, 10)").unwrap();
    run(&mut db, "INSERT INTO parent VALUES(2, 20)").unwrap();
    run(&mut db, "INSERT INTO child VALUES(1, 1, 100)").unwrap();

    run(&mut db, "UPDATE child SET fk_col = 2 WHERE id = 1")
        .unwrap_or_else(|e| panic!("valid FK-column update via fast path must succeed: {e}"));

    let child = rows(&db, "child");
    assert_eq!(child[0][1], SqlValue::Integer(2), "valid FK update did not apply: {child:?}");
}

// ---------------------------------------------------------------------------
// Bug #2: UPDATE-triggered cascade against a UNIQUE (non-PK) parent key.
// ---------------------------------------------------------------------------

/// Child FK references a UNIQUE column (not the PK) with ON UPDATE CASCADE.
/// Changing only that UNIQUE parent key must cascade into the child — before
/// the fix, the parent check keyed off the (untouched) PRIMARY KEY and silently
/// skipped the cascade, leaving the child pointing at the old value.
#[test]
fn update_of_unique_parent_key_cascades_to_child() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    run(&mut db, "CREATE TABLE parent(id INTEGER PRIMARY KEY, u UNIQUE)").unwrap();
    run(&mut db, "CREATE TABLE child(x REFERENCES parent(u) ON UPDATE CASCADE, y)").unwrap();
    run(&mut db, "INSERT INTO parent VALUES(1, 100)").unwrap();
    run(&mut db, "INSERT INTO child VALUES(100, 5)").unwrap();

    // Change only the UNIQUE parent key; the PK (id) is untouched.
    run(&mut db, "UPDATE parent SET u = 200 WHERE id = 1")
        .unwrap_or_else(|e| panic!("cascading UNIQUE-parent-key update must succeed: {e}"));

    let child = rows(&db, "child");
    assert_eq!(
        child[0][0],
        SqlValue::Integer(200),
        "ON UPDATE CASCADE against a UNIQUE parent key did not fire — child left dangling: {child:?}"
    );
}

/// Same schema shape but with the default RESTRICT action: changing the UNIQUE
/// parent key while a child still references the old value must be rejected —
/// before the fix this referential check was silently skipped and the update
/// wrongly succeeded, orphaning the child.
#[test]
fn update_of_unique_parent_key_with_referencing_child_is_restricted() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    run(&mut db, "CREATE TABLE parent(id INTEGER PRIMARY KEY, u UNIQUE)").unwrap();
    // No ON UPDATE action → NO ACTION / RESTRICT-style enforcement.
    run(&mut db, "CREATE TABLE child(x REFERENCES parent(u), y)").unwrap();
    run(&mut db, "INSERT INTO parent VALUES(1, 100)").unwrap();
    run(&mut db, "INSERT INTO child VALUES(100, 5)").unwrap();

    let res = run(&mut db, "UPDATE parent SET u = 200 WHERE id = 1");
    assert!(
        res.is_err(),
        "changing a UNIQUE parent key still referenced by a child must raise an FK violation, got {res:?}"
    );

    // The child must remain untouched and still reference the (unchanged) key.
    let parent = rows(&db, "parent");
    assert_eq!(
        parent[0][1],
        SqlValue::Integer(100),
        "parent key changed despite restriction: {parent:?}"
    );
    let child = rows(&db, "child");
    assert_eq!(
        child[0][0],
        SqlValue::Integer(100),
        "child reference changed unexpectedly: {child:?}"
    );
}

// ---------------------------------------------------------------------------
// Bug #3 (#6170): single-row UPDATE fast path bypassed FK enforcement on a
// parent key backed by a plain CREATE UNIQUE INDEX (not the table's own PK and
// not a column/table-level UNIQUE constraint).
// ---------------------------------------------------------------------------
//
// The fast path decided whether an incoming FK needed re-validation by checking
// only whether the UPDATE touched the table's own PRIMARY KEY columns. A FK from
// another table can target a *non-PK* composite parent key backed by a separate
// `CREATE UNIQUE INDEX` — which, unlike a column/table-level `UNIQUE`, never
// populates `schema.unique_constraints`, so the pre-existing "skip fast path if
// table has unique constraints" guard did not catch it either. `UPDATE parent
// SET b=201 WHERE a=1` therefore touched no PK column, the fast path applied the
// write directly with ZERO FK enforcement, and the child row was silently
// orphaned (fkey2-ce7c13.1.2/1.3/1.5/1.6). The fix falls back to the normal path
// whenever an assigned column is part of any other table's FK parent key that
// references this table.

/// Composite parent key `(a,b)` backed by `CREATE UNIQUE INDEX` (the table's PK
/// is only `a`). A child FK references `(a,b)`. `UPDATE parent SET b=<new>` via
/// the single-row fast path, while a child still references the old `(a,b)`,
/// must be rejected — not silently applied with the child left dangling.
#[test]
fn fast_path_update_of_unique_index_backed_parent_key_is_enforced() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    // Parent PK is only `a`; the composite parent key `(a,b)` is materialized
    // solely by a plain CREATE UNIQUE INDEX — the exact shape that never lands
    // in `schema.unique_constraints`.
    run(&mut db, "CREATE TABLE tce71(a INTEGER PRIMARY KEY, b INTEGER)").unwrap();
    run(&mut db, "CREATE UNIQUE INDEX tce71_ab ON tce71(a, b)").unwrap();
    run(&mut db, "CREATE TABLE tce72(x, y, FOREIGN KEY(x, y) REFERENCES tce71(a, b))").unwrap();
    run(&mut db, "INSERT INTO tce71 VALUES(1, 100)").unwrap();
    run(&mut db, "INSERT INTO tce72 VALUES(1, 100)").unwrap();

    // `WHERE a = 1` resolves a single row (fast-path trigger); `b` is not the PK
    // and carries no UNIQUE constraint of its own. Changing it orphans tce72's
    // (1,100) reference, so this must raise an FK violation.
    let res = run(&mut db, "UPDATE tce71 SET b = 201 WHERE a = 1");
    assert!(
        res.is_err(),
        "single-row fast-path UPDATE of a UNIQUE-INDEX-backed FK parent key with a referencing child must be rejected, got {res:?}"
    );

    // And it must not have been silently applied: the parent key is unchanged.
    let parent = rows(&db, "tce71");
    assert_eq!(parent.len(), 1, "parent row count changed unexpectedly");
    assert_eq!(
        parent[0][1],
        SqlValue::Integer(100),
        "UNIQUE-INDEX-backed parent key was silently mutated despite a referencing child: {parent:?}"
    );
}

/// Companion guard: the same fast-path UPDATE of the UNIQUE-INDEX-backed parent
/// key must still succeed when no child references the old value (the fallback
/// to the normal path must not over-reject a legitimate update).
#[test]
fn fast_path_update_of_unique_index_backed_parent_key_without_child_ref_succeeds() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    run(&mut db, "CREATE TABLE tce71(a INTEGER PRIMARY KEY, b INTEGER)").unwrap();
    run(&mut db, "CREATE UNIQUE INDEX tce71_ab ON tce71(a, b)").unwrap();
    run(&mut db, "CREATE TABLE tce72(x, y, FOREIGN KEY(x, y) REFERENCES tce71(a, b))").unwrap();
    run(&mut db, "INSERT INTO tce71 VALUES(1, 100)").unwrap();
    // No child row references (1,100), so changing b is free.
    run(&mut db, "UPDATE tce71 SET b = 201 WHERE a = 1").unwrap_or_else(|e| {
        panic!("unreferenced UNIQUE-INDEX-backed parent-key update must succeed: {e}")
    });

    let parent = rows(&db, "tce71");
    assert_eq!(
        parent[0][1],
        SqlValue::Integer(201),
        "valid parent-key update did not apply: {parent:?}"
    );
}

// ---------------------------------------------------------------------------
// Bug #4 (#6170): CREATE TABLE FK validation reported the wrong error when a
// column-count mismatch and an unknown child column coincided.
// ---------------------------------------------------------------------------
//
// `FOREIGN KEY(...)` validation resolved child column names to indices (raising
// "unknown column ... in foreign key definition") *before* checking whether the
// child/parent column counts even matched. SQLite reports the count-mismatch
// error first when an explicit parent column list is given, regardless of
// whether a child column also happens to be unknown (e_fkey-54.B). The fix runs
// the count check before the unknown-column resolution.

/// `FOREIGN KEY(c, b) REFERENCES t2(d)` on a table with no column `c`: the child
/// list has 2 columns, the parent list has 1 — SQLite reports the count mismatch
/// even though `c` is also unknown. We must surface the count-mismatch message,
/// not "unknown column c".
#[test]
fn create_table_fk_count_mismatch_reported_before_unknown_child_column() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    run(&mut db, "CREATE TABLE t2(d INTEGER PRIMARY KEY, e)").unwrap();

    // Child column `c` does not exist AND the (2 vs 1) column counts mismatch.
    let res = run(&mut db, "CREATE TABLE t1(a, b, FOREIGN KEY(c, b) REFERENCES t2(d))");
    let err = res.expect_err("mismatched-count FK with an unknown child column must be rejected");
    assert!(
        err.contains("number of columns in foreign key does not match"),
        "expected the column-count-mismatch message to win over the unknown-column message, got: {err}"
    );
    assert!(
        !err.contains("unknown column"),
        "unknown-column message leaked ahead of the count-mismatch check: {err}"
    );
}

/// Control: when the counts DO match but a child column is genuinely unknown,
/// the unknown-column message is still the one reported (the reorder must not
/// swallow the unknown-column path when there is no count mismatch to report).
#[test]
fn create_table_fk_unknown_child_column_reported_when_counts_match() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    run(&mut db, "CREATE TABLE t2(d INTEGER PRIMARY KEY)").unwrap();

    // Counts match (1 vs 1) but child column `c` does not exist.
    let res = run(&mut db, "CREATE TABLE t1(a, b, FOREIGN KEY(c) REFERENCES t2(d))");
    let err = res.expect_err("FK referencing an unknown child column must be rejected");
    assert!(
        !err.contains("number of columns in foreign key"),
        "count-mismatch message wrongly reported when counts actually match: {err}"
    );
    assert!(
        err.contains("unknown column"),
        "expected the unknown-column message when counts match but the child column is unknown, got: {err}"
    );
}
