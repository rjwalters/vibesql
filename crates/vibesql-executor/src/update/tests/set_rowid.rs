//! `UPDATE ... SET rowid = <expr>` on base tables (issue #5517).
//!
//! SQLite lets a rowid table relocate a row by assigning its rowid:
//! `UPDATE t SET rowid=99 WHERE rowid=1` moves the row to rowid 99, fires the
//! table's BEFORE/AFTER UPDATE triggers with OLD.rowid=1 / NEW.rowid=99, and
//! raises `UNIQUE constraint failed: t.rowid` if the target rowid is already
//! taken. `_rowid_` / `oid` are aliases. For an INTEGER PRIMARY KEY table the
//! rowid IS the IPK column, so `SET rowid=` writes the IPK (and a collision is
//! reported against the IPK column name). These behaviors were verified against
//! sqlite3 3.51.0; see triggerC-7.x.

use vibesql_ast::Statement;
use vibesql_parser::Parser;
use vibesql_types::SqlValue;

use crate::*;

/// Execute a non-query statement (CREATE/INSERT/CREATE INDEX/CREATE TRIGGER).
fn ddl(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {:?}: {:?}", sql, e));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE");
        }
        Statement::CreateIndex(s) => {
            CreateIndexExecutor::execute(&s, db).expect("CREATE INDEX");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT");
        }
        Statement::CreateTrigger(s) => {
            crate::advanced_objects::execute_create_trigger(&s, db).expect("CREATE TRIGGER");
        }
        other => panic!("unsupported ddl: {:?}", other),
    }
}

/// Run an UPDATE, returning the executor result (so conflict errors are observable).
fn update(db: &mut vibesql_storage::Database, sql: &str) -> Result<usize, crate::ExecutorError> {
    match Parser::parse_sql(sql).expect("parse update") {
        Statement::Update(s) => UpdateExecutor::execute(&s, db),
        other => panic!("expected UPDATE, got {:?}", other),
    }
}

/// Run a SELECT and return rows (each row carries its resolved row_id).
fn select(db: &mut vibesql_storage::Database, sql: &str) -> Vec<vibesql_storage::Row> {
    match Parser::parse_sql(sql).expect("parse select") {
        Statement::Select(s) => SelectExecutor::new(db).execute(&s).expect("SELECT"),
        other => panic!("expected SELECT, got {:?}", other),
    }
}

fn int(v: &SqlValue) -> i64 {
    match v {
        SqlValue::Integer(i) => *i,
        SqlValue::Bigint(i) => *i,
        other => panic!("expected integer, got {:?}", other),
    }
}

fn text(v: &SqlValue) -> String {
    match v {
        SqlValue::Varchar(s) => s.to_string(),
        other => panic!("expected text, got {:?}", other),
    }
}

/// `UPDATE t SET rowid=N` relocates the row on a virtual-rowid table.
#[test]
fn set_rowid_relocates_row() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(x)");
    ddl(&mut db, "INSERT INTO t VALUES(1)");
    ddl(&mut db, "INSERT INTO t VALUES(2)");
    ddl(&mut db, "INSERT INTO t VALUES(3)");

    assert_eq!(update(&mut db, "UPDATE t SET rowid=99 WHERE rowid=1").unwrap(), 1);

    // sqlite3: rows are (rowid=2,x=2), (rowid=3,x=3), (rowid=99,x=1).
    let rows = select(&mut db, "SELECT rowid, x FROM t ORDER BY rowid");
    let got: Vec<(i64, i64)> =
        rows.iter().map(|r| (int(&r.values[0]), int(&r.values[1]))).collect();
    assert_eq!(got, vec![(2, 2), (3, 3), (99, 1)]);
}

/// `_rowid_` is an alias for `rowid`.
#[test]
fn set_underscore_rowid_alias() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(x)");
    ddl(&mut db, "INSERT INTO t VALUES(1)");

    assert_eq!(update(&mut db, "UPDATE t SET _rowid_=50 WHERE rowid=1").unwrap(), 1);

    let rows = select(&mut db, "SELECT rowid, x FROM t");
    assert_eq!(int(&rows[0].values[0]), 50);
    assert_eq!(int(&rows[0].values[1]), 1);
}

/// Relocating onto an occupied rowid is `UNIQUE constraint failed: t.rowid`.
#[test]
fn set_rowid_conflict_errors() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(x)");
    ddl(&mut db, "INSERT INTO t VALUES(1)");
    ddl(&mut db, "INSERT INTO t VALUES(2)");
    ddl(&mut db, "INSERT INTO t VALUES(3)");

    let err = update(&mut db, "UPDATE t SET rowid=2 WHERE rowid=1").unwrap_err();
    assert!(
        err.to_string().contains("UNIQUE constraint failed: t.rowid"),
        "expected rowid UNIQUE error, got: {}",
        err
    );

    // The table must be unchanged after the aborted UPDATE.
    let rows = select(&mut db, "SELECT rowid, x FROM t ORDER BY rowid");
    let got: Vec<(i64, i64)> =
        rows.iter().map(|r| (int(&r.values[0]), int(&r.values[1]))).collect();
    assert_eq!(got, vec![(1, 1), (2, 2), (3, 3)]);
}

/// Single-statement rowid SWAP must error with `UNIQUE constraint failed:
/// t.rowid` (issue #5559). sqlite3 3.51.0 processes the UPDATE row-by-row in
/// ascending rowid order: relocating rowid 1 -> 2 collides with rowid 2, which
/// is still live at that moment. The final state would be consistent, but
/// sqlite3 checks the intermediate state, so this must be rejected.
#[test]
fn set_rowid_swap_errors_intermediate_collision() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(a)");
    ddl(&mut db, "INSERT INTO t VALUES(10)"); // rowid 1
    ddl(&mut db, "INSERT INTO t VALUES(20)"); // rowid 2

    let err = update(
        &mut db,
        "UPDATE t SET rowid = CASE rowid WHEN 1 THEN 2 WHEN 2 THEN 1 END",
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("UNIQUE constraint failed: t.rowid"),
        "expected rowid UNIQUE error for swap, got: {}",
        err
    );

    // Table unchanged after the aborted UPDATE.
    let rows = select(&mut db, "SELECT rowid, a FROM t ORDER BY rowid");
    let got: Vec<(i64, i64)> =
        rows.iter().map(|r| (int(&r.values[0]), int(&r.values[1]))).collect();
    assert_eq!(got, vec![(1, 10), (2, 20)]);
}

/// A 3-cycle rotation (1->2, 2->3, 3->1) also errors: processed ascending,
/// 1->2 collides with the still-live rowid 2. Matches sqlite3 3.51.0.
#[test]
fn set_rowid_three_cycle_errors() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(a)");
    ddl(&mut db, "INSERT INTO t VALUES(10)");
    ddl(&mut db, "INSERT INTO t VALUES(20)");
    ddl(&mut db, "INSERT INTO t VALUES(30)");

    let err = update(
        &mut db,
        "UPDATE t SET rowid = CASE rowid WHEN 1 THEN 2 WHEN 2 THEN 3 WHEN 3 THEN 1 END",
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("UNIQUE constraint failed: t.rowid"),
        "expected rowid UNIQUE error for 3-cycle, got: {}",
        err
    );
}

/// `SET rowid = rowid + 1` over ascending rows cascade-collides: processed
/// ascending, rowid 1 -> 2 collides with the still-live rowid 2. sqlite3 3.51.0
/// errors (processing order matters; see the `-1` counterpart below).
#[test]
fn set_rowid_plus_one_cascade_errors() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(a)");
    ddl(&mut db, "INSERT INTO t VALUES(10)");
    ddl(&mut db, "INSERT INTO t VALUES(20)");
    ddl(&mut db, "INSERT INTO t VALUES(30)");

    let err = update(&mut db, "UPDATE t SET rowid = rowid + 1").unwrap_err();
    assert!(
        err.to_string().contains("UNIQUE constraint failed: t.rowid"),
        "expected rowid UNIQUE error for +1 cascade, got: {}",
        err
    );
}

/// `SET rowid = rowid - 1` over ascending rows succeeds: processed ascending,
/// each row's old slot is vacated before the next row needs it (1->0, then
/// 2->1 onto the now-free slot 1, ...). sqlite3 3.51.0 accepts this — the
/// asymmetry with `+1` confirms the row-by-row ascending processing order.
#[test]
fn set_rowid_minus_one_cascade_ok() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(a)");
    ddl(&mut db, "INSERT INTO t VALUES(10)");
    ddl(&mut db, "INSERT INTO t VALUES(20)");
    ddl(&mut db, "INSERT INTO t VALUES(30)");

    assert_eq!(update(&mut db, "UPDATE t SET rowid = rowid - 1").unwrap(), 3);

    let rows = select(&mut db, "SELECT rowid, a FROM t ORDER BY rowid");
    let got: Vec<(i64, i64)> =
        rows.iter().map(|r| (int(&r.values[0]), int(&r.values[1]))).collect();
    assert_eq!(got, vec![(0, 10), (1, 20), (2, 30)]);
}

/// Relocating into a free gap (no collision at any intermediate step) succeeds.
/// Matches sqlite3 3.51.0.
#[test]
fn set_rowid_move_to_gap_ok() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(a)");
    ddl(&mut db, "INSERT INTO t VALUES(10)");
    ddl(&mut db, "INSERT INTO t VALUES(20)");

    assert_eq!(update(&mut db, "UPDATE t SET rowid = rowid + 100 WHERE rowid = 1").unwrap(), 1);

    let rows = select(&mut db, "SELECT rowid, a FROM t ORDER BY rowid");
    let got: Vec<(i64, i64)> =
        rows.iter().map(|r| (int(&r.values[0]), int(&r.values[1]))).collect();
    assert_eq!(got, vec![(2, 20), (101, 10)]);
}

/// `SET rowid = rowid` is a no-op for every row and must not error. Matches
/// sqlite3 3.51.0.
#[test]
fn set_rowid_self_noop_ok() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(a)");
    ddl(&mut db, "INSERT INTO t VALUES(10)");
    ddl(&mut db, "INSERT INTO t VALUES(20)");
    ddl(&mut db, "INSERT INTO t VALUES(30)");

    assert_eq!(update(&mut db, "UPDATE t SET rowid = rowid").unwrap(), 3);

    let rows = select(&mut db, "SELECT rowid, a FROM t ORDER BY rowid");
    let got: Vec<(i64, i64)> =
        rows.iter().map(|r| (int(&r.values[0]), int(&r.values[1]))).collect();
    assert_eq!(got, vec![(1, 10), (2, 20), (3, 30)]);
}

/// On an INTEGER PRIMARY KEY table, `SET rowid=` writes the IPK column.
#[test]
fn set_rowid_aliases_ipk() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(k INTEGER PRIMARY KEY, v)");
    ddl(&mut db, "INSERT INTO t VALUES(1, 'a')");

    assert_eq!(update(&mut db, "UPDATE t SET rowid=7 WHERE k=1").unwrap(), 1);

    let rows = select(&mut db, "SELECT rowid, k, v FROM t");
    assert_eq!(int(&rows[0].values[0]), 7); // rowid
    assert_eq!(int(&rows[0].values[1]), 7); // k (IPK)
    assert_eq!(text(&rows[0].values[2]), "a");
}

/// IPK collision via `SET rowid=` reports against the IPK column, not `.rowid`.
#[test]
fn set_rowid_ipk_conflict_errors() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(k INTEGER PRIMARY KEY, v)");
    ddl(&mut db, "INSERT INTO t VALUES(1, 'a')");
    ddl(&mut db, "INSERT INTO t VALUES(2, 'b')");

    let err = update(&mut db, "UPDATE t SET rowid=2 WHERE k=1").unwrap_err();
    assert!(
        err.to_string().contains("UNIQUE constraint failed: t.k"),
        "expected IPK UNIQUE error, got: {}",
        err
    );
}

// ---------------------------------------------------------------------------
// Issue #5575: single-statement IPK (rowid-alias) relocation is checked
// IMMEDIATELY row-by-row, exactly like the virtual-rowid path above. On an
// INTEGER PRIMARY KEY table the IPK column IS the rowid, so a `SET <ipk>=` /
// `SET rowid=` swap / N-cycle / ascending `+1` cascade must error on the
// intermediate collision (NOT be deferred to the final state), reported against
// `<table>.<ipk>`. The `-1` descending cascade, move-to-gap, and self no-op
// still succeed. All verified against sqlite3 3.51.0.
//
// This is the IPK counterpart of the virtual-rowid tests above. It must NOT
// change deferred PK/UNIQUE checking for regular columns — see
// `regular_unique_column_swap_still_allowed_deferred` below.
// ---------------------------------------------------------------------------

/// IPK swap via the IPK column: `UPDATE t SET k=CASE...` errors on the
/// intermediate collision (sqlite3 3.51.0: `UNIQUE constraint failed: t.k`).
#[test]
fn set_ipk_swap_errors_intermediate_collision() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(k INTEGER PRIMARY KEY, v)");
    ddl(&mut db, "INSERT INTO t VALUES(1, 'a')");
    ddl(&mut db, "INSERT INTO t VALUES(2, 'b')");

    let err = update(&mut db, "UPDATE t SET k = CASE k WHEN 1 THEN 2 WHEN 2 THEN 1 END")
        .unwrap_err();
    assert!(
        err.to_string().contains("UNIQUE constraint failed: t.k"),
        "expected IPK UNIQUE error for swap, got: {}",
        err
    );

    // Table unchanged after the aborted UPDATE.
    let rows = select(&mut db, "SELECT k, v FROM t ORDER BY k");
    let got: Vec<(i64, String)> =
        rows.iter().map(|r| (int(&r.values[0]), text(&r.values[1]))).collect();
    assert_eq!(got, vec![(1, "a".to_string()), (2, "b".to_string())]);
}

/// IPK swap via the `rowid` alias on an IPK table — same immediate check, still
/// reported against the IPK column name (sqlite3 3.51.0).
#[test]
fn set_rowid_alias_ipk_swap_errors() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(k INTEGER PRIMARY KEY, v)");
    ddl(&mut db, "INSERT INTO t VALUES(1, 'a')");
    ddl(&mut db, "INSERT INTO t VALUES(2, 'b')");

    let err = update(
        &mut db,
        "UPDATE t SET rowid = CASE rowid WHEN 1 THEN 2 WHEN 2 THEN 1 END",
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("UNIQUE constraint failed: t.k"),
        "expected IPK UNIQUE error for rowid-alias swap, got: {}",
        err
    );
}

/// IPK 3-cycle (1->2, 2->3, 3->1) errors: ascending processing, 1->2 collides
/// with the still-live IPK 2. Matches sqlite3 3.51.0.
#[test]
fn set_ipk_three_cycle_errors() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(k INTEGER PRIMARY KEY, v)");
    ddl(&mut db, "INSERT INTO t VALUES(1, 'a')");
    ddl(&mut db, "INSERT INTO t VALUES(2, 'b')");
    ddl(&mut db, "INSERT INTO t VALUES(3, 'c')");

    let err = update(
        &mut db,
        "UPDATE t SET k = CASE k WHEN 1 THEN 2 WHEN 2 THEN 3 WHEN 3 THEN 1 END",
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("UNIQUE constraint failed: t.k"),
        "expected IPK UNIQUE error for 3-cycle, got: {}",
        err
    );
}

/// `UPDATE t SET k = k + 1` over ascending IPK rows cascade-collides: 1 -> 2
/// collides with the still-live IPK 2. sqlite3 3.51.0 errors (the IPK is the
/// rowid; checked immediately, not deferred).
#[test]
fn set_ipk_plus_one_cascade_errors() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(k INTEGER PRIMARY KEY, v)");
    ddl(&mut db, "INSERT INTO t VALUES(1, 'a')");
    ddl(&mut db, "INSERT INTO t VALUES(2, 'b')");
    ddl(&mut db, "INSERT INTO t VALUES(3, 'c')");

    let err = update(&mut db, "UPDATE t SET k = k + 1").unwrap_err();
    assert!(
        err.to_string().contains("UNIQUE constraint failed: t.k"),
        "expected IPK UNIQUE error for +1 cascade, got: {}",
        err
    );
}

/// `UPDATE t SET k = k - 1` over ascending IPK rows succeeds: processed
/// ascending, each old IPK slot is vacated before the next row needs it. sqlite3
/// 3.51.0 accepts this — the asymmetry with `+1` confirms the immediate
/// row-by-row ascending order (same as virtual rowid).
#[test]
fn set_ipk_minus_one_cascade_ok() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(k INTEGER PRIMARY KEY, v)");
    ddl(&mut db, "INSERT INTO t VALUES(2, 'a')");
    ddl(&mut db, "INSERT INTO t VALUES(3, 'b')");
    ddl(&mut db, "INSERT INTO t VALUES(4, 'c')");

    assert_eq!(update(&mut db, "UPDATE t SET k = k - 1").unwrap(), 3);

    let rows = select(&mut db, "SELECT k, v FROM t ORDER BY k");
    let got: Vec<(i64, String)> =
        rows.iter().map(|r| (int(&r.values[0]), text(&r.values[1]))).collect();
    assert_eq!(
        got,
        vec![(1, "a".to_string()), (2, "b".to_string()), (3, "c".to_string())]
    );
}

/// Relocating an IPK row into a free gap (no collision at any step) succeeds.
/// Matches sqlite3 3.51.0.
#[test]
fn set_ipk_move_to_gap_ok() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(k INTEGER PRIMARY KEY, v)");
    ddl(&mut db, "INSERT INTO t VALUES(1, 'a')");
    ddl(&mut db, "INSERT INTO t VALUES(2, 'b')");

    assert_eq!(update(&mut db, "UPDATE t SET k = k + 100 WHERE k = 1").unwrap(), 1);

    let rows = select(&mut db, "SELECT k, v FROM t ORDER BY k");
    let got: Vec<(i64, String)> =
        rows.iter().map(|r| (int(&r.values[0]), text(&r.values[1]))).collect();
    assert_eq!(got, vec![(2, "b".to_string()), (101, "a".to_string())]);
}

/// `UPDATE t SET k = k` is a no-op for every IPK row and must not error. Matches
/// sqlite3 3.51.0.
#[test]
fn set_ipk_self_noop_ok() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(k INTEGER PRIMARY KEY, v)");
    ddl(&mut db, "INSERT INTO t VALUES(1, 'a')");
    ddl(&mut db, "INSERT INTO t VALUES(2, 'b')");
    ddl(&mut db, "INSERT INTO t VALUES(3, 'c')");

    assert_eq!(update(&mut db, "UPDATE t SET k = k").unwrap(), 3);

    let rows = select(&mut db, "SELECT k, v FROM t ORDER BY k");
    let got: Vec<(i64, String)> =
        rows.iter().map(|r| (int(&r.values[0]), text(&r.values[1]))).collect();
    assert_eq!(
        got,
        vec![(1, "a".to_string()), (2, "b".to_string()), (3, "c".to_string())]
    );
}

/// REGRESSION GUARD (#5575 caution): the immediate IPK-as-rowid check must NOT
/// leak into regular (non-rowid) UNIQUE columns. VibeSQL defers UNIQUE checks
/// for regular columns to the final state (issue #5137), which lets a swap on a
/// non-IPK UNIQUE column be accepted here. `k` below is a plain UNIQUE column
/// (the rowid is the separate `id` IPK), so the immediate rowid relocation check
/// must not fire — this swap must STILL be allowed exactly as before this fix.
///
/// (sqlite3 3.51.0 actually rejects this regular-column swap too — VibeSQL's
/// deferred acceptance is a separate, pre-existing parity gap that is explicitly
/// OUT OF SCOPE for #5575; #5575 must not regress it in either direction.)
#[test]
fn regular_unique_column_swap_still_allowed_deferred() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE u(id INTEGER PRIMARY KEY, k INT UNIQUE)");
    ddl(&mut db, "INSERT INTO u VALUES(1, 10)");
    ddl(&mut db, "INSERT INTO u VALUES(2, 20)");

    // Swap the regular UNIQUE column `k` (NOT the IPK). Deferred final-state check
    // sees no duplicate, so VibeSQL accepts it — the immediate IPK relocation
    // check (which only inspects the rowid-alias column) must stay clear of it.
    assert_eq!(
        update(&mut db, "UPDATE u SET k = CASE k WHEN 10 THEN 20 WHEN 20 THEN 10 END").unwrap(),
        2
    );

    let rows = select(&mut db, "SELECT id, k FROM u ORDER BY id");
    let got: Vec<(i64, i64)> =
        rows.iter().map(|r| (int(&r.values[0]), int(&r.values[1]))).collect();
    assert_eq!(got, vec![(1, 20), (2, 10)]);
}

/// REGRESSION GUARD: a regular UNIQUE-column *shift* (`k = k - 10`) on a table
/// whose IPK (`id`) is unchanged must still succeed via the deferred path
/// (issue #5137). The IPK column is untouched, so the immediate rowid check finds
/// no relocations and does not interfere.
#[test]
fn regular_unique_column_shift_still_allowed_deferred() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE u(id INTEGER PRIMARY KEY, k INT UNIQUE)");
    ddl(&mut db, "INSERT INTO u VALUES(1, 10)");
    ddl(&mut db, "INSERT INTO u VALUES(2, 20)");
    ddl(&mut db, "INSERT INTO u VALUES(3, 30)");

    assert_eq!(update(&mut db, "UPDATE u SET k = k - 10").unwrap(), 3);

    let rows = select(&mut db, "SELECT id, k FROM u ORDER BY id");
    let got: Vec<(i64, i64)> =
        rows.iter().map(|r| (int(&r.values[0]), int(&r.values[1]))).collect();
    assert_eq!(got, vec![(1, 0), (2, 10), (3, 20)]);
}

/// An index on another column still finds the row after a rowid relocation.
#[test]
fn index_consistent_after_relocate() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(x, y)");
    ddl(&mut db, "CREATE INDEX iy ON t(y)");
    ddl(&mut db, "INSERT INTO t VALUES(1, 'aa')");
    ddl(&mut db, "INSERT INTO t VALUES(2, 'bb')");

    assert_eq!(update(&mut db, "UPDATE t SET rowid=99 WHERE rowid=1").unwrap(), 1);

    // Index scan on y must report the relocated rowid (99), not the slot (1).
    let rows = select(&mut db, "SELECT rowid, x, y FROM t WHERE y='aa'");
    assert_eq!(rows.len(), 1);
    assert_eq!(int(&rows[0].values[0]), 99);
    assert_eq!(int(&rows[0].values[1]), 1);
    assert_eq!(text(&rows[0].values[2]), "aa");
}

/// BEFORE/AFTER UPDATE triggers see OLD.rowid (old) and NEW.rowid (new).
#[test]
fn triggers_see_old_and_new_rowid() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t8(x)");
    ddl(&mut db, "CREATE TABLE t7(a, b)");
    ddl(&mut db, "INSERT INTO t7 VALUES(1, 2)");
    ddl(
        &mut db,
        "CREATE TRIGGER t7tb BEFORE UPDATE ON t7 BEGIN \
         INSERT INTO t8 VALUES('before ' || old.rowid || '->' || new.rowid); END",
    );
    ddl(
        &mut db,
        "CREATE TRIGGER t7ta AFTER UPDATE ON t7 BEGIN \
         INSERT INTO t8 VALUES('after ' || old.rowid || '->' || new.rowid); END",
    );

    assert_eq!(update(&mut db, "UPDATE t7 SET rowid=8 WHERE rowid=1").unwrap(), 1);

    let t7 = select(&mut db, "SELECT rowid, a, b FROM t7");
    assert_eq!(int(&t7[0].values[0]), 8);
    assert_eq!(int(&t7[0].values[1]), 1);
    assert_eq!(int(&t7[0].values[2]), 2);

    let t8 = select(&mut db, "SELECT x FROM t8 ORDER BY rowid");
    let log: Vec<String> = t8.iter().map(|r| text(&r.values[0])).collect();
    assert_eq!(log, vec!["before 1->8".to_string(), "after 1->8".to_string()]);
}

/// triggerC-7.5: a BEFORE UPDATE trigger relocates *another* row via
/// `SET rowid=`, then the AFTER UPDATE trigger logs old/new rowid. Verified
/// against sqlite3 3.51.0:
///   T7 = (2,3,4) (3,5,7) (8,1,2)
///   T8 = "after fired 1->8", "after fired 3->3"
///
/// The SET-rowid relocation (#5517) makes the *table* state (T7) match sqlite3
/// exactly: the nested `UPDATE t7 SET rowid=8` fired inside the BEFORE trigger
/// relocates rowid 1 -> 8.
///
/// The T8 AFTER-trigger log now ALSO matches sqlite3 exactly. The nested
/// `UPDATE t7 SET rowid=8` fired inside the BEFORE trigger body fires its own
/// AFTER trigger ("after fired 1->8") and logs FIRST (it completes before the
/// outer UPDATE's AFTER trigger runs), then the outer UPDATE's AFTER trigger
/// logs "after fired 3->3". This is the post-#5550 behavior: #5550 made nested
/// DML fired from a trigger body fire its own row triggers, which fully resolved
/// the gap previously tracked by #5557 for this scenario. (PR #5556 was written
/// against a base predating #5550 and pinned the THEN-missing "after fired 1->8"
/// as a documented follow-on; that delta no longer exists.)
#[test]
fn triggerc_7_5_relocate_in_before_trigger() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t8(x)");
    ddl(&mut db, "CREATE TABLE t7(a, b)");
    ddl(&mut db, "INSERT INTO t7 VALUES(1, 2)");
    ddl(&mut db, "INSERT INTO t7 VALUES(3, 4)");
    ddl(&mut db, "INSERT INTO t7 VALUES(5, 6)");
    ddl(
        &mut db,
        "CREATE TRIGGER t7t BEFORE UPDATE ON t7 WHEN (old.rowid!=1 OR new.rowid!=8) \
         BEGIN UPDATE t7 SET rowid = 8 WHERE rowid=1; END",
    );
    ddl(
        &mut db,
        "CREATE TRIGGER t7ta AFTER UPDATE ON t7 BEGIN \
         INSERT INTO t8 VALUES('after fired ' || old.rowid || '->' || new.rowid); END",
    );

    update(&mut db, "UPDATE t7 SET b=7 WHERE a = 5").unwrap();

    // Table state matches sqlite3 exactly: the nested SET-rowid relocate applied.
    let t7 = select(&mut db, "SELECT rowid, a, b FROM t7 ORDER BY rowid");
    let got: Vec<(i64, i64, i64)> = t7
        .iter()
        .map(|r| (int(&r.values[0]), int(&r.values[1]), int(&r.values[2])))
        .collect();
    assert_eq!(got, vec![(2, 3, 4), (3, 5, 7), (8, 1, 2)]);

    // Both AFTER triggers fire, in sqlite3 3.51.0 order: the nested UPDATE's
    // AFTER trigger logs "after fired 1->8" first (it completes inside the BEFORE
    // trigger body), then the outer UPDATE's AFTER trigger logs "after fired 3->3".
    let t8 = select(&mut db, "SELECT x FROM t8 ORDER BY rowid");
    let log: Vec<String> = t8.iter().map(|r| text(&r.values[0])).collect();
    assert_eq!(
        log,
        vec![
            "after fired 1->8".to_string(),
            "after fired 3->3".to_string()
        ]
    );
}
