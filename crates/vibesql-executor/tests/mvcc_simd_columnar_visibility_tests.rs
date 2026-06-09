//! Issue #5206: SIMD/columnar fast-path visibility under MVCC.
//!
//! Phase 1d (#5151) of #5136 gated the SIMD/columnar fast paths in
//! `select::scan::table` to MVCC-OFF only, because the original fast
//! paths bypassed `Row::visible_to` and would surface rows that the
//! current snapshot must not see (uncommitted/future-txn writes, or
//! tombstones from a concurrent writer).
//!
//! Issue #5206 re-enables these fast paths under `mvcc_enabled = ON` by
//! applying an `is_row_visible` post-filter on the SIMD-passing indices
//! (Approach A from the issue). These tests pin that behavior.
//!
//! ## Why the table is sized at ≥500 rows
//!
//! The columnar fast paths are gated by `SIMD_COLUMNAR_THRESHOLD = 500`
//! in `select::scan::table`. Below that, the row-by-row path is used
//! (which has always honored visibility). To exercise the SIMD-cached
//! columnar path, every test must populate enough live rows to cross
//! that threshold.
//!
//! Run with:
//! ```text
//! cargo test -p vibesql-executor --test mvcc_simd_columnar_visibility_tests
//! cargo test -p vibesql-executor --test mvcc_simd_columnar_visibility_tests --features mvcc_enabled
//! ```

use vibesql_ast::{SelectStmt, Statement};
use vibesql_executor::{
    BeginTransactionExecutor, CommitExecutor, CreateTableExecutor, DeleteExecutor, InsertExecutor,
    SelectExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Cross this to engage the SIMD/cached-columnar fast path in
/// `select::scan::table::execute_table_scan`.
const SIMD_THRESHOLD: usize = 500;

// ============================================================================
// Helpers
// ============================================================================

fn exec(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE failed");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT failed");
        }
        Statement::Delete(s) => {
            DeleteExecutor::execute(&s, db).expect("DELETE failed");
        }
        Statement::BeginTransaction(s) => {
            BeginTransactionExecutor::execute(&s, db).expect("BEGIN failed");
        }
        Statement::Commit(s) => {
            CommitExecutor::execute(&s, db).expect("COMMIT failed");
        }
        other => panic!("unsupported statement in test helper: {:?}", other),
    }
}

fn select(db: &mut Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    let select_stmt: SelectStmt = match stmt {
        Statement::Select(s) => *s,
        other => panic!("expected SELECT, got {:?}", other),
    };
    let executor = SelectExecutor::new(db);
    let rows = executor.execute(&select_stmt).expect("SELECT failed");
    rows.into_iter().map(|r| r.values.to_vec()).collect()
}

/// Build a database with a `metrics(id INTEGER PRIMARY KEY, bucket INTEGER, value INTEGER)`
/// table holding `count` pre-MVCC rows. Each row has:
///   - `id = i` (1..=count)
///   - `bucket = i % 10`
///   - `value = i * 2`
///
/// `count` should be ≥ `SIMD_THRESHOLD` so subsequent SELECTs engage the
/// columnar fast path.
fn db_with_metrics(count: usize) -> Database {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE metrics (id INTEGER PRIMARY KEY, bucket INTEGER, value INTEGER)");
    for i in 1..=count {
        let sql = format!("INSERT INTO metrics VALUES ({}, {}, {})", i, i % 10, i * 2);
        exec(&mut db, &sql);
    }
    db
}

/// Same shape as [`db_with_metrics`], but the table uses **native columnar
/// storage** (`STORAGE = COLUMNAR`). This exercises the
/// `filter_with_simd_columnar` fast path in `select::scan::table` (the
/// `is_native_columnar()` branch) instead of the cached-columnar path.
fn db_with_columnar_metrics(count: usize) -> Database {
    let mut db = Database::new();
    exec(
        &mut db,
        "CREATE TABLE metrics (id INTEGER PRIMARY KEY, bucket INTEGER, value INTEGER) STORAGE = COLUMNAR",
    );
    for i in 1..=count {
        let sql = format!("INSERT INTO metrics VALUES ({}, {}, {})", i, i % 10, i * 2);
        exec(&mut db, &sql);
    }
    db
}

// ============================================================================
// Off-state + on-state common: the columnar fast path returns the same
// results as the row-by-row path on pre-MVCC data. This is the safety
// net for both feature states.
// ============================================================================

#[test]
fn columnar_fast_path_filter_returns_correct_rows_on_pre_mvcc_data() {
    // Populate enough rows to trip the SIMD threshold.
    let row_count = SIMD_THRESHOLD * 2; // 1000
    let mut db = db_with_metrics(row_count);

    // WHERE bucket = 3 should match exactly one tenth of rows.
    let rows = select(&mut db, "SELECT id FROM metrics WHERE bucket = 3");
    assert_eq!(
        rows.len(),
        row_count / 10,
        "columnar fast path must return all pre-MVCC rows matching the predicate"
    );

    // All ids should satisfy id % 10 == 3.
    for r in &rows {
        match r[0] {
            SqlValue::Integer(id) => assert_eq!(id % 10, 3, "id={}", id),
            ref other => panic!("expected Integer id, got {:?}", other),
        }
    }
}

#[test]
fn columnar_fast_path_respects_deletion_bitmap() {
    // Pre-existing semantics: the bitmap-deleted rows must never be
    // surfaced even by the columnar fast path. This is enforced by
    // `Table::is_row_visible` (which is now applied as a post-filter
    // under MVCC-ON, and was always the contract under MVCC-OFF via
    // `scan_columnar`'s deletion filtering).
    let row_count = SIMD_THRESHOLD * 2; // 1000
    let mut db = db_with_metrics(row_count);

    // Delete all rows in bucket 3 (= 100 rows).
    exec(&mut db, "DELETE FROM metrics WHERE bucket = 3");

    // SELECT on bucket 3 must now return zero rows.
    let rows = select(&mut db, "SELECT id FROM metrics WHERE bucket = 3");
    assert_eq!(rows.len(), 0, "deleted rows must not be visible through the columnar fast path");

    // And bucket 4 should still return its full share.
    let rows = select(&mut db, "SELECT id FROM metrics WHERE bucket = 4");
    assert_eq!(rows.len(), row_count / 10);
}

// ============================================================================
// On-state: transactional inserts/deletes are observed correctly through
// the columnar fast path. These checks are only assertable when
// `Row::visible_to` is actually consulted — i.e., under
// `mvcc_enabled = ON`. Under MVCC-OFF, the inner snapshot argument is
// inert and the assertions still hold because `is_row_visible` reduces
// to the deletion-bitmap check.
// ============================================================================

#[test]
fn columnar_fast_path_sees_committed_transactional_insert() {
    // Insert ≥ SIMD_THRESHOLD pre-MVCC rows, then commit a transactional
    // INSERT that adds one more matching row. After commit, the autocommit
    // SELECT (which now synthesizes a commit-time snapshot — fix from
    // #5207) must see the new row through the columnar fast path.
    let row_count = SIMD_THRESHOLD * 2; // 1000
    let mut db = db_with_metrics(row_count);

    // Baseline: bucket 7 has 100 rows.
    let baseline = select(&mut db, "SELECT id FROM metrics WHERE bucket = 7");
    assert_eq!(baseline.len(), row_count / 10);

    // Commit a transactional INSERT that matches bucket = 7.
    let new_id = (row_count + 7) as i64; // 1007 -> bucket = 7
    exec(&mut db, "BEGIN");
    exec(&mut db, &format!("INSERT INTO metrics VALUES ({}, 7, {})", new_id, new_id * 2));
    exec(&mut db, "COMMIT");

    let after = select(&mut db, "SELECT id FROM metrics WHERE bucket = 7");
    assert_eq!(
        after.len(),
        baseline.len() + 1,
        "columnar fast path must see the committed transactional insert"
    );
    let ids: Vec<i64> = after
        .iter()
        .map(|r| match r[0] {
            SqlValue::Integer(v) => v,
            ref other => panic!("expected Integer, got {:?}", other),
        })
        .collect();
    assert!(ids.contains(&new_id));
}

#[test]
fn columnar_fast_path_hides_rows_tombstoned_by_committed_delete() {
    // Issue #5206 invariant: a committed DELETE inside a txn must hide
    // its rows from subsequent autocommit reads going through the
    // columnar fast path. Without the post-SIMD `is_row_visible` filter
    // this would silently expose tombstoned rows.
    let row_count = SIMD_THRESHOLD * 2;
    let mut db = db_with_metrics(row_count);

    exec(&mut db, "BEGIN");
    exec(&mut db, "DELETE FROM metrics WHERE bucket = 5");
    exec(&mut db, "COMMIT");

    let after = select(&mut db, "SELECT id FROM metrics WHERE bucket = 5");
    assert_eq!(
        after.len(),
        0,
        "columnar fast path must hide rows tombstoned by a committed transactional DELETE"
    );

    // Sanity: rows in other buckets are unaffected.
    let other = select(&mut db, "SELECT id FROM metrics WHERE bucket = 6");
    assert_eq!(other.len(), row_count / 10);
}

#[cfg(feature = "mvcc_enabled")]
#[test]
fn columnar_fast_path_in_txn_sees_pre_begin_data() {
    // Snapshot-isolation foundation: a txn's BEGIN-time snapshot must
    // observe every pre-MVCC row through the columnar fast path. This is
    // the cross-feature invariant — if it ever broke, every analytical
    // query inside a transaction would silently start returning empty.
    let row_count = SIMD_THRESHOLD * 2;
    let mut db = db_with_metrics(row_count);

    exec(&mut db, "BEGIN");
    let rows = select(&mut db, "SELECT id FROM metrics WHERE bucket = 2");
    assert_eq!(
        rows.len(),
        row_count / 10,
        "txn snapshot must see all pre-MVCC rows through the columnar fast path"
    );
    exec(&mut db, "COMMIT");
}

// ============================================================================
// Native columnar storage (`STORAGE = COLUMNAR`): the
// `filter_with_simd_columnar` fast path. The `native_columnar` mirror
// holds exactly the bitmap-live rows (inserts append, deletes rebuild,
// rollback restores the BEGIN-time clone), and under the single-writer
// model every bitmap-live row is visible to the active reader — so this
// path runs unfiltered under MVCC-ON. These tests pin that equivalence
// with the row-by-row path.
// ============================================================================

#[test]
fn native_columnar_fast_path_filter_returns_correct_rows() {
    let row_count = SIMD_THRESHOLD * 2; // 1000
    let mut db = db_with_columnar_metrics(row_count);

    let rows = select(&mut db, "SELECT id FROM metrics WHERE bucket = 3");
    assert_eq!(
        rows.len(),
        row_count / 10,
        "native columnar fast path must return all rows matching the predicate"
    );
    for r in &rows {
        match r[0] {
            SqlValue::Integer(id) => assert_eq!(id % 10, 3, "id={}", id),
            ref other => panic!("expected Integer id, got {:?}", other),
        }
    }
}

#[test]
fn native_columnar_fast_path_sees_committed_transactional_insert() {
    let row_count = SIMD_THRESHOLD * 2;
    let mut db = db_with_columnar_metrics(row_count);

    let baseline = select(&mut db, "SELECT id FROM metrics WHERE bucket = 7");
    assert_eq!(baseline.len(), row_count / 10);

    let new_id = (row_count + 7) as i64; // bucket = 7
    exec(&mut db, "BEGIN");
    exec(&mut db, &format!("INSERT INTO metrics VALUES ({}, 7, {})", new_id, new_id * 2));
    exec(&mut db, "COMMIT");

    let after = select(&mut db, "SELECT id FROM metrics WHERE bucket = 7");
    assert_eq!(
        after.len(),
        baseline.len() + 1,
        "native columnar fast path must see the committed transactional insert"
    );
}

#[test]
fn native_columnar_fast_path_hides_rows_deleted_in_committed_txn() {
    let row_count = SIMD_THRESHOLD * 2;
    let mut db = db_with_columnar_metrics(row_count);

    exec(&mut db, "BEGIN");
    exec(&mut db, "DELETE FROM metrics WHERE bucket = 5");
    exec(&mut db, "COMMIT");

    let after = select(&mut db, "SELECT id FROM metrics WHERE bucket = 5");
    assert_eq!(
        after.len(),
        0,
        "native columnar fast path must hide rows deleted by a committed transactional DELETE"
    );

    let other = select(&mut db, "SELECT id FROM metrics WHERE bucket = 6");
    assert_eq!(other.len(), row_count / 10);
}

#[cfg(feature = "mvcc_enabled")]
#[test]
fn native_columnar_fast_path_sees_own_txn_insert() {
    let row_count = SIMD_THRESHOLD * 2;
    let mut db = db_with_columnar_metrics(row_count);

    exec(&mut db, "BEGIN");
    let new_id = (row_count + 9) as i64; // bucket = 9
    exec(&mut db, &format!("INSERT INTO metrics VALUES ({}, 9, {})", new_id, new_id * 2));

    let in_txn = select(&mut db, "SELECT id FROM metrics WHERE bucket = 9");
    assert_eq!(
        in_txn.len(),
        row_count / 10 + 1,
        "txn must see its own INSERT through the native columnar fast path"
    );

    exec(&mut db, "COMMIT");
}

#[cfg(feature = "mvcc_enabled")]
#[test]
fn columnar_fast_path_sees_own_txn_insert() {
    // #5207 + #5206: a txn must see its own writes in subsequent reads.
    // Exercise that through the columnar fast path by inserting > 0
    // matching rows inside a txn and then SELECTing within the same txn.
    let row_count = SIMD_THRESHOLD * 2;
    let mut db = db_with_metrics(row_count);

    exec(&mut db, "BEGIN");
    let new_id = (row_count + 9) as i64; // bucket = 9
    exec(&mut db, &format!("INSERT INTO metrics VALUES ({}, 9, {})", new_id, new_id * 2));

    let in_txn = select(&mut db, "SELECT id FROM metrics WHERE bucket = 9");
    assert_eq!(
        in_txn.len(),
        row_count / 10 + 1,
        "txn must see its own INSERT through the columnar fast path"
    );

    exec(&mut db, "COMMIT");
}
