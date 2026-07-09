//! Path-assertion + parity tests for issue #6012: STDDEV / VARIANCE aggregates
//! on the native columnar path.
//!
//! Coverage:
//!   - The native columnar path (not row fallback) is taken for flat
//!     `STDDEV/VARIANCE`, a mixed SELECT list combining `SUM/AVG` with a
//!     statistical aggregate (proves the whole-query drop is gone), and
//!     `GROUP BY` with a statistical aggregate.
//!   - Result parity vs the row path (`VIBESQL_DISABLE_COLUMNAR=1`) for all six
//!     spellings, within a float epsilon.
//!
//! Wall-clock timing is NOT used (the machine is loaded). We capture `log`
//! output and assert the positive marker "Native columnar execution completed"
//! appears and the row-fallback marker does NOT. Every test acquires the
//! `SERIAL` mutex (PR #6006 pattern) because `VIBESQL_DISABLE_COLUMNAR` and the
//! log buffer are process-global.

use log::Level;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

mod common;
use common::{execute_sql, init_logger, run_select, take_logs, SERIAL};

/// Row-fallback marker (single-table path): the columnar runtime declined.
const ROW_FALLBACK: &str = "Standard columnar runtime fallback to row-oriented";
/// Positive marker emitted only when the native columnar path completes.
const COLUMNAR_COMPLETED: &str = "Native columnar execution completed";

/// Coerce a numeric SqlValue to f64 for epsilon comparison; NULL → None.
fn as_f64(v: &SqlValue) -> Option<f64> {
    match v {
        SqlValue::Null => None,
        SqlValue::Integer(x) => Some(*x as f64),
        SqlValue::Bigint(x) => Some(*x as f64),
        SqlValue::Smallint(x) => Some(*x as f64),
        SqlValue::Double(x) => Some(*x),
        SqlValue::Float(x) => Some(*x as f64),
        SqlValue::Real(x) => Some(*x),
        SqlValue::Numeric(x) => Some(*x),
        other => panic!("non-numeric aggregate result: {other:?}"),
    }
}

/// Compare two row sets in emitted order, cell by cell, with a float epsilon for
/// numeric cells (statistical aggregates are REAL, so exact equality is brittle).
fn assert_rows_close_in_order(
    columnar: &[vibesql_storage::Row],
    row: &[vibesql_storage::Row],
    ctx: &str,
) {
    assert_eq!(
        columnar.len(),
        row.len(),
        "row count differs for {ctx}: columnar={} row={}",
        columnar.len(),
        row.len()
    );
    for (i, (c, r)) in columnar.iter().zip(row.iter()).enumerate() {
        assert_eq!(c.values.len(), r.values.len(), "column count differs at row {i} for {ctx}");
        for (j, (cv, rv)) in c.values.iter().zip(r.values.iter()).enumerate() {
            match (as_f64(cv), as_f64(rv)) {
                (Some(a), Some(b)) => {
                    let tol = 1e-9 * (1.0 + a.abs().max(b.abs()));
                    assert!(
                        (a - b).abs() <= tol,
                        "row {i} col {j} numeric mismatch for {ctx}: columnar={a} row={b}"
                    );
                }
                (None, None) => {} // both NULL — OK
                _ => {
                    assert_eq!(
                        cv, rv,
                        "row {i} col {j} mismatch for {ctx}: columnar={cv:?} row={rv:?}"
                    );
                }
            }
        }
    }
}

/// Run `sql` on the columnar path (asserting it is taken) and on the row path,
/// then assert epsilon parity. Returns the columnar rows for extra checks.
fn columnar_matches_row(db: &Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let _ = take_logs(); // discard prior logs

    let columnar = run_select(db, sql);
    let logs = take_logs();
    let joined = logs.join("\n");

    assert!(
        !joined.contains(ROW_FALLBACK),
        "row-fallback marker emitted for `{sql}`; columnar path was skipped:\n{joined}"
    );
    assert!(
        logs.iter().any(|l| l.contains(COLUMNAR_COMPLETED)),
        "expected native columnar execution to run for `{sql}`:\n{joined}"
    );

    std::env::set_var("VIBESQL_DISABLE_COLUMNAR", "1");
    let row = run_select(db, sql);
    std::env::remove_var("VIBESQL_DISABLE_COLUMNAR");

    assert_rows_close_in_order(&columnar, &row, sql);
    columnar
}

/// Populate `t(g INTEGER, v INTEGER, r DOUBLE)` with `n` rows. `g` is a
/// low-cardinality group key, `v`/`r` are aggregated measures.
fn setup(db: &mut Database, n: i64) {
    execute_sql(db, "CREATE TABLE t (g INTEGER, v INTEGER, r DOUBLE)");
    let mut ins = String::new();
    for i in 0..n {
        let g = i % 5;
        let v = (i % 17) + 1;
        let r = (i as f64 % 13.0) * 1.5 + 0.25;
        ins.push_str(&format!("INSERT INTO t VALUES ({g}, {v}, {r});"));
    }
    execute_sql(db, &ins);
}

const SPELLINGS: [&str; 6] =
    ["STDDEV", "STDDEV_SAMP", "STDDEV_POP", "VARIANCE", "VAR_SAMP", "VAR_POP"];

#[test]
fn flat_stddev_variance_all_spellings_columnar_parity() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    setup(&mut db, 600);
    for f in SPELLINGS {
        columnar_matches_row(&db, &format!("SELECT {f}(v) FROM t"));
        columnar_matches_row(&db, &format!("SELECT {f}(r) FROM t"));
    }
}

#[test]
fn mixed_select_list_removes_whole_query_drop() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    setup(&mut db, 600);
    // A SELECT list combining supported aggregates with a statistical aggregate
    // must take the columnar path as a whole (no fallback for the entire query).
    columnar_matches_row(&db, "SELECT SUM(v), AVG(v), STDDEV(v), VAR_POP(v), COUNT(*) FROM t");
}

#[test]
fn group_by_stddev_variance_all_spellings_columnar_parity() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    setup(&mut db, 900);
    for f in SPELLINGS {
        columnar_matches_row(&db, &format!("SELECT g, {f}(v) FROM t GROUP BY g ORDER BY g"));
    }
}

#[test]
fn group_by_mixed_aggregates_columnar_parity() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    setup(&mut db, 900);
    columnar_matches_row(
        &db,
        "SELECT g, SUM(v), STDDEV_SAMP(v), VAR_POP(r) FROM t GROUP BY g ORDER BY g",
    );
}

#[test]
fn stddev_variance_with_where_filter_columnar_parity() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    setup(&mut db, 600);
    // WHERE filtering must apply before the statistical aggregate.
    columnar_matches_row(&db, "SELECT VAR_POP(v), STDDEV_POP(v) FROM t WHERE v > 5");
}

#[test]
fn stddev_variance_single_value_group_edge_cases() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    // Each group has exactly one row: sample variants → NULL, population → 0.0.
    execute_sql(&mut db, "CREATE TABLE s (g INTEGER, v INTEGER)");
    execute_sql(&mut db, "INSERT INTO s VALUES (1, 10), (2, 20), (3, 30)");
    columnar_matches_row(
        &db,
        "SELECT g, VAR_SAMP(v), VAR_POP(v), STDDEV_SAMP(v), STDDEV_POP(v) \
         FROM s GROUP BY g ORDER BY g",
    );
}

#[test]
fn stddev_variance_null_handling_columnar_parity() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE nn (g INTEGER, v INTEGER)");
    let mut ins = String::new();
    for i in 0..400i64 {
        let g = i % 4;
        if i % 5 == 0 {
            ins.push_str(&format!("INSERT INTO nn VALUES ({g}, NULL);"));
        } else {
            ins.push_str(&format!("INSERT INTO nn VALUES ({g}, {});", (i % 11) + 1));
        }
    }
    execute_sql(&mut db, &ins);
    // NULLs skipped; result parity per group across all spellings.
    columnar_matches_row(
        &db,
        "SELECT g, VAR_POP(v), STDDEV_SAMP(v), VARIANCE(v) FROM nn GROUP BY g ORDER BY g",
    );
}

/// Numerical stability: a near-constant, large-magnitude column must not diverge
/// from the row path (guards against naive sum-of-squares cancellation).
#[test]
fn stddev_variance_large_magnitude_stability() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE big (v DOUBLE)");
    let mut ins = String::new();
    for i in 0..500i64 {
        // Values clustered tightly around 1e9.
        let v = 1.0e9 + (i % 7) as f64 * 0.5;
        ins.push_str(&format!("INSERT INTO big VALUES ({v});"));
    }
    execute_sql(&mut db, &ins);
    columnar_matches_row(&db, "SELECT VAR_POP(v), STDDEV_POP(v), VAR_SAMP(v) FROM big");
}
