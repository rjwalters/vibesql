//! Join-path parity tests for issue #6012: STDDEV / VARIANCE over an INNER JOIN
//! with GROUP BY, on the columnar join path.
//!
//! Confirms the columnar join GROUP BY path is taken (positive log marker) and
//! that its statistical-aggregate results match the row path
//! (`VIBESQL_DISABLE_COLUMNAR_JOIN=1`) within a float epsilon. Uses the PR #6006
//! SERIAL-mutex pattern because the env var and the log buffer are process-global.

use log::Level;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

mod common;
use common::{execute_sql, init_logger, run_select_values as run_select, take_logs, SERIAL};

/// Row-fallback marker: a joined GROUP BY key couldn't be resolved to a column.
const ROW_FALLBACK: &str = "Columnar join: some GROUP BY columns couldn't be resolved";
/// Positive marker emitted only when the columnar join path produced the result.
const COLUMNAR_JOIN_SUCCEEDED: &str = "Columnar join execution succeeded";

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

/// Assert two (already sorted) row sets are equal, with a float epsilon on
/// numeric cells and exact equality on everything else.
fn assert_rows_close(columnar: &[Vec<SqlValue>], row: &[Vec<SqlValue>], ctx: &str) {
    assert_eq!(columnar.len(), row.len(), "row count differs for {ctx}");
    for (i, (c, r)) in columnar.iter().zip(row.iter()).enumerate() {
        assert_eq!(c.len(), r.len(), "column count differs at row {i} for {ctx}");
        for (j, (cv, rv)) in c.iter().zip(r.iter()).enumerate() {
            match (as_f64(cv), as_f64(rv)) {
                (Some(a), Some(b)) => {
                    let tol = 1e-9 * (1.0 + a.abs().max(b.abs()));
                    assert!(
                        (a - b).abs() <= tol,
                        "row {i} col {j} numeric mismatch for {ctx}: columnar={a} row={b}"
                    );
                }
                (None, None) => {}
                _ => assert_eq!(cv, rv, "row {i} col {j} mismatch for {ctx}"),
            }
        }
    }
}

/// Run the query on both the columnar-join path and the row path
/// (`VIBESQL_DISABLE_COLUMNAR_JOIN=1`); returns `(columnar, row, logs)` with both
/// result sets sorted (parity is order-independent here).
fn run_both(db: &Database, sql: &str) -> (Vec<Vec<SqlValue>>, Vec<Vec<SqlValue>>, Vec<String>) {
    let _ = take_logs();
    let mut columnar = run_select(db, sql);
    let logs = take_logs();

    std::env::set_var("VIBESQL_DISABLE_COLUMNAR_JOIN", "1");
    let mut row = run_select(db, sql);
    std::env::remove_var("VIBESQL_DISABLE_COLUMNAR_JOIN");

    columnar.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    row.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    (columnar, row, logs)
}

fn assert_columnar_join_ran(logs: &[String]) {
    let joined = logs.join("\n");
    assert!(
        !joined.contains(ROW_FALLBACK),
        "row-fallback line emitted; columnar join path skipped:\n{joined}"
    );
    assert!(
        logs.iter().any(|l| l.contains(COLUMNAR_JOIN_SUCCEEDED)),
        "expected columnar join GROUP BY to run:\n{joined}"
    );
}

/// `l(k, a, v)` fact table joined to `r(k, d)` dim table on `k`.
fn setup_inner(db: &mut Database, n: i64) {
    execute_sql(db, "CREATE TABLE l (k INTEGER, a INTEGER, v INTEGER)");
    execute_sql(db, "CREATE TABLE r (k INTEGER, d INTEGER)");
    let mut ins = String::new();
    for i in 0..n {
        let k = i % 4;
        let a = i % 3;
        let v = (i % 17) + 1;
        ins.push_str(&format!("INSERT INTO l VALUES ({k}, {a}, {v});"));
    }
    for k in 0..4 {
        ins.push_str(&format!("INSERT INTO r VALUES ({k}, {});", k * 10));
    }
    execute_sql(db, &ins);
}

const SPELLINGS: [&str; 6] =
    ["STDDEV", "STDDEV_SAMP", "STDDEV_POP", "VARIANCE", "VAR_SAMP", "VAR_POP"];

#[test]
fn join_group_by_stddev_variance_all_spellings_match_row_path() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    setup_inner(&mut db, 800);

    for f in SPELLINGS {
        let sql = format!("SELECT l.a, {f}(l.v) FROM l JOIN r ON l.k = r.k GROUP BY l.a");
        let (columnar, row, logs) = run_both(&db, &sql);
        assert_columnar_join_ran(&logs);
        assert_rows_close(&columnar, &row, &sql);
    }
}

#[test]
fn join_group_by_mixed_aggregates_match_row_path() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    setup_inner(&mut db, 800);

    // A SUM/COUNT alongside a statistical aggregate must all run columnar.
    let sql = "SELECT l.a, SUM(l.v), VAR_POP(l.v), STDDEV_SAMP(l.v), COUNT(*) \
               FROM l JOIN r ON l.k = r.k GROUP BY l.a";
    let (columnar, row, logs) = run_both(&db, sql);
    assert_columnar_join_ran(&logs);
    assert_rows_close(&columnar, &row, sql);
}

#[test]
fn join_group_by_stddev_variance_null_values_match_row_path() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE l (k INTEGER, a INTEGER, v INTEGER)");
    execute_sql(&mut db, "CREATE TABLE r (k INTEGER, d INTEGER)");
    let mut ins = String::new();
    for i in 0..500i64 {
        let k = i % 3;
        let a = i % 4;
        if i % 6 == 0 {
            ins.push_str(&format!("INSERT INTO l VALUES ({k}, {a}, NULL);"));
        } else {
            ins.push_str(&format!("INSERT INTO l VALUES ({k}, {a}, {});", (i % 13) + 1));
        }
    }
    for k in 0..3 {
        ins.push_str(&format!("INSERT INTO r VALUES ({k}, {});", k));
    }
    execute_sql(&mut db, &ins);

    let sql = "SELECT l.a, VAR_POP(l.v), STDDEV_SAMP(l.v) FROM l JOIN r ON l.k = r.k GROUP BY l.a";
    let (columnar, row, logs) = run_both(&db, sql);
    assert_columnar_join_ran(&logs);
    assert_rows_close(&columnar, &row, sql);
}
