//! Path-assertion + parity tests for issue #6009 (Stage S): confirm that a
//! single-table `GROUP BY ... [HAVING ...] ORDER BY ...` query takes the native
//! columnar path instead of falling back to row-oriented execution, and that its
//! emitted row order matches the row path exactly.
//!
//! Per the acceptance criteria:
//!   - Wall-clock timing is NOT used (the machine is loaded). Instead we capture
//!     `log` output and assert the positive marker "Native columnar execution
//!     completed" appears and the row-fallback marker "Standard columnar runtime
//!     fallback to row-oriented" does NOT.
//!   - Result parity is checked against the row path (`VIBESQL_DISABLE_COLUMNAR=1`).
//!   - Ordering parity is compared in EMITTED ROW ORDER — neither side is sorted
//!     by the test harness. (A prior pre-sorting harness masked a real ordering
//!     bug; we must not repeat it.)
//!
//! This test installs a process-global `log` logger, so it lives in its own test
//! binary. Every test acquires the `SERIAL` mutex (PR #6006 pattern) because
//! `VIBESQL_DISABLE_COLUMNAR` and the log buffer are process-global.

use log::Level;
use vibesql_storage::Database;

mod common;
use common::{execute_sql, init_logger, run_select, take_logs, SERIAL};

/// Row-fallback marker (single-table path): the columnar runtime declined.
const ROW_FALLBACK: &str = "Standard columnar runtime fallback to row-oriented";
/// Positive marker emitted only when the native columnar path completes.
const COLUMNAR_COMPLETED: &str = "Native columnar execution completed";

/// Compare two row sets in EMITTED ORDER (no sorting either side).
fn assert_rows_eq_in_order(
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
        assert_eq!(
            c.values, r.values,
            "row {i} differs for {ctx} (emitted order, NOT sorted):\ncolumnar={:?}\nrow     ={:?}",
            c.values, r.values
        );
    }
}

/// Run `sql` on the columnar path (asserting it is taken) and on the row path,
/// then assert emitted-order parity. Returns the columnar rows for extra checks.
fn columnar_matches_row_ordered(db: &Database, sql: &str) -> Vec<vibesql_storage::Row> {
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

    assert_rows_eq_in_order(&columnar, &row, sql);
    columnar
}

/// Populate `t(a INTEGER, b INTEGER, v INTEGER)` with `n` rows. `a`/`b` are the
/// low-cardinality group keys, `v` is the aggregated measure.
fn setup(db: &mut Database, n: i64) {
    execute_sql(db, "CREATE TABLE t (a INTEGER, b INTEGER, v INTEGER)");
    let mut ins = String::new();
    for i in 0..n {
        let a = i % 5;
        let b = i % 3;
        let v = (i % 17) + 1;
        ins.push_str(&format!("INSERT INTO t VALUES ({a}, {b}, {v});"));
    }
    execute_sql(db, &ins);
}

#[test]
fn group_by_order_by_bare_key_asc() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    setup(&mut db, 600);
    columnar_matches_row_ordered(&db, "SELECT a, SUM(v) FROM t GROUP BY a ORDER BY a");
}

#[test]
fn group_by_order_by_bare_key_desc() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    setup(&mut db, 600);
    columnar_matches_row_ordered(&db, "SELECT a, SUM(v) FROM t GROUP BY a ORDER BY a DESC");
}

#[test]
fn group_by_order_by_aggregate_desc() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    setup(&mut db, 600);
    // ORDER BY an aggregate that appears in the SELECT list.
    columnar_matches_row_ordered(&db, "SELECT a, SUM(v) FROM t GROUP BY a ORDER BY SUM(v) DESC");
}

#[test]
fn group_by_order_by_ordinal_position() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    setup(&mut db, 600);
    // ORDER BY 2 references the aggregate (second output column).
    columnar_matches_row_ordered(&db, "SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY 2");
}

#[test]
fn group_by_order_by_multi_key() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    setup(&mut db, 900);
    // Two group keys, multi-key ORDER BY with mixed direction.
    columnar_matches_row_ordered(
        &db,
        "SELECT a, b, SUM(v) FROM t GROUP BY a, b ORDER BY a DESC, b ASC",
    );
}

#[test]
fn group_by_order_by_derived_key_expression() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    setup(&mut db, 900);
    // Derived-key GROUP BY (#6001-style): ORDER BY must resolve to the derived
    // key output column, not re-evaluate against the base schema.
    columnar_matches_row_ordered(
        &db,
        "SELECT a + b, SUM(v) FROM t GROUP BY a + b ORDER BY a + b DESC",
    );
}

#[test]
fn group_by_having_then_order_by() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    setup(&mut db, 900);
    // HAVING filters groups, then ORDER BY sorts the survivors — verifies the
    // sort runs AFTER the HAVING filter, matching the row path.
    columnar_matches_row_ordered(
        &db,
        "SELECT a, SUM(v) FROM t GROUP BY a HAVING SUM(v) > 100 ORDER BY SUM(v) DESC",
    );
}

/// Populate `n(k INTEGER, v INTEGER)` with 400 rows where `k` is NULL every 7th
/// row (the data shape from issue #6014's repro), then GROUP BY `k`.
fn setup_null_keyed(db: &mut Database) {
    execute_sql(db, "CREATE TABLE n (k INTEGER, v INTEGER)");
    let mut ins = String::new();
    for i in 0..400i64 {
        if i % 7 == 0 {
            ins.push_str(&format!("INSERT INTO n VALUES (NULL, {});", (i % 11) + 1));
        } else {
            ins.push_str(&format!("INSERT INTO n VALUES ({}, {});", i % 4, (i % 11) + 1));
        }
    }
    execute_sql(db, &ins);
    let _ = take_logs();
}

/// NULL group key ordering, ASC (#6014). The columnar and row-oriented aggregate
/// GROUP BY + ORDER BY paths must BOTH place a NULL group key FIRST for
/// `ORDER BY k ASC`, matching SQLite (NULL is the smallest value). Prior to the
/// #6014 fix the row path inverted the default NULL placement (NULL last on ASC);
/// this now asserts row/columnar parity in emitted order rather than documenting
/// the discrepancy.
#[test]
fn group_by_order_by_null_keys_first_ascending() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    setup_null_keyed(&mut db);

    // Row/columnar parity in emitted order (both must match SQLite semantics).
    let cols =
        columnar_matches_row_ordered(&db, "SELECT k, SUM(v) FROM n GROUP BY k ORDER BY k ASC");

    // SQLite-correct order: NULL group key sorts first ascending.
    assert_eq!(
        cols.first().map(|r| &r.values[0]),
        Some(&vibesql_types::SqlValue::Null),
        "expected NULL group key to sort first ascending (SQLite semantics); got {cols:?}"
    );
    // The remaining group keys must be in ascending order after the NULL.
    let non_null_keys: Vec<i64> = cols
        .iter()
        .skip(1)
        .map(|r| match &r.values[0] {
            vibesql_types::SqlValue::Integer(v) => *v,
            other => panic!("unexpected non-integer group key: {other:?}"),
        })
        .collect();
    let mut sorted = non_null_keys.clone();
    sorted.sort();
    assert_eq!(non_null_keys, sorted, "non-NULL group keys must be ascending: {non_null_keys:?}");
}

/// NULL group key ordering, DESC (#6014). Mirror of the ASC test: both paths must
/// place a NULL group key LAST for `ORDER BY k DESC`, matching SQLite (NULL is the
/// smallest value, so it sorts last under DESC). Prior to the #6014 fix the row
/// path put NULL first on DESC (direction-inverted default placement).
#[test]
fn group_by_order_by_null_keys_last_descending() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    setup_null_keyed(&mut db);

    // Row/columnar parity in emitted order (both must match SQLite semantics).
    let cols =
        columnar_matches_row_ordered(&db, "SELECT k, SUM(v) FROM n GROUP BY k ORDER BY k DESC");

    // SQLite-correct order: NULL group key sorts last descending.
    assert_eq!(
        cols.last().map(|r| &r.values[0]),
        Some(&vibesql_types::SqlValue::Null),
        "expected NULL group key to sort last descending (SQLite semantics); got {cols:?}"
    );
    // The leading group keys (before the trailing NULL) must be in descending order.
    let non_null_keys: Vec<i64> = cols
        .iter()
        .take(cols.len().saturating_sub(1))
        .map(|r| match &r.values[0] {
            vibesql_types::SqlValue::Integer(v) => *v,
            other => panic!("unexpected non-integer group key: {other:?}"),
        })
        .collect();
    let mut sorted = non_null_keys.clone();
    sorted.sort_by(|a, b| b.cmp(a));
    assert_eq!(non_null_keys, sorted, "non-NULL group keys must be descending: {non_null_keys:?}");
}

#[test]
fn group_by_order_by_text_affinity_key() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE tx (k TEXT, v INTEGER)");
    let keys = ["banana", "apple", "cherry", "Apple", "10", "2"];
    let mut ins = String::new();
    for i in 0..600i64 {
        let k = keys[(i as usize) % keys.len()];
        ins.push_str(&format!("INSERT INTO tx VALUES ('{k}', {});", (i % 9) + 1));
    }
    execute_sql(&mut db, &ins);

    // TEXT-affinity sort key: emitted order must match the row path exactly.
    columnar_matches_row_ordered(&db, "SELECT k, SUM(v) FROM tx GROUP BY k ORDER BY k");
}

/// An ORDER BY term that does NOT reference an output column (a group key not in
/// the SELECT list) must decline to the row path — it is not resolvable
/// positionally. We still expect a correct result via row fallback.
#[test]
fn group_by_order_by_unresolvable_term_falls_back_and_is_correct() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger(Level::Debug);
    let mut db = Database::new();
    setup(&mut db, 600);
    let _ = take_logs();

    // ORDER BY b, but b is not in the SELECT list (only a, SUM(v)). The columnar
    // path can't resolve `b` to an output column, so it declines. The result is
    // still produced (by the row path) and must be correct.
    let sql = "SELECT a, SUM(v) FROM t GROUP BY a, b ORDER BY b";
    let result = run_select(&db, sql);

    std::env::set_var("VIBESQL_DISABLE_COLUMNAR", "1");
    let row = run_select(&db, sql);
    std::env::remove_var("VIBESQL_DISABLE_COLUMNAR");

    assert_rows_eq_in_order(&result, &row, sql);
}
