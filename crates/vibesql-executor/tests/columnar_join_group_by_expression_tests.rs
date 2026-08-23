//! Path-assertion + parity tests for issue #5995 (residual scope): columnar
//! GROUP BY on an *expression* key over a *joined* batch
//! (`execute_columnar_join_group_by`), instead of falling back to row-oriented
//! execution.
//!
//! The single-table columnar GROUP BY expression-key path landed in PR #6001.
//! The joined-batch GROUP BY site was left ColumnRef-only: any
//! `GROUP BY <expr>` over a join emitted the debug marker
//! *"Columnar join: some GROUP BY columns couldn't be resolved"* and threw the
//! whole query back to the row path. This change materializes the grouping
//! expression as a derived key column on the joined batch (reusing the same
//! shared #5994 `extract_derived_expr` / `materialize_derived_column` helpers
//! the single-table path uses) and feeds it into the existing group-key
//! machinery.
//!
//! Per the acceptance criteria, wall-clock timing is NOT used (the machine is
//! loaded). Instead we capture `log` output and assert:
//!   - the row-fallback marker *"Columnar join: some GROUP BY columns couldn't be resolved"* must
//!     NOT appear;
//!   - the positive info marker *"Columnar join execution succeeded"* proves the columnar join path
//!     produced the result.
//!
//! Parity is checked by comparing the columnar-join result against the row path
//! (`VIBESQL_DISABLE_COLUMNAR_JOIN=1`) for the same query, including NULL keys
//! (both intrinsic NULLs and outer-join NULL-padded columns), integer-overflow
//! keys (which coerce to Double on the row-path arithmetic evaluator), the empty
//! join result, permuted SELECT lists (must decline to the row path), and both
//! sides of the SIMD row threshold.
//!
//! This test installs a process-global `log` logger, so it lives in its own
//! test binary to avoid clashing with other integration tests.

use log::Level;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

mod common;
use common::{execute_sql, init_logger, run_select_values as run_select, take_logs, SERIAL};

/// Run the same query on both the columnar-join path and the row path
/// (`VIBESQL_DISABLE_COLUMNAR_JOIN=1`) and return `(columnar_rows, row_rows, logs)`.
///
/// The columnar-join result is sorted by the full row so parity comparison is
/// order-independent (the columnar join path sorts group results by group key,
/// but we normalize both sides to be safe against any residual ordering skew).
fn run_both(db: &Database, sql: &str) -> (Vec<Vec<SqlValue>>, Vec<Vec<SqlValue>>, Vec<String>) {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());

    let _ = take_logs(); // discard prior logs
    let mut columnar = run_select(db, sql);
    let logs = take_logs();

    std::env::set_var("VIBESQL_DISABLE_COLUMNAR_JOIN", "1");
    let mut row = run_select(db, sql);
    std::env::remove_var("VIBESQL_DISABLE_COLUMNAR_JOIN");

    columnar.sort();
    row.sort();
    (columnar, row, logs)
}

/// Like `run_both`, but does NOT sort either result set — the columnar and row
/// results are compared in their emitted order. This is the only way to catch
/// ORDER BY / row-order bugs (a sorting harness masks every ordering defect).
/// Used for HAVING (result must match the row path unfiltered-vs-filtered) and
/// ORDER BY (emitted order must match) fallback tests (#5995 review).
fn run_both_unsorted(
    db: &Database,
    sql: &str,
) -> (Vec<Vec<SqlValue>>, Vec<Vec<SqlValue>>, Vec<String>) {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());

    let _ = take_logs(); // discard prior logs
    let columnar = run_select(db, sql);
    let logs = take_logs();

    std::env::set_var("VIBESQL_DISABLE_COLUMNAR_JOIN", "1");
    let row = run_select(db, sql);
    std::env::remove_var("VIBESQL_DISABLE_COLUMNAR_JOIN");

    (columnar, row, logs)
}

/// Row-fallback marker: a joined GROUP BY key couldn't be resolved to a column.
const ROW_FALLBACK: &str = "Columnar join: some GROUP BY columns couldn't be resolved";
/// Positive marker emitted only when the columnar join path produced the result.
const COLUMNAR_JOIN_SUCCEEDED: &str = "Columnar join execution succeeded";

fn assert_columnar_join_group_by_ran(logs: &[String]) {
    let joined = logs.join("\n");
    assert!(
        !joined.contains(ROW_FALLBACK),
        "row-fallback line emitted for a joined GROUP BY expression key; columnar path skipped:\n{joined}"
    );
    assert!(
        logs.iter().any(|l| l.contains(COLUMNAR_JOIN_SUCCEEDED)),
        "expected columnar join GROUP BY to run:\n{joined}"
    );
}

/// Two tables joined on a shared key. `l(k, a, b, c)` is the "fact" table;
/// `r(k, d)` is the "dim" table. Populate `n` fact rows and a small dim table.
fn setup_inner(db: &mut Database, n: i64) {
    execute_sql(db, "CREATE TABLE l (k INTEGER, a INTEGER, b INTEGER, c INTEGER)");
    execute_sql(db, "CREATE TABLE r (k INTEGER, d INTEGER)");
    let mut ins = String::new();
    for i in 0..n {
        let k = i % 4; // join key: 4 distinct values (all present in r)
        let a = i % 7;
        let b = i % 5;
        let c = i;
        ins.push_str(&format!("INSERT INTO l VALUES ({k}, {a}, {b}, {c});"));
    }
    for k in 0..4 {
        ins.push_str(&format!("INSERT INTO r VALUES ({k}, {});", k * 10));
    }
    execute_sql(db, &ins);
}

/// `GROUP BY a + b` over an INNER JOIN takes the columnar join path and matches
/// the row path exactly. Runs both sides of the SIMD row threshold.
#[test]
fn join_group_by_sum_expression_takes_columnar_path_and_matches_row_path() {
    init_logger(Level::Debug);

    for n in [50i64, 1000i64] {
        let mut db = Database::new();
        setup_inner(&mut db, n);

        let sql = "SELECT l.a + l.b, SUM(l.c) FROM l JOIN r ON l.k = r.k GROUP BY l.a + l.b";
        let (columnar, row, logs) = run_both(&db, sql);

        assert_columnar_join_group_by_ran(&logs);
        assert_eq!(columnar, row, "columnar join GROUP BY a+b result must equal row path (n={n})");
    }
}

/// `GROUP BY a * b` with COUNT(*) over an INNER JOIN — a different arithmetic op
/// — also runs columnar and matches the row path.
#[test]
fn join_group_by_product_count_matches_row_path() {
    init_logger(Level::Debug);

    let mut db = Database::new();
    setup_inner(&mut db, 800);

    let sql = "SELECT l.a * l.b, COUNT(*) FROM l JOIN r ON l.k = r.k GROUP BY l.a * l.b";
    let (columnar, row, logs) = run_both(&db, sql);

    assert_columnar_join_group_by_ran(&logs);
    assert_eq!(columnar, row, "columnar join GROUP BY a*b result must equal row path");
}

/// A grouping expression that spans BOTH joined tables (`l.a + r.d`) resolves
/// its column indices against the combined joined-batch schema and groups
/// correctly. This exercises the qualified-name -> joined-batch-index resolution.
#[test]
fn join_group_by_cross_table_expression_matches_row_path() {
    init_logger(Level::Debug);

    let mut db = Database::new();
    setup_inner(&mut db, 600);

    let sql = "SELECT l.a + r.d, SUM(l.c) FROM l JOIN r ON l.k = r.k GROUP BY l.a + r.d";
    let (columnar, row, logs) = run_both(&db, sql);

    assert_columnar_join_group_by_ran(&logs);
    assert_eq!(
        columnar, row,
        "columnar join GROUP BY over a cross-table expression must equal row path"
    );
}

/// LEFT OUTER JOIN NULL keys: unmatched left rows get a NULL-padded `r.d`, so
/// `l.a + r.d` is NULL for those rows. All NULL derived keys must group as one
/// group, identically to the row path. This is the critical outer-join parity
/// case from the acceptance criteria.
#[test]
fn join_group_by_left_outer_null_keys_match_row_path() {
    init_logger(Level::Debug);

    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE l (k INTEGER, a INTEGER, c INTEGER)");
    execute_sql(&mut db, "CREATE TABLE r (k INTEGER, d INTEGER)");
    let mut ins = String::new();
    // Left keys 0..6; right only has keys 0,1,2 -> keys 3,4,5 are unmatched and
    // get NULL-padded r.d, making l.a + r.d NULL.
    for i in 0..500i64 {
        let k = i % 6;
        let a = i % 5;
        ins.push_str(&format!("INSERT INTO l VALUES ({k}, {a}, {i});"));
    }
    for k in 0..3 {
        ins.push_str(&format!("INSERT INTO r VALUES ({k}, {});", k + 1));
    }
    execute_sql(&mut db, &ins);

    let sql = "SELECT l.a + r.d, SUM(l.c) FROM l LEFT JOIN r ON l.k = r.k GROUP BY l.a + r.d";
    let (columnar, row, logs) = run_both(&db, sql);

    assert_columnar_join_group_by_ran(&logs);
    assert_eq!(
        columnar, row,
        "LEFT OUTER JOIN NULL-padded derived keys must group as one group, matching row path"
    );
}

/// Integer-overflow keys over a join: `a + b` where the sum overflows i64
/// coerces to Double on the row-path arithmetic evaluator. The columnar join
/// path reuses the same row-path arithmetic (`materialize_derived_column`), so
/// grouping is identical by construction. This asserts that parity holds.
#[test]
fn join_group_by_overflow_keys_match_row_path() {
    init_logger(Level::Debug);

    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE l (k INTEGER, a BIGINT, b BIGINT, c INTEGER)");
    execute_sql(&mut db, "CREATE TABLE r (k INTEGER, d INTEGER)");
    let ins = "\
        INSERT INTO l VALUES (0, 1, 2, 10);\
        INSERT INTO l VALUES (0, 2, 1, 20);\
        INSERT INTO l VALUES (1, 9223372036854775807, 1, 30);\
        INSERT INTO l VALUES (1, 9223372036854775806, 2, 40);\
        INSERT INTO l VALUES (0, 1, 2, 50);\
        INSERT INTO r VALUES (0, 100);\
        INSERT INTO r VALUES (1, 200);";
    execute_sql(&mut db, ins);

    let sql = "SELECT l.a + l.b, SUM(l.c) FROM l JOIN r ON l.k = r.k GROUP BY l.a + l.b";
    let (columnar, row, _logs) = run_both(&db, sql);

    assert_eq!(
        columnar, row,
        "columnar join GROUP BY with overflow (Integer/Double) keys must match row path"
    );
}

/// Empty join result: an INNER JOIN whose keys never match produces no rows, so
/// GROUP BY on an expression key returns no groups, same as the row path.
#[test]
fn join_group_by_empty_result_matches_row_path() {
    init_logger(Level::Debug);

    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE l (k INTEGER, a INTEGER, b INTEGER, c INTEGER)");
    execute_sql(&mut db, "CREATE TABLE r (k INTEGER, d INTEGER)");
    // Disjoint keys -> no matches.
    execute_sql(&mut db, "INSERT INTO l VALUES (1, 1, 2, 10); INSERT INTO r VALUES (99, 0);");

    let sql = "SELECT l.a + l.b, SUM(l.c) FROM l JOIN r ON l.k = r.k GROUP BY l.a + l.b";
    let (columnar, row, _logs) = run_both(&db, sql);

    assert!(columnar.is_empty(), "empty join must produce no groups");
    assert_eq!(columnar, row, "empty-join result must match row path");
}

/// Multi-part: mixing a bare-column key and an expression key in one GROUP BY
/// over a join (`GROUP BY l.a, l.b + l.c`) still runs columnar and matches the
/// row path.
#[test]
fn join_group_by_mixed_column_and_expression_keys_match_row_path() {
    init_logger(Level::Debug);

    let mut db = Database::new();
    setup_inner(&mut db, 600);

    let sql = "SELECT l.a, l.b + l.c, COUNT(*) FROM l JOIN r ON l.k = r.k GROUP BY l.a, l.b + l.c";
    let (columnar, row, logs) = run_both(&db, sql);

    assert_columnar_join_group_by_ran(&logs);
    assert_eq!(columnar, row, "columnar join GROUP BY (col, expr) result must equal row path");
}

// --- Positional SELECT-list validation (the transposition hazard) ------------

/// Debug marker emitted when the SELECT list permutes the GROUP BY key order,
/// forcing a positional-mismatch decline to the row path. The join GROUP BY path
/// emits key columns in GROUP BY order with no re-projection, so a permuted
/// SELECT list would transpose the output columns.
const POSITIONAL_DECLINE: &str = "does not positionally match GROUP BY key";

fn assert_row_path_via_positional_decline(logs: &[String]) {
    let joined = logs.join("\n");
    assert!(
        logs.iter().any(|l| l.contains(POSITIONAL_DECLINE)),
        "expected columnar join to decline (positional mismatch) and fall back to the row path:\n{joined}"
    );
    assert!(
        !logs.iter().any(|l| l.contains(COLUMNAR_JOIN_SUCCEEDED)),
        "columnar join GROUP BY must NOT run for a permuted SELECT list (would transpose columns):\n{joined}"
    );
}

/// REGRESSION: permuted EXPRESSION keys over a join.
/// `SELECT l.a*l.b, l.a+l.b, COUNT(*) FROM l JOIN r ... GROUP BY l.a+l.b, l.a*l.b`
/// — the SELECT-list key order is the reverse of the GROUP BY key order. Without
/// the positional check, the join path would emit key columns in GROUP BY order
/// (`a+b, a*b`) with no re-projection, transposing columns 0 and 1 vs the row
/// path (`a*b, a+b`). The positional check declines to the row path.
#[test]
fn join_group_by_permuted_expression_keys_match_row_path() {
    init_logger(Level::Debug);

    let mut db = Database::new();
    setup_inner(&mut db, 1000);

    let sql = "SELECT l.a * l.b, l.a + l.b, COUNT(*) FROM l JOIN r ON l.k = r.k \
               GROUP BY l.a + l.b, l.a * l.b";
    let (columnar, row, logs) = run_both(&db, sql);

    assert_row_path_via_positional_decline(&logs);
    assert_eq!(
        columnar, row,
        "permuted expression-key SELECT list over a join must match row path (no transposed columns)"
    );
}

/// REGRESSION (drive-by): permuted BARE-COLUMN keys over a join.
/// `SELECT l.b, l.a, COUNT(*) FROM l JOIN r ... GROUP BY l.a, l.b` — same
/// transposition hazard with bare columns. Before this change the join path had
/// NO SELECT-list validation at all, so this was silently transposed on the
/// columnar path. The positional check now declines it to the row path.
#[test]
fn join_group_by_permuted_bare_column_keys_match_row_path() {
    init_logger(Level::Debug);

    let mut db = Database::new();
    setup_inner(&mut db, 1000);

    let sql = "SELECT l.b, l.a, COUNT(*) FROM l JOIN r ON l.k = r.k GROUP BY l.a, l.b";
    let (columnar, row, logs) = run_both(&db, sql);

    assert_row_path_via_positional_decline(&logs);
    assert_eq!(
        columnar, row,
        "permuted bare-column SELECT list over a join must match row path (no transposed columns)"
    );
}

/// The MATCHING-ORDER multi-key case over a join must STILL take the columnar
/// fast path (the positional-check must not over-decline aligned queries).
#[test]
fn join_group_by_aligned_multi_expression_keys_stay_columnar() {
    init_logger(Level::Debug);

    let mut db = Database::new();
    setup_inner(&mut db, 1000);

    let sql = "SELECT l.a + l.b, l.a * l.b, COUNT(*) FROM l JOIN r ON l.k = r.k \
               GROUP BY l.a + l.b, l.a * l.b";
    let (columnar, row, logs) = run_both(&db, sql);

    assert_columnar_join_group_by_ran(&logs);
    assert_eq!(
        columnar, row,
        "aligned multi-expression-key join GROUP BY must stay columnar and match row path"
    );
}

/// Unsupported grouping-key shape over a join (a scalar function) must still
/// fall back to the row path (no silent wrong answer).
#[test]
fn join_group_by_unsupported_expression_falls_back() {
    init_logger(Level::Debug);

    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE l (k INTEGER, a INTEGER, b TEXT, c INTEGER)");
    execute_sql(&mut db, "CREATE TABLE r (k INTEGER, d INTEGER)");
    let mut ins = String::new();
    for i in 0..300i64 {
        ins.push_str(&format!("INSERT INTO l VALUES ({}, {}, 'k{}', {});", i % 3, i % 5, i % 3, i));
    }
    for k in 0..3 {
        ins.push_str(&format!("INSERT INTO r VALUES ({k}, {});", k * 10));
    }
    execute_sql(&mut db, &ins);

    // substr(...) is not a DerivedExpr arithmetic shape -> must decline.
    let sql = "SELECT substr(l.b, 1, 1), COUNT(*) FROM l JOIN r ON l.k = r.k \
               GROUP BY substr(l.b, 1, 1)";
    let (columnar, row, _logs) = run_both(&db, sql);

    assert_eq!(
        columnar, row,
        "unsupported join GROUP BY expression must still produce correct (row-path) results"
    );
}

// --- HAVING / ORDER BY on the columnar join GROUP BY path (Issue #6009) --------
//
// The join GROUP BY branch now applies HAVING (via the shared columnar HAVING
// filter) and a terminal ORDER BY (via the shared positional resolver), instead
// of declining to the row path (which is what PR #6003 did). These tests assert
// the columnar join path IS taken (positive marker, no positional/resolve
// decline) AND compare results against the row path WITHOUT sorting (so ordering
// bugs cannot hide — the pre-sorting harness that masked the #6003 ordering bug
// must not return).
//
// A HAVING clause that references a *bare* (non-aggregate) GROUP BY column is not
// supported by the columnar HAVING filter and correctly declines to the row path;
// that case is covered separately below.

/// HAVING with an aggregate predicate on a bare-column join GROUP BY. The
/// columnar HAVING filter handles aggregate predicates, so this now runs on the
/// columnar join path (Issue #6009). a=7 appears exactly once, so
/// HAVING COUNT(*) > 1 must exclude it. Compare WITHOUT sorting.
#[test]
fn join_group_by_having_aggregate_runs_columnar_and_matches_row_path() {
    init_logger(Level::Debug);

    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE l (k INTEGER, a INTEGER, c INTEGER)");
    execute_sql(&mut db, "CREATE TABLE r (k INTEGER, d INTEGER)");
    execute_sql(
        &mut db,
        "INSERT INTO l VALUES (0,3,10),(1,3,20),(2,5,30),(3,5,40),\
         (0,3,50),(1,7,60),(2,3,70),(3,5,80)",
    );
    execute_sql(&mut db, "INSERT INTO r VALUES (0,100),(1,200),(2,300),(3,400)");

    // a=7 appears exactly once, so HAVING COUNT(*) > 1 must exclude it.
    let sql = "SELECT l.a, COUNT(*) FROM l JOIN r ON l.k = r.k GROUP BY l.a HAVING COUNT(*) > 1";
    let (columnar, row, logs) = run_both_unsorted(&db, sql);

    assert_columnar_join_group_by_ran(&logs);
    assert_eq!(columnar, row, "HAVING on a join GROUP BY must match the row path (HAVING applied)");
    // Guard: the singleton group must actually be filtered out.
    assert!(
        !columnar.iter().any(|r| r[0] == SqlValue::Integer(7)),
        "HAVING COUNT(*) > 1 must exclude the a=7 singleton group: {columnar:?}"
    );
}

/// HAVING on an *expression*-key join GROUP BY — aggregate predicate runs on the
/// columnar path and matches the row path.
#[test]
fn join_group_by_having_expression_key_runs_columnar_and_matches_row_path() {
    init_logger(Level::Debug);

    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE l (k INTEGER, a INTEGER, c INTEGER)");
    execute_sql(&mut db, "CREATE TABLE r (k INTEGER, d INTEGER)");
    execute_sql(
        &mut db,
        "INSERT INTO l VALUES (0,3,10),(1,3,20),(2,5,30),(3,5,40),\
         (0,3,50),(1,7,60),(2,3,70),(3,5,80)",
    );
    execute_sql(&mut db, "INSERT INTO r VALUES (0,100),(1,200),(2,300),(3,400)");

    let sql = "SELECT l.a + l.k, SUM(l.c) FROM l JOIN r ON l.k = r.k \
               GROUP BY l.a + l.k HAVING SUM(l.c) > 60";
    let (columnar, row, logs) = run_both_unsorted(&db, sql);

    assert_columnar_join_group_by_ran(&logs);
    assert_eq!(columnar, row, "HAVING on an expression-key join GROUP BY must match the row path");
}

/// HAVING referencing a *bare* GROUP BY column (not an aggregate) is unsupported
/// by the columnar HAVING filter and must decline to the row path (which handles
/// it). Result must still be correct. Compare WITHOUT sorting.
#[test]
fn join_group_by_having_bare_column_declines_and_matches_row_path() {
    init_logger(Level::Debug);

    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE l (k INTEGER, a INTEGER, c INTEGER)");
    execute_sql(&mut db, "CREATE TABLE r (k INTEGER, d INTEGER)");
    execute_sql(
        &mut db,
        "INSERT INTO l VALUES (0,3,10),(1,3,20),(2,5,30),(3,5,40),\
         (0,3,50),(1,7,60),(2,3,70),(3,5,80)",
    );
    execute_sql(&mut db, "INSERT INTO r VALUES (0,100),(1,200),(2,300),(3,400)");

    // HAVING l.a > 3 references a bare GROUP BY column -> columnar HAVING filter
    // declines -> row path. Result must match and be correct.
    let sql = "SELECT l.a, COUNT(*) FROM l JOIN r ON l.k = r.k GROUP BY l.a HAVING l.a > 3";
    let (columnar, row, _logs) = run_both_unsorted(&db, sql);

    assert_eq!(
        columnar, row,
        "HAVING on a bare GROUP BY column must produce correct (row-path) results"
    );
    // Guard: only groups with a > 3 survive.
    assert!(
        columnar.iter().all(|r| matches!(r[0], SqlValue::Integer(v) if v > 3)),
        "HAVING l.a > 3 must keep only a>3 groups: {columnar:?}"
    );
}

/// ORDER BY on an expression-key join GROUP BY — the #6003 repro. Runs on the
/// columnar path now and matches the row path's sorted order in EMITTED ORDER
/// (no sort by the harness).
#[test]
fn join_group_by_order_by_expression_key_runs_columnar_and_matches_row_path() {
    init_logger(Level::Debug);

    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE l (k INTEGER, a INTEGER, c INTEGER)");
    execute_sql(&mut db, "CREATE TABLE r (k INTEGER, d INTEGER)");
    execute_sql(
        &mut db,
        "INSERT INTO l VALUES (0,3,10),(1,3,20),(2,5,30),(3,5,40),\
         (0,3,50),(1,7,60),(2,3,70),(3,5,80)",
    );
    execute_sql(&mut db, "INSERT INTO r VALUES (0,100),(1,200),(2,300),(3,400)");

    let sql = "SELECT l.a + l.k, SUM(l.c) FROM l JOIN r ON l.k = r.k \
               GROUP BY l.a + l.k ORDER BY l.a + l.k";
    let (columnar, row, logs) = run_both_unsorted(&db, sql);

    assert_columnar_join_group_by_ran(&logs);
    assert_eq!(
        columnar, row,
        "ORDER BY on an expression-key join GROUP BY must match the row path IN EMITTED ORDER"
    );
    // The keys must be ascending (3,4,5,7,8), not raw group order (4,7,3,5,8).
    let keys: Vec<SqlValue> = columnar.iter().map(|r| r[0].clone()).collect();
    assert_eq!(
        keys,
        vec![
            SqlValue::Integer(3),
            SqlValue::Integer(4),
            SqlValue::Integer(5),
            SqlValue::Integer(7),
            SqlValue::Integer(8),
        ],
        "keys must be emitted in ascending ORDER BY order: {keys:?}"
    );
}

/// ORDER BY on a *bare-column* join GROUP BY — the second #6003 repro. Runs
/// columnar and matches emitted order.
#[test]
fn join_group_by_order_by_bare_column_runs_columnar_and_matches_row_path() {
    init_logger(Level::Debug);

    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE l (k INTEGER, a INTEGER, c INTEGER)");
    execute_sql(&mut db, "CREATE TABLE r (k INTEGER, d INTEGER)");
    execute_sql(
        &mut db,
        "INSERT INTO l VALUES (0,3,10),(1,3,20),(2,5,30),(3,5,40),\
         (0,3,50),(1,7,60),(2,3,70),(3,5,80)",
    );
    execute_sql(&mut db, "INSERT INTO r VALUES (0,100),(1,200),(2,300),(3,400)");

    let sql = "SELECT l.a, SUM(l.c) FROM l JOIN r ON l.k = r.k GROUP BY l.a ORDER BY l.a";
    let (columnar, row, logs) = run_both_unsorted(&db, sql);

    assert_columnar_join_group_by_ran(&logs);
    assert_eq!(
        columnar, row,
        "ORDER BY on a bare-column join GROUP BY must match the row path IN EMITTED ORDER"
    );
    let keys: Vec<SqlValue> = columnar.iter().map(|r| r[0].clone()).collect();
    assert_eq!(
        keys,
        vec![SqlValue::Integer(3), SqlValue::Integer(5), SqlValue::Integer(7)],
        "bare-column keys must be emitted ascending: {keys:?}"
    );
}

/// HAVING then ORDER BY together on a join GROUP BY: HAVING filters, then ORDER BY
/// sorts the survivors, all on the columnar path.
#[test]
fn join_group_by_having_then_order_by_runs_columnar_and_matches_row_path() {
    init_logger(Level::Debug);

    let mut db = Database::new();
    setup_inner(&mut db, 800);

    let sql = "SELECT l.a, SUM(l.c) FROM l JOIN r ON l.k = r.k \
               GROUP BY l.a HAVING SUM(l.c) > 100 ORDER BY SUM(l.c) DESC";
    let (columnar, row, logs) = run_both_unsorted(&db, sql);

    assert_columnar_join_group_by_ran(&logs);
    assert_eq!(
        columnar, row,
        "HAVING + ORDER BY on a join GROUP BY must match the row path IN EMITTED ORDER"
    );
}

/// LEFT JOIN with NULL-padded derived keys plus ORDER BY: the #6003 repro. The
/// NULL-key group and ORDER BY together now run on the columnar path and match
/// the row path in emitted order — NULL keys sort first ascending (SQLite).
#[test]
fn join_group_by_left_outer_null_keys_with_order_by_runs_columnar_and_matches_row_path() {
    init_logger(Level::Debug);

    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE l (k INTEGER, a INTEGER, c INTEGER)");
    execute_sql(&mut db, "CREATE TABLE r (k INTEGER, d INTEGER)");
    let mut ins = String::new();
    for i in 0..120i64 {
        let k = i % 6; // right has keys 0,1,2 -> 3,4,5 unmatched (NULL r.d)
        let a = i % 5;
        ins.push_str(&format!("INSERT INTO l VALUES ({k}, {a}, {i});"));
    }
    for k in 0..3 {
        ins.push_str(&format!("INSERT INTO r VALUES ({k}, {});", k + 1));
    }
    execute_sql(&mut db, &ins);

    let sql = "SELECT l.a + r.d, SUM(l.c) FROM l LEFT JOIN r ON l.k = r.k \
               GROUP BY l.a + r.d ORDER BY l.a + r.d";
    let (columnar, row, logs) = run_both_unsorted(&db, sql);

    assert_columnar_join_group_by_ran(&logs);
    assert_eq!(
        columnar, row,
        "LEFT JOIN NULL-key GROUP BY with ORDER BY must match the row path in emitted order"
    );
    // The NULL-key group (unmatched LEFT rows -> l.a + NULL = NULL) must sort
    // first ascending.
    assert_eq!(
        columnar.first().map(|r| &r[0]),
        Some(&SqlValue::Null),
        "NULL derived group key must sort first ascending: {columnar:?}"
    );
}
