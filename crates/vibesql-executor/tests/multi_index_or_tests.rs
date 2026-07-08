//! Correctness tests for MULTI-INDEX OR single-table execution (epic #5668, PR 2).
//!
//! These tests exercise the flag-gated MULTI-INDEX OR execution path wired into
//! the single-table scan dispatch. The dominant risk for this PR is **rowid
//! deduplication correctness**: a row satisfying multiple OR branches must appear
//! exactly once. See #5668 §2b/§3.
//!
//! Trigger reminder: MULTI-INDEX OR is selected only when (a) the
//! `MULTI_INDEX_OR_DISABLED` flag is unset (default ON), (b) no single index
//! (regular or skip-scan) applies to the WHERE clause, and (c) every top-level OR
//! branch is independently indexable. These tests build schemas where each OR
//! branch column has its own single-column index so the analyzer resolves every
//! branch while no single index covers the whole OR.

use std::sync::Mutex;

use vibesql_executor::SelectExecutor;
use vibesql_types::SqlValue;

// `std::env::set_var` is process-global; serialize the flag-toggling test so it
// cannot race with the default-ON tests.
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn run_stmt(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt =
        vibesql_parser::Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql}: {e:?}"));
    match stmt {
        vibesql_ast::Statement::CreateTable(c) => {
            vibesql_executor::CreateTableExecutor::execute(&c, db).unwrap();
        }
        vibesql_ast::Statement::CreateIndex(c) => {
            vibesql_executor::CreateIndexExecutor::execute(&c, db).unwrap();
        }
        vibesql_ast::Statement::Insert(i) => {
            vibesql_executor::InsertExecutor::execute(db, &i).unwrap();
        }
        other => panic!("Unsupported statement in setup: {other:?}"),
    }
}

/// Run a SELECT returning a single integer column as a Vec<i64>, in the order
/// the executor emits rows (no sorting applied here).
fn query_ints_raw(db: &vibesql_storage::Database, sql: &str) -> Vec<i64> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(select) = stmt else { panic!("not a SELECT: {sql}") };
    SelectExecutor::new(db)
        .execute(&select)
        .unwrap_or_else(|e| panic!("query failed {sql}: {e:?}"))
        .into_iter()
        .map(|row| match &row.values[0] {
            SqlValue::Integer(n) => *n,
            SqlValue::Bigint(n) => *n,
            other => panic!("expected integer, got {other:?}"),
        })
        .collect()
}

/// Run a SELECT and return the single-int column **sorted ascending**.
///
/// NOTE: a SQL `ORDER BY` would bypass the MULTI-INDEX OR conservative trigger
/// (ORDER BY is deferred to the single-index path), so these tests must NOT use
/// `ORDER BY` in SQL — they sort in-test for deterministic set comparison.
fn query_ints_sorted(db: &vibesql_storage::Database, sql: &str) -> Vec<i64> {
    let mut v = query_ints_raw(db, sql);
    v.sort_unstable();
    v
}

/// A table `t(a INTEGER PRIMARY KEY, b INT, c INT, d INT)` with single-column
/// indexes on `c` and `d`. No composite/index covers an OR over `c` and `d`, so
/// `WHERE c=? OR d=?` routes through MULTI-INDEX OR.
fn setup_cd_table(db: &mut vibesql_storage::Database) {
    run_stmt(db, "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER, c INTEGER, d INTEGER)");
    run_stmt(db, "CREATE INDEX t_c ON t(c)");
    run_stmt(db, "CREATE INDEX t_d ON t(d)");
}

#[test]
fn rows_matching_both_branches_appear_exactly_once() {
    let _g = ENV_LOCK.lock().unwrap();
    let mut db = vibesql_storage::Database::new();
    setup_cd_table(&mut db);
    // Row a=1 satisfies BOTH branches (c=10 AND d=10). It must appear once.
    run_stmt(&mut db, "INSERT INTO t VALUES (1, 0, 10, 10)");
    run_stmt(&mut db, "INSERT INTO t VALUES (2, 0, 10, 99)"); // c only
    run_stmt(&mut db, "INSERT INTO t VALUES (3, 0, 99, 10)"); // d only
    run_stmt(&mut db, "INSERT INTO t VALUES (4, 0, 99, 99)"); // neither

    let rows = query_ints_sorted(&db, "SELECT a FROM t WHERE c = 10 OR d = 10");
    assert_eq!(rows, vec![1, 2, 3], "row 1 (both branches) must appear exactly once");
}

#[test]
fn is_null_branch_returns_correct_rows_distinct_from_equality() {
    let _g = ENV_LOCK.lock().unwrap();
    let mut db = vibesql_storage::Database::new();
    setup_cd_table(&mut db);
    run_stmt(&mut db, "INSERT INTO t VALUES (1, 0, 10, NULL)"); // c matches, d NULL
    run_stmt(&mut db, "INSERT INTO t VALUES (2, 0, 99, NULL)"); // d IS NULL only
    run_stmt(&mut db, "INSERT INTO t VALUES (3, 0, 99, 5)"); // neither
    run_stmt(&mut db, "INSERT INTO t VALUES (4, 0, 10, 5)"); // c matches only

    // `d IS NULL` is a NULL-key match, NOT `d = NULL` (which matches nothing).
    let rows = query_ints_sorted(&db, "SELECT a FROM t WHERE c = 10 OR d IS NULL");
    assert_eq!(rows, vec![1, 2, 4]);

    // Sanity: `d = NULL` (equality with NULL) matches nothing — the IS NULL
    // branch is genuinely distinct from an equality seek.
    let none = query_ints_sorted(&db, "SELECT a FROM t WHERE d = NULL");
    assert!(none.is_empty(), "d = NULL must match no rows");
}

#[test]
fn empty_branch_results_are_handled() {
    let _g = ENV_LOCK.lock().unwrap();
    let mut db = vibesql_storage::Database::new();
    setup_cd_table(&mut db);
    run_stmt(&mut db, "INSERT INTO t VALUES (1, 0, 10, 1)");
    run_stmt(&mut db, "INSERT INTO t VALUES (2, 0, 20, 2)");

    // One branch matches nothing (c = 777), the other matches one row.
    let rows = query_ints_sorted(&db, "SELECT a FROM t WHERE c = 777 OR d = 2");
    assert_eq!(rows, vec![2]);

    // Both branches empty.
    let none = query_ints_sorted(&db, "SELECT a FROM t WHERE c = 777 OR d = 888");
    assert!(none.is_empty());
}

#[test]
fn null_keys_do_not_spuriously_match_equality_branches() {
    let _g = ENV_LOCK.lock().unwrap();
    let mut db = vibesql_storage::Database::new();
    setup_cd_table(&mut db);
    // Rows with NULL in the indexed columns must not match `c = ?` / `d = ?`.
    run_stmt(&mut db, "INSERT INTO t VALUES (1, 0, NULL, NULL)");
    run_stmt(&mut db, "INSERT INTO t VALUES (2, 0, 10, NULL)");
    run_stmt(&mut db, "INSERT INTO t VALUES (3, 0, NULL, 20)");

    let rows = query_ints_sorted(&db, "SELECT a FROM t WHERE c = 10 OR d = 20");
    assert_eq!(rows, vec![2, 3], "NULL-key row 1 must not match either equality branch");
}

#[test]
fn residual_filter_applied_around_union() {
    let _g = ENV_LOCK.lock().unwrap();
    let mut db = vibesql_storage::Database::new();
    setup_cd_table(&mut db);
    // (c = 10 OR d = 20) AND b > 1000
    run_stmt(&mut db, "INSERT INTO t VALUES (1, 5000, 10, 0)"); // c match, b>1000  -> in
    run_stmt(&mut db, "INSERT INTO t VALUES (2, 5, 10, 0)"); // c match, b<=1000 -> out
    run_stmt(&mut db, "INSERT INTO t VALUES (3, 9000, 0, 20)"); // d match, b>1000  -> in
    run_stmt(&mut db, "INSERT INTO t VALUES (4, 9000, 0, 0)"); // neither branch -> out

    let rows = query_ints_sorted(&db, "SELECT a FROM t WHERE (c = 10 OR d = 20) AND b > 1000");
    assert_eq!(rows, vec![1, 3]);
}

#[test]
fn three_way_or_dedups_across_all_branches() {
    let _g = ENV_LOCK.lock().unwrap();
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(a INTEGER PRIMARY KEY, c INT, d INT, e INT)");
    run_stmt(&mut db, "CREATE INDEX t_c ON t(c)");
    run_stmt(&mut db, "CREATE INDEX t_d ON t(d)");
    run_stmt(&mut db, "CREATE INDEX t_e ON t(e)");
    // Row 1 matches all three branches; must appear once.
    run_stmt(&mut db, "INSERT INTO t VALUES (1, 1, 1, 1)");
    run_stmt(&mut db, "INSERT INTO t VALUES (2, 1, 9, 9)");
    run_stmt(&mut db, "INSERT INTO t VALUES (3, 9, 1, 9)");
    run_stmt(&mut db, "INSERT INTO t VALUES (4, 9, 9, 1)");
    run_stmt(&mut db, "INSERT INTO t VALUES (5, 9, 9, 9)");

    let rows = query_ints_sorted(&db, "SELECT a FROM t WHERE c = 1 OR d = 1 OR e = 1");
    assert_eq!(rows, vec![1, 2, 3, 4]);
}

#[test]
fn disabled_flag_reverts_to_prior_behavior() {
    let _g = ENV_LOCK.lock().unwrap();
    // With the flag set, the MULTI-INDEX OR path must not be taken; the query
    // still returns correct results via the existing full-scan + residual path.
    // SAFETY: serialized by ENV_LOCK; restored before unlock.
    unsafe {
        std::env::set_var("MULTI_INDEX_OR_DISABLED", "1");
    }

    let result = std::panic::catch_unwind(|| {
        let mut db = vibesql_storage::Database::new();
        setup_cd_table(&mut db);
        run_stmt(&mut db, "INSERT INTO t VALUES (1, 0, 10, 10)");
        run_stmt(&mut db, "INSERT INTO t VALUES (2, 0, 10, 99)");
        run_stmt(&mut db, "INSERT INTO t VALUES (3, 0, 99, 10)");
        run_stmt(&mut db, "INSERT INTO t VALUES (4, 0, 99, 99)");
        query_ints_sorted(&db, "SELECT a FROM t WHERE c = 10 OR d = 10")
    });

    unsafe {
        std::env::remove_var("MULTI_INDEX_OR_DISABLED");
    }

    let rows = result.expect("query panicked");
    // Same correct result as the enabled path — the flag only affects the
    // access path, never the row set.
    assert_eq!(rows, vec![1, 2, 3]);
}

#[test]
fn same_index_or_does_not_use_multi_index_path() {
    let _g = ENV_LOCK.lock().unwrap();
    // `c = 1 OR c = 2` resolves both branches to the SAME index (t_c). The
    // conservative trigger requires >= 2 distinct indexes, so this stays on the
    // existing single-index path — and must still be correct.
    let mut db = vibesql_storage::Database::new();
    setup_cd_table(&mut db);
    run_stmt(&mut db, "INSERT INTO t VALUES (1, 0, 1, 0)");
    run_stmt(&mut db, "INSERT INTO t VALUES (2, 0, 2, 0)");
    run_stmt(&mut db, "INSERT INTO t VALUES (3, 0, 3, 0)");

    let rows = query_ints_sorted(&db, "SELECT a FROM t WHERE c = 1 OR c = 2");
    assert_eq!(rows, vec![1, 2]);
}

// ---- Oracle / property test ------------------------------------------------
//
// For randomized data + randomized indexable-OR predicates, the MULTI-INDEX OR
// result set must be identical to a reference computed directly from the raw
// data in Rust (the authoritative "full-scan residual" baseline). This is the
// single strongest guard against a wrong rowid dedup (#5668 §3): the reference
// is computed independently of any executor scan path.

use rand::{RngExt, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// One generated row: (rowid a, b, c, d) where c/d may be NULL.
#[derive(Clone, Copy)]
struct OracleRow {
    a: i64,
    b: i64,
    c: Option<i64>,
    d: Option<i64>,
}

/// A generated indexable-OR predicate over (c, d) with an optional residual on b.
struct OraclePredicate {
    /// Branches: each is a column ('c' or 'd') and an equality value, or an
    /// IS NULL test (`None` value).
    branches: Vec<(char, Option<i64>)>,
    /// Optional residual `b > threshold`.
    residual_b_gt: Option<i64>,
}

impl OraclePredicate {
    fn to_sql(&self) -> String {
        let or_part = self
            .branches
            .iter()
            .map(|(col, val)| match val {
                Some(v) => format!("{col} = {v}"),
                None => format!("{col} IS NULL"),
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        match self.residual_b_gt {
            Some(t) => format!("SELECT a FROM t WHERE ({or_part}) AND b > {t}"),
            None => format!("SELECT a FROM t WHERE {or_part}"),
        }
    }

    /// Reference evaluation directly over the raw rows (the oracle baseline).
    fn eval_reference(&self, rows: &[OracleRow]) -> Vec<i64> {
        let mut out: Vec<i64> = rows
            .iter()
            .filter(|r| {
                let or_ok = self.branches.iter().any(|(col, val)| {
                    let cell = if *col == 'c' { r.c } else { r.d };
                    match val {
                        // Equality: NULL never matches (SQL semantics).
                        Some(v) => cell == Some(*v),
                        // IS NULL.
                        None => cell.is_none(),
                    }
                });
                let residual_ok = match self.residual_b_gt {
                    Some(t) => r.b > t,
                    None => true,
                };
                or_ok && residual_ok
            })
            .map(|r| r.a)
            .collect();
        out.sort_unstable();
        out
    }
}

#[test]
fn oracle_property_test_multi_index_or_matches_reference() {
    let _g = ENV_LOCK.lock().unwrap();
    let mut rng = ChaCha8Rng::seed_from_u64(0x5668_0002_DEAD_BEEF);

    // Run many independent trials with fresh randomized data + predicates.
    for trial in 0..200u32 {
        let mut db = vibesql_storage::Database::new();
        setup_cd_table(&mut db);

        // Generate 10..40 rows. Small value domains => frequent multi-branch
        // overlaps (the dedup stress case) and frequent NULLs.
        let n = rng.random_range(10..40);
        let mut rows = Vec::with_capacity(n);
        for i in 0..n {
            let a = (i + 1) as i64;
            let b = rng.random_range(0..2000);
            let c = if rng.random_range(0..4) == 0 { None } else { Some(rng.random_range(0..6)) };
            let d = if rng.random_range(0..4) == 0 { None } else { Some(rng.random_range(0..6)) };
            rows.push(OracleRow { a, b, c, d });
            let c_sql = c.map(|v| v.to_string()).unwrap_or_else(|| "NULL".into());
            let d_sql = d.map(|v| v.to_string()).unwrap_or_else(|| "NULL".into());
            run_stmt(&mut db, &format!("INSERT INTO t VALUES ({a}, {b}, {c_sql}, {d_sql})"));
        }

        // Build a randomized indexable-OR predicate with 2..4 branches spanning
        // both columns (so >= 2 distinct indexes => the MULTI-INDEX OR trigger
        // fires) plus an optional residual.
        let branch_count = rng.random_range(2..5);
        let mut branches = Vec::with_capacity(branch_count);
        for _ in 0..branch_count {
            let col = if rng.random_bool(0.5) { 'c' } else { 'd' };
            // 1-in-4 IS NULL, else equality on a small value.
            let val = if rng.random_range(0..4) == 0 { None } else { Some(rng.random_range(0..6)) };
            branches.push((col, val));
        }
        // Force both columns to appear at least once so >=2 distinct indexes
        // resolve (otherwise the trial would silently fall to the single-index
        // path and not exercise MULTI-INDEX OR).
        if branches.iter().all(|(col, _)| *col == 'c') {
            branches[0].0 = 'd';
        } else if branches.iter().all(|(col, _)| *col == 'd') {
            branches[0].0 = 'c';
        }

        let residual_b_gt =
            if rng.random_bool(0.5) { Some(rng.random_range(0..2000)) } else { None };
        let pred = OraclePredicate { branches, residual_b_gt };

        let sql = pred.to_sql();
        let actual = query_ints_sorted(&db, &sql);
        let expected = pred.eval_reference(&rows);

        assert_eq!(
            actual, expected,
            "trial {trial}: MULTI-INDEX OR result diverged from reference\n  SQL: {sql}",
        );

        // Also assert NO duplicate rowids slipped through (exactly-once).
        let raw = query_ints_raw(&db, &sql);
        let mut sorted_raw = raw.clone();
        sorted_raw.sort_unstable();
        sorted_raw.dedup();
        assert_eq!(
            raw.len(),
            sorted_raw.len(),
            "trial {trial}: duplicate rowid in MULTI-INDEX OR output\n  SQL: {sql}\n  raw: {raw:?}",
        );
    }
}
