//! Path-assertion test for issue #5993: confirm `WHERE col IS NULL` and the
//! anti-join `LEFT JOIN ... WHERE r.col IS NULL` take the columnar filter/join
//! path rather than falling back to row-oriented execution.
//!
//! Per the acceptance criteria, wall-clock timing is NOT used (the machine is
//! loaded). Instead we capture the `log` output and assert on the debug/info
//! lines emitted by the columnar pipeline: the columnar join emits
//! `"Columnar join: N rows after join and filter"` only on its own path, and
//! the retired guardrail's `"... IS NULL/IS NOT NULL in WHERE, falling back"`
//! line must never appear.
//!
//! This test installs a process-global `log` logger, so it lives in its own
//! test binary to avoid clashing with other integration tests.

use log::Level;
use vibesql_storage::Database;

mod common;
use common::{execute_sql, init_logger, run_select, take_logs};

/// The retired guardrail's fallback marker (must never appear post-#5993).
const GUARDRAIL_FALLBACK: &str = "IS NULL/IS NOT NULL in WHERE, falling back";

#[test]
fn anti_join_is_null_takes_columnar_path() {
    init_logger(Level::Info);

    // Left ids 1..=600; right matches even ids only. Large enough to exercise
    // the columnar join + SIMD filter path.
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE l (id INTEGER PRIMARY KEY)");
    execute_sql(&mut db, "CREATE TABLE r (id INTEGER PRIMARY KEY, tag INTEGER)");
    let mut l = String::new();
    let mut r = String::new();
    for id in 1..=600i64 {
        l.push_str(&format!("INSERT INTO l VALUES ({id});"));
        if id % 2 == 0 {
            r.push_str(&format!("INSERT INTO r VALUES ({id}, {});", id * 10));
        }
    }
    execute_sql(&mut db, &l);
    execute_sql(&mut db, &r);

    let _ = take_logs(); // discard setup logs

    let rows = run_select(&db, "SELECT l.id FROM l LEFT JOIN r ON l.id = r.id WHERE r.id IS NULL");
    assert_eq!(rows.len(), 300, "anti-join must return the 300 unmatched left rows");

    let logs = take_logs();
    let joined = logs.join("\n");

    // The retired guardrail must not fire.
    assert!(
        !joined.contains(GUARDRAIL_FALLBACK),
        "guardrail fallback line was emitted for the anti-join; columnar path was skipped:\n{joined}"
    );

    // The columnar join path emits this info line only when it actually runs
    // the join + filter (join.rs). Its presence proves the columnar path.
    assert!(
        logs.iter().any(|l| l.contains("rows after join and filter")),
        "expected the columnar join path to run (missing 'rows after join and filter' log):\n{joined}"
    );
}
