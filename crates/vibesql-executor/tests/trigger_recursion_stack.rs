//! Depth-cap + stack-safety regression tests for trigger recursion (#5479).
//!
//! VibeSQL caps trigger recursion at `MAX_TRIGGER_RECURSION_DEPTH = 700`. The
//! previous cap of 16 wrongly rejected legitimate recursive-trigger programs
//! that SQLite accepts (SQLite's `SQLITE_MAX_TRIGGER_DEPTH` is 1000; its
//! triggerC-2.x conformance tests drive ~500 levels). The cap is 700 rather
//! than the full 1000 for stack safety: trigger recursion uses native Rust
//! call-stack recursion (~8.7 KiB/level in release), and the 8 MiB main thread
//! used by the CLI/server overflows at depth ~960. 700 keeps headroom below
//! that cliff while comfortably clearing the ~500 levels SQLite's tests need.
//!
//! These tests verify that:
//!   1. recursion well past the old cap of 16 SUCCEEDS,
//!   2. recursion at ~500 levels (the SQLite triggerC-2.x depth) SUCCEEDS,
//!   3. recursion that reaches the 700 cap errors with SQLite's exact wording,
//!      `too many levels of trigger recursion`, instead of overflowing.
//!
//! Per-level stack cost is high in DEBUG builds (~50 KiB/level vs ~8.7 KiB in
//! release), so a default 2 MiB libtest worker thread would overflow well
//! before depth 700. To exercise the cap deterministically in BOTH debug and
//! release `cargo test` runs, each deep case runs on a dedicated thread with a
//! generously sized stack. The point being demonstrated: when the recursion is
//! given enough stack, depths up to the cap complete cleanly and the over-cap
//! case errors rather than crashing. The *production* stack-safety guarantee
//! (cap < release/8 MiB overflow depth) is enforced by the cap value itself,
//! documented on `MAX_TRIGGER_RECURSION_DEPTH`.

use vibesql_executor::{InsertExecutor, SelectExecutor, TriggerExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// A stack large enough to run up to the 700 cap even in debug builds, where a
/// single trigger level can consume ~50 KiB: 700 * 50 KiB ~= 35 MiB, so 96 MiB
/// leaves a wide margin.
const DEEP_TEST_STACK: usize = 96 * 1024 * 1024;

/// The actual worker-thread stack the SERVER configures for its tokio runtime
/// (`crates/vibesql-server/src/main.rs::SERVER_WORKER_STACK_SIZE`). Trigger DML
/// runs synchronously on these threads, so the production stack-safety of the
/// 700 cap is exactly the stack-safety of recursion on a thread THIS size — not
/// the 96 MiB used by `DEEP_TEST_STACK`. The server-path regression test below
/// pins to this value so it would catch a regression (e.g. dropping the
/// `thread_stack_size` override back to tokio's 2 MiB default) that the
/// 96 MiB-pinned tests cannot see.
const SERVER_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;

/// Parse and dispatch a single SQL statement (CREATE TABLE / CREATE TRIGGER /
/// INSERT) against `db`.
fn exec(db: &mut Database, sql: &str) -> Result<(), vibesql_executor::ExecutorError> {
    match Parser::parse_sql(sql).expect("test SQL should parse") {
        vibesql_ast::Statement::CreateTable(s) => {
            vibesql_executor::CreateTableExecutor::execute(&s, db)?;
        }
        vibesql_ast::Statement::CreateTrigger(s) => {
            TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql))?;
        }
        vibesql_ast::Statement::Insert(s) => {
            InsertExecutor::execute(db, &s)?;
        }
        other => panic!("unexpected statement in test: {other:?}"),
    }
    Ok(())
}

fn count_rows(db: &Database, table: &str) -> usize {
    match Parser::parse_sql(&format!("SELECT * FROM {table}")).unwrap() {
        vibesql_ast::Statement::Select(s) => {
            SelectExecutor::new(db).execute(&s).expect("select should succeed").len()
        }
        _ => unreachable!(),
    }
}

/// Build a self-recursive AFTER INSERT trigger guarded by `WHEN new.a > 0`
/// (mirrors triggerC-2.1 case 1): inserting `start` recurses `start` levels deep
/// producing rows `start..=0`, unless it first hits the recursion cap.
fn run_countdown(start: i64) -> (Database, Result<(), vibesql_executor::ExecutorError>) {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t2(a INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER t2_trig AFTER INSERT ON t2 WHEN (new.a > 0) BEGIN \
         INSERT INTO t2 VALUES(new.a - 1); END",
    )
    .unwrap();
    let res = exec(&mut db, &format!("INSERT INTO t2 VALUES({start})"));
    (db, res)
}

/// Run `f` on a thread with a large stack so deep recursion does not overflow
/// the (small) default libtest worker stack in debug builds.
fn on_big_stack<F: FnOnce() + Send + 'static>(name: &str, f: F) {
    on_stack(name, DEEP_TEST_STACK, f);
}

/// Run `f` on a freshly spawned thread with an explicit `stack_size`, panicking
/// if the thread aborts (e.g. a stack overflow). Used both for the wide-margin
/// `DEEP_TEST_STACK` cases and the server-config-pinned regression test.
fn on_stack<F: FnOnce() + Send + 'static>(name: &str, stack_size: usize, f: F) {
    std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(stack_size)
        .spawn(f)
        .expect("spawn deep-recursion test thread")
        .join()
        .unwrap_or_else(|_| panic!("{name}: trigger recursion overflowed the stack"));
}

#[test]
fn recursion_succeeds_past_old_cap_of_16() {
    // Depth 20 > old cap (16). Must succeed now that the cap is 700. Depth 20 is
    // safe even on the default libtest stack, so no big-stack thread needed.
    let (db, res) = run_countdown(20);
    res.expect("depth-20 recursion should succeed under the 700 cap");
    assert_eq!(count_rows(&db, "t2"), 21, "rows 20..=0 should all be inserted");
}

#[test]
fn recursion_succeeds_at_500_levels() {
    // ~500 levels is the depth SQLite's triggerC-2.2/2.3 require (and is < the
    // 700 cap). Run on a big stack so the debug per-level frame cost cannot
    // overflow a default test thread.
    on_big_stack("trigger-depth-500", || {
        let (db, res) = run_countdown(500);
        res.expect("depth-500 recursion should succeed under the 700 cap");
        assert_eq!(count_rows(&db, "t2"), 501);
    });
}

#[test]
fn recursion_at_cap_errors_with_sqlite_wording_not_overflow() {
    // start = 5000 drives recursion to the 700 cap. With enough stack, the cap
    // check fires and the program errors with SQLite's exact wording rather than
    // overflowing the stack. This is the core stack-safety assertion: hitting
    // the cap is a clean error, not a crash.
    on_big_stack("trigger-over-cap", || {
        let (db, res) = run_countdown(5000);
        let err = res.expect_err("recursion reaching the cap must error");
        assert_eq!(
            err.to_string(),
            "too many levels of trigger recursion",
            "over-cap recursion must use SQLite's exact wording",
        );
        // The whole INSERT is rolled back; the failed program leaves no rows.
        assert_eq!(count_rows(&db, "t2"), 0, "failed recursive program leaves no rows");
    });
}

/// Server-DoS regression (#5533): drive recursion to the 700 cap on a thread
/// sized EXACTLY like the server's tokio worker (`SERVER_WORKER_STACK_SIZE`,
/// 8 MiB) and assert it errors cleanly with SQLite's wording instead of
/// overflowing the stack (SIGABRT).
///
/// This is the test the original PR was missing: the other deep cases pin to a
/// 96 MiB stack, which proves the *cap logic* is correct but hides the
/// production failure mode — the server runs trigger DML synchronously on tokio
/// worker threads, and tokio's default 2 MiB worker stack overflows at depth
/// ~240, BEFORE the 700 cap fires. A remote client sending a self-recursive
/// trigger INSERT could thus crash the server. The fix sizes the server's
/// worker stack at 8 MiB; this test reflects that real config, so if anyone
/// drops the override (or shrinks the stack below what 700 levels need) it fails
/// here rather than only crashing in production.
///
/// Release-only: per-level frame cost is ~8.7 KiB in release (700 ~= 6.1 MiB,
/// fits comfortably in 8 MiB) but ~50 KiB in DEBUG (700 ~= 35 MiB, would
/// overflow an 8 MiB stack — debug is not the shipping configuration, and the
/// server is run in release). The 96 MiB-pinned `*_at_cap_*` test above still
/// exercises the cap logic in debug builds.
#[cfg(not(debug_assertions))]
#[test]
fn server_runtime_stack_reaches_cap_cleanly_not_overflow() {
    on_stack("trigger-server-stack-700", SERVER_WORKER_STACK_SIZE, || {
        // 5000 drives recursion to the 700 cap. On the server's 8 MiB worker
        // stack this must produce a clean cap error, NOT a stack overflow.
        let (db, res) = run_countdown(5000);
        let err = res.expect_err("recursion reaching the cap must error, not overflow");
        assert_eq!(
            err.to_string(),
            "too many levels of trigger recursion",
            "over-cap recursion on the server's 8 MiB stack must error cleanly",
        );
        assert_eq!(count_rows(&db, "t2"), 0, "failed recursive program leaves no rows");
    });
}

/// In DEBUG builds, prove the server's 8 MiB worker stack clears the depth that
/// overflows tokio's DEFAULT 2 MiB worker (~240 in release; the debug frame is
/// larger, so we use a conservative 150 that exceeds the ~40-level debug/2 MiB
/// cliff yet stays well within 8 MiB at ~50 KiB/level). Confirms the
/// stack-size bump actually buys headroom over the un-fixed default.
#[cfg(debug_assertions)]
#[test]
fn server_runtime_stack_clears_default_2mib_cliff() {
    on_stack("trigger-server-stack-debug", SERVER_WORKER_STACK_SIZE, || {
        let (db, res) = run_countdown(150);
        res.expect("depth-150 must succeed on the server's 8 MiB worker stack");
        assert_eq!(count_rows(&db, "t2"), 151);
    });
}
