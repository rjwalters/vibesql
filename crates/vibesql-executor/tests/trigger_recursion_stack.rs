//! Depth-cap + stack-safety regression tests for trigger recursion
//! (#5479 / #5533 / #5534).
//!
//! VibeSQL caps trigger recursion at `MAX_TRIGGER_RECURSION_DEPTH`, which on
//! native targets is now **1000** — matching SQLite's `SQLITE_MAX_TRIGGER_DEPTH`
//! exactly (verified against sqlite3 3.51.0). The previous cap of 16 wrongly
//! rejected legitimate recursive-trigger programs that SQLite accepts; #5533
//! raised it to a stack-safe 700, and #5534 raises it the rest of the way to
//! 1000 by making the recursion stack-safe instead of relying on the
//! worker-thread stack being large enough for 1000 native frames.
//!
//! Stack safety (#5534): firing a trigger re-enters the full DML path (parse +
//! plan + execute + fire) as native Rust recursion (~8.7 KiB/level release,
//! ~50 KiB/level debug). Rather than cap below the overflow cliff, the executor
//! now grows the native stack on demand at the trigger-recursion entry point
//! (`stacker::maybe_grow`), so reaching the 1000 cap is a clean
//! `too many levels of trigger recursion` error on ANY thread size, never an
//! overflow. The load-bearing test here drives recursion to the cap on a
//! deliberately SMALL fixed stack (smaller than 1000 native frames would need)
//! and asserts a clean cap error — i.e. proving on-demand growth works.
//!
//! These tests verify that:
//!   1. recursion well past the old cap of 16 SUCCEEDS,
//!   2. recursion at ~500 levels (the SQLite triggerC-2.x depth) SUCCEEDS,
//!   3. recursion at exactly the 1000 cap SUCCEEDS (triggerC-3.3.x depth),
//!   4. recursion that exceeds the 1000 cap errors with SQLite's exact wording, `too many levels of
//!      trigger recursion`, instead of overflowing,
//!   5. the cap is reached cleanly even on a stack far too small to hold 1000 native frames,
//!      proving the on-demand stack growth (no overflow / DoS).

use vibesql_executor::{InsertExecutor, SelectExecutor, TriggerExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Native trigger recursion cap (mirrors `MAX_TRIGGER_RECURSION_DEPTH` for
/// non-wasm targets). Kept in sync with `trigger_execution.rs`.
const NATIVE_TRIGGER_CAP: usize = 1000;

/// A stack large enough to run up to the cap even in debug builds (~50 KiB per
/// level): 1000 * 50 KiB ~= 49 MiB, so 96 MiB leaves a wide margin. Used by the
/// cases that simply exercise the cap *logic*; the on-demand-growth proof below
/// deliberately uses a SMALL stack instead.
const DEEP_TEST_STACK: usize = 96 * 1024 * 1024;

/// The actual worker-thread stack the SERVER configures for its tokio runtime
/// (`crates/vibesql-server/src/main.rs::SERVER_WORKER_STACK_SIZE`). Trigger DML
/// runs synchronously on these threads. With #5534's on-demand stack growth the
/// cap no longer depends on this size, but pinning to it documents the real
/// production config and guards the #5533 DoS fix.
///
/// Only referenced by the release-only server-stack regression test; in debug
/// builds the on-demand-growth proof uses `SMALL_FIXED_STACK` instead.
#[cfg_attr(debug_assertions, allow(dead_code))]
const SERVER_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;

/// A stack deliberately too small to hold 1000 native trigger frames in release
/// (1000 * ~8.7 KiB ~= 8.5 MiB; 2 MiB overflows at ~240 without growth). The
/// on-demand-growth test pins to this to prove `stacker` lets recursion reach
/// the 1000 cap cleanly anyway. (Release-only: debug frames are ~50 KiB and the
/// stacker red zone/segment sizing differs; the cap *logic* in debug is covered
/// by the 96 MiB cases.)
const SMALL_FIXED_STACK: usize = 2 * 1024 * 1024;

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
    // These tests exercise deep native trigger recursion, which requires
    // recursive_triggers ON. The default is now OFF (SQLite default, #5840),
    // which would suppress the self-re-entry after one level, so enable it here.
    db.set_recursive_triggers(true);
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
    // Depth 20 > old cap (16). Must succeed under the current cap. Depth 20 is
    // safe even on the default libtest stack, so no big-stack thread needed.
    let (db, res) = run_countdown(20);
    res.expect("depth-20 recursion should succeed under the cap");
    assert_eq!(count_rows(&db, "t2"), 21, "rows 20..=0 should all be inserted");
}

#[test]
fn recursion_succeeds_at_500_levels() {
    // ~500 levels is the depth SQLite's triggerC-2.2/2.3 require (and is < the
    // 1000 cap). Run on a big stack so the debug per-level frame cost cannot
    // overflow a default test thread.
    on_big_stack("trigger-depth-500", || {
        let (db, res) = run_countdown(500);
        res.expect("depth-500 recursion should succeed under the cap");
        assert_eq!(count_rows(&db, "t2"), 501);
    });
}

#[test]
fn recursion_succeeds_at_full_1000_cap() {
    // triggerC-3.3.x require reaching SQLite's full SQLITE_MAX_TRIGGER_DEPTH of
    // 1000. Driving the chain to exactly the cap must SUCCEED (the error fires
    // only when the cap is *exceeded*). Run on a big stack: this case checks the
    // cap *boundary*, not the on-demand growth (covered separately below).
    on_big_stack("trigger-depth-1000", || {
        // Each INSERT(new.a) recurses one level per decrement; start = cap - 1
        // descends exactly to the cap on the final insert and stops at a == 0.
        let (db, res) = run_countdown((NATIVE_TRIGGER_CAP - 1) as i64);
        res.expect("recursion to the full 1000 cap should succeed");
        assert_eq!(count_rows(&db, "t2"), NATIVE_TRIGGER_CAP);
    });
}

#[test]
fn recursion_over_cap_errors_with_sqlite_wording_not_overflow() {
    // start = 5000 drives recursion past the 1000 cap. With enough stack, the
    // cap check fires and the program errors with SQLite's exact wording rather
    // than overflowing the stack. Core stack-safety assertion: exceeding the cap
    // is a clean error, not a crash.
    on_big_stack("trigger-over-cap", || {
        let (db, res) = run_countdown(5000);
        let err = res.expect_err("recursion exceeding the cap must error");
        assert_eq!(
            err.to_string(),
            "too many levels of trigger recursion",
            "over-cap recursion must use SQLite's exact wording",
        );
        // The whole INSERT is rolled back; the failed program leaves no rows.
        assert_eq!(count_rows(&db, "t2"), 0, "failed recursive program leaves no rows");
    });
}

/// #5534 load-bearing test: prove on-demand stack growth lets recursion reach
/// the full 1000 cap on a fixed stack FAR too small to hold 1000 native frames.
///
/// 1000 levels need ~8.5 MiB of native stack in release; this thread is pinned
/// to 2 MiB, which overflows at depth ~240 WITHOUT growth. With
/// `stacker::maybe_grow` at the recursion entry point, the executor allocates
/// fresh heap-backed stack segments on demand, so:
///   - depth exactly 1000 SUCCEEDS, and
///   - depth beyond 1000 errors cleanly with SQLite's wording — never a SIGABRT stack overflow.
/// If on-demand growth regressed (or were removed), this thread would overflow
/// and the harness would report the join failure rather than a clean error.
///
/// Release-only: the 8.7 KiB/level figure (and thus the 2 MiB-is-too-small
/// premise) is the release frame size; debug frames are ~50 KiB.
#[cfg(not(debug_assertions))]
#[test]
fn ondemand_growth_reaches_1000_cap_on_small_stack_no_overflow() {
    on_stack("trigger-grow-1000-small-stack", SMALL_FIXED_STACK, || {
        // Reach exactly the cap on a 2 MiB stack: only possible via on-demand
        // growth (1000 native frames > 2 MiB).
        let (db, res) = run_countdown((NATIVE_TRIGGER_CAP - 1) as i64);
        res.expect("on-demand stack growth should let recursion reach the 1000 cap");
        assert_eq!(count_rows(&db, "t2"), NATIVE_TRIGGER_CAP);
    });
    on_stack("trigger-grow-over-cap-small-stack", SMALL_FIXED_STACK, || {
        // Exceeding the cap must still be a clean error on the small stack — the
        // growth makes us reach the cap, the cap (not the stack) stops us.
        let (db, res) = run_countdown(5000);
        let err = res.expect_err("over-cap recursion must error cleanly, not overflow");
        assert_eq!(
            err.to_string(),
            "too many levels of trigger recursion",
            "over-cap recursion on a small stack must error with SQLite's wording",
        );
        assert_eq!(count_rows(&db, "t2"), 0);
    });
}

/// Server-DoS regression (#5533 / #5534): drive recursion past the 1000 cap on
/// a thread sized EXACTLY like the server's tokio worker
/// (`SERVER_WORKER_STACK_SIZE`, 8 MiB) and assert it errors cleanly with
/// SQLite's wording instead of overflowing the stack (SIGABRT).
///
/// The server runs trigger DML synchronously on tokio worker threads. Before
/// #5534 the cap (700) had to stay below what 8 MiB could hold; with on-demand
/// stack growth the cap is the full 1000 and the executor grows the stack rather
/// than relying on the worker stack alone. This test pins to the real server
/// stack size and asserts a clean cap error, guarding both the #5533 8 MiB
/// override and the #5534 growth: if the growth regressed AND the stack were
/// shrunk below what 1000 frames need, this would overflow here rather than only
/// crashing in production.
///
/// Release-only: per-level frame cost is ~8.7 KiB in release vs ~50 KiB in
/// DEBUG; the over-cap depth is exercised in debug by the 96 MiB-pinned
/// `*_over_cap_*` case above.
#[cfg(not(debug_assertions))]
#[test]
fn server_runtime_stack_reaches_cap_cleanly_not_overflow() {
    on_stack("trigger-server-stack-1000", SERVER_WORKER_STACK_SIZE, || {
        // 5000 drives recursion past the 1000 cap. On the server's 8 MiB worker
        // stack this must produce a clean cap error, NOT a stack overflow.
        let (db, res) = run_countdown(5000);
        let err = res.expect_err("recursion exceeding the cap must error, not overflow");
        assert_eq!(
            err.to_string(),
            "too many levels of trigger recursion",
            "over-cap recursion on the server's 8 MiB stack must error cleanly",
        );
        assert_eq!(count_rows(&db, "t2"), 0, "failed recursive program leaves no rows");
    });
}

/// In DEBUG builds, prove on-demand stack growth clears the depth that overflows
/// tokio's DEFAULT 2 MiB worker. The debug per-level frame is ~50 KiB, so 2 MiB
/// overflows at ~40 levels without growth; here we drive depth 300 on a 2 MiB
/// stack and require success — only possible because the executor grows the
/// stack on demand (#5534). Confirms the growth, not merely the 8 MiB bump,
/// provides headroom.
#[cfg(debug_assertions)]
#[test]
fn ondemand_growth_clears_default_2mib_cliff_debug() {
    on_stack("trigger-grow-debug", SMALL_FIXED_STACK, || {
        let (db, res) = run_countdown(300);
        res.expect("depth-300 must succeed on a 2 MiB stack via on-demand growth");
        assert_eq!(count_rows(&db, "t2"), 301);
    });
}
