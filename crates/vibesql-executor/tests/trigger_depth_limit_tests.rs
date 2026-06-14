//! Tests for the per-connection runtime trigger-depth limit (#5536).
//!
//! SQLite lets a connection lower its trigger-recursion limit at runtime via
//! `sqlite3_limit(db, SQLITE_LIMIT_TRIGGER_DEPTH, N)` (exercised by SQLite's
//! triggerC-3.5.x / 3.6.x). VibeSQL stores that value on the `Database`
//! (`set_trigger_depth_limit`, surfaced to the CLI/TCL shim via the internal
//! `PRAGMA trigger_depth_limit`) and honors it in `RecursionGuard`, clamped to
//! the stack-safe compile-time cap.
//!
//! Every expectation below mirrors SQLite's triggerC-3.6.x against
//! sqlite3 3.51.0, where `SQLITE_LIMIT_TRIGGER_DEPTH` is set to 1.

use vibesql_executor::{
    CreateTableExecutor, DeleteExecutor, InsertExecutor, SelectExecutor, TriggerExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Parse and execute a single non-SELECT statement, returning the executor
/// result so callers can assert on success *or* failure.
fn try_exec(db: &mut Database, sql: &str) -> Result<(), String> {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("parse failed for `{sql}`: {e:?}"))?;
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).map_err(|e| e.to_string())?;
        }
        Statement::CreateTrigger(s) => {
            TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql))
                .map_err(|e| e.to_string())?;
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).map_err(|e| e.to_string())?;
        }
        Statement::Delete(s) => {
            DeleteExecutor::execute(&s, db).map_err(|e| e.to_string())?;
        }
        other => panic!("unsupported statement in test helper: {other:?}"),
    }
    Ok(())
}

/// Execute a statement that must succeed.
fn exec(db: &mut Database, sql: &str) {
    try_exec(db, sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
}

/// Run a single-value SELECT count(*) and return it.
fn count(db: &Database, sql: &str) -> i64 {
    use vibesql_ast::Statement;
    let Statement::Select(select) = Parser::parse_sql(sql).expect("parse failed") else {
        panic!("expected SELECT");
    };
    let rows = SelectExecutor::new(db).execute(&select).expect("SELECT failed");
    match rows[0].values[0] {
        SqlValue::Integer(n) => n,
        ref other => panic!("expected integer count, got {other:?}"),
    }
}

/// triggerC-3.6.x shape: a WHEN-guarded self-insert with the runtime trigger
/// limit lowered to 1.
///
/// - Inserting 2000 fires zero nested triggers (`WHEN new.x<2000` is false), so it succeeds even
///   with a limit of 1 (triggerC-3.6.1/3.6.2).
/// - Inserting 1999 fires one nested trigger (1999 -> 2000), which trips the limit of 1, so the
///   statement aborts with SQLite's exact wording and rolls back (triggerC-3.6.3/3.6.4).
#[test]
fn runtime_limit_of_one_aborts_at_one_level() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t3b(x)");
    exec(
        &mut db,
        "CREATE TRIGGER t3bi AFTER INSERT ON t3b WHEN new.x<2000 \
         BEGIN INSERT INTO t3b VALUES(new.x+1); END",
    );

    // sqlite3_limit(db, SQLITE_LIMIT_TRIGGER_DEPTH, 1)
    db.set_trigger_depth_limit(1);
    assert_eq!(db.trigger_depth_limit(), Some(1));

    // Non-recursing insert succeeds under the limit (3.6.1/3.6.2).
    exec(&mut db, "INSERT INTO t3b VALUES(2000)");
    assert_eq!(count(&db, "SELECT count(*) FROM t3b"), 1);

    exec(&mut db, "DELETE FROM t3b");

    // One-level recursion trips the limit of 1 and aborts (3.6.3).
    let err = try_exec(&mut db, "INSERT INTO t3b VALUES(1999)")
        .expect_err("recursion must abort at the runtime limit");
    assert_eq!(err, "too many levels of trigger recursion");

    // The aborted statement rolled back: table empty (3.6.4).
    assert_eq!(count(&db, "SELECT count(*) FROM t3b"), 0);
}

/// Without a per-connection limit, the same one-level recursion succeeds: the
/// default cap is far above 1. This pins the regression — the limit must only
/// take effect when explicitly lowered.
#[test]
fn default_limit_allows_recursion_blocked_by_a_low_runtime_limit() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t3b(x)");
    exec(
        &mut db,
        "CREATE TRIGGER t3bi AFTER INSERT ON t3b WHEN new.x<2000 \
         BEGIN INSERT INTO t3b VALUES(new.x+1); END",
    );

    assert_eq!(db.trigger_depth_limit(), None, "no runtime limit by default");

    // 1999 -> 2000 is one level of recursion; with the default cap it succeeds.
    exec(&mut db, "INSERT INTO t3b VALUES(1999)");
    assert_eq!(count(&db, "SELECT count(*) FROM t3b"), 2);
}

/// A runtime limit at or above the compile-time cap is treated as "no override":
/// it must not raise the stack-safe cap (clamping is one-directional, downward).
/// triggerC resets the limit to `SQLITE_MAX_TRIGGER_DEPTH` (1000) at the end of
/// the 3.x block to restore the default — this guards that path.
#[test]
fn runtime_limit_above_cap_does_not_raise_the_stack_safe_cap() {
    let mut db = Database::new();
    db.set_trigger_depth_limit(100_000);

    // Recursion still aborts at the compile-time cap, not at 100_000 — proven by
    // an unconditional self-insert hitting the canonical error well before the
    // huge requested limit. Run on a generous stack: the native recursion goes
    // to the cap (1000) before erroring (debug stack frames are large).
    std::thread::Builder::new()
        .name("runtime_limit_above_cap".to_string())
        .stack_size(96 * 1024 * 1024)
        .spawn(move || {
            exec(&mut db, "CREATE TABLE t(x)");
            exec(
                &mut db,
                "CREATE TRIGGER ti AFTER INSERT ON t BEGIN INSERT INTO t VALUES(new.x+1); END",
            );
            let err = try_exec(&mut db, "INSERT INTO t VALUES(1)")
                .expect_err("unbounded recursion must hit the compile-time cap");
            assert_eq!(err, "too many levels of trigger recursion");
        })
        .expect("spawn thread")
        .join()
        .expect("cap must error, not overflow the stack");
}
