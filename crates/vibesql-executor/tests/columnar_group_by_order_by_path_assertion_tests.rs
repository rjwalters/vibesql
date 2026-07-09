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

use std::sync::{Mutex, OnceLock};

use log::{Level, LevelFilter, Log, Metadata, Record};
use vibesql_executor::{CreateTableExecutor, InsertExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

struct CaptureLogger;

static LOG_BUFFER: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

fn buffer() -> &'static Mutex<Vec<String>> {
    LOG_BUFFER.get_or_init(|| Mutex::new(Vec::new()))
}

impl Log for CaptureLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }
    fn log(&self, record: &Record) {
        if record.level() <= Level::Debug {
            buffer().lock().unwrap().push(format!("{}", record.args()));
        }
    }
    fn flush(&self) {}
}

static LOGGER: CaptureLogger = CaptureLogger;

fn init_logger() {
    let _ = log::set_logger(&LOGGER);
    log::set_max_level(LevelFilter::Debug);
}

fn take_logs() -> Vec<String> {
    std::mem::take(&mut *buffer().lock().unwrap())
}

/// Serializes every test in this binary: `VIBESQL_DISABLE_COLUMNAR` and the log
/// capture buffer are both process-global, so concurrent tests would race
/// (one test's row-path toggle could disable columnar in another's columnar
/// run; captured logs would interleave). PR #6006 pattern. Poisoned-lock
/// recovery keeps one panicking test from cascading failures.
static SERIAL: Mutex<()> = Mutex::new(());

/// Row-fallback marker (single-table path): the columnar runtime declined.
const ROW_FALLBACK: &str = "Standard columnar runtime fallback to row-oriented";
/// Positive marker emitted only when the native columnar path completes.
const COLUMNAR_COMPLETED: &str = "Native columnar execution completed";

fn execute_sql(db: &mut Database, sql: &str) {
    for sql_stmt in sql.split(';') {
        let trimmed = sql_stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        let stmt = Parser::parse_sql(trimmed).expect("Failed to parse SQL");
        match stmt {
            vibesql_ast::Statement::CreateTable(s) => {
                CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
            }
            vibesql_ast::Statement::Insert(s) => {
                InsertExecutor::execute(db, &s).expect("INSERT failed");
            }
            other => panic!("Unsupported statement type: {:?}", other),
        }
    }
}

fn run_select(db: &Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SELECT");
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        vibesql_executor::SelectExecutor::new(db).execute(&select_stmt).expect("SELECT failed")
    } else {
        panic!("Expected SELECT");
    }
}

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
    init_logger();
    let mut db = Database::new();
    setup(&mut db, 600);
    columnar_matches_row_ordered(&db, "SELECT a, SUM(v) FROM t GROUP BY a ORDER BY a");
}

#[test]
fn group_by_order_by_bare_key_desc() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger();
    let mut db = Database::new();
    setup(&mut db, 600);
    columnar_matches_row_ordered(&db, "SELECT a, SUM(v) FROM t GROUP BY a ORDER BY a DESC");
}

#[test]
fn group_by_order_by_aggregate_desc() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger();
    let mut db = Database::new();
    setup(&mut db, 600);
    // ORDER BY an aggregate that appears in the SELECT list.
    columnar_matches_row_ordered(&db, "SELECT a, SUM(v) FROM t GROUP BY a ORDER BY SUM(v) DESC");
}

#[test]
fn group_by_order_by_ordinal_position() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger();
    let mut db = Database::new();
    setup(&mut db, 600);
    // ORDER BY 2 references the aggregate (second output column).
    columnar_matches_row_ordered(&db, "SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY 2");
}

#[test]
fn group_by_order_by_multi_key() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger();
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
    init_logger();
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
    init_logger();
    let mut db = Database::new();
    setup(&mut db, 900);
    // HAVING filters groups, then ORDER BY sorts the survivors — verifies the
    // sort runs AFTER the HAVING filter, matching the row path.
    columnar_matches_row_ordered(
        &db,
        "SELECT a, SUM(v) FROM t GROUP BY a HAVING SUM(v) > 100 ORDER BY SUM(v) DESC",
    );
}

/// NULL group key ordering. The columnar GROUP BY + ORDER BY path must place a
/// NULL group key FIRST for `ORDER BY k ASC`, matching SQLite (NULLs are the
/// smallest value).
///
/// NOTE ON PARITY: the *row-oriented* aggregate ORDER BY path has a PRE-EXISTING
/// bug where it places NULL group keys LAST for ASC (verified: a plain
/// non-aggregate `ORDER BY k ASC` correctly puts NULL first, but the GROUP BY
/// aggregate ORDER BY path puts NULL last — non-SQLite). This is independent of
/// the columnar work here and is NOT fixed by this PR (out of scope). Because the
/// row path is wrong for this case, we assert the columnar path against the
/// SQLite-correct order directly rather than blind row-path parity. Filed as a
/// follow-up (see PR description).
#[test]
fn group_by_order_by_null_keys_first_ascending() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger();
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE n (k INTEGER, v INTEGER)");
    let mut ins = String::new();
    for i in 0..400i64 {
        if i % 7 == 0 {
            ins.push_str(&format!("INSERT INTO n VALUES (NULL, {});", (i % 11) + 1));
        } else {
            ins.push_str(&format!("INSERT INTO n VALUES ({}, {});", i % 4, (i % 11) + 1));
        }
    }
    execute_sql(&mut db, &ins);
    let _ = take_logs();

    // Path assertion: columnar path is taken.
    let sql = "SELECT k, SUM(v) FROM n GROUP BY k ORDER BY k ASC";
    let cols = run_select(&db, sql);
    let logs = take_logs();
    let joined = logs.join("\n");
    assert!(
        !joined.contains(ROW_FALLBACK),
        "row-fallback marker emitted for NULL-key GROUP BY ORDER BY:\n{joined}"
    );
    assert!(
        logs.iter().any(|l| l.contains(COLUMNAR_COMPLETED)),
        "expected native columnar execution for NULL-key GROUP BY ORDER BY:\n{joined}"
    );

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

#[test]
fn group_by_order_by_text_affinity_key() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    init_logger();
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
    init_logger();
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
