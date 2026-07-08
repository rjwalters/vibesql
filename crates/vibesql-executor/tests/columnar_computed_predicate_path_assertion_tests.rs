//! Path-assertion + parity tests for issue #5994: confirm that a single-table
//! aggregate whose WHERE clause is a computed arithmetic expression over
//! columns (`WHERE a * b > 100`, `WHERE (a - b) <= c`) takes the native
//! columnar filter path instead of falling back to row-oriented execution.
//!
//! Per the acceptance criteria, wall-clock timing is NOT used (the machine is
//! loaded). Instead we capture the `log` output and assert:
//!   - the negative marker "WHERE clause contains unsupported predicates"
//!     (row fallback) must NOT appear;
//!   - the positive marker "Native columnar execution completed" (info) proves
//!     the columnar path ran with the predicate applied.
//!
//! Parity is checked by comparing the columnar result against the row path
//! (`VIBESQL_DISABLE_COLUMNAR=1`) for the same query.
//!
//! This test installs a process-global `log` logger, so it lives in its own
//! test binary to avoid clashing with other integration tests.

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
        if record.level() <= Level::Info {
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

/// Row-fallback marker: the WHERE clause could not be represented columnarly.
const ROW_FALLBACK: &str = "WHERE clause contains unsupported predicates";
/// Positive marker emitted only when the native columnar path completes.
const COLUMNAR_COMPLETED: &str = "Native columnar execution completed";

/// Populate a table `t(a INTEGER, b INTEGER, c INTEGER)` with `n` rows.
fn setup(db: &mut Database, n: i64) {
    execute_sql(db, "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER)");
    let mut ins = String::new();
    for i in 0..n {
        let a = i % 20;
        let b = i % 13;
        let c = i % 7;
        ins.push_str(&format!("INSERT INTO t VALUES ({a}, {b}, {c});"));
    }
    execute_sql(db, &ins);
}

/// Scalar sum of a single-column result set (the aggregate result).
fn sum_result(rows: &[vibesql_storage::Row]) -> i64 {
    // The query returns one row with one column (the SUM). Extract it robustly.
    match rows.first().and_then(|r| r.get(0)) {
        Some(vibesql_types::SqlValue::Integer(v)) => *v,
        Some(vibesql_types::SqlValue::Bigint(v)) => *v,
        Some(vibesql_types::SqlValue::Null) | None => 0,
        Some(other) => panic!("unexpected aggregate value: {other:?}"),
    }
}

#[test]
fn computed_predicate_mul_takes_columnar_path_and_matches_row_path() {
    init_logger();

    // Enough rows (>256) to exercise the SIMD streaming filter path.
    let mut db = Database::new();
    setup(&mut db, 1000);
    let _ = take_logs(); // discard setup logs

    let sql = "SELECT sum(a) FROM t WHERE a * b > 100";
    let rows = run_select(&db, sql);
    let logs = take_logs();
    let joined = logs.join("\n");

    // Path assertion: no row fallback, columnar path completed.
    assert!(
        !joined.contains(ROW_FALLBACK),
        "row-fallback line emitted for a computed WHERE predicate; columnar path skipped:\n{joined}"
    );
    assert!(
        logs.iter().any(|l| l.contains(COLUMNAR_COMPLETED)),
        "expected native columnar execution to run for 'a * b > 100':\n{joined}"
    );

    // Parity: same query with columnar disabled (row path).
    std::env::set_var("VIBESQL_DISABLE_COLUMNAR", "1");
    let row_rows = run_select(&db, sql);
    std::env::remove_var("VIBESQL_DISABLE_COLUMNAR");

    assert_eq!(
        sum_result(&rows),
        sum_result(&row_rows),
        "columnar result must equal row-path result for 'a * b > 100'"
    );
}

#[test]
fn computed_predicate_sub_le_column_matches_row_path() {
    init_logger();

    let mut db = Database::new();
    setup(&mut db, 800);
    let _ = take_logs();

    // (a - b) <= c : LHS is arithmetic over columns, RHS is a constant here
    // (column-vs-column on the RHS is out of Phase 1 scope), so compare to 3.
    let sql = "SELECT sum(c) FROM t WHERE (a - b) <= 3";
    let rows = run_select(&db, sql);
    let logs = take_logs();
    let joined = logs.join("\n");

    assert!(
        !joined.contains(ROW_FALLBACK),
        "row-fallback line emitted for '(a - b) <= 3':\n{joined}"
    );
    assert!(
        logs.iter().any(|l| l.contains(COLUMNAR_COMPLETED)),
        "expected native columnar execution for '(a - b) <= 3':\n{joined}"
    );

    std::env::set_var("VIBESQL_DISABLE_COLUMNAR", "1");
    let row_rows = run_select(&db, sql);
    std::env::remove_var("VIBESQL_DISABLE_COLUMNAR");

    assert_eq!(sum_result(&rows), sum_result(&row_rows));
}

/// NULL propagation parity: rows with a NULL operand must not match, matching
/// the row path exactly.
#[test]
fn computed_predicate_null_propagation_matches_row_path() {
    init_logger();

    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE u (a INTEGER, b INTEGER)");
    let mut ins = String::new();
    for i in 0..500i64 {
        if i % 9 == 0 {
            ins.push_str(&format!("INSERT INTO u VALUES (NULL, {});", i % 11));
        } else {
            ins.push_str(&format!("INSERT INTO u VALUES ({}, {});", i % 11, i % 7));
        }
    }
    execute_sql(&mut db, &ins);
    let _ = take_logs();

    let sql = "SELECT count(*) FROM u WHERE a * b > 5";

    let columnar = run_select(&db, sql);
    let logs = take_logs();
    let joined = logs.join("\n");
    assert!(!joined.contains(ROW_FALLBACK), "unexpected row fallback:\n{joined}");

    std::env::set_var("VIBESQL_DISABLE_COLUMNAR", "1");
    let row = run_select(&db, sql);
    std::env::remove_var("VIBESQL_DISABLE_COLUMNAR");

    assert_eq!(
        sum_result(&columnar),
        sum_result(&row),
        "NULL-propagation count must match the row path"
    );
}
