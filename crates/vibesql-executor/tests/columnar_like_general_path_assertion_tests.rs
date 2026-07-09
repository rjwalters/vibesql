//! Path-assertion + parity tests for issue #6011: the columnar SIMD filter path
//! must take the *general* LIKE matcher (multi-`%` / `_`-bearing patterns) — the
//! rewrite that replaced the per-row `Vec<Vec<bool>>` dynamic-programming matrix
//! with an allocation-free two-pointer matcher (and `memchr`-accelerated
//! literal-run search).
//!
//! Per the acceptance criteria, wall-clock timing is NOT used (the machine is
//! loaded). Instead we capture `log` output and assert:
//!   - the row-fallback marker ("skipping - WHERE clause contains unsupported
//!     predicates") must NOT appear;
//!   - the positive marker "Native columnar execution completed" (an `info!`
//!     emitted only once the native columnar filter+aggregate path commits)
//!     proves the columnar SIMD filter path ran for the `General`-classified
//!     LIKE.
//!
//! The queries are `COUNT(*)`/`SUM(id)` aggregates filtered by LIKE — that is
//! the shape that reliably routes through the native columnar zero-copy path
//! (`try_native_columnar_execution`), which applies the SIMD LIKE filter via
//! `batch_string_like` (the code under test). Aggregate parity (matching count
//! and id-sum) proves the underlying boolean mask matches the row path.
//!
//! Parity is checked by comparing the columnar result against the row path
//! (`VIBESQL_DISABLE_COLUMNAR=1`) for a matrix of general LIKE patterns: leading/
//! trailing/interior `%`, single and multiple `_`, adjacent wildcards, empty
//! pattern-adjacent cases, patterns longer than all rows, non-ASCII text (ASCII
//! case-fold must not fold non-ASCII), NULL handling, and BLOB affinity.
//!
//! This test installs a process-global `log` logger, so it lives in its own test
//! binary to avoid clashing with other integration tests. `VIBESQL_DISABLE_COLUMNAR`
//! and the capture buffer are process-global, so a SERIAL mutex serializes the
//! columnar-vs-row runs (PR #6006 precedent).

use std::sync::{Mutex, OnceLock};

use log::{Level, LevelFilter, Log, Metadata, Record};
use vibesql_executor::{CreateTableExecutor, InsertExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

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
        // Capture Debug and above: the fallback marker is a debug! line and the
        // positive marker is an info! line.
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

/// Serializes the columnar-vs-row runs across tests. `VIBESQL_DISABLE_COLUMNAR`
/// is a process-global env var and the `log` capture buffer is process-global,
/// so concurrent tests would otherwise race. Holding this mutex for the whole
/// `run_both` keeps each test's two runs and their captured logs isolated.
static SERIAL: Mutex<()> = Mutex::new(());

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

fn run_select(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SELECT");
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let rows =
            vibesql_executor::SelectExecutor::new(db).execute(&select_stmt).expect("SELECT failed");
        rows.into_iter().map(|r| r.values.to_vec()).collect()
    } else {
        panic!("Expected SELECT");
    }
}

/// Run the same query on both the columnar path and the row path
/// (`VIBESQL_DISABLE_COLUMNAR=1`) and return `(columnar_rows, row_rows, logs)`.
fn run_both(db: &Database, sql: &str) -> (Vec<Vec<SqlValue>>, Vec<Vec<SqlValue>>, Vec<String>) {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());

    let _ = take_logs(); // discard prior logs
    let columnar = run_select(db, sql);
    let logs = take_logs();

    std::env::set_var("VIBESQL_DISABLE_COLUMNAR", "1");
    let row = run_select(db, sql);
    std::env::remove_var("VIBESQL_DISABLE_COLUMNAR");

    (columnar, row, logs)
}

/// Row-fallback marker: the WHERE predicate could not be extracted for columnar.
const ROW_FALLBACK: &str = "WHERE clause contains unsupported predicates";
/// Positive marker: the native columnar filter+aggregate path completed.
const COLUMNAR_COMPLETED: &str = "Native columnar execution completed";
/// Adaptive-model fallback: the query was routed to row-oriented up front.
const ADAPTIVE_ROW: &str = "adaptive model selected row-oriented";

fn assert_columnar_filter_ran(logs: &[String]) {
    let joined = logs.join("\n");
    assert!(
        !joined.contains(ROW_FALLBACK),
        "row-fallback (unsupported predicate) line emitted; columnar LIKE filter skipped:\n{joined}"
    );
    assert!(
        !joined.contains(ADAPTIVE_ROW),
        "adaptive model routed to row-oriented; columnar LIKE filter skipped:\n{joined}"
    );
    assert!(
        logs.iter().any(|l| l.contains(COLUMNAR_COMPLETED)),
        "expected native columnar filter+aggregate path to run for the general LIKE:\n{joined}"
    );
}

/// A mix of strings exercising general LIKE shapes, including the TPC-H Q13/Q16
/// analytically-relevant patterns.
fn setup_words(db: &mut Database, n: usize) {
    execute_sql(db, "CREATE TABLE w (id INTEGER, s TEXT)");
    let samples = [
        "special requests here",
        "the special package requests",
        "MEDIUM POLISHED BRASS",
        "MEDIUM ANODIZED POLISHED STEEL",
        "SMALL POLISHED COPPER",
        "no wildcards match this",
        "specialrequests",
        "requests special order",
        "café au lait", // non-ASCII
        "CAFÉ CLOSED",  // non-ASCII, uppercase
        "a_literal_underscore",
        "abcdefghij",
    ];
    let mut ins = String::new();
    for i in 0..n {
        let s = samples[i % samples.len()].replace('\'', "''");
        ins.push_str(&format!("INSERT INTO w VALUES ({i}, '{s}');"));
    }
    execute_sql(db, &ins);
}

/// The core Q13/Q16 general shapes take the columnar filter path and match the
/// row path exactly. Runs on both sides of the SIMD row threshold (256).
#[test]
fn general_like_takes_columnar_path_and_matches_row_path() {
    init_logger();

    // `%a%b%` (interior), `_`-bearing, and mixed `%_` — all classify General.
    // (Single-word `%x%` shapes classify as Contains and take a different
    // kernel, so they are intentionally excluded here.)
    let patterns = [
        "%special%requests%",   // Q13 shape (multi-interior %)
        "MEDIUM%POLISHED%",     // Q16 shape (prefix%mid% -> General)
        "M_DIUM%POLISHED%",     // mixed _ and %
        "%P_LISHED%",           // interior _ inside %...%
        "specialrequest_",      // trailing single _
        "a_literal_underscore", // multiple _ , no %
    ];

    for n in [40usize, 600usize] {
        let mut db = Database::new();
        setup_words(&mut db, n);

        for p in patterns {
            // COUNT + SUM aggregate over the LIKE filter: routes through the
            // native columnar path and exercises the SIMD LIKE mask directly.
            let sql = format!("SELECT COUNT(*), SUM(id) FROM w WHERE s LIKE '{p}'");
            let (columnar, row, logs) = run_both(&db, &sql);

            assert_columnar_filter_ran(&logs);
            assert_eq!(
                columnar, row,
                "columnar general-LIKE result must equal row path (pattern={p:?}, n={n})"
            );
        }
    }
}

/// NULL handling: NULL rows must be false in the mask (LIKE) and stay false for
/// NOT LIKE — identical to the row path.
#[test]
fn general_like_null_handling_matches_row_path() {
    init_logger();

    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE t (id INTEGER, s TEXT)");
    let mut ins = String::new();
    for i in 0..300i64 {
        if i % 5 == 0 {
            ins.push_str(&format!("INSERT INTO t VALUES ({i}, NULL);"));
        } else {
            ins.push_str(&format!("INSERT INTO t VALUES ({i}, 'a{i}b');"));
        }
    }
    execute_sql(&mut db, &ins);

    for sql in [
        "SELECT COUNT(*), SUM(id) FROM t WHERE s LIKE '%a%b%'",
        "SELECT COUNT(*), SUM(id) FROM t WHERE s NOT LIKE '%a_b%'",
        "SELECT COUNT(*), SUM(id) FROM t WHERE s LIKE 'a_b'",
    ] {
        let (columnar, row, logs) = run_both(&db, sql);
        assert_columnar_filter_ran(&logs);
        assert_eq!(columnar, row, "NULL handling parity failed for: {sql}");
    }
}

/// Patterns longer than every row, empty-ish, and adjacent-wildcard edge cases
/// must all match the row path.
#[test]
fn general_like_edge_cases_match_row_path() {
    init_logger();

    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE t (id INTEGER, s TEXT)");
    let mut ins = String::new();
    let samples = ["", "a", "ab", "abc", "aabbcc", "xy", "underscore_here"];
    for i in 0..350usize {
        let s = samples[i % samples.len()];
        ins.push_str(&format!("INSERT INTO t VALUES ({i}, '{s}');"));
    }
    execute_sql(&mut db, &ins);

    let patterns = [
        "%_%",                   // at least one char
        "_%_",                   // at least two chars
        "a%%b",                  // adjacent %
        "a__%b",                 // multiple _ then %
        "____",                  // exactly four chars, no %
        "%abcdefghij_longtail%", // longer than every row -> length fast-reject
        "under_core\\_here",     // (backslash is literal here, no ESCAPE clause)
    ];
    for p in patterns {
        let sql = format!("SELECT COUNT(*), SUM(id) FROM t WHERE s LIKE '{p}'");
        let (columnar, row, logs) = run_both(&db, &sql);
        assert_columnar_filter_ran(&logs);
        assert_eq!(columnar, row, "edge-case parity failed for pattern={p:?}");
    }
}

/// Non-ASCII text: ASCII case folding must NOT fold non-ASCII characters, and
/// `_` must consume exactly one (possibly multi-byte) Unicode character.
#[test]
fn general_like_non_ascii_matches_row_path() {
    init_logger();

    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE t (id INTEGER, s TEXT)");
    let samples = ["café", "CAFÉ", "naïve", "Ωmega", "über", "aሴc", "a😀c", "cafe"];
    let mut ins = String::new();
    for i in 0..320usize {
        let s = samples[i % samples.len()];
        ins.push_str(&format!("INSERT INTO t VALUES ({i}, '{s}');"));
    }
    execute_sql(&mut db, &ins);

    // Only `General`-classified patterns (each has a `_` and/or multiple
    // interior `%`), so they route through the general matcher under test.
    // (Single `%suffix` / `%sub%` shapes classify as Suffix/Contains and take
    // different kernels, outside this issue's scope.)
    let patterns = ["caf_", "a_c", "_mega", "%ሴ%é%", "caf_%", "%é%café%"];
    for p in patterns {
        let sql = format!("SELECT COUNT(*), SUM(id) FROM t WHERE s LIKE '{p}'");
        let (columnar, row, logs) = run_both(&db, &sql);
        assert_columnar_filter_ran(&logs);
        assert_eq!(columnar, row, "non-ASCII parity failed for pattern={p:?}");
    }
}

/// Type affinity: LIKE against a BLOB column returns false (per #5070), matching
/// the row path — it must not error.
#[test]
fn general_like_blob_affinity_matches_row_path() {
    init_logger();

    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE b (id INTEGER, data BLOB)");
    let mut ins = String::new();
    for i in 0..200usize {
        ins.push_str(&format!("INSERT INTO b VALUES ({i}, x'61{:02x}62');", i % 256));
    }
    execute_sql(&mut db, &ins);

    let sql = "SELECT COUNT(*) FROM b WHERE data LIKE '%a%b%'";
    // BLOB LIKE may or may not route columnar; the essential guarantee is
    // result parity with the row path (and no error). Do not assert the path.
    let (columnar, row, _logs) = run_both(&db, sql);
    assert_eq!(columnar, row, "BLOB LIKE must match row path (returns false, not error)");
}
