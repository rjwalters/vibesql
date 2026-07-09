//! Common test utilities for executor tests

pub mod insert_constraint_fixtures;

use vibesql_executor::ExpressionEvaluator;

// ---------------------------------------------------------------------------
// Columnar path-assertion log-capture harness (shared by the `columnar_*`
// path-assertion / parity test binaries).
//
// IMPORTANT: `tests/common/mod.rs` is compiled independently into every test
// binary that declares `mod common;`. Each test binary therefore gets its OWN
// private copy of the `LOG_BUFFER`, `LOGGER`, `CAPTURE_LEVEL`, and `SERIAL`
// statics below. This is the whole point of the "one path-assertion test per
// binary" design: the process-global `log` logger and the process-global
// `VIBESQL_DISABLE_COLUMNAR*` env vars never clash across binaries, and the
// `SERIAL` mutex serializes the columnar-vs-row runs within a single binary so
// env-var toggling and log capture do not race between that binary's tests.
// ---------------------------------------------------------------------------

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

use log::{Level, LevelFilter, Log, Metadata, Record};
use vibesql_storage::Database;

/// Serializes the columnar-vs-row runs across tests within a single binary.
/// `VIBESQL_DISABLE_COLUMNAR` / `VIBESQL_DISABLE_COLUMNAR_JOIN` are process-global
/// env vars and the `log` capture buffer is process-global, so concurrent tests
/// would otherwise race: one test toggling the disable env var for its row-path
/// parity run could silently disable columnar in another test's columnar run
/// (failing that test's path assertion), and their captured logs would
/// interleave. Every path-assertion test either toggles the env var or reads the
/// shared buffer, so each acquires this mutex for its whole body. Poisoned-lock
/// recovery (`unwrap_or_else(|e| e.into_inner())`) keeps a panic in one test from
/// cascading into spurious failures in the rest.
#[allow(dead_code)] // Test helper - not every path-assertion binary toggles env vars.
pub static SERIAL: Mutex<()> = Mutex::new(());

/// Captures every log message at-or-below the configured level into a shared
/// buffer. `log`'s `Level` ordering is `Error(1) < Warn(2) < Info(3) < Debug(4)
/// < Trace(5)`, so `record.level() <= capture_upto` captures the configured
/// level and everything more severe.
struct CaptureLogger;

static LOG_BUFFER: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

/// The configured capture level, stored as `Level as usize` (1..=5). Defaults to
/// `Level::Debug` (4) until `init_logger` sets it. Runtime-configurable so Info-
/// level and Debug-level callers share one `CaptureLogger` implementation.
static CAPTURE_LEVEL: AtomicUsize = AtomicUsize::new(Level::Debug as usize);

#[allow(dead_code)] // Test helper - available for all test modules.
fn buffer() -> &'static Mutex<Vec<String>> {
    LOG_BUFFER.get_or_init(|| Mutex::new(Vec::new()))
}

impl Log for CaptureLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }
    fn log(&self, record: &Record) {
        if (record.level() as usize) <= CAPTURE_LEVEL.load(Ordering::Relaxed) {
            buffer().lock().unwrap().push(format!("{}", record.args()));
        }
    }
    fn flush(&self) {}
}

static LOGGER: CaptureLogger = CaptureLogger;

/// Install the process-global capture logger for this test binary and set the
/// level below which messages are captured (`Level::Info` or `Level::Debug`).
/// `set_logger` is idempotent across the binary; the "already set" error is
/// ignored so every test may call this. The capture level is refreshed on each
/// call, so all tests in a binary should pass the same level.
#[allow(dead_code)] // Test helper - available for all test modules.
pub fn init_logger(capture_upto: Level) {
    CAPTURE_LEVEL.store(capture_upto as usize, Ordering::Relaxed);
    let _ = log::set_logger(&LOGGER);
    log::set_max_level(LevelFilter::Debug);
}

/// Drain and return every captured log line since the last call.
#[allow(dead_code)] // Test helper - available for all test modules.
pub fn take_logs() -> Vec<String> {
    std::mem::take(&mut *buffer().lock().unwrap())
}

/// Execute a `;`-separated batch of `CREATE TABLE` / `INSERT` statements against
/// `db`, panicking on any parse or execution error. Used to set up fixtures for
/// the columnar path-assertion tests.
#[allow(dead_code)] // Test helper - available for all test modules.
pub fn execute_sql(db: &mut Database, sql: &str) {
    use vibesql_executor::{CreateTableExecutor, InsertExecutor};
    use vibesql_parser::Parser;

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

/// Parse and execute a `SELECT`, returning the raw storage rows. Panics on a
/// parse or execution error, or if `sql` is not a `SELECT`.
#[allow(dead_code)] // Test helper - available for all test modules.
pub fn run_select(db: &Database, sql: &str) -> Vec<vibesql_storage::Row> {
    use vibesql_parser::Parser;

    let stmt = Parser::parse_sql(sql).expect("Failed to parse SELECT");
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        vibesql_executor::SelectExecutor::new(db).execute(&select_stmt).expect("SELECT failed")
    } else {
        panic!("Expected SELECT");
    }
}

/// Like [`run_select`], but projects each row to its `Vec<SqlValue>` so callers
/// can compare result sets by value (`Vec<Vec<SqlValue>>`).
#[allow(dead_code)] // Test helper - available for all test modules.
pub fn run_select_values(db: &Database, sql: &str) -> Vec<Vec<vibesql_types::SqlValue>> {
    run_select(db, sql).into_iter().map(|r| r.values.to_vec()).collect()
}

/// Creates a test evaluator with a simple schema for testing.
/// Returns an evaluator and a simple test row.
#[allow(dead_code)] // Test helper - available for all test modules
pub fn create_test_evaluator() -> (ExpressionEvaluator<'static>, vibesql_storage::Row) {
    let schema = Box::leak(Box::new(vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "id".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    )));

    let evaluator = ExpressionEvaluator::new(schema);
    let row = vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]);

    (evaluator, row)
}

/// Sets up the standard employees test table with sample data.
/// This table is used across multiple update test files.
#[allow(dead_code)] // Test helper - available for all test modules
pub fn setup_test_table(db: &mut vibesql_storage::Database) {
    // Create table schema
    let schema = vibesql_catalog::TableSchema::new(
        "employees".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "salary".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
            vibesql_catalog::ColumnSchema::new(
                "department".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            vibesql_types::SqlValue::Integer(45000),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            vibesql_types::SqlValue::Integer(48000),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Engineering")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
            vibesql_types::SqlValue::Integer(42000),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Sales")),
        ]),
    )
    .unwrap();
}

/// Sets up a simple users test table with just id and name columns.
/// This table is used across multiple insert and transaction test files.
#[allow(dead_code)] // Test helper - available for all test modules
pub fn setup_users_table(db: &mut vibesql_storage::Database) {
    // CREATE TABLE users (id INT, name VARCHAR(50))
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
}

/// Sets up a users test table with id, name, and active columns.
/// This table is used across delete test files.
#[allow(dead_code)] // Test helper - available for all test modules
pub fn setup_users_table_with_active(db: &mut vibesql_storage::Database) {
    // CREATE TABLE users (id INT, name VARCHAR(50), active BOOLEAN)
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "active".to_string(),
                vibesql_types::DataType::Boolean,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            vibesql_types::SqlValue::Boolean(true),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            vibesql_types::SqlValue::Boolean(false),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
            vibesql_types::SqlValue::Boolean(true),
        ]),
    )
    .unwrap();
}

/// Sets up a timestamps test table for timestamp format tests.
/// Returns the table name on success, or an error message.
#[allow(dead_code)] // Test helper - available for all test modules
pub fn setup_timestamps_table(db: &mut vibesql_storage::Database) -> Result<String, String> {
    use vibesql_ast::Statement;
    use vibesql_executor::CreateTableExecutor;
    use vibesql_parser::Parser;

    let sql = "CREATE TABLE timestamps (id INTEGER, ts TIMESTAMP)";
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        Statement::CreateTable(create_stmt) => CreateTableExecutor::execute(&create_stmt, db)
            .map_err(|e| format!("Execution error: {:?}", e)),
        other => Err(format!("Expected CREATE TABLE statement, got {:?}", other)),
    }
}
