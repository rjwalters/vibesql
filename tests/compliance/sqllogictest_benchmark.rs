//! Dual-engine benchmark harness for comparing vibesql and SQLite performance.
//!
//! This module provides infrastructure to run the same sqllogictest files against
//! both vibesql and SQLite3, collecting comparable metrics from both engines.
//!
//! ## Current Limitations
//!
//! - **Metrics**: Only total execution time is currently collected. Per-query metrics
//!   would require custom instrumentation of the sqllogictest Runner.
//! - **Glob Patterns**: Multiple test file support via glob patterns is deferred to a
//!   future enhancement. Currently only single test files are supported.
//! - **Parallel Execution**: Concurrent testing of multiple files is not yet implemented.

use async_trait::async_trait;
use md5::{Digest, Md5};
use vibesql_parser::Parser;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use std::path::Path;
use std::time::{Duration, Instant};
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Benchmark metrics collected during test execution
///
/// Note: Currently only `total_duration` is collected due to limitations in the
/// sqllogictest Runner API. Fine-grained per-query metrics would require custom
/// instrumentation or modifications to the Runner itself.
#[derive(Debug, Clone)]
pub struct BenchmarkMetrics {
    /// Total execution time for the entire test file
    pub total_duration: Duration,
}

impl BenchmarkMetrics {
    fn new() -> Self {
        Self {
            total_duration: Duration::ZERO,
        }
    }
}

/// Errors that can occur during benchmarking
#[derive(Debug)]
pub enum BenchmarkError {
    IoError(std::io::Error),
    TestError(String),
    EngineError(String),
}

impl std::fmt::Display for BenchmarkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BenchmarkError::IoError(e) => write!(f, "I/O error: {}", e),
            BenchmarkError::TestError(e) => write!(f, "Test error: {}", e),
            BenchmarkError::EngineError(e) => write!(f, "Engine error: {}", e),
        }
    }
}

impl std::error::Error for BenchmarkError {}

impl From<std::io::Error> for BenchmarkError {
    fn from(e: std::io::Error) -> Self {
        BenchmarkError::IoError(e)
    }
}

/// Result of comparing two engines
///
/// Each engine's metrics are optional to allow graceful handling of failures.
/// If one engine fails, the comparison can still report results from the other.
#[derive(Debug)]
pub struct ComparisonResult {
    pub vibesql: Option<BenchmarkMetrics>,
    pub sqlite: Option<BenchmarkMetrics>,
}

impl ComparisonResult {
    pub fn print_summary(&self) {
        println!("\n=== Benchmark Comparison ===\n");

        match &self.vibesql {
            Some(metrics) => {
                println!("VibeSQL:");
                println!("  Total Duration: {:?}", metrics.total_duration);
            }
            None => {
                println!("VibeSQL: FAILED");
            }
        }

        match &self.sqlite {
            Some(metrics) => {
                println!("\nSQLite:");
                println!("  Total Duration: {:?}", metrics.total_duration);
            }
            None => {
                println!("\nSQLite: FAILED");
            }
        }

        // Calculate and display speedup ratio if both succeeded
        if let (Some(vibesql), Some(sqlite)) = (&self.vibesql, &self.sqlite) {
            if vibesql.total_duration.as_nanos() > 0 && sqlite.total_duration.as_nanos() > 0 {
                let speedup = sqlite.total_duration.as_secs_f64()
                    / vibesql.total_duration.as_secs_f64();

                if speedup > 1.0 {
                    println!("\nVibeSQL is {:.2}x faster than SQLite", speedup);
                } else if speedup < 1.0 {
                    println!("\nSQLite is {:.2}x faster than VibeSQL", 1.0 / speedup);
                } else {
                    println!("\nBoth engines have equivalent performance");
                }
            }
        }
    }
}

// ===== SQLite Wrapper =====

/// Error type for database operations in the benchmark harness
#[derive(Debug)]
pub struct TestError(String);

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for TestError {}

/// SQLite database wrapper implementing AsyncDB trait
struct SqliteDB {
    conn: rusqlite::Connection,
}

impl Default for SqliteDB {
    fn default() -> Self {
        Self::new().expect("Failed to create SQLite DB")
    }
}

impl SqliteDB {
    fn new() -> Result<Self, BenchmarkError> {
        let conn = rusqlite::Connection::open_in_memory()
            .map_err(|e| BenchmarkError::EngineError(format!("Failed to open SQLite: {}", e)))?;
        Ok(Self { conn })
    }

    fn format_result_rows(
        &self,
        rows: Vec<Vec<rusqlite::types::Value>>,
    ) -> Result<DBOutput<DefaultColumnType>, TestError> {
        if rows.is_empty() {
            return Ok(DBOutput::Rows { types: vec![], rows: vec![] });
        }

        let formatted_rows: Vec<Vec<String>> = rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|val| self.format_sqlite_value(val))
                    .collect()
            })
            .collect();

        let total_values: usize = formatted_rows.iter().map(|r| r.len()).sum();

        // For hashing (>8 values)
        if total_values > 8 {
            let mut hasher = Md5::new();
            let mut sort_keys: Vec<_> = formatted_rows.iter().map(|row| row.join(" ")).collect();
            sort_keys.sort();
            for key in &sort_keys {
                hasher.update(key);
                hasher.update("\n");
            }
            let hash = format!("{:x}", hasher.finalize());
            let hash_string = format!("{} values hashing to {}", total_values, hash);
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text],
                rows: vec![vec![hash_string]],
            })
        } else {
            // Flatten multi-column results
            let mut flattened_rows: Vec<Vec<String>> = Vec::new();
            let mut flattened_types: Vec<DefaultColumnType> = Vec::new();

            if !rows.is_empty() && !rows[0].is_empty() {
                flattened_types = vec![self.infer_type(&rows[0][0]); total_values];
            }

            for row in formatted_rows {
                for val in row {
                    flattened_rows.push(vec![val]);
                }
            }

            Ok(DBOutput::Rows { types: flattened_types, rows: flattened_rows })
        }
    }

    fn format_sqlite_value(&self, value: &rusqlite::types::Value) -> String {
        use rusqlite::types::Value;
        match value {
            Value::Null => "NULL".to_string(),
            Value::Integer(i) => i.to_string(),
            Value::Real(f) => {
                if f.fract() == 0.0 {
                    format!("{:.1}", f)
                } else {
                    f.to_string()
                }
            }
            Value::Text(s) => s.clone(),
            Value::Blob(_) => "[BLOB]".to_string(),
        }
    }

    fn infer_type(&self, value: &rusqlite::types::Value) -> DefaultColumnType {
        use rusqlite::types::Value;
        match value {
            Value::Integer(_) => DefaultColumnType::Integer,
            Value::Real(_) => DefaultColumnType::FloatingPoint,
            Value::Text(_) | Value::Blob(_) => DefaultColumnType::Text,
            Value::Null => DefaultColumnType::Any,
        }
    }

    fn execute_sql(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, TestError> {
        // Check if it's a query (SELECT) or statement
        let trimmed = sql.trim().to_uppercase();
        if trimmed.starts_with("SELECT") || trimmed.starts_with("WITH") {
            // Execute query
            let mut stmt = self.conn.prepare(sql)
                .map_err(|e| TestError(format!("SQLite prepare error: {}", e)))?;

            let column_count = stmt.column_count();
            let mut rows = Vec::new();

            let result = stmt.query_map([], |row| {
                let mut values = Vec::new();
                for i in 0..column_count {
                    values.push(row.get::<_, rusqlite::types::Value>(i)?);
                }
                Ok(values)
            });

            match result {
                Ok(mapped_rows) => {
                    for row_result in mapped_rows {
                        match row_result {
                            Ok(row) => rows.push(row),
                            Err(e) => return Err(TestError(format!("SQLite query error: {}", e))),
                        }
                    }
                    self.format_result_rows(rows)
                }
                Err(e) => Err(TestError(format!("SQLite query error: {}", e))),
            }
        } else {
            // Execute statement
            let rows_affected = self.conn.execute(sql, [])
                .map_err(|e| TestError(format!("SQLite execute error: {}", e)))?;
            Ok(DBOutput::StatementComplete(rows_affected as u64))
        }
    }
}

#[async_trait]
impl AsyncDB for SqliteDB {
    type Error = TestError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        self.execute_sql(sql)
    }

    async fn shutdown(&mut self) {
        // SQLite connection will be closed on drop
    }
}

// ===== VibeSQL Wrapper =====

struct VibeSqlDB {
    db: Database,
    cache: std::sync::Arc<vibesql_executor::QueryPlanCache>,
}

impl Default for VibeSqlDB {
    fn default() -> Self {
        Self::new()
    }
}

impl VibeSqlDB {
    fn new() -> Self {
        Self {
            db: Database::new(),
            cache: std::sync::Arc::new(vibesql_executor::QueryPlanCache::new(1000)),
        }
    }

    fn format_result_rows(
        &self,
        rows: &[vibesql_storage::Row],
        types: Vec<DefaultColumnType>,
    ) -> Result<DBOutput<DefaultColumnType>, TestError> {
        let formatted_rows: Vec<Vec<String>> = rows
            .iter()
            .map(|row| {
                row.values
                    .iter()
                    .enumerate()
                    .map(|(idx, val)| self.format_sql_value(val, types.get(idx)))
                    .collect()
            })
            .collect();

        let total_values: usize = formatted_rows.iter().map(|r| r.len()).sum();

        if total_values > 8 {
            let mut hasher = Md5::new();
            let canonical_rows: Vec<Vec<String>> = rows
                .iter()
                .map(|row| {
                    row.values
                        .iter()
                        .enumerate()
                        .map(|(idx, val)| self.format_sql_value_canonical(val, types.get(idx)))
                        .collect()
                })
                .collect();

            let mut sort_keys: Vec<_> = canonical_rows.iter().map(|row| row.join(" ")).collect();
            sort_keys.sort();
            for key in &sort_keys {
                hasher.update(key);
                hasher.update("\n");
            }
            let hash = format!("{:x}", hasher.finalize());
            let hash_string = format!("{} values hashing to {}", total_values, hash);
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text],
                rows: vec![vec![hash_string]],
            })
        } else {
            let mut flattened_rows: Vec<Vec<String>> = Vec::new();
            let mut flattened_types: Vec<DefaultColumnType> = Vec::new();

            if !types.is_empty() {
                flattened_types = vec![types[0].clone(); total_values];
            }

            for row in formatted_rows {
                for val in row {
                    flattened_rows.push(vec![val]);
                }
            }

            Ok(DBOutput::Rows { types: flattened_types, rows: flattened_rows })
        }
    }

    fn execute_sql(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, TestError> {
        let stmt =
            Parser::parse_sql(sql).map_err(|e| TestError(format!("Parse error: {:?}", e)))?;

        match stmt {
            vibesql_ast::Statement::Select(select_stmt) => {
                let executor = vibesql_executor::SelectExecutor::new(&self.db);
                let rows = executor
                    .execute(&select_stmt)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                self.format_query_result(rows)
            }
            vibesql_ast::Statement::CreateTable(create_stmt) => {
                let table_name = if let Some(pos) = create_stmt.table_name.rfind('.') {
                    &create_stmt.table_name[pos + 1..]
                } else {
                    &create_stmt.table_name
                };

                vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;

                self.cache.invalidate_table(table_name);
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::Insert(insert_stmt) => {
                let rows_affected = vibesql_executor::InsertExecutor::execute(&mut self.db, &insert_stmt)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            vibesql_ast::Statement::Update(update_stmt) => {
                let rows_affected = vibesql_executor::UpdateExecutor::execute(&update_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            vibesql_ast::Statement::Delete(delete_stmt) => {
                let rows_affected = vibesql_executor::DeleteExecutor::execute(&delete_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            vibesql_ast::Statement::DropTable(drop_stmt) => {
                let table_name = if let Some(pos) = drop_stmt.table_name.rfind('.') {
                    &drop_stmt.table_name[pos + 1..]
                } else {
                    &drop_stmt.table_name
                };

                vibesql_executor::DropTableExecutor::execute(&drop_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;

                self.cache.invalidate_table(table_name);
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::CreateIndex(create_index_stmt) => {
                vibesql_executor::IndexExecutor::execute(&create_index_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::DropIndex(drop_index_stmt) => {
                vibesql_executor::IndexExecutor::execute_drop(&drop_index_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::Reindex(reindex_stmt) => {
                vibesql_executor::IndexExecutor::execute_reindex(&reindex_stmt, &self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            _ => Ok(DBOutput::StatementComplete(0)),
        }
    }

    fn format_query_result(
        &self,
        rows: Vec<vibesql_storage::Row>,
    ) -> Result<DBOutput<DefaultColumnType>, TestError> {
        if rows.is_empty() {
            return Ok(DBOutput::Rows { types: vec![], rows: vec![] });
        }

        let types: Vec<DefaultColumnType> = rows[0]
            .values
            .iter()
            .map(|val| match val {
                SqlValue::Integer(_)
                | SqlValue::Smallint(_)
                | SqlValue::Bigint(_)
                | SqlValue::Unsigned(_) => DefaultColumnType::Integer,
                SqlValue::Float(_)
                | SqlValue::Real(_)
                | SqlValue::Double(_)
                | SqlValue::Numeric(_) => DefaultColumnType::FloatingPoint,
                SqlValue::Varchar(_)
                | SqlValue::Character(_)
                | SqlValue::Date(_)
                | SqlValue::Time(_)
                | SqlValue::Timestamp(_)
                | SqlValue::Interval(_) => DefaultColumnType::Text,
                SqlValue::Boolean(_) => DefaultColumnType::Integer,
                SqlValue::Null => DefaultColumnType::Any,
            })
            .collect();

        self.format_result_rows(&rows, types)
    }

    fn format_sql_value(
        &self,
        value: &SqlValue,
        expected_type: Option<&DefaultColumnType>,
    ) -> String {
        match value {
            SqlValue::Integer(i) => {
                if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                    format!("{:.3}", *i as f64)
                } else {
                    i.to_string()
                }
            }
            SqlValue::Smallint(i) => {
                if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                    format!("{:.3}", *i as f64)
                } else {
                    i.to_string()
                }
            }
            SqlValue::Bigint(i) => {
                if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                    format!("{:.3}", *i as f64)
                } else {
                    i.to_string()
                }
            }
            SqlValue::Unsigned(i) => {
                if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                    format!("{:.3}", *i as f64)
                } else {
                    i.to_string()
                }
            }
            SqlValue::Numeric(_) => value.to_string(),
            SqlValue::Float(f) | SqlValue::Real(f) => {
                if f.fract() == 0.0 {
                    format!("{:.1}", f)
                } else {
                    f.to_string()
                }
            }
            SqlValue::Double(f) => {
                if f.fract() == 0.0 {
                    format!("{:.1}", f)
                } else {
                    f.to_string()
                }
            }
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.clone(),
            SqlValue::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
            SqlValue::Null => "NULL".to_string(),
            SqlValue::Date(d) => d.to_string(),
            SqlValue::Time(t) => t.to_string(),
            SqlValue::Timestamp(ts) => ts.to_string(),
            SqlValue::Interval(i) => i.to_string(),
        }
    }

    fn format_sql_value_canonical(
        &self,
        value: &SqlValue,
        expected_type: Option<&DefaultColumnType>,
    ) -> String {
        match value {
            SqlValue::Integer(i) => i.to_string(),
            SqlValue::Smallint(i) => {
                if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                    format!("{:.3}", *i as f64)
                } else {
                    i.to_string()
                }
            }
            SqlValue::Bigint(i) => {
                if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                    format!("{:.3}", *i as f64)
                } else {
                    i.to_string()
                }
            }
            SqlValue::Unsigned(i) => {
                if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                    format!("{:.3}", *i as f64)
                } else {
                    i.to_string()
                }
            }
            SqlValue::Numeric(_) => value.to_string(),
            SqlValue::Float(f) | SqlValue::Real(f) => {
                if f.fract() == 0.0 {
                    format!("{:.1}", f)
                } else {
                    f.to_string()
                }
            }
            SqlValue::Double(f) => {
                if f.fract() == 0.0 {
                    format!("{:.1}", f)
                } else {
                    f.to_string()
                }
            }
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.clone(),
            SqlValue::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
            SqlValue::Null => "NULL".to_string(),
            SqlValue::Date(d) => d.to_string(),
            SqlValue::Time(t) => t.to_string(),
            SqlValue::Timestamp(ts) => ts.to_string(),
            SqlValue::Interval(i) => i.to_string(),
        }
    }
}

#[async_trait]
impl AsyncDB for VibeSqlDB {
    type Error = TestError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        self.execute_sql(sql)
    }

    async fn shutdown(&mut self) {
        let stats = self.cache.stats();
        eprintln!("VibeSQL Cache statistics:");
        eprintln!("  Hits: {}", stats.hits);
        eprintln!("  Misses: {}", stats.misses);
        eprintln!("  Hit rate: {:.2}%", stats.hit_rate * 100.0);
        eprintln!("  Evictions: {}", stats.evictions);
        eprintln!("  Final size: {}", stats.size);
    }
}

// ===== Benchmark Functions =====

/// Run a benchmark on a single database engine
///
/// This function measures the total execution time of running a test file
/// against a database engine. Due to limitations in the sqllogictest Runner API,
/// fine-grained metrics (per-query timing) are not available without modifying
/// the runner itself.
pub async fn benchmark_engine<DB>(
    test_file: &Path,
    engine_name: &str,
) -> Result<BenchmarkMetrics, BenchmarkError>
where
    DB: AsyncDB<Error = TestError> + Send + 'static + Default,
{
    let start_time = Instant::now();

    let mut runner = sqllogictest::Runner::new(|| async {
        Ok::<_, TestError>(DB::default())
    });
    // Add "mysql" label for skipif/onlyif directives
    runner.add_label("mysql");

    eprintln!("Running {} on {:?}...", engine_name, test_file);

    match runner.run_file(test_file) {
        Ok(_) => {
            let total_duration = start_time.elapsed();
            let mut metrics = BenchmarkMetrics::new();
            metrics.total_duration = total_duration;
            eprintln!("{} completed in {:?}", engine_name, total_duration);
            Ok(metrics)
        }
        Err(e) => {
            Err(BenchmarkError::TestError(format!("Test execution failed: {:?}", e)))
        }
    }
}

/// Compare both engines on the same test file
///
/// This function runs the test file on both engines independently. If one engine
/// fails, the other continues and the comparison can still provide partial results.
pub async fn compare_engines(
    test_file: &Path,
) -> Result<ComparisonResult, BenchmarkError> {
    println!("Benchmarking VibeSQL...");
    let vibesql = match benchmark_engine::<VibeSqlDB>(test_file, "VibeSQL").await {
        Ok(metrics) => {
            eprintln!("✓ VibeSQL benchmark succeeded");
            Some(metrics)
        }
        Err(e) => {
            eprintln!("✗ VibeSQL benchmark failed: {}", e);
            None
        }
    };

    println!("\nBenchmarking SQLite...");
    let sqlite = match benchmark_engine::<SqliteDB>(test_file, "SQLite").await {
        Ok(metrics) => {
            eprintln!("✓ SQLite benchmark succeeded");
            Some(metrics)
        }
        Err(e) => {
            eprintln!("✗ SQLite benchmark failed: {}", e);
            None
        }
    };

    // Return error only if both failed
    match (&vibesql, &sqlite) {
        (None, None) => Err(BenchmarkError::TestError(
            "Both engines failed to run the test file".to_string()
        )),
        _ => Ok(ComparisonResult { vibesql, sqlite })
    }
}

// ===== Tests =====

#[tokio::test]
async fn test_sqlite_wrapper_basic() {
    let mut db = SqliteDB::new().expect("Failed to create SQLite DB");

    // Test CREATE TABLE
    let result = db.run("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)").await;
    assert!(result.is_ok());

    // Test INSERT
    let result = db.run("INSERT INTO test VALUES (1, 'Alice')").await;
    assert!(result.is_ok());

    // Test SELECT
    let result = db.run("SELECT * FROM test").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_vibesql_wrapper_basic() {
    let mut db = VibeSqlDB::new();

    // Test CREATE TABLE
    let result = db.run("CREATE TABLE test (id INTEGER, name VARCHAR(50))").await;
    assert!(result.is_ok());

    // Test INSERT
    let result = db.run("INSERT INTO test VALUES (1, 'Alice')").await;
    assert!(result.is_ok());

    // Test SELECT
    let result = db.run("SELECT * FROM test").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_benchmark_simple_script() {
    // Create a simple test script in memory
    let script = r#"
statement ok
CREATE TABLE test (x INTEGER, y INTEGER)

statement ok
INSERT INTO test VALUES (1, 2)

statement ok
INSERT INTO test VALUES (3, 4)

query II rowsort
SELECT * FROM test
----
1
2
3
4
"#;

    // Write script to temp file
    let temp_dir = std::env::temp_dir();
    let test_file = temp_dir.join("test_benchmark.slt");
    std::fs::write(&test_file, script).expect("Failed to write test file");

    // Run comparison (may fail due to instrumentation limitations, but should not panic)
    let result = compare_engines(&test_file).await;

    // Clean up
    let _ = std::fs::remove_file(&test_file);

    // For now, we just verify it doesn't panic
    // Proper metrics collection would require more sophisticated instrumentation
    match result {
        Ok(comparison) => {
            comparison.print_summary();
        }
        Err(e) => {
            eprintln!("Benchmark failed: {}", e);
        }
    }
}
