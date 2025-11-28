//! Core Runner struct and configuration.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use futures::executor::block_on;

use crate::error_handling::TestError;
use crate::output::{RecordOutput, DBOutput, Normalizer, ColumnTypeValidator, Validator};
use crate::parser::*;
use crate::substitution::Substitution;
use crate::{ColumnType, Connections, MakeConnection};
use crate::error_handling::AnyError;

/// The async database to be tested.
#[async_trait]
pub trait AsyncDB {
    /// The error type of SQL execution.
    type Error: std::error::Error + Send + Sync + 'static;
    /// The type of result columns
    type ColumnType: ColumnType;

    /// Async run a SQL query and return the output.
    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error>;

    /// Shutdown the connection gracefully.
    async fn shutdown(&mut self);

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        ""
    }

    /// [`Runner`] calls this function to perform sleep.
    ///
    /// The default implementation is `std::thread::sleep`, which is universal to any async runtime
    /// but would block the current thread. If you are running in tokio runtime, you should override
    /// this by `tokio::time::sleep`.
    async fn sleep(dur: Duration) {
        std::thread::sleep(dur);
    }

    /// [`Runner`] calls this function to run a system command.
    ///
    /// The default implementation is `std::process::Command::output`, which is universal to any
    /// async runtime but would block the current thread. If you are running in tokio runtime, you
    /// should override this by `tokio::process::Command::output`.
    async fn run_command(mut command: Command) -> std::io::Result<std::process::Output> {
        command.output()
    }
}

/// The database to be tested.
pub trait DB {
    /// The error type of SQL execution.
    type Error: std::error::Error + Send + Sync + 'static;
    /// The type of result columns
    type ColumnType: ColumnType;

    /// Run a SQL query and return the output.
    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error>;

    /// Shutdown the connection gracefully.
    fn shutdown(&mut self) {}

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        ""
    }
}

/// Compat-layer for the new AsyncDB and DB trait
#[async_trait]
impl<D> AsyncDB for D
where
    D: DB + Send,
{
    type Error = D::Error;
    type ColumnType = D::ColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        D::run(self, sql)
    }

    async fn shutdown(&mut self) {
        D::shutdown(self);
    }

    fn engine_name(&self) -> &str {
        D::engine_name(self)
    }
}

/// Decide whether a test file should be run. Useful for partitioning tests into multiple
/// parallel machines to speed up test runs.
pub trait Partitioner: Send + Sync + 'static {
    /// Returns true if the given file name matches the partition and should be run.
    fn matches(&self, file_name: &str) -> bool;
}

impl<F> Partitioner for F
where
    F: Fn(&str) -> bool + Send + Sync + 'static,
{
    fn matches(&self, file_name: &str) -> bool {
        self(file_name)
    }
}

/// The default partitioner matches all files.
pub fn default_partitioner(_file_name: &str) -> bool {
    true
}

#[derive(Default)]
pub struct RunnerLocals {
    /// The temporary directory. Test cases can use `__TEST_DIR__` to refer to this directory.
    /// Lazily initialized and cleaned up when dropped.
    test_dir: OnceLock<tempfile::TempDir>,
    /// Runtime variables for substitution.
    variables: BTreeMap<String, String>,
}

impl RunnerLocals {
    pub fn test_dir(&self) -> String {
        let test_dir = self
            .test_dir
            .get_or_init(|| tempfile::TempDir::new().expect("failed to create testdir"));
        test_dir.path().to_string_lossy().into_owned()
    }

    pub(super) fn set_var(&mut self, key: String, value: String) {
        self.variables.insert(key, value);
    }

    pub fn get_var(&self, key: &str) -> Option<&String> {
        self.variables.get(key)
    }

    pub fn vars(&self) -> &BTreeMap<String, String> {
        &self.variables
    }
}

/// Supported SQL dialects for auto-switching.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SqlDialect {
    /// MySQL 8.0+ compatibility mode (default)
    #[default]
    MySQL,
    /// SQLite 3 compatibility mode
    SQLite,
}

impl SqlDialect {
    /// Get the SQL mode string for SET SQL_MODE command.
    ///
    /// The dolthub/sqllogictest test corpus was regenerated against MySQL 8.x
    /// (see third_party/sqllogictest/README.md), so MySQL mode uses full MySQL
    /// semantics including decimal division (INTEGER / INTEGER → DECIMAL).
    pub fn as_sql_mode_str(&self) -> &'static str {
        match self {
            SqlDialect::MySQL => "mysql",
            SqlDialect::SQLite => "sqlite",
        }
    }
}

/// Statistics tracking tests executed per SQL dialect.
#[derive(Debug, Clone, Default)]
pub struct DialectStats {
    /// Number of tests that passed in MySQL mode.
    pub mysql_passed: usize,
    /// Number of tests that failed in MySQL mode.
    pub mysql_failed: usize,
    /// Number of tests that passed in SQLite mode.
    pub sqlite_passed: usize,
    /// Number of tests that failed in SQLite mode.
    pub sqlite_failed: usize,
}

impl DialectStats {
    /// Create new empty dialect stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a test result for the given dialect.
    pub fn record(&mut self, dialect: &SqlDialect, passed: bool) {
        match (dialect, passed) {
            (SqlDialect::MySQL, true) => self.mysql_passed += 1,
            (SqlDialect::MySQL, false) => self.mysql_failed += 1,
            (SqlDialect::SQLite, true) => self.sqlite_passed += 1,
            (SqlDialect::SQLite, false) => self.sqlite_failed += 1,
        }
    }

    /// Total tests run in MySQL mode.
    pub fn mysql_total(&self) -> usize {
        self.mysql_passed + self.mysql_failed
    }

    /// Total tests run in SQLite mode.
    pub fn sqlite_total(&self) -> usize {
        self.sqlite_passed + self.sqlite_failed
    }

    /// Total tests run across all dialects.
    pub fn total(&self) -> usize {
        self.mysql_total() + self.sqlite_total()
    }

    /// Merge another DialectStats into this one.
    pub fn merge(&mut self, other: &DialectStats) {
        self.mysql_passed += other.mysql_passed;
        self.mysql_failed += other.mysql_failed;
        self.sqlite_passed += other.sqlite_passed;
        self.sqlite_failed += other.sqlite_failed;
    }
}

/// Sqllogictest runner.
pub struct Runner<D: AsyncDB, M: MakeConnection<Conn = D>> {
    pub(super) conn: Connections<D, M>,
    // validator is used for validate if the result of query equals to expected.
    pub(super) validator: Validator,
    // normalizer is used to normalize the result text
    pub(super) normalizer: Normalizer,
    pub(super) column_type_validator: ColumnTypeValidator<D::ColumnType>,
    pub(super) partitioner: Arc<dyn Partitioner>,
    pub(super) substitution_on: bool,
    pub(super) sort_mode: Option<SortMode>,
    pub(super) result_mode: Option<ResultMode>,
    /// 0 means never hashing
    pub(super) hash_threshold: usize,
    /// Labels for condition `skipif` and `onlyif`.
    pub(super) labels: HashSet<String>,
    /// Local variables/context for the runner.
    pub(super) locals: RunnerLocals,
    /// Whether to automatically switch SQL dialect based on skipif/onlyif conditions.
    /// When enabled, instead of skipping tests with `skipif mysql` or `skipif sqlite`,
    /// the harness switches to the appropriate dialect and runs the test.
    pub(super) auto_switch_dialect: bool,
    /// Current SQL dialect being used.
    pub(super) current_dialect: SqlDialect,
    /// Statistics tracking tests executed per dialect.
    pub(super) dialect_stats: DialectStats,
}

impl<D: AsyncDB, M: MakeConnection<Conn = D>> Runner<D, M> {
    /// Create a new test runner on the database, with the given connection maker.
    ///
    /// See [`MakeConnection`] for more details.
    pub fn new(make_conn: M) -> Self {
        Runner {
            validator: crate::output::default_validator,
            normalizer: crate::output::default_normalizer,
            column_type_validator: crate::output::default_column_validator,
            partitioner: Arc::new(default_partitioner),
            substitution_on: false,
            sort_mode: None,
            result_mode: None,
            hash_threshold: 0,
            labels: HashSet::new(),
            conn: Connections::new(make_conn),
            locals: RunnerLocals::default(),
            auto_switch_dialect: false,
            current_dialect: SqlDialect::default(),
            dialect_stats: DialectStats::default(),
        }
    }

    /// Add a label for condition `skipif` and `onlyif`.
    pub fn add_label(&mut self, label: &str) {
        self.labels.insert(label.to_string());
    }

    /// Enable automatic SQL dialect switching based on skipif/onlyif conditions.
    ///
    /// When enabled, instead of skipping tests with `skipif mysql` or `skipif sqlite`,
    /// the harness automatically switches to the appropriate dialect and runs the test.
    ///
    /// This allows the test harness to run all tests regardless of dialect, maximizing
    /// test coverage.
    ///
    /// # Example
    /// ```ignore
    /// let mut tester = Runner::new(make_conn);
    /// tester.enable_auto_switch_dialect();
    ///
    /// // Tests with `skipif mysql` will now run in sqlite mode
    /// // Tests with `skipif sqlite` will now run in mysql mode
    /// ```
    pub fn enable_auto_switch_dialect(&mut self) {
        self.auto_switch_dialect = true;
    }

    /// Disable automatic SQL dialect switching.
    pub fn disable_auto_switch_dialect(&mut self) {
        self.auto_switch_dialect = false;
    }

    /// Set the default SQL dialect for the runner.
    pub fn with_default_dialect(&mut self, dialect: SqlDialect) {
        self.current_dialect = dialect;
    }

    /// Get the current SQL dialect.
    pub fn current_dialect(&self) -> &SqlDialect {
        &self.current_dialect
    }

    /// Check if auto dialect switching is enabled.
    pub fn is_auto_switch_dialect_enabled(&self) -> bool {
        self.auto_switch_dialect
    }

    /// Get the dialect statistics (tests run per dialect).
    pub fn dialect_stats(&self) -> &DialectStats {
        &self.dialect_stats
    }

    /// Get a mutable reference to dialect statistics.
    pub fn dialect_stats_mut(&mut self) -> &mut DialectStats {
        &mut self.dialect_stats
    }

    /// Reset dialect statistics to zero.
    pub fn reset_dialect_stats(&mut self) {
        self.dialect_stats = DialectStats::default();
    }

    /// Set a local variable for substitution.
    pub fn set_var(&mut self, key: String, value: String) {
        self.locals.set_var(key, value);
    }

    pub fn with_normalizer(&mut self, normalizer: Normalizer) {
        self.normalizer = normalizer;
    }
    pub fn with_validator(&mut self, validator: Validator) {
        self.validator = validator;
    }

    pub fn with_column_validator(&mut self, validator: ColumnTypeValidator<D::ColumnType>) {
        self.column_type_validator = validator;
    }

    /// Set the partitioner for the runner. Only files that match the partitioner will be run.
    ///
    /// This only takes effect when running tests in parallel.
    pub fn with_partitioner(&mut self, partitioner: impl Partitioner + 'static) {
        self.partitioner = Arc::new(partitioner);
    }

    pub fn with_hash_threshold(&mut self, hash_threshold: usize) {
        self.hash_threshold = hash_threshold;
    }

    /// Run a single record with retry logic.
    ///
    /// Returns the output of the record if successful.
    pub async fn run_async(
        &mut self,
        record: Record<D::ColumnType>,
    ) -> Result<RecordOutput<D::ColumnType>, TestError> {
        let retry = match &record {
            Record::Statement { retry, .. } => retry.clone(),
            Record::Query { retry, .. } => retry.clone(),
            Record::System { retry, .. } => retry.clone(),
            _ => None,
        };
        if retry.is_none() {
            return super::validator::run_async_no_retry(self, record).await;
        }

        // Retry for `retry.attempts` times. The parser ensures that `retry.attempts` must > 0.
        let retry = retry.unwrap();
        let mut last_error = None;
        for _ in 0..retry.attempts {
            let result = super::validator::run_async_no_retry(self, record.clone()).await;
            if result.is_ok() {
                return result;
            }
            tracing::warn!(target:"sqllogictest::retry", backoff = ?retry.backoff, error = ?result, "retrying");
            D::sleep(retry.backoff).await;
            last_error = result.err();
        }

        Err(last_error.unwrap())
    }

    /// Run a single record.
    ///
    /// Returns the output of the record if successful.
    pub fn run(
        &mut self,
        record: Record<D::ColumnType>,
    ) -> Result<RecordOutput<D::ColumnType>, TestError> {
        block_on(self.run_async(record))
    }

    /// Run multiple records.
    ///
    /// The runner will stop early once a halt record is seen.
    ///
    /// To acquire the result of each record, manually call `run_async` for each record instead.
    pub async fn run_multi_async(
        &mut self,
        records: impl IntoIterator<Item = Record<D::ColumnType>>,
    ) -> Result<(), TestError> {
        for record in records.into_iter() {
            if let Record::Halt { .. } = record {
                break;
            }
            self.run_async(record).await?;
        }
        Ok(())
    }

    /// Run multiple records.
    ///
    /// The runner will stop early once a halt record is seen.
    ///
    /// To acquire the result of each record, manually call `run` for each record instead.
    pub fn run_multi(
        &mut self,
        records: impl IntoIterator<Item = Record<D::ColumnType>>,
    ) -> Result<(), TestError> {
        block_on(self.run_multi_async(records))
    }

    /// Run a sqllogictest script.
    pub async fn run_script_async(&mut self, script: &str) -> Result<(), TestError> {
        let records = crate::parser::parse(script).expect("failed to parse sqllogictest");
        self.run_multi_async(records).await
    }

    /// Run a sqllogictest script with a given script name.
    pub async fn run_script_with_name_async(
        &mut self,
        script: &str,
        name: impl Into<Arc<str>>,
    ) -> Result<(), TestError> {
        let records = crate::parser::parse_with_name(script, name).expect("failed to parse sqllogictest");
        self.run_multi_async(records).await
    }

    /// Run a sqllogictest file.
    pub async fn run_file_async(&mut self, filename: impl AsRef<Path>) -> Result<(), TestError> {
        let records = crate::parser::parse_file(filename)?;
        self.run_multi_async(records).await
    }

    /// Run a sqllogictest script.
    pub fn run_script(&mut self, script: &str) -> Result<(), TestError> {
        block_on(self.run_script_async(script))
    }

    /// Run a sqllogictest script with a given script name.
    pub fn run_script_with_name(
        &mut self,
        script: &str,
        name: impl Into<Arc<str>>,
    ) -> Result<(), TestError> {
        block_on(self.run_script_with_name_async(script, name))
    }

    /// Run a sqllogictest file.
    pub fn run_file(&mut self, filename: impl AsRef<Path>) -> Result<(), TestError> {
        block_on(self.run_file_async(filename))
    }

    /// Substitute the input SQL or command with [`Substitution`], if enabled by `control
    /// substitution`.
    ///
    /// If `subst_env_vars`, we will use the `subst` crate to support extensive substitutions, incl.
    /// `$NAME`, `${NAME}`, `${NAME:default}`. The cost is that we will have to use escape
    /// characters, e.g., `\$` & `\\`.
    ///
    /// Otherwise, we just do simple string substitution for `__TEST_DIR__` and `__NOW__`.
    /// This is useful for `system` commands: The shell can do the environment variables, and we can
    /// write strings like `\n` without escaping.
    pub(super) fn may_substitute(&self, input: String, subst_env_vars: bool) -> Result<String, AnyError> {
        if self.substitution_on {
            Substitution::new(&self.locals, subst_env_vars)
                .substitute(&input)
                .map_err(|e| Arc::new(e) as AnyError)
        } else {
            Ok(input)
        }
    }
}

impl<D: AsyncDB, M: MakeConnection<Conn = D>> Runner<D, M> {
    /// Shutdown all connections in the runner.
    pub async fn shutdown_async(&mut self) {
        tracing::debug!("shutting down runner...");
        self.conn.shutdown_all().await;
    }

    /// Shutdown all connections in the runner.
    pub fn shutdown(&mut self) {
        block_on(self.shutdown_async());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_dialect_default() {
        assert_eq!(SqlDialect::default(), SqlDialect::MySQL);
    }

    #[test]
    fn test_sql_dialect_as_sql_mode_str() {
        // MySQL uses full MySQL mode (decimal division) since dolthub/sqllogictest
        // was regenerated against MySQL 8.x
        assert_eq!(SqlDialect::MySQL.as_sql_mode_str(), "mysql");
        assert_eq!(SqlDialect::SQLite.as_sql_mode_str(), "sqlite");
    }

    #[test]
    fn test_sql_dialect_equality() {
        assert_eq!(SqlDialect::MySQL, SqlDialect::MySQL);
        assert_eq!(SqlDialect::SQLite, SqlDialect::SQLite);
        assert_ne!(SqlDialect::MySQL, SqlDialect::SQLite);
    }

    #[test]
    fn test_sql_dialect_clone() {
        let dialect = SqlDialect::SQLite;
        let cloned = dialect.clone();
        assert_eq!(dialect, cloned);
    }
}
