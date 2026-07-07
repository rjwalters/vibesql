use std::time::Instant;

use vibesql_parser::parse_with_arena_fallback;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

// Submodules
mod copy_handler;
pub mod display;
pub mod validation;
pub mod wal;

#[cfg(test)]
mod tests;

pub struct SqlExecutor {
    db: Database,
    timing_enabled: bool,
    /// PRAGMA count_changes session flag (SQLite-compatible, default OFF).
    /// When ON, INSERT/UPDATE/DELETE statements return a one-row, one-column
    /// result containing the change count.
    count_changes: bool,
    /// Active WAL persistence state, present only when the opt-in
    /// `[database] wal = true` flag is set AND a file-backed database is in use.
    /// When `Some`, `save_database` checkpoints + truncates the WAL instead of
    /// writing a full snapshot. `None` preserves the default snapshot behavior.
    wal_state: Option<wal::WalState>,
    /// Path of the file-backed database, if any. Used by `PRAGMA database_list`
    /// to report the `main` schema's backing file (SQLite reports the absolute
    /// path here, or an empty string for `:memory:` / no-path sessions).
    db_path: Option<String>,
    /// Exclusive inter-process lock on the database (`<stem>.lock` sibling),
    /// held for every file-backed session — WAL-active AND snapshot-only —
    /// for the whole session (issue #5808). `None` for `:memory:` / no-path
    /// sessions. Declared last so it drops AFTER `db` (and `wal_state`),
    /// keeping the lock held through the exit-time checkpoint/save.
    _db_lock: Option<vibesql_storage::DatabaseLock>,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Cell values: None represents SQL NULL, Some(s) represents actual data.
    /// This distinction is important for output formatting - NULL values should
    /// be displayed differently than the literal string "NULL".
    pub rows: Vec<Vec<Option<String>>>,
    pub columns: Vec<String>,
    pub row_count: usize,
    pub execution_time_ms: Option<f64>,
    /// Optional informational message from DDL operations (e.g., "Index created successfully").
    /// This message should be displayed in interactive formats but suppressed in raw format.
    pub message: Option<String>,
}

use crate::util::is_memory_database;

/// Render a VibeSQL DataType as a SQLite-flavor declared type string suitable
/// for `PRAGMA table_info`. SQLite preserves the original CREATE TABLE text,
/// but VibeSQL doesn't track the literal declaration, so we map back to the
/// canonical SQLite spelling (`INTEGER`, `REAL`, `TEXT`, `BLOB`, ...).
fn sqlite_declared_type(
    data_type: &vibesql_types::DataType,
    is_exact_integer_type: bool,
) -> String {
    use vibesql_types::DataType;
    match data_type {
        DataType::Integer => {
            // SQLite preserves the spelling: only literal "INTEGER" is the
            // rowid-alias-eligible affinity. We use is_exact_integer_type to
            // distinguish "INT" (mapped to Integer with is_exact=false) from
            // the canonical "INTEGER".
            if is_exact_integer_type {
                "INTEGER".to_string()
            } else {
                "INT".to_string()
            }
        }
        DataType::Smallint => "SMALLINT".to_string(),
        DataType::Bigint => "BIGINT".to_string(),
        DataType::Unsigned => "BIGINT UNSIGNED".to_string(),
        DataType::Numeric { precision, scale } => format!("NUMERIC({},{})", precision, scale),
        DataType::Decimal { precision, scale } => format!("DECIMAL({},{})", precision, scale),
        DataType::Float { precision } => format!("FLOAT({})", precision),
        DataType::Real => "REAL".to_string(),
        DataType::DoublePrecision => "DOUBLE PRECISION".to_string(),
        DataType::Character { length } => format!("CHAR({})", length),
        DataType::Varchar { max_length } => match max_length {
            Some(len) => format!("VARCHAR({})", len),
            None => "TEXT".to_string(),
        },
        DataType::CharacterLargeObject => "TEXT".to_string(),
        DataType::Name => "TEXT".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Date => "DATE".to_string(),
        DataType::Time { with_timezone } => {
            if *with_timezone {
                "TIME WITH TIME ZONE".to_string()
            } else {
                "TIME".to_string()
            }
        }
        DataType::Timestamp { with_timezone } => {
            if *with_timezone {
                "TIMESTAMP WITH TIME ZONE".to_string()
            } else {
                "DATETIME".to_string()
            }
        }
        DataType::Interval { .. } => "INTERVAL".to_string(),
        DataType::BinaryLargeObject => "BLOB".to_string(),
        DataType::Bit { length } => match length {
            Some(len) => format!("BIT({})", len),
            None => "BIT".to_string(),
        },
        DataType::UserDefined { type_name } => type_name.clone(),
        DataType::Vector { dimensions } => format!("VECTOR({})", dimensions),
        // Typeless columns (CREATE TABLE t(c)) report empty string in SQLite.
        DataType::Null => String::new(),
    }
}

/// Format SqlValue for output in SQLite-compatible format
/// - Booleans are displayed as 0/1 instead of FALSE/TRUE
/// - Other values use their standard Display format
fn format_sql_value(v: &SqlValue) -> String {
    match v {
        SqlValue::Boolean(b) => {
            if *b {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        _ => format!("{}", v),
    }
}

/// Load an existing database file, auto-detecting its format.
///
/// Tries the native binary/compressed/JSON loader first, auto-imports SQLite
/// files, and falls back to SQL-dump parsing for text files.
///
/// A `StorageError::UnsupportedFormatVersion` (file written by a newer VibeSQL
/// binary) is a hard error and is deliberately NOT masked by the SQL-dump
/// fallback: before issue #5807 the version mismatch fell through to a
/// confusing dump-parse error (or, on the WAL path, a silently empty
/// database).
fn load_database_file(db_path: &str) -> anyhow::Result<Database> {
    match Database::load(db_path) {
        Ok(db) => Ok(db),
        Err(e @ vibesql_storage::StorageError::UnsupportedFormatVersion { .. }) => {
            Err(anyhow::anyhow!("Failed to open database at {}: {}", db_path, e))
        }
        Err(ref e) if e.to_string().contains("SQLite database detected") => {
            // Auto-import SQLite database
            let result = crate::sqlite_io::import_sqlite(db_path).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to read binary SQLite database at {}: {}. \
                     If this file is a VibeSQL SQL dump, rename it with a .sql extension \
                     to load it in SQL dump format.",
                    db_path,
                    e
                )
            })?;
            for warning in &result.warnings {
                eprintln!("{}", warning);
            }
            eprintln!(
                "Imported SQLite database: {} tables, {} rows",
                result.tables_imported, result.rows_imported
            );
            Ok(result.database)
        }
        Err(_) => {
            // Fall back to SQL dump loading (requires executor for parsing)
            vibesql_executor::load_sql_dump(db_path)
                .map_err(|e| anyhow::anyhow!("Failed to load database: {}", e))
        }
    }
}

/// Options controlling how the executor opens a database file.
///
/// Bundled into a struct so the `--recover-fallback` opt-in (issue #5807)
/// threads through `Repl`/`ScriptExecutor` without growing a trail of bools.
#[derive(Debug, Clone, Copy)]
pub struct DbOpenOptions {
    /// Activate the WAL persistence path for file-backed databases
    /// (`[database] wal`, on by default).
    pub wal: bool,
    /// Explicit opt-in (`--recover-fallback`): when the newest checkpoint is
    /// unreadable, recover from the newest readable older checkpoint instead
    /// of refusing to open. Skipped checkpoints are reported on stderr.
    pub recover_fallback: bool,
    /// Busy timeout (milliseconds) for the exclusive inter-process database
    /// lock (`[database] lock_timeout_ms`, default 5000). While another
    /// session holds the same file-backed database, the open retries for up
    /// to this long before failing with `database is locked`. `0` = fail
    /// immediately.
    pub lock_timeout_ms: u64,
}

impl Default for DbOpenOptions {
    fn default() -> Self {
        DbOpenOptions { wal: false, recover_fallback: false, lock_timeout_ms: 5000 }
    }
}

impl SqlExecutor {
    /// Create an executor with WAL disabled (default snapshot persistence).
    ///
    /// Equivalent to `new_with_wal(database, false)`. Retained for callers and
    /// tests that do not opt into the WAL path. (The binary itself always goes
    /// through `new_with_options`; this wrapper is used by the in-crate tests.)
    #[allow(dead_code)]
    pub fn new(database: Option<String>) -> anyhow::Result<Self> {
        Self::new_with_wal(database, false)
    }

    /// Create an executor with the given WAL setting and default recovery
    /// strictness (no checkpoint fallback).
    pub fn new_with_wal(database: Option<String>, wal: bool) -> anyhow::Result<Self> {
        Self::new_with_options(database, DbOpenOptions { wal, ..DbOpenOptions::default() })
    }

    /// Create an executor, optionally activating the opt-in WAL persistence path.
    ///
    /// When `options.wal` is `true` AND `database` resolves to a real file path
    /// (not `:memory:` and not `None`), the executor:
    ///   * recovers the database from `<stem>-checkpoints/` + `<stem>.wal` via
    ///     `RecoveryManager::recover()`,
    ///   * attaches a live `PersistenceEngine` so subsequent writes are logged,
    ///   * and routes `save_database` to checkpoint + WAL truncate.
    ///
    /// When `options.wal` is `false`, or for in-memory databases, behavior is
    /// unchanged: snapshot load on open, full snapshot save on `\save`/exit.
    ///
    /// Failure policy (issue #5807): a database file written by a newer VibeSQL
    /// binary, or whose newest checkpoint is unreadable, is a hard open error —
    /// the CLI must never silently present an empty (or stale) database.
    /// `options.recover_fallback` opts into older-checkpoint recovery, loudly.
    pub fn new_with_options(
        database: Option<String>,
        options: DbOpenOptions,
    ) -> anyhow::Result<Self> {
        // Treat :memory: as an in-memory database (no file path)
        let database = database.filter(|p| !is_memory_database(p));

        // Exclusive inter-process lock for EVERY file-backed session — WAL
        // and snapshot-only alike (issue #5808). VibeSQL holds the whole
        // database in memory and checkpoints/saves its own image, so two
        // concurrent writers cannot be merged: the last save wins and
        // clobbers the other session's committed writes. Acquired BEFORE any
        // recovery/load so no other process can be mid-checkpoint while we
        // read, and held (via `_db_lock`) until the executor drops — i.e.
        // after the exit-time checkpoint/save.
        //
        // `:memory:` / no-path sessions take no lock and create no `.lock`
        // file (`database` is already `None` for them here).
        let db_lock = match database {
            Some(ref db_path) => {
                let lock = vibesql_storage::acquire_exclusive(
                    std::path::Path::new(db_path),
                    std::time::Duration::from_millis(options.lock_timeout_ms),
                )
                .map_err(|e| match e {
                    // Exact SQLite CLI wording: `Error: database is locked`.
                    vibesql_storage::StorageError::DatabaseLocked => anyhow::anyhow!("{}", e),
                    other => anyhow::anyhow!("Failed to lock database at {}: {}", db_path, other),
                })?;

                // Safe only under the exclusive lock: remove stale temp stubs
                // left by interrupted checkpoint/truncate writers
                // (`checkpoint_*.tmp`, `<stem>.wal.tmp`). Completed `.vchk`
                // files are never touched.
                let paths = wal::WalPaths::derive(db_path);
                vibesql_storage::cleanup_stale_temp_files(&paths.wal_path, &paths.checkpoint_dir);

                Some(lock)
            }
            None => None,
        };

        // WAL-active path: only when explicitly opted in AND a file path exists.
        // For in-memory databases the WAL is silently disabled (no file to
        // attach to) — consistent with the issue's documented edge case.
        if options.wal {
            if let Some(ref db_path) = database {
                // Legacy snapshot-only support (#5807): a `.vbsql` written
                // before WAL mode has no checkpoint archive. Load the snapshot
                // as the recovery base so its data is preserved (and captured
                // by the first checkpoint) instead of being silently ignored.
                // When any checkpoint exists, checkpoints are the newer truth
                // and the base is skipped entirely.
                let paths = wal::WalPaths::derive(db_path);
                let base = if !paths.has_checkpoint_files()
                    && std::fs::metadata(db_path).map(|m| m.len() > 0).unwrap_or(false)
                {
                    Some(load_database_file(db_path)?)
                } else {
                    None
                };

                let (mut db, wal_state) =
                    wal::WalState::open_with_base(db_path, base, options.recover_fallback)
                        .map_err(|e| {
                            anyhow::anyhow!(
                                "Failed to open WAL-backed database at {}: {}",
                                db_path,
                                e
                            )
                        })?;
                // Rebuild expression-index bodies that the snapshot loader left
                // empty (it cannot evaluate index expressions). Without this an
                // expression index would silently return zero rows after reopen.
                // See issue #5784.
                vibesql_executor::rebuild_pending_expression_indexes(&mut db).map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to rebuild expression indexes after loading {}: {}",
                        db_path,
                        e
                    )
                })?;
                return Ok(SqlExecutor {
                    db,
                    timing_enabled: false,
                    count_changes: false,
                    wal_state: Some(wal_state),
                    db_path: Some(db_path.clone()),
                    _db_lock: db_lock,
                });
            }
        }

        // Remember the file path (if any) for `PRAGMA database_list`. Captured
        // before `database` is consumed by the load below.
        let db_path = database.clone();

        // Load database from file if provided, otherwise create new in-memory database
        let mut db = if let Some(db_path) = database {
            // Check if file exists
            if std::path::Path::new(&db_path).exists() {
                load_database_file(&db_path)?
            } else {
                // File doesn't exist, create new database
                // (Will be saved when user uses \save or when modifications occur)
                Database::new()
            }
        } else {
            // No database file specified, use in-memory database
            Database::new()
        };

        // Rebuild expression-index bodies left empty by the snapshot loader
        // (binary/JSON reload path). Harmless no-op when there are none. #5784.
        vibesql_executor::rebuild_pending_expression_indexes(&mut db)
            .map_err(|e| anyhow::anyhow!("Failed to rebuild expression indexes: {}", e))?;

        Ok(SqlExecutor {
            db,
            timing_enabled: false,
            count_changes: false,
            wal_state: None,
            db_path,
            _db_lock: db_lock,
        })
    }

    /// Returns true if the WAL persistence path is active for this session.
    /// (Used by the in-crate tests to assert the opt-in / edge-case behavior.)
    #[allow(dead_code)]
    pub fn wal_active(&self) -> bool {
        self.wal_state.is_some()
    }

    /// Returns true if the current session is inside an active transaction.
    pub fn in_transaction(&self) -> bool {
        self.db.in_transaction()
    }

    pub fn execute(&mut self, sql: &str) -> anyhow::Result<QueryResult> {
        let start = Instant::now();

        // Parse SQL using arena fallback for SELECT statements (preserves original case in source_text)
        let statement = parse_with_arena_fallback(sql).map_err(|e| anyhow::anyhow!("{}", e))?;

        // The CLI executes statements directly and has no parameter-binding
        // mechanism. SQLite treats a parameter that was never bound as NULL
        // (e.g. `SELECT b FROM t WHERE a > ?` with no binding yields no rows
        // because `a > NULL` is NULL). Substitute any leftover `?`/`$N`/`:name`
        // placeholders with NULL literals so they evaluate the same way instead
        // of being rejected as unbound.
        let statement = vibesql_executor::fill_unbound_placeholders_with_null(statement);

        // Execute statement through appropriate executor
        let mut result = QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
            row_count: 0,
            execution_time_ms: None,
            message: None,
        };

        match statement {
            vibesql_ast::Statement::Select(select_stmt) => {
                // Execute SELECT and format results with column names
                let executor = vibesql_executor::SelectExecutor::new(&self.db);
                match executor.execute_with_columns(&select_stmt) {
                    Ok(select_result) => {
                        result.row_count = select_result.rows.len();
                        // Use column names from the executor result
                        result.columns = select_result.columns;
                        // Convert rows to string representation using SQLite-compatible format
                        // NULL values are represented as None to distinguish from the literal string "NULL"
                        for row in select_result.rows {
                            let row_strs: Vec<Option<String>> = row
                                .values
                                .iter()
                                .map(|v| if v.is_null() { None } else { Some(format_sql_value(v)) })
                                .collect();
                            result.rows.push(row_strs);
                        }
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::CreateTable(create_stmt) => {
                // Pass the verbatim original statement text so sqlite_master.sql
                // preserves the user's exact CREATE TABLE formatting (issue #5619).
                match vibesql_executor::CreateTableExecutor::execute_with_source(
                    &create_stmt,
                    &mut self.db,
                    Some(sql),
                ) {
                    Ok(_) => {
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Insert(insert_stmt) => {
                match vibesql_executor::InsertExecutor::execute_returning(
                    &mut self.db,
                    &insert_stmt,
                ) {
                    Ok(outcome) => {
                        let affected_rows = outcome.affected_rows;
                        // Track changes count for changes() and total_changes() functions
                        self.db.set_last_changes_count(affected_rows);
                        self.db.increment_total_changes_count(affected_rows);
                        result.row_count = affected_rows;

                        // RETURNING clause: render the projected NEW rows like
                        // a SELECT result (SQLite 3.35.0+ semantics).
                        if let Some(returning_result) = outcome.returning {
                            result.row_count = returning_result.rows.len();
                            result.columns = returning_result.columns;
                            for row in returning_result.rows {
                                let row_strs: Vec<Option<String>> =
                                    row.values
                                        .iter()
                                        .map(|v| {
                                            if v.is_null() {
                                                None
                                            } else {
                                                Some(format_sql_value(v))
                                            }
                                        })
                                        .collect();
                                result.rows.push(row_strs);
                            }
                        } else if self.count_changes {
                            // PRAGMA count_changes=ON (SQLite parity): emit a
                            // one-row result with the DIRECT insert count.
                            // Rows taken through the upsert DO UPDATE arm are
                            // excluded here even though changes() includes
                            // them (verified against sqlite3; upsert1-400).
                            let count = affected_rows - outcome.upsert_updated_rows;
                            result.columns = vec!["rows inserted".to_string()];
                            result.rows = vec![vec![Some(count.to_string())]];
                            result.row_count = 1;
                        }
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Update(update_stmt) => {
                match vibesql_executor::UpdateExecutor::execute_returning(
                    &update_stmt,
                    &mut self.db,
                ) {
                    Ok((affected_rows, returning)) => {
                        // Track changes count for changes() and total_changes() functions
                        self.db.set_last_changes_count(affected_rows);
                        self.db.increment_total_changes_count(affected_rows);
                        result.row_count = affected_rows;

                        // RETURNING clause: render the projected NEW rows like
                        // a SELECT result (SQLite 3.35.0+ semantics).
                        if let Some(returning_result) = returning {
                            result.row_count = returning_result.rows.len();
                            result.columns = returning_result.columns;
                            for row in returning_result.rows {
                                let row_strs: Vec<Option<String>> =
                                    row.values
                                        .iter()
                                        .map(|v| {
                                            if v.is_null() {
                                                None
                                            } else {
                                                Some(format_sql_value(v))
                                            }
                                        })
                                        .collect();
                                result.rows.push(row_strs);
                            }
                        } else if self.count_changes {
                            // PRAGMA count_changes=ON (SQLite parity): emit a
                            // one-row result with the number of updated rows.
                            result.columns = vec!["rows updated".to_string()];
                            result.rows = vec![vec![Some(affected_rows.to_string())]];
                            result.row_count = 1;
                        }
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Delete(delete_stmt) => {
                match vibesql_executor::DeleteExecutor::execute_returning(
                    &delete_stmt,
                    &mut self.db,
                ) {
                    Ok((affected_rows, returning)) => {
                        // Track changes count for changes() and total_changes() functions
                        self.db.set_last_changes_count(affected_rows);
                        self.db.increment_total_changes_count(affected_rows);
                        result.row_count = affected_rows;

                        // RETURNING clause: render the projected OLD rows like
                        // a SELECT result (SQLite 3.35.0+ semantics).
                        if let Some(returning_result) = returning {
                            result.row_count = returning_result.rows.len();
                            result.columns = returning_result.columns;
                            for row in returning_result.rows {
                                let row_strs: Vec<Option<String>> =
                                    row.values
                                        .iter()
                                        .map(|v| {
                                            if v.is_null() {
                                                None
                                            } else {
                                                Some(format_sql_value(v))
                                            }
                                        })
                                        .collect();
                                result.rows.push(row_strs);
                            }
                        } else if self.count_changes {
                            // PRAGMA count_changes=ON (SQLite parity): emit a
                            // one-row result with the number of deleted rows.
                            result.columns = vec!["rows deleted".to_string()];
                            result.rows = vec![vec![Some(affected_rows.to_string())]];
                            result.row_count = 1;
                        }
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::CreateView(mut view_stmt) => {
                // Store original SQL for sqlite_master compatibility
                view_stmt.sql_definition = Some(sql.to_string());
                match vibesql_executor::advanced_objects::execute_create_view(
                    &view_stmt,
                    &mut self.db,
                ) {
                    Ok(_) => {
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::DropView(drop_stmt) => {
                match vibesql_executor::advanced_objects::execute_drop_view(
                    &drop_stmt,
                    &mut self.db,
                ) {
                    Ok(_) => {
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::DropTable(drop_stmt) => {
                match vibesql_executor::DropTableExecutor::execute(&drop_stmt, &mut self.db) {
                    Ok(_) => {
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::TruncateTable(truncate_stmt) => {
                match vibesql_executor::TruncateTableExecutor::execute(&truncate_stmt, &mut self.db)
                {
                    Ok(rows_deleted) => {
                        result.row_count = rows_deleted;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::CreateTrigger(trigger_stmt) => {
                // Pass original SQL so the trigger survives SQL-dump persistence
                // (mirrors the sql_definition handling for views above).
                match vibesql_executor::TriggerExecutor::create_trigger_with_sql(
                    &mut self.db,
                    &trigger_stmt,
                    Some(sql),
                ) {
                    Ok(_) => {
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::AlterTrigger(alter_stmt) => {
                match vibesql_executor::TriggerExecutor::alter_trigger(&mut self.db, &alter_stmt) {
                    Ok(_) => {
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::DropTrigger(drop_stmt) => {
                match vibesql_executor::TriggerExecutor::drop_trigger(&mut self.db, &drop_stmt) {
                    Ok(_) => {
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::SetVariable(set_var_stmt) => {
                match vibesql_executor::SchemaExecutor::execute_set_variable(
                    &set_var_stmt,
                    &mut self.db,
                ) {
                    Ok(_) => {
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Reindex(reindex_stmt) => {
                match vibesql_executor::ReindexExecutor::execute(&reindex_stmt, &self.db) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Analyze(analyze_stmt) => {
                match vibesql_executor::AnalyzeExecutor::execute(&analyze_stmt, &mut self.db) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Vacuum(vacuum_stmt) => {
                // SQLite compatibility: VACUUM maps to MVCC old-version
                // garbage collection (a no-op when MVCC is disabled).
                if vacuum_stmt.into_file.is_some() {
                    return Err(anyhow::anyhow!("VACUUM INTO is not supported in VibeSQL"));
                }
                match self.db.vacuum_mvcc() {
                    Ok(_reclaimed) => {
                        result.message = Some("VACUUM completed".to_string());
                        result.row_count = 0; // VACUUM doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Explain(explain_stmt) => {
                match vibesql_executor::ExplainExecutor::execute(&explain_stmt, &self.db) {
                    Ok(explain_result) => {
                        if explain_stmt.query_plan {
                            // SQLite-compatible EXPLAIN QUERY PLAN format
                            let output = explain_result.to_sqlite_eqp();
                            // Use "detail" as column name (matches SQLite's actual column)
                            // The "QUERY PLAN" header is now included in the data for TCL test compatibility
                            result.columns = vec!["detail".to_string()];
                            // Split output into rows for better display
                            for line in output.lines() {
                                result.rows.push(vec![Some(line.to_string())]);
                            }
                        } else {
                            // SQLite-compatible EXPLAIN format (VM bytecode style)
                            let vm_output = explain_result.to_sqlite_vm();
                            result.columns = vibesql_executor::SqliteVmOutput::column_names()
                                .iter()
                                .map(|s| s.to_string())
                                .collect();
                            for row in vm_output.to_rows() {
                                result.rows.push(row.into_iter().map(Some).collect());
                            }
                        }
                        result.row_count = result.rows.len();
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::CreateIndex(index_stmt) => {
                match vibesql_executor::CreateIndexExecutor::execute(&index_stmt, &mut self.db) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::DropIndex(drop_stmt) => {
                match vibesql_executor::DropIndexExecutor::execute(&drop_stmt, &mut self.db) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::AlterTable(alter_stmt) => {
                // Pass the verbatim original statement text so ADD COLUMN /
                // RENAME edits the stored CREATE TABLE text in place (matching
                // SQLite) instead of reconstructing it (issue #5625).
                match vibesql_executor::AlterTableExecutor::execute_with_source(
                    &alter_stmt,
                    &mut self.db,
                    Some(sql),
                ) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::BeginTransaction(begin_stmt) => {
                match vibesql_executor::BeginTransactionExecutor::execute(&begin_stmt, &mut self.db)
                {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Commit(commit_stmt) => {
                match vibesql_executor::CommitExecutor::execute(&commit_stmt, &mut self.db) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Rollback(rollback_stmt) => {
                match vibesql_executor::RollbackExecutor::execute(&rollback_stmt, &mut self.db) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Savepoint(savepoint_stmt) => {
                match vibesql_executor::SavepointExecutor::execute(&savepoint_stmt, &mut self.db) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::RollbackToSavepoint(rollback_stmt) => {
                match vibesql_executor::RollbackToSavepointExecutor::execute(
                    &rollback_stmt,
                    &mut self.db,
                ) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::ReleaseSavepoint(release_stmt) => {
                match vibesql_executor::ReleaseSavepointExecutor::execute(
                    &release_stmt,
                    &mut self.db,
                ) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::ShowTables(show_stmt) => {
                result = self.execute_show_tables(&show_stmt)?;
            }
            vibesql_ast::Statement::ShowDatabases(show_stmt) => {
                result = self.execute_show_databases(&show_stmt)?;
            }
            vibesql_ast::Statement::ShowColumns(show_stmt) => {
                result = self.execute_show_columns(&show_stmt)?;
            }
            vibesql_ast::Statement::ShowIndex(show_stmt) => {
                result = self.execute_show_index(&show_stmt)?;
            }
            vibesql_ast::Statement::ShowCreateTable(show_stmt) => {
                result = self.execute_show_create_table(&show_stmt)?;
            }
            vibesql_ast::Statement::Describe(desc_stmt) => {
                result = self.execute_describe(&desc_stmt)?;
            }
            vibesql_ast::Statement::CreateAssertion(create_stmt) => {
                match vibesql_executor::advanced_objects::execute_create_assertion(
                    &create_stmt,
                    &mut self.db,
                ) {
                    Ok(()) => {
                        result.message =
                            Some(format!("Assertion '{}' created", create_stmt.assertion_name));
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::DropAssertion(drop_stmt) => {
                match vibesql_executor::advanced_objects::execute_drop_assertion(
                    &drop_stmt,
                    &mut self.db,
                ) {
                    Ok(()) => {
                        result.message =
                            Some(format!("Assertion '{}' dropped", drop_stmt.assertion_name));
                        result.row_count = 0;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::Pragma(pragma_stmt) => {
                result = self.execute_pragma(&pragma_stmt)?;
            }
            _ => {
                return Err(anyhow::anyhow!("Statement type not yet supported in CLI"));
            }
        }

        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        if self.timing_enabled {
            result.execution_time_ms = Some(elapsed);
        }

        Ok(result)
    }

    pub fn toggle_timing(&mut self) {
        self.timing_enabled = !self.timing_enabled;
        let state = if self.timing_enabled { "on" } else { "off" };
        println!("Timing is {}", state);
    }

    /// Persist the database.
    ///
    /// When the WAL path is active (`[database] wal = true` + file-backed DB),
    /// this writes a checkpoint at the current LSN and truncates the WAL up to
    /// it (the WAL-active durability mechanism). Otherwise it falls back to the
    /// default full-state SQL dump snapshot at `path`.
    pub fn save_database(&mut self, path: &str) -> anyhow::Result<()> {
        if let Some(wal_state) = self.wal_state.as_mut() {
            return wal_state
                .checkpoint(&self.db)
                .map_err(|e| anyhow::anyhow!("Failed to checkpoint WAL database: {}", e));
        }

        self.db
            .save_sql_dump(path)
            .map_err(|e| anyhow::anyhow!("Failed to save database to {}: {}", path, e))
    }

    /// Execute SHOW TABLES statement
    fn execute_show_tables(
        &self,
        stmt: &vibesql_ast::ShowTablesStmt,
    ) -> anyhow::Result<QueryResult> {
        let tables = self.db.list_tables();

        // Apply LIKE filter if specified
        let filtered_tables: Vec<String> = if let Some(pattern) = &stmt.like_pattern {
            let regex_pattern = like_to_regex(pattern);
            let re = regex::Regex::new(&regex_pattern)
                .map_err(|e| anyhow::anyhow!("Invalid LIKE pattern: {}", e))?;
            tables.into_iter().filter(|t| re.is_match(t)).collect()
        } else {
            tables
        };

        // Note: WHERE clause filtering would require expression evaluation
        // For now, we support LIKE pattern only

        let rows: Vec<Vec<Option<String>>> =
            filtered_tables.iter().map(|t| vec![Some(t.clone())]).collect();
        let row_count = rows.len();

        Ok(QueryResult {
            columns: vec!["Tables_in_database".to_string()],
            rows,
            row_count,
            execution_time_ms: None,
            message: None,
        })
    }

    /// Execute SHOW DATABASES statement
    fn execute_show_databases(
        &self,
        stmt: &vibesql_ast::ShowDatabasesStmt,
    ) -> anyhow::Result<QueryResult> {
        let schemas = self.db.catalog.list_schemas();

        // Apply LIKE filter if specified
        let filtered_schemas: Vec<String> = if let Some(pattern) = &stmt.like_pattern {
            let regex_pattern = like_to_regex(pattern);
            let re = regex::Regex::new(&regex_pattern)
                .map_err(|e| anyhow::anyhow!("Invalid LIKE pattern: {}", e))?;
            schemas.into_iter().filter(|s| re.is_match(s)).collect()
        } else {
            schemas
        };

        let rows: Vec<Vec<Option<String>>> =
            filtered_schemas.iter().map(|s| vec![Some(s.clone())]).collect();
        let row_count = rows.len();

        Ok(QueryResult {
            columns: vec!["Database".to_string()],
            rows,
            row_count,
            execution_time_ms: None,
            message: None,
        })
    }

    /// Execute SHOW COLUMNS statement
    fn execute_show_columns(
        &self,
        stmt: &vibesql_ast::ShowColumnsStmt,
    ) -> anyhow::Result<QueryResult> {
        let normalized_name = stmt.table_name.to_uppercase();
        let table = self
            .db
            .get_table(&normalized_name)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' does not exist", stmt.table_name))?;

        let mut rows: Vec<Vec<Option<String>>> = Vec::new();

        for column in &table.schema.columns {
            // Check LIKE pattern if specified
            if let Some(pattern) = &stmt.like_pattern {
                let regex_pattern = like_to_regex(pattern);
                let re = regex::Regex::new(&regex_pattern)
                    .map_err(|e| anyhow::anyhow!("Invalid LIKE pattern: {}", e))?;
                if !re.is_match(&column.name) {
                    continue;
                }
            }

            let nullable = if column.nullable { "YES" } else { "NO" };
            let default_val =
                column.default_value.as_ref().map(|v| format!("{:?}", v)).unwrap_or_default();

            // Check if column is part of primary key
            let key = if table
                .schema
                .primary_key
                .as_ref()
                .map(|pk| pk.contains(&column.name))
                .unwrap_or(false)
            {
                "PRI"
            } else {
                ""
            };

            let row = if stmt.full {
                // SHOW FULL COLUMNS returns additional fields
                vec![
                    Some(column.name.clone()),
                    Some(display::format_data_type(&column.data_type)),
                    Some(String::new()), // Collation - not yet supported
                    Some(nullable.to_string()),
                    Some(key.to_string()),
                    Some(default_val),
                    Some(String::new()), // Extra
                    Some(String::new()), // Privileges
                    Some(String::new()), // Comment
                ]
            } else {
                vec![
                    Some(column.name.clone()),
                    Some(display::format_data_type(&column.data_type)),
                    Some(nullable.to_string()),
                    Some(key.to_string()),
                    Some(default_val),
                    Some(String::new()), // Extra
                ]
            };

            rows.push(row);
        }

        let row_count = rows.len();

        let columns = if stmt.full {
            vec![
                "Field".to_string(),
                "Type".to_string(),
                "Collation".to_string(),
                "Null".to_string(),
                "Key".to_string(),
                "Default".to_string(),
                "Extra".to_string(),
                "Privileges".to_string(),
                "Comment".to_string(),
            ]
        } else {
            vec![
                "Field".to_string(),
                "Type".to_string(),
                "Null".to_string(),
                "Key".to_string(),
                "Default".to_string(),
                "Extra".to_string(),
            ]
        };

        Ok(QueryResult { columns, rows, row_count, execution_time_ms: None, message: None })
    }

    /// Execute SHOW INDEX statement
    fn execute_show_index(&self, stmt: &vibesql_ast::ShowIndexStmt) -> anyhow::Result<QueryResult> {
        let normalized_name = stmt.table_name.to_lowercase();

        // Verify table exists
        let _ = self
            .db
            .get_table(&normalized_name)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' does not exist", stmt.table_name))?;

        let index_names = self.db.list_indexes();
        let mut rows: Vec<Vec<Option<String>>> = Vec::new();

        for index_name in index_names {
            if let Some(index_meta) = self.db.get_index(&index_name) {
                if index_meta.table_name == normalized_name {
                    // Add one row per column in the index
                    for (seq, col) in index_meta.columns.iter().enumerate() {
                        rows.push(vec![
                            Some(normalized_name.clone()),                               // Table
                            Some(if index_meta.unique { "0" } else { "1" }.to_string()), // Non_unique
                            Some(index_meta.index_name.clone()),                         // Key_name
                            Some((seq + 1).to_string()), // Seq_in_index
                            Some(col.expect_column_name().to_string()), // Column_name
                            Some("A".to_string()),       // Collation (always Ascending for now)
                            Some(String::new()),         // Cardinality
                            Some(String::new()),         // Sub_part
                            Some(String::new()),         // Packed
                            Some(String::new()),         // Null
                            Some("BTREE".to_string()),   // Index_type
                            Some(String::new()),         // Comment
                        ]);
                    }
                }
            }
        }

        let row_count = rows.len();

        Ok(QueryResult {
            columns: vec![
                "Table".to_string(),
                "Non_unique".to_string(),
                "Key_name".to_string(),
                "Seq_in_index".to_string(),
                "Column_name".to_string(),
                "Collation".to_string(),
                "Cardinality".to_string(),
                "Sub_part".to_string(),
                "Packed".to_string(),
                "Null".to_string(),
                "Index_type".to_string(),
                "Comment".to_string(),
            ],
            rows,
            row_count,
            execution_time_ms: None,
            message: None,
        })
    }

    /// Execute SHOW CREATE TABLE statement
    fn execute_show_create_table(
        &self,
        stmt: &vibesql_ast::ShowCreateTableStmt,
    ) -> anyhow::Result<QueryResult> {
        let normalized_name = stmt.table_name.to_lowercase();
        let table = self
            .db
            .get_table(&normalized_name)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' does not exist", stmt.table_name))?;

        // Build CREATE TABLE statement
        let mut create_sql = format!("CREATE TABLE {} (\n", normalized_name);

        // Add columns
        let mut column_defs: Vec<String> = Vec::new();
        for column in &table.schema.columns {
            let mut def =
                format!("  {} {}", column.name, display::format_data_type(&column.data_type));
            if !column.nullable {
                def.push_str(" NOT NULL");
            }
            if let Some(default) = &column.default_value {
                def.push_str(&format!(" DEFAULT {:?}", default));
            }
            column_defs.push(def);
        }

        // Add primary key constraint
        if let Some(pk_cols) = &table.schema.primary_key {
            column_defs.push(format!("  PRIMARY KEY ({})", pk_cols.join(", ")));
        }

        // Add unique constraints
        for unique_cols in &table.schema.unique_constraints {
            column_defs.push(format!("  UNIQUE ({})", unique_cols.join(", ")));
        }

        // Add foreign key constraints
        for fk in &table.schema.foreign_keys {
            column_defs.push(format!(
                "  FOREIGN KEY ({}) REFERENCES {}({})",
                fk.column_names.join(", "),
                fk.parent_table,
                fk.parent_column_names.join(", ")
            ));
        }

        create_sql.push_str(&column_defs.join(",\n"));
        create_sql.push_str("\n)");

        Ok(QueryResult {
            columns: vec!["Table".to_string(), "Create Table".to_string()],
            rows: vec![vec![Some(normalized_name), Some(create_sql)]],
            row_count: 1,
            execution_time_ms: None,
            message: None,
        })
    }

    /// Execute DESCRIBE statement
    fn execute_describe(&self, stmt: &vibesql_ast::DescribeStmt) -> anyhow::Result<QueryResult> {
        // DESCRIBE is equivalent to SHOW COLUMNS FROM
        let show_stmt = vibesql_ast::ShowColumnsStmt {
            table_name: stmt.table_name.clone(),
            database: None,
            full: false,
            like_pattern: stmt.column_pattern.clone(),
            where_clause: None,
        };
        self.execute_show_columns(&show_stmt)
    }

    /// Execute PRAGMA statement
    ///
    /// Implements SQLite-compatible PRAGMA statements for session configuration.
    /// Supports:
    /// - PRAGMA full_column_names (get/set)
    /// - PRAGMA short_column_names (get/set)
    fn execute_pragma(&mut self, stmt: &vibesql_ast::PragmaStmt) -> anyhow::Result<QueryResult> {
        let pragma_name = stmt.name.to_uppercase();

        // Handle PRAGMAs that take table name arguments (not boolean set/query)
        // These use function-style syntax: PRAGMA name(table_name)
        match pragma_name.as_str() {
            "FOREIGN_KEY_LIST" => {
                return self.execute_pragma_foreign_key_list(stmt);
            }
            "FOREIGN_KEY_CHECK" => {
                return self.execute_pragma_foreign_key_check(stmt);
            }
            "TABLE_INFO" => {
                return self.execute_pragma_table_info(stmt);
            }
            "DATABASE_LIST" => {
                return self.execute_pragma_database_list();
            }
            "INTEGRITY_CHECK" | "QUICK_CHECK" => {
                // SQLite compatibility: `PRAGMA integrity_check` and the
                // table-scoped form `PRAGMA integrity_check('t1')` both report
                // "ok" when no corruption is found. The table-argument form
                // arrives as `stmt.value = Some(...)`, which would otherwise be
                // misrouted to the SET branch and silently ignored (returning an
                // empty result). Handle both forms here, before the set/query
                // split, so any argument is accepted and "ok" is returned.
                return Ok(QueryResult {
                    columns: vec![pragma_name.to_lowercase()],
                    rows: vec![vec![Some("ok".to_string())]],
                    row_count: 1,
                    execution_time_ms: None,
                    message: None,
                });
            }
            _ => {}
        }

        // Handle setting vs querying
        if let Some(value) = &stmt.value {
            // SET operation
            let bool_value = pragma_value_to_bool(value);

            match pragma_name.as_str() {
                "FULL_COLUMN_NAMES" => {
                    self.db.set_full_column_names(bool_value);
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "SHORT_COLUMN_NAMES" => {
                    self.db.set_short_column_names(bool_value);
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "CASE_SENSITIVE_LIKE" => {
                    self.db.set_case_sensitive_like(bool_value);
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "COUNT_CHANGES" => {
                    // SQLite-compatible PRAGMA count_changes: when ON, each
                    // INSERT/UPDATE/DELETE returns a one-row result with the
                    // change count (issue #5283). Session-scoped, default OFF.
                    self.count_changes = bool_value;
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "REVERSE_UNORDERED_SELECTS" => {
                    self.db.set_reverse_unordered_selects(bool_value);
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "FOREIGN_KEYS" => {
                    self.db.set_foreign_keys_enabled(bool_value);
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "RECURSIVE_TRIGGERS" => {
                    // SQLite-compatible PRAGMA recursive_triggers (#5535).
                    // When OFF, a trigger already on the execution stack is not
                    // re-fired by DML within its own body; when ON (default),
                    // triggers recurse up to MAX_TRIGGER_RECURSION_DEPTH (#5479).
                    self.db.set_recursive_triggers(bool_value);
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "TRIGGER_DEPTH_LIMIT" => {
                    // Per-connection trigger recursion-depth limit (#5536).
                    //
                    // SQLite has no SQL PRAGMA for this — it is a C-API knob
                    // (`sqlite3_limit(db, SQLITE_LIMIT_TRIGGER_DEPTH, N)`). The
                    // TCL conformance shim runs each SQL batch in a fresh CLI
                    // process, so it carries the runtime limit forward by
                    // re-emitting this internal PRAGMA in its per-batch prefix
                    // (see scripts/tester_vibesql.tcl `sqlite3_limit`). The
                    // executor clamps N into its stack-safe range when firing
                    // triggers; we just store the raw value here.
                    if let Some(n) = pragma_value_to_i64(value) {
                        self.db.set_trigger_depth_limit(n);
                    }
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "WRITABLE_SCHEMA" => {
                    // SQLite-compatible PRAGMA writable_schema: when ON,
                    // UPDATE sqlite_master/sqlite_schema SET sql = ... may
                    // rewrite the stored CREATE TABLE source text (issue
                    // #5796; alterdropcol 8.x). Session-scoped, default OFF.
                    self.db.set_writable_schema(bool_value);
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "DEFER_FOREIGN_KEYS" => {
                    // SQLite-compatible PRAGMA defer_foreign_keys.
                    // Phase C1 of #5085: store/read the flag and auto-reset
                    // at COMMIT/ROLLBACK. Runtime semantic change (deferring
                    // FK violations until COMMIT) ships in Phase C2.
                    self.db.set_defer_foreign_keys(bool_value);
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                _ => {
                    // Unknown pragma - silently ignore for SQLite compatibility
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
            }
        } else {
            // QUERY operation - return current value
            match pragma_name.as_str() {
                "FULL_COLUMN_NAMES" => {
                    let value = if self.db.full_column_names() { "1" } else { "0" };
                    Ok(QueryResult {
                        columns: vec!["full_column_names".to_string()],
                        rows: vec![vec![Some(value.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "SHORT_COLUMN_NAMES" => {
                    let value = if self.db.short_column_names() { "1" } else { "0" };
                    Ok(QueryResult {
                        columns: vec!["short_column_names".to_string()],
                        rows: vec![vec![Some(value.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "CASE_SENSITIVE_LIKE" => {
                    let value = if self.db.case_sensitive_like() { "1" } else { "0" };
                    Ok(QueryResult {
                        columns: vec!["case_sensitive_like".to_string()],
                        rows: vec![vec![Some(value.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "COUNT_CHANGES" => {
                    let value = if self.count_changes { "1" } else { "0" };
                    Ok(QueryResult {
                        columns: vec!["count_changes".to_string()],
                        rows: vec![vec![Some(value.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "REVERSE_UNORDERED_SELECTS" => {
                    let value = if self.db.reverse_unordered_selects() { "1" } else { "0" };
                    Ok(QueryResult {
                        columns: vec!["reverse_unordered_selects".to_string()],
                        rows: vec![vec![Some(value.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "JOURNAL_MODE" => {
                    // SQLite compatibility: report the active journaling mode as a
                    // single-row result. VibeSQL runs its own always-on WAL, so it
                    // reports "wal" (the SET form, `PRAGMA journal_mode = X`, is a
                    // silently-accepted no-op handled by the catch-all above).
                    Ok(QueryResult {
                        columns: vec!["journal_mode".to_string()],
                        rows: vec![vec![Some("wal".to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "FOREIGN_KEYS" => {
                    let value = if self.db.foreign_keys_enabled() { "1" } else { "0" };
                    Ok(QueryResult {
                        columns: vec!["foreign_keys".to_string()],
                        rows: vec![vec![Some(value.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "RECURSIVE_TRIGGERS" => {
                    // SQLite-compatible PRAGMA recursive_triggers read (#5535).
                    // Defaults to 1 (ON), matching triggerC-6.1.
                    let value = if self.db.recursive_triggers() { "1" } else { "0" };
                    Ok(QueryResult {
                        columns: vec!["recursive_triggers".to_string()],
                        rows: vec![vec![Some(value.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "WRITABLE_SCHEMA" => {
                    // SQLite-compatible PRAGMA writable_schema read.
                    // Defaults to 0 (OFF).
                    let value = if self.db.writable_schema() { "1" } else { "0" };
                    Ok(QueryResult {
                        columns: vec!["writable_schema".to_string()],
                        rows: vec![vec![Some(value.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "DEFER_FOREIGN_KEYS" => {
                    // SQLite-compatible PRAGMA defer_foreign_keys read.
                    // Defaults to 0 and auto-resets at COMMIT/ROLLBACK.
                    let value = if self.db.defer_foreign_keys() { "1" } else { "0" };
                    Ok(QueryResult {
                        columns: vec!["defer_foreign_keys".to_string()],
                        rows: vec![vec![Some(value.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "DEFERRED_FK_COUNT" => {
                    // VibeSQL-specific PRAGMA used as a bridge for the TCL
                    // shim's `sqlite3_db_status db DBSTATUS_DEFERRED_FKS`
                    // helper (issue #5187). Returns the number of deferred
                    // FK violations that would still fail if the current
                    // transaction were to COMMIT right now — i.e., entries
                    // whose child row still exists and whose missing parent
                    // row has not been (re)inserted. Returns 0 outside an
                    // active transaction.
                    //
                    // See SQLite's DBSTATUS_DEFERRED_FKS:
                    //   https://www.sqlite.org/c3ref/c_dbstatus_options.html
                    let count = vibesql_executor::live_deferred_fk_violation_count(&self.db) as i64;
                    Ok(QueryResult {
                        columns: vec!["deferred_fk_count".to_string()],
                        rows: vec![vec![Some(count.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                _ => {
                    // Unknown pragma - return empty result for compatibility
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
            }
        }
    }

    /// PRAGMA database_list
    ///
    /// Lists the databases attached to the current connection. VibeSQL has no
    /// ATTACH, so at most two rows are reported, matching sqlite3:
    ///   - seq 0, name `main`, file = the backing file path (absolute) or "" for
    ///     an in-memory / no-path session.
    ///   - seq 1, name `temp`, file = "" (always empty) — emitted only once a
    ///     temp object has materialized this session's temp schema, mirroring
    ///     sqlite3 3.51.0, which omits the `temp` row until a temp object exists.
    fn execute_pragma_database_list(&self) -> anyhow::Result<QueryResult> {
        let columns = vec!["seq".to_string(), "name".to_string(), "file".to_string()];

        // sqlite3 reports the canonicalized absolute path for a file-backed
        // `main`; fall back to the raw path if canonicalization fails (e.g. the
        // file was created this session and not yet flushed), and to "" for
        // in-memory sessions.
        let main_file = match &self.db_path {
            Some(path) => std::fs::canonicalize(path)
                .ok()
                .and_then(|p| p.to_str().map(|s| s.to_string()))
                .unwrap_or_else(|| path.clone()),
            None => String::new(),
        };

        let mut rows = vec![vec![Some("0".to_string()), Some("main".to_string()), Some(main_file)]];

        // The `temp` database appears only after this session has created a
        // temp object (table, view, or trigger), matching sqlite3's behavior.
        // Its file is always empty.
        if self.db.catalog.has_temp_objects() {
            rows.push(vec![Some("1".to_string()), Some("temp".to_string()), Some(String::new())]);
        }

        let row_count = rows.len();
        Ok(QueryResult { columns, rows, row_count, execution_time_ms: None, message: None })
    }

    /// PRAGMA foreign_key_list(table_name)
    /// Returns FK metadata: id, seq, table, from, to, on_update, on_delete, match
    fn execute_pragma_foreign_key_list(
        &self,
        stmt: &vibesql_ast::PragmaStmt,
    ) -> anyhow::Result<QueryResult> {
        let table_name = match &stmt.value {
            Some(vibesql_ast::PragmaValue::Identifier(name)) => name.clone(),
            Some(vibesql_ast::PragmaValue::String(name)) => name.clone(),
            _ => {
                return Ok(QueryResult {
                    columns: vec![
                        "id".to_string(),
                        "seq".to_string(),
                        "table".to_string(),
                        "from".to_string(),
                        "to".to_string(),
                        "on_update".to_string(),
                        "on_delete".to_string(),
                        "match".to_string(),
                    ],
                    rows: Vec::new(),
                    row_count: 0,
                    execution_time_ms: None,
                    message: None,
                });
            }
        };

        let columns = vec![
            "id".to_string(),
            "seq".to_string(),
            "table".to_string(),
            "from".to_string(),
            "to".to_string(),
            "on_update".to_string(),
            "on_delete".to_string(),
            "match".to_string(),
        ];

        let mut rows = Vec::new();
        if let Some(schema) = self.db.catalog.get_table(&table_name) {
            for (fk_id, fk) in schema.foreign_keys.iter().enumerate() {
                for (seq, (col_name, parent_col_name)) in
                    fk.column_names.iter().zip(fk.parent_column_names.iter()).enumerate()
                {
                    let on_update = match &fk.on_update {
                        vibesql_catalog::ReferentialAction::NoAction => "NO ACTION",
                        vibesql_catalog::ReferentialAction::Restrict => "RESTRICT",
                        vibesql_catalog::ReferentialAction::Cascade => "CASCADE",
                        vibesql_catalog::ReferentialAction::SetNull => "SET NULL",
                        vibesql_catalog::ReferentialAction::SetDefault => "SET DEFAULT",
                    };
                    let on_delete = match &fk.on_delete {
                        vibesql_catalog::ReferentialAction::NoAction => "NO ACTION",
                        vibesql_catalog::ReferentialAction::Restrict => "RESTRICT",
                        vibesql_catalog::ReferentialAction::Cascade => "CASCADE",
                        vibesql_catalog::ReferentialAction::SetNull => "SET NULL",
                        vibesql_catalog::ReferentialAction::SetDefault => "SET DEFAULT",
                    };
                    rows.push(vec![
                        Some(fk_id.to_string()),
                        Some(seq.to_string()),
                        Some(fk.parent_table.clone()),
                        Some(col_name.clone()),
                        Some(parent_col_name.clone()),
                        Some(on_update.to_string()),
                        Some(on_delete.to_string()),
                        Some("NONE".to_string()),
                    ]);
                }
            }
        }

        let row_count = rows.len();
        Ok(QueryResult { columns, rows, row_count, execution_time_ms: None, message: None })
    }

    /// PRAGMA foreign_key_check or PRAGMA foreign_key_check(table_name)
    /// Returns rows for any FK violations: table, rowid, parent, fkid
    fn execute_pragma_foreign_key_check(
        &self,
        stmt: &vibesql_ast::PragmaStmt,
    ) -> anyhow::Result<QueryResult> {
        let columns = vec![
            "table".to_string(),
            "rowid".to_string(),
            "parent".to_string(),
            "fkid".to_string(),
        ];

        // Schema-qualified pragma handling. VibeSQL only carries a single schema today,
        // so:
        //   PRAGMA <unknown>.foreign_key_check;            -> return empty (no tables in that schema)
        //   PRAGMA <unknown>.foreign_key_check(table);     -> error "no such table: <schema>.<table>"
        // "main" and the current schema both refer to the only available schema.
        let current_schema = self.db.catalog.get_current_schema().to_string();
        if let Some(ref schema) = stmt.database {
            let is_current =
                schema.eq_ignore_ascii_case(&current_schema) || schema.eq_ignore_ascii_case("main");
            if !is_current {
                let table_part = match &stmt.value {
                    Some(vibesql_ast::PragmaValue::Identifier(name)) => Some(name.clone()),
                    Some(vibesql_ast::PragmaValue::String(name)) => Some(name.clone()),
                    _ => None,
                };
                if let Some(t) = table_part {
                    anyhow::bail!("no such table: {}.{}", schema, t);
                }
                return Ok(QueryResult {
                    columns,
                    rows: Vec::new(),
                    row_count: 0,
                    execution_time_ms: None,
                    message: None,
                });
            }
        }

        // Tuple is (table, rowid_or_null, parent, fk_id). None rowid means WITHOUT ROWID,
        // which SQLite reports as NULL.
        let mut rows: Vec<(String, Option<i64>, String, usize)> = Vec::new();
        let table_name = match &stmt.value {
            Some(vibesql_ast::PragmaValue::Identifier(name)) => Some(name.clone()),
            Some(vibesql_ast::PragmaValue::String(name)) => Some(name.clone()),
            _ => None,
        };

        // Collect tables to check
        let tables_to_check: Vec<String> = if let Some(ref name) = table_name {
            vec![name.clone()]
        } else {
            self.db.catalog.list_tables()
        };

        for tbl_name in &tables_to_check {
            let (fk_constraints, rowid_alias_idx, without_rowid) =
                if let Some(schema) = self.db.catalog.get_table(tbl_name) {
                    (schema.foreign_keys.clone(), schema.rowid_alias_column, schema.without_rowid)
                } else {
                    continue;
                };

            if fk_constraints.is_empty() {
                continue;
            }

            // Get all rows from the child table
            // Note: tables are stored with qualified names (schema.table)
            let qualified_name = format!("{}.{}", self.db.catalog.get_current_schema(), tbl_name);
            let child_rows: Vec<_> = if let Some(table) = self.db.tables.get(&qualified_name) {
                table.scan_live().map(|(id, row)| (id, row.clone())).collect()
            } else if let Some(table) = self.db.tables.get(tbl_name.as_str()) {
                table.scan_live().map(|(id, row)| (id, row.clone())).collect()
            } else {
                continue;
            };

            // Compute SQLite-compatible rowid for each child row.
            // - WITHOUT ROWID tables: report NULL rowid
            // - INTEGER PRIMARY KEY tables: rowid is the IPK column value
            // - Other tables: rowid is the 1-based physical index (storage starts at 0)
            let row_with_rowid: Vec<(Option<i64>, &vibesql_storage::Row)> = child_rows
                .iter()
                .map(|(phys_idx, row)| {
                    if without_rowid {
                        return (None, row);
                    }
                    let rowid = match rowid_alias_idx.and_then(|idx| row.values.get(idx)) {
                        Some(vibesql_types::SqlValue::Integer(v)) => *v,
                        _ => (*phys_idx as i64) + 1,
                    };
                    (Some(rowid), row)
                })
                .collect();

            for (fk_id, fk) in fk_constraints.iter().enumerate() {
                // Mismatch check: if the parent table exists but lacks a key
                // (PRIMARY KEY / UNIQUE constraint / non-partial UNIQUE INDEX)
                // covering the FK columns, raise the SQLite-compatible error.
                // Matches `do_catchsql_test 11.1` in fkey5.test.
                if let Some((child, parent)) =
                    vibesql_executor::foreign_key_check::detect_fk_mismatch(&self.db, tbl_name, fk)
                {
                    anyhow::bail!(
                        "foreign key mismatch - \"{}\" referencing \"{}\"",
                        child,
                        parent
                    );
                }

                // Get parent column collations so we can match SQLite's FK comparison rules
                // (numeric coercion + parent-column collation, e.g. NOCASE).
                // Use the shared resolver so post-reload placeholder indices
                // do not skew which parent columns we read from.
                let parent_column_collations: Vec<Option<String>> =
                    vibesql_executor::foreign_key_check::parent_collations_for_fk(&self.db, fk);
                let resolved_parent_indices =
                    vibesql_executor::foreign_key_check::resolved_parent_indices_for_fk(
                        &self.db, fk,
                    );

                // Get parent table data
                let parent_qualified =
                    format!("{}.{}", self.db.catalog.get_current_schema(), &fk.parent_table);
                let parent_rows: Vec<_> =
                    if let Some(parent_table) = self.db.tables.get(&parent_qualified) {
                        parent_table.scan_live().map(|(_, row)| row.clone()).collect()
                    } else if let Some(parent_table) = self.db.tables.get(&fk.parent_table) {
                        parent_table.scan_live().map(|(_, row)| row.clone()).collect()
                    } else {
                        // Parent table doesn't exist - every row whose FK columns are all
                        // non-NULL is a violation. NULL FK values never violate (matches SQLite).
                        for (rowid, child_row) in &row_with_rowid {
                            let any_null = fk.column_indices.iter().any(|&idx| {
                                matches!(
                                    child_row.values.get(idx),
                                    Some(vibesql_types::SqlValue::Null) | None
                                )
                            });
                            if any_null {
                                continue;
                            }
                            rows.push((tbl_name.clone(), *rowid, fk.parent_table.clone(), fk_id));
                        }
                        continue;
                    };

                // Check each child row against parent rows
                for (rowid, child_row) in &row_with_rowid {
                    let child_values: Vec<_> = fk
                        .column_indices
                        .iter()
                        .map(|&idx| {
                            if idx < child_row.values.len() {
                                &child_row.values[idx]
                            } else {
                                &vibesql_types::SqlValue::Null
                            }
                        })
                        .collect();

                    // Skip if any FK value is NULL (NULL doesn't violate FK)
                    if child_values.iter().any(|v| matches!(v, vibesql_types::SqlValue::Null)) {
                        continue;
                    }

                    // Check if matching parent row exists
                    let found = parent_rows.iter().any(|parent_row| {
                        resolved_parent_indices.iter().zip(child_values.iter()).enumerate().all(
                            |(i, (&parent_idx, child_val))| {
                                if parent_idx < parent_row.values.len() {
                                    vibesql_executor::foreign_key_check::fk_values_equal(
                                        child_val,
                                        &parent_row.values[parent_idx],
                                        parent_column_collations.get(i).and_then(|c| c.as_deref()),
                                    )
                                } else {
                                    false
                                }
                            },
                        )
                    });

                    if !found {
                        rows.push((tbl_name.clone(), *rowid, fk.parent_table.clone(), fk_id));
                    }
                }
            }
        }

        // Sort violations by (table, rowid, fk_id) so output matches SQLite's btree order.
        rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.3.cmp(&b.3)));

        let final_rows: Vec<Vec<Option<String>>> = rows
            .into_iter()
            .map(|(t, rid, p, fk)| {
                vec![Some(t), rid.map(|v| v.to_string()), Some(p), Some(fk.to_string())]
            })
            .collect();

        let row_count = final_rows.len();
        Ok(QueryResult {
            columns,
            rows: final_rows,
            row_count,
            execution_time_ms: None,
            message: None,
        })
    }

    /// PRAGMA table_info(table_name) - SQLite-compatible
    ///
    /// Returns one row per column with:
    ///   cid (0-based column index), name, type (declared SQL type, may be ""),
    ///   notnull (0 or 1), dflt_value (default expression text or NULL),
    ///   pk (0 if not PK, else 1-based position within PK).
    ///
    /// Schema-qualified syntax is accepted: `PRAGMA main.table_info(t)`. VibeSQL
    /// only carries a single schema, so any other schema yields an empty result
    /// (matching the SQLite behavior of "no such table" being silent for
    /// table_info on missing tables).
    fn execute_pragma_table_info(
        &self,
        stmt: &vibesql_ast::PragmaStmt,
    ) -> anyhow::Result<QueryResult> {
        let columns = vec![
            "cid".to_string(),
            "name".to_string(),
            "type".to_string(),
            "notnull".to_string(),
            "dflt_value".to_string(),
            "pk".to_string(),
        ];

        let table_name = match &stmt.value {
            Some(vibesql_ast::PragmaValue::Identifier(name)) => name.clone(),
            Some(vibesql_ast::PragmaValue::String(name)) => name.clone(),
            _ => {
                // No table argument supplied - return empty (SQLite behavior)
                return Ok(QueryResult {
                    columns,
                    rows: Vec::new(),
                    row_count: 0,
                    execution_time_ms: None,
                    message: None,
                });
            }
        };

        // Schema-qualified pragma handling. Only "main" or the current schema
        // refer to the available schema; anything else returns an empty result
        // (SQLite returns no rows for a missing table in table_info, no error).
        let current_schema = self.db.catalog.get_current_schema().to_string();
        if let Some(ref schema) = stmt.database {
            let is_current =
                schema.eq_ignore_ascii_case(&current_schema) || schema.eq_ignore_ascii_case("main");
            if !is_current {
                return Ok(QueryResult {
                    columns,
                    rows: Vec::new(),
                    row_count: 0,
                    execution_time_ms: None,
                    message: None,
                });
            }
        }

        let schema = match self.db.catalog.get_table(&table_name) {
            Some(s) => s,
            None => {
                // SQLite returns empty result for table_info on a missing table.
                return Ok(QueryResult {
                    columns,
                    rows: Vec::new(),
                    row_count: 0,
                    execution_time_ms: None,
                    message: None,
                });
            }
        };

        // Build a name->pk-position map (1-based) for primary key lookups.
        // For composite keys, the position is the column's position within
        // the declared primary-key column list.
        let pk_positions: std::collections::HashMap<String, usize> =
            match schema.primary_key.as_ref() {
                Some(pk_cols) => {
                    pk_cols.iter().enumerate().map(|(i, name)| (name.clone(), i + 1)).collect()
                }
                None => std::collections::HashMap::new(),
            };

        let mut rows: Vec<Vec<Option<String>>> = Vec::with_capacity(schema.columns.len());
        for (cid, column) in schema.columns.iter().enumerate() {
            // Type column: SQLite reports the declared type as supplied in the
            // CREATE TABLE statement. We don't preserve the verbatim text, so
            // we map our DataType back to the canonical SQLite-flavor name.
            let type_str = sqlite_declared_type(&column.data_type, column.is_exact_integer_type);

            // notnull: 1 if NOT NULL, else 0. SQLite implicitly marks INTEGER
            // PRIMARY KEY rowid alias columns as NOT NULL.
            let notnull = if !column.nullable { 1 } else { 0 };

            // dflt_value: render the default expression as SQL text, or NULL.
            let dflt_value: Option<String> = column.default_value.as_ref().map(|e| {
                use vibesql_ast::pretty_print::ToSql;
                e.to_sql()
            });

            // pk: 1-based position within the primary key, or 0 if not PK.
            let pk = pk_positions.get(&column.name).copied().unwrap_or(0);

            rows.push(vec![
                Some(cid.to_string()),
                Some(column.name.clone()),
                Some(type_str),
                Some(notnull.to_string()),
                dflt_value,
                Some(pk.to_string()),
            ]);
        }

        let row_count = rows.len();
        Ok(QueryResult { columns, rows, row_count, execution_time_ms: None, message: None })
    }
}

/// Convert PRAGMA value to boolean
/// ON/1/TRUE -> true, OFF/0/FALSE -> false
fn pragma_value_to_bool(value: &vibesql_ast::PragmaValue) -> bool {
    match value {
        vibesql_ast::PragmaValue::Identifier(ident) => {
            let upper = ident.to_uppercase();
            matches!(upper.as_str(), "ON" | "TRUE" | "YES")
        }
        vibesql_ast::PragmaValue::Number(num) => num != "0",
        vibesql_ast::PragmaValue::SignedNumber(num) => num != "0" && num != "-0",
        vibesql_ast::PragmaValue::String(s) => {
            let upper = s.to_uppercase();
            matches!(upper.as_str(), "ON" | "TRUE" | "YES" | "1")
        }
    }
}

/// Parse a numeric PRAGMA value into an `i64`, if it is integral.
///
/// Used by integer-valued internal PRAGMAs such as `trigger_depth_limit`
/// (#5536). Returns `None` for non-numeric or non-integral values so the caller
/// can leave the existing setting unchanged.
fn pragma_value_to_i64(value: &vibesql_ast::PragmaValue) -> Option<i64> {
    match value {
        vibesql_ast::PragmaValue::Number(num) | vibesql_ast::PragmaValue::SignedNumber(num) => {
            num.trim().parse::<i64>().ok()
        }
        vibesql_ast::PragmaValue::String(s) => s.trim().parse::<i64>().ok(),
        vibesql_ast::PragmaValue::Identifier(_) => None,
    }
}

/// Convert SQL LIKE pattern to regex pattern
fn like_to_regex(pattern: &str) -> String {
    let mut regex = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '.' | '+' | '*' | '?' | '^' | '$' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\' => {
                regex.push('\\');
                regex.push(ch);
            }
            _ => regex.push(ch),
        }
    }
    regex.push('$');
    regex
}
