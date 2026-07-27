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
    /// PRAGMA auto_vacuum session setting (SQLite-compatible, default 0=NONE).
    /// VibeSQL does not implement pager auto-vacuum, but it parses, normalizes
    /// and echoes the setting exactly like SQLite so introspection round-trips
    /// (pragma.test pragma-17). Canonical codes: 0=NONE, 1=FULL, 2=INCREMENTAL.
    auto_vacuum: i64,
    /// PRAGMA temp_store session setting (SQLite-compatible, default 0=DEFAULT).
    /// Parsed/normalized/echoed like SQLite (pragma.test pragma-18); VibeSQL
    /// demotes TEMP tables to persistent so the value is advisory only.
    /// Canonical codes: 0=DEFAULT, 1=FILE, 2=MEMORY.
    temp_store: i64,
    /// PRAGMA encoding session setting (SQLite-compatible, default "UTF-8").
    /// VibeSQL only ever stores TEXT as UTF-8, but it parses, normalizes and
    /// echoes the setting exactly like SQLite so introspection round-trips
    /// (numcast.test numcast-utf8.0/utf16le.0/utf16be.0) even though the
    /// UTF-16 encodings themselves are not actually implemented.
    encoding: String,
    /// PRAGMA synchronous session setting (SQLite-compatible, default 2=FULL).
    /// VibeSQL has no pager to actually fsync at different safety levels, but
    /// it reproduces SQLite's exact get/set arithmetic (pragma.test pragma-1.*,
    /// pragma-5.*) including the quirky `((raw+1) & PAGER_SYNCHRONOUS_MASK)`
    /// wraparound for out-of-range numeric values and the "changed inside a
    /// transaction" error. Canonical values: 0=OFF, 1=NORMAL, 2=FULL, 3=EXTRA.
    synchronous: i64,
    /// PRAGMA cache_size session setting (SQLite-compatible, default -2000 —
    /// SQLITE_DEFAULT_CACHE_SIZE, meaning "2000 KiB" by SQLite's negative-size
    /// convention). VibeSQL has no page cache to actually resize, but it
    /// echoes the raw signed value exactly like SQLite (pragma.test
    /// pragma-1.*): unlike `default_cache_size` below, the value set here is
    /// stored and read back verbatim (not normalized to a positive count) and
    /// is session-only — it resets to the default on every reconnect.
    cache_size: i64,
    /// PRAGMA default_cache_size session setting (SQLite-compatible; default
    /// 0, meaning "never set" — reads back as -2000 via `resolve_cache_size_cookie`,
    /// matching SQLite's `SQLITE_DEFAULT_CACHE_SIZE`). SQLite tracks this as a
    /// separate on-disk header cookie from the in-memory `cache_size` field:
    /// `PRAGMA default_cache_size=N` normalizes to `abs(N)`, stores it here,
    /// AND immediately updates `cache_size` too; but `PRAGMA cache_size=N`
    /// does NOT touch this cookie (pragma.test pragma-1.2/1.5 vs. pragma-1.8).
    /// Known gap vs. real SQLite: SQLite persists this cookie into the
    /// database file header so it survives a `db close` / reopen; VibeSQL has
    /// no such on-disk cookie storage yet, so this is session-only and resets
    /// to 0 on every reconnect (tracked as follow-up work under #6175, not a
    /// Bucket-A pager internal — it is a genuine SQL-visible value, just
    /// missing durable storage).
    default_cache_size_cookie: i64,
    /// PRAGMA cache_spill session setting (SQLite-compatible; default ON with
    /// no explicit size, meaning the spill threshold mirrors `cache_size`).
    /// VibeSQL has no pager to actually spill dirty pages, but it echoes the
    /// get/set values like SQLite (pragma2.test pragma2-4.1/4.2):
    /// `(enabled, explicit_size)` — when disabled, reads as 0 regardless of
    /// `explicit_size`; when enabled with no explicit size, reads as the
    /// current `cache_size`.
    cache_spill_enabled: bool,
    cache_spill_explicit_size: Option<i64>,
    /// PRAGMA user_version session setting (SQLite-compatible, default 0).
    /// SQLite persists this as a raw signed 32-bit cookie in the database
    /// file header, available for application use; VibeSQL has no such
    /// on-disk cookie storage yet, so this is session-only and resets to 0
    /// on every reconnect (the TCL shim replays the last-set value across
    /// its per-batch CLI processes so it survives a `db close` / reopen
    /// against the same file within one logical connection — see
    /// `tester_vibesql.tcl`'s `pragma_user_version_cookie`, issue #6175).
    user_version: i64,
    /// PRAGMA application_id session setting (SQLite-compatible, default 0).
    /// Same persistence model as `user_version` above (a raw signed 32-bit
    /// header cookie in real SQLite; session-only + shim-replayed here).
    application_id: i64,
    /// PRAGMA schema_version session setting (SQLite-compatible, default 0).
    /// Unlike `user_version`/`application_id`, SQLite auto-increments this
    /// cookie every time the schema changes (CREATE/DROP/ALTER TABLE/INDEX/
    /// VIEW/TRIGGER, and VACUUM) — see the explicit bump at each successful
    /// DDL statement's dispatch site below (pragma.test pragma-8.1.*/8.2.4.*,
    /// #6175). Same session-only + shim-replayed persistence model as
    /// `user_version` (no on-disk cookie storage yet).
    schema_version: i64,
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

/// Strip a single surrounding pair of SQL identifier delimiters from a declared
/// type name, so `PRAGMA table_info` echoes it the way SQLite does.
///
/// SQLite records the declared type verbatim but without the delimiters that
/// quote it: `CREATE TABLE t(b [TYPE_Y], c "TYPE_Z")` reports the types
/// `TYPE_Y` and `TYPE_Z` (pragma-6.2). A non-delimited type such as
/// `VARCHAR(45, 65)` is returned unchanged, parentheses and all. Only a matching
/// outer pair of `[...]`, `"..."`, or `` `...` `` is removed; an unmatched or
/// absent delimiter leaves the text untouched.
fn strip_type_delimiters(type_source: &str) -> String {
    let t = type_source.trim();
    let bytes = t.as_bytes();
    if bytes.len() >= 2 {
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        let matched = (first == b'[' && last == b']')
            || (first == b'"' && last == b'"')
            || (first == b'`' && last == b'`');
        if matched {
            return t[1..t.len() - 1].to_string();
        }
    }
    t.to_string()
}

/// Apply SQLite's case normalization to a (delimiter-stripped) declared type
/// name for `PRAGMA table_info`.
///
/// SQLite echoes declared types verbatim (preserving case and any argument
/// list) with one exception: when the whole type name matches — case
/// insensitively — one of the five canonical storage-class names `INTEGER`,
/// `INT`, `TEXT`, `BLOB`, or `REAL`, it is reported upper-cased. So `text`
/// becomes `TEXT` and `integer` becomes `INTEGER`, but `numeric`, `varchar`,
/// `double`, `int(11)`, and `bigint` are all left exactly as written (verified
/// against sqlite3 3.x). Anything not in the set is returned unchanged.
fn canonicalize_sqlite_decltype(stripped_type: &str) -> String {
    const CANONICAL: [&str; 5] = ["INTEGER", "INT", "TEXT", "BLOB", "REAL"];
    for name in CANONICAL {
        if stripped_type.eq_ignore_ascii_case(name) {
            return name.to_string();
        }
    }
    stripped_type.to_string()
}

/// Strip a single balanced outer parenthesis pair from a DEFAULT expression's
/// verbatim source, matching SQLite's `dflt_value` normalization.
///
/// SQLite reports `CREATE TABLE t(b DEFAULT (5+3))` as the default `5+3`
/// (pragma-6.2.2) — one layer of the outer parentheses that wrap the whole
/// expression is removed. Text that is not wholly wrapped in a single balanced
/// pair (e.g. `(1)+(2)` or `-1`) is returned unchanged.
fn strip_outer_parens(default_source: &str) -> String {
    let s = default_source.trim();
    let bytes = s.as_bytes();
    if bytes.first() != Some(&b'(') || bytes.last() != Some(&b')') {
        return s.to_string();
    }
    // Confirm the leading '(' closes at the trailing ')', not earlier — so
    // `(1)+(2)` (whose first '(' closes mid-string) is left untouched.
    let mut depth = 0usize;
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    // The opening paren's match: strip only if it is the final byte.
                    if i == bytes.len() - 1 {
                        return s[1..s.len() - 1].trim().to_string();
                    }
                    return s.to_string();
                }
            }
            _ => {}
        }
    }
    s.to_string()
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
        // TEXT values are rendered to the client like SQLite's
        // sqlite3_column_text(): the returned string terminates at the first
        // embedded NUL byte. This is the MEM_Zero / OP_ToText behavior that
        // makes CAST(zeroblob(N) AS text) surface as an empty string and
        // CAST(x'4142004344' AS text) surface as "AB" (fuzz-1.8). The stored
        // value keeps all its bytes (hex()/quote() still see them); only this
        // client text-rendering boundary truncates.
        SqlValue::Varchar(s) | SqlValue::Character(s) => match s.find('\0') {
            Some(nul_idx) => s[..nul_idx].to_string(),
            None => s.to_string(),
        },
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
    /// Number of checkpoint files to retain after a successful checkpoint
    /// (`[database] keep_checkpoints`, default 2). After each `\save` /
    /// clean-exit checkpoint the oldest archived checkpoints are pruned so the
    /// `<stem>-checkpoints/` directory does not grow unboundedly (issue #6023).
    /// Clamped to a minimum of 1 so the newest checkpoint always survives.
    pub keep_checkpoints: usize,
    /// Memory budget (bytes) for the columnar representation cache
    /// (`[database] columnar_cache_budget`, default 256MB). `0` disables the
    /// cache so analytical queries take the row path. Applied to the `Database`
    /// right after open, before any query runs — the cache populates lazily, so
    /// setting the budget post-load discards nothing (issue #6200).
    pub columnar_cache_budget: usize,
}

impl Default for DbOpenOptions {
    fn default() -> Self {
        DbOpenOptions {
            wal: false,
            recover_fallback: false,
            lock_timeout_ms: 5000,
            keep_checkpoints: 2,
            // Matches vibesql_storage::database::DEFAULT_COLUMNAR_CACHE_BUDGET.
            columnar_cache_budget: 256 * 1024 * 1024,
        }
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

                let (mut db, wal_state) = wal::WalState::open_with_base(
                    db_path,
                    base,
                    options.recover_fallback,
                    options.keep_checkpoints,
                )
                .map_err(|e| {
                    anyhow::anyhow!("Failed to open WAL-backed database at {}: {}", db_path, e)
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
                // Apply the configured columnar cache budget (issue #6200).
                // Safe post-load: the cache populates lazily on the first
                // analytical query, so no cached data is discarded here. `0`
                // disables the cache entirely.
                db.set_columnar_cache_budget(options.columnar_cache_budget);
                return Ok(SqlExecutor {
                    db,
                    timing_enabled: false,
                    count_changes: false,
                    auto_vacuum: 0,
                    temp_store: 0,
                    encoding: default_encoding(),
                    synchronous: SQLITE_DEFAULT_SYNCHRONOUS,
                    cache_size: SQLITE_DEFAULT_CACHE_SIZE,
                    default_cache_size_cookie: 0,
                    cache_spill_enabled: true,
                    cache_spill_explicit_size: None,
                    user_version: 0,
                    application_id: 0,
                    schema_version: 0,
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

        // Apply the configured columnar cache budget (issue #6200). Applied
        // before any query runs; the cache populates lazily so this discards
        // nothing. `0` disables the cache (analytical queries take the row path).
        db.set_columnar_cache_budget(options.columnar_cache_budget);

        Ok(SqlExecutor {
            db,
            timing_enabled: false,
            count_changes: false,
            auto_vacuum: 0,
            temp_store: 0,
            encoding: default_encoding(),
            synchronous: SQLITE_DEFAULT_SYNCHRONOUS,
            cache_size: SQLITE_DEFAULT_CACHE_SIZE,
            default_cache_size_cookie: 0,
            cache_spill_enabled: true,
            cache_spill_explicit_size: None,
            user_version: 0,
            application_id: 0,
            schema_version: 0,
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

    /// Bump the session's `PRAGMA schema_version` cookie after a successful
    /// schema-changing statement (CREATE/DROP/ALTER TABLE/INDEX/VIEW/TRIGGER,
    /// or VACUUM) — mirrors SQLite's on-disk schema-cookie increment
    /// (pragma.test pragma-8.1.5/8.1.6, #6175). Wrapping add: SQLite's cookie
    /// is a 32-bit signed int that can in principle wrap; matching that is
    /// simpler than special-casing overflow for a value no real test drives
    /// anywhere near i64::MAX.
    fn bump_schema_version(&mut self) {
        self.schema_version = self.schema_version.wrapping_add(1);
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
                        self.bump_schema_version();
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
                        self.bump_schema_version();
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
                        self.bump_schema_version();
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::DropTable(drop_stmt) => {
                match vibesql_executor::DropTableExecutor::execute(&drop_stmt, &mut self.db) {
                    Ok(_) => {
                        result.row_count = 0; // DDL doesn't return rows
                        self.bump_schema_version();
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
                        self.bump_schema_version();
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::AlterTrigger(alter_stmt) => {
                match vibesql_executor::TriggerExecutor::alter_trigger(&mut self.db, &alter_stmt) {
                    Ok(_) => {
                        result.row_count = 0; // DDL doesn't return rows
                        self.bump_schema_version();
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::DropTrigger(drop_stmt) => {
                match vibesql_executor::TriggerExecutor::drop_trigger(&mut self.db, &drop_stmt) {
                    Ok(_) => {
                        result.row_count = 0; // DDL doesn't return rows
                        self.bump_schema_version();
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
                                              // SQLite bumps the schema-version cookie on VACUUM too
                                              // (pragma.test pragma-8.2.4.2/8.2.4.3, #6175).
                        self.bump_schema_version();
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
                        self.bump_schema_version();
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            vibesql_ast::Statement::DropIndex(drop_stmt) => {
                match vibesql_executor::DropIndexExecutor::execute(&drop_stmt, &mut self.db) {
                    Ok(msg) => {
                        result.message = Some(msg);
                        result.row_count = 0; // DDL doesn't return rows
                        self.bump_schema_version();
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
                        self.bump_schema_version();
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
            "INDEX_LIST" => {
                return self.execute_pragma_index_list(stmt);
            }
            "INDEX_INFO" => {
                return self.execute_pragma_index_info(stmt, false);
            }
            "INDEX_XINFO" => {
                return self.execute_pragma_index_info(stmt, true);
            }
            "COLLATION_LIST" => {
                return self.execute_pragma_collation_list();
            }
            "DATA_VERSION" => {
                // SQLite `PRAGMA data_version` returns an integer that a given
                // connection observes changing only when *another* connection
                // commits to the database file; commits made on the same
                // connection never change it (R-47505-58569), and writing to
                // the pragma is a no-op that still reports the current value.
                //
                // The VibeSQL TCL conformance shim runs each SQL batch as a
                // fresh connection to the file, so every read legitimately sees
                // the initial value 1 — which is exactly SQLite's behaviour for
                // a connection that has observed no external commit. The
                // multi-connection cases in pragma3 (a persistent `db2` seeing
                // the counter advance to 2, 3, ...) cannot be emulated across
                // the shim's ephemeral processes; those are a shim limitation,
                // not an engine gap, and are left failing rather than forced.
                //
                // Handled here (before the set/query split) so both the query
                // form and the read-only-write form `= N` report 1.
                return Ok(QueryResult {
                    columns: vec!["data_version".to_string()],
                    rows: vec![vec![Some("1".to_string())]],
                    row_count: 1,
                    execution_time_ms: None,
                    message: None,
                });
            }
            "INTEGRITY_CHECK" | "QUICK_CHECK" => {
                // SQLite compatibility: `PRAGMA integrity_check` and the
                // table-scoped form `PRAGMA integrity_check('t1')` both report
                // "ok" when no corruption is found. The table-argument form
                // arrives as `stmt.value = Some(...)`, which would otherwise be
                // misrouted to the SET branch and silently ignored (returning an
                // empty result). Handle both forms here, before the set/query
                // split, so any argument is accepted.
                //
                // Argument taxonomy (SQLite):
                //   PRAGMA integrity_check;            -- check whole database
                //   PRAGMA integrity_check=N;          -- whole database, cap at N errors
                //   PRAGMA integrity_check(N);         -- same, function form
                //   PRAGMA integrity_check=NAME;       -- check only table/schema NAME
                //   PRAGMA integrity_check='NAME';     -- quoted -> table NAME
                // A numeric argument is an error-count *limit*; a string or bare
                // identifier names a specific table to check and, if that name is
                // not an existing table (nor one of the schema tables), SQLite
                // errors with "no such table: NAME" (pragma-3.5.2 / pragma-3.6).
                // VibeSQL never finds corruption in a healthy table, so every
                // valid target still resolves to "ok"; we only add the missing
                // no-such-table validation for the name-argument form.
                if let Some(name) = match &stmt.value {
                    Some(vibesql_ast::PragmaValue::Identifier(name)) => Some(name.clone()),
                    Some(vibesql_ast::PragmaValue::String(name)) => Some(name.clone()),
                    // Number / SignedNumber / None are the whole-database forms
                    // (optionally with an error-count limit) — always "ok".
                    _ => None,
                } {
                    // The schema tables (sqlite_master and its aliases) are always
                    // valid integrity-check targets, even though they are not
                    // ordinary user tables in the catalog (pragma-3.6c).
                    let lower = name.to_ascii_lowercase();
                    let is_schema_table = matches!(
                        lower.as_str(),
                        "sqlite_master" | "sqlite_schema" | "sqlite_temp_schema"
                    );
                    if !is_schema_table {
                        let lookup = match &stmt.database {
                            Some(db) => format!("{}.{}", db, name),
                            None => name.clone(),
                        };
                        if self.db.catalog.get_table(&lookup).is_none() {
                            anyhow::bail!("no such table: {}", name);
                        }
                    }
                }
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
                "AUTO_VACUUM" => {
                    // SQLite-compatible PRAGMA auto_vacuum set (pragma.test
                    // pragma-17). VibeSQL has no pager auto-vacuum, but it
                    // parses/normalizes/echoes the setting so a set-then-read
                    // round-trip matches SQLite. Symbolic (none/full/incremental)
                    // and numeric spellings are both accepted; out-of-range or
                    // negative integers normalize to 0 (NONE), matching SQLite.
                    self.auto_vacuum = normalize_auto_vacuum(value);
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "TEMP_STORE" => {
                    // SQLite-compatible PRAGMA temp_store set (pragma.test
                    // pragma-18). Parsed/normalized/echoed like SQLite; the
                    // value is advisory (VibeSQL demotes TEMP tables to
                    // persistent). Symbolic (file/memory) and numeric spellings
                    // accepted; out-of-range/negative integers -> 0 (DEFAULT).
                    self.temp_store = normalize_temp_store(value);
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "ENCODING" => {
                    // SQLite-compatible PRAGMA encoding set (numcast.test
                    // numcast-utf8.0/utf16le.0/utf16be.0). VibeSQL only ever
                    // stores TEXT as UTF-8 — an unrecognized or UTF-16 value
                    // is still accepted and echoed back verbatim-normalized
                    // (matching SQLite's textual round-trip), it just has no
                    // effect on actual storage. An unrecognized value is a
                    // silent no-op, matching SQLite's behavior of ignoring an
                    // invalid encoding name.
                    if let Some(normalized) = normalize_encoding(value) {
                        self.encoding = normalized;
                    }
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "SYNCHRONOUS" => {
                    // SQLite-compatible PRAGMA synchronous set (pragma.test
                    // pragma-1.*, pragma-5.1). VibeSQL has no pager to actually
                    // fsync at different safety levels, but it reproduces
                    // SQLite's exact `getSafetyLevel()` + `((raw+1) &
                    // PAGER_SYNCHRONOUS_MASK)` arithmetic so get/set round-trips
                    // match, including the "changed inside a transaction" guard
                    // (real SQLite: `if (!db->autoCommit) error`).
                    if self.db.in_transaction() {
                        return Err(anyhow::anyhow!(
                            "Safety level may not be changed inside a transaction"
                        ));
                    }
                    self.synchronous = synchronous_read_value(parse_synchronous_raw(value));
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "CACHE_SIZE" => {
                    // SQLite-compatible PRAGMA cache_size set (pragma.test
                    // pragma-1.*). Session-only (SQLite's `pSchema->cache_size`
                    // is in-memory too and would be reloaded from the file
                    // header's `default_cache_size` cookie on reconnect —
                    // VibeSQL has no such cookie storage yet, see
                    // `default_cache_size_cookie`'s doc comment). Stores the
                    // raw signed value verbatim, unlike `default_cache_size`.
                    self.cache_size = pragma_value_atoi(value);
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "DEFAULT_CACHE_SIZE" => {
                    // SQLite-compatible PRAGMA default_cache_size set
                    // (pragma.test pragma-1.8+, deprecated but still tested).
                    // Normalizes to `abs(N)` and updates both the (session-only)
                    // persisted-cookie stand-in and `cache_size` immediately,
                    // matching SQLite's dual write to the header cookie and
                    // `pSchema->cache_size`.
                    let size = pragma_value_atoi(value).abs();
                    self.default_cache_size_cookie = size;
                    self.cache_size = size;
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "CACHE_SPILL" => {
                    // SQLite-compatible PRAGMA cache_spill set (pragma2.test
                    // pragma2-4.1/4.2). VibeSQL has no pager to actually spill
                    // dirty pages, but it echoes the enabled/size state like
                    // SQLite: a numeric argument sets an explicit spill-size
                    // threshold (and toggles enabled off only for `0`); a
                    // keyword argument (ON/OFF/...) toggles enabled without
                    // touching any previously-set explicit size.
                    let text = pragma_value_text(value).trim();
                    if let Ok(size) = text.parse::<i64>() {
                        self.cache_spill_explicit_size = Some(size);
                        self.cache_spill_enabled = size != 0;
                    } else {
                        self.cache_spill_enabled = pragma_value_to_bool(value);
                    }
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "USER_VERSION" => {
                    // SQLite-compatible PRAGMA user_version set (pragma.test
                    // pragma-8.2.*, #6175). Accepts both `= N` and the
                    // function-style `(N)` syntax (both parse to the same
                    // `stmt.value`). A non-integral argument is a silent no-op,
                    // matching SQLite's `getSafetyLevel`-style tolerance for
                    // unparsable pragma arguments.
                    if let Some(n) = pragma_value_to_i64(value) {
                        self.user_version = n;
                    }
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "APPLICATION_ID" => {
                    // SQLite-compatible PRAGMA application_id set (pragma.test
                    // pragma-8.3.2, #6175). Same argument handling as
                    // user_version above.
                    if let Some(n) = pragma_value_to_i64(value) {
                        self.application_id = n;
                    }
                    Ok(QueryResult {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        row_count: 0,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "SCHEMA_VERSION" => {
                    // SQLite-compatible PRAGMA schema_version set (pragma.test
                    // pragma-8.1.1/8.1.4/8.1.8, #6175). Same argument handling
                    // as user_version above. Note: real SQLite additionally
                    // blocks this write when DEFENSIVE mode is enabled
                    // (pragma-8.1.3) — VibeSQL has no DEFENSIVE mode (the
                    // `sqlite3_db_config` C-API stub is a no-op), so that one
                    // sub-case is a known, documented gap rather than
                    // reclassified/masked.
                    if let Some(n) = pragma_value_to_i64(value) {
                        self.schema_version = n;
                    }
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
                "AUTO_VACUUM" => {
                    // SQLite-compatible PRAGMA auto_vacuum read (pragma.test
                    // pragma-17). Reports the normalized session setting
                    // (0=NONE, 1=FULL, 2=INCREMENTAL); default 0.
                    Ok(QueryResult {
                        columns: vec!["auto_vacuum".to_string()],
                        rows: vec![vec![Some(self.auto_vacuum.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "TEMP_STORE" => {
                    // SQLite-compatible PRAGMA temp_store read (pragma.test
                    // pragma-18). Reports the normalized session setting
                    // (0=DEFAULT, 1=FILE, 2=MEMORY); default 0.
                    Ok(QueryResult {
                        columns: vec!["temp_store".to_string()],
                        rows: vec![vec![Some(self.temp_store.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "ENCODING" => {
                    // SQLite-compatible PRAGMA encoding read (numcast.test
                    // numcast-utf8.0/utf16le.0/utf16be.0). Reports the
                    // normalized session setting; default "UTF-8".
                    Ok(QueryResult {
                        columns: vec!["encoding".to_string()],
                        rows: vec![vec![Some(self.encoding.clone())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "SYNCHRONOUS" => {
                    // SQLite-compatible PRAGMA synchronous read (pragma.test
                    // pragma-1.*, pragma-5.0/5.2). Default 2 (FULL).
                    Ok(QueryResult {
                        columns: vec!["synchronous".to_string()],
                        rows: vec![vec![Some(self.synchronous.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "CACHE_SIZE" => {
                    // SQLite-compatible PRAGMA cache_size read (pragma.test
                    // pragma-1.*). Returns the raw signed session value;
                    // default -2000 (SQLITE_DEFAULT_CACHE_SIZE).
                    Ok(QueryResult {
                        columns: vec!["cache_size".to_string()],
                        rows: vec![vec![Some(self.cache_size.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "DEFAULT_CACHE_SIZE" => {
                    // SQLite-compatible PRAGMA default_cache_size read
                    // (pragma.test pragma-1.*). Resolves the (session-only)
                    // persisted-cookie stand-in: an unset/zero cookie reads
                    // back as -2000 (SQLITE_DEFAULT_CACHE_SIZE), matching
                    // SQLite's `OP_ReadCookie` + fallback arithmetic.
                    let value = resolve_cache_size_cookie(self.default_cache_size_cookie);
                    Ok(QueryResult {
                        columns: vec!["default_cache_size".to_string()],
                        rows: vec![vec![Some(value.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "CACHE_SPILL" => {
                    // SQLite-compatible PRAGMA cache_spill read (pragma2.test
                    // pragma2-4.1/4.2). Disabled reads as 0 regardless of any
                    // stored explicit size; enabled with no explicit size
                    // mirrors the current `cache_size` (SQLite's spill
                    // threshold defaults to the cache size until set).
                    let value = if !self.cache_spill_enabled {
                        0
                    } else {
                        self.cache_spill_explicit_size.unwrap_or(self.cache_size)
                    };
                    Ok(QueryResult {
                        columns: vec!["cache_spill".to_string()],
                        rows: vec![vec![Some(value.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "USER_VERSION" => {
                    // SQLite-compatible PRAGMA user_version read (pragma.test
                    // pragma-8.2.*, #6175). Default 0.
                    Ok(QueryResult {
                        columns: vec!["user_version".to_string()],
                        rows: vec![vec![Some(self.user_version.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "APPLICATION_ID" => {
                    // SQLite-compatible PRAGMA application_id read (pragma.test
                    // pragma-8.3.*, #6175). Default 0.
                    Ok(QueryResult {
                        columns: vec!["application_id".to_string()],
                        rows: vec![vec![Some(self.application_id.to_string())]],
                        row_count: 1,
                        execution_time_ms: None,
                        message: None,
                    })
                }
                "SCHEMA_VERSION" => {
                    // SQLite-compatible PRAGMA schema_version read (pragma.test
                    // pragma-8.1.*, #6175). Default 0; auto-incremented on
                    // every successful DDL statement / VACUUM (see the bump
                    // sites at each DDL statement's dispatch arm below).
                    Ok(QueryResult {
                        columns: vec!["schema_version".to_string()],
                        rows: vec![vec![Some(self.schema_version.to_string())]],
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

        // Schema-qualified table resolution. A bare `PRAGMA table_info(t)` follows
        // SQLite's shadowing rule — a TEMP table hides a main-schema table of the
        // same name — via the catalog's temp-first `get_table`. A schema-qualified
        // form instead pins the lookup to that schema: `PRAGMA temp.table_info(t)`
        // reads the TEMP table and `PRAGMA main.table_info(t)` reads the
        // main-schema table even when a TEMP table of the same name shadows it
        // (ticket #3320; pragma-6.6.3 / pragma-6.6.4). Routing the qualifier
        // straight through `get_table` (which resolves `temp` to this session's
        // temp schema and `main` to the default schema) yields the correct table
        // for each; an unknown schema resolves to nothing and produces an empty
        // result — SQLite reports no rows, not an error, for a missing table.
        let lookup = match &stmt.database {
            Some(db) => format!("{}.{}", db, table_name),
            None => table_name.clone(),
        };
        let schema = match self.db.catalog.get_table(&lookup) {
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

        // Recover per-column *declaration* facts that the catalog's affinity-only
        // `data_type` / `nullable` fields cannot express, by re-parsing the
        // verbatim CREATE TABLE text (`sql_source`). Several SQLite `table_info`
        // quirks depend on the original declaration rather than the internal
        // affinity/rowid state:
        //
        //   * The `type` column echoes the *declared* type text. A typeless
        //     column (`CREATE TABLE t(a)`) reports an empty type in SQLite, but
        //     VibeSQL folds it into BLOB affinity — so without this we'd wrongly
        //     print "BLOB". `type_source == None` marks the typeless case.
        //   * The `notnull` column reflects only an *explicit* NOT NULL clause.
        //     An `INTEGER PRIMARY KEY` rowid alias is internally non-nullable
        //     (VibeSQL sets `nullable = false`) yet SQLite reports `notnull = 0`
        //     for it. Deriving notnull from the explicit NOT NULL constraint in
        //     the source matches SQLite exactly.
        //   * The `pk` column reports the 1-based position of a column within the
        //     declared PRIMARY KEY. SQLite keys this off the *first* occurrence
        //     of each column in the declared key list but still advances the
        //     ordinal for repeated columns, so `PRIMARY KEY(a,b,a,c)` yields
        //     a=1, b=2, c=4 (the duplicate `a` consumes position 3). VibeSQL's
        //     catalog `primary_key` list is de-duplicated and loses that gap, so
        //     we recover the raw ordinals from the re-parsed table-level PK
        //     constraint.
        //
        // `decl_facts` is keyed by lowercase column name. Absent (no sql_source,
        // a CREATE ... AS SELECT with no explicit column list, or a re-parse
        // failure) means we fall back to the catalog-derived behavior below,
        // unchanged. `pk_source_positions` is likewise a best-effort override.
        //   * The `type` column echoes the *declared* type text verbatim, as
        //     written in the CREATE TABLE statement (only the surrounding
        //     delimiters of a bracketed/quoted type name are stripped). The
        //     catalog's affinity-only `data_type` is lossy — it renders
        //     `VARCHAR(45, 65)` as `VARCHAR(45)` — so we prefer the re-parsed
        //     `type_source`. `decl_type` holds the delimiter-stripped verbatim
        //     text; `None` marks a typeless column (empty type).
        //   * The `dflt_value` column echoes the *verbatim* DEFAULT expression
        //     source (e.g. `X'abcdef'`, `'abcde'`, `-1`, `CURRENT_TIME`) rather
        //     than a lossy `ToSql` re-render that uppercases blob hex and drops
        //     operator spacing. A single balanced outer parenthesis pair is
        //     stripped (`DEFAULT (5+3)` -> `5+3`), matching SQLite.
        let mut decl_facts: std::collections::HashMap<String, (bool, bool)> =
            std::collections::HashMap::new();
        let mut decl_types: std::collections::HashMap<String, Option<String>> =
            std::collections::HashMap::new();
        let mut default_sources: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        let mut pk_source_positions: Option<std::collections::HashMap<String, usize>> = None;
        if let Some(src) = schema.sql_source.as_deref() {
            if let Ok((vibesql_ast::Statement::CreateTable(create), dflt_srcs)) =
                vibesql_parser::Parser::parse_sql_with_default_sources(src)
            {
                if create.as_query.is_none() {
                    default_sources = dflt_srcs;
                    for col in &create.columns {
                        let is_typeless = col.type_source.is_none();
                        let explicit_not_null = col.constraints.iter().any(|c| {
                            matches!(
                                c.kind,
                                vibesql_ast::ColumnConstraintKind::NotNull
                                    | vibesql_ast::ColumnConstraintKind::NotNullWithConflict { .. }
                            )
                        });
                        decl_facts
                            .insert(col.name.to_lowercase(), (is_typeless, explicit_not_null));
                        // Delimiter-stripped verbatim declared-type text; `None`
                        // for a typeless column (reports empty type in SQLite).
                        let decl_type = col
                            .type_source
                            .as_deref()
                            .map(|ts| canonicalize_sqlite_decltype(&strip_type_delimiters(ts)));
                        decl_types.insert(col.name.to_lowercase(), decl_type);
                    }

                    // Derive raw pk ordinals from a table-level PRIMARY KEY
                    // constraint, preserving the duplicate-consumes-a-position
                    // rule. Column-level PKs (single column) are left to the
                    // catalog fallback, which already reports position 1.
                    for tc in &create.table_constraints {
                        if let vibesql_ast::TableConstraintKind::PrimaryKey { columns, .. } =
                            &tc.kind
                        {
                            let mut map = std::collections::HashMap::new();
                            for (idx, ic) in columns.iter().enumerate() {
                                if let Some(name) = ic.column_name() {
                                    // First occurrence wins; later duplicates
                                    // still advanced `idx`, leaving the gap.
                                    // Keyed by lowercase for case-insensitive
                                    // column matching (SQLite semantics).
                                    map.entry(name.to_lowercase()).or_insert(idx + 1);
                                }
                            }
                            if !map.is_empty() {
                                pk_source_positions = Some(map);
                            }
                            break;
                        }
                    }
                }
            }
        }

        // Build a name->pk-position map (1-based) for primary key lookups.
        // Prefer the source-derived ordinals (which honor SQLite's
        // duplicate-column gap); otherwise fall back to the catalog's
        // de-duplicated primary-key list.
        let pk_positions: std::collections::HashMap<String, usize> = match pk_source_positions {
            Some(map) => map,
            None => match schema.primary_key.as_ref() {
                Some(pk_cols) => pk_cols
                    .iter()
                    .enumerate()
                    .map(|(i, name)| (name.to_lowercase(), i + 1))
                    .collect(),
                None => std::collections::HashMap::new(),
            },
        };

        let mut rows: Vec<Vec<Option<String>>> = Vec::with_capacity(schema.columns.len());
        for (cid, column) in schema.columns.iter().enumerate() {
            let decl = decl_facts.get(&column.name.to_lowercase());

            // Type column: SQLite reports the declared type verbatim, exactly as
            // supplied in the CREATE TABLE statement (delimiters aside). Prefer
            // the re-parsed `type_source` (delimiter-stripped) so declarations
            // the catalog's affinity mapping cannot round-trip — e.g.
            // `VARCHAR(45, 65)` — echo faithfully. A typeless column reports the
            // empty string. Fall back to the canonical affinity name only when
            // no source declaration is available (programmatic table, reload
            // without `sql_source`, or a re-parse failure).
            let type_str = match decl_types.get(&column.name.to_lowercase()) {
                Some(Some(decl_type)) => decl_type.clone(),
                Some(None) => String::new(),
                None => {
                    if matches!(decl, Some((true, _))) {
                        String::new()
                    } else {
                        sqlite_declared_type(&column.data_type, column.is_exact_integer_type)
                    }
                }
            };

            // notnull: 1 only for an *explicit* NOT NULL clause. An INTEGER
            // PRIMARY KEY rowid alias is internally non-nullable but SQLite
            // still reports notnull=0. Prefer the re-parsed declaration; fall
            // back to the catalog nullable flag when no source is available.
            let notnull = match decl {
                Some((_, explicit_not_null)) => {
                    if *explicit_not_null {
                        1
                    } else {
                        0
                    }
                }
                None => {
                    if !column.nullable {
                        1
                    } else {
                        0
                    }
                }
            };

            // dflt_value: echo the verbatim DEFAULT source text (SQLite does),
            // falling back to a `ToSql` re-render only when the verbatim source
            // is unavailable (programmatic table or reload without `sql_source`).
            // The verbatim text preserves original spelling that `ToSql` loses:
            // blob-literal hex casing (`X'abcdef'`, not `x'ABCDEF'`), quoted
            // string delimiters, and operator spacing.
            let dflt_value: Option<String> = default_sources
                .get(&column.name.to_lowercase())
                .map(|s| strip_outer_parens(s))
                .or_else(|| {
                    column.default_value.as_ref().map(|e| {
                        use vibesql_ast::pretty_print::ToSql;
                        e.to_sql()
                    })
                });

            // pk: 1-based position within the primary key, or 0 if not PK.
            let pk = pk_positions.get(&column.name.to_lowercase()).copied().unwrap_or(0);

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

    /// PRAGMA collation_list - SQLite-compatible
    ///
    /// Returns one row (`seq`, `name`) per registered collating sequence. VibeSQL
    /// ships the three built-in collations SQLite always registers — BINARY,
    /// NOCASE and RTRIM — listed most-recently-registered first (BINARY is
    /// registered first internally, so it sorts last), matching SQLite's
    /// `pragma-11.1` fixture `{seq 0 name RTRIM seq 1 name NOCASE seq 2 name
    /// BINARY}`. User-defined collations registered through the C API
    /// (`db collate ...`) cannot be added through the CLI, so they are not
    /// reported.
    fn execute_pragma_collation_list(&self) -> anyhow::Result<QueryResult> {
        let columns = vec!["seq".to_string(), "name".to_string()];
        let names = ["RTRIM", "NOCASE", "BINARY"];
        let rows: Vec<Vec<Option<String>>> = names
            .iter()
            .enumerate()
            .map(|(seq, name)| vec![Some(seq.to_string()), Some((*name).to_string())])
            .collect();
        let row_count = rows.len();
        Ok(QueryResult { columns, rows, row_count, execution_time_ms: None, message: None })
    }

    /// PRAGMA index_list(table-name) - SQLite-compatible
    ///
    /// Returns one row per index on the named table with:
    ///   seq (index number), name, unique (0/1), origin, partial (0/1).
    ///
    /// `origin` is `c` for an index created by CREATE INDEX, `u` for the implicit
    /// index backing a UNIQUE constraint, and `pk` for the implicit index backing
    /// a (non-rowid) PRIMARY KEY. Implicit indexes are named `sqlite_autoindex_*`
    /// and are materialized in the catalog, so they are reported here. Indexes are
    /// listed newest-first (matching SQLite, which walks its per-table index list
    /// in reverse creation order).
    fn execute_pragma_index_list(
        &self,
        stmt: &vibesql_ast::PragmaStmt,
    ) -> anyhow::Result<QueryResult> {
        let columns = vec![
            "seq".to_string(),
            "name".to_string(),
            "unique".to_string(),
            "origin".to_string(),
            "partial".to_string(),
        ];

        let empty = QueryResult {
            columns: columns.clone(),
            rows: Vec::new(),
            row_count: 0,
            execution_time_ms: None,
            message: None,
        };

        let table_name = match &stmt.value {
            Some(vibesql_ast::PragmaValue::Identifier(name)) => name.clone(),
            Some(vibesql_ast::PragmaValue::String(name)) => name.clone(),
            // No table argument supplied - SQLite returns no rows.
            _ => return Ok(empty),
        };

        // Schema-qualified handling: VibeSQL carries a single schema, so a
        // qualifier other than the current schema / `main` yields no rows.
        let current_schema = self.db.catalog.get_current_schema().to_string();
        if let Some(ref schema) = stmt.database {
            let is_current =
                schema.eq_ignore_ascii_case(&current_schema) || schema.eq_ignore_ascii_case("main");
            if !is_current {
                return Ok(empty);
            }
        }

        // Unknown table -> no rows (SQLite is silent for index_list on a missing
        // table).
        let table = match self.db.catalog.get_table(&table_name) {
            Some(t) => t,
            None => return Ok(empty),
        };

        // Primary-key column set, used to distinguish a `pk`-origin autoindex
        // from a `u`-origin (UNIQUE) autoindex.
        let pk_cols: Option<Vec<String>> =
            table.primary_key.as_ref().map(|cols| cols.iter().map(|c| c.to_lowercase()).collect());

        // SQLite lists indexes in reverse creation order (newest first); the
        // catalog stores them oldest-first, so reverse for parity.
        let mut indexes = self.db.catalog.get_table_indexes(&table_name);
        indexes.reverse();

        let mut rows: Vec<Vec<Option<String>>> = Vec::with_capacity(indexes.len());
        for (seq, index) in indexes.iter().enumerate() {
            let unique = if index.is_unique { 1 } else { 0 };
            let partial = if index.where_clause.is_some() { 1 } else { 0 };

            let origin = if index.name.to_lowercase().starts_with("sqlite_autoindex_") {
                // Implicit index: classify as pk vs u by comparing its key
                // columns to the table's declared PRIMARY KEY.
                let index_cols: Vec<String> = index
                    .columns
                    .iter()
                    .filter_map(|c| c.column_name().map(|n| n.to_lowercase()))
                    .collect();
                match &pk_cols {
                    Some(pk) if !pk.is_empty() && *pk == index_cols => "pk",
                    _ => "u",
                }
            } else {
                "c"
            };

            rows.push(vec![
                Some(seq.to_string()),
                Some(index.name.clone()),
                Some(unique.to_string()),
                Some(origin.to_string()),
                Some(partial.to_string()),
            ]);
        }

        let row_count = rows.len();
        Ok(QueryResult { columns, rows, row_count, execution_time_ms: None, message: None })
    }

    /// PRAGMA index_info(index-name) / index_xinfo(index-name) - SQLite-compatible
    ///
    /// `index_info` returns one row per key column of the named index:
    ///   seqno (rank within the index, 0-based), cid (rank of the column within
    ///   the table, or -1 for a rowid/expression), name (column name, NULL for a
    ///   rowid or expression column).
    ///
    /// `index_xinfo` (when `extended` is true) adds three columns —
    ///   desc (1 if DESC), coll (collation name), key (1 for a key column, 0 for
    ///   an auxiliary column) — and additionally lists the auxiliary columns that
    ///   SQLite appends to every index on a rowid table: a trailing rowid entry
    ///   (cid -1, name NULL, key 0).
    fn execute_pragma_index_info(
        &self,
        stmt: &vibesql_ast::PragmaStmt,
        extended: bool,
    ) -> anyhow::Result<QueryResult> {
        let columns = if extended {
            vec![
                "seqno".to_string(),
                "cid".to_string(),
                "name".to_string(),
                "desc".to_string(),
                "coll".to_string(),
                "key".to_string(),
            ]
        } else {
            vec!["seqno".to_string(), "cid".to_string(), "name".to_string()]
        };

        let empty = QueryResult {
            columns: columns.clone(),
            rows: Vec::new(),
            row_count: 0,
            execution_time_ms: None,
            message: None,
        };

        let index_name = match &stmt.value {
            Some(vibesql_ast::PragmaValue::Identifier(name)) => name.clone(),
            Some(vibesql_ast::PragmaValue::String(name)) => name.clone(),
            // No index argument supplied - SQLite returns no rows.
            _ => return Ok(empty),
        };

        // Unknown index -> no rows (SQLite is silent for index_info on a missing
        // index).
        let index = match self.db.catalog.find_index_by_name(&index_name) {
            Some(i) => i,
            None => return Ok(empty),
        };

        // Resolve the backing table so key columns can be mapped to their table
        // column position (cid).
        let table = self.db.catalog.get_table(&index.table_name);

        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        for (seqno, column) in index.columns.iter().enumerate() {
            let (cid, name): (i64, Option<String>) = match column.column_name() {
                Some(col_name) => {
                    let cid = table
                        .and_then(|t| t.get_column_index(col_name))
                        .map(|i| i as i64)
                        .unwrap_or(-1);
                    (cid, Some(col_name.to_string()))
                }
                // Expression column: SQLite reports cid -2 (not -1, which is
                // reserved for a rowid reference) and a NULL name (pragma.test
                // 23.2e, #6175).
                None => (-2, None),
            };

            if extended {
                let desc = if matches!(column.order(), vibesql_catalog::SortOrder::Descending) {
                    1
                } else {
                    0
                };
                // Collation echoed by `coll`: an explicit `COLLATE` on this
                // index-column wins; otherwise fall back to the underlying
                // table column's declared collation; otherwise BINARY
                // (SQLite's implicit default). Matches pragma.test 23.2d/2e
                // (#6175).
                let coll = column
                    .explicit_collation()
                    .map(|s| s.to_string())
                    .or_else(|| {
                        column.column_name().and_then(|col_name| {
                            table
                                .and_then(|t| t.get_column(col_name))
                                .and_then(|c| c.collation.clone())
                        })
                    })
                    .unwrap_or_else(|| "BINARY".to_string());
                rows.push(vec![
                    Some(seqno.to_string()),
                    Some(cid.to_string()),
                    name,
                    Some(desc.to_string()),
                    Some(coll),
                    Some("1".to_string()),
                ]);
            } else {
                rows.push(vec![Some(seqno.to_string()), Some(cid.to_string()), name]);
            }
        }

        // index_xinfo lists the auxiliary columns appended to make the index a
        // covering key. For an ordinary rowid table this is the trailing rowid
        // (cid -1, name NULL, key 0). index_info omits auxiliary columns
        // (R-23114-21695).
        if extended {
            let seqno = index.columns.len();
            rows.push(vec![
                Some(seqno.to_string()),
                Some("-1".to_string()),
                None,
                Some("0".to_string()),
                Some("BINARY".to_string()),
                Some("0".to_string()),
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

/// Extract the raw textual spelling of a PRAGMA value, regardless of how the
/// parser classified it (bare identifier, string literal, or number). Used by
/// the enum-style config PRAGMAs (`auto_vacuum`, `temp_store`) that accept both
/// symbolic (`full`, `memory`) and numeric (`1`, `2`) spellings.
fn pragma_value_text(value: &vibesql_ast::PragmaValue) -> &str {
    match value {
        vibesql_ast::PragmaValue::Identifier(s)
        | vibesql_ast::PragmaValue::String(s)
        | vibesql_ast::PragmaValue::Number(s)
        | vibesql_ast::PragmaValue::SignedNumber(s) => s.as_str(),
    }
}

/// Normalize a `PRAGMA auto_vacuum = <value>` argument to its canonical integer
/// code, matching SQLite's parse rules (pragma.test pragma-17):
///   `none` / `0` / any other integer (incl. negative or out-of-range) -> 0
///   `full` / `1`        -> 1
///   `incremental` / `2` -> 2
/// Symbolic names are case-insensitive.
fn normalize_auto_vacuum(value: &vibesql_ast::PragmaValue) -> i64 {
    let text = pragma_value_text(value);
    match text.to_ascii_uppercase().as_str() {
        "NONE" => 0,
        "FULL" => 1,
        "INCREMENTAL" => 2,
        _ => match text.trim().parse::<i64>() {
            Ok(1) => 1,
            Ok(2) => 2,
            _ => 0,
        },
    }
}

/// Normalize a `PRAGMA temp_store = <value>` argument to its canonical integer
/// code, matching SQLite's parse rules (pragma.test pragma-18):
///   `default` / `0` / any other integer (incl. negative or out-of-range) -> 0
///   `file` / `1`   -> 1
///   `memory` / `2` -> 2
/// Symbolic names are case-insensitive.
fn normalize_temp_store(value: &vibesql_ast::PragmaValue) -> i64 {
    let text = pragma_value_text(value);
    match text.to_ascii_uppercase().as_str() {
        "DEFAULT" => 0,
        "FILE" => 1,
        "MEMORY" => 2,
        _ => match text.trim().parse::<i64>() {
            Ok(1) => 1,
            Ok(2) => 2,
            _ => 0,
        },
    }
}

/// The default `PRAGMA encoding` value for a fresh session, matching SQLite's
/// default text encoding.
fn default_encoding() -> String {
    "UTF-8".to_string()
}

/// SQLite's default `PRAGMA synchronous` level (2 = FULL).
const SQLITE_DEFAULT_SYNCHRONOUS: i64 = 2;

/// SQLite's `SQLITE_DEFAULT_CACHE_SIZE` compile-time constant: the value
/// `PRAGMA cache_size` / `PRAGMA default_cache_size` report when no explicit
/// size has ever been set.
const SQLITE_DEFAULT_CACHE_SIZE: i64 = -2000;

/// Mirrors SQLite's `getSafetyLevel()` (pragma.c) used by `PRAGMA
/// synchronous = <value>`: a numeric string is parsed via a C-style
/// leading-digit `atoi` (a non-digit first character, including a leading
/// `-`, is NOT treated as numeric — matching `sqlite3Isdigit(*z)`); a
/// recognized keyword maps to its table value; anything else (including the
/// unlisted `NORMAL` spelling) falls back to 1. This is the *raw*
/// pre-adjustment value — `synchronous_read_value` below applies SQLite's
/// `((raw+1) & PAGER_SYNCHRONOUS_MASK)` wraparound to get the value actually
/// stored/reported.
fn parse_synchronous_raw(value: &vibesql_ast::PragmaValue) -> i64 {
    let text = pragma_value_text(value);
    let trimmed = text.trim();
    if trimmed.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        // C `atoi`-style: parse the leading run of digits, ignore the rest.
        let digits: String = trimmed.chars().take_while(|c| c.is_ascii_digit()).collect();
        return digits.parse::<i64>().unwrap_or(0);
    }
    match trimmed.to_ascii_lowercase().as_str() {
        "on" => 1,
        "no" => 0,
        "off" => 0,
        "false" => 0,
        "yes" => 1,
        "true" => 1,
        "extra" => 3,
        "full" => 2,
        // SQLite's keyword table has no "normal" entry — it (like any other
        // unrecognized spelling) falls through to the default of 1, which
        // happens to be exactly NORMAL's value.
        _ => 1,
    }
}

/// Applies SQLite's `((raw+1) & PAGER_SYNCHRONOUS_MASK)` wraparound (with the
/// "never let the stored level be 0" correction) and returns the value that
/// `PRAGMA synchronous` reports back afterward — matching SQLite's exact
/// arithmetic, including its quirky handling of out-of-range numeric input
/// (pragma.test pragma-1.13/1.14.x: `synchronous=8` reads back as `0`,
/// `=10` reads back as `2`).
fn synchronous_read_value(raw: i64) -> i64 {
    const PAGER_SYNCHRONOUS_MASK: i64 = 0x07;
    let mut level = (raw + 1) & PAGER_SYNCHRONOUS_MASK;
    if level == 0 {
        level = 1;
    }
    level - 1
}

/// C-`atoi`-style integer parse used by `cache_size` / `default_cache_size`:
/// parses an optional leading sign followed by a run of digits and ignores
/// any trailing non-digit content; returns 0 if there are no usable leading
/// digits (matching SQLite's `sqlite3Atoi`).
fn pragma_value_atoi(value: &vibesql_ast::PragmaValue) -> i64 {
    let text = pragma_value_text(value).trim();
    let mut chars = text.chars().peekable();
    let mut sign = 1i64;
    if let Some(&c) = chars.peek() {
        if c == '-' {
            sign = -1;
            chars.next();
        } else if c == '+' {
            chars.next();
        }
    }
    let digits: String = chars.take_while(|c| c.is_ascii_digit()).collect();
    sign * digits.parse::<i64>().unwrap_or(0)
}

/// Resolves the `default_cache_size` persisted-cookie stand-in to the value
/// `PRAGMA default_cache_size` reports: a nonzero cookie reports its
/// absolute value, an unset (zero) cookie reports `SQLITE_DEFAULT_CACHE_SIZE`
/// (mirrors SQLite's `OP_ReadCookie` + `IfPos`/`Subtract` VDBE program).
fn resolve_cache_size_cookie(cookie: i64) -> i64 {
    if cookie != 0 {
        cookie.abs()
    } else {
        SQLITE_DEFAULT_CACHE_SIZE
    }
}

/// Normalize a `PRAGMA encoding = <value>` argument to SQLite's canonical
/// echoed spelling, matching `sqlite3_db_config`/`pragma.c`'s `encnames[]`
/// table (numcast.test numcast-utf8.0/utf16le.0/utf16be.0):
///   `utf8` / `utf-8`               -> `UTF-8`
///   `utf16le` / `utf-16le`         -> `UTF-16le`
///   `utf16be` / `utf-16be`         -> `UTF-16be`
///   `utf16` / `utf-16`             -> native byte order (`UTF-16le` here)
/// Matching is case-insensitive and tolerant of an optional `-` before `8`/`16`
/// (SQLite accepts both spellings). An unrecognized value returns `None` so
/// the caller can leave the previous setting untouched, matching SQLite's
/// silent-no-op behavior for an invalid encoding name.
///
/// VibeSQL only ever stores TEXT as UTF-8 internally — this normalizes the
/// pragma's *echoed* value only, it does not switch the actual storage
/// encoding.
fn normalize_encoding(value: &vibesql_ast::PragmaValue) -> Option<String> {
    let text = pragma_value_text(value);
    let canon: String = text.trim().to_ascii_lowercase().chars().filter(|&c| c != '-').collect();
    match canon.as_str() {
        "utf8" => Some("UTF-8".to_string()),
        "utf16le" => Some("UTF-16le".to_string()),
        "utf16be" => Some("UTF-16be".to_string()),
        // Bare "UTF-16" resolves to the host's native byte order; VibeSQL
        // targets little-endian platforms (SQLite: SQLITE_UTF16NATIVE).
        "utf16" => Some("UTF-16le".to_string()),
        _ => None,
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
