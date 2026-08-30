use std::time::Instant;

use vibesql_parser::parse_with_arena_fallback;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

// Submodules
mod copy_handler;
pub mod display;
mod pragma;
mod schema_qualify;
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
    /// PRAGMA temp_store_directory session setting (SQLite-compatible,
    /// default "" — unset). Get/set round-trips like SQLite (pragma.test
    /// pragma-9.4..9.8): setting a nonexistent path errors with "not a
    /// writable directory"; setting `''` resets to the default. VibeSQL has
    /// no separate temp-file directory to actually redirect (TEMP tables are
    /// demoted to persistent, matching `temp_store` above), so the value is
    /// advisory only.
    temp_store_directory: String,
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
    /// PRAGMA page_size session setting (SQLite-compatible; default 4096 —
    /// `SQLITE_DEFAULT_PAGE_SIZE`). VibeSQL's storage is not paged, so nothing
    /// is actually resized, but the value is stored and echoed exactly like
    /// SQLite: a set is accepted only for a power of two in [512, 65536] and
    /// silently ignored otherwise (`sqlite3BtreeSetPageSize`'s guard —
    /// pragma4.test 1.18 vs 1.19). It is load-bearing beyond the echo:
    /// `cache_spill` resolves its negative "KiB budget" arguments to page
    /// counts by dividing by this value (pragma2-5.3).
    ///
    /// Known gap vs. real SQLite: SQLite refuses the change once the database
    /// file already holds pages; VibeSQL has no page store to consult, so a
    /// valid size is always accepted.
    page_size: i64,
    /// PRAGMA cache_spill session setting (SQLite-compatible; default ON with
    /// a spill threshold of 1 page, matching `sqlite3PcacheOpen`'s
    /// `szSpill = 1`). VibeSQL has no pager to actually spill dirty pages, but
    /// it reproduces SQLite's `sqlite3PcacheSetSpillsize` arithmetic so get/set
    /// round-trips match (pragma2.test pragma2-4.*/5.*): when disabled it reads
    /// as 0; otherwise it reads as
    /// `max(numberOfCachePages(cache_size), cache_spill_size)`.
    cache_spill_enabled: bool,
    /// The spill threshold in *pages* (SQLite's `PCache.szSpill`). A negative
    /// `PRAGMA cache_spill=-N` argument is a KiB budget and is converted to a
    /// page count against `page_size` at set time, exactly like SQLite.
    cache_spill_size: i64,
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
    /// Highest LSN observed when each file-backed attachment was loaded
    /// (canonical alias -> LSN), or 0 when nothing was recovered for it
    /// (#6531).
    ///
    /// Keyed by the lowercase alias this session attached the file under.
    /// Used to stamp the attachment's own checkpoint at a strictly higher LSN
    /// on save/detach, so the checkpoint the attachment writes always wins
    /// recovery's newest-checkpoint selection instead of losing to a
    /// checkpoint or WAL tail this session already recovered from (#5766's
    /// monotonicity hazard, applied to attachments).
    attached_recovered_lsn: std::collections::HashMap<String, u64>,
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
    /// PRAGMA automatic_index session setting (SQLite-compatible, default ON).
    /// SQLite's query planner may build a transient in-memory index for the
    /// duration of a single query when no suitable index exists; VibeSQL's
    /// planner does not implement this optimization, but the setting is
    /// parsed, stored and echoed like SQLite so introspection round-trips
    /// (pragma4.test pragma4-1.2, #6175).
    automatic_index: bool,
    /// PRAGMA cell_size_check session setting (SQLite-compatible, default
    /// OFF). A B-tree page/cell integrity sanity check with no VibeSQL
    /// equivalent (no page-based storage); stored and echoed only
    /// (pragma4.test pragma4-1.6, #6175).
    cell_size_check: bool,
    /// PRAGMA checkpoint_fullfsync session setting (SQLite-compatible,
    /// default OFF). Controls whether WAL checkpoint fsyncs use F_FULLFSYNC
    /// on macOS; VibeSQL's WAL implementation has no equivalent knob, so the
    /// value is stored and echoed only (pragma4.test pragma4-1.7, #6175).
    checkpoint_fullfsync: bool,
    /// PRAGMA empty_result_callbacks session setting (SQLite-compatible,
    /// default OFF). Legacy C-API knob controlling whether the row callback
    /// fires once for a zero-row result; no SQL-visible effect, stored and
    /// echoed only (pragma4.test pragma4-1.11, #6175).
    empty_result_callbacks: bool,
    /// PRAGMA fullfsync session setting (SQLite-compatible, default OFF).
    /// Controls whether ordinary (non-checkpoint) fsyncs use F_FULLFSYNC on
    /// macOS; VibeSQL's WAL implementation has no equivalent knob, so the
    /// value is stored and echoed only (pragma4.test pragma4-1.15, #6175).
    fullfsync: bool,
    /// PRAGMA query_only session setting (SQLite-compatible, default OFF).
    /// When ON, real SQLite rejects any data-modifying statement with
    /// "attempt to write a readonly database". VibeSQL stores and echoes the
    /// flag (pragma4.test pragma4-1.20, #6175); enforcing the write-rejection
    /// behavior is deferred follow-up work, not exercised by the covered
    /// test files.
    query_only: bool,
    /// PRAGMA read_uncommitted session setting (SQLite-compatible, default
    /// OFF). Controls SQLite's shared-cache isolation level; VibeSQL has no
    /// shared-cache mode, so the value is stored and echoed only
    /// (pragma4.test pragma4-1.21, #6175).
    read_uncommitted: bool,
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

/// Surface expression-index-rebuild diagnostics from
/// `vibesql_executor::rebuild_pending_expression_indexes` on stderr, but only
/// when stderr is an actual terminal a human is watching interactively.
///
/// Real SQLite never emits a diagnostic when it cannot resolve a persisted
/// (e.g. `PRAGMA writable_schema`-created) expression index — the object
/// simply stays unusable until an evaluation is actually attempted. VibeSQL's
/// warning is a best-effort, human-facing extra on top of that, so it must
/// never appear in output any other consumer might capture: piped stdin,
/// `-c`/`-f` script execution, or (most concretely) the SQLite TCL test
/// harness, which merges the CLI's stderr into its captured stdout at every
/// `exec` call site — an unconditional `eprintln!` here would corrupt the
/// captured query-result text for any test that happens to trigger a rebuild
/// (issue #6621). Gating on `stderr().is_terminal()` keeps the diagnostic
/// available for an ordinary interactive `vibesql` REPL session (where stderr
/// really is a separate terminal stream a human can see) while suppressing it
/// everywhere output is redirected or captured.
fn print_expression_index_rebuild_warnings(warnings: &[String]) {
    use std::io::IsTerminal;
    if warnings.is_empty() || !std::io::stderr().is_terminal() {
        return;
    }
    for warning in warnings {
        eprintln!("{}", warning);
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

/// True when `path` carries WAL durability siblings — a checkpoint archive
/// holding at least one `.vchk`, or a non-empty WAL file (#6531).
///
/// Either one means the raw file at `path` is NOT the freshest state of that
/// database: a WAL-active session writes its state into the checkpoint
/// archive and leaves the snapshot file untouched. `ATTACH` must therefore
/// consult the siblings exactly like a direct `main` open does, rather than
/// deserializing the (possibly arbitrarily stale) snapshot.
fn has_wal_siblings(paths: &wal::WalPaths) -> bool {
    paths.has_checkpoint_files()
        || std::fs::metadata(&paths.wal_path).map(|m| m.len() > 0).unwrap_or(false)
}

/// Load an attached database file the way a direct `main` open of that same
/// path would (#6531).
///
/// When the path has WAL durability siblings (see [`has_wal_siblings`]) this
/// runs the same checkpoint-load + WAL-replay recovery
/// `SqlExecutor::new_with_options`'s WAL-active branch runs
/// (`RecoveryManager::recover_with_base`), including the legacy
/// snapshot-as-base rule from #5807: a raw snapshot is used as the recovery
/// base only while no checkpoint exists yet, since checkpoints are the newer
/// truth once any exists. Without siblings it falls back to the plain
/// snapshot/dump load ([`load_database_file`]).
///
/// This is unconditional — it does not depend on whether *this* session is
/// WAL-active. The freshest state of the attached file is a property of that
/// file, not of the attaching session, so a snapshot-only session must not
/// read a stale view of a WAL-active database either.
///
/// Recovery strictness matches a `main` open's default: an unreadable newest
/// checkpoint is a hard error rather than a silent fall back to older state
/// (`--recover-fallback` is a `main`-open opt-in and deliberately does not
/// extend to attachments).
///
/// Returns the loaded database plus the highest LSN recovery observed (0 when
/// no recovery ran); the caller records it so the attachment's own checkpoint
/// can be stamped past it.
fn load_attached_database_file(path: &str) -> anyhow::Result<(Database, u64)> {
    let paths = wal::WalPaths::derive(path);
    if !has_wal_siblings(&paths) {
        return Ok((load_database_file(path)?, 0));
    }

    let has_checkpoints = paths.has_checkpoint_files();
    let base = if !has_checkpoints && std::fs::metadata(path).map(|m| m.len() > 0).unwrap_or(false)
    {
        Some(load_database_file(path)?)
    } else {
        None
    };

    let manager =
        vibesql_storage::wal::RecoveryManager::new(&paths.checkpoint_dir).with_wal(&paths.wal_path);
    let (mut db, stats) = manager.recover_with_base(base).map_err(|e| {
        anyhow::anyhow!("Failed to recover attached WAL-backed database at {}: {}", path, e)
    })?;

    // Same post-load repair a `main` open performs: the snapshot/checkpoint
    // loader cannot evaluate index expressions, so expression-index bodies
    // come back empty and must be rebuilt or they silently return no rows
    // (#5784).
    let warnings = vibesql_executor::rebuild_pending_expression_indexes(&mut db).map_err(|e| {
        anyhow::anyhow!("Failed to rebuild expression indexes after recovering {}: {}", path, e)
    })?;
    print_expression_index_rebuild_warnings(&warnings);

    Ok((db, stats.last_lsn))
}

/// Translate a catalog-level [`vibesql_catalog::IndexType`] (the type
/// actually recorded for an index) into the AST-level
/// [`vibesql_ast::IndexType`] a synthetic `CreateIndexStmt` needs (#6407).
///
/// Used only when re-homing an attached schema's indexes on `ATTACH` of an
/// existing file (`load_attached_schema_from_file`): the loaded standalone
/// database's catalog metadata is the only surviving record of the original
/// index's type once its physical body has been left behind in the
/// throwaway `Database`. `vibesql_catalog::IndexType::Hash` denotes an
/// auto-generated PRIMARY KEY/UNIQUE-constraint index, which is filtered out
/// by the `pk_`/`sqlite_autoindex_` name-prefix skip before this is ever
/// called; it is mapped to a plain (non-unique) `BTree` here purely as a
/// defensive fallback that can never actually be exercised on that path.
fn ast_index_type_from_catalog(
    index_type: &vibesql_catalog::IndexType,
    unique: bool,
) -> vibesql_ast::IndexType {
    fn convert_metric(
        m: vibesql_catalog::VectorDistanceMetric,
    ) -> vibesql_ast::VectorDistanceMetric {
        match m {
            vibesql_catalog::VectorDistanceMetric::L2 => vibesql_ast::VectorDistanceMetric::L2,
            vibesql_catalog::VectorDistanceMetric::Cosine => {
                vibesql_ast::VectorDistanceMetric::Cosine
            }
            vibesql_catalog::VectorDistanceMetric::InnerProduct => {
                vibesql_ast::VectorDistanceMetric::InnerProduct
            }
        }
    }

    match index_type {
        vibesql_catalog::IndexType::BTree | vibesql_catalog::IndexType::Hash => {
            vibesql_ast::IndexType::BTree { unique }
        }
        vibesql_catalog::IndexType::RTree => vibesql_ast::IndexType::Spatial,
        vibesql_catalog::IndexType::Fulltext => vibesql_ast::IndexType::Fulltext,
        vibesql_catalog::IndexType::IVFFlat { metric, lists } => {
            vibesql_ast::IndexType::IVFFlat { metric: convert_metric(*metric), lists: *lists }
        }
        vibesql_catalog::IndexType::Hnsw { metric, m, ef_construction } => {
            vibesql_ast::IndexType::Hnsw {
                metric: convert_metric(*metric),
                m: *m,
                ef_construction: *ef_construction,
            }
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
                let warnings = vibesql_executor::rebuild_pending_expression_indexes(&mut db)
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to rebuild expression indexes after loading {}: {}",
                            db_path,
                            e
                        )
                    })?;
                print_expression_index_rebuild_warnings(&warnings);
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
                    temp_store_directory: String::new(),
                    encoding: default_encoding(),
                    synchronous: SQLITE_DEFAULT_SYNCHRONOUS,
                    cache_size: SQLITE_DEFAULT_CACHE_SIZE,
                    default_cache_size_cookie: 0,
                    cache_spill_enabled: true,
                    cache_spill_size: SQLITE_DEFAULT_SPILL_PAGES,
                    page_size: SQLITE_DEFAULT_PAGE_SIZE,
                    user_version: 0,
                    application_id: 0,
                    schema_version: 0,
                    wal_state: Some(wal_state),
                    attached_recovered_lsn: std::collections::HashMap::new(),
                    db_path: Some(db_path.clone()),
                    _db_lock: db_lock,
                    automatic_index: true,
                    cell_size_check: false,
                    checkpoint_fullfsync: false,
                    empty_result_callbacks: false,
                    fullfsync: false,
                    query_only: false,
                    read_uncommitted: false,
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
        let warnings = vibesql_executor::rebuild_pending_expression_indexes(&mut db)
            .map_err(|e| anyhow::anyhow!("Failed to rebuild expression indexes: {}", e))?;
        print_expression_index_rebuild_warnings(&warnings);

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
            temp_store_directory: String::new(),
            encoding: default_encoding(),
            synchronous: SQLITE_DEFAULT_SYNCHRONOUS,
            cache_size: SQLITE_DEFAULT_CACHE_SIZE,
            default_cache_size_cookie: 0,
            cache_spill_enabled: true,
            cache_spill_size: SQLITE_DEFAULT_SPILL_PAGES,
            page_size: SQLITE_DEFAULT_PAGE_SIZE,
            user_version: 0,
            application_id: 0,
            schema_version: 0,
            wal_state: None,
            attached_recovered_lsn: std::collections::HashMap::new(),
            db_path,
            _db_lock: db_lock,
            automatic_index: true,
            cell_size_check: false,
            checkpoint_fullfsync: false,
            empty_result_callbacks: false,
            fullfsync: false,
            query_only: false,
            read_uncommitted: false,
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

        // Parse SQL using arena fallback for SELECT statements (preserves original case in
        // source_text)
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
                        // NULL values are represented as None to distinguish from the literal
                        // string "NULL"
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
                            // The "QUERY PLAN" header is now included in the data for TCL test
                            // compatibility
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
            vibesql_ast::Statement::Attach(attach_stmt) => {
                self.execute_attach(&attach_stmt)?;
                result.row_count = 0;
            }
            vibesql_ast::Statement::Detach(detach_stmt) => {
                self.execute_detach(&detach_stmt)?;
                result.row_count = 0;
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
    ///
    /// File-backed attached databases (#6362) are persisted first,
    /// unconditionally: attachment persistence is deliberately snapshot-only
    /// regardless of whether the *main* database is WAL-active, so this one
    /// call covers both branches below rather than being duplicated into each.
    pub fn save_database(&mut self, path: &str) -> anyhow::Result<()> {
        self.save_attached_schemas()?;

        if let Some(wal_state) = self.wal_state.as_mut() {
            return wal_state
                .checkpoint(&self.db)
                .map_err(|e| anyhow::anyhow!("Failed to checkpoint WAL database: {}", e));
        }

        self.db
            .save_sql_dump(path)
            .map_err(|e| anyhow::anyhow!("Failed to save database to {}: {}", path, e))
    }

    /// Persist every file-backed attached schema to its own file (#6362).
    ///
    /// `:memory:` and the empty path (session-scoped, never-yet-saved
    /// attachments) are skipped — matching Phase 1's "nothing about
    /// `:memory:` is ever persisted" semantics and SQLite's own per-connection
    /// ATTACH scoping (only the attached file's *contents* persist; the
    /// attachment registry itself never does).
    fn save_attached_schemas(&self) -> anyhow::Result<()> {
        for attached in self.db.catalog.attached_databases() {
            if attached.path == ":memory:" || attached.path.is_empty() {
                continue;
            }
            self.persist_attached_schema(&attached.name, &attached.path)?;
        }
        Ok(())
    }

    /// Persist one file-backed attached schema to its own path.
    ///
    /// Two artifacts, deliberately (#6362 + #6531):
    ///
    /// 1. The self-contained SQL dump at `path` — the snapshot form a snapshot-only session (or a
    ///    session with no checkpoint archive present) reads back.
    /// 2. When this session is WAL-active, a **checkpoint** in that path's own checkpoint archive,
    ///    stamped past everything this session recovered from it. This is what gives an alias-side
    ///    write the same durability a `main`-schema write has: a WAL-active *direct* open of the
    ///    path recovers from the checkpoint archive and ignores the raw snapshot entirely, so
    ///    writing only the dump left the two access paths permanently diverged (#6531,
    ///    pragma4-4.4.3).
    ///
    /// Both artifacts are written from the same state, so whichever one a
    /// later open consults it sees the same database. Step 2 materializes
    /// that state by re-loading the dump written in step 1 — by construction
    /// exactly what a later `ATTACH` (or direct open) would reconstruct from
    /// it, so the checkpoint can never disagree with the snapshot beside it.
    ///
    /// Ordering is fail-safe: the dump is written atomically (temp + rename)
    /// first, and the checkpoint only afterwards, so a failure at any point
    /// leaves a readable database behind.
    fn persist_attached_schema(&self, alias: &str, path: &str) -> anyhow::Result<()> {
        self.db.save_attached_schema_sql_dump(alias, path).map_err(|e| {
            anyhow::anyhow!("Failed to save attached database '{}' to {}: {}", alias, path, e)
        })?;

        let Some(wal_state) = self.wal_state.as_ref() else {
            return Ok(());
        };

        let standalone = load_database_file(path).map_err(|e| {
            anyhow::anyhow!(
                "Failed to re-read the just-written dump for attached database '{}' at {} \
                 while checkpointing it: {}",
                alias,
                path,
                e
            )
        })?;
        let recovered_lsn = self.attached_recovered_lsn.get(alias).copied().unwrap_or(0);
        wal_state.checkpoint_attached(&standalone, path, recovered_lsn).map_err(|e| {
            anyhow::anyhow!("Failed to checkpoint attached database '{}' at {}: {}", alias, path, e)
        })
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

    /// Execute `ATTACH [DATABASE] 'filename' AS schema-name` (#6310 Phase 1,
    /// #6362 Phase 2).
    ///
    /// `':memory:'` and the empty filename create a session-scoped empty
    /// schema (nothing about it is ever persisted, matching SQLite's
    /// per-connection ATTACH semantics). A nonexistent file path likewise
    /// starts empty — it becomes real only once something is saved into it.
    /// An existing non-empty file is loaded via [`load_attached_schema_from_file`]
    /// (format auto-detection, and the existing recovery failure policy: a
    /// newer-format-version file is a hard error, never silently empty).
    ///
    /// [`load_attached_schema_from_file`]: Self::load_attached_schema_from_file
    fn execute_attach(&mut self, stmt: &vibesql_ast::AttachStmt) -> anyhow::Result<()> {
        // SQLite rejects ATTACH inside an explicit transaction.
        if self.db.in_transaction() {
            return Err(anyhow::anyhow!("cannot ATTACH database within transaction"));
        }

        // "Has existing state to load" is NOT just "the snapshot file is
        // non-empty" (#6531): a WAL-active session writes its state into the
        // checkpoint archive and never rewrites the snapshot, so a database
        // whose whole content lives in `<file>-checkpoints/` + `<file>.wal`
        // has an absent or 0-byte snapshot file. Treat the durability
        // siblings as existing state too, exactly as a direct `main` open does.
        let existing_nonempty = stmt.filename != ":memory:" && !stmt.filename.is_empty() && {
            let path = std::path::Path::new(&stmt.filename);
            let snapshot_nonempty =
                path.exists() && std::fs::metadata(path).map(|m| m.len() > 0).unwrap_or(false);
            snapshot_nonempty || has_wal_siblings(&wal::WalPaths::derive(&stmt.filename))
        };

        self.db.catalog.attach_database(&stmt.schema_name, &stmt.filename).map_err(
            |e| match e {
                // SQLite-compat wording for name collisions with main/temp,
                // existing schemas, and existing attachments.
                vibesql_catalog::CatalogError::SchemaAlreadyExists(name) => {
                    anyhow::anyhow!("database {} is already in use", name)
                }
                other => anyhow::anyhow!("{}", other),
            },
        )?;

        let canonical = stmt.schema_name.to_ascii_lowercase();
        // A never-loaded attachment starts at LSN 0; its first checkpoint is
        // stamped at 1 (or past whatever the archive already holds).
        self.attached_recovered_lsn.insert(canonical.clone(), 0);

        if existing_nonempty {
            match self.load_attached_schema_from_file(&canonical, &stmt.filename) {
                Ok(recovered_lsn) => {
                    self.attached_recovered_lsn.insert(canonical.clone(), recovered_lsn);
                }
                Err(e) => {
                    // Roll back the just-created (possibly partially populated)
                    // attachment so a failed load never leaves a half-registered
                    // schema behind — mirrors execute_detach's own cleanup order
                    // (tables first, then the registry entry).
                    for table in self.db.catalog.attached_table_names(&canonical) {
                        let _ = self.db.drop_table(&format!("{}.{}", canonical, table));
                    }
                    let _ = self.db.catalog.detach_database(&canonical);
                    self.attached_recovered_lsn.remove(&canonical);
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Load an existing on-disk database file into a newly-attached schema
    /// (#6362 Phase 2, #6407).
    ///
    /// Loads `path` via [`load_attached_database_file`] (checkpoint-archive
    /// load plus WAL replay when the path carries durability siblings,
    /// otherwise format auto-detection: SQL dump, binary/JSON snapshot, or
    /// SQLite import) into a standalone `Database`, then re-homes each of its
    /// default-schema tables
    /// (definition + live row data), indexes, views, and triggers into
    /// `schema_name` of the live session. `schema_name` must already be an
    /// empty, freshly-attached schema (the caller creates it via
    /// `Catalog::attach_database` first).
    ///
    /// Order matters: tables are re-homed first (indexes/views/triggers all
    /// depend on their target table already existing with its row data),
    /// then indexes (rebuilt against the live table via
    /// `CreateIndexExecutor` — partial-predicate and expression-index bodies
    /// are evaluated fresh rather than transplanted, mirroring
    /// `rebuild_pending_expression_indexes`), then views, then triggers
    /// (matching `write_sql_dump_to_file`'s emission order so an `INSTEAD OF`
    /// trigger's target view already exists).
    ///
    /// The loaded file's objects are always schema-relative (the writer,
    /// `Database::save_attached_schema_sql_dump`, stripped the attachment's
    /// schema qualifier before persisting), so they are re-qualified here
    /// with `schema_name` — the alias *this* session attached the file
    /// under, which need not match the alias used when it was saved.
    ///
    /// Re-qualification covers the view's *body* as well as its name
    /// ([`schema_qualify::qualify_unqualified_tables`]): an unqualified table
    /// reference left in the body would otherwise late-bind through
    /// `Catalog::get_table`'s temp → main → attached search order and
    /// silently read `main`'s same-named table.
    ///
    /// Two documented gaps, both pre-existing name-resolution defects that
    /// reproduce in a live session with no save/reload, and neither of which
    /// can prevent the `ATTACH` itself from succeeding:
    ///
    /// - **Trigger bodies** are not re-bound to `schema_name` — see the `KNOWN LIMITATION (#6477)`
    ///   note at the trigger loop below.
    /// - **Index rebuilds are best-effort**: an index whose bare target-table name is shadowed by a
    ///   same-named table in `main` (or temp, or an earlier attachment) is skipped with a logged
    ///   warning rather than rebuilt, because the storage-side body build binds by bare name — see
    ///   the `KNOWN LIMITATION (#6487)` note at the index loop below. The attachment's tables,
    ///   rows, views, and triggers still re-home normally; only the index is absent.
    ///
    /// Returns the highest LSN recovery observed for `path` (0 when no WAL
    /// recovery ran), which the caller records so the attachment's own
    /// checkpoint can be stamped past it (#6531).
    fn load_attached_schema_from_file(
        &mut self,
        schema_name: &str,
        path: &str,
    ) -> anyhow::Result<u64> {
        let (mut loaded, recovered_lsn) = load_attached_database_file(path)?;

        let table_names = loaded
            .catalog
            .get_schema(vibesql_catalog::DEFAULT_SCHEMA)
            .map(|s| s.list_tables())
            .unwrap_or_default();

        for table_name in &table_names {
            let Some(table_schema) = loaded
                .catalog
                .get_schema(vibesql_catalog::DEFAULT_SCHEMA)
                .and_then(|s| s.get_table(table_name, true))
                .cloned()
            else {
                continue;
            };

            self.db
                .catalog
                .create_table_in_schema(schema_name, table_schema)
                .map_err(|e| anyhow::anyhow!("{}", e))?;

            let src_key = format!("{}.{}", vibesql_catalog::DEFAULT_SCHEMA, table_name);
            if let Some(table) = loaded.tables.remove(&src_key) {
                self.db.tables.insert(format!("{}.{}", schema_name, table_name), table);
            }
        }

        // Indexes: rebuild the physical index body against the now-populated
        // attached-schema table rather than transplanting `loaded`'s index
        // structures (#6407). `IndexColumn`/`where_clause` are shared AST
        // types between the catalog and executor crates, so the loaded
        // metadata plugs directly into a synthetic `CreateIndexStmt`.
        //
        // The auto-generated-index filter below deliberately tests
        // `storage_meta.index_name` (the bare name) rather than the value
        // yielded by `list_indexes()`, which is the storage *map key*.
        // `loaded` is a standalone database whose objects all live in the
        // default schema, so its keys happen to be bare today and testing
        // either would work — but `make_index_key` prefixes any non-`main`
        // schema, so a key-based test silently stops excluding auto-indexes
        // the moment this loop is pointed at a schema-bearing database. That
        // is exactly the defect that shipped on the writer side in
        // `save_attached_schema_sql_dump`; filtering on the metadata name
        // costs nothing and removes the latent trap.
        for index_key in loaded.list_indexes() {
            let Some(storage_meta) = loaded.get_index(&index_key) else { continue };
            let lower_name = storage_meta.index_name.to_lowercase();
            if lower_name.starts_with("pk_")
                || lower_name.starts_with("sqlite_autoindex_")
                || lower_name.starts_with(vibesql_catalog::WITHOUT_ROWID_PK_INDEX_PREFIX)
            {
                continue;
            }
            let Some(catalog_meta) = loaded.catalog.find_index_by_name(&storage_meta.index_name)
            else {
                continue;
            };

            let index_type =
                ast_index_type_from_catalog(&catalog_meta.index_type, storage_meta.unique);
            let create_stmt = vibesql_ast::CreateIndexStmt {
                if_not_exists: false,
                index_name: storage_meta.index_name.clone(),
                schema: None,
                table_name: format!("{}.{}", schema_name, storage_meta.table_name),
                index_type,
                columns: storage_meta.columns.clone(),
                where_clause: catalog_meta.where_clause.clone(),
            };

            // KNOWN LIMITATION (#6487): the index body build binds by BARE
            // table name, so it must be skipped when the bare name does not
            // resolve to this attachment.
            //
            // `CreateIndexExecutor` validates against the *qualified* target
            // (`aux.t`) but `build_btree_index_body` then hands storage the
            // **bare** name, and storage re-resolves it through the temp →
            // main → attached search order. When `main` holds a same-named
            // table that resolution lands on `main.t` instead, with two bad
            // outcomes: if `main.t` lacks the indexed column the build errors
            // (`Column 'z' not found in table 't'`), and if it happens to
            // have one the body is silently built from `main.t`'s rows and
            // registered as a `main`-schema index. Propagating either from
            // here would make an attachment that merely *contains* an index
            // impossible to re-`ATTACH` at all — strictly worse than
            // pre-#6407, where indexes were never persisted.
            //
            // So the rebuild is best-effort: it is attempted only when the
            // bare name provably resolves back to this attachment (the same
            // `resolve_table_schema_name` order storage itself uses), and any
            // error from the attempt is logged and skipped rather than
            // propagated. A skipped index degrades the reload to "the index
            // is missing" — exactly the pre-#6407 behavior — while the tables,
            // their rows, views, and triggers all still re-home and the
            // `ATTACH` succeeds. Removing this guard is safe (and desirable)
            // once #6487 makes the body build bind to the qualified name.
            // `test_attach_reattach_index_skipped_on_main_table_name_collision`
            // and its `*_with_matching_column` sibling pin both branches.
            let resolves_to_attachment = self
                .db
                .catalog
                .resolve_table_schema_name(&storage_meta.table_name)
                .is_some_and(|resolved| resolved.eq_ignore_ascii_case(schema_name));
            if !resolves_to_attachment {
                log::warn!(
                    "ATTACH '{}' AS {}: skipping rebuild of index '{}' on '{}.{}' — the bare \
                     table name '{}' resolves to another schema (a same-named table shadows the \
                     attachment), and the index body build would bind to that table instead \
                     (#6487). The attached data is unaffected; only the index is missing.",
                    path,
                    schema_name,
                    storage_meta.index_name,
                    schema_name,
                    storage_meta.table_name,
                    storage_meta.table_name,
                );
                continue;
            }
            if let Err(e) =
                vibesql_executor::CreateIndexExecutor::execute(&create_stmt, &mut self.db)
            {
                log::warn!(
                    "ATTACH '{}' AS {}: failed to rebuild index '{}' on '{}.{}': {} — continuing \
                     without it (#6487). The attached data is unaffected; only the index is \
                     missing.",
                    path,
                    schema_name,
                    storage_meta.index_name,
                    schema_name,
                    storage_meta.table_name,
                    e,
                );
            }
        }

        // Views: re-qualify the (already schema-relative) view name and
        // definition with the live session's attachment alias. Views have no
        // separate `schema` AST field — the qualifier lives directly in the
        // name (`ViewDefinition::name`), matching how the writer strips it.
        //
        // The view's *body* must be re-bound too, not just its name (#6476
        // review). An unqualified table reference inside the body is
        // late-bound through `Catalog::get_table`'s temp → main → attached
        // search order, so a body persisted as `SELECT x FROM t` would
        // silently read `main.t` whenever `main` happens to hold a same-named
        // table — returning another database's rows rather than erroring.
        // `qualify_unqualified_tables` applies SQLite's rule (an unqualified
        // name in a view body resolves within the schema containing the view)
        // by rewriting every bare base-table reference to
        // `<this session's alias>.<table>`; explicitly-qualified references
        // (`main.mt`, `other.u`) — which the writer never stripped — are left
        // alone.
        // Re-homed in the *loaded* standalone catalog's own creation order
        // (#6508), not `iter_views()`'s underlying `HashMap` iteration order.
        // Each re-homed view gets a fresh `creation_seq` ordinal in *this*
        // session the moment it is re-created below, so the order this loop
        // iterates in is exactly the order views come back from
        // `<alias>.sqlite_master` afterward.
        let mut view_defs: Vec<&vibesql_catalog::ViewDefinition> =
            loaded.catalog.iter_views().filter(|v| !v.is_temp()).collect();
        view_defs.sort_by_key(|view_def| {
            loaded
                .catalog
                .creation_seq(vibesql_catalog::DEFAULT_SCHEMA, &view_def.name)
                .unwrap_or(u64::MAX)
        });
        for view_def in view_defs {
            let qualified_name = format!("{}.{}", schema_name, view_def.name);
            let mut query = view_def.query.clone();
            schema_qualify::qualify_unqualified_tables(&mut query, schema_name);
            let create_stmt = vibesql_ast::CreateViewStmt {
                view_name: qualified_name,
                columns: view_def.columns.clone(),
                query: Box::new(query),
                with_check_option: view_def.with_check_option,
                or_replace: false,
                if_not_exists: false,
                temporary: false,
                sql_definition: view_def.sql_definition.clone(),
            };
            vibesql_executor::ViewExecutor::execute_create_view(&create_stmt, &mut self.db)
                .map_err(|e| anyhow::anyhow!("{}", e))?;
        }

        // Triggers: unlike views, triggers carry an explicit `schema` AST
        // field distinct from their bare name/table, so re-homing is a
        // direct field-for-field translation from the loaded
        // `TriggerDefinition` into a synthetic `CreateTriggerStmt`, replayed
        // through `TriggerExecutor` so target-table/schema validation runs
        // exactly as it would for a live `CREATE TRIGGER`.
        //
        // KNOWN LIMITATION (#6477): the trigger's *body* is NOT re-bound to
        // `schema_name` the way the view body above is. A trigger body is
        // stored as `TriggerAction::RawSql` and re-parsed when the trigger
        // fires, and the parser rejects a qualified table name inside a
        // trigger body outright ("qualified table names are not allowed on
        // INSERT, UPDATE, and DELETE statements within triggers", matching
        // SQLite) — so there is no AST to rewrite and no parseable text form
        // that would express the binding. Its unqualified names therefore
        // still resolve through `Catalog::get_table`'s temp → main →
        // attached search order, so a same-named table in `main` wins. That
        // late binding is pre-existing and reproduces with no save/reload at
        // all; fixing it means teaching trigger execution to resolve a
        // body's names in the trigger's own schema, which is #6477's job.
        // `test_attach_reattach_trigger_body_binds_to_main_on_name_collision`
        // pins the actual behavior so it cannot change unnoticed.
        // Re-homed in the *loaded* standalone catalog's own creation order
        // (#6508) — same rationale as the views loop above.
        let mut trigger_defs: Vec<&vibesql_catalog::TriggerDefinition> =
            loaded.catalog.iter_triggers().filter(|t| !t.is_temp()).collect();
        trigger_defs.sort_by_key(|trigger_def| {
            loaded
                .catalog
                .creation_seq(vibesql_catalog::DEFAULT_SCHEMA, &trigger_def.name)
                .unwrap_or(u64::MAX)
        });
        for trigger_def in trigger_defs {
            let create_stmt = vibesql_ast::CreateTriggerStmt {
                if_not_exists: false,
                schema: Some(schema_name.to_string()),
                trigger_name: trigger_def.name.clone(),
                name_source: None,
                timing: trigger_def.timing.clone(),
                event: trigger_def.event.clone(),
                table_name: trigger_def.table_name.clone(),
                granularity: trigger_def.granularity.clone(),
                when_condition: trigger_def.when_condition.clone(),
                triggered_action: trigger_def.triggered_action.clone(),
            };
            vibesql_executor::TriggerExecutor::create_trigger_with_sql(
                &mut self.db,
                &create_stmt,
                trigger_def.sql_definition.as_deref(),
            )
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        }

        Ok(recovered_lsn)
    }

    /// Execute `DETACH [DATABASE] schema-name` (#6310 Phase 1, #6362 Phase 2).
    ///
    /// Flushes the attached schema's pending state to its own file (if
    /// file-backed), drops the schema's tables from storage, then removes the
    /// schema, its views/triggers/indexes, and the registry entry.
    fn execute_detach(&mut self, stmt: &vibesql_ast::DetachStmt) -> anyhow::Result<()> {
        // SQLite rejects DETACH inside an explicit transaction.
        if self.db.in_transaction() {
            return Err(anyhow::anyhow!("cannot DETACH database within transaction"));
        }

        let name = &stmt.schema_name;
        if !self.db.catalog.is_attached_schema(name) {
            return Err(anyhow::anyhow!("no such database: {}", name));
        }

        let canonical = name.to_ascii_lowercase();

        // Persist BEFORE dropping tables (#6362): the drop loop below removes
        // row data from in-memory storage, so it must run after the flush or
        // there would be nothing left to persist.
        let attached_path = self
            .db
            .catalog
            .attached_databases()
            .iter()
            .find(|a| a.name == canonical)
            .map(|a| a.path.clone())
            .filter(|p| p != ":memory:" && !p.is_empty());
        if let Some(path) = attached_path {
            self.persist_attached_schema(&canonical, &path)?;
        }

        // Drop the schema's tables from storage (row data lives there); WAL
        // emission is already suppressed for attached-schema tables.
        for table in self.db.catalog.attached_table_names(&canonical) {
            let qualified = format!("{}.{}", canonical, table);
            self.db.drop_table(&qualified).map_err(|e| anyhow::anyhow!("{}", e))?;
        }

        self.attached_recovered_lsn.remove(&canonical);
        self.db.catalog.detach_database(&canonical).map_err(|e| anyhow::anyhow!("{}", e))
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

/// SQLite's `SQLITE_DEFAULT_PAGE_SIZE` compile-time constant: the value
/// `PRAGMA page_size` reports for a database whose page size has never been
/// changed.
const SQLITE_DEFAULT_PAGE_SIZE: i64 = 4096;

/// SQLite's `SQLITE_MAX_PAGE_SIZE` compile-time constant — the upper bound
/// `sqlite3BtreeSetPageSize` accepts.
const SQLITE_MAX_PAGE_SIZE: i64 = 65536;

/// The spill threshold (`PCache.szSpill`) a fresh connection starts with,
/// matching `sqlite3PcacheOpen`'s `p->szSpill = 1`.
const SQLITE_DEFAULT_SPILL_PAGES: i64 = 1;

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
