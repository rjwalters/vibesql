// ============================================================================
// CLI WAL (Write-Ahead Log) Persistence Wiring
// ============================================================================
//
// This module wires VibeSQL's WAL + crash-recovery engine
// (`crates/vibesql-storage/src/wal/`) into the CLI. WAL is on by default
// (`[database] wal = true`) for file-backed databases; set `wal = false` to opt
// out and use the snapshot-only path.
//
// ## File layout
//
// For a database file `mydata.vbsql`, WAL-active mode derives two sibling
// paths from the file stem:
//
// ```text
// mydata.vbsql          — binary snapshot (legacy; loaded as the recovery base
//                         when no checkpoint archive exists yet — see #5807)
// mydata.wal            — active write-ahead log
// mydata-checkpoints/   — checkpoint archive directory (checkpoint_*.vchk)
// ```
//
// Only the canonical `.vbsql` extension is stripped when deriving those
// siblings; a file with any other extension keeps its full name as the base
// (`test.db2` -> `test.db2.wal` / `test.db2-checkpoints/`) so two databases
// differing only by extension can never share one WAL or checkpoint archive
// (#6531). See [`WalPaths::derive`].
//
// ## ATTACH
//
// `ATTACH` reaches the same file through the same derivation and the same
// `RecoveryManager`, so an attached view of a database and a direct `main`
// open of it never diverge (#6531). See `load_attached_database_file` and
// [`WalState::checkpoint_attached`] in `super`.
//
// ## Durability model
//
// On open (when WAL is active), `RecoveryManager::recover()` loads the latest
// checkpoint and replays WAL entries written after it. On `\save` / clean exit,
// the CLI writes a fresh checkpoint at the engine's current LSN and truncates
// the WAL up to that LSN.
//
// Both DDL and committed DML survive an unclean shutdown (SIGKILL, power loss):
// `RecoveryManager::apply_op` replays CreateTable/DropTable *and*
// Insert/Update/Delete, routing each row mutation by the inline `table_name`
// carried in WAL format v2 DML ops. Uncommitted transactions at crash time are
// discarded (their ops are buffered but never applied). A truncated WAL tail
// (partial final write) recovers up to the last complete, checksum-valid entry.

use std::path::{Path, PathBuf};

use vibesql_storage::{
    wal::{
        truncate_wal, CheckpointWriter, PersistenceConfig, PersistenceEngine, RecoveryConfig,
        RecoveryManager, DEFAULT_KEEP_CHECKPOINTS,
    },
    Database, StorageError,
};

/// Sibling paths derived from a database file path for WAL-active mode.
#[derive(Debug, Clone)]
pub struct WalPaths {
    /// Active write-ahead log file (`<base>.wal`, see [`WalPaths::derive`]
    /// for how `<base>` is derived).
    pub wal_path: PathBuf,
    /// Checkpoint archive directory (`<base>-checkpoints/`).
    pub checkpoint_dir: PathBuf,
}

/// The canonical VibeSQL database-file extension.
///
/// Paths carrying it keep the historical *stem*-based sibling layout so every
/// database written by an earlier VibeSQL still finds its WAL and checkpoint
/// archive after the #6531 collision fix (and so the documented layout in
/// `CLAUDE.md` / the module header above stays true).
const CANONICAL_DB_EXTENSION: &str = "vbsql";

impl WalPaths {
    /// Derive WAL sibling paths from a database file path.
    ///
    /// `mydata.vbsql` -> `{ wal: mydata.wal, checkpoints: mydata-checkpoints/ }`
    ///
    /// **Collision safety (#6531).** The derivation used to drop *any*
    /// extension (`with_extension("wal")` for the WAL, `file_stem()` for the
    /// checkpoint dir), so `test.db2` and `test.db3` in one directory both
    /// resolved to `test.wal` + `test-checkpoints/` and would clobber each
    /// other's durability files. That became directly reachable once `ATTACH`
    /// started routing through this derivation, because `test.db2`/`test.db3`
    /// is exactly how SQLite's TCL corpus names auxiliary databases.
    ///
    /// Only the canonical [`CANONICAL_DB_EXTENSION`] is stripped now; every
    /// other path keeps its **full file name** as the sibling base:
    ///
    /// ```text
    /// mydata.vbsql -> mydata.wal        mydata-checkpoints/     (unchanged)
    /// mydata       -> mydata.wal        mydata-checkpoints/     (unchanged)
    /// test.db2     -> test.db2.wal      test.db2-checkpoints/   (was test.wal)
    /// test.db3     -> test.db3.wal      test.db3-checkpoints/   (was test.wal)
    /// ```
    ///
    /// Both access paths to one file — a direct `main` open and an `ATTACH` —
    /// go through this single function, so they always agree on where a
    /// file's durability siblings live.
    pub fn derive(db_path: &str) -> Self {
        let path = Path::new(db_path);

        // Sibling base name: the stem for the canonical `.vbsql` extension
        // (backward compatibility), the full file name otherwise.
        let is_canonical =
            path.extension().is_some_and(|ext| ext.eq_ignore_ascii_case(CANONICAL_DB_EXTENSION));
        let base = if is_canonical { path.file_stem() } else { path.file_name() }
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();

        let wal_name = format!("{base}.wal");
        let checkpoint_name = format!("{base}-checkpoints");
        match path.parent().filter(|p| !p.as_os_str().is_empty()) {
            Some(parent) => WalPaths {
                wal_path: parent.join(wal_name),
                checkpoint_dir: parent.join(checkpoint_name),
            },
            None => WalPaths {
                wal_path: PathBuf::from(wal_name),
                checkpoint_dir: PathBuf::from(checkpoint_name),
            },
        }
    }

    /// True when the checkpoint archive directory contains at least one
    /// `.vchk` file.
    ///
    /// Used to detect a legacy snapshot-only database (issue #5807): a
    /// `.vbsql` file written before WAL mode has no checkpoint archive, and
    /// its snapshot must be loaded as the recovery base instead of being
    /// silently ignored.
    pub fn has_checkpoint_files(&self) -> bool {
        std::fs::read_dir(&self.checkpoint_dir)
            .map(|entries| {
                entries.flatten().any(|e| e.path().extension().is_some_and(|ext| ext == "vchk"))
            })
            .unwrap_or(false)
    }
}

/// Active WAL state held by `SqlExecutor` when `[database] wal = true`.
///
/// Holds the derived sibling paths and a `CheckpointWriter` for the checkpoint
/// archive directory. The `PersistenceEngine` itself lives on the `Database`
/// (installed via `Database::enable_persistence`).
pub struct WalState {
    paths: WalPaths,
    checkpoint_writer: CheckpointWriter,
    /// Number of checkpoint files to retain after each successful checkpoint
    /// (issue #6023). Clamped to a minimum of 1 at construction so the newest
    /// checkpoint always survives pruning.
    keep_checkpoints: usize,
}

impl WalState {
    /// Recover a database from `<stem>-checkpoints/` + `<stem>.wal`, then attach
    /// a live `PersistenceEngine` so subsequent writes are logged to the WAL.
    ///
    /// Returns the recovered (and persistence-enabled) `Database` together with
    /// the `WalState` the executor must keep for checkpoint-on-save.
    ///
    /// (The binary itself goes through [`WalState::open_with_base`]; this
    /// wrapper is used by the in-crate tests.)
    #[allow(dead_code)]
    pub fn open(db_path: &str) -> Result<(Database, WalState), StorageError> {
        Self::open_with_base(db_path, None, false, DEFAULT_KEEP_CHECKPOINTS)
    }

    /// Like [`WalState::open`], with two additions for issue #5807:
    ///
    /// * `base` — pre-loaded snapshot used **only when no checkpoint files exist** (legacy
    ///   snapshot-only `.vbsql` databases), so their data is never silently ignored under
    ///   WAL-default.
    /// * `recover_fallback` — explicit opt-in (`--recover-fallback`) to recover from an older
    ///   checkpoint when the newest is unreadable. Off (the default), an unreadable checkpoint is a
    ///   hard open error. Every checkpoint skipped under the opt-in is reported loudly on stderr —
    ///   the CLI installs no `log` backend, so `log::warn!` from the recovery engine would be
    ///   silently discarded.
    pub fn open_with_base(
        db_path: &str,
        base: Option<Database>,
        recover_fallback: bool,
        keep_checkpoints: usize,
    ) -> Result<(Database, WalState), StorageError> {
        let paths = WalPaths::derive(db_path);

        // Step 1: recover from the last checkpoint + replay post-checkpoint WAL.
        let config = RecoveryConfig {
            allow_checkpoint_fallback: recover_fallback,
            ..RecoveryConfig::default()
        };
        let manager =
            RecoveryManager::with_config(&paths.checkpoint_dir, config).with_wal(&paths.wal_path);
        let (mut db, stats) = manager.recover_with_base(base)?;

        // Surface skipped checkpoints prominently (issue #5807). This can only
        // be non-empty under the explicit --recover-fallback opt-in, but the
        // consequence (opening state older than the newest checkpoint) must
        // still be impossible to miss.
        if !stats.skipped_checkpoints.is_empty() {
            eprintln!(
                "WARNING: recovery skipped {} unreadable checkpoint file(s) in {}:",
                stats.skipped_checkpoints.len(),
                paths.checkpoint_dir.display()
            );
            for skipped in &stats.skipped_checkpoints {
                eprintln!("  {}: {}", skipped.path.display(), skipped.error);
            }
            eprintln!(
                "WARNING: the database was opened from an OLDER checkpoint (LSN {}); \
                 changes committed after it may be missing. The skipped files were \
                 left on disk untouched.",
                stats.checkpoint_lsn
            );
        }

        // Step 2: attach the live persistence engine so new ops hit the WAL.
        //
        // Crucially, resume LSN numbering *past* everything recovery saw
        // (`last_lsn` covers the loaded checkpoint and every WAL entry). Each new
        // checkpoint is stamped from `persistence_next_lsn`, and recovery selects
        // the checkpoint with the highest LSN. If we restarted LSNs at 1 on every
        // open (the pre-#5766 behavior), a later process's checkpoint could carry
        // a *lower* LSN than an earlier one and recovery would resurrect stale,
        // pre-mutation state — silently losing committed DELETE/UPDATE/INSERT data
        // across CLI restarts. Resuming at `last_lsn + 1` keeps checkpoint LSNs
        // monotonic so the newest committed state always wins.
        let resume_lsn = stats.last_lsn.saturating_add(1);
        let engine = PersistenceEngine::open_with_start_lsn(
            &paths.wal_path,
            PersistenceConfig::default(),
            resume_lsn,
        )?;
        db.enable_persistence(engine);

        let checkpoint_writer = CheckpointWriter::new(&paths.checkpoint_dir)?;

        // Clamp to a minimum of 1: `keep_checkpoints = 0` would otherwise prune
        // every checkpoint (including the newest) after a save, leaving nothing
        // for recovery to load. Keeping at least the newest checkpoint upholds
        // the fail-closed recovery policy (#5807/#5850). Documented for the CLI
        // knob in `.vibesqlrc.example` and `DatabaseConfig::keep_checkpoints`.
        let keep_checkpoints = keep_checkpoints.max(1);

        Ok((db, WalState { paths, checkpoint_writer, keep_checkpoints }))
    }

    /// Create a checkpoint of the current database state and truncate the WAL
    /// up to the checkpoint LSN.
    ///
    /// This is the WAL-active replacement for the snapshot-on-save path. It:
    ///   1. Flushes any pending WAL entries to disk (`sync_persistence`).
    ///   2. Serializes the in-memory database to uncompressed binary bytes.
    ///   3. Writes a checkpoint at the engine's current LSN.
    ///   4. Truncates the WAL up to that LSN.
    ///   5. Prunes old checkpoints, keeping the `keep_checkpoints` most recent (issue #6023) so the
    ///      archive does not grow unboundedly. This runs only after steps 1–4 succeed; a pruning
    ///      failure is logged, not propagated, so a successful checkpoint is never reported as
    ///      failed.
    ///
    /// **Fail-safe invariant (issue #5832):** the WAL is truncated in step 4
    /// only after steps 1–3 all succeed. Any failure — serialization error,
    /// unwritable checkpoint directory, I/O error — propagates immediately and
    /// leaves the WAL untouched, so committed changes are always recoverable
    /// on the next open. Callers must surface the error loudly and exit
    /// non-zero (see `crate::util::report_save_failure`); a checkpoint failure
    /// must never be silent (cross-link #5807 / PR #5850's fail-closed policy).
    pub fn checkpoint(&mut self, db: &Database) -> Result<(), StorageError> {
        // 1. Make sure everything already emitted is on disk before we snapshot.
        db.sync_persistence()?;

        // 2. Determine the LSN this checkpoint covers. `persistence_next_lsn` returns the next LSN
        //    the engine will assign, so every entry with LSN < that is captured by this checkpoint
        //    snapshot and safe to truncate.
        let checkpoint_lsn = db.persistence_next_lsn().unwrap_or(1).saturating_sub(1);

        // 3. Serialize the database and write the checkpoint.
        let data = db.to_uncompressed_bytes()?;
        let num_tables = db.list_tables().len() as u32;
        self.checkpoint_writer.create_checkpoint(checkpoint_lsn, &data, num_tables)?;

        // 4. Truncate the WAL up to (and including) the checkpoint LSN. Use a zero safety buffer:
        //    the checkpoint fully captures state at this LSN.
        if self.paths.wal_path.exists() {
            truncate_wal(&self.paths.wal_path, checkpoint_lsn, Some(0))?;
        }

        // 5. Prune old checkpoints (issue #6023). This runs ONLY after the new checkpoint is
        //    durably written AND the WAL has been truncated, so pruning can never remove a
        //    checkpoint the WAL still depends on and the newest checkpoint is always present
        //    (fail-closed recovery, #5807/#5850). `keep_checkpoints` is clamped to >= 1 at
        //    construction.
        //
        //    A cleanup failure is a logged warning, NOT a checkpoint failure:
        //    the durable checkpoint + truncated WAL already succeeded, so the
        //    save must still return Ok.
        //
        //    Success is logged via `log::debug!` (not stderr): a routine save
        //    must stay silent on stderr — an existing contract that other tests
        //    rely on — and the CLI installs no `log` backend, so this is a
        //    no-op for end users but observable if one is attached. A *failure*
        //    is surfaced loudly on stderr, since the CLI would otherwise discard
        //    a `log::warn!` and the operator must know pruning did not happen.
        match self.checkpoint_writer.cleanup_old_checkpoints(self.keep_checkpoints) {
            Ok(removed) if removed > 0 => {
                log::debug!(
                    "Pruned {removed} old checkpoint(s), keeping the {} most recent in {}",
                    self.keep_checkpoints,
                    self.paths.checkpoint_dir.display()
                );
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!(
                    "WARNING: failed to prune old checkpoints in {} (checkpoint itself succeeded): {}",
                    self.paths.checkpoint_dir.display(),
                    e
                );
            }
        }

        Ok(())
    }

    /// Publish an **attached** database's current state into that file's own
    /// checkpoint archive (#6531).
    ///
    /// This is the attachment-side counterpart of [`WalState::checkpoint`]:
    /// it gives writes made through an attached alias the same
    /// checkpoint/WAL durability a `main`-schema write gets, so an
    /// alias-side write is visible to a later direct open of that path (and
    /// vice versa — see `load_attached_database_file`, which recovers an
    /// attachment through the same archive).
    ///
    /// `attached_db` is the standalone database the attached file now
    /// represents (its objects live in the default schema, exactly as a
    /// direct open of the path would reconstruct them). `recovered_lsn` is
    /// the highest LSN this session observed when it loaded the attachment
    /// (0 when nothing was recovered).
    ///
    /// **LSN monotonicity.** The checkpoint is stamped one past the highest
    /// of `recovered_lsn` and the newest checkpoint currently in the archive,
    /// so it always wins recovery's newest-checkpoint selection and can never
    /// resurrect pre-mutation state (the #5766 hazard).
    ///
    /// **Fail-safe ordering.** The WAL is truncated only after the checkpoint
    /// is durably written — same invariant as [`WalState::checkpoint`]
    /// (#5832). Checkpoint pruning is best-effort and never turns a
    /// successful checkpoint into a failure (#6023).
    ///
    /// Note that an attached schema's individual statements are still not
    /// WAL-logged as they execute (the `Database` carries one persistence
    /// engine, which belongs to `main`); durability for an attachment is
    /// established at each save/detach/exit checkpoint boundary.
    pub fn checkpoint_attached(
        &self,
        attached_db: &Database,
        path: &str,
        recovered_lsn: u64,
    ) -> Result<(), StorageError> {
        let paths = WalPaths::derive(path);
        let mut writer = CheckpointWriter::new(&paths.checkpoint_dir)?;

        let latest_lsn = writer.latest_checkpoint()?.map(|info| info.lsn).unwrap_or(0);
        let checkpoint_lsn = recovered_lsn.max(latest_lsn).saturating_add(1);

        let data = attached_db.to_uncompressed_bytes()?;
        let num_tables = attached_db.list_tables().len() as u32;
        writer.create_checkpoint(checkpoint_lsn, &data, num_tables)?;

        // Truncate only a WAL that actually has a header to read: a 0-byte
        // WAL (nothing ever written through a direct open) would fail the
        // header read (#5785).
        if std::fs::metadata(&paths.wal_path).map(|m| m.len() > 0).unwrap_or(false) {
            truncate_wal(&paths.wal_path, checkpoint_lsn, Some(0))?;
        }

        if let Err(e) = writer.cleanup_old_checkpoints(self.keep_checkpoints) {
            eprintln!(
                "WARNING: failed to prune old checkpoints in {} (checkpoint itself succeeded): {}",
                paths.checkpoint_dir.display(),
                e
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_paths() {
        let p = WalPaths::derive("/tmp/mydata.vbsql");
        assert_eq!(p.wal_path, PathBuf::from("/tmp/mydata.wal"));
        assert_eq!(p.checkpoint_dir, PathBuf::from("/tmp/mydata-checkpoints"));
    }

    #[test]
    fn test_derive_paths_no_parent() {
        let p = WalPaths::derive("mydata.vbsql");
        assert_eq!(p.wal_path, PathBuf::from("mydata.wal"));
        assert_eq!(p.checkpoint_dir, PathBuf::from("mydata-checkpoints"));
    }

    #[test]
    fn test_derive_paths_no_extension() {
        let p = WalPaths::derive("/tmp/mydata");
        assert_eq!(p.wal_path, PathBuf::from("/tmp/mydata.wal"));
        assert_eq!(p.checkpoint_dir, PathBuf::from("/tmp/mydata-checkpoints"));
    }

    /// Regression for #6531: two database files in the same directory that
    /// differ only by extension must NOT share one WAL / checkpoint archive.
    ///
    /// Before the fix both `test.db2` and `test.db3` derived `test.wal` +
    /// `test-checkpoints/` (the WAL name came from `with_extension`, the
    /// checkpoint dir from `file_stem`), so opening both WAL-actively would
    /// have each clobbering the other's durability files. `test.db2` /
    /// `test.db3` is exactly the naming convention SQLite's own TCL corpus
    /// uses for auxiliary databases, and routing ATTACH through this
    /// derivation makes the collision directly reachable.
    #[test]
    fn test_derive_paths_distinct_extensions_do_not_collide() {
        let db2 = WalPaths::derive("/tmp/test.db2");
        let db3 = WalPaths::derive("/tmp/test.db3");

        assert_ne!(db2.wal_path, db3.wal_path, "test.db2 and test.db3 must not share a WAL file");
        assert_ne!(
            db2.checkpoint_dir, db3.checkpoint_dir,
            "test.db2 and test.db3 must not share a checkpoint archive"
        );

        // The non-canonical-extension layout keeps the FULL file name.
        assert_eq!(db2.wal_path, PathBuf::from("/tmp/test.db2.wal"));
        assert_eq!(db2.checkpoint_dir, PathBuf::from("/tmp/test.db2-checkpoints"));
        assert_eq!(db3.wal_path, PathBuf::from("/tmp/test.db3.wal"));
        assert_eq!(db3.checkpoint_dir, PathBuf::from("/tmp/test.db3-checkpoints"));
    }

    /// The canonical `.vbsql` extension keeps its historical stem-based
    /// layout (`mydata.vbsql` -> `mydata.wal` / `mydata-checkpoints/`) so
    /// databases written by earlier VibeSQL versions still find their WAL and
    /// checkpoint archive after the #6531 collision fix. Distinct `.vbsql`
    /// files never collide with each other, since their stems differ.
    #[test]
    fn test_derive_paths_canonical_extension_layout_is_unchanged() {
        let a = WalPaths::derive("/tmp/one.vbsql");
        let b = WalPaths::derive("/tmp/two.vbsql");
        assert_eq!(a.wal_path, PathBuf::from("/tmp/one.wal"));
        assert_eq!(a.checkpoint_dir, PathBuf::from("/tmp/one-checkpoints"));
        assert_ne!(a.wal_path, b.wal_path);
        assert_ne!(a.checkpoint_dir, b.checkpoint_dir);
    }

    /// Regression for #5785: the very first checkpoint on a freshly-opened
    /// WAL-backed database must succeed even when no WAL entries have been
    /// written yet (e.g. the opening statement is a no-op `DROP TABLE IF EXISTS`
    /// that produces zero WAL ops). Before the fix, the WAL header was buffered
    /// but never flushed, so the on-disk WAL was 0 bytes and `truncate_wal`
    /// failed its header read with "failed to fill whole buffer", surfacing as a
    /// leaked auto-save warning.
    #[test]
    fn test_checkpoint_on_fresh_wal_with_no_entries_succeeds() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("fresh.vbsql");
        let db_path_str = db_path.to_string_lossy().to_string();

        let (db, mut wal_state) = WalState::open(&db_path_str).unwrap();

        // No writes performed — this mirrors an opening no-op DDL. The checkpoint
        // must not error.
        wal_state.checkpoint(&db).expect("checkpoint on empty fresh WAL must succeed");

        // A second checkpoint (already-checkpointed / still empty) must also be a
        // clean no-op.
        wal_state.checkpoint(&db).expect("repeat checkpoint must succeed");

        // The WAL on disk must now be a valid header-bearing (or empty) file that
        // recovery can reopen without error.
        let (_db2, _stats) = WalState::open(&db_path_str).expect("reopen after checkpoint");
    }

    /// Regression for issue #5832: a checkpoint-write failure must NEVER
    /// truncate the WAL. We force `create_checkpoint` to fail by making the
    /// checkpoint directory read-only, then assert (a) `checkpoint()` returns
    /// an error and (b) the on-disk WAL bytes are unchanged, so every
    /// committed change is still recoverable on the next open.
    #[cfg(unix)]
    #[test]
    fn test_failed_checkpoint_leaves_wal_intact() {
        use std::os::unix::fs::PermissionsExt;

        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("failsafe.vbsql");
        let db_path_str = db_path.to_string_lossy().to_string();

        let (mut db, mut wal_state) = WalState::open(&db_path_str).unwrap();

        // Write some committed DDL so the WAL has real content.
        db.create_table(TableSchema::new(
            "survivors".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, true)],
        ))
        .unwrap();
        db.sync_persistence().unwrap();

        let paths = wal_state.paths.clone();
        let wal_before = std::fs::read(&paths.wal_path).unwrap();
        assert!(!wal_before.is_empty(), "WAL must contain the CreateTable op");

        // Inject the failure: make the checkpoint directory unwritable.
        let dir = &paths.checkpoint_dir;
        let orig_perms = std::fs::metadata(dir).unwrap().permissions();
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o555)).unwrap();

        // Skip when running as root (root bypasses permission checks).
        if std::fs::File::create(dir.join(".probe")).is_ok() {
            let _ = std::fs::remove_file(dir.join(".probe"));
            std::fs::set_permissions(dir, orig_perms).unwrap();
            eprintln!("skipping test_failed_checkpoint_leaves_wal_intact: running as root");
            return;
        }

        let result = wal_state.checkpoint(&db);
        assert!(result.is_err(), "checkpoint into a read-only directory must fail");

        // THE invariant: the WAL was not truncated (bytes are unchanged).
        let wal_after = std::fs::read(&paths.wal_path).unwrap();
        assert_eq!(wal_before, wal_after, "a failed checkpoint must leave the WAL byte-identical");

        // Restore permissions and prove the data is still recoverable.
        std::fs::set_permissions(dir, orig_perms).unwrap();
        drop(db);
        drop(wal_state);
        let (db2, _state2) = WalState::open(&db_path_str).expect("reopen after failed checkpoint");
        assert!(
            db2.list_tables().iter().any(|t| t == "survivors"),
            "committed DDL must be recovered from the intact WAL"
        );
    }
}
