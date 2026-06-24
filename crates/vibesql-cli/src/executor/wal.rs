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
// mydata.vbsql          — binary snapshot (loaded on open if it exists)
// mydata.wal            — active write-ahead log
// mydata-checkpoints/   — checkpoint archive directory (checkpoint_*.vchk)
// ```
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
    wal::{truncate_wal, CheckpointWriter, PersistenceConfig, PersistenceEngine, RecoveryManager},
    Database, StorageError,
};

/// Sibling paths derived from a database file path for WAL-active mode.
#[derive(Debug, Clone)]
pub struct WalPaths {
    /// Active write-ahead log file (`<stem>.wal`).
    pub wal_path: PathBuf,
    /// Checkpoint archive directory (`<stem>-checkpoints/`).
    pub checkpoint_dir: PathBuf,
}

impl WalPaths {
    /// Derive WAL sibling paths from the main database file path.
    ///
    /// `mydata.vbsql` -> `{ wal: mydata.wal, checkpoints: mydata-checkpoints/ }`
    pub fn derive(db_path: &str) -> Self {
        let path = Path::new(db_path);
        let wal_path = path.with_extension("wal");

        // `<stem>-checkpoints/` as a sibling of the database file.
        let stem = path.file_stem().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
        let checkpoint_dir = match path.parent() {
            Some(parent) => parent.join(format!("{stem}-checkpoints")),
            None => PathBuf::from(format!("{stem}-checkpoints")),
        };

        WalPaths { wal_path, checkpoint_dir }
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
}

impl WalState {
    /// Recover a database from `<stem>-checkpoints/` + `<stem>.wal`, then attach
    /// a live `PersistenceEngine` so subsequent writes are logged to the WAL.
    ///
    /// Returns the recovered (and persistence-enabled) `Database` together with
    /// the `WalState` the executor must keep for checkpoint-on-save.
    pub fn open(db_path: &str) -> Result<(Database, WalState), StorageError> {
        let paths = WalPaths::derive(db_path);

        // Step 1: recover from the last checkpoint + replay post-checkpoint WAL.
        // (Phase 1: DDL is replayed; DML replay is a Phase 2 stub — see module docs.)
        let manager = RecoveryManager::new(&paths.checkpoint_dir).with_wal(&paths.wal_path);
        let (mut db, _stats) = manager.recover()?;

        // Step 2: attach the live persistence engine so new ops hit the WAL.
        let engine = PersistenceEngine::new(&paths.wal_path, PersistenceConfig::default())?;
        db.enable_persistence(engine);

        let checkpoint_writer = CheckpointWriter::new(&paths.checkpoint_dir)?;

        Ok((db, WalState { paths, checkpoint_writer }))
    }

    /// Create a checkpoint of the current database state and truncate the WAL
    /// up to the checkpoint LSN.
    ///
    /// This is the WAL-active replacement for the snapshot-on-save path. It:
    ///   1. Flushes any pending WAL entries to disk (`sync_persistence`).
    ///   2. Serializes the in-memory database to uncompressed binary bytes.
    ///   3. Writes a checkpoint at the engine's current LSN.
    ///   4. Truncates the WAL up to that LSN.
    pub fn checkpoint(&mut self, db: &Database) -> Result<(), StorageError> {
        // 1. Make sure everything already emitted is on disk before we snapshot.
        db.sync_persistence()?;

        // 2. Determine the LSN this checkpoint covers. `persistence_next_lsn`
        //    returns the next LSN the engine will assign, so every entry with
        //    LSN < that is captured by this checkpoint snapshot and safe to
        //    truncate.
        let checkpoint_lsn = db.persistence_next_lsn().unwrap_or(1).saturating_sub(1);

        // 3. Serialize the database and write the checkpoint.
        let data = db.to_uncompressed_bytes()?;
        let num_tables = db.list_tables().len() as u32;
        self.checkpoint_writer.create_checkpoint(checkpoint_lsn, &data, num_tables)?;

        // 4. Truncate the WAL up to (and including) the checkpoint LSN. Use a
        //    zero safety buffer: the checkpoint fully captures state at this LSN.
        if self.paths.wal_path.exists() {
            truncate_wal(&self.paths.wal_path, checkpoint_lsn, Some(0))?;
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
}
