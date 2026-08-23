// ============================================================================
// Inter-Process Database Locking (issue #5808)
// ============================================================================
//
// VibeSQL loads the entire database into memory at open and checkpoints its
// own image at save time (`db.to_uncompressed_bytes()`). Two concurrent
// writer sessions therefore cannot be merged — the last checkpoint wins and
// clobbers the other session's committed writes wholesale, and concurrent WAL
// appends interleave entries from divergent in-memory states. Whole-database,
// whole-session EXCLUSIVE locking is the semantically correct scope, not
// merely the simplest.
//
// ## Mechanism
//
// A kernel advisory lock (`flock` on Unix, `LockFileEx` on Windows) via
// `std::fs::File::try_lock` (stable since Rust 1.89 — no new dependency).
// The kernel releases the lock automatically when the holding process dies,
// including SIGKILL, so no stale-lock recovery scheme is needed.
//
// ## Lock target
//
// A dedicated sibling `<stem>.lock` file that is never renamed or deleted.
// We deliberately do NOT lock the `.vbsql` or `.wal` files directly: both are
// replaced via temp-file + rename (snapshot save; `truncate_wal`), which would
// orphan the lock on the old inode and let a second process acquire a "fresh"
// lock on the new one. The `.lock` file is created with `create(true)` (so a
// brand-new database path locks fine) and is left in place on exit.
//
// ## Caveat
//
// Advisory locks are unreliable on NFS filesystems — the same caveat SQLite
// carries. Server/Raft mode is out of scope: replicated writes already
// serialize through the consensus log.

use std::{
    fs::{File, OpenOptions, TryLockError},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use crate::error::StorageError;

/// Interval between lock acquisition retries while waiting out the busy
/// timeout.
const RETRY_INTERVAL: Duration = Duration::from_millis(10);

/// Derive the lock file path for a database file path.
///
/// `mydata.vbsql` -> `mydata.lock` (sibling, shares the stem).
pub fn lock_path_for(db_path: &Path) -> PathBuf {
    db_path.with_extension("lock")
}

/// RAII guard for the exclusive inter-process database lock.
///
/// The lock is released when the guard is dropped (the kernel also releases
/// it automatically if the process dies first, including SIGKILL).
#[derive(Debug)]
pub struct DatabaseLock {
    /// Held open for the lifetime of the guard; closing the file releases the
    /// advisory lock.
    file: File,
    /// Path of the `.lock` file (kept for diagnostics).
    path: PathBuf,
}

impl DatabaseLock {
    /// Path of the underlying `.lock` file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for DatabaseLock {
    fn drop(&mut self) {
        // Explicit unlock before close for clarity; closing the file would
        // release the flock anyway. Errors are ignored — there is no useful
        // recovery at drop time and the kernel cleans up on process exit.
        let _ = self.file.unlock();
    }
}

/// Acquire the exclusive inter-process lock for the database at `db_path`.
///
/// Retries every [`RETRY_INTERVAL`] until `timeout` elapses (a `timeout` of
/// zero means a single attempt: fail immediately when contended), then fails
/// with [`StorageError::DatabaseLocked`], whose display is the exact
/// SQLite-compatible wording `database is locked`.
///
/// The lock is scoped to the whole session: hold the returned guard until the
/// final checkpoint/save has completed (i.e. drop it last).
pub fn acquire_exclusive(db_path: &Path, timeout: Duration) -> Result<DatabaseLock, StorageError> {
    let lock_path = lock_path_for(db_path);

    // `truncate(false)`: the lock file's contents are irrelevant (it is a pure
    // flock target), but truncating another process's open lock file would be
    // needless churn.
    let file =
        OpenOptions::new().create(true).write(true).truncate(false).open(&lock_path).map_err(
            |e| {
                StorageError::IoError(format!(
                    "Failed to open lock file {}: {}",
                    lock_path.display(),
                    e
                ))
            },
        )?;

    let deadline = Instant::now() + timeout;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(DatabaseLock { file, path: lock_path }),
            Err(TryLockError::WouldBlock) => {
                if Instant::now() >= deadline {
                    return Err(StorageError::DatabaseLocked);
                }
                std::thread::sleep(
                    RETRY_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
                );
            }
            Err(TryLockError::Error(e)) => {
                return Err(StorageError::IoError(format!(
                    "Failed to lock {}: {}",
                    lock_path.display(),
                    e
                )));
            }
        }
    }
}

/// Remove stale temp files left behind by interrupted checkpoint/truncate
/// writers: `checkpoint_*.tmp` stubs in the checkpoint archive directory and
/// the `<stem>.wal.tmp` truncate scratch file.
///
/// This is safe ONLY while holding the exclusive [`DatabaseLock`] — no other
/// process can be mid-checkpoint. Completed `.vchk` checkpoint files are
/// never touched. Best-effort: individual removal failures are skipped (the
/// stubs are inert; they will be retried on the next open).
///
/// Returns the number of files removed.
pub fn cleanup_stale_temp_files(wal_path: &Path, checkpoint_dir: &Path) -> usize {
    let mut removed = 0usize;

    // `<stem>.wal` -> `<stem>.wal.tmp` (mirrors `truncate_wal`'s temp path).
    let wal_tmp = wal_path.with_extension("wal.tmp");
    if wal_tmp.exists() && std::fs::remove_file(&wal_tmp).is_ok() {
        removed += 1;
    }

    if let Ok(entries) = std::fs::read_dir(checkpoint_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let is_checkpoint_stub = path.extension().is_some_and(|ext| ext == "tmp")
                && path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("checkpoint_"));
            if is_checkpoint_stub && std::fs::remove_file(&path).is_ok() {
                removed += 1;
            }
        }
    }

    removed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_path_derivation() {
        assert_eq!(
            lock_path_for(Path::new("/tmp/mydata.vbsql")),
            PathBuf::from("/tmp/mydata.lock")
        );
        assert_eq!(lock_path_for(Path::new("mydata")), PathBuf::from("mydata.lock"));
    }

    #[test]
    fn test_acquire_creates_lock_file_and_release_allows_reacquire() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db.vbsql");

        let guard = acquire_exclusive(&db_path, Duration::ZERO).expect("first acquire");
        assert!(guard.path().exists(), ".lock sibling must be created");
        assert_eq!(guard.path(), dir.path().join("db.lock"));
        drop(guard);

        // Released on drop: a fresh handle can lock again immediately.
        let guard2 = acquire_exclusive(&db_path, Duration::ZERO).expect("reacquire after drop");
        drop(guard2);
    }

    /// A brand-new database path (no `.vbsql` on disk yet) must lock fine.
    #[test]
    fn test_acquire_on_nonexistent_database_path() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("does_not_exist_yet.vbsql");
        assert!(!db_path.exists());
        let _guard = acquire_exclusive(&db_path, Duration::ZERO).expect("acquire on new path");
    }

    /// On Unix, `flock` locks are per open-file-description, so a second
    /// handle in the SAME process conflicts too — enough for a fast unit test
    /// of the timeout path. (Cross-process behavior is covered by the
    /// two-process integration tests in `crates/vibesql-cli/tests/`.)
    #[cfg(unix)]
    #[test]
    fn test_contended_acquire_times_out_with_database_is_locked() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db.vbsql");

        let _holder = acquire_exclusive(&db_path, Duration::ZERO).expect("holder acquires");

        let start = Instant::now();
        let err = acquire_exclusive(&db_path, Duration::from_millis(100))
            .expect_err("second acquire must fail while held");
        assert!(matches!(err, StorageError::DatabaseLocked));
        // Exact SQLite-compatible wording — contractual (issue #5808).
        assert_eq!(err.to_string(), "database is locked");
        assert!(
            start.elapsed() >= Duration::from_millis(100),
            "must wait out the busy timeout before failing"
        );
    }

    /// Zero timeout = single attempt, immediate failure when contended.
    #[cfg(unix)]
    #[test]
    fn test_zero_timeout_fails_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db.vbsql");

        let _holder = acquire_exclusive(&db_path, Duration::ZERO).expect("holder acquires");
        let err = acquire_exclusive(&db_path, Duration::ZERO).expect_err("must fail");
        assert!(matches!(err, StorageError::DatabaseLocked));
    }

    /// Waiting within the busy timeout succeeds once the holder releases.
    #[cfg(unix)]
    #[test]
    fn test_waiter_acquires_after_holder_releases() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db.vbsql");

        let holder = acquire_exclusive(&db_path, Duration::ZERO).expect("holder acquires");
        let db_path_clone = db_path.clone();
        let waiter =
            std::thread::spawn(move || acquire_exclusive(&db_path_clone, Duration::from_secs(30)));
        // Brief hold, then release — the waiter must then acquire well within
        // its generous timeout.
        std::thread::sleep(Duration::from_millis(50));
        drop(holder);
        let acquired = waiter.join().unwrap();
        assert!(acquired.is_ok(), "waiter must acquire after release: {:?}", acquired.err());
    }

    #[test]
    fn test_cleanup_removes_tmp_stubs_and_preserves_vchk() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("db.wal");
        let ckpt_dir = dir.path().join("db-checkpoints");
        std::fs::create_dir_all(&ckpt_dir).unwrap();

        // 32-byte stubs — exactly what interrupted checkpoint writers leave.
        std::fs::write(ckpt_dir.join("checkpoint_99.tmp"), [0u8; 32]).unwrap();
        std::fs::write(ckpt_dir.join("checkpoint_100.tmp"), [0u8; 32]).unwrap();
        std::fs::write(ckpt_dir.join("checkpoint_98.vchk"), b"complete checkpoint").unwrap();
        // Truncate scratch file next to the WAL.
        std::fs::write(dir.path().join("db.wal.tmp"), b"partial rewrite").unwrap();
        // Unrelated .tmp (no checkpoint_ prefix) must be left alone.
        std::fs::write(ckpt_dir.join("other.tmp"), b"not ours").unwrap();

        let removed = cleanup_stale_temp_files(&wal_path, &ckpt_dir);
        assert_eq!(removed, 3);

        assert!(!ckpt_dir.join("checkpoint_99.tmp").exists());
        assert!(!ckpt_dir.join("checkpoint_100.tmp").exists());
        assert!(!dir.path().join("db.wal.tmp").exists());
        assert!(ckpt_dir.join("checkpoint_98.vchk").exists(), ".vchk must be untouched");
        assert!(ckpt_dir.join("other.tmp").exists(), "unrelated .tmp must be untouched");
    }

    /// Cleanup on a database with no WAL siblings at all is a quiet no-op.
    #[test]
    fn test_cleanup_noop_when_nothing_to_clean() {
        let dir = tempfile::tempdir().unwrap();
        let removed = cleanup_stale_temp_files(
            &dir.path().join("db.wal"),
            &dir.path().join("db-checkpoints"),
        );
        assert_eq!(removed, 0);
    }
}
