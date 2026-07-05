/// Check if a database path represents an in-memory database.
/// SQLite uses ":memory:" as a special value for in-memory databases.
pub fn is_memory_database(path: &str) -> bool {
    path == ":memory:" || path == "file::memory:" || path.starts_with("file::memory:?")
}

/// Print a loud, fail-closed error when persisting the database fails.
///
/// Invariant (issues #5832, #5807 / PR #5850): a checkpoint/save failure must
/// NEVER be a quiet warning followed by exit 0. `WalState::checkpoint` only
/// truncates the WAL *after* the checkpoint file is durably written, so on
/// failure the WAL still holds every committed change — tell the user that
/// loudly, and the caller must propagate a non-zero exit code.
pub fn report_save_failure(path: &str, wal_active: bool, error: &anyhow::Error) {
    eprintln!("ERROR: failed to persist database to '{path}': {error}");
    if wal_active {
        let wal_path = crate::executor::wal::WalPaths::derive(path).wal_path;
        eprintln!(
            "ERROR: the write-ahead log at '{}' was left intact; committed changes \
             will be recovered on the next open. Do NOT delete the .wal file.",
            wal_path.display()
        );
    } else {
        eprintln!(
            "ERROR: recent changes are NOT durable on disk; the previous snapshot at \
             '{path}' was left untouched."
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_database_detection() {
        // Standard :memory: format
        assert!(is_memory_database(":memory:"));

        // URI format
        assert!(is_memory_database("file::memory:"));

        // URI format with parameters
        assert!(is_memory_database("file::memory:?cache=shared"));

        // Regular file paths should not be detected as memory
        assert!(!is_memory_database("test.db"));
        assert!(!is_memory_database("/path/to/database.db"));
        assert!(!is_memory_database("memory.db"));
        assert!(!is_memory_database(":memory")); // Missing trailing colon
    }
}
