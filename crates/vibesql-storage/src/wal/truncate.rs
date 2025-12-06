// ============================================================================
// WAL Truncation
// ============================================================================
//
// Provides functionality to truncate the WAL after a successful checkpoint.
// This reclaims disk space by removing entries that are no longer needed
// for recovery (they've been persisted in a checkpoint).
//
// ## Strategy
//
// After a checkpoint at LSN N, we can safely remove all WAL entries with
// LSN <= N, since the checkpoint contains all state up to that point.
//
// However, we keep a small buffer of entries after the checkpoint LSN for
// safety, in case of any race conditions during checkpoint creation.
//
// ## Implementation
//
// WAL truncation works by:
// 1. Reading the WAL to find entries after the truncation point
// 2. Writing those entries to a new WAL file
// 3. Atomically replacing the old WAL with the new one

use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::Path;

use crate::wal::entry::Lsn;
use crate::wal::reader::{ReadResult, WalReader};
use crate::wal::writer::WalWriter;
use crate::StorageError;

/// Default number of entries to keep after checkpoint LSN (safety buffer)
pub const DEFAULT_SAFETY_BUFFER: u64 = 100;

/// Result of a WAL truncation operation
#[derive(Debug, Clone)]
pub struct TruncateResult {
    /// Number of entries removed
    pub entries_removed: u64,
    /// Number of entries kept
    pub entries_kept: u64,
    /// Bytes saved (approximate)
    pub bytes_saved: u64,
    /// LSN of the oldest entry in the new WAL
    pub oldest_lsn: Option<Lsn>,
    /// LSN of the newest entry in the new WAL
    pub newest_lsn: Option<Lsn>,
}

/// Truncate a WAL file, removing entries up to and including the checkpoint LSN
///
/// # Arguments
/// * `wal_path` - Path to the WAL file
/// * `checkpoint_lsn` - LSN of the checkpoint; entries with LSN <= this will be removed
/// * `safety_buffer` - Number of extra entries to keep after checkpoint_lsn (default: 100)
///
/// # Returns
/// Information about what was truncated
///
/// # Safety
/// This operation is atomic - either the truncation succeeds completely or
/// the original WAL remains unchanged.
pub fn truncate_wal(
    wal_path: &Path,
    checkpoint_lsn: Lsn,
    safety_buffer: Option<u64>,
) -> Result<TruncateResult, StorageError> {
    let safety_buffer = safety_buffer.unwrap_or(DEFAULT_SAFETY_BUFFER);

    // Calculate the cutoff LSN (keep entries after this)
    let cutoff_lsn = checkpoint_lsn.saturating_sub(safety_buffer);

    // Get original file size
    let original_size = fs::metadata(wal_path)
        .map_err(|e| StorageError::IoError(format!("Failed to stat WAL: {}", e)))?
        .len();

    // Open the WAL for reading
    let file = File::open(wal_path)
        .map_err(|e| StorageError::IoError(format!("Failed to open WAL: {}", e)))?;
    let reader = BufReader::new(file);
    let mut wal_reader = WalReader::open(reader)?;

    // Create temp file for new WAL
    let temp_path = wal_path.with_extension("wal.tmp");

    // Collect entries to keep
    let mut entries_to_keep = Vec::new();
    let mut entries_removed = 0u64;

    loop {
        match wal_reader.read_entry()? {
            ReadResult::Entry(entry) => {
                if entry.lsn > cutoff_lsn {
                    entries_to_keep.push(entry);
                } else {
                    entries_removed += 1;
                }
            }
            ReadResult::Eof => break,
            ReadResult::Corruption { position } => {
                log::warn!("WAL corruption detected at position {} during truncation", position);
                break;
            }
        }
    }

    // If nothing to remove, no need to rewrite
    if entries_removed == 0 {
        return Ok(TruncateResult {
            entries_removed: 0,
            entries_kept: entries_to_keep.len() as u64,
            bytes_saved: 0,
            oldest_lsn: entries_to_keep.first().map(|e| e.lsn),
            newest_lsn: entries_to_keep.last().map(|e| e.lsn),
        });
    }

    // Write new WAL with remaining entries
    {
        let temp_file = File::create(&temp_path)
            .map_err(|e| StorageError::IoError(format!("Failed to create temp WAL: {}", e)))?;
        let writer = BufWriter::new(temp_file);

        let mut wal_writer = WalWriter::create(writer)?;

        for entry in &entries_to_keep {
            wal_writer.append(entry)?;
        }

        wal_writer.flush()?;
    }

    // Get new file size
    let new_size = fs::metadata(&temp_path)
        .map_err(|e| StorageError::IoError(format!("Failed to stat temp WAL: {}", e)))?
        .len();

    // Atomically replace old WAL with new one
    fs::rename(&temp_path, wal_path)
        .map_err(|e| StorageError::IoError(format!("Failed to replace WAL: {}", e)))?;

    let bytes_saved = original_size.saturating_sub(new_size);

    log::info!(
        "WAL truncated: removed {} entries, kept {}, saved {} bytes",
        entries_removed,
        entries_to_keep.len(),
        bytes_saved
    );

    Ok(TruncateResult {
        entries_removed,
        entries_kept: entries_to_keep.len() as u64,
        bytes_saved,
        oldest_lsn: entries_to_keep.first().map(|e| e.lsn),
        newest_lsn: entries_to_keep.last().map(|e| e.lsn),
    })
}

/// Truncate WAL in-place by seeking and truncating (more efficient for large files)
///
/// This is an alternative implementation that truncates the file in-place
/// rather than copying entries. It's more efficient but slightly more complex.
///
/// Note: This requires finding the file position of the first entry to keep,
/// which means we still need to read through the entries, but we don't need
/// to copy them.
pub fn truncate_wal_inplace(
    wal_path: &Path,
    checkpoint_lsn: Lsn,
    safety_buffer: Option<u64>,
) -> Result<TruncateResult, StorageError> {
    // For now, use the copy-based approach which is simpler and atomic
    // In the future, we could implement a more efficient in-place truncation
    // by copying entries to the beginning of the file and truncating
    truncate_wal(wal_path, checkpoint_lsn, safety_buffer)
}

/// Get information about what would be truncated without actually truncating
pub fn preview_truncation(
    wal_path: &Path,
    checkpoint_lsn: Lsn,
    safety_buffer: Option<u64>,
) -> Result<TruncateResult, StorageError> {
    let safety_buffer = safety_buffer.unwrap_or(DEFAULT_SAFETY_BUFFER);
    let cutoff_lsn = checkpoint_lsn.saturating_sub(safety_buffer);

    let file = File::open(wal_path)
        .map_err(|e| StorageError::IoError(format!("Failed to open WAL: {}", e)))?;
    let reader = BufReader::new(file);
    let mut wal_reader = WalReader::open(reader)?;

    let mut entries_to_remove = 0u64;
    let mut entries_to_keep = 0u64;
    let mut oldest_kept_lsn: Option<Lsn> = None;
    let mut newest_kept_lsn: Option<Lsn> = None;
    let mut bytes_to_remove = 0u64;

    loop {
        let position_before = wal_reader.position();
        match wal_reader.read_entry()? {
            ReadResult::Entry(entry) => {
                let entry_size = wal_reader.position() - position_before;
                if entry.lsn > cutoff_lsn {
                    entries_to_keep += 1;
                    if oldest_kept_lsn.is_none() {
                        oldest_kept_lsn = Some(entry.lsn);
                    }
                    newest_kept_lsn = Some(entry.lsn);
                } else {
                    entries_to_remove += 1;
                    bytes_to_remove += entry_size;
                }
            }
            ReadResult::Eof => break,
            ReadResult::Corruption { .. } => break,
        }
    }

    Ok(TruncateResult {
        entries_removed: entries_to_remove,
        entries_kept: entries_to_keep,
        bytes_saved: bytes_to_remove,
        oldest_lsn: oldest_kept_lsn,
        newest_lsn: newest_kept_lsn,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use vibesql_types::SqlValue;

    use super::*;
    use crate::wal::entry::{WalEntry, WalOp};
    use tempfile::TempDir;

    fn create_test_wal(dir: &Path, num_entries: u64) -> PathBuf {
        let wal_path = dir.join("test.wal");
        let file = File::create(&wal_path).unwrap();
        let writer = BufWriter::new(file);
        let mut wal_writer = WalWriter::create(writer).unwrap();

        for i in 1..=num_entries {
            let entry = WalEntry::new(
                i,
                1234567890 + i,
                WalOp::Insert {
                    table_id: 1,
                    row_id: i,
                    values: vec![SqlValue::Integer(i as i64)],
                },
            );
            wal_writer.append(&entry).unwrap();
        }
        wal_writer.flush().unwrap();

        wal_path
    }

    #[test]
    fn test_truncate_wal_basic() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = create_test_wal(temp_dir.path(), 100);

        // Truncate at LSN 50 with safety buffer of 10
        let result = truncate_wal(&wal_path, 50, Some(10)).unwrap();

        // Should have removed entries 1-40, kept 41-100
        assert_eq!(result.entries_removed, 40);
        assert_eq!(result.entries_kept, 60);
        assert_eq!(result.oldest_lsn, Some(41));
        assert_eq!(result.newest_lsn, Some(100));

        // Verify the WAL is still readable
        let file = File::open(&wal_path).unwrap();
        let mut reader = WalReader::open(BufReader::new(file)).unwrap();
        let entries = reader.read_all().unwrap();
        assert_eq!(entries.len(), 60);
        assert_eq!(entries[0].lsn, 41);
    }

    #[test]
    fn test_truncate_wal_no_removal() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = create_test_wal(temp_dir.path(), 100);

        // Truncate at LSN 0 - nothing should be removed
        let result = truncate_wal(&wal_path, 0, Some(0)).unwrap();

        assert_eq!(result.entries_removed, 0);
        assert_eq!(result.entries_kept, 100);
    }

    #[test]
    fn test_truncate_wal_remove_all() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = create_test_wal(temp_dir.path(), 100);

        // Truncate at LSN 100 with no safety buffer - remove everything
        let result = truncate_wal(&wal_path, 100, Some(0)).unwrap();

        assert_eq!(result.entries_removed, 100);
        assert_eq!(result.entries_kept, 0);

        // Verify the WAL is empty (just header)
        let file = File::open(&wal_path).unwrap();
        let mut reader = WalReader::open(BufReader::new(file)).unwrap();
        let entries = reader.read_all().unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_preview_truncation() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = create_test_wal(temp_dir.path(), 100);

        let original_size = fs::metadata(&wal_path).unwrap().len();

        // Preview truncation
        let preview = preview_truncation(&wal_path, 50, Some(10)).unwrap();

        assert_eq!(preview.entries_removed, 40);
        assert_eq!(preview.entries_kept, 60);

        // File should be unchanged
        let new_size = fs::metadata(&wal_path).unwrap().len();
        assert_eq!(original_size, new_size);

        // Verify all entries still present
        let file = File::open(&wal_path).unwrap();
        let mut reader = WalReader::open(BufReader::new(file)).unwrap();
        let entries = reader.read_all().unwrap();
        assert_eq!(entries.len(), 100);
    }

    #[test]
    fn test_truncate_with_default_safety_buffer() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = create_test_wal(temp_dir.path(), 200);

        // Use default safety buffer (100)
        let result = truncate_wal(&wal_path, 150, None).unwrap();

        // Cutoff should be 150 - 100 = 50
        // Should keep entries 51-200
        assert_eq!(result.entries_removed, 50);
        assert_eq!(result.entries_kept, 150);
    }
}
