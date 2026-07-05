// ============================================================================
// Checkpoint System
// ============================================================================
//
// Implements periodic checkpointing that creates consistent snapshots and
// enables WAL truncation.
//
// ## Checkpoint File Format
//
// ```text
// ┌────────────────────────────────────────┐
// │ Checkpoint Header (32 bytes)           │
// │ - Magic: "VCHK" (4 bytes)              │
// │ - Version: u32                         │
// │ - LSN: u64 (WAL LSN at checkpoint)     │
// │ - Timestamp: u64                       │
// │ - Num Tables: u32                      │
// │ - Checksum: u32                        │
// ├────────────────────────────────────────┤
// │ Table Data (using existing .vbsql)     │
// └────────────────────────────────────────┘
// ```
//
// The checkpoint file reuses the existing binary format (.vbsql) for table
// data serialization, prefixed with a checkpoint-specific header.
//
// ## Checksum coverage (issue #5855)
//
// * **Version 2 (current)**: the CRC32 at bytes 28..32 covers the first 28
//   header bytes (magic, version, LSN, timestamp, num_tables) followed by the
//   entire payload. Flipping ANY bit in the file — header field or body —
//   fails verification. This matters because the header LSN drives both
//   checkpoint selection (newest-by-LSN) and WAL replay (entries at or below
//   the checkpoint LSN are skipped): an unprotected LSN flip could silently
//   open stale state or silently drop committed WAL entries.
// * **Version 1 (legacy, read-only)**: the CRC covers only the payload; the
//   header fields are NOT integrity-protected. Existing v1 files remain
//   readable; the cross-file creation-order guard in `recovery.rs` limits the
//   stale-selection blast radius for them.

use std::{
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use crate::{
    persistence::binary::io::{read_u32, read_u64, write_u32, write_u64},
    wal::entry::Lsn,
    StorageError,
};

/// Magic number for checkpoint files: "VCHK"
pub const CHECKPOINT_MAGIC: &[u8; 4] = b"VCHK";

/// Current checkpoint format version.
///
/// * v1: CRC32 covers the payload only (header fields unprotected).
/// * v2: CRC32 covers the first 28 header bytes + payload (issue #5855).
pub const CHECKPOINT_VERSION: u32 = 2;

/// Size of the checkpoint header in bytes
pub const CHECKPOINT_HEADER_SIZE: usize = 32;

/// Number of leading header bytes covered by the v2 checksum (everything
/// before the checksum field itself: magic, version, LSN, timestamp,
/// num_tables).
pub const CHECKSUMMED_HEADER_PREFIX_SIZE: usize = 28;

/// Checkpoint file header
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointHeader {
    /// Format version
    pub version: u32,
    /// WAL LSN at checkpoint time (all operations up to this LSN are included)
    pub lsn: Lsn,
    /// Timestamp when checkpoint was created (milliseconds since epoch)
    pub timestamp_ms: u64,
    /// Number of tables included in the checkpoint
    pub num_tables: u32,
    /// CRC32 checksum of the checkpoint data (excluding header)
    pub checksum: u32,
}

impl CheckpointHeader {
    /// Create a new checkpoint header
    pub fn new(lsn: Lsn, timestamp_ms: u64, num_tables: u32, checksum: u32) -> Self {
        Self { version: CHECKPOINT_VERSION, lsn, timestamp_ms, num_tables, checksum }
    }

    /// Write the checkpoint header to a writer
    pub fn write<W: Write>(&self, writer: &mut W) -> Result<(), StorageError> {
        // Magic number (4 bytes)
        writer.write_all(CHECKPOINT_MAGIC).map_err(|e| {
            StorageError::IoError(format!("Failed to write checkpoint magic: {}", e))
        })?;

        // Version (4 bytes)
        write_u32(writer, self.version)?;

        // LSN (8 bytes)
        write_u64(writer, self.lsn)?;

        // Timestamp (8 bytes)
        write_u64(writer, self.timestamp_ms)?;

        // Number of tables (4 bytes)
        write_u32(writer, self.num_tables)?;

        // Checksum (4 bytes)
        write_u32(writer, self.checksum)?;

        Ok(())
    }

    /// Serialize the CRC-covered header prefix (bytes 0..28: magic, version,
    /// LSN, timestamp, num_tables) exactly as `write` lays them out on disk.
    ///
    /// Used to compute/verify the v2 checksum, which covers this prefix
    /// followed by the payload (issue #5855).
    pub fn checksummed_prefix(&self) -> [u8; CHECKSUMMED_HEADER_PREFIX_SIZE] {
        let mut prefix = [0u8; CHECKSUMMED_HEADER_PREFIX_SIZE];
        prefix[0..4].copy_from_slice(CHECKPOINT_MAGIC);
        prefix[4..8].copy_from_slice(&self.version.to_le_bytes());
        prefix[8..16].copy_from_slice(&self.lsn.to_le_bytes());
        prefix[16..24].copy_from_slice(&self.timestamp_ms.to_le_bytes());
        prefix[24..28].copy_from_slice(&self.num_tables.to_le_bytes());
        prefix
    }

    /// Compute the checksum this header *should* carry for `data`, per the
    /// header's own format version:
    ///
    /// * v1 (legacy): CRC32 of the payload only.
    /// * v2+: CRC32 of the 28-byte header prefix followed by the payload.
    pub fn expected_checksum(&self, data: &[u8]) -> u32 {
        if self.version >= 2 {
            let mut hasher = Crc32::new();
            hasher.update(&self.checksummed_prefix());
            hasher.update(data);
            hasher.finalize()
        } else {
            crc32(data)
        }
    }

    /// Read and validate checkpoint header from a reader
    pub fn read<R: Read>(reader: &mut R) -> Result<Self, StorageError> {
        // Read magic number
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic).map_err(|e| {
            StorageError::IoError(format!("Failed to read checkpoint magic: {}", e))
        })?;

        if &magic != CHECKPOINT_MAGIC {
            return Err(StorageError::IoError(format!(
                "Invalid checkpoint file: expected magic 'VCHK', got '{}'",
                String::from_utf8_lossy(&magic)
            )));
        }

        // Read version
        let version = read_u32(reader)?;
        if version > CHECKPOINT_VERSION {
            return Err(StorageError::IoError(format!(
                "Unsupported checkpoint version: {} (current: {})",
                version, CHECKPOINT_VERSION
            )));
        }

        // Read LSN
        let lsn = read_u64(reader)?;

        // Read timestamp
        let timestamp_ms = read_u64(reader)?;

        // Read number of tables
        let num_tables = read_u32(reader)?;

        // Read checksum
        let checksum = read_u32(reader)?;

        Ok(Self { version, lsn, timestamp_ms, num_tables, checksum })
    }
}

/// Information about a completed checkpoint
#[derive(Debug, Clone)]
pub struct CheckpointInfo {
    /// Path to the checkpoint file
    pub path: PathBuf,
    /// Monotonic checkpoint id parsed from the `checkpoint_<id>.vchk` filename.
    ///
    /// Ids strictly increase in creation order, so they break ties when two
    /// checkpoints share an LSN — recovery can then deterministically pick the
    /// most recently written one instead of relying on directory iteration
    /// order (issue #5766).
    pub id: u64,
    /// LSN at which the checkpoint was taken
    pub lsn: Lsn,
    /// Timestamp when the checkpoint was created
    pub timestamp_ms: u64,
    /// Number of tables in the checkpoint
    pub num_tables: u32,
    /// Size of the checkpoint file in bytes
    pub file_size: u64,
}

/// Checkpoint writer for creating consistent database snapshots
pub struct CheckpointWriter {
    /// Directory where checkpoint files are stored
    checkpoint_dir: PathBuf,
    /// Next checkpoint ID
    next_checkpoint_id: u64,
}

impl CheckpointWriter {
    /// Create a new checkpoint writer
    pub fn new<P: AsRef<Path>>(checkpoint_dir: P) -> Result<Self, StorageError> {
        let checkpoint_dir = checkpoint_dir.as_ref().to_path_buf();

        // Create checkpoint directory if it doesn't exist
        fs::create_dir_all(&checkpoint_dir).map_err(|e| {
            StorageError::IoError(format!("Failed to create checkpoint dir: {}", e))
        })?;

        // Find the next checkpoint ID by scanning existing checkpoints
        let next_checkpoint_id = Self::find_next_checkpoint_id(&checkpoint_dir)?;

        Ok(Self { checkpoint_dir, next_checkpoint_id })
    }

    /// Find the next checkpoint ID by scanning existing checkpoint files
    fn find_next_checkpoint_id(dir: &Path) -> Result<u64, StorageError> {
        let mut max_id = 0u64;

        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                if let Some(id) = checkpoint_id_from_path(&entry.path()) {
                    max_id = max_id.max(id);
                }
            }
        }

        Ok(max_id + 1)
    }

    /// Create a checkpoint file with the given data
    ///
    /// The checkpoint is written atomically: data is first written to a temp file,
    /// then renamed to the final location.
    pub fn create_checkpoint(
        &mut self,
        lsn: Lsn,
        data: &[u8],
        num_tables: u32,
    ) -> Result<CheckpointInfo, StorageError> {
        let checkpoint_id = self.next_checkpoint_id;
        self.next_checkpoint_id += 1;

        let timestamp_ms = current_timestamp_ms();

        // v2 checksum: covers the 28-byte header prefix + the payload, so a
        // bit flip anywhere in the file (header LSN included) fails
        // verification on read (issue #5855). Computed in one streaming pass;
        // the payload is never re-read or copied.
        let mut header = CheckpointHeader::new(lsn, timestamp_ms, num_tables, 0);
        header.checksum = header.expected_checksum(data);

        // Create temp file path
        let temp_path = self.checkpoint_dir.join(format!("checkpoint_{}.tmp", checkpoint_id));
        let final_path = self.checkpoint_dir.join(format!("checkpoint_{}.vchk", checkpoint_id));

        // Write to temp file
        {
            let file = File::create(&temp_path).map_err(|e| {
                StorageError::IoError(format!("Failed to create temp checkpoint: {}", e))
            })?;
            let mut writer = BufWriter::new(file);

            // Write header
            header.write(&mut writer)?;

            // Write data
            writer.write_all(data).map_err(|e| {
                StorageError::IoError(format!("Failed to write checkpoint data: {}", e))
            })?;

            writer
                .flush()
                .map_err(|e| StorageError::IoError(format!("Failed to flush checkpoint: {}", e)))?;
        }

        // Atomically rename temp file to final path
        fs::rename(&temp_path, &final_path)
            .map_err(|e| StorageError::IoError(format!("Failed to finalize checkpoint: {}", e)))?;

        // Get file size
        let file_size = fs::metadata(&final_path)
            .map_err(|e| StorageError::IoError(format!("Failed to get checkpoint size: {}", e)))?
            .len();

        log::info!(
            "Created checkpoint {} at LSN {} ({} tables, {} bytes)",
            checkpoint_id,
            lsn,
            num_tables,
            file_size
        );

        Ok(CheckpointInfo {
            path: final_path,
            id: checkpoint_id,
            lsn,
            timestamp_ms,
            num_tables,
            file_size,
        })
    }

    /// Get the path to the checkpoint directory
    pub fn checkpoint_dir(&self) -> &Path {
        &self.checkpoint_dir
    }

    /// List all checkpoint files in order
    pub fn list_checkpoints(&self) -> Result<Vec<CheckpointInfo>, StorageError> {
        let mut checkpoints = Vec::new();

        if let Ok(entries) = fs::read_dir(&self.checkpoint_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "vchk") {
                    if let Ok(info) = Self::read_checkpoint_info(&path) {
                        checkpoints.push(info);
                    }
                }
            }
        }

        // Sort by LSN, then by checkpoint id as a deterministic tie-break so that
        // when two checkpoints share an LSN the most recently written one (higher
        // id) sorts last and "wins" newest-first selection during recovery
        // (issue #5766). Without the id tie-break, equal-LSN checkpoints were
        // ordered by directory-iteration order and recovery could load stale
        // state.
        checkpoints.sort_by(|a, b| a.lsn.cmp(&b.lsn).then(a.id.cmp(&b.id)));
        Ok(checkpoints)
    }

    /// Read checkpoint info from a file
    pub fn read_checkpoint_info(path: &Path) -> Result<CheckpointInfo, StorageError> {
        let file = File::open(path)
            .map_err(|e| StorageError::IoError(format!("Failed to open checkpoint: {}", e)))?;
        let mut reader = BufReader::new(file);

        let header = CheckpointHeader::read(&mut reader)?;

        let file_size = fs::metadata(path)
            .map_err(|e| StorageError::IoError(format!("Failed to get checkpoint size: {}", e)))?
            .len();

        let id = checkpoint_id_from_path(path).unwrap_or(0);

        Ok(CheckpointInfo {
            path: path.to_path_buf(),
            id,
            lsn: header.lsn,
            timestamp_ms: header.timestamp_ms,
            num_tables: header.num_tables,
            file_size,
        })
    }

    /// Find the latest checkpoint
    pub fn latest_checkpoint(&self) -> Result<Option<CheckpointInfo>, StorageError> {
        let checkpoints = self.list_checkpoints()?;
        Ok(checkpoints.into_iter().last())
    }

    /// Remove old checkpoints, keeping only the most recent N
    pub fn cleanup_old_checkpoints(&self, keep_count: usize) -> Result<usize, StorageError> {
        let checkpoints = self.list_checkpoints()?;
        let mut removed = 0;

        if checkpoints.len() > keep_count {
            let to_remove = checkpoints.len() - keep_count;
            for checkpoint in checkpoints.into_iter().take(to_remove) {
                if fs::remove_file(&checkpoint.path).is_ok() {
                    log::debug!("Removed old checkpoint: {:?}", checkpoint.path);
                    removed += 1;
                }
            }
        }

        Ok(removed)
    }
}

/// Parse the monotonic checkpoint id from a `checkpoint_<id>.vchk` path.
///
/// Returns `None` for paths that don't match the naming scheme.
fn checkpoint_id_from_path(path: &Path) -> Option<u64> {
    let name = path.file_name().and_then(|n| n.to_str())?;
    name.strip_prefix("checkpoint_").and_then(|s| s.strip_suffix(".vchk"))?.parse::<u64>().ok()
}

/// Read checkpoint data (excluding header) from a checkpoint file
pub fn read_checkpoint_data(path: &Path) -> Result<(CheckpointHeader, Vec<u8>), StorageError> {
    let file = File::open(path)
        .map_err(|e| StorageError::IoError(format!("Failed to open checkpoint: {}", e)))?;
    let mut reader = BufReader::new(file);

    // Read header
    let header = CheckpointHeader::read(&mut reader)?;

    // Read remaining data
    let mut data = Vec::new();
    reader
        .read_to_end(&mut data)
        .map_err(|e| StorageError::IoError(format!("Failed to read checkpoint data: {}", e)))?;

    // Verify checksum per the header's format version:
    //   v1 (legacy): payload only — header fields are unprotected.
    //   v2+: header prefix + payload — any bit flip in the file fails here
    //        (issue #5855).
    if header.expected_checksum(&data) != header.checksum {
        return Err(StorageError::IoError(
            "Checkpoint checksum mismatch - header or data corrupted".to_string(),
        ));
    }

    Ok((header, data))
}

const CRC32_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

/// Incremental CRC-32 (IEEE) hasher — same polynomial/table as `crc32` and
/// `writer.rs`, but streamable so the v2 checkpoint checksum can cover the
/// header prefix followed by the payload in a single pass without
/// concatenating them into a temporary buffer (issue #5855).
pub(crate) struct Crc32 {
    state: u32,
}

impl Crc32 {
    pub(crate) fn new() -> Self {
        Self { state: 0xFFFFFFFF }
    }

    pub(crate) fn update(&mut self, data: &[u8]) {
        let mut crc = self.state;
        for &byte in data {
            let index = ((crc ^ byte as u32) & 0xFF) as usize;
            crc = (crc >> 8) ^ CRC32_TABLE[index];
        }
        self.state = crc;
    }

    pub(crate) fn finalize(self) -> u32 {
        self.state ^ 0xFFFFFFFF
    }
}

/// CRC-32 implementation (same as in writer.rs for consistency)
///
/// `pub(crate)` so recovery tests can re-stamp a checkpoint envelope checksum
/// after deliberately patching the payload (issue #5807 forward-version tests).
pub(crate) fn crc32(data: &[u8]) -> u32 {
    let mut hasher = Crc32::new();
    hasher.update(data);
    hasher.finalize()
}

/// Get current timestamp in milliseconds since epoch
fn current_timestamp_ms() -> u64 {
    use instant::SystemTime;
    SystemTime::now()
        .duration_since(instant::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_checkpoint_header_roundtrip() {
        let header = CheckpointHeader::new(100, 1234567890, 5, 0xDEADBEEF);

        let mut buf = Vec::new();
        header.write(&mut buf).unwrap();

        assert_eq!(buf.len(), CHECKPOINT_HEADER_SIZE);

        let mut reader = &buf[..];
        let decoded = CheckpointHeader::read(&mut reader).unwrap();

        assert_eq!(header, decoded);
    }

    #[test]
    fn test_checkpoint_header_invalid_magic() {
        let mut buf = [0u8; CHECKPOINT_HEADER_SIZE];
        buf[0..4].copy_from_slice(b"XXXX");

        let mut reader = &buf[..];
        let result = CheckpointHeader::read(&mut reader);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid checkpoint file"));
    }

    #[test]
    fn test_checkpoint_writer_create() {
        let temp_dir = TempDir::new().unwrap();
        let writer = CheckpointWriter::new(temp_dir.path()).unwrap();

        assert!(writer.checkpoint_dir().exists());
    }

    #[test]
    fn test_checkpoint_writer_create_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let mut writer = CheckpointWriter::new(temp_dir.path()).unwrap();

        let data = b"test checkpoint data";
        let info = writer.create_checkpoint(100, data, 3).unwrap();

        assert_eq!(info.lsn, 100);
        assert_eq!(info.num_tables, 3);
        assert!(info.path.exists());
        assert!(info.file_size > CHECKPOINT_HEADER_SIZE as u64);
    }

    #[test]
    fn test_checkpoint_writer_list_checkpoints() {
        let temp_dir = TempDir::new().unwrap();
        let mut writer = CheckpointWriter::new(temp_dir.path()).unwrap();

        // Create multiple checkpoints
        writer.create_checkpoint(10, b"data1", 1).unwrap();
        writer.create_checkpoint(20, b"data2", 2).unwrap();
        writer.create_checkpoint(30, b"data3", 3).unwrap();

        let checkpoints = writer.list_checkpoints().unwrap();

        assert_eq!(checkpoints.len(), 3);
        assert_eq!(checkpoints[0].lsn, 10);
        assert_eq!(checkpoints[1].lsn, 20);
        assert_eq!(checkpoints[2].lsn, 30);
    }

    #[test]
    fn test_checkpoint_writer_latest() {
        let temp_dir = TempDir::new().unwrap();
        let mut writer = CheckpointWriter::new(temp_dir.path()).unwrap();

        assert!(writer.latest_checkpoint().unwrap().is_none());

        writer.create_checkpoint(10, b"data1", 1).unwrap();
        writer.create_checkpoint(20, b"data2", 2).unwrap();

        let latest = writer.latest_checkpoint().unwrap().unwrap();
        assert_eq!(latest.lsn, 20);
    }

    #[test]
    fn test_checkpoint_writer_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let mut writer = CheckpointWriter::new(temp_dir.path()).unwrap();

        // Create 5 checkpoints
        for i in 1..=5 {
            writer.create_checkpoint(i * 10, format!("data{}", i).as_bytes(), 1).unwrap();
        }

        assert_eq!(writer.list_checkpoints().unwrap().len(), 5);

        // Keep only 2
        let removed = writer.cleanup_old_checkpoints(2).unwrap();
        assert_eq!(removed, 3);
        assert_eq!(writer.list_checkpoints().unwrap().len(), 2);

        // Verify we kept the latest ones
        let remaining = writer.list_checkpoints().unwrap();
        assert_eq!(remaining[0].lsn, 40);
        assert_eq!(remaining[1].lsn, 50);
    }

    #[test]
    fn test_equal_lsn_checkpoints_tiebreak_by_id() {
        // Regression for #5766: when two checkpoints share an LSN, recovery must
        // deterministically prefer the most recently written one. `list_checkpoints`
        // sorts by (lsn, id) and `latest_checkpoint` returns the last, so the
        // higher-id checkpoint at the same LSN must win — regardless of the order
        // the filesystem happens to enumerate the directory.
        let temp_dir = TempDir::new().unwrap();
        let mut writer = CheckpointWriter::new(temp_dir.path()).unwrap();

        // Three checkpoints, all stamped at the SAME LSN (the pre-fix failure
        // mode where the LSN counter reset each restart). The last one written
        // (highest id) carries the newest state.
        let _c1 = writer.create_checkpoint(5, b"oldest", 1).unwrap();
        let _c2 = writer.create_checkpoint(5, b"middle", 1).unwrap();
        let c3 = writer.create_checkpoint(5, b"newest-state", 1).unwrap();

        let checkpoints = writer.list_checkpoints().unwrap();
        assert_eq!(checkpoints.len(), 3);
        // Sorted ascending by id when LSNs tie.
        assert!(checkpoints[0].id < checkpoints[1].id);
        assert!(checkpoints[1].id < checkpoints[2].id);

        let latest = writer.latest_checkpoint().unwrap().unwrap();
        assert_eq!(latest.id, c3.id, "highest-id checkpoint must win an LSN tie");

        let (_, data) = read_checkpoint_data(&latest.path).unwrap();
        assert_eq!(data, b"newest-state", "recovery must load the newest equal-LSN checkpoint");
    }

    #[test]
    fn test_checkpoint_info_carries_id() {
        let temp_dir = TempDir::new().unwrap();
        let mut writer = CheckpointWriter::new(temp_dir.path()).unwrap();

        let info = writer.create_checkpoint(10, b"data", 1).unwrap();
        assert_eq!(info.id, 1);

        // The id round-trips through a fresh read of the directory.
        let listed = writer.list_checkpoints().unwrap();
        assert_eq!(listed[0].id, 1);
    }

    #[test]
    fn test_read_checkpoint_data() {
        let temp_dir = TempDir::new().unwrap();
        let mut writer = CheckpointWriter::new(temp_dir.path()).unwrap();

        let original_data = b"test checkpoint data for verification";
        let info = writer.create_checkpoint(100, original_data, 5).unwrap();

        let (header, data) = read_checkpoint_data(&info.path).unwrap();

        assert_eq!(header.lsn, 100);
        assert_eq!(header.num_tables, 5);
        assert_eq!(data, original_data);
    }

    #[test]
    fn test_crc32() {
        // Test vectors
        assert_eq!(crc32(b"123456789"), 0xCBF43926);
        assert_eq!(crc32(b""), 0x00000000);
    }

    #[test]
    fn test_crc32_incremental_matches_oneshot() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let mut hasher = Crc32::new();
        hasher.update(&data[..10]);
        hasher.update(&data[10..]);
        assert_eq!(hasher.finalize(), crc32(data));
    }

    /// Issue #5855: flipping ANY byte of a checkpoint file — header fields
    /// (version, LSN, timestamp, num_tables, checksum) or payload — must fail
    /// verification. Before v2 the CRC covered only the payload, so a bit
    /// flip in the header LSN silently changed checkpoint selection and WAL
    /// replay while the file still "verified".
    #[test]
    fn test_any_byte_flip_fails_checksum() {
        let temp_dir = TempDir::new().unwrap();
        let mut writer = CheckpointWriter::new(temp_dir.path()).unwrap();

        let data = b"payload bytes that stand in for a binary snapshot";
        let info = writer.create_checkpoint(42, data, 3).unwrap();
        let original = fs::read(&info.path).unwrap();

        // Sanity: pristine file verifies.
        read_checkpoint_data(&info.path).unwrap();

        for offset in 0..original.len() {
            let mut corrupted = original.clone();
            corrupted[offset] ^= 0xFF;
            fs::write(&info.path, &corrupted).unwrap();

            assert!(
                read_checkpoint_data(&info.path).is_err(),
                "byte flip at offset {} must fail verification",
                offset
            );
        }

        // Restore and confirm it verifies again (the loop's failures came
        // from the flips, not from the write cycle).
        fs::write(&info.path, &original).unwrap();
        read_checkpoint_data(&info.path).unwrap();
    }

    /// Legacy v1 checkpoints (checksum over payload only) must remain
    /// readable: existing databases carry them and a hard error on every
    /// pre-upgrade checkpoint would brick every existing DB.
    #[test]
    fn test_v1_legacy_checkpoint_still_reads() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("checkpoint_1.vchk");

        let data = b"legacy v1 payload";
        // Hand-write a v1 envelope: checksum = crc32(payload) only.
        let header = CheckpointHeader {
            version: 1,
            lsn: 7,
            timestamp_ms: 123,
            num_tables: 1,
            checksum: crc32(data),
        };
        let mut bytes = Vec::new();
        header.write(&mut bytes).unwrap();
        bytes.extend_from_slice(data);
        fs::write(&path, &bytes).unwrap();

        let (read_header, read_data) = read_checkpoint_data(&path).unwrap();
        assert_eq!(read_header.version, 1);
        assert_eq!(read_header.lsn, 7);
        assert_eq!(read_data, data);

        // v1 payload corruption is still caught.
        let mut corrupted = bytes.clone();
        *corrupted.last_mut().unwrap() ^= 0xFF;
        fs::write(&path, &corrupted).unwrap();
        assert!(read_checkpoint_data(&path).is_err(), "v1 payload flip must fail");
    }

    /// A v2 checkpoint whose version byte is rolled back to 1 must not verify
    /// under the (weaker) v1 rule: the stored checksum was computed over
    /// prefix+payload, so the v1 payload-only check fails. No downgrade path
    /// re-opens the header-unprotected hole.
    #[test]
    fn test_version_rollback_flip_fails() {
        let temp_dir = TempDir::new().unwrap();
        let mut writer = CheckpointWriter::new(temp_dir.path()).unwrap();
        let info = writer.create_checkpoint(42, b"some payload", 1).unwrap();

        let mut bytes = fs::read(&info.path).unwrap();
        bytes[4..8].copy_from_slice(&1u32.to_le_bytes());
        fs::write(&info.path, &bytes).unwrap();

        assert!(read_checkpoint_data(&info.path).is_err());
    }

    /// Perf sanity for the v2 checksum (issue #5855): write + read a ~50MB
    /// checkpoint and print timings. The v2 change adds only 28 extra bytes
    /// to the CRC input, so the delta vs v1 must be noise. Run with:
    /// `cargo test -p vibesql-storage --release bench_checkpoint_50mb -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn bench_checkpoint_50mb_write_read() {
        let temp_dir = TempDir::new().unwrap();
        let mut writer = CheckpointWriter::new(temp_dir.path()).unwrap();

        // ~50MB of non-trivial bytes.
        let data: Vec<u8> =
            (0..50_000_000usize).map(|i| (i.wrapping_mul(2654435761) >> 16) as u8).collect();

        let t0 = std::time::Instant::now();
        let info = writer.create_checkpoint(1, &data, 10).unwrap();
        let write_elapsed = t0.elapsed();

        let t1 = std::time::Instant::now();
        let (_, read_back) = read_checkpoint_data(&info.path).unwrap();
        let read_elapsed = t1.elapsed();

        assert_eq!(read_back.len(), data.len());
        println!(
            "checkpoint 50MB: write {:?} (checksum+IO), read {:?} (IO+verify)",
            write_elapsed, read_elapsed
        );
    }

    /// Forward envelope versions stay a hard error (fail closed, #5807).
    #[test]
    fn test_forward_envelope_version_is_error() {
        let temp_dir = TempDir::new().unwrap();
        let mut writer = CheckpointWriter::new(temp_dir.path()).unwrap();
        let info = writer.create_checkpoint(42, b"some payload", 1).unwrap();

        let mut bytes = fs::read(&info.path).unwrap();
        bytes[4..8].copy_from_slice(&(CHECKPOINT_VERSION + 1).to_le_bytes());
        fs::write(&info.path, &bytes).unwrap();

        let err = read_checkpoint_data(&info.path).unwrap_err();
        assert!(
            err.to_string().contains("Unsupported checkpoint version"),
            "expected version error, got: {err}"
        );
    }
}
