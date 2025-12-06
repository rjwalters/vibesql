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

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::persistence::binary::io::{read_u32, read_u64, write_u32, write_u64};
use crate::wal::entry::Lsn;
use crate::wal::writer::verify_checksum;
use crate::StorageError;

/// Magic number for checkpoint files: "VCHK"
pub const CHECKPOINT_MAGIC: &[u8; 4] = b"VCHK";

/// Current checkpoint format version
pub const CHECKPOINT_VERSION: u32 = 1;

/// Size of the checkpoint header in bytes
pub const CHECKPOINT_HEADER_SIZE: usize = 32;

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
        writer
            .write_all(CHECKPOINT_MAGIC)
            .map_err(|e| StorageError::IoError(format!("Failed to write checkpoint magic: {}", e)))?;

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

    /// Read and validate checkpoint header from a reader
    pub fn read<R: Read>(reader: &mut R) -> Result<Self, StorageError> {
        // Read magic number
        let mut magic = [0u8; 4];
        reader
            .read_exact(&mut magic)
            .map_err(|e| StorageError::IoError(format!("Failed to read checkpoint magic: {}", e)))?;

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
        fs::create_dir_all(&checkpoint_dir)
            .map_err(|e| StorageError::IoError(format!("Failed to create checkpoint dir: {}", e)))?;

        // Find the next checkpoint ID by scanning existing checkpoints
        let next_checkpoint_id = Self::find_next_checkpoint_id(&checkpoint_dir)?;

        Ok(Self { checkpoint_dir, next_checkpoint_id })
    }

    /// Find the next checkpoint ID by scanning existing checkpoint files
    fn find_next_checkpoint_id(dir: &Path) -> Result<u64, StorageError> {
        let mut max_id = 0u64;

        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if let Some(id_str) = name.strip_prefix("checkpoint_").and_then(|s| s.strip_suffix(".vchk")) {
                        if let Ok(id) = id_str.parse::<u64>() {
                            max_id = max_id.max(id);
                        }
                    }
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
        let checksum = crc32(data);

        // Create temp file path
        let temp_path = self.checkpoint_dir.join(format!("checkpoint_{}.tmp", checkpoint_id));
        let final_path = self.checkpoint_dir.join(format!("checkpoint_{}.vchk", checkpoint_id));

        // Write to temp file
        {
            let file = File::create(&temp_path)
                .map_err(|e| StorageError::IoError(format!("Failed to create temp checkpoint: {}", e)))?;
            let mut writer = BufWriter::new(file);

            // Write header
            let header = CheckpointHeader::new(lsn, timestamp_ms, num_tables, checksum);
            header.write(&mut writer)?;

            // Write data
            writer
                .write_all(data)
                .map_err(|e| StorageError::IoError(format!("Failed to write checkpoint data: {}", e)))?;

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

        Ok(CheckpointInfo { path: final_path, lsn, timestamp_ms, num_tables, file_size })
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

        // Sort by LSN
        checkpoints.sort_by_key(|c| c.lsn);
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

        Ok(CheckpointInfo {
            path: path.to_path_buf(),
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

    // Verify checksum
    if !verify_checksum(&data, header.checksum) {
        return Err(StorageError::IoError(
            "Checkpoint data checksum mismatch - file may be corrupted".to_string(),
        ));
    }

    Ok((header, data))
}

/// CRC-32 implementation (same as in writer.rs for consistency)
fn crc32(data: &[u8]) -> u32 {
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

    let mut crc = 0xFFFFFFFF;
    for &byte in data {
        let index = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32_TABLE[index];
    }
    crc ^ 0xFFFFFFFF
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
    use super::*;
    use tempfile::TempDir;

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
}
