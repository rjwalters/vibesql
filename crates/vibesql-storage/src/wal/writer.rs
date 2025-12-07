// ============================================================================
// WAL Writer
// ============================================================================
//
// Append-only writer for WAL entries with CRC32 checksums.

use std::io::{Seek, SeekFrom, Write};

use crate::persistence::binary::io::{write_u32, write_u64};
use crate::StorageError;

use super::entry::{Lsn, WalEntry};
use super::format::{WAL_MAGIC, WAL_VERSION};

#[cfg(test)]
use super::format::WAL_HEADER_SIZE;

/// CRC-32 implementation using the IEEE polynomial
/// This is a simple, portable implementation that works on both native and WASM
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

/// WAL writer for appending entries to a WAL file
pub struct WalWriter<W: Write + Seek> {
    writer: W,
    /// Current log sequence number (next to be assigned)
    next_lsn: Lsn,
    /// Number of entries written
    entry_count: u64,
}

impl<W: Write + Seek> WalWriter<W> {
    /// Create a new WAL file with header
    pub fn create(mut writer: W) -> Result<Self, StorageError> {
        // Write header
        write_header(&mut writer)?;

        Ok(Self { writer, next_lsn: 1, entry_count: 0 })
    }

    /// Open an existing WAL file for appending
    /// Returns the writer positioned at the end, ready to append
    pub fn open(mut writer: W, next_lsn: Lsn) -> Result<Self, StorageError> {
        // Seek to end to append
        writer.seek(SeekFrom::End(0)).map_err(|e| StorageError::IoError(e.to_string()))?;

        Ok(Self { writer, next_lsn, entry_count: 0 })
    }

    /// Append an entry to the WAL
    /// Returns the LSN assigned to the entry
    pub fn append(&mut self, entry: &WalEntry) -> Result<Lsn, StorageError> {
        // Serialize the entry to a buffer first to calculate length and checksum
        let mut entry_buf = Vec::new();
        entry.serialize(&mut entry_buf)?;

        let len = entry_buf.len() as u32;
        let checksum = crc32(&entry_buf);

        // Write: [len:u32][crc:u32][data:...]
        write_u32(&mut self.writer, len)?;
        write_u32(&mut self.writer, checksum)?;
        self.writer.write_all(&entry_buf).map_err(|e| StorageError::IoError(e.to_string()))?;

        let assigned_lsn = entry.lsn;
        self.next_lsn = assigned_lsn + 1;
        self.entry_count += 1;

        Ok(assigned_lsn)
    }

    /// Append an operation, automatically assigning LSN and timestamp
    pub fn append_op(&mut self, op: super::entry::WalOp) -> Result<Lsn, StorageError> {
        let lsn = self.next_lsn;
        let timestamp_ms = current_timestamp_ms();

        let entry = WalEntry::new(lsn, timestamp_ms, op);
        self.append(&entry)
    }

    /// Get the next LSN that will be assigned
    pub fn next_lsn(&self) -> Lsn {
        self.next_lsn
    }

    /// Get the number of entries written
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Flush any buffered writes to the underlying storage
    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.writer.flush().map_err(|e| StorageError::IoError(e.to_string()))
    }

    /// Sync writes to disk (if supported by the underlying writer)
    /// For maximum durability, this should be called after critical operations
    pub fn sync(&mut self) -> Result<(), StorageError> {
        self.flush()
        // Note: For actual fsync behavior, the underlying writer would need
        // to be a File. The StorageBackend trait would handle this in Phase 2.
    }

    /// Consume the writer and return the underlying writer
    pub fn into_inner(self) -> W {
        self.writer
    }
}

/// Write the WAL file header
fn write_header<W: Write>(writer: &mut W) -> Result<(), StorageError> {
    // Magic number (4 bytes)
    writer.write_all(WAL_MAGIC).map_err(|e| StorageError::IoError(e.to_string()))?;

    // Version (4 bytes)
    write_u32(writer, WAL_VERSION)?;

    // Created timestamp (8 bytes)
    write_u64(writer, current_timestamp_ms())?;

    // Reserved (16 bytes)
    writer.write_all(&[0u8; 16]).map_err(|e| StorageError::IoError(e.to_string()))?;

    Ok(())
}

/// Get current timestamp in milliseconds since epoch
fn current_timestamp_ms() -> u64 {
    // Use instant crate for WASM compatibility
    use instant::SystemTime;
    SystemTime::now()
        .duration_since(instant::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Verify a CRC32 checksum
pub fn verify_checksum(data: &[u8], expected: u32) -> bool {
    crc32(data) == expected
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use vibesql_types::SqlValue;

    use super::*;
    use crate::wal::entry::WalOp;

    #[test]
    fn test_create_wal_file() {
        let buf = Vec::new();
        let cursor = Cursor::new(buf);

        let writer = WalWriter::create(cursor).unwrap();
        assert_eq!(writer.next_lsn(), 1);
        assert_eq!(writer.entry_count(), 0);
    }

    #[test]
    fn test_append_entry() {
        let buf = Vec::new();
        let cursor = Cursor::new(buf);

        let mut writer = WalWriter::create(cursor).unwrap();

        let entry = WalEntry::new(
            1,
            1234567890,
            WalOp::Insert {
                table_id: 1,
                row_id: 100,
                values: vec![SqlValue::Integer(42)],
            },
        );

        let lsn = writer.append(&entry).unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(writer.next_lsn(), 2);
        assert_eq!(writer.entry_count(), 1);
    }

    #[test]
    fn test_append_multiple_entries() {
        let buf = Vec::new();
        let cursor = Cursor::new(buf);

        let mut writer = WalWriter::create(cursor).unwrap();

        for i in 1..=10 {
            let entry = WalEntry::new(
                i,
                1234567890 + i,
                WalOp::Insert { table_id: 1, row_id: i, values: vec![SqlValue::Integer(i as i64)] },
            );
            let lsn = writer.append(&entry).unwrap();
            assert_eq!(lsn, i);
        }

        assert_eq!(writer.next_lsn(), 11);
        assert_eq!(writer.entry_count(), 10);
    }

    #[test]
    fn test_append_op() {
        let buf = Vec::new();
        let cursor = Cursor::new(buf);

        let mut writer = WalWriter::create(cursor).unwrap();

        let lsn = writer
            .append_op(WalOp::Insert {
                table_id: 1,
                row_id: 100,
                values: vec![SqlValue::Varchar(arcstr::ArcStr::from("test"))],
            })
            .unwrap();

        assert_eq!(lsn, 1);
        assert_eq!(writer.next_lsn(), 2);
    }

    #[test]
    fn test_crc32() {
        // Test vectors from the CRC-32 IEEE standard
        assert_eq!(crc32(b"123456789"), 0xCBF43926);
        assert_eq!(crc32(b""), 0x00000000);
        assert_eq!(crc32(b"a"), 0xE8B7BE43);
    }

    #[test]
    fn test_verify_checksum() {
        let data = b"test data for checksum";
        let checksum = crc32(data);
        assert!(verify_checksum(data, checksum));
        assert!(!verify_checksum(data, checksum + 1));
    }

    #[test]
    fn test_header_size() {
        let buf = Vec::new();
        let cursor = Cursor::new(buf);

        let writer = WalWriter::create(cursor).unwrap();
        let inner = writer.into_inner().into_inner();
        assert_eq!(inner.len(), WAL_HEADER_SIZE);
    }
}
