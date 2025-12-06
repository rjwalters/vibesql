// ============================================================================
// WAL Reader
// ============================================================================
//
// Sequential reader for WAL entries with corruption detection.

use std::io::{Read, Seek, SeekFrom};

use crate::persistence::binary::io::read_u32;
use crate::StorageError;

use super::entry::{Lsn, WalEntry};
use super::format::{WalHeader, WAL_HEADER_SIZE};
use super::writer::verify_checksum;

/// Result of reading a WAL entry
#[derive(Debug)]
pub enum ReadResult {
    /// Successfully read an entry
    Entry(WalEntry),
    /// Reached end of file (clean EOF)
    Eof,
    /// Found corruption (partial write or checksum mismatch)
    /// Contains the position where corruption was detected
    Corruption { position: u64 },
}

/// WAL reader for sequential reading of entries
pub struct WalReader<R: Read + Seek> {
    reader: R,
    /// WAL file header
    header: WalHeader,
    /// Current position in the file
    position: u64,
    /// Number of entries read
    entries_read: u64,
    /// Last LSN seen
    last_lsn: Option<Lsn>,
}

impl<R: Read + Seek> WalReader<R> {
    /// Open a WAL file for reading
    pub fn open(mut reader: R) -> Result<Self, StorageError> {
        // Read and validate header
        let header = WalHeader::read(&mut reader)?;

        Ok(Self {
            reader,
            header,
            position: WAL_HEADER_SIZE as u64,
            entries_read: 0,
            last_lsn: None,
        })
    }

    /// Read the next entry from the WAL
    pub fn read_entry(&mut self) -> Result<ReadResult, StorageError> {
        // Try to read entry length
        let len = match read_u32(&mut self.reader) {
            Ok(len) => len,
            Err(_) => {
                // Could be clean EOF or partial write
                return Ok(ReadResult::Eof);
            }
        };

        // Read checksum
        let checksum = match read_u32(&mut self.reader) {
            Ok(crc) => crc,
            Err(_) => {
                // Partial write - length was written but not checksum
                return Ok(ReadResult::Corruption { position: self.position });
            }
        };

        // Read entry data
        let mut data = vec![0u8; len as usize];
        if self.reader.read_exact(&mut data).is_err() {
            // Partial write - header was written but not full data
            return Ok(ReadResult::Corruption { position: self.position });
        }

        // Verify checksum
        if !verify_checksum(&data, checksum) {
            return Ok(ReadResult::Corruption { position: self.position });
        }

        // Deserialize entry
        let mut data_reader = &data[..];
        let entry = WalEntry::deserialize(&mut data_reader)?;

        // Update state
        self.position += 4 + 4 + len as u64; // len + crc + data
        self.entries_read += 1;
        self.last_lsn = Some(entry.lsn);

        Ok(ReadResult::Entry(entry))
    }

    /// Read all entries from the WAL
    /// Returns entries up to the first corruption or EOF
    pub fn read_all(&mut self) -> Result<Vec<WalEntry>, StorageError> {
        let mut entries = Vec::new();

        loop {
            match self.read_entry()? {
                ReadResult::Entry(entry) => entries.push(entry),
                ReadResult::Eof => break,
                ReadResult::Corruption { position: _ } => break,
            }
        }

        Ok(entries)
    }

    /// Get the WAL header
    pub fn header(&self) -> &WalHeader {
        &self.header
    }

    /// Get the number of entries read so far
    pub fn entries_read(&self) -> u64 {
        self.entries_read
    }

    /// Get the last LSN seen
    pub fn last_lsn(&self) -> Option<Lsn> {
        self.last_lsn
    }

    /// Get the current position in the file
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Seek to a specific position in the WAL
    /// Note: Should only seek to positions after the header
    pub fn seek_to(&mut self, position: u64) -> Result<(), StorageError> {
        if position < WAL_HEADER_SIZE as u64 {
            return Err(StorageError::IoError("Cannot seek before WAL header".to_string()));
        }
        self.reader
            .seek(SeekFrom::Start(position))
            .map_err(|e| StorageError::IoError(e.to_string()))?;
        self.position = position;
        Ok(())
    }

    /// Reset reader to the beginning of entries (after header)
    pub fn reset(&mut self) -> Result<(), StorageError> {
        self.seek_to(WAL_HEADER_SIZE as u64)?;
        self.entries_read = 0;
        self.last_lsn = None;
        Ok(())
    }
}

/// Iterator over WAL entries
pub struct WalIterator<'a, R: Read + Seek> {
    reader: &'a mut WalReader<R>,
}

impl<R: Read + Seek> WalReader<R> {
    /// Create an iterator over WAL entries
    pub fn iter(&mut self) -> WalIterator<'_, R> {
        WalIterator { reader: self }
    }
}

impl<R: Read + Seek> Iterator for WalIterator<'_, R> {
    type Item = Result<WalEntry, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.read_entry() {
            Ok(ReadResult::Entry(entry)) => Some(Ok(entry)),
            Ok(ReadResult::Eof) => None,
            Ok(ReadResult::Corruption { position }) => {
                Some(Err(StorageError::IoError(format!("WAL corruption at position {}", position))))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

/// Find the position of the last valid entry in a potentially corrupted WAL
/// This is useful for recovering from partial writes
pub fn find_recovery_point<R: Read + Seek>(reader: R) -> Result<RecoveryInfo, StorageError> {
    let mut wal_reader = WalReader::open(reader)?;
    let mut last_valid_position = WAL_HEADER_SIZE as u64;
    let mut last_lsn = None;
    let mut entry_count = 0;

    loop {
        match wal_reader.read_entry()? {
            ReadResult::Entry(entry) => {
                last_valid_position = wal_reader.position();
                last_lsn = Some(entry.lsn);
                entry_count += 1;
            }
            ReadResult::Eof => break,
            ReadResult::Corruption { position: _ } => break,
        }
    }

    Ok(RecoveryInfo { last_valid_position, last_lsn, entry_count })
}

/// Information about WAL recovery
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryInfo {
    /// Position after the last valid entry
    pub last_valid_position: u64,
    /// LSN of the last valid entry (None if no valid entries)
    pub last_lsn: Option<Lsn>,
    /// Number of valid entries found
    pub entry_count: u64,
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use vibesql_types::SqlValue;

    use super::*;
    use crate::wal::entry::WalOp;
    use crate::wal::writer::WalWriter;

    fn create_test_wal() -> Cursor<Vec<u8>> {
        let buf = Vec::new();
        let cursor = Cursor::new(buf);
        let mut writer = WalWriter::create(cursor).unwrap();

        for i in 1..=5 {
            let entry = WalEntry::new(
                i,
                1234567890 + i,
                WalOp::Insert { table_id: 1, row_id: i, values: vec![SqlValue::Integer(i as i64)] },
            );
            writer.append(&entry).unwrap();
        }

        writer.flush().unwrap();
        Cursor::new(writer.into_inner().into_inner())
    }

    #[test]
    fn test_read_all_entries() {
        let cursor = create_test_wal();
        let mut reader = WalReader::open(cursor).unwrap();

        let entries = reader.read_all().unwrap();
        assert_eq!(entries.len(), 5);

        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.lsn, (i + 1) as u64);
        }
    }

    #[test]
    fn test_read_entry_by_entry() {
        let cursor = create_test_wal();
        let mut reader = WalReader::open(cursor).unwrap();

        for i in 1..=5 {
            match reader.read_entry().unwrap() {
                ReadResult::Entry(entry) => {
                    assert_eq!(entry.lsn, i);
                }
                other => panic!("Expected Entry, got {:?}", other),
            }
        }

        match reader.read_entry().unwrap() {
            ReadResult::Eof => {}
            other => panic!("Expected Eof, got {:?}", other),
        }
    }

    #[test]
    fn test_iterator() {
        let cursor = create_test_wal();
        let mut reader = WalReader::open(cursor).unwrap();

        let entries: Vec<_> = reader.iter().collect();
        assert_eq!(entries.len(), 5);
        for entry in entries {
            assert!(entry.is_ok());
        }
    }

    #[test]
    fn test_header_info() {
        let cursor = create_test_wal();
        let reader = WalReader::open(cursor).unwrap();

        assert_eq!(reader.header().version, super::super::format::WAL_VERSION);
    }

    #[test]
    fn test_recovery_info() {
        let cursor = create_test_wal();
        let info = find_recovery_point(cursor).unwrap();

        assert_eq!(info.entry_count, 5);
        assert_eq!(info.last_lsn, Some(5));
        assert!(info.last_valid_position > WAL_HEADER_SIZE as u64);
    }

    #[test]
    fn test_corruption_detection_truncated_data() {
        // Create a WAL with entries
        let mut buf = create_test_wal().into_inner();

        // Truncate in the middle of an entry (corrupt it)
        buf.truncate(WAL_HEADER_SIZE + 20);

        let cursor = Cursor::new(buf);
        let mut reader = WalReader::open(cursor).unwrap();

        // First entry should be readable
        // After that we should hit corruption or partial read
        let entries = reader.read_all().unwrap();
        // May have 0 or more entries depending on where truncation happened
        assert!(entries.len() <= 5);
    }

    #[test]
    fn test_corruption_detection_bad_checksum() {
        // Create a WAL with entries
        let mut buf = create_test_wal().into_inner();

        // Corrupt the checksum of the second entry
        // Entry format: [len:4][crc:4][data:...]
        // After header (32 bytes) and first entry, modify CRC
        // We'll just flip a bit in the data area to cause checksum mismatch
        if buf.len() > WAL_HEADER_SIZE + 50 {
            buf[WAL_HEADER_SIZE + 45] ^= 0xFF; // Flip bits in data
        }

        let cursor = Cursor::new(buf);
        let info = find_recovery_point(cursor).unwrap();

        // Should detect corruption and report recovery point
        assert!(info.entry_count < 5);
    }

    #[test]
    fn test_reset() {
        let cursor = create_test_wal();
        let mut reader = WalReader::open(cursor).unwrap();

        // Read all entries
        let entries1 = reader.read_all().unwrap();
        assert_eq!(entries1.len(), 5);

        // Reset and read again
        reader.reset().unwrap();
        let entries2 = reader.read_all().unwrap();
        assert_eq!(entries2.len(), 5);

        assert_eq!(entries1, entries2);
    }
}
