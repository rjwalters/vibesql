// ============================================================================
// WAL File Format
// ============================================================================
//
// Defines the WAL file format constants and header structure.
//
// File Structure:
// ┌────────────────────────────────────────┐
// │ WAL Header (32 bytes)                  │
// │ - Magic: "VWAL" (4 bytes)              │
// │ - Version: u32                         │
// │ - Created: u64 timestamp               │
// │ - Reserved: 16 bytes                   │
// ├────────────────────────────────────────┤
// │ Entry 1: [len:u32][crc:u32][data:...]  │
// ├────────────────────────────────────────┤
// │ Entry 2: [len:u32][crc:u32][data:...]  │
// ├────────────────────────────────────────┤
// │ ...                                    │
// └────────────────────────────────────────┘

use std::io::Read;

use crate::persistence::binary::io::{read_u32, read_u64};
use crate::StorageError;

/// Magic number for WAL files: "VWAL"
pub const WAL_MAGIC: &[u8; 4] = b"VWAL";

/// Current WAL format version
pub const WAL_VERSION: u32 = 1;

/// Size of the WAL header in bytes
pub const WAL_HEADER_SIZE: usize = 32;

/// WAL file header
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalHeader {
    /// Format version
    pub version: u32,
    /// Creation timestamp (milliseconds since epoch)
    pub created_ms: u64,
}

impl WalHeader {
    /// Read and validate WAL header from a reader
    pub fn read<R: Read>(reader: &mut R) -> Result<Self, StorageError> {
        // Read magic number
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic).map_err(|e| StorageError::IoError(e.to_string()))?;

        if &magic != WAL_MAGIC {
            return Err(StorageError::IoError(format!(
                "Invalid WAL file: expected magic 'VWAL', got '{}'",
                String::from_utf8_lossy(&magic)
            )));
        }

        // Read version
        let version = read_u32(reader)?;
        if version > WAL_VERSION {
            return Err(StorageError::IoError(format!(
                "Unsupported WAL version: {} (current: {})",
                version, WAL_VERSION
            )));
        }

        // Read creation timestamp
        let created_ms = read_u64(reader)?;

        // Skip reserved bytes
        let mut reserved = [0u8; 16];
        reader.read_exact(&mut reserved).map_err(|e| StorageError::IoError(e.to_string()))?;

        Ok(Self { version, created_ms })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::persistence::binary::io::{write_u32, write_u64};

    fn create_valid_header() -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(WAL_MAGIC);
        write_u32(&mut buf, WAL_VERSION).unwrap();
        write_u64(&mut buf, 1234567890).unwrap();
        buf.extend_from_slice(&[0u8; 16]); // reserved
        buf
    }

    #[test]
    fn test_read_valid_header() {
        let buf = create_valid_header();
        let mut reader = Cursor::new(buf);

        let header = WalHeader::read(&mut reader).unwrap();
        assert_eq!(header.version, WAL_VERSION);
        assert_eq!(header.created_ms, 1234567890);
    }

    #[test]
    fn test_invalid_magic() {
        let mut buf = create_valid_header();
        buf[0..4].copy_from_slice(b"XXXX");
        let mut reader = Cursor::new(buf);

        let result = WalHeader::read(&mut reader);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid WAL file"));
    }

    #[test]
    fn test_unsupported_version() {
        let mut buf = Vec::new();
        buf.extend_from_slice(WAL_MAGIC);
        write_u32(&mut buf, WAL_VERSION + 100).unwrap(); // Future version
        write_u64(&mut buf, 1234567890).unwrap();
        buf.extend_from_slice(&[0u8; 16]);
        let mut reader = Cursor::new(buf);

        let result = WalHeader::read(&mut reader);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unsupported WAL version"));
    }

    #[test]
    fn test_header_size() {
        let buf = create_valid_header();
        assert_eq!(buf.len(), WAL_HEADER_SIZE);
    }
}
