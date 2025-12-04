// ============================================================================
// Binary File Format: Header and Type Tags
// ============================================================================
//
// Handles file format constants, version management, and type tag definitions.

use std::io::{Read, Write};

use crate::StorageError;

/// Magic number for vibesql binary format: "VBSQL" in ASCII
pub const MAGIC: &[u8; 5] = b"VBSQL";

/// Current format version
/// Version history:
/// - v1: Initial binary format
/// - v2: Added sequence persistence and column default_value expressions
/// - v3: Added primary key and unique constraints persistence
pub const VERSION: u8 = 3;

/// Type tags for binary serialization
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeTag {
    Null = 0x00,
    Smallint = 0x01,
    Integer = 0x02,
    Bigint = 0x03,
    Unsigned = 0x04,
    Numeric = 0x05,
    Float = 0x06,
    Real = 0x07,
    Double = 0x08,
    Character = 0x10,
    Varchar = 0x11,
    Boolean = 0x20,
    Date = 0x30,
    Time = 0x31,
    Timestamp = 0x32,
    Interval = 0x33,
    Vector = 0x40,
}

impl TypeTag {
    pub fn from_u8(tag: u8) -> Result<Self, StorageError> {
        match tag {
            0x00 => Ok(TypeTag::Null),
            0x01 => Ok(TypeTag::Smallint),
            0x02 => Ok(TypeTag::Integer),
            0x03 => Ok(TypeTag::Bigint),
            0x04 => Ok(TypeTag::Unsigned),
            0x05 => Ok(TypeTag::Numeric),
            0x06 => Ok(TypeTag::Float),
            0x07 => Ok(TypeTag::Real),
            0x08 => Ok(TypeTag::Double),
            0x10 => Ok(TypeTag::Character),
            0x11 => Ok(TypeTag::Varchar),
            0x20 => Ok(TypeTag::Boolean),
            0x30 => Ok(TypeTag::Date),
            0x31 => Ok(TypeTag::Time),
            0x32 => Ok(TypeTag::Timestamp),
            0x33 => Ok(TypeTag::Interval),
            0x40 => Ok(TypeTag::Vector),
            _ => Err(StorageError::NotImplemented(format!("Unknown type tag: 0x{:02X}", tag))),
        }
    }
}

/// Write binary file header
///
/// Format:
/// - Magic number (5 bytes)
/// - Version (1 byte)
/// - Flags (1 byte) - reserved for future use
/// - Reserved (9 bytes)
///
/// Total: 16 bytes
pub fn write_header<W: Write>(writer: &mut W) -> Result<(), StorageError> {
    // Magic number (5 bytes)
    writer
        .write_all(MAGIC)
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

    // Version (1 byte)
    writer
        .write_all(&[VERSION])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

    // Flags (1 byte) - reserved for future use (compression, encryption, etc.)
    writer
        .write_all(&[0])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

    // Reserved (9 bytes)
    writer
        .write_all(&[0; 9])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

    Ok(())
}

/// Read and validate binary file header
/// Returns the format version for backward compatibility handling
pub fn read_header<R: Read>(reader: &mut R) -> Result<u8, StorageError> {
    // Read and validate magic number
    let mut magic = [0u8; 5];
    reader
        .read_exact(&mut magic)
        .map_err(|e| StorageError::NotImplemented(format!("Failed to read header: {}", e)))?;

    if &magic != MAGIC {
        return Err(StorageError::NotImplemented(format!(
            "Invalid file format: expected VBSQL magic number, got {:?}",
            String::from_utf8_lossy(&magic)
        )));
    }

    // Read and validate version
    let mut version = [0u8; 1];
    reader
        .read_exact(&mut version)
        .map_err(|e| StorageError::NotImplemented(format!("Failed to read version: {}", e)))?;

    if version[0] > VERSION {
        return Err(StorageError::NotImplemented(format!(
            "Unsupported format version: {} (current version: {})",
            version[0], VERSION
        )));
    }

    // Read flags (reserved for future use)
    let mut flags = [0u8; 1];
    reader
        .read_exact(&mut flags)
        .map_err(|e| StorageError::NotImplemented(format!("Failed to read flags: {}", e)))?;

    // Skip reserved bytes
    let mut reserved = [0u8; 9];
    reader.read_exact(&mut reserved).map_err(|e| {
        StorageError::NotImplemented(format!("Failed to read reserved bytes: {}", e))
    })?;

    Ok(version[0])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let mut buf = Vec::new();
        write_header(&mut buf).unwrap();

        assert_eq!(buf.len(), 16);
        assert_eq!(&buf[0..5], MAGIC);
        assert_eq!(buf[5], VERSION);

        let mut reader = &buf[..];
        let version = read_header(&mut reader).unwrap();
        assert_eq!(version, VERSION);
    }
}
