// ============================================================================
// Low-Level I/O Primitives
// ============================================================================
//
// Handles reading and writing primitive types with proper endianness.

use std::io::{Read, Write};

use crate::StorageError;

pub fn read_u8<R: Read>(reader: &mut R) -> Result<u8, StorageError> {
    let mut buf = [0u8; 1];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::NotImplemented(format!("Read error: {}", e)))?;
    Ok(buf[0])
}

pub fn write_u32<W: Write>(writer: &mut W, value: u32) -> Result<(), StorageError> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

pub fn read_u32<R: Read>(reader: &mut R) -> Result<u32, StorageError> {
    let mut buf = [0u8; 4];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::NotImplemented(format!("Read error: {}", e)))?;
    Ok(u32::from_le_bytes(buf))
}

pub fn write_u64<W: Write>(writer: &mut W, value: u64) -> Result<(), StorageError> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

pub fn read_u64<R: Read>(reader: &mut R) -> Result<u64, StorageError> {
    let mut buf = [0u8; 8];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::NotImplemented(format!("Read error: {}", e)))?;
    Ok(u64::from_le_bytes(buf))
}

pub fn write_i16<W: Write>(writer: &mut W, value: i16) -> Result<(), StorageError> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

pub fn read_i16<R: Read>(reader: &mut R) -> Result<i16, StorageError> {
    let mut buf = [0u8; 2];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::NotImplemented(format!("Read error: {}", e)))?;
    Ok(i16::from_le_bytes(buf))
}

pub fn write_i64<W: Write>(writer: &mut W, value: i64) -> Result<(), StorageError> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

pub fn read_i64<R: Read>(reader: &mut R) -> Result<i64, StorageError> {
    let mut buf = [0u8; 8];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::NotImplemented(format!("Read error: {}", e)))?;
    Ok(i64::from_le_bytes(buf))
}

pub fn write_f32<W: Write>(writer: &mut W, value: f32) -> Result<(), StorageError> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

pub fn read_f32<R: Read>(reader: &mut R) -> Result<f32, StorageError> {
    let mut buf = [0u8; 4];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::NotImplemented(format!("Read error: {}", e)))?;
    Ok(f32::from_le_bytes(buf))
}

pub fn write_f64<W: Write>(writer: &mut W, value: f64) -> Result<(), StorageError> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

pub fn read_f64<R: Read>(reader: &mut R) -> Result<f64, StorageError> {
    let mut buf = [0u8; 8];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::NotImplemented(format!("Read error: {}", e)))?;
    Ok(f64::from_le_bytes(buf))
}

pub fn write_bool<W: Write>(writer: &mut W, value: bool) -> Result<(), StorageError> {
    writer
        .write_all(&[if value { 1 } else { 0 }])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

pub fn read_bool<R: Read>(reader: &mut R) -> Result<bool, StorageError> {
    let mut buf = [0u8; 1];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::NotImplemented(format!("Read error: {}", e)))?;
    Ok(buf[0] != 0)
}

pub fn write_string<W: Write>(writer: &mut W, s: &str) -> Result<(), StorageError> {
    let bytes = s.as_bytes();
    write_u32(writer, bytes.len() as u32)?;
    writer.write_all(bytes).map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

pub fn read_string<R: Read>(reader: &mut R) -> Result<String, StorageError> {
    let len = read_u32(reader)?;
    let mut buf = vec![0u8; len as usize];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::NotImplemented(format!("Read error: {}", e)))?;
    String::from_utf8(buf)
        .map_err(|e| StorageError::NotImplemented(format!("Invalid UTF-8: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitives() {
        let mut buf = Vec::new();

        write_u32(&mut buf, 12345).unwrap();
        write_i64(&mut buf, -9876543210).unwrap();
        write_f64(&mut buf, 3.14159).unwrap();
        write_bool(&mut buf, true).unwrap();
        write_string(&mut buf, "Hello, VBSQL!").unwrap();

        let mut reader = &buf[..];
        assert_eq!(read_u32(&mut reader).unwrap(), 12345);
        assert_eq!(read_i64(&mut reader).unwrap(), -9876543210);
        assert!((read_f64(&mut reader).unwrap() - 3.14159).abs() < 1e-10);
        assert!(read_bool(&mut reader).unwrap());
        assert_eq!(read_string(&mut reader).unwrap(), "Hello, VBSQL!");
    }
}
