// ============================================================================
// Binary Persistence Format
// ============================================================================
//
// Efficient binary serialization for vibesql databases.
//
// File Format:
// - Header (16 bytes): Magic number, version, flags
// - Catalog section: Schemas, tables, indexes, roles
// - Data section: Table data with type tags
//
// Uses little-endian byte order for cross-platform compatibility.

use std::{
    fs::File,
    io::{BufReader, BufWriter, Write},
    path::Path,
};

use crate::{Database, StorageError};

// Public submodules
pub mod catalog;
pub mod data;
pub mod expression;
pub mod format;
pub mod io;
pub mod value;

// Re-export public API
pub use catalog::{read_catalog, read_catalog_v, write_catalog};
pub use data::{read_data, write_data};
pub use expression::{read_expression, write_expression};
pub use format::{read_header, write_header};

impl Database {
    /// Save database in efficient binary format
    ///
    /// Binary format is faster and more compact than SQL dumps.
    /// Use `.vbsql` extension to indicate binary format.
    ///
    /// # Example
    /// ```no_run
    /// # use vibesql_storage::Database;
    /// let db = Database::new();
    /// db.save_binary("database.vbsql").unwrap();
    /// ```
    pub fn save_binary<P: AsRef<Path>>(&self, path: P) -> Result<(), StorageError> {
        let file = File::create(path)
            .map_err(|e| StorageError::NotImplemented(format!("Failed to create file: {}", e)))?;

        let mut writer = BufWriter::new(file);

        // Write header
        write_header(&mut writer)?;

        // Write catalog section
        write_catalog(&mut writer, self)?;

        // Write data section
        write_data(&mut writer, self)?;

        writer
            .flush()
            .map_err(|e| StorageError::NotImplemented(format!("Failed to flush: {}", e)))?;

        Ok(())
    }

    /// Load database from binary format
    ///
    /// Reads a binary `.vbsql` file and reconstructs the database.
    ///
    /// # Example
    /// ```no_run
    /// # use vibesql_storage::Database;
    /// let db = Database::load_binary("database.vbsql").unwrap();
    /// ```
    pub fn load_binary<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let file = File::open(path.as_ref()).map_err(|e| {
            StorageError::NotImplemented(format!("Failed to open file {:?}: {}", path.as_ref(), e))
        })?;

        let mut reader = BufReader::new(file);

        // Read and validate header, get version for backward compatibility
        let version = read_header(&mut reader)?;

        // Read catalog section with version awareness
        let mut db = read_catalog_v(&mut reader, version)?;

        // Read data section
        read_data(&mut reader, &mut db)?;

        Ok(db)
    }

    /// Save database in default format
    ///
    /// Uses compressed format when `compression` feature is enabled (default),
    /// otherwise falls back to uncompressed binary format.
    ///
    /// # Example
    /// ```no_run
    /// # use vibesql_storage::Database;
    /// let db = Database::new();
    /// db.save("database.vbsql").unwrap();
    /// ```
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<(), StorageError> {
        #[cfg(feature = "compression")]
        {
            self.save_compressed(path)
        }
        #[cfg(not(feature = "compression"))]
        {
            self.save_binary(path)
        }
    }

    /// Save database in uncompressed binary format
    ///
    /// Use this if you need uncompressed `.vbsql` files (e.g., for debugging
    /// or when compression overhead is not desired).
    ///
    /// # Example
    /// ```no_run
    /// # use vibesql_storage::Database;
    /// let db = Database::new();
    /// db.save_uncompressed("database.vbsql").unwrap();
    /// ```
    pub fn save_uncompressed<P: AsRef<Path>>(&self, path: P) -> Result<(), StorageError> {
        self.save_binary(path)
    }

    /// Save database in compressed binary format (zstd compression)
    ///
    /// Creates a `.vbsqlz` file containing zstd-compressed binary data.
    /// Typically 50-70% smaller than uncompressed `.vbsql` files.
    ///
    /// Note: This method requires the `compression` feature to be enabled.
    ///
    /// # Example
    /// ```no_run
    /// # use vibesql_storage::Database;
    /// let db = Database::new();
    /// db.save_compressed("database.vbsqlz").unwrap();
    /// ```
    #[cfg(feature = "compression")]
    pub fn save_compressed<P: AsRef<Path>>(&self, path: P) -> Result<(), StorageError> {
        // First, save to temporary in-memory buffer
        let mut uncompressed_data = Vec::new();
        {
            let mut writer = BufWriter::new(&mut uncompressed_data);

            // Write header
            write_header(&mut writer)?;

            // Write catalog section
            write_catalog(&mut writer, self)?;

            // Write data section
            write_data(&mut writer, self)?;

            writer
                .flush()
                .map_err(|e| StorageError::NotImplemented(format!("Failed to flush: {}", e)))?;
        }

        // Compress the data using zstd (level 3 - good balance of speed and compression)
        let compressed_data = zstd::encode_all(&uncompressed_data[..], 3)
            .map_err(|e| StorageError::NotImplemented(format!("Compression failed: {}", e)))?;

        // Write compressed data to file
        std::fs::write(path.as_ref(), compressed_data)
            .map_err(|e| StorageError::NotImplemented(format!("Failed to write file: {}", e)))?;

        Ok(())
    }

    /// Load database from compressed binary format
    ///
    /// Reads a zstd-compressed `.vbsqlz` file and reconstructs the database.
    ///
    /// Note: This method requires the `compression` feature to be enabled.
    ///
    /// # Example
    /// ```no_run
    /// # use vibesql_storage::Database;
    /// let db = Database::load_compressed("database.vbsqlz").unwrap();
    /// ```
    #[cfg(feature = "compression")]
    pub fn load_compressed<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        // Read compressed file
        let compressed_data = std::fs::read(path.as_ref()).map_err(|e| {
            StorageError::NotImplemented(format!("Failed to read file {:?}: {}", path.as_ref(), e))
        })?;

        // Decompress using zstd
        let uncompressed_data = zstd::decode_all(&compressed_data[..])
            .map_err(|e| StorageError::NotImplemented(format!("Decompression failed: {}", e)))?;

        // Parse the uncompressed binary data
        let mut reader = BufReader::new(&uncompressed_data[..]);

        // Read and validate header, get version for backward compatibility
        let version = read_header(&mut reader)?;

        // Read catalog section with version awareness
        let mut db = read_catalog_v(&mut reader, version)?;

        // Read data section
        read_data(&mut reader, &mut db)?;

        Ok(db)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        format::{MAGIC, VERSION},
        io::*,
        *,
    };

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
