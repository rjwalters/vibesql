// ============================================================================
// Database Persistence Module
// ============================================================================
//
// Provides multiple formats for database persistence:
//
// 1. SQL dump format (human-readable, portable SQL statements):
//    - `save`: SQL dump generation and serialization
//    - `load`: SQL dump parsing and deserialization utilities
//
// 2. Binary format (fast, efficient):
//    - `binary`: Binary serialization/deserialization
//
// 3. Compressed binary format (zstd-compressed, smallest size):
//    - `binary`: Compressed binary serialization/deserialization
//
// 4. JSON format (structured, tool-friendly):
//    - `json`: JSON serialization/deserialization
//
// The `save_sql_dump`, `save_binary`, `save_compressed`, `save_json`, and
// `load_json` methods are implemented directly on the `Database` type via
// impl blocks in their respective modules.
//
// Load utilities are exported for use by the CLI layer to parse and execute
// dump files.

use std::{fs, io::Read, path::Path};

pub mod binary;
pub mod json;
pub mod load;
mod save;

#[cfg(test)]
mod tests;

/// Persistence format detection and auto-loading
impl crate::Database {
    /// Load database from file with automatic format detection
    ///
    /// Detects format based on:
    /// 1. File extension (.vbsql for binary, .vbsqlz for compressed, .json for JSON, .sql for SQL
    ///    dump)
    /// 2. Magic number in file header (if extension is ambiguous)
    ///
    /// # Example
    /// ```no_run
    /// # use vibesql_storage::Database;
    /// // Auto-detects format from extension and content
    /// let db = Database::load("database.vbsql").unwrap();
    /// let db2 = Database::load("database.vbsqlz").unwrap();
    /// let db3 = Database::load("database.json").unwrap();
    /// let db4 = Database::load("database.sql").unwrap();
    /// ```
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, crate::StorageError> {
        let path_ref = path.as_ref();
        let format = detect_format(path_ref)?;

        match format {
            PersistenceFormat::Binary => Self::load_binary(path),
            PersistenceFormat::BinaryCompressed => Self::load_compressed(path),
            PersistenceFormat::Json => Self::load_json(path),
            PersistenceFormat::Sql => {
                // SQL dump requires executor for parsing, so we return an error
                // directing users to use the executor layer's load_sql_dump function
                Err(crate::StorageError::NotImplemented(
                    "SQL dump loading requires the executor layer. \
                     Use vibesql_executor::load_sql_dump() instead, or use binary format (.vbsql/.vbsqlz) or JSON format (.json)"
                        .to_string(),
                ))
            }
        }
    }
}

/// Persistence format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceFormat {
    /// SQL dump format (.sql) - human-readable text
    Sql,
    /// Binary format (.vbsql) - efficient binary
    Binary,
    /// Compressed binary format (.vbsqlz) - zstd-compressed binary
    BinaryCompressed,
    /// JSON format (.json) - structured, tool-friendly
    Json,
}

/// Detect persistence format from file
fn detect_format<P: AsRef<Path>>(path: P) -> Result<PersistenceFormat, crate::StorageError> {
    let path_ref = path.as_ref();

    // For .vbsqlz, we know it's compressed
    if let Some(ext) = path_ref.extension() {
        match ext.to_str() {
            Some("vbsqlz") => return Ok(PersistenceFormat::BinaryCompressed),
            Some("json") => return Ok(PersistenceFormat::Json),
            Some("sql") => return Ok(PersistenceFormat::Sql),
            _ => {}
        }
    }

    // For .vbsql or unknown extensions, check magic number to distinguish
    // between compressed and uncompressed binary formats
    // (save() with compression feature creates compressed content even with .vbsql extension)
    let mut file = fs::File::open(path_ref).map_err(|e| {
        crate::StorageError::NotImplemented(format!("Failed to open file {:?}: {}", path_ref, e))
    })?;

    let mut magic = [0u8; 5];
    if file.read_exact(&mut magic).is_ok() && &magic == b"VBSQL" {
        return Ok(PersistenceFormat::Binary);
    }

    // Try reading as zstd-compressed (check for zstd magic number 0x28, 0xB5, 0x2F, 0xFD)
    file = fs::File::open(path_ref).map_err(|e| {
        crate::StorageError::NotImplemented(format!("Failed to open file {:?}: {}", path_ref, e))
    })?;
    let mut zstd_magic = [0u8; 4];
    if file.read_exact(&mut zstd_magic).is_ok() && &zstd_magic == b"\x28\xB5\x2F\xFD" {
        return Ok(PersistenceFormat::BinaryCompressed);
    }

    // Check for JSON by looking for opening brace
    file = fs::File::open(path_ref).map_err(|e| {
        crate::StorageError::NotImplemented(format!("Failed to open file {:?}: {}", path_ref, e))
    })?;
    let mut first_byte = [0u8; 1];
    if file.read_exact(&mut first_byte).is_ok() && first_byte[0] == b'{' {
        return Ok(PersistenceFormat::Json);
    }

    // Default to SQL if we can't determine
    Ok(PersistenceFormat::Sql)
}
