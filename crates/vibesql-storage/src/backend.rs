//! Storage Backend Abstraction for Cross-Platform Storage
//!
//! This module provides a trait-based abstraction over filesystem operations,
//! enabling vibesql to run on different platforms including WebAssembly with OPFS.

#![allow(clippy::suspicious_open_options)]

#[cfg(not(target_arch = "wasm32"))]
use std::io;

use crate::StorageError;

/// Trait for platform-specific file operations
///
/// Implementations provide file I/O for different platforms:
/// - `NativeFile`: Standard filesystem (Linux, macOS, Windows)
/// - `OpfsFile`: Origin Private File System (WebAssembly/browsers)
pub trait StorageFile: Send + Sync {
    /// Read data from the file at a specific offset
    ///
    /// # Arguments
    /// * `offset` - Byte offset from start of file
    /// * `buf` - Buffer to read data into
    ///
    /// # Returns
    /// Number of bytes read, or error if operation fails
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize, StorageError>;

    /// Write data to the file at a specific offset
    ///
    /// # Arguments
    /// * `offset` - Byte offset from start of file
    /// * `buf` - Data to write
    ///
    /// # Returns
    /// Number of bytes written, or error if operation fails
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<usize, StorageError>;

    /// Ensure all data is persisted to storage
    ///
    /// # Returns
    /// Ok on success, error if sync fails
    fn sync_all(&mut self) -> Result<(), StorageError>;

    /// Ensure file data (not metadata) is persisted to storage
    ///
    /// # Returns
    /// Ok on success, error if sync fails
    fn sync_data(&mut self) -> Result<(), StorageError>;

    /// Get the current size of the file in bytes
    ///
    /// # Returns
    /// Size in bytes, or error if operation fails
    fn size(&self) -> Result<u64, StorageError>;
}

/// Trait for platform-specific storage backend
///
/// Implementations provide filesystem operations for different platforms:
/// - `NativeStorage`: Standard filesystem using std::fs
/// - `OpfsStorage`: Browser-based Origin Private File System
pub trait StorageBackend: Send + Sync {
    /// Create a new file at the specified path
    ///
    /// If the file already exists, it will be truncated.
    ///
    /// # Arguments
    /// * `path` - Path to create the file
    ///
    /// # Returns
    /// File handle, or error if creation fails
    fn create_file(&self, path: &str) -> Result<Box<dyn StorageFile>, StorageError>;

    /// Open an existing file, or create it if it doesn't exist
    ///
    /// # Arguments
    /// * `path` - Path to open/create
    ///
    /// # Returns
    /// File handle, or error if operation fails
    fn open_file(&self, path: &str) -> Result<Box<dyn StorageFile>, StorageError>;

    /// Delete a file at the specified path
    ///
    /// # Arguments
    /// * `path` - Path to delete
    ///
    /// # Returns
    /// Ok on success, error if deletion fails or file doesn't exist
    fn delete_file(&self, path: &str) -> Result<(), StorageError>;

    /// Check if a file exists at the specified path
    ///
    /// # Arguments
    /// * `path` - Path to check
    ///
    /// # Returns
    /// true if file exists, false otherwise
    fn file_exists(&self, path: &str) -> bool;

    /// Get the size of a file in bytes
    ///
    /// # Arguments
    /// * `path` - Path to check
    ///
    /// # Returns
    /// Size in bytes, or error if file doesn't exist
    fn file_size(&self, path: &str) -> Result<u64, StorageError>;
}

/// Native filesystem storage implementation using std::fs
///
/// This backend provides storage using the standard library's filesystem operations.
/// It works on all platforms that support std::fs (Linux, macOS, Windows).
#[cfg(not(target_arch = "wasm32"))]
pub mod native {
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::{Path, PathBuf};

    #[cfg(not(target_arch = "wasm32"))]
    use parking_lot::Mutex;

    #[cfg(target_arch = "wasm32")]
    use std::sync::Mutex;

    use super::*;

    /// Native file implementation using std::fs::File
    pub struct NativeFile {
        file: Mutex<File>,
    }

    impl StorageFile for NativeFile {
        fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize, StorageError> {
            let mut file = self.file.lock();

            file.seek(SeekFrom::Start(offset)).map_err(|e| StorageError::IoError(e.to_string()))?;

            match file.read_exact(buf) {
                Ok(()) => Ok(buf.len()),
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // Try to read what we can
                    file.seek(SeekFrom::Start(offset))
                        .map_err(|e| StorageError::IoError(e.to_string()))?;
                    file.read(buf).map_err(|e| StorageError::IoError(e.to_string()))
                }
                Err(e) => Err(StorageError::IoError(e.to_string())),
            }
        }

        fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<usize, StorageError> {
            let mut file = self.file.lock();

            file.seek(SeekFrom::Start(offset)).map_err(|e| StorageError::IoError(e.to_string()))?;

            file.write_all(buf).map_err(|e| StorageError::IoError(e.to_string()))?;
            Ok(buf.len())
        }

        fn sync_all(&mut self) -> Result<(), StorageError> {
            let file = self.file.lock();
            file.sync_all().map_err(|e| StorageError::IoError(e.to_string()))
        }

        fn sync_data(&mut self) -> Result<(), StorageError> {
            let file = self.file.lock();
            file.sync_data().map_err(|e| StorageError::IoError(e.to_string()))
        }

        fn size(&self) -> Result<u64, StorageError> {
            let file = self.file.lock();
            Ok(file.metadata().map_err(|e| StorageError::IoError(e.to_string()))?.len())
        }
    }

    /// Native storage backend using std::fs
    pub struct NativeStorage {
        root: PathBuf,
    }

    impl NativeStorage {
        /// Create a new native storage backend
        ///
        /// # Arguments
        /// * `root` - Root directory for all files (will be created if it doesn't exist)
        pub fn new<P: AsRef<Path>>(root: P) -> Result<Self, StorageError> {
            let root = root.as_ref().to_path_buf();

            // Create root directory if it doesn't exist
            if !root.exists() {
                std::fs::create_dir_all(&root).map_err(|e| StorageError::IoError(e.to_string()))?;
            }

            Ok(NativeStorage { root })
        }

        /// Get the full path for a relative path
        fn full_path(&self, path: &str) -> PathBuf {
            self.root.join(path)
        }
    }

    impl StorageBackend for NativeStorage {
        fn create_file(&self, path: &str) -> Result<Box<dyn StorageFile>, StorageError> {
            let full_path = self.full_path(path);

            // Create parent directories if needed
            if let Some(parent) = full_path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
            }

            // Open with read+write permissions and truncate if exists
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&full_path)
                .map_err(|e| StorageError::IoError(e.to_string()))?;

            Ok(Box::new(NativeFile { file: Mutex::new(file) }))
        }

        fn open_file(&self, path: &str) -> Result<Box<dyn StorageFile>, StorageError> {
            let full_path = self.full_path(path);

            // Create parent directories if needed
            if let Some(parent) = full_path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
            }

            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&full_path)
                .map_err(|e| StorageError::IoError(e.to_string()))?;

            Ok(Box::new(NativeFile { file: Mutex::new(file) }))
        }

        fn delete_file(&self, path: &str) -> Result<(), StorageError> {
            let full_path = self.full_path(path);
            std::fs::remove_file(&full_path).map_err(|e| StorageError::IoError(e.to_string()))
        }

        fn file_exists(&self, path: &str) -> bool {
            self.full_path(path).exists()
        }

        fn file_size(&self, path: &str) -> Result<u64, StorageError> {
            let full_path = self.full_path(path);
            let metadata =
                std::fs::metadata(&full_path).map_err(|e| StorageError::IoError(e.to_string()))?;
            Ok(metadata.len())
        }
    }

    #[cfg(test)]
    mod tests {
        use tempfile::TempDir;

        use super::*;

        #[test]
        fn test_native_file_operations() {
            let temp_dir = TempDir::new().unwrap();
            let storage = NativeStorage::new(temp_dir.path()).unwrap();

            // Create and write to file
            let mut file = storage.create_file("test.db").unwrap();

            let data = b"Hello, Storage!";
            let written = file.write_at(0, data).unwrap();
            assert_eq!(written, data.len());

            file.sync_all().unwrap();

            // Read back
            let mut buf = vec![0u8; data.len()];
            let read = file.read_at(0, &mut buf).unwrap();
            assert_eq!(read, data.len());
            assert_eq!(&buf, data);

            // Check size
            let size = file.size().unwrap();
            assert_eq!(size, data.len() as u64);
        }

        #[test]
        fn test_native_storage_operations() {
            let temp_dir = TempDir::new().unwrap();
            let storage = NativeStorage::new(temp_dir.path()).unwrap();

            // File shouldn't exist initially
            assert!(!storage.file_exists("test.db"));

            // Create file
            let mut file = storage.create_file("test.db").unwrap();
            file.write_at(0, b"test").unwrap();
            drop(file);

            // Now it should exist
            assert!(storage.file_exists("test.db"));

            // Check size
            let size = storage.file_size("test.db").unwrap();
            assert_eq!(size, 4);

            // Delete file
            storage.delete_file("test.db").unwrap();
            assert!(!storage.file_exists("test.db"));
        }

        #[test]
        fn test_native_storage_with_subdirectories() {
            let temp_dir = TempDir::new().unwrap();
            let storage = NativeStorage::new(temp_dir.path()).unwrap();

            // Create file in subdirectory (should auto-create parent dirs)
            let mut file = storage.create_file("subdir/nested/test.db").unwrap();
            file.write_at(0, b"nested").unwrap();
            drop(file);

            assert!(storage.file_exists("subdir/nested/test.db"));
        }

        #[test]
        fn test_read_write_at_different_offsets() {
            let temp_dir = TempDir::new().unwrap();
            let storage = NativeStorage::new(temp_dir.path()).unwrap();

            let mut file = storage.create_file("test.db").unwrap();

            // Write at offset 0
            file.write_at(0, b"AAAA").unwrap();

            // Write at offset 100
            file.write_at(100, b"BBBB").unwrap();

            // Read at offset 0
            let mut buf = vec![0u8; 4];
            file.read_at(0, &mut buf).unwrap();
            assert_eq!(&buf, b"AAAA");

            // Read at offset 100
            file.read_at(100, &mut buf).unwrap();
            assert_eq!(&buf, b"BBBB");
        }
    }
}

/// Re-export native storage for convenience
#[cfg(not(target_arch = "wasm32"))]
pub use native::{NativeFile, NativeStorage};

/// OPFS storage implementation for WebAssembly browsers
///
/// This backend provides storage using the Origin Private File System API,
/// enabling persistent storage in web browsers.
#[cfg(target_arch = "wasm32")]
pub mod opfs;

/// Re-export OPFS storage for convenience
#[cfg(target_arch = "wasm32")]
pub use opfs::{MemoryFile, MemoryStorage, OpfsFile, OpfsStorage};
