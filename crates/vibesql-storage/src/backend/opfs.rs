//! Origin Private File System (OPFS) Storage Backend for WebAssembly
//!
//! This module provides a storage backend using the browser's Origin Private File System API,
//! enabling persistent disk-backed storage for vibesql running in web browsers.
//!
//! # Browser Compatibility
//!
//! - Chrome 86+
//! - Firefox 111+
//! - Safari 15.2+
//!
//! # Architecture
//!
//! OPFS is inherently asynchronous. This module provides async initialization via
//! `OpfsStorage::new_async()` and uses internal async operations with synchronous
//! wrappers via `wasm_bindgen_futures::spawn_local` for StorageBackend trait compatibility.

use js_sys::{Object, Reflect, Uint8Array};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::mpsc::{channel, Receiver, Sender};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemWritableFileStream};

use crate::{
    backend::{StorageBackend, StorageFile},
    StorageError,
};

/// OPFS-specific error types
#[derive(Debug)]
pub enum OpfsError {
    /// Browser doesn't support OPFS API
    NotSupported,
    /// Storage quota exceeded
    QuotaExceeded,
    /// Security or permission error
    SecurityError,
    /// File or directory not found
    NotFound,
    /// File handle in invalid state
    InvalidState,
    /// JavaScript error
    JsError(String),
    /// Channel communication error
    ChannelError(String),
}

impl From<OpfsError> for StorageError {
    fn from(error: OpfsError) -> Self {
        match error {
            OpfsError::NotSupported => {
                StorageError::IoError("OPFS not supported in this browser".to_string())
            }
            OpfsError::QuotaExceeded => StorageError::IoError("Storage quota exceeded".to_string()),
            OpfsError::SecurityError => {
                StorageError::IoError("Security error accessing OPFS".to_string())
            }
            OpfsError::NotFound => StorageError::IoError("File not found".to_string()),
            OpfsError::InvalidState => {
                StorageError::IoError("File handle in invalid state".to_string())
            }
            OpfsError::JsError(msg) => StorageError::IoError(format!("OPFS error: {}", msg)),
            OpfsError::ChannelError(msg) => {
                StorageError::IoError(format!("Channel error: {}", msg))
            }
        }
    }
}

impl From<JsValue> for OpfsError {
    fn from(js_error: JsValue) -> Self {
        // Try to extract error name
        let error_name = Reflect::get(&js_error, &"name".into()).ok().and_then(|v| v.as_string());

        match error_name.as_deref() {
            Some("SecurityError") => OpfsError::SecurityError,
            Some("QuotaExceededError") => OpfsError::QuotaExceeded,
            Some("NotFoundError") => OpfsError::NotFound,
            Some("InvalidStateError") => OpfsError::InvalidState,
            _ => {
                // Try to get error message
                let message = Reflect::get(&js_error, &"message".into())
                    .ok()
                    .and_then(|v| v.as_string())
                    .unwrap_or_else(|| format!("{:?}", js_error));
                OpfsError::JsError(message)
            }
        }
    }
}

/// Helper function to block on async operations in WASM
///
/// This spawns an async task and blocks the current thread until it completes.
/// In WASM, "blocking" means yielding control to the event loop.
fn block_on<F, T>(future: F) -> Result<T, OpfsError>
where
    F: std::future::Future<Output = Result<T, OpfsError>> + 'static,
    T: 'static,
{
    let (tx, rx): (Sender<Result<T, OpfsError>>, Receiver<Result<T, OpfsError>>) = channel();

    wasm_bindgen_futures::spawn_local(async move {
        let result = future.await;
        let _ = tx.send(result);
    });

    rx.recv().map_err(|e| OpfsError::ChannelError(e.to_string()))?
}

/// OPFS storage backend for browser-based storage
///
/// Note: This struct uses Rc<RefCell<>> instead of Arc<Mutex<>> because
/// WASM is single-threaded and web-sys types are not Send/Sync.
pub struct OpfsStorage {
    root: Rc<RefCell<FileSystemDirectoryHandle>>,
}

// SAFETY: WASM is single-threaded, so Send/Sync are safe even though
// the underlying types (Rc, RefCell, web-sys types) are !Send/!Sync.
// These trait implementations are required by the StorageBackend trait.
unsafe impl Send for OpfsStorage {}
unsafe impl Sync for OpfsStorage {}

impl OpfsStorage {
    /// Create a new OPFS storage backend asynchronously
    ///
    /// This is the primary initialization method for OPFS storage.
    /// It must be called from an async context.
    ///
    /// # Returns
    ///
    /// Returns an error if OPFS is not supported or cannot be initialized
    ///
    /// # Example
    /// ```rust,no_run
    /// # use vibesql_storage::backend::OpfsStorage;
    /// # async fn example() {
    /// let storage = OpfsStorage::new_async().await.unwrap();
    /// # }
    /// ```
    pub async fn new_async() -> Result<Self, OpfsError> {
        // Get window object
        let window = web_sys::window().ok_or(OpfsError::NotSupported)?;

        let navigator = window.navigator();

        // Check if storage API exists
        if !Reflect::has(&navigator, &"storage".into()).unwrap_or(false) {
            return Err(OpfsError::NotSupported);
        }

        let storage = navigator.storage();

        // Try to get directory handle
        let root_promise = storage.get_directory();
        let root_js = JsFuture::from(root_promise).await.map_err(|e| OpfsError::from(e))?;

        let root: FileSystemDirectoryHandle = root_js.into();

        Ok(Self { root: Rc::new(RefCell::new(root)) })
    }

    /// Get a file handle, optionally creating it if it doesn't exist
    async fn get_file_handle_async(
        &self,
        path: &str,
        create: bool,
    ) -> Result<FileSystemFileHandle, OpfsError> {
        let root = self.root.borrow().clone();

        let promise = if create {
            // Use FileSystemGetFileOptions to set create: true
            let options = web_sys::FileSystemGetFileOptions::new();
            options.set_create(true);
            root.get_file_handle_with_options(path, &options)
        } else {
            root.get_file_handle(path)
        };

        let handle_js = JsFuture::from(promise).await.map_err(|e| OpfsError::from(e))?;

        Ok(handle_js.into())
    }

    /// Get a file handle (synchronous wrapper)
    fn get_file_handle(&self, path: &str, create: bool) -> Result<FileSystemFileHandle, OpfsError> {
        let path_owned = path.to_string();
        let storage_clone = Self { root: self.root.clone() };

        block_on(async move { storage_clone.get_file_handle_async(&path_owned, create).await })
    }
}

impl StorageBackend for OpfsStorage {
    fn create_file(&self, path: &str) -> Result<Box<dyn StorageFile>, StorageError> {
        let handle = self.get_file_handle(path, true).map_err(|e| StorageError::from(e))?;

        Ok(Box::new(OpfsFile::new(handle)))
    }

    fn open_file(&self, path: &str) -> Result<Box<dyn StorageFile>, StorageError> {
        let handle = self.get_file_handle(path, true).map_err(|e| StorageError::from(e))?;

        Ok(Box::new(OpfsFile::new(handle)))
    }

    fn delete_file(&self, path: &str) -> Result<(), StorageError> {
        let path_owned = path.to_string();
        let root = self.root.clone();

        block_on(async move {
            let root_handle = root.borrow().clone();
            let promise = root_handle.remove_entry(&path_owned);
            JsFuture::from(promise).await.map_err(|e| OpfsError::from(e))?;
            Ok(())
        })
        .map_err(|e: OpfsError| StorageError::from(e))
    }

    fn file_exists(&self, path: &str) -> bool {
        self.get_file_handle(path, false).is_ok()
    }

    fn file_size(&self, path: &str) -> Result<u64, StorageError> {
        let handle = self.get_file_handle(path, false).map_err(|e| StorageError::from(e))?;

        let file = OpfsFile::new(handle);
        file.size()
    }
}

/// OPFS file handle with read/write operations
///
/// Note: This struct uses Rc<RefCell<>> instead of Arc<Mutex<>> because
/// WASM is single-threaded and web-sys types are not Send/Sync.
pub struct OpfsFile {
    handle: Rc<RefCell<FileSystemFileHandle>>,
}

// SAFETY: WASM is single-threaded, so Send/Sync are safe even though
// the underlying types (Rc, RefCell, web-sys types) are !Send/!Sync.
// These trait implementations are required by the StorageFile trait.
unsafe impl Send for OpfsFile {}
unsafe impl Sync for OpfsFile {}

impl OpfsFile {
    /// Create a new OpfsFile from a file handle
    pub fn new(handle: FileSystemFileHandle) -> Self {
        Self { handle: Rc::new(RefCell::new(handle)) }
    }

    /// Get the File object for reading
    async fn get_file_async(&self) -> Result<web_sys::File, OpfsError> {
        let handle = self.handle.borrow().clone();
        let promise = handle.get_file();
        let file_js = JsFuture::from(promise).await.map_err(|e| OpfsError::from(e))?;

        Ok(file_js.into())
    }

    /// Read data at a specific offset (async)
    async fn read_at_async(&self, offset: u64, buf: &mut [u8]) -> Result<usize, OpfsError> {
        let file: web_sys::File = self.get_file_async().await?;

        let end = offset + buf.len() as u64;
        let blob = file
            .slice_with_f64_and_f64(offset as f64, end as f64)
            .map_err(|e| OpfsError::from(e))?;

        let array_buffer_promise = blob.array_buffer();
        let array_buffer =
            JsFuture::from(array_buffer_promise).await.map_err(|e| OpfsError::from(e))?;

        let uint8_array = Uint8Array::new(&array_buffer);
        let bytes_read = uint8_array.length() as usize;

        // Copy data to buffer
        uint8_array.copy_to(&mut buf[..bytes_read]);

        Ok(bytes_read)
    }

    /// Write data at a specific offset (async)
    async fn write_at_async(&self, offset: u64, buf: &[u8]) -> Result<usize, OpfsError> {
        let handle = self.handle.borrow().clone();

        // Create writable stream
        let writable_promise = handle.create_writable();
        let writable_js = JsFuture::from(writable_promise).await.map_err(|e| OpfsError::from(e))?;

        let writable: FileSystemWritableFileStream = writable_js.into();

        // For non-zero offsets, we need to seek first
        // The OPFS API write() method accepts various types including command objects
        if offset > 0 {
            // Create a seek command object
            let seek_cmd = Object::new();
            Reflect::set(&seek_cmd, &"type".into(), &"seek".into())
                .map_err(|e| OpfsError::from(e))?;
            Reflect::set(&seek_cmd, &"position".into(), &(offset as f64).into())
                .map_err(|e| OpfsError::from(e))?;

            // Use Reflect to call the write method with the seek command object
            let write_fn =
                Reflect::get(&writable, &"write".into()).map_err(|e| OpfsError::from(e))?;

            let promise_js =
                Reflect::apply(&write_fn.into(), &writable, &js_sys::Array::of1(&seek_cmd))
                    .map_err(|e| OpfsError::from(e))?;

            JsFuture::from(js_sys::Promise::from(promise_js))
                .await
                .map_err(|e| OpfsError::from(e))?;
        }

        // Write data
        let uint8_array = Uint8Array::from(buf);
        let write_promise =
            writable.write_with_buffer_source(&uint8_array).map_err(|e| OpfsError::from(e))?;
        JsFuture::from(write_promise).await.map_err(|e| OpfsError::from(e))?;

        // Close the writable stream
        let close_promise = writable.close();
        JsFuture::from(close_promise).await.map_err(|e| OpfsError::from(e))?;

        Ok(buf.len())
    }
}

impl StorageFile for OpfsFile {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize, StorageError> {
        let handle_clone = self.handle.clone();
        let buf_len = buf.len();

        // We need to create a temporary buffer for the async operation
        let result = block_on(async move {
            let file = OpfsFile { handle: handle_clone };
            let mut temp_buf = vec![0u8; buf_len];
            let bytes_read = file.read_at_async(offset, &mut temp_buf).await?;
            Ok((temp_buf, bytes_read))
        })
        .map_err(|e: OpfsError| StorageError::from(e))?;

        let (temp_buf, bytes_read) = result;
        buf[..bytes_read].copy_from_slice(&temp_buf[..bytes_read]);
        Ok(bytes_read)
    }

    fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<usize, StorageError> {
        let handle_clone = self.handle.clone();
        let buf_owned = buf.to_vec();

        block_on(async move {
            let file = OpfsFile { handle: handle_clone };
            file.write_at_async(offset, &buf_owned).await
        })
        .map_err(|e: OpfsError| StorageError::from(e))
    }

    fn sync_all(&mut self) -> Result<(), StorageError> {
        // OPFS writes are atomic via the writable stream close operation
        // No explicit sync needed
        Ok(())
    }

    fn sync_data(&mut self) -> Result<(), StorageError> {
        // OPFS writes are atomic via the writable stream close operation
        // No explicit sync needed
        Ok(())
    }

    fn size(&self) -> Result<u64, StorageError> {
        let handle_clone = self.handle.clone();

        block_on(async move {
            let file = OpfsFile { handle: handle_clone };
            let web_file: web_sys::File = file.get_file_async().await?;
            Ok(web_file.size() as u64)
        })
        .map_err(|e: OpfsError| StorageError::from(e))
    }
}

// ============================================================================
// Memory Storage - Temporary in-memory backend for WASM initialization
// ============================================================================

/// Simple in-memory storage backend for WASM
///
/// This is used as a temporary placeholder during database initialization
/// before OPFS storage is asynchronously initialized. It does not persist data.
pub struct MemoryStorage {
    files: Rc<RefCell<HashMap<String, Vec<u8>>>>,
}

// SAFETY: WASM is single-threaded
unsafe impl Send for MemoryStorage {}
unsafe impl Sync for MemoryStorage {}

impl MemoryStorage {
    /// Create a new in-memory storage backend
    pub fn new() -> Self {
        Self { files: Rc::new(RefCell::new(HashMap::new())) }
    }
}

impl StorageBackend for MemoryStorage {
    fn create_file(&self, path: &str) -> Result<Box<dyn StorageFile>, StorageError> {
        let mut files = self.files.borrow_mut();
        files.insert(path.to_string(), Vec::new());
        Ok(Box::new(MemoryFile { path: path.to_string(), files: self.files.clone() }))
    }

    fn open_file(&self, path: &str) -> Result<Box<dyn StorageFile>, StorageError> {
        let mut files = self.files.borrow_mut();
        files.entry(path.to_string()).or_insert_with(Vec::new);
        Ok(Box::new(MemoryFile { path: path.to_string(), files: self.files.clone() }))
    }

    fn delete_file(&self, path: &str) -> Result<(), StorageError> {
        let mut files = self.files.borrow_mut();
        files.remove(path);
        Ok(())
    }

    fn file_exists(&self, path: &str) -> bool {
        let files = self.files.borrow();
        files.contains_key(path)
    }

    fn file_size(&self, path: &str) -> Result<u64, StorageError> {
        let files = self.files.borrow();
        files
            .get(path)
            .map(|data| data.len() as u64)
            .ok_or_else(|| StorageError::IoError("File not found".to_string()))
    }
}

/// In-memory file implementation
pub struct MemoryFile {
    path: String,
    files: Rc<RefCell<HashMap<String, Vec<u8>>>>,
}

// SAFETY: WASM is single-threaded
unsafe impl Send for MemoryFile {}
unsafe impl Sync for MemoryFile {}

impl StorageFile for MemoryFile {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize, StorageError> {
        let files = self.files.borrow();
        let data = files
            .get(&self.path)
            .ok_or_else(|| StorageError::IoError("File not found".to_string()))?;

        let offset = offset as usize;
        let end = std::cmp::min(offset + buf.len(), data.len());

        if offset >= data.len() {
            return Ok(0);
        }

        let bytes_to_read = end - offset;
        buf[..bytes_to_read].copy_from_slice(&data[offset..end]);
        Ok(bytes_to_read)
    }

    fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<usize, StorageError> {
        let mut files = self.files.borrow_mut();
        let data = files
            .get_mut(&self.path)
            .ok_or_else(|| StorageError::IoError("File not found".to_string()))?;

        let offset = offset as usize;
        let end = offset + buf.len();

        // Expand file if necessary
        if end > data.len() {
            data.resize(end, 0);
        }

        data[offset..end].copy_from_slice(buf);
        Ok(buf.len())
    }

    fn sync_all(&mut self) -> Result<(), StorageError> {
        // No-op for in-memory storage
        Ok(())
    }

    fn sync_data(&mut self) -> Result<(), StorageError> {
        // No-op for in-memory storage
        Ok(())
    }

    fn size(&self) -> Result<u64, StorageError> {
        let files = self.files.borrow();
        files
            .get(&self.path)
            .map(|data| data.len() as u64)
            .ok_or_else(|| StorageError::IoError("File not found".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require running in a browser environment with wasm-pack test
    // They won't run in a standard Rust test environment

    #[cfg(target_arch = "wasm32")]
    mod wasm_tests {
        use super::*;
        use wasm_bindgen_test::*;

        wasm_bindgen_test_configure!(run_in_browser);

        #[wasm_bindgen_test]
        async fn test_opfs_initialization() {
            let storage = OpfsStorage::new();
            assert!(storage.is_ok(), "OPFS should be supported in this browser");
        }

        #[wasm_bindgen_test]
        async fn test_opfs_create_and_write() {
            let storage = OpfsStorage::new().expect("Failed to initialize OPFS");

            let mut file = storage.create_file("test.db").expect("Failed to create file");
            let data = b"Hello OPFS!";

            let written = file.write_at(0, data).expect("Failed to write");
            assert_eq!(written, data.len());

            let mut buf = vec![0u8; data.len()];
            let read = file.read_at(0, &mut buf).expect("Failed to read");

            assert_eq!(read, data.len());
            assert_eq!(&buf, data);
        }

        #[wasm_bindgen_test]
        async fn test_opfs_file_persistence() {
            let storage = OpfsStorage::new().expect("Failed to initialize OPFS");

            // Write data
            {
                let mut file = storage.create_file("persistent.db").expect("Failed to create");
                file.write_at(0, b"persistent").expect("Failed to write");
            }

            // Read it back
            {
                let mut file = storage.open_file("persistent.db").expect("Failed to open");
                let mut buf = vec![0u8; 10];
                file.read_at(0, &mut buf).expect("Failed to read");
                assert_eq!(&buf, b"persistent");
            }

            // Clean up
            storage.delete_file("persistent.db").expect("Failed to delete");
        }

        #[wasm_bindgen_test]
        async fn test_opfs_file_operations() {
            let storage = OpfsStorage::new().expect("Failed to initialize OPFS");

            // File shouldn't exist initially
            assert!(!storage.file_exists("operations.db"));

            // Create file
            let mut file = storage.create_file("operations.db").expect("Failed to create");
            file.write_at(0, b"test").expect("Failed to write");
            drop(file);

            // Now it should exist
            assert!(storage.file_exists("operations.db"));

            // Check size
            let size = storage.file_size("operations.db").expect("Failed to get size");
            assert_eq!(size, 4);

            // Delete file
            storage.delete_file("operations.db").expect("Failed to delete");
            assert!(!storage.file_exists("operations.db"));
        }

        #[wasm_bindgen_test]
        async fn test_read_write_at_different_offsets() {
            let storage = OpfsStorage::new().expect("Failed to initialize OPFS");

            let mut file = storage.create_file("offsets.db").expect("Failed to create");

            // Write at offset 0
            file.write_at(0, b"AAAA").expect("Failed to write at 0");

            // Write at offset 100
            file.write_at(100, b"BBBB").expect("Failed to write at 100");

            // Read at offset 0
            let mut buf = vec![0u8; 4];
            file.read_at(0, &mut buf).expect("Failed to read at 0");
            assert_eq!(&buf, b"AAAA");

            // Read at offset 100
            file.read_at(100, &mut buf).expect("Failed to read at 100");
            assert_eq!(&buf, b"BBBB");

            // Clean up
            drop(file);
            storage.delete_file("offsets.db").expect("Failed to delete");
        }
    }
}
