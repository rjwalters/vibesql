//! Page Management Infrastructure for Disk-Backed Storage
//!
//! This module provides page-based storage management for disk-backed indexes.
//! Pages are fixed-size blocks (4KB) that form the foundation of persistent storage.

use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use parking_lot::Mutex;

#[cfg(target_arch = "wasm32")]
use std::sync::Mutex;

use crate::{StorageBackend, StorageError, StorageFile};

/// Helper macro to lock a mutex, handling both parking_lot and std::sync variants
macro_rules! lock {
    ($mutex:expr) => {{
        #[cfg(not(target_arch = "wasm32"))]
        {
            $mutex.lock()
        }
        #[cfg(target_arch = "wasm32")]
        {
            $mutex.lock().unwrap()
        }
    }};
}

/// Page size in bytes (4KB standard)
pub const PAGE_SIZE: usize = 4096;

/// Page identifier type
pub type PageId = u64;

/// Reserved page ID for null references
pub const NULL_PAGE_ID: PageId = 0;

/// A single page of data
#[derive(Debug, Clone)]
pub struct Page {
    /// Unique page identifier
    pub id: PageId,
    /// Page data (fixed size)
    pub data: Vec<u8>,
    /// Whether the page has been modified since last write
    pub dirty: bool,
}

impl Page {
    /// Create a new empty page
    pub fn new(id: PageId) -> Self {
        Page { id, data: vec![0; PAGE_SIZE], dirty: false }
    }

    /// Create a page from existing data
    pub fn from_data(id: PageId, data: Vec<u8>) -> Result<Self, StorageError> {
        if data.len() != PAGE_SIZE {
            return Err(StorageError::InvalidPageSize { expected: PAGE_SIZE, actual: data.len() });
        }
        Ok(Page { id, data, dirty: false })
    }

    /// Mark page as dirty (modified)
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Mark page as clean (written to disk)
    pub fn mark_clean(&mut self) {
        self.dirty = false;
    }
}

/// Mutable state for page manager
#[derive(Debug)]
struct PageManagerState {
    /// List of free page IDs that can be reused
    free_pages: Vec<PageId>,
    /// Next page ID to allocate (if no free pages available)
    next_page_id: PageId,
}

/// Page manager handles allocation, deallocation, and I/O of pages
#[derive(Clone)]
pub struct PageManager {
    /// File handle for page storage
    file: Arc<Mutex<Box<dyn StorageFile>>>,
    /// Mutable state (allocation tracking)
    state: Arc<Mutex<PageManagerState>>,
}

impl std::fmt::Debug for PageManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = lock!(self.state);
        f.debug_struct("PageManager")
            .field(
                "state",
                &format!(
                    "next_page_id: {}, free_pages: {}",
                    state.next_page_id,
                    state.free_pages.len()
                ),
            )
            .finish()
    }
}

impl PageManager {
    /// Create a new page manager with a storage backend
    ///
    /// # Arguments
    /// * `path` - Filename for the page storage
    /// * `storage` - Storage backend to use for file operations
    pub fn new(path: &str, storage: Arc<dyn StorageBackend>) -> Result<Self, StorageError> {
        let file = storage.open_file(path)?;

        // Read metadata from first page (page 0 is reserved for metadata)
        let manager = PageManager {
            file: Arc::new(Mutex::new(file)),
            state: Arc::new(Mutex::new(PageManagerState {
                free_pages: Vec::new(),
                next_page_id: 1, // Page 0 is reserved
            })),
        };

        // Try to read metadata if file already exists and has content
        if let Ok(metadata_page) = manager.read_page(0) {
            if metadata_page.data.iter().any(|&b| b != 0) {
                manager.load_metadata(&metadata_page)?;
            }
        }

        Ok(manager)
    }

    /// Allocate a new page
    pub fn allocate_page(&self) -> Result<PageId, StorageError> {
        let mut state = lock!(self.state);

        // Reuse a free page if available
        if let Some(page_id) = state.free_pages.pop() {
            return Ok(page_id);
        }

        // Otherwise, allocate a new page ID
        let page_id = state.next_page_id;
        state.next_page_id += 1;
        Ok(page_id)
    }

    /// Deallocate a page, making it available for reuse
    pub fn deallocate_page(&self, page_id: PageId) -> Result<(), StorageError> {
        if page_id == 0 {
            return Err(StorageError::InvalidPageId(page_id));
        }

        let mut state = lock!(self.state);

        state.free_pages.push(page_id);
        Ok(())
    }

    /// Read a page from disk
    pub fn read_page(&self, page_id: PageId) -> Result<Page, StorageError> {
        let mut file = lock!(self.file);

        let offset = page_id * (PAGE_SIZE as u64);
        let mut data = vec![0u8; PAGE_SIZE];

        match file.read_at(offset, &mut data) {
            Ok(bytes_read) if bytes_read == PAGE_SIZE => Ok(Page::from_data(page_id, data)?),
            Ok(_) => {
                // Partial read or EOF - return empty page
                Ok(Page::new(page_id))
            }
            Err(_) => {
                // File doesn't have this page yet, return empty page
                Ok(Page::new(page_id))
            }
        }
    }

    /// Write a page to disk with immediate sync
    ///
    /// This ensures durability by syncing data to disk after each write.
    /// For bulk operations, consider using `write_page_no_sync` followed
    /// by a single `sync` call for better performance.
    pub fn write_page(&self, page: &mut Page) -> Result<(), StorageError> {
        let mut file = lock!(self.file);

        let offset = page.id * (PAGE_SIZE as u64);
        file.write_at(offset, &page.data)?;
        file.sync_data()?;

        page.mark_clean();
        Ok(())
    }

    /// Write a page to disk without syncing
    ///
    /// This is faster than `write_page` but does not guarantee durability
    /// until `sync` is called. Use this for bulk operations where you want
    /// to batch many writes before a single sync.
    ///
    /// # Safety
    /// Data may be lost on crash until `sync` is called.
    pub fn write_page_no_sync(&self, page: &mut Page) -> Result<(), StorageError> {
        let mut file = lock!(self.file);

        let offset = page.id * (PAGE_SIZE as u64);
        file.write_at(offset, &page.data)?;

        page.mark_clean();
        Ok(())
    }

    /// Sync all pending writes to disk
    ///
    /// This ensures all data written via `write_page_no_sync` is persisted.
    /// Call this after a batch of `write_page_no_sync` operations to ensure
    /// durability.
    pub fn sync(&self) -> Result<(), StorageError> {
        let mut file = lock!(self.file);
        file.sync_data()?;
        Ok(())
    }

    /// Save metadata to page 0
    pub fn save_metadata(&self) -> Result<(), StorageError> {
        let state = lock!(self.state);

        let mut metadata_page = Page::new(0);

        // Serialize metadata into page 0
        // Format: [next_page_id: 8 bytes][free_page_count: 8 bytes][free_pages: N*8 bytes]
        let mut offset = 0;

        // Write next_page_id
        metadata_page.data[offset..offset + 8].copy_from_slice(&state.next_page_id.to_le_bytes());
        offset += 8;

        // Write free page count
        let free_count = state.free_pages.len() as u64;
        metadata_page.data[offset..offset + 8].copy_from_slice(&free_count.to_le_bytes());
        offset += 8;

        // Write free pages (up to limit based on page size)
        let max_free_pages = (PAGE_SIZE - 16) / 8;
        let free_pages_to_write = state.free_pages.len().min(max_free_pages);
        for i in 0..free_pages_to_write {
            metadata_page.data[offset..offset + 8]
                .copy_from_slice(&state.free_pages[i].to_le_bytes());
            offset += 8;
        }

        metadata_page.mark_dirty();
        self.write_page(&mut metadata_page)?;
        Ok(())
    }

    /// Load metadata from page 0
    fn load_metadata(&self, metadata_page: &Page) -> Result<(), StorageError> {
        let mut state = lock!(self.state);

        let mut offset = 0;

        // Read next_page_id
        let next_page_id_bytes: [u8; 8] =
            metadata_page.data[offset..offset + 8].try_into().unwrap();
        state.next_page_id = u64::from_le_bytes(next_page_id_bytes);
        offset += 8;

        // Read free page count
        let free_count_bytes: [u8; 8] = metadata_page.data[offset..offset + 8].try_into().unwrap();
        let free_count = u64::from_le_bytes(free_count_bytes) as usize;
        offset += 8;

        // Read free pages
        state.free_pages.clear();
        for _ in 0..free_count {
            if offset + 8 <= PAGE_SIZE {
                let page_id_bytes: [u8; 8] =
                    metadata_page.data[offset..offset + 8].try_into().unwrap();
                let page_id = u64::from_le_bytes(page_id_bytes);
                state.free_pages.push(page_id);
                offset += 8;
            }
        }

        Ok(())
    }

    /// Flush all pending writes and metadata
    pub fn flush(&self) -> Result<(), StorageError> {
        self.save_metadata()?;
        let mut file = lock!(self.file);
        file.sync_all()?;
        Ok(())
    }

    /// Get the next page ID that would be allocated
    pub fn next_page_id(&self) -> PageId {
        lock!(self.state).next_page_id
    }

    /// Get the number of free pages
    pub fn free_page_count(&self) -> usize {
        lock!(self.state).free_pages.len()
    }
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use std::sync::Arc;
    use tempfile::TempDir;

    use super::*;
    use crate::NativeStorage;

    #[test]
    fn test_page_creation() {
        let page = Page::new(1);
        assert_eq!(page.id, 1);
        assert_eq!(page.data.len(), PAGE_SIZE);
        assert!(!page.dirty);
    }

    #[test]
    fn test_page_manager_allocation() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(NativeStorage::new(temp_dir.path()).unwrap());
        let manager = PageManager::new("test.db", storage).unwrap();

        let page1 = manager.allocate_page().unwrap();
        let page2 = manager.allocate_page().unwrap();
        let page3 = manager.allocate_page().unwrap();

        assert_eq!(page1, 1);
        assert_eq!(page2, 2);
        assert_eq!(page3, 3);
    }

    #[test]
    fn test_page_manager_deallocation_reuse() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(NativeStorage::new(temp_dir.path()).unwrap());
        let manager = PageManager::new("test.db", storage).unwrap();

        let page1 = manager.allocate_page().unwrap();
        let _page2 = manager.allocate_page().unwrap();

        // Deallocate page1
        manager.deallocate_page(page1).unwrap();

        // Next allocation should reuse page1
        let page3 = manager.allocate_page().unwrap();
        assert_eq!(page3, page1);
    }

    #[test]
    fn test_page_read_write() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(NativeStorage::new(temp_dir.path()).unwrap());
        let manager = PageManager::new("test.db", storage).unwrap();

        let mut page = Page::new(1);
        page.data[0] = 42;
        page.data[100] = 99;
        page.mark_dirty();

        manager.write_page(&mut page).unwrap();

        let read_page = manager.read_page(1).unwrap();
        assert_eq!(read_page.data[0], 42);
        assert_eq!(read_page.data[100], 99);
    }

    #[test]
    fn test_metadata_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(NativeStorage::new(temp_dir.path()).unwrap());

        // Create manager, allocate pages, save metadata
        {
            let manager = PageManager::new("test.db", storage.clone()).unwrap();
            manager.allocate_page().unwrap(); // 1
            manager.allocate_page().unwrap(); // 2
            manager.allocate_page().unwrap(); // 3
            manager.deallocate_page(2).unwrap();
            manager.save_metadata().unwrap();
        }

        // Reopen and verify state was preserved
        {
            let manager = PageManager::new("test.db", storage).unwrap();
            assert_eq!(manager.next_page_id(), 4);
            assert_eq!(manager.free_page_count(), 1);

            // Next allocation should reuse page 2
            let page = manager.allocate_page().unwrap();
            assert_eq!(page, 2);
        }
    }

    #[test]
    fn test_cannot_deallocate_reserved_page() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(NativeStorage::new(temp_dir.path()).unwrap());
        let manager = PageManager::new("test.db", storage).unwrap();

        let result = manager.deallocate_page(0);
        assert!(result.is_err());
    }
}
