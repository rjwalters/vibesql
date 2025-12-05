//! Buffer Pool - LRU Cache for Page Management
//!
//! This module provides an LRU (Least Recently Used) buffer pool that caches
//! hot pages in memory, improving disk-backed B+ tree performance.

use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

#[cfg(not(target_arch = "wasm32"))]
use parking_lot::Mutex;

#[cfg(target_arch = "wasm32")]
use std::sync::Mutex;

use lru::LruCache;

use crate::{
    page::{Page, PageId, PageManager},
    StorageError,
};

/// Helper macro to handle platform-specific mutex locking
/// - parking_lot::Mutex::lock() returns guard directly
/// - std::sync::Mutex::lock() returns Result<Guard, PoisonError>
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

/// Statistics for buffer pool performance monitoring
#[derive(Debug, Default)]
pub struct BufferPoolStats {
    /// Number of cache hits (page found in cache)
    hits: AtomicU64,
    /// Number of cache misses (page loaded from disk)
    misses: AtomicU64,
    /// Number of page evictions
    evictions: AtomicU64,
}

impl BufferPoolStats {
    /// Create new statistics tracker
    pub fn new() -> Self {
        Self::default()
    }

    /// Get number of cache hits
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Get number of cache misses
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Get number of evictions
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Calculate hit rate as a percentage (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits();
        let misses = self.misses();
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Record a cache hit
    fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss
    fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an eviction
    fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }
}

/// LRU buffer pool for caching pages in memory
#[derive(Debug)]
pub struct BufferPool {
    /// LRU cache mapping page IDs to pages
    cache: Arc<Mutex<LruCache<PageId, Page>>>,
    /// Page manager for disk I/O
    page_manager: Arc<PageManager>,
    /// Maximum number of pages to cache
    capacity: usize,
    /// Performance statistics
    stats: BufferPoolStats,
}

impl BufferPool {
    /// Create a new buffer pool with the specified capacity
    ///
    /// # Arguments
    /// * `page_manager` - Page manager for disk I/O
    /// * `capacity` - Maximum number of pages to cache (default: 1000 = ~4MB)
    pub fn new(page_manager: Arc<PageManager>, capacity: usize) -> Self {
        let capacity = if capacity == 0 { 1000 } else { capacity };
        let cache = Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(capacity).unwrap())));

        BufferPool { cache, page_manager, capacity, stats: BufferPoolStats::new() }
    }

    /// Get a page from the cache or load from disk
    ///
    /// # Arguments
    /// * `page_id` - ID of the page to retrieve
    ///
    /// # Returns
    /// The requested page, either from cache or loaded from disk
    pub fn get_page(&self, page_id: PageId) -> Result<Page, StorageError> {
        // Try to get from cache first
        {
            let mut cache = lock!(self.cache);

            if let Some(page) = cache.get(&page_id) {
                self.stats.record_hit();
                return Ok(page.clone());
            }
        }

        // Cache miss - load from disk
        self.stats.record_miss();
        let page = self.page_manager.read_page(page_id)?;

        // Insert into cache
        self.put_page_internal(page.clone())?;

        Ok(page)
    }

    /// Put a page into the cache
    ///
    /// # Arguments
    /// * `page` - The page to cache
    pub fn put_page(&self, page: Page) -> Result<(), StorageError> {
        self.put_page_internal(page)
    }

    /// Internal method to put a page into the cache
    fn put_page_internal(&self, page: Page) -> Result<(), StorageError> {
        let mut cache = lock!(self.cache);

        // If cache is at capacity, LRU will evict automatically
        if let Some((_evicted_id, mut evicted_page)) = cache.push(page.id, page) {
            // If evicted page is dirty, write to disk
            if evicted_page.dirty {
                drop(cache); // Release lock before disk I/O
                self.page_manager.write_page(&mut evicted_page)?;
            }
            self.stats.record_eviction();
        }

        Ok(())
    }

    /// Flush all dirty pages to disk
    pub fn flush_dirty(&self) -> Result<(), StorageError> {
        let cache = lock!(self.cache);

        // Collect dirty pages (we need to release the lock before writing)
        let dirty_pages: Vec<(PageId, Page)> = cache
            .iter()
            .filter(|(_, page)| page.dirty)
            .map(|(id, page)| (*id, page.clone()))
            .collect();

        drop(cache); // Release lock before disk I/O

        // Write all dirty pages
        for (_id, mut page) in dirty_pages {
            self.page_manager.write_page(&mut page)?;

            // Update the page in cache to mark it clean
            let mut cache = lock!(self.cache);
            if let Some(cached_page) = cache.get_mut(&page.id) {
                cached_page.mark_clean();
            }
        }

        Ok(())
    }

    /// Manually evict a page from the cache
    ///
    /// # Arguments
    /// * `page_id` - ID of the page to evict
    pub fn evict(&self, page_id: PageId) -> Result<(), StorageError> {
        let mut cache = lock!(self.cache);

        if let Some(mut page) = cache.pop(&page_id) {
            // If page is dirty, write to disk
            if page.dirty {
                drop(cache); // Release lock before disk I/O
                self.page_manager.write_page(&mut page)?;
            }
            self.stats.record_eviction();
        }

        Ok(())
    }

    /// Get the current capacity of the buffer pool
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the current number of pages in the cache
    pub fn size(&self) -> Result<usize, StorageError> {
        let cache = lock!(self.cache);
        Ok(cache.len())
    }

    /// Get statistics for the buffer pool
    pub fn stats(&self) -> &BufferPoolStats {
        &self.stats
    }
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::NativeStorage;

    #[test]
    fn test_buffer_pool_creation() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(NativeStorage::new(temp_dir.path()).unwrap());
        let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());
        let buffer_pool = BufferPool::new(page_manager, 10);

        assert_eq!(buffer_pool.capacity(), 10);
        assert_eq!(buffer_pool.size().unwrap(), 0);
    }

    #[test]
    fn test_cache_hit() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(NativeStorage::new(temp_dir.path()).unwrap());
        let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());
        let buffer_pool = BufferPool::new(page_manager.clone(), 10);

        // Create and write a page to disk
        let mut page = Page::new(1);
        page.data[0] = 42;
        page.mark_dirty();
        page_manager.write_page(&mut page).unwrap();

        // First access - cache miss
        let page1 = buffer_pool.get_page(1).unwrap();
        assert_eq!(page1.data[0], 42);
        assert_eq!(buffer_pool.stats().misses(), 1);
        assert_eq!(buffer_pool.stats().hits(), 0);

        // Second access - cache hit
        let page2 = buffer_pool.get_page(1).unwrap();
        assert_eq!(page2.data[0], 42);
        assert_eq!(buffer_pool.stats().misses(), 1);
        assert_eq!(buffer_pool.stats().hits(), 1);
    }

    #[test]
    fn test_cache_miss() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(NativeStorage::new(temp_dir.path()).unwrap());
        let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());
        let buffer_pool = BufferPool::new(page_manager.clone(), 10);

        // Access a page that doesn't exist yet
        let page = buffer_pool.get_page(1).unwrap();
        assert_eq!(page.id, 1);
        assert_eq!(buffer_pool.stats().misses(), 1);
        assert_eq!(buffer_pool.stats().hits(), 0);
    }

    #[test]
    fn test_eviction() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(NativeStorage::new(temp_dir.path()).unwrap());
        let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());
        let buffer_pool = BufferPool::new(page_manager.clone(), 3);

        // Fill cache to capacity
        for i in 1..=3 {
            let mut page = Page::new(i);
            page.data[0] = i as u8;
            buffer_pool.put_page(page).unwrap();
        }

        assert_eq!(buffer_pool.size().unwrap(), 3);
        assert_eq!(buffer_pool.stats().evictions(), 0);

        // Add one more page - should trigger eviction
        let page4 = Page::new(4);
        buffer_pool.put_page(page4).unwrap();

        assert_eq!(buffer_pool.size().unwrap(), 3);
        assert_eq!(buffer_pool.stats().evictions(), 1);
    }

    #[test]
    fn test_dirty_page_write_on_eviction() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(NativeStorage::new(temp_dir.path()).unwrap());
        let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());
        let buffer_pool = BufferPool::new(page_manager.clone(), 2);

        // Add a dirty page
        let mut page1 = Page::new(1);
        page1.data[0] = 42;
        page1.mark_dirty();
        buffer_pool.put_page(page1).unwrap();

        // Add another page
        let page2 = Page::new(2);
        buffer_pool.put_page(page2).unwrap();

        // Add a third page - should evict page1 and write it to disk
        let page3 = Page::new(3);
        buffer_pool.put_page(page3).unwrap();

        assert_eq!(buffer_pool.stats().evictions(), 1);

        // Read page1 from disk to verify it was written
        let read_page = page_manager.read_page(1).unwrap();
        assert_eq!(read_page.data[0], 42);
    }

    #[test]
    fn test_flush_dirty_pages() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(NativeStorage::new(temp_dir.path()).unwrap());
        let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());
        let buffer_pool = BufferPool::new(page_manager.clone(), 10);

        // Add multiple dirty pages
        for i in 1..=5 {
            let mut page = Page::new(i);
            page.data[0] = i as u8;
            page.mark_dirty();
            buffer_pool.put_page(page).unwrap();
        }

        // Flush all dirty pages
        buffer_pool.flush_dirty().unwrap();

        // Verify all pages were written to disk
        for i in 1..=5 {
            let page = page_manager.read_page(i).unwrap();
            assert_eq!(page.data[0], i as u8);
        }
    }

    #[test]
    fn test_cache_statistics() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(NativeStorage::new(temp_dir.path()).unwrap());
        let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());
        let buffer_pool = BufferPool::new(page_manager.clone(), 10);

        // Mix of hits and misses
        buffer_pool.get_page(1).unwrap(); // miss
        buffer_pool.get_page(1).unwrap(); // hit
        buffer_pool.get_page(2).unwrap(); // miss
        buffer_pool.get_page(1).unwrap(); // hit
        buffer_pool.get_page(2).unwrap(); // hit

        assert_eq!(buffer_pool.stats().hits(), 3);
        assert_eq!(buffer_pool.stats().misses(), 2);
        assert_eq!(buffer_pool.stats().hit_rate(), 0.6);
    }

    #[test]
    fn test_manual_eviction() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(NativeStorage::new(temp_dir.path()).unwrap());
        let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());
        let buffer_pool = BufferPool::new(page_manager.clone(), 10);

        // Add a page
        let mut page = Page::new(1);
        page.data[0] = 42;
        page.mark_dirty();
        buffer_pool.put_page(page).unwrap();

        assert_eq!(buffer_pool.size().unwrap(), 1);

        // Manually evict the page
        buffer_pool.evict(1).unwrap();

        assert_eq!(buffer_pool.size().unwrap(), 0);
        assert_eq!(buffer_pool.stats().evictions(), 1);

        // Verify page was written to disk
        let read_page = page_manager.read_page(1).unwrap();
        assert_eq!(read_page.data[0], 42);
    }

    #[test]
    fn test_zero_capacity_defaults_to_1000() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(NativeStorage::new(temp_dir.path()).unwrap());
        let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());
        let buffer_pool = BufferPool::new(page_manager, 0);

        assert_eq!(buffer_pool.capacity(), 1000);
    }
}
