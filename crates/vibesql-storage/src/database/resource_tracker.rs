// ============================================================================
// Resource Tracker - Memory and disk budget tracking for adaptive index management
// ============================================================================

use instant::Instant;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[cfg(not(target_arch = "wasm32"))]
use parking_lot::RwLock;

#[cfg(target_arch = "wasm32")]
use std::sync::RwLock;

// Helper macros to abstract over parking_lot vs std::sync RwLock differences
macro_rules! read_lock {
    ($rwlock:expr) => {{
        #[cfg(not(target_arch = "wasm32"))]
        {
            $rwlock.read()
        }
        #[cfg(target_arch = "wasm32")]
        {
            $rwlock.read().unwrap()
        }
    }};
}

macro_rules! write_lock {
    ($rwlock:expr) => {{
        #[cfg(not(target_arch = "wasm32"))]
        {
            $rwlock.write()
        }
        #[cfg(target_arch = "wasm32")]
        {
            $rwlock.write().unwrap()
        }
    }};
}

/// Which backend an index is currently using
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexBackend {
    InMemory,
    DiskBacked,
}

/// Statistics for a single index
#[derive(Debug)]
pub struct IndexStats {
    /// Memory used by this index (bytes)
    pub memory_bytes: usize,

    /// Disk space used by this index (bytes)
    pub disk_bytes: usize,

    /// Number of times this index has been accessed
    pub access_count: AtomicU64,

    /// Last time this index was accessed
    pub last_access: Instant,

    /// Current backend for this index
    pub backend: IndexBackend,
}

impl Clone for IndexStats {
    fn clone(&self) -> Self {
        IndexStats {
            memory_bytes: self.memory_bytes,
            disk_bytes: self.disk_bytes,
            access_count: AtomicU64::new(self.access_count.load(Ordering::Relaxed)),
            last_access: self.last_access,
            backend: self.backend,
        }
    }
}

impl IndexStats {
    /// Create new index stats
    pub fn new(memory_bytes: usize, disk_bytes: usize, backend: IndexBackend) -> Self {
        IndexStats {
            memory_bytes,
            disk_bytes,
            access_count: AtomicU64::new(0),
            last_access: Instant::now(),
            backend,
        }
    }

    /// Record an access to this index
    pub fn record_access(&mut self) {
        self.access_count.fetch_add(1, Ordering::Relaxed);
        self.last_access = Instant::now();
    }

    /// Get the access count
    pub fn get_access_count(&self) -> u64 {
        self.access_count.load(Ordering::Relaxed)
    }
}

/// Tracks resource usage across all indexes for budget enforcement
#[derive(Debug)]
pub struct ResourceTracker {
    /// Total memory used by all indexes (bytes)
    memory_used: AtomicUsize,

    /// Total disk space used by all indexes (bytes)
    disk_used: AtomicUsize,

    /// Per-index statistics for LRU tracking (using RwLock for thread-safe interior mutability)
    index_stats: RwLock<HashMap<String, IndexStats>>,
}

impl Clone for ResourceTracker {
    fn clone(&self) -> Self {
        ResourceTracker {
            memory_used: AtomicUsize::new(self.memory_used.load(Ordering::Relaxed)),
            disk_used: AtomicUsize::new(self.disk_used.load(Ordering::Relaxed)),
            index_stats: RwLock::new(read_lock!(self.index_stats).clone()),
        }
    }
}

impl ResourceTracker {
    /// Create a new empty resource tracker
    pub fn new() -> Self {
        ResourceTracker {
            memory_used: AtomicUsize::new(0),
            disk_used: AtomicUsize::new(0),
            index_stats: RwLock::new(HashMap::new()),
        }
    }

    /// Get total memory used across all indexes
    pub fn memory_used(&self) -> usize {
        self.memory_used.load(Ordering::Relaxed)
    }

    /// Get total disk space used across all indexes
    pub fn disk_used(&self) -> usize {
        self.disk_used.load(Ordering::Relaxed)
    }

    /// Register a new index
    pub fn register_index(
        &mut self,
        index_name: String,
        memory_bytes: usize,
        disk_bytes: usize,
        backend: IndexBackend,
    ) {
        // Normalize index name to uppercase (consistent with SQL identifier normalization)
        let normalized = index_name.to_uppercase();

        // Update totals
        self.memory_used.fetch_add(memory_bytes, Ordering::Relaxed);
        self.disk_used.fetch_add(disk_bytes, Ordering::Relaxed);

        // Create stats entry
        let stats = IndexStats::new(memory_bytes, disk_bytes, backend);
        write_lock!(self.index_stats).insert(normalized, stats);
    }

    /// Remove an index from tracking
    pub fn unregister_index(&mut self, index_name: &str) {
        // Normalize index name to uppercase for lookup
        let normalized = index_name.to_uppercase();
        if let Some(stats) = write_lock!(self.index_stats).remove(&normalized) {
            // Subtract from totals
            self.memory_used.fetch_sub(stats.memory_bytes, Ordering::Relaxed);
            self.disk_used.fetch_sub(stats.disk_bytes, Ordering::Relaxed);
        }
    }

    /// Record an access to an index
    /// Uses interior mutability to allow recording from immutable references
    pub fn record_access(&self, index_name: &str) {
        // Normalize index name to uppercase for lookup
        let normalized = index_name.to_uppercase();
        if let Some(stats) = write_lock!(self.index_stats).get_mut(&normalized) {
            stats.record_access();
        }
    }

    /// Mark an index as spilled from memory to disk
    pub fn mark_spilled(&mut self, index_name: &str, new_disk_bytes: usize) {
        // Normalize index name to uppercase for lookup
        let normalized = index_name.to_uppercase();
        if let Some(stats) = write_lock!(self.index_stats).get_mut(&normalized) {
            // Subtract memory usage
            self.memory_used.fetch_sub(stats.memory_bytes, Ordering::Relaxed);

            // Add disk usage (might be different size due to compression, overhead, etc.)
            let disk_delta = new_disk_bytes.saturating_sub(stats.disk_bytes);
            self.disk_used.fetch_add(disk_delta, Ordering::Relaxed);

            // Update stats
            stats.memory_bytes = 0;
            stats.disk_bytes = new_disk_bytes;
            stats.backend = IndexBackend::DiskBacked;
        }
    }

    /// Get stats for a specific index
    pub fn get_index_stats(&self, index_name: &str) -> Option<IndexStats> {
        // Normalize index name to uppercase for lookup (consistent with create_index)
        let normalized = index_name.to_uppercase();
        read_lock!(self.index_stats).get(&normalized).cloned()
    }

    /// Find the coldest (least recently used) in-memory index
    /// Returns the index name and its last access time
    pub fn find_coldest_in_memory_index(&self) -> Option<(String, Instant)> {
        read_lock!(self.index_stats)
            .iter()
            .filter(|(_, stats)| stats.backend == IndexBackend::InMemory)
            .min_by_key(|(_, stats)| stats.last_access)
            .map(|(name, stats)| (name.clone(), stats.last_access))
    }

    /// Get all in-memory indexes sorted by last access (oldest first)
    pub fn get_in_memory_indexes_by_lru(&self) -> Vec<String> {
        let mut indexes: Vec<_> = read_lock!(self.index_stats)
            .iter()
            .filter(|(_, stats)| stats.backend == IndexBackend::InMemory)
            .map(|(name, stats)| (name.clone(), stats.last_access))
            .collect();

        indexes.sort_by_key(|(_, last_access)| *last_access);
        indexes.into_iter().map(|(name, _)| name).collect()
    }

    /// Get the backend type for an index
    pub fn get_backend(&self, index_name: &str) -> Option<IndexBackend> {
        // Normalize index name to uppercase for lookup
        let normalized = index_name.to_uppercase();
        read_lock!(self.index_stats).get(&normalized).map(|stats| stats.backend)
    }
}

impl Default for ResourceTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use instant::Duration;
    use std::thread;

    #[test]
    fn test_resource_tracker_basic() {
        let mut tracker = ResourceTracker::new();

        // Initially empty
        assert_eq!(tracker.memory_used(), 0);
        assert_eq!(tracker.disk_used(), 0);

        // Register an in-memory index
        tracker.register_index(
            "idx1".to_string(),
            1000, // 1KB memory
            0,    // 0 disk
            IndexBackend::InMemory,
        );

        assert_eq!(tracker.memory_used(), 1000);
        assert_eq!(tracker.disk_used(), 0);

        // Register a disk-backed index
        tracker.register_index(
            "idx2".to_string(),
            0,    // 0 memory
            5000, // 5KB disk
            IndexBackend::DiskBacked,
        );

        assert_eq!(tracker.memory_used(), 1000);
        assert_eq!(tracker.disk_used(), 5000);
    }

    #[test]
    fn test_unregister_index() {
        let mut tracker = ResourceTracker::new();

        tracker.register_index("idx1".to_string(), 1000, 0, IndexBackend::InMemory);
        tracker.register_index("idx2".to_string(), 2000, 0, IndexBackend::InMemory);

        assert_eq!(tracker.memory_used(), 3000);

        tracker.unregister_index("idx1");
        assert_eq!(tracker.memory_used(), 2000);

        tracker.unregister_index("idx2");
        assert_eq!(tracker.memory_used(), 0);
    }

    #[test]
    fn test_access_tracking() {
        let mut tracker = ResourceTracker::new();

        tracker.register_index("idx1".to_string(), 1000, 0, IndexBackend::InMemory);

        // Record some accesses
        tracker.record_access("idx1");
        tracker.record_access("idx1");
        tracker.record_access("idx1");

        let stats = tracker.get_index_stats("idx1").unwrap();
        assert_eq!(stats.get_access_count(), 3);
    }

    #[test]
    fn test_lru_tracking() {
        let mut tracker = ResourceTracker::new();

        tracker.register_index("idx1".to_string(), 1000, 0, IndexBackend::InMemory);
        thread::sleep(Duration::from_millis(10));

        tracker.register_index("idx2".to_string(), 2000, 0, IndexBackend::InMemory);
        thread::sleep(Duration::from_millis(10));

        tracker.register_index("idx3".to_string(), 3000, 0, IndexBackend::InMemory);

        // idx1 should be coldest (created first)
        // Note: Index names are normalized to uppercase
        let (coldest, _) = tracker.find_coldest_in_memory_index().unwrap();
        assert_eq!(coldest, "IDX1");

        // Access idx1 to make it hot
        tracker.record_access("idx1");
        thread::sleep(Duration::from_millis(10));

        // Now idx2 should be coldest
        let (coldest, _) = tracker.find_coldest_in_memory_index().unwrap();
        assert_eq!(coldest, "IDX2");
    }

    #[test]
    fn test_spill_to_disk() {
        let mut tracker = ResourceTracker::new();

        tracker.register_index("idx1".to_string(), 1000, 0, IndexBackend::InMemory);

        assert_eq!(tracker.memory_used(), 1000);
        assert_eq!(tracker.disk_used(), 0);
        assert_eq!(tracker.get_backend("idx1"), Some(IndexBackend::InMemory));

        // Spill to disk
        tracker.mark_spilled("idx1", 800); // Might compress when writing to disk

        assert_eq!(tracker.memory_used(), 0);
        assert_eq!(tracker.disk_used(), 800);
        assert_eq!(tracker.get_backend("idx1"), Some(IndexBackend::DiskBacked));
    }

    #[test]
    fn test_get_in_memory_indexes_by_lru() {
        let mut tracker = ResourceTracker::new();

        tracker.register_index("idx1".to_string(), 1000, 0, IndexBackend::InMemory);
        thread::sleep(Duration::from_millis(10));
        tracker.register_index("idx2".to_string(), 2000, 0, IndexBackend::InMemory);
        thread::sleep(Duration::from_millis(10));
        tracker.register_index("idx3".to_string(), 0, 3000, IndexBackend::DiskBacked);
        thread::sleep(Duration::from_millis(10));
        tracker.register_index("idx4".to_string(), 4000, 0, IndexBackend::InMemory);

        let lru_order = tracker.get_in_memory_indexes_by_lru();

        // Should return only in-memory indexes in LRU order
        // Note: Index names are normalized to uppercase
        assert_eq!(lru_order, vec!["IDX1", "IDX2", "IDX4"]);
    }
}
