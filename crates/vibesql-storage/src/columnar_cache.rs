//! Columnar Cache - LRU Cache for Columnar Table Representations
//!
//! This module provides a database-level LRU cache for columnar table representations,
//! enabling automatic workload adaptation for analytical queries without manual configuration.
//!
//! ## Design Goals
//!
//! 1. **Arc-based sharing** - Avoid clone overhead by sharing cached data
//! 2. **Memory-bounded** - Configurable memory budget with LRU eviction
//! 3. **Database-level** - Global view enables smart eviction decisions
//! 4. **Statistics** - Monitor cache effectiveness for tuning
//!
//! ## Usage
//!
//! The cache is integrated at the `Database` level and used transparently by the executor
//! when performing analytical queries. Tables are automatically cached on first access
//! and evicted when the memory budget is exceeded.
//!
//! ## Thread Safety
//!
//! Uses `parking_lot::RwLock` on native platforms and `std::sync::RwLock` on WASM
//! for thread-safe access to the cache.

use std::sync::Arc;
#[cfg(target_arch = "wasm32")]
use std::sync::RwLock;

// Platform-specific synchronization primitives
#[cfg(not(target_arch = "wasm32"))]
use parking_lot::RwLock;

use crate::ColumnarTable;

/// Normalize table name for cache key (case-insensitive matching)
/// Uses uppercase to match SQLite/SQL standard behavior
#[inline]
fn normalize_cache_key(table_name: &str) -> String {
    table_name.to_uppercase()
}

/// Statistics for monitoring cache effectiveness
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Number of cache hits
    pub hits: u64,
    /// Number of cache misses
    pub misses: u64,
    /// Number of cache evictions
    pub evictions: u64,
    /// Number of conversions performed (subset of misses that resulted in caching)
    pub conversions: u64,
    /// Number of invalidations (due to table modifications)
    pub invalidations: u64,
}

impl CacheStats {
    /// Get the cache hit rate as a percentage (0.0 to 100.0)
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            (self.hits as f64 / total as f64) * 100.0
        }
    }
}

/// Entry in the columnar cache
struct CacheEntry {
    /// The cached columnar table data
    data: Arc<ColumnarTable>,
    /// Size in bytes (cached to avoid recomputation)
    size_bytes: usize,
    /// #6199 Phase 2: structural "hotness" of this table (higher = more
    /// analytically hot). Computed from the per-table access signal by
    /// `database::columnar_hotness` and refreshed on insert / access. Under
    /// memory pressure the *coldest* resident entry is evicted first, so hot
    /// analytical tables stay resident. `0` (the default supplied by the legacy
    /// two-argument [`ColumnarCache::insert`]) makes eviction fall back to pure
    /// LRU tie-breaking, preserving the historical behavior for callers that do
    /// not supply a hotness.
    hotness: u64,
}

/// LRU cache for columnar table representations
///
/// Provides memory-bounded caching of columnar table data with automatic
/// eviction when the memory budget is exceeded. Tables are stored as `Arc`
/// to enable zero-copy sharing between queries.
///
/// # Memory Management
///
/// The cache tracks memory usage based on `ColumnarTable::size_in_bytes()`.
/// When inserting a new entry would exceed the budget, the least recently
/// used entries are evicted until there's sufficient space.
///
/// # Example
///
/// ```text
/// use vibesql_storage::columnar_cache::ColumnarCache;
///
/// // Create a cache with 256MB budget
/// let cache = ColumnarCache::new(256 * 1024 * 1024);
///
/// // Get or create columnar representation
/// if let Some(columnar) = cache.get("lineitem") {
///     // Use cached data
/// } else {
///     // Convert and cache
///     let columnar = table.scan_columnar()?;
///     cache.insert("lineitem", columnar);
/// }
/// ```
pub struct ColumnarCache {
    /// LRU cache: table_name -> CacheEntry
    /// The lru crate handles ordering, we just need to track size
    cache: RwLock<lru::LruCache<String, CacheEntry>>,
    /// Memory budget in bytes
    max_memory: usize,
    /// Current memory usage in bytes
    current_memory: RwLock<usize>,
    /// Cache statistics
    stats: RwLock<CacheStats>,
}

/// Evict the coldest resident entries until `size_bytes` fits within
/// `max_memory`, or until only entries strictly hotter than `incoming_hotness`
/// remain (#6199 Phase 2, hotness-aware eviction).
///
/// Victim selection is by lowest `hotness`, tie-broken toward the LRU end:
/// `LruCache::iter` yields most- to least-recently-used, and the `<=`
/// comparison lets a later (more-LRU) entry win a hotness tie. A resident entry
/// strictly hotter than the incoming table's `incoming_hotness` is protected —
/// eviction stops rather than displace hotter analytical data for a colder
/// newcomer. The caller then admits the newcomer even if that leaves the cache
/// over budget; the next insert reclaims it (bounded to budget + one entry).
///
/// Operates on already-acquired lock targets so it is shared between the native
/// and wasm code paths.
fn evict_coldest_to_fit(
    cache: &mut lru::LruCache<String, CacheEntry>,
    current_memory: &mut usize,
    stats: &mut CacheStats,
    size_bytes: usize,
    max_memory: usize,
    incoming_hotness: u64,
) {
    while *current_memory + size_bytes > max_memory {
        // Find the coldest resident entry (tie-break toward LRU).
        let mut victim_key: Option<String> = None;
        let mut victim_hotness = u64::MAX;
        for (k, entry) in cache.iter() {
            if entry.hotness <= victim_hotness {
                victim_hotness = entry.hotness;
                victim_key = Some(k.clone());
            }
        }

        match victim_key {
            // Protect residents strictly hotter than the incoming table.
            Some(_) if victim_hotness > incoming_hotness => break,
            Some(key) => {
                if let Some(evicted) = cache.pop(&key) {
                    *current_memory = current_memory.saturating_sub(evicted.size_bytes);
                    stats.evictions += 1;
                } else {
                    break;
                }
            }
            // Cache empty — nothing left to evict.
            None => break,
        }
    }
}

impl ColumnarCache {
    /// Create a new columnar cache with the specified memory budget
    ///
    /// # Arguments
    /// * `max_memory` - Maximum memory budget in bytes
    ///
    /// # Example
    /// ```text
    /// // 256MB cache
    /// let cache = ColumnarCache::new(256 * 1024 * 1024);
    /// ```
    pub fn new(max_memory: usize) -> Self {
        // Use a reasonable capacity (1000 tables) - we manage eviction via memory budget
        // The LRU library can't handle usize::MAX due to internal hash table allocation
        let capacity = std::num::NonZeroUsize::new(1000).unwrap();
        ColumnarCache {
            cache: RwLock::new(lru::LruCache::new(capacity)),
            max_memory,
            current_memory: RwLock::new(0),
            stats: RwLock::new(CacheStats::default()),
        }
    }

    /// Get a cached columnar table representation
    ///
    /// Returns `Some(Arc<ColumnarTable>)` if the table is cached, `None` otherwise.
    /// Accessing a cached entry marks it as recently used.
    /// Table names are normalized for case-insensitive matching.
    pub fn get(&self, table_name: &str) -> Option<Arc<ColumnarTable>> {
        let key = normalize_cache_key(table_name);
        #[cfg(not(target_arch = "wasm32"))]
        {
            let mut cache = self.cache.write();
            let mut stats = self.stats.write();

            if let Some(entry) = cache.get(&key) {
                stats.hits += 1;
                Some(Arc::clone(&entry.data))
            } else {
                stats.misses += 1;
                None
            }
        }

        #[cfg(target_arch = "wasm32")]
        {
            let mut cache = self.cache.write().unwrap();
            let mut stats = self.stats.write().unwrap();

            if let Some(entry) = cache.get(&key) {
                stats.hits += 1;
                Some(Arc::clone(&entry.data))
            } else {
                stats.misses += 1;
                None
            }
        }
    }

    /// Insert or update a columnar table in the cache (hotness-agnostic).
    ///
    /// Equivalent to [`ColumnarCache::insert_with_hotness`] with a hotness of
    /// `0`, which makes eviction fall back to pure LRU tie-breaking. Retained
    /// for callers (and tests) that have no access-pattern signal to supply;
    /// the adaptive [`crate::database::Database::get_columnar`] path uses
    /// `insert_with_hotness` so that hot analytical tables are protected under
    /// memory pressure.
    ///
    /// # Returns
    /// The Arc-wrapped columnar table (for immediate use)
    pub fn insert(&self, table_name: &str, columnar: ColumnarTable) -> Arc<ColumnarTable> {
        self.insert_with_hotness(table_name, columnar, 0)
    }

    /// Insert or update a columnar table in the cache with a structural
    /// **hotness** (#6199 Phase 2).
    ///
    /// If the table is already cached, the existing entry is replaced. If
    /// inserting would exceed the memory budget, the *coldest* resident entries
    /// are evicted first (hotness-aware eviction, not pure LRU) until there is
    /// room — but a resident entry strictly hotter than `hotness` is never
    /// displaced by this colder newcomer. When only hotter residents remain the
    /// incoming entry is still admitted (possibly over budget); the over-budget
    /// condition self-corrects on the next insert (bounded to budget + one
    /// entry), matching the historical "always admit" behavior.
    ///
    /// Table names are normalized for case-insensitive matching.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `columnar` - The columnar table data to cache
    /// * `hotness` - Structural hotness from `database::columnar_hotness`
    ///
    /// # Returns
    /// The Arc-wrapped columnar table (for immediate use)
    pub fn insert_with_hotness(
        &self,
        table_name: &str,
        columnar: ColumnarTable,
        hotness: u64,
    ) -> Arc<ColumnarTable> {
        let data = Arc::new(columnar);

        // #6199 Phase 0: a zero budget disables the representation cache. Never
        // hold a resident copy — return the Arc for immediate use without
        // caching (no `put`, no eviction, no conversion accounting). Combined
        // with the `get_columnar` short-circuit in `database/cache.rs`, this
        // keeps a disabled cache truly inert: `CacheStats` stays all-zero and
        // `memory_usage()` stays 0.
        if self.max_memory == 0 {
            return data;
        }

        let key = normalize_cache_key(table_name);
        let size_bytes = data.size_in_bytes();

        #[cfg(not(target_arch = "wasm32"))]
        {
            let mut cache = self.cache.write();
            let mut current_memory = self.current_memory.write();
            let mut stats = self.stats.write();

            // Remove existing entry if present
            if let Some(old_entry) = cache.pop(&key) {
                *current_memory = current_memory.saturating_sub(old_entry.size_bytes);
            }

            evict_coldest_to_fit(
                &mut cache,
                &mut current_memory,
                &mut stats,
                size_bytes,
                self.max_memory,
                hotness,
            );

            // Insert new entry
            let entry = CacheEntry { data: Arc::clone(&data), size_bytes, hotness };
            cache.put(key, entry);
            *current_memory += size_bytes;
            stats.conversions += 1;
        }

        #[cfg(target_arch = "wasm32")]
        {
            let mut cache = self.cache.write().unwrap();
            let mut current_memory = self.current_memory.write().unwrap();
            let mut stats = self.stats.write().unwrap();

            // Remove existing entry if present
            if let Some(old_entry) = cache.pop(&key) {
                *current_memory = current_memory.saturating_sub(old_entry.size_bytes);
            }

            evict_coldest_to_fit(
                &mut cache,
                &mut current_memory,
                &mut stats,
                size_bytes,
                self.max_memory,
                hotness,
            );

            // Insert new entry
            let entry = CacheEntry { data: Arc::clone(&data), size_bytes, hotness };
            cache.put(key, entry);
            *current_memory += size_bytes;
            stats.conversions += 1;
        }

        data
    }

    /// Refresh the stored hotness of a cached table without disturbing its LRU
    /// recency or the memory accounting (#6199 Phase 2). No-op if the table is
    /// not resident. Called on cache hits so a table's eviction priority tracks
    /// its evolving access pattern rather than the hotness captured at insert.
    pub fn update_hotness(&self, table_name: &str, hotness: u64) {
        let key = normalize_cache_key(table_name);
        #[cfg(not(target_arch = "wasm32"))]
        {
            let mut cache = self.cache.write();
            if let Some(entry) = cache.peek_mut(&key) {
                entry.hotness = hotness;
            }
        }

        #[cfg(target_arch = "wasm32")]
        {
            let mut cache = self.cache.write().unwrap();
            if let Some(entry) = cache.peek_mut(&key) {
                entry.hotness = hotness;
            }
        }
    }

    /// Invalidate a cached table entry
    ///
    /// Called when a table is modified (INSERT/UPDATE/DELETE) to ensure
    /// the cache doesn't serve stale data.
    /// Table names are normalized for case-insensitive matching.
    pub fn invalidate(&self, table_name: &str) {
        let key = normalize_cache_key(table_name);
        #[cfg(not(target_arch = "wasm32"))]
        {
            let mut cache = self.cache.write();
            let mut current_memory = self.current_memory.write();
            let mut stats = self.stats.write();

            if let Some(entry) = cache.pop(&key) {
                *current_memory = current_memory.saturating_sub(entry.size_bytes);
                stats.invalidations += 1;
            }
        }

        #[cfg(target_arch = "wasm32")]
        {
            let mut cache = self.cache.write().unwrap();
            let mut current_memory = self.current_memory.write().unwrap();
            let mut stats = self.stats.write().unwrap();

            if let Some(entry) = cache.pop(&key) {
                *current_memory = current_memory.saturating_sub(entry.size_bytes);
                stats.invalidations += 1;
            }
        }
    }

    /// Clear all cached entries
    pub fn clear(&self) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let mut cache = self.cache.write();
            let mut current_memory = self.current_memory.write();
            cache.clear();
            *current_memory = 0;
        }

        #[cfg(target_arch = "wasm32")]
        {
            let mut cache = self.cache.write().unwrap();
            let mut current_memory = self.current_memory.write().unwrap();
            cache.clear();
            *current_memory = 0;
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.stats.read().clone()
        }

        #[cfg(target_arch = "wasm32")]
        {
            self.stats.read().unwrap().clone()
        }
    }

    /// Get current memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        #[cfg(not(target_arch = "wasm32"))]
        {
            *self.current_memory.read()
        }

        #[cfg(target_arch = "wasm32")]
        {
            *self.current_memory.read().unwrap()
        }
    }

    /// Get the memory budget in bytes
    pub fn max_memory(&self) -> usize {
        self.max_memory
    }

    /// Get the number of cached tables
    pub fn len(&self) -> usize {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.cache.read().len()
        }

        #[cfg(target_arch = "wasm32")]
        {
            self.cache.read().unwrap().len()
        }
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if a table is cached
    /// Table names are normalized for case-insensitive matching.
    pub fn contains(&self, table_name: &str) -> bool {
        let key = normalize_cache_key(table_name);
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.cache.read().contains(&key)
        }

        #[cfg(target_arch = "wasm32")]
        {
            self.cache.read().unwrap().contains(&key)
        }
    }
}

impl std::fmt::Debug for ColumnarCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnarCache")
            .field("max_memory", &self.max_memory)
            .field("current_memory", &self.memory_usage())
            .field("entries", &self.len())
            .field("stats", &self.stats())
            .finish()
    }
}

// Clone creates a new cache with same configuration but empty data
impl Clone for ColumnarCache {
    fn clone(&self) -> Self {
        ColumnarCache::new(self.max_memory)
    }
}

#[cfg(test)]
mod tests {
    use vibesql_types::SqlValue;

    use super::*;
    use crate::Row;

    fn create_test_columnar(rows: usize) -> ColumnarTable {
        let row_data: Vec<Row> = (0..rows)
            .map(|i| {
                Row::new(vec![
                    SqlValue::Integer(i as i64),
                    SqlValue::Varchar(arcstr::ArcStr::from(format!("name_{}", i))),
                ])
            })
            .collect();
        let column_names = vec!["id".to_string(), "name".to_string()];
        ColumnarTable::from_rows(&row_data, &column_names).unwrap()
    }

    #[test]
    fn test_cache_basic_operations() {
        let cache = ColumnarCache::new(1024 * 1024); // 1MB

        // Initially empty
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert!(cache.get("test").is_none());

        // Insert
        let columnar = create_test_columnar(100);
        let _ = cache.insert("test", columnar);

        // Should be cached now
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);
        assert!(cache.contains("test"));
        assert!(cache.get("test").is_some());

        // Stats should reflect operations
        let stats = cache.stats();
        assert_eq!(stats.hits, 1); // The get above
        assert_eq!(stats.misses, 1); // The initial get
        assert_eq!(stats.conversions, 1);
    }

    #[test]
    fn test_cache_invalidation() {
        let cache = ColumnarCache::new(1024 * 1024);

        let columnar = create_test_columnar(100);
        let _ = cache.insert("test", columnar);
        assert!(cache.contains("test"));

        cache.invalidate("test");
        assert!(!cache.contains("test"));

        let stats = cache.stats();
        assert_eq!(stats.invalidations, 1);
    }

    #[test]
    fn test_cache_eviction() {
        // Small cache that can only hold one entry
        let cache = ColumnarCache::new(1024); // 1KB

        // Insert first entry
        let columnar1 = create_test_columnar(10);
        let _ = cache.insert("table1", columnar1);
        assert!(cache.contains("table1"));

        // Insert second entry - should evict first due to memory pressure
        let columnar2 = create_test_columnar(10);
        let _ = cache.insert("table2", columnar2);

        // At least one table should be evicted if memory is tight
        // The exact behavior depends on sizes
        let stats = cache.stats();
        // Either evictions occurred or both fit
        assert!(stats.evictions > 0 || cache.len() == 2);
    }

    #[test]
    fn test_cache_arc_sharing() {
        let cache = ColumnarCache::new(1024 * 1024);

        let columnar = create_test_columnar(100);
        let arc1 = cache.insert("test", columnar);
        let arc2 = cache.get("test").unwrap();

        // Both should point to the same data
        assert!(Arc::ptr_eq(&arc1, &arc2));

        // Reference count should be 3 (cache entry + arc1 + arc2)
        assert_eq!(Arc::strong_count(&arc1), 3);
    }

    #[test]
    fn test_cache_clear() {
        let cache = ColumnarCache::new(1024 * 1024);

        let _ = cache.insert("table1", create_test_columnar(10));
        let _ = cache.insert("table2", create_test_columnar(10));

        assert_eq!(cache.len(), 2);
        assert!(cache.memory_usage() > 0);

        cache.clear();

        assert_eq!(cache.len(), 0);
        assert_eq!(cache.memory_usage(), 0);
    }

    #[test]
    fn test_cache_update_existing() {
        let cache = ColumnarCache::new(1024 * 1024);

        let columnar1 = create_test_columnar(10);
        let _ = cache.insert("test", columnar1);
        let memory1 = cache.memory_usage();

        // Update with larger entry
        let columnar2 = create_test_columnar(100);
        let _ = cache.insert("test", columnar2);
        let memory2 = cache.memory_usage();

        // Memory should have changed
        assert_ne!(memory1, memory2);

        // Should still be just one entry
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_hit_rate() {
        let stats =
            CacheStats { hits: 80, misses: 20, evictions: 0, conversions: 0, invalidations: 0 };

        assert!((stats.hit_rate() - 80.0).abs() < 0.001);
    }

    #[test]
    fn test_hit_rate_empty() {
        let stats = CacheStats::default();
        assert_eq!(stats.hit_rate(), 0.0);
    }
}
