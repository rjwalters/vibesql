//! String interning for columns with limited distinct values.
//!
//! This module implements string interning to reduce memory usage for columns
//! with many repeated string values (e.g., enum-like columns, status codes).
//!
//! ## How It Works
//!
//! - Maintains a cache of interned strings using a HashMap
//! - When pushing a string value, checks if it's already in the cache
//! - Returns a clone of the Arc<str> if found (O(1) refcount bump)
//! - Otherwise, creates a new Arc<str> and caches it
//! - Significantly reduces memory for low-cardinality string columns
//!
//! ## Performance Characteristics
//!
//! - **Memory**: O(k * s) where k = distinct values, s = avg string length
//! - **Insert**: O(1) average case for cache hits, O(s) for hash of string
//! - **Best for**: Columns with <1000 distinct values and frequent repetition
//! - **Neutral for**: High-cardinality columns (each value unique)

use std::collections::HashMap;
use std::sync::Arc;

/// String interner for deduplicating identical strings in a column
///
/// Maintains a cache of Arc<str> values, allowing O(1) clones of repeated strings
/// instead of allocating new memory for each occurrence.
#[derive(Debug, Clone)]
pub struct StringInterner {
    /// Cache of interned strings: maps string content to Arc<str>
    cache: HashMap<Box<str>, Arc<str>>,
    /// Statistics for monitoring cache effectiveness
    stats: InternerStats,
}

/// Statistics about interner performance
#[derive(Debug, Clone, Default)]
pub struct InternerStats {
    /// Total number of values interned
    pub total_interned: usize,
    /// Number of cache hits (same value seen again)
    pub cache_hits: usize,
    /// Number of unique strings stored in cache
    pub unique_strings: usize,
}

impl StringInterner {
    /// Create a new string interner
    pub fn new() -> Self {
        StringInterner {
            cache: HashMap::new(),
            stats: InternerStats::default(),
        }
    }

    /// Create a new string interner with pre-allocated capacity
    ///
    /// # Arguments
    /// * `capacity` - Expected number of unique strings
    pub fn with_capacity(capacity: usize) -> Self {
        StringInterner {
            cache: HashMap::with_capacity(capacity),
            stats: InternerStats::default(),
        }
    }

    /// Intern a string, returning Arc<str>
    ///
    /// If the string is already in the cache, returns a clone of the cached Arc.
    /// Otherwise, creates a new Arc<str>, caches it, and returns it.
    ///
    /// # Arguments
    /// * `s` - The string to intern
    ///
    /// # Returns
    /// Arc<str> that may be shared across multiple rows
    pub fn intern(&mut self, s: &str) -> Arc<str> {
        self.stats.total_interned += 1;

        if let Some(cached) = self.cache.get(s) {
            self.stats.cache_hits += 1;
            return cached.clone();
        }

        let arc: Arc<str> = s.into();
        let boxed = s.into();
        self.cache.insert(boxed, arc.clone());
        self.stats.unique_strings = self.cache.len();
        arc
    }

    /// Get statistics about interning performance
    pub fn stats(&self) -> InternerStats {
        self.stats.clone()
    }

    /// Get the cache size (number of unique strings)
    pub fn cache_size(&self) -> usize {
        self.cache.len()
    }

    /// Check if a string is in the cache
    pub fn contains(&self, s: &str) -> bool {
        self.cache.contains_key(s)
    }

    /// Get cardinality (distinct value count)
    pub fn cardinality(&self) -> usize {
        self.cache.len()
    }

    /// Calculate the hit rate (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        if self.stats.total_interned == 0 {
            return 0.0;
        }
        self.stats.cache_hits as f64 / self.stats.total_interned as f64
    }

    /// Clear the cache
    pub fn clear(&mut self) {
        self.cache.clear();
        self.stats = InternerStats::default();
    }
}

impl Default for StringInterner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_interning() {
        let mut interner = StringInterner::new();

        let s1 = interner.intern("hello");
        let s2 = interner.intern("hello");
        let s3 = interner.intern("world");

        // Same string should have the same Arc
        assert_eq!(s1.as_ptr(), s2.as_ptr());

        // Different strings should have different Arcs
        assert_ne!(s1.as_ptr(), s3.as_ptr());

        // Content should be correct
        assert_eq!(s1.as_ref(), "hello");
        assert_eq!(s3.as_ref(), "world");
    }

    #[test]
    fn test_statistics() {
        let mut interner = StringInterner::new();

        interner.intern("status");
        interner.intern("status");
        interner.intern("pending");
        interner.intern("status");
        interner.intern("active");
        interner.intern("active");

        assert_eq!(interner.stats().total_interned, 6);
        assert_eq!(interner.stats().cache_hits, 3);
        assert_eq!(interner.stats().unique_strings, 3);
        assert_eq!(interner.cardinality(), 3);
        assert!((interner.hit_rate() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_empty_interner() {
        let interner = StringInterner::new();
        assert_eq!(interner.cache_size(), 0);
        assert_eq!(interner.cardinality(), 0);
        assert_eq!(interner.hit_rate(), 0.0);
    }

    #[test]
    fn test_contains() {
        let mut interner = StringInterner::new();
        interner.intern("hello");

        assert!(interner.contains("hello"));
        assert!(!interner.contains("world"));
    }

    #[test]
    fn test_clear() {
        let mut interner = StringInterner::new();
        interner.intern("hello");
        interner.intern("world");

        assert_eq!(interner.cache_size(), 2);

        interner.clear();
        assert_eq!(interner.cache_size(), 0);
        assert_eq!(interner.stats().total_interned, 0);
    }

    #[test]
    fn test_enum_like_column() {
        // Simulate an enum-like column with limited distinct values
        let mut interner = StringInterner::new();
        let statuses = vec!["pending", "active", "completed", "cancelled"];

        // 1000 rows with only 4 distinct values
        for _ in 0..250 {
            for status in &statuses {
                interner.intern(status);
            }
        }

        assert_eq!(interner.cardinality(), 4);
        assert_eq!(interner.stats().total_interned, 1000);
        assert_eq!(interner.stats().cache_hits, 996);
        assert!((interner.hit_rate() - 0.996).abs() < 0.01);
    }

    #[test]
    fn test_with_capacity() {
        let interner = StringInterner::with_capacity(100);
        // Just verify it creates successfully with capacity hint
        assert_eq!(interner.cache_size(), 0);
    }
}
