//! String interning for low-cardinality string columns.
//!
//! This module provides string interning for enum-like columns with few distinct values.
//! By sharing the same `Arc<str>` reference for identical strings, we achieve:
//! - Memory reduction: N identical strings share one allocation
//! - Faster equality: `Arc::ptr_eq` enables O(1) pointer comparison
//! - Cache efficiency: fewer unique allocations improve cache behavior
//!
//! ## Usage
//!
//! ```text
//! let mut interner = StringInterner::new(32); // threshold of 32 distinct values
//!
//! let s1 = interner.intern("active");
//! let s2 = interner.intern("active");
//! assert!(Arc::ptr_eq(&s1, &s2)); // Same Arc reference
//!
//! // Once cardinality exceeds threshold, interning is disabled
//! for i in 0..100 {
//!     interner.intern(&format!("value_{}", i));
//! }
//! assert!(!interner.is_active()); // Interning disabled
//! ```
//!
//! ## Performance Characteristics
//!
//! - **Low cardinality (< threshold)**: O(1) lookup + O(1) Arc clone
//! - **High cardinality (>= threshold)**: Falls back to regular Arc::from allocation
//! - **Memory**: O(cardinality) for the intern pool

use std::collections::HashSet;
use std::sync::Arc;

/// Default cardinality threshold for string interning.
///
/// Columns with fewer distinct values than this threshold will have their
/// strings interned for memory and comparison efficiency.
pub const DEFAULT_CARDINALITY_THRESHOLD: usize = 32;

/// String interner for low-cardinality string columns.
///
/// Tracks distinct string values and provides interning (deduplication)
/// when the number of distinct values is below a configurable threshold.
///
/// ## Example Use Cases
///
/// - TPC-H `l_returnflag`: 3 values ('A', 'N', 'R')
/// - TPC-H `l_linestatus`: 2 values ('O', 'F')
/// - Status columns: ('active', 'pending', 'completed')
/// - Country codes: ('US', 'UK', 'DE', ...)
#[derive(Debug)]
pub struct StringInterner {
    /// The intern pool storing unique string references.
    /// Uses HashSet with Arc<str> for O(1) lookup by string content.
    pool: HashSet<Arc<str>>,

    /// Maximum number of distinct values before disabling interning.
    threshold: usize,

    /// Whether interning is still active.
    /// Set to false when cardinality exceeds threshold.
    active: bool,
}

impl Default for StringInterner {
    fn default() -> Self {
        Self::new(DEFAULT_CARDINALITY_THRESHOLD)
    }
}

impl StringInterner {
    /// Create a new string interner with the specified cardinality threshold.
    ///
    /// # Arguments
    /// * `threshold` - Maximum distinct values before disabling interning.
    ///   Recommended: 32 for typical enum-like columns.
    pub fn new(threshold: usize) -> Self {
        Self {
            pool: HashSet::with_capacity(threshold.min(64)),
            threshold,
            active: true,
        }
    }

    /// Intern a string, returning a shared `Arc<str>` reference.
    ///
    /// If the string is already in the pool, returns the existing Arc (O(1) clone).
    /// If the string is new and cardinality is below threshold, adds it to the pool.
    /// If cardinality exceeds threshold, disables interning and returns a new Arc.
    ///
    /// # Arguments
    /// * `s` - The string to intern
    ///
    /// # Returns
    /// An `Arc<str>` reference to the string. For low-cardinality columns,
    /// identical strings will return the same Arc (pointer equality).
    pub fn intern(&mut self, s: &str) -> Arc<str> {
        if !self.active {
            // Interning disabled - just allocate a new Arc
            return Arc::from(s);
        }

        // Check if string is already in the pool
        if let Some(existing) = self.pool.get(s) {
            return existing.clone();
        }

        // New string - check if we're at the threshold
        if self.pool.len() >= self.threshold {
            // Exceeded threshold - disable interning
            self.active = false;
            return Arc::from(s);
        }

        // Add to pool and return
        let arc: Arc<str> = Arc::from(s);
        self.pool.insert(arc.clone());
        arc
    }

    /// Intern an existing `Arc<str>`, potentially deduplicating it.
    ///
    /// If an identical string is already in the pool, returns the pooled Arc.
    /// This allows deduplication even when the caller already has an Arc.
    ///
    /// # Arguments
    /// * `arc` - The Arc<str> to potentially deduplicate
    ///
    /// # Returns
    /// Either the original Arc (if interning is disabled or string is new)
    /// or a shared Arc from the pool (if string was already interned).
    pub fn intern_arc(&mut self, arc: Arc<str>) -> Arc<str> {
        if !self.active {
            return arc;
        }

        // Check if string is already in the pool
        if let Some(existing) = self.pool.get(arc.as_ref()) {
            return existing.clone();
        }

        // New string - check threshold
        if self.pool.len() >= self.threshold {
            self.active = false;
            return arc;
        }

        // Add the provided arc to the pool (it's already an Arc<str>)
        self.pool.insert(arc.clone());
        arc
    }

    /// Check if interning is still active.
    ///
    /// Returns false if cardinality has exceeded the threshold.
    #[inline]
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Get the current number of distinct values in the intern pool.
    #[inline]
    pub fn cardinality(&self) -> usize {
        self.pool.len()
    }

    /// Get the cardinality threshold.
    #[inline]
    pub fn threshold(&self) -> usize {
        self.threshold
    }

    /// Check if a string is already interned.
    ///
    /// Returns true if the string is in the pool, false otherwise.
    /// This does not modify the interner.
    pub fn contains(&self, s: &str) -> bool {
        self.pool.contains(s)
    }

    /// Clear the intern pool and reset to active state.
    ///
    /// This is useful for reusing an interner for a new column.
    pub fn clear(&mut self) {
        self.pool.clear();
        self.active = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_interning() {
        let mut interner = StringInterner::new(32);

        let s1 = interner.intern("hello");
        let s2 = interner.intern("hello");
        let s3 = interner.intern("world");

        // Same string should return same Arc
        assert!(Arc::ptr_eq(&s1, &s2));
        // Different strings should have different Arcs
        assert!(!Arc::ptr_eq(&s1, &s3));
        // Content should match
        assert_eq!(s1.as_ref(), "hello");
        assert_eq!(s3.as_ref(), "world");
    }

    #[test]
    fn test_cardinality_tracking() {
        let mut interner = StringInterner::new(32);

        interner.intern("a");
        interner.intern("b");
        interner.intern("a"); // duplicate

        assert_eq!(interner.cardinality(), 2);
        assert!(interner.is_active());
    }

    #[test]
    fn test_threshold_exceeded() {
        let mut interner = StringInterner::new(3);

        interner.intern("a");
        interner.intern("b");
        interner.intern("c");
        assert!(interner.is_active());

        // This should exceed the threshold
        let d = interner.intern("d");
        assert!(!interner.is_active());

        // New strings should still work but not be interned
        let e = interner.intern("e");
        assert_eq!(d.as_ref(), "d");
        assert_eq!(e.as_ref(), "e");

        // Pool size should not have grown
        assert_eq!(interner.cardinality(), 3);
    }

    #[test]
    fn test_intern_arc() {
        let mut interner = StringInterner::new(32);

        // First intern a string
        let s1 = interner.intern("test");

        // Then intern an Arc of the same string
        let arc = Arc::from("test");
        let s2 = interner.intern_arc(arc);

        // Should return the pooled version
        assert!(Arc::ptr_eq(&s1, &s2));
    }

    #[test]
    fn test_contains() {
        let mut interner = StringInterner::new(32);

        interner.intern("exists");

        assert!(interner.contains("exists"));
        assert!(!interner.contains("not_exists"));
    }

    #[test]
    fn test_clear() {
        let mut interner = StringInterner::new(2);

        interner.intern("a");
        interner.intern("b");
        interner.intern("c"); // exceeds threshold
        assert!(!interner.is_active());

        interner.clear();
        assert!(interner.is_active());
        assert_eq!(interner.cardinality(), 0);
    }

    #[test]
    fn test_tpch_like_columns() {
        let mut interner = StringInterner::new(32);

        // Simulate l_returnflag (3 distinct values)
        for _ in 0..10000 {
            interner.intern("A");
            interner.intern("N");
            interner.intern("R");
        }

        assert!(interner.is_active());
        assert_eq!(interner.cardinality(), 3);

        // All references should be shared
        let a1 = interner.intern("A");
        let a2 = interner.intern("A");
        assert!(Arc::ptr_eq(&a1, &a2));
    }

    #[test]
    fn test_default() {
        let interner = StringInterner::default();
        assert_eq!(interner.threshold(), DEFAULT_CARDINALITY_THRESHOLD);
        assert!(interner.is_active());
    }
}
