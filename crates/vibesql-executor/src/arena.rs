//! Query-lifetime arena allocator for temporary buffers
//!
//! This module provides fast bump-pointer allocation for temporary data
//! that lives only for the duration of a query execution. Using an arena
//! dramatically reduces allocation overhead by:
//!
//! 1. **Batch Allocation**: Single large allocation serves many requests
//! 2. **Zero Fragmentation**: Sequential layout, no gaps
//! 3. **Fast Deallocation**: Reset entire arena in O(1)
//! 4. **Cache Friendly**: Sequential allocations improve locality
//!
//! # Usage
//!
//! ```text
//! let arena = QueryArena::new();
//!
//! // Allocate temporary buffers
//! let bitmap = arena.alloc_bitmap(1024);
//! bitmap.fill(false);
//!
//! // Arena automatically freed when dropped (end of query)
//! ```
//!
//! # Performance
//!
//! - Allocation: O(1) bump pointer vs. O(log n) malloc
//! - Deallocation: O(1) reset vs. O(1) per free
//! - Memory: ~10% overhead vs. ~15-20% malloc overhead
//!
//! # Memory Safety
//!
//! Arena allocations are tied to the arena's lifetime. The borrow checker
//! ensures borrowed data cannot outlive the arena.

use bumpalo::Bump;

/// Query-lifetime arena allocator
///
/// Provides fast bump-pointer allocation for temporary buffers during query execution.
/// All allocations are freed together when the arena is dropped (end of query).
///
/// # Thread Safety
///
/// `QueryArena` is NOT thread-safe (no Sync/Send). Each thread should have its own
/// arena for parallel query execution. Use `thread_local!` or pass per-thread.
///
/// # Examples
///
/// ```text
/// // Create arena at query start
/// let arena = QueryArena::new();
///
/// // Allocate filter bitmap (fast O(1) bump allocation)
/// let bitmap = arena.alloc_bitmap(10000);
/// for i in 0..10000 {
///     bitmap[i] = evaluate_predicate(i);
/// }
///
/// // Allocate temp buffer for expression results
/// let results = arena.alloc_values(10000);
/// for i in 0..10000 {
///     results[i] = evaluate_expression(i);
/// }
///
/// // Arena automatically freed at end of query (drop)
/// ```
pub struct QueryArena {
    /// Underlying bump allocator
    bump: Bump,
}

impl QueryArena {
    /// Create a new query arena with default capacity (1 MB)
    ///
    /// The arena pre-allocates a large chunk to serve many small allocations
    /// without frequent system calls.
    #[inline]
    pub fn new() -> Self {
        Self::with_capacity(1024 * 1024) // 1 MB default
    }

    /// Create a new query arena with specified initial capacity
    ///
    /// Use this if you have a good estimate of memory needs:
    /// - Small queries (< 10K rows): 128 KB
    /// - Medium queries (10K-100K rows): 1 MB
    /// - Large queries (> 100K rows): 10 MB
    ///
    /// Arena will grow automatically if needed.
    #[inline]
    pub fn with_capacity(bytes: usize) -> Self {
        Self { bump: Bump::with_capacity(bytes) }
    }

    /// Allocate a boolean bitmap with specified length
    ///
    /// Common use: Filter bitmaps marking rows that pass predicates
    ///
    /// # Example
    ///
    /// ```text
    /// let bitmap = arena.alloc_bitmap(1000);
    /// for i in 0..1000 {
    ///     bitmap[i] = row_passes_filter(i);
    /// }
    /// ```
    #[inline]
    pub fn alloc_bitmap(&self, len: usize) -> &mut [bool] {
        self.bump.alloc_slice_fill_default(len)
    }

    /// Allocate a bitmap and initialize with a specific value
    ///
    /// More efficient than alloc_bitmap + fill for large bitmaps.
    ///
    /// # Example
    ///
    /// ```text
    /// // All rows pass by default, then mark failures
    /// let bitmap = arena.alloc_bitmap_filled(1000, true);
    /// for i in failing_rows {
    ///     bitmap[i] = false;
    /// }
    /// ```
    #[inline]
    pub fn alloc_bitmap_filled(&self, len: usize, value: bool) -> &mut [bool] {
        self.bump.alloc_slice_fill_copy(len, value)
    }

    /// Allocate an array of values
    ///
    /// Use for temporary expression results, intermediate calculations, etc.
    ///
    /// # Example
    ///
    /// ```text
    /// let temps: &mut [f64] = arena.alloc_slice(1000);
    /// for i in 0..1000 {
    ///     temps[i] = calculate(i);
    /// }
    /// ```
    #[inline]
    pub fn alloc_slice<T: Default + Copy>(&self, len: usize) -> &mut [T] {
        self.bump.alloc_slice_fill_default(len)
    }

    /// Allocate an array and initialize with a specific value
    #[inline]
    pub fn alloc_slice_filled<T: Copy>(&self, len: usize, value: T) -> &mut [T] {
        self.bump.alloc_slice_fill_copy(len, value)
    }

    /// Allocate a single value
    ///
    /// Use sparingly - arena allocation is optimized for bulk allocations.
    /// For single values, normal heap allocation may be comparable.
    #[inline]
    pub fn alloc<T>(&self, value: T) -> &mut T {
        self.bump.alloc(value)
    }

    /// Allocate a Vec-like collection with specified capacity
    ///
    /// The returned Vec will allocate from the arena's chunk, not the heap.
    ///
    /// # Example
    ///
    /// ```text
    /// let mut results = arena.alloc_vec();
    /// for row in rows {
    ///     if predicate(row) {
    ///         results.push(row);
    ///     }
    /// }
    /// ```
    #[inline]
    pub fn alloc_vec<T>(&self) -> bumpalo::collections::Vec<'_, T> {
        bumpalo::collections::Vec::new_in(&self.bump)
    }

    /// Allocate a Vec with specified capacity
    #[inline]
    pub fn alloc_vec_with_capacity<T>(&self, capacity: usize) -> bumpalo::collections::Vec<'_, T> {
        bumpalo::collections::Vec::with_capacity_in(capacity, &self.bump)
    }

    /// Reset the arena, freeing all allocations at once
    ///
    /// This is much faster than freeing individual allocations.
    /// Use between queries when reusing the same arena.
    ///
    /// # Safety
    ///
    /// All references to arena-allocated data become invalid after reset.
    /// The borrow checker prevents this at compile time for normal usage.
    ///
    /// # Example
    ///
    /// ```text
    /// let arena = QueryArena::new();
    ///
    /// for query in queries {
    ///     let bitmap = arena.alloc_bitmap(1000);
    ///     // ... execute query ...
    ///     arena.reset(); // Free all at once for next query
    /// }
    /// ```
    #[inline]
    pub fn reset(&mut self) {
        self.bump.reset();
    }

    /// Get the number of bytes allocated from the arena
    ///
    /// Useful for profiling and capacity tuning.
    #[inline]
    pub fn allocated_bytes(&self) -> usize {
        self.bump.allocated_bytes()
    }
}

impl Default for QueryArena {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_allocation() {
        let arena = QueryArena::new();

        let bitmap = arena.alloc_bitmap(100);
        assert_eq!(bitmap.len(), 100);

        bitmap[0] = true;
        bitmap[99] = true;
        assert!(bitmap[0]);
        assert!(!bitmap[50]);
        assert!(bitmap[99]);
    }

    #[test]
    fn test_bitmap_filled() {
        let arena = QueryArena::new();

        let all_true = arena.alloc_bitmap_filled(50, true);
        assert!(all_true.iter().all(|&b| b));

        let all_false = arena.alloc_bitmap_filled(50, false);
        assert!(all_false.iter().all(|&b| !b));
    }

    #[test]
    fn test_slice_allocation() {
        let arena = QueryArena::new();

        let ints: &mut [i64] = arena.alloc_slice(10);
        for (i, val) in ints.iter_mut().enumerate() {
            *val = i as i64;
        }
        assert_eq!(ints[5], 5);

        let floats = arena.alloc_slice_filled(5, 3.5);
        assert_eq!(floats.len(), 5);
        assert_eq!(floats[0], 3.5);
    }

    #[test]
    fn test_vec_allocation() {
        let arena = QueryArena::new();

        let mut v = arena.alloc_vec();
        v.push(1);
        v.push(2);
        v.push(3);
        assert_eq!(v.len(), 3);
        assert_eq!(v[1], 2);
    }

    #[test]
    fn test_reset() {
        let mut arena = QueryArena::new();

        // First allocation
        let _bitmap1 = arena.alloc_bitmap(1000);
        let bytes1 = arena.allocated_bytes();
        assert!(bytes1 > 0);

        // Reset - bumpalo keeps chunks allocated but resets the bump pointer
        arena.reset();
        let bytes2 = arena.allocated_bytes();
        // After reset, chunks are kept allocated (not freed) for reuse
        assert_eq!(bytes2, bytes1, "Reset should keep chunks for reuse");

        // Second allocation (reuses space)
        let _bitmap2 = arena.alloc_bitmap(1000);
        let bytes3 = arena.allocated_bytes();
        // Should reuse the same chunk without growing
        assert_eq!(bytes3, bytes1, "Should reuse existing chunk without growing");
    }

    #[test]
    fn test_with_capacity() {
        let arena = QueryArena::with_capacity(1024);
        let initial_bytes = arena.allocated_bytes();

        // Bumpalo rounds up capacity to efficient chunk sizes (often page-aligned)
        // so allocated_bytes() may be larger than requested capacity
        assert!(initial_bytes >= 1024, "Should allocate at least requested capacity");

        let _bitmap = arena.alloc_bitmap(100);
        let after_alloc = arena.allocated_bytes();

        // Should not need to grow for this small allocation
        assert_eq!(after_alloc, initial_bytes, "Small allocation should not grow chunk");
    }

    #[test]
    fn test_large_allocation() {
        let arena = QueryArena::new();

        // Allocate more than default capacity
        let large = arena.alloc_bitmap(10_000_000); // 10M bools
        assert_eq!(large.len(), 10_000_000);

        // Arena should have grown automatically
        assert!(arena.allocated_bytes() >= 10_000_000);
    }
}
