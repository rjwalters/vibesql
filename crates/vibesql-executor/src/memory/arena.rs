//! Arena allocator for query-scoped memory allocation
//!
//! This module provides a bump-pointer arena allocator that eliminates
//! malloc/free overhead by allocating from a pre-allocated buffer.
//!
//! # Performance
//!
//! - Allocation: ~1ns (vs ~100ns for malloc)
//! - Deallocation: 0ns (arena dropped at end)
//! - Cache locality: sequential allocations in contiguous memory
//! - No fragmentation: single buffer reused per query
//!
//! # Safety
//!
//! All allocations are tied to the arena's lifetime via Rust's borrow checker.
//! References returned cannot outlive the arena.

use std::alloc::{Layout, alloc, dealloc};
use std::cell::Cell;
use std::marker::PhantomData;
use std::mem::{self, MaybeUninit};
use std::ptr;

/// Arena allocator for query-scoped allocations
///
/// Uses a bump-pointer strategy to allocate from a pre-allocated buffer.
/// All allocations are freed when the arena is reset or dropped.
///
/// # Example
///
/// ```rust
/// use vibesql_executor::memory::QueryArena;
/// use std::mem::MaybeUninit;
///
/// let arena = QueryArena::with_capacity(1024);
///
/// // Allocate a value
/// let x = arena.alloc(42i64);
/// assert_eq!(*x, 42);
///
/// // Allocate a slice (returns uninitialized memory)
/// let slice = arena.alloc_slice::<i32>(10);
/// for i in 0..10 {
///     slice[i] = MaybeUninit::new(i as i32);
/// }
/// ```
/// Maximum alignment we support (16 bytes covers most use cases including SIMD types)
const MAX_ALIGN: usize = 16;

pub struct QueryArena {
    /// Pre-allocated buffer for all allocations (aligned to MAX_ALIGN)
    buffer: *mut u8,
    /// Capacity of the buffer in bytes
    capacity: usize,
    /// Current offset into the buffer (bump pointer)
    offset: Cell<usize>,
    /// Phantom data to ensure proper variance
    _marker: PhantomData<Cell<u8>>,
}

impl QueryArena {
    /// Default arena size: 10MB
    ///
    /// Based on TPC-H Q6 analysis:
    /// - 60K rows × 16 columns = 960K SqlValues
    /// - Intermediate results: ~2MB
    /// - Expression temporaries: ~1MB
    /// - Safety margin: 7MB
    pub const DEFAULT_CAPACITY: usize = 10 * 1024 * 1024; // 10MB

    /// Create a new arena with the specified capacity in bytes
    ///
    /// # Arguments
    ///
    /// * `bytes` - Number of bytes to pre-allocate
    ///
    /// # Example
    ///
    /// ```rust
    /// use vibesql_executor::memory::QueryArena;
    ///
    /// // 1MB arena
    /// let arena = QueryArena::with_capacity(1024 * 1024);
    /// ```
    pub fn with_capacity(bytes: usize) -> Self {
        // SAFETY: Allocate properly aligned memory
        let layout = Layout::from_size_align(bytes, MAX_ALIGN)
            .expect("invalid arena layout");
        let buffer = unsafe { alloc(layout) };
        if buffer.is_null() {
            panic!("arena allocation failed");
        }
        Self {
            buffer,
            capacity: bytes,
            offset: Cell::new(0),
            _marker: PhantomData,
        }
    }

    /// Create a new arena with the default capacity (10MB)
    ///
    /// # Example
    ///
    /// ```rust
    /// use vibesql_executor::memory::QueryArena;
    ///
    /// let arena = QueryArena::new();
    /// ```
    pub fn new() -> Self {
        Self::with_capacity(Self::DEFAULT_CAPACITY)
    }

    /// Allocate a value in the arena
    ///
    /// Returns a reference to the allocated value with lifetime tied to the arena.
    ///
    /// # Performance
    ///
    /// Allocation overhead: ~1ns (vs ~100ns for heap allocation)
    ///
    /// # Panics
    ///
    /// Panics if the arena is out of space. Use `try_alloc` for fallible allocation.
    ///
    /// # Example
    ///
    /// ```rust
    /// use vibesql_executor::memory::QueryArena;
    ///
    /// let arena = QueryArena::new();
    /// let x = arena.alloc(42i64);
    /// let y = arena.alloc(3.5f64);
    ///
    /// assert_eq!(*x, 42);
    /// assert_eq!(*y, 3.5);
    /// ```
    #[inline(always)]
    pub fn alloc<T>(&self, value: T) -> &T {
        let offset = self.offset.get();
        let size = mem::size_of::<T>();
        let align = mem::align_of::<T>();

        assert!(
            align <= MAX_ALIGN,
            "arena does not support alignment greater than {}",
            MAX_ALIGN
        );

        // Align pointer to T's alignment requirement
        let aligned_offset = (offset + align - 1) & !(align - 1);

        // Check capacity
        let end_offset = aligned_offset.checked_add(size).expect("arena offset overflow");

        assert!(
            end_offset <= self.capacity,
            "arena overflow: need {} bytes, have {} remaining",
            size,
            self.capacity - aligned_offset
        );

        // Bump pointer
        self.offset.set(end_offset);

        // Write value and return reference
        // SAFETY: We've verified:
        // - Buffer has enough space (checked above)
        // - Pointer is properly aligned (buffer is MAX_ALIGN aligned, aligned_offset ensures T's alignment)
        // - Lifetime is tied to arena via borrow checker
        unsafe {
            let ptr = self.buffer.add(aligned_offset) as *mut T;
            ptr::write(ptr, value);
            &*ptr
        }
    }

    /// Allocate a slice in the arena
    ///
    /// Returns a mutable slice of `MaybeUninit<T>` with the specified length.
    /// Caller must initialize elements before assuming them to be initialized.
    ///
    /// Using `MaybeUninit` ensures memory safety - uninitialized memory cannot
    /// be accidentally read or dropped, preventing undefined behavior.
    ///
    /// # Performance
    ///
    /// Allocation overhead: ~1ns + negligible bounds check
    ///
    /// # Panics
    ///
    /// Panics if the arena is out of space.
    ///
    /// # Example
    ///
    /// ```rust
    /// use vibesql_executor::memory::QueryArena;
    /// use std::mem::MaybeUninit;
    ///
    /// let arena = QueryArena::new();
    /// let slice = arena.alloc_slice::<i32>(100);
    ///
    /// // Initialize the slice
    /// for i in 0..100 {
    ///     slice[i] = MaybeUninit::new(i as i32);
    /// }
    ///
    /// // SAFETY: All elements have been initialized above
    /// let initialized_slice = unsafe {
    ///     std::slice::from_raw_parts(
    ///         slice.as_ptr() as *const i32,
    ///         slice.len()
    ///     )
    /// };
    /// assert_eq!(initialized_slice[50], 50);
    /// ```
    #[inline(always)]
    #[allow(clippy::mut_from_ref)]
    pub fn alloc_slice<T>(&self, len: usize) -> &mut [MaybeUninit<T>] {
        if len == 0 {
            // Return empty slice without allocating
            return &mut [];
        }

        let offset = self.offset.get();
        let size = mem::size_of::<T>().checked_mul(len).expect("slice size overflow");
        let align = mem::align_of::<T>();

        assert!(
            align <= MAX_ALIGN,
            "arena does not support alignment greater than {}",
            MAX_ALIGN
        );

        // Align pointer to T's alignment requirement
        let aligned_offset = (offset + align - 1) & !(align - 1);

        // Check capacity
        let end_offset = aligned_offset.checked_add(size).expect("arena offset overflow");

        assert!(
            end_offset <= self.capacity,
            "arena overflow: need {} bytes, have {} remaining",
            size,
            self.capacity - aligned_offset
        );

        // Bump pointer
        self.offset.set(end_offset);

        // Return slice of MaybeUninit<T> (safe for uninitialized memory)
        // SAFETY: We've verified:
        // - Buffer has enough space (checked above)
        // - Pointer is properly aligned (buffer is MAX_ALIGN aligned, aligned_offset ensures T's alignment)
        // - Size doesn't overflow (checked above)
        // - Lifetime is tied to arena via borrow checker
        // - MaybeUninit<T> is safe to construct from uninitialized memory
        unsafe {
            let ptr = self.buffer.add(aligned_offset) as *mut MaybeUninit<T>;
            std::slice::from_raw_parts_mut(ptr, len)
        }
    }

    /// Try to allocate a value in the arena
    ///
    /// Returns None if the arena is out of space.
    ///
    /// # Example
    ///
    /// ```rust
    /// use vibesql_executor::memory::QueryArena;
    ///
    /// let arena = QueryArena::with_capacity(64);
    ///
    /// // This succeeds
    /// assert!(arena.try_alloc(42i64).is_some());
    ///
    /// // Eventually this fails (arena full)
    /// let mut count = 0;
    /// while arena.try_alloc(count).is_some() {
    ///     count += 1;
    /// }
    /// ```
    #[inline]
    pub fn try_alloc<T>(&self, value: T) -> Option<&T> {
        let offset = self.offset.get();
        let size = mem::size_of::<T>();
        let align = mem::align_of::<T>();

        if align > MAX_ALIGN {
            return None;
        }

        let aligned_offset = (offset + align - 1) & !(align - 1);
        let end_offset = aligned_offset.checked_add(size)?;

        if end_offset > self.capacity {
            return None;
        }

        self.offset.set(end_offset);

        // SAFETY: Same guarantees as alloc(), but checked for space
        unsafe {
            let ptr = self.buffer.add(aligned_offset) as *mut T;
            ptr::write(ptr, value);
            Some(&*ptr)
        }
    }

    /// Reset the arena, allowing it to be reused
    ///
    /// This doesn't deallocate the buffer, just resets the bump pointer.
    /// All previously allocated values are invalidated.
    ///
    /// # Example
    ///
    /// ```rust
    /// use vibesql_executor::memory::QueryArena;
    ///
    /// let mut arena = QueryArena::new();
    ///
    /// // Allocate some values (i64 = 8 bytes)
    /// let x = arena.alloc(42i64);
    /// assert_eq!(arena.used_bytes(), 8);
    ///
    /// // Reset and reuse
    /// arena.reset();
    /// assert_eq!(arena.used_bytes(), 0);
    ///
    /// // Can allocate again
    /// let y = arena.alloc(100i64);
    /// ```
    pub fn reset(&mut self) {
        self.offset.set(0);
        // Note: We don't need to clear the buffer since we'll overwrite it
    }

    /// Get the number of bytes used in this arena
    ///
    /// # Example
    ///
    /// ```rust
    /// use vibesql_executor::memory::QueryArena;
    ///
    /// let arena = QueryArena::new();
    ///
    /// arena.alloc(42i64); // 8 bytes
    /// arena.alloc(3.5f64); // 8 bytes
    ///
    /// assert_eq!(arena.used_bytes(), 16);
    /// ```
    #[inline]
    pub fn used_bytes(&self) -> usize {
        self.offset.get()
    }

    /// Get the total capacity of this arena in bytes
    ///
    /// # Example
    ///
    /// ```rust
    /// use vibesql_executor::memory::QueryArena;
    ///
    /// let arena = QueryArena::with_capacity(1024);
    /// assert_eq!(arena.capacity_bytes(), 1024);
    /// ```
    #[inline]
    pub fn capacity_bytes(&self) -> usize {
        self.capacity
    }

    /// Get the number of bytes remaining in this arena
    ///
    /// # Example
    ///
    /// ```rust
    /// use vibesql_executor::memory::QueryArena;
    ///
    /// let arena = QueryArena::with_capacity(1024);
    /// arena.alloc(42i64); // 8 bytes
    ///
    /// assert_eq!(arena.remaining_bytes(), 1024 - 8);
    /// ```
    #[inline]
    pub fn remaining_bytes(&self) -> usize {
        self.capacity - self.offset.get()
    }
}

impl Drop for QueryArena {
    fn drop(&mut self) {
        // SAFETY: buffer was allocated with Layout::from_size_align(capacity, MAX_ALIGN)
        unsafe {
            let layout = Layout::from_size_align_unchecked(self.capacity, MAX_ALIGN);
            dealloc(self.buffer, layout);
        }
    }
}

// SAFETY: QueryArena can be sent between threads because:
// - The buffer is only accessed through methods that take &self
// - Cell provides interior mutability but is single-threaded
// - The arena is not Sync (cannot be shared between threads) but is Send
unsafe impl Send for QueryArena {}

impl Default for QueryArena {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_basic_allocation() {
        let arena = QueryArena::with_capacity(1024);

        let x = arena.alloc(42i64);
        let y = arena.alloc(3.5f64);

        assert_eq!(*x, 42);
        assert_eq!(*y, 3.5);
    }

    #[test]
    fn test_arena_slice_allocation() {
        let arena = QueryArena::with_capacity(4096);

        let slice = arena.alloc_slice::<i32>(100);

        // Initialize the slice with MaybeUninit
        for (i, elem) in slice.iter_mut().enumerate() {
            *elem = MaybeUninit::new(i as i32);
        }

        // SAFETY: All elements have been initialized above
        let initialized_slice =
            unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const i32, slice.len()) };

        assert_eq!(initialized_slice[50], 50);
        assert_eq!(slice.len(), 100);
    }

    #[test]
    fn test_arena_empty_slice() {
        let arena = QueryArena::new();
        let slice: &mut [MaybeUninit<i32>] = arena.alloc_slice::<i32>(0);
        assert_eq!(slice.len(), 0);
    }

    #[test]
    fn test_arena_reset() {
        let mut arena = QueryArena::with_capacity(1024);

        let x = arena.alloc(42i64);
        assert_eq!(*x, 42);
        assert_eq!(arena.used_bytes(), 8);

        arena.reset();
        assert_eq!(arena.used_bytes(), 0);

        // Can allocate again after reset
        let y = arena.alloc(100i64);
        assert_eq!(*y, 100);
    }

    #[test]
    fn test_arena_alignment() {
        let arena = QueryArena::with_capacity(1024);

        // Allocate misaligned type first
        let _byte = arena.alloc(1u8);

        // This should be properly aligned despite previous allocation
        let x = arena.alloc(42i64);
        assert_eq!(*x, 42);

        // Verify alignment
        let ptr = x as *const i64 as usize;
        assert_eq!(ptr % std::mem::align_of::<i64>(), 0);
    }

    #[test]
    #[should_panic(expected = "arena overflow")]
    fn test_arena_overflow() {
        let arena = QueryArena::with_capacity(64);

        // Allocate more than capacity
        for i in 0..1000 {
            arena.alloc(i);
        }
    }

    #[test]
    fn test_try_alloc_success() {
        let arena = QueryArena::with_capacity(1024);

        let x = arena.try_alloc(42i64);
        assert!(x.is_some());
        assert_eq!(*x.unwrap(), 42);
    }

    #[test]
    fn test_try_alloc_failure() {
        let arena = QueryArena::with_capacity(8);

        // First allocation succeeds
        assert!(arena.try_alloc(42i64).is_some());

        // Second allocation fails (not enough space)
        assert!(arena.try_alloc(100i64).is_none());
    }

    #[test]
    fn test_arena_used_bytes() {
        let arena = QueryArena::new();

        assert_eq!(arena.used_bytes(), 0);

        arena.alloc(42i64); // 8 bytes
        assert_eq!(arena.used_bytes(), 8);

        arena.alloc(3.5f64); // 8 bytes
        assert_eq!(arena.used_bytes(), 16);
    }

    #[test]
    fn test_arena_capacity() {
        let arena = QueryArena::with_capacity(2048);
        assert_eq!(arena.capacity_bytes(), 2048);
        assert_eq!(arena.remaining_bytes(), 2048);

        arena.alloc(42i64);
        assert_eq!(arena.remaining_bytes(), 2048 - 8);
    }

    #[test]
    fn test_default_arena() {
        let arena = QueryArena::default();
        assert_eq!(arena.capacity_bytes(), QueryArena::DEFAULT_CAPACITY);
    }

    #[test]
    fn test_multiple_types() {
        let arena = QueryArena::new();

        let a = arena.alloc(1u8);
        let b = arena.alloc(2u16);
        let c = arena.alloc(3u32);
        let d = arena.alloc(4u64);
        let e = arena.alloc(5.0f32);
        let f = arena.alloc(6.0f64);

        assert_eq!(*a, 1);
        assert_eq!(*b, 2);
        assert_eq!(*c, 3);
        assert_eq!(*d, 4);
        assert_eq!(*e, 5.0);
        assert_eq!(*f, 6.0);
    }
}
