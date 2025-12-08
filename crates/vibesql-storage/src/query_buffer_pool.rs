//! Memory buffer pooling for query execution
//!
//! Provides reusable buffers to reduce allocation overhead in high-volume query execution.
//! Thread-local pools eliminate lock contention for concurrent access.

use crate::Row;
use std::cell::RefCell;
use vibesql_types::SqlValue;

/// Default initial capacity for row buffers
const DEFAULT_ROW_CAPACITY: usize = 128;

/// Default initial capacity for value buffers
const DEFAULT_VALUE_CAPACITY: usize = 16;

/// Maximum number of buffers to keep in each pool
const MAX_POOLED_BUFFERS: usize = 32;

thread_local! {
    static ROW_POOL: RefCell<Vec<Vec<Row>>> = const { RefCell::new(Vec::new()) };
    static VALUE_POOL: RefCell<Vec<Vec<SqlValue>>> = const { RefCell::new(Vec::new()) };
}

/// Thread-safe buffer pool for reusing allocations across query executions
///
/// Uses thread-local storage to eliminate lock contention. Each thread maintains
/// its own pool of buffers, bounded at MAX_POOLED_BUFFERS entries.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryBufferPool;

impl QueryBufferPool {
    /// Create a new empty buffer pool
    pub fn new() -> Self {
        Self
    }

    /// Get a row buffer from the pool, or create a new one
    ///
    /// Returns a buffer with at least `min_capacity` capacity.
    /// The buffer will be empty but may have allocated capacity.
    pub fn get_row_buffer(&self, min_capacity: usize) -> Vec<Row> {
        ROW_POOL.with(|pool| {
            let mut pool = pool.borrow_mut();

            // Try to find a buffer with sufficient capacity
            if let Some(pos) = pool.iter().position(|buf| buf.capacity() >= min_capacity) {
                let mut buffer = pool.swap_remove(pos);
                buffer.clear();
                buffer
            } else {
                // No suitable buffer found, create new one
                Vec::with_capacity(min_capacity.max(DEFAULT_ROW_CAPACITY))
            }
        })
    }

    /// Return a row buffer to the pool for reuse
    ///
    /// The buffer is cleared before being returned to the pool.
    /// If the pool is full, the buffer is dropped.
    pub fn return_row_buffer(&self, mut buffer: Vec<Row>) {
        buffer.clear();

        ROW_POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            if pool.len() < MAX_POOLED_BUFFERS {
                pool.push(buffer);
            }
            // else: drop the buffer (pool is full)
        });
    }

    /// Get a value buffer from the pool, or create a new one
    ///
    /// Returns a buffer with at least `min_capacity` capacity.
    /// The buffer will be empty but may have allocated capacity.
    pub fn get_value_buffer(&self, min_capacity: usize) -> Vec<SqlValue> {
        VALUE_POOL.with(|pool| {
            let mut pool = pool.borrow_mut();

            // Try to find a buffer with sufficient capacity
            if let Some(pos) = pool.iter().position(|buf| buf.capacity() >= min_capacity) {
                let mut buffer = pool.swap_remove(pos);
                buffer.clear();
                buffer
            } else {
                // No suitable buffer found, create new one
                Vec::with_capacity(min_capacity.max(DEFAULT_VALUE_CAPACITY))
            }
        })
    }

    /// Return a value buffer to the pool for reuse
    ///
    /// The buffer is cleared before being returned to the pool.
    /// If the pool is full, the buffer is dropped.
    pub fn return_value_buffer(&self, mut buffer: Vec<SqlValue>) {
        buffer.clear();

        VALUE_POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            if pool.len() < MAX_POOLED_BUFFERS {
                pool.push(buffer);
            }
            // else: drop the buffer (pool is full)
        });
    }

    /// Get statistics about pool usage (for debugging/monitoring)
    ///
    /// Returns stats for the current thread's pool only.
    pub fn stats(&self) -> QueryBufferPoolStats {
        let row_buffers_pooled = ROW_POOL.with(|pool| pool.borrow().len());
        let value_buffers_pooled = VALUE_POOL.with(|pool| pool.borrow().len());

        QueryBufferPoolStats { row_buffers_pooled, value_buffers_pooled }
    }

    /// Clear all thread-local buffer pools, releasing memory back to the allocator.
    ///
    /// This is useful for benchmarks and long-running processes where memory
    /// pressure needs to be reduced between query batches. The pools will
    /// automatically refill as needed during subsequent query execution.
    ///
    /// # Example
    ///
    /// ```text
    /// use vibesql_storage::QueryBufferPool;
    ///
    /// // Run a batch of queries...
    ///
    /// // Clear pools to release memory
    /// QueryBufferPool::clear_thread_local_pools();
    /// ```
    pub fn clear_thread_local_pools() {
        ROW_POOL.with(|pool| {
            pool.borrow_mut().clear();
        });
        VALUE_POOL.with(|pool| {
            pool.borrow_mut().clear();
        });
    }
}

/// Statistics about buffer pool usage
#[derive(Debug, Clone, Copy)]
pub struct QueryBufferPoolStats {
    pub row_buffers_pooled: usize,
    pub value_buffers_pooled: usize,
}

/// RAII guard for row buffers - automatically returns buffer to pool when dropped
pub struct RowBufferGuard {
    buffer: Option<Vec<Row>>,
    pool: QueryBufferPool,
}

impl RowBufferGuard {
    /// Create a new guard wrapping a buffer
    pub fn new(buffer: Vec<Row>, pool: QueryBufferPool) -> Self {
        Self { buffer: Some(buffer), pool }
    }

    /// Take ownership of the buffer, consuming the guard without returning to pool
    pub fn take(mut self) -> Vec<Row> {
        self.buffer.take().expect("buffer already taken")
    }

    /// Get a reference to the buffer
    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &Vec<Row> {
        self.buffer.as_ref().expect("buffer already taken")
    }

    /// Get a mutable reference to the buffer
    #[allow(clippy::should_implement_trait)]
    pub fn as_mut(&mut self) -> &mut Vec<Row> {
        self.buffer.as_mut().expect("buffer already taken")
    }
}

impl Drop for RowBufferGuard {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.return_row_buffer(buffer);
        }
    }
}

/// RAII guard for value buffers - automatically returns buffer to pool when dropped
pub struct ValueBufferGuard {
    buffer: Option<Vec<SqlValue>>,
    pool: QueryBufferPool,
}

impl ValueBufferGuard {
    /// Create a new guard wrapping a buffer
    pub fn new(buffer: Vec<SqlValue>, pool: QueryBufferPool) -> Self {
        Self { buffer: Some(buffer), pool }
    }

    /// Take ownership of the buffer, consuming the guard without returning to pool
    pub fn take(mut self) -> Vec<SqlValue> {
        self.buffer.take().expect("buffer already taken")
    }

    /// Get a reference to the buffer
    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &Vec<SqlValue> {
        self.buffer.as_ref().expect("buffer already taken")
    }

    /// Get a mutable reference to the buffer
    #[allow(clippy::should_implement_trait)]
    pub fn as_mut(&mut self) -> &mut Vec<SqlValue> {
        self.buffer.as_mut().expect("buffer already taken")
    }
}

impl Drop for ValueBufferGuard {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.return_value_buffer(buffer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_buffer_pool_reuse() {
        let pool = QueryBufferPool::new();

        // Get a buffer
        let buffer = pool.get_row_buffer(10);
        assert!(buffer.capacity() >= 10);

        // Return it
        pool.return_row_buffer(buffer);

        // Get it again - should reuse the same allocation
        let buffer2 = pool.get_row_buffer(10);
        assert!(buffer2.capacity() >= 10);
    }

    #[test]
    fn test_value_buffer_pool_reuse() {
        let pool = QueryBufferPool::new();

        // Get a buffer
        let buffer = pool.get_value_buffer(5);
        assert!(buffer.capacity() >= 5);

        // Return it
        pool.return_value_buffer(buffer);

        // Get it again - should reuse
        let buffer2 = pool.get_value_buffer(5);
        assert!(buffer2.capacity() >= 5);
    }

    #[test]
    fn test_buffer_guard_auto_return() {
        let pool = QueryBufferPool::new();

        {
            let buffer = pool.get_row_buffer(10);
            let _guard = RowBufferGuard::new(buffer, pool);
            // Guard dropped here, should return buffer to pool
        }

        let stats = pool.stats();
        assert_eq!(stats.row_buffers_pooled, 1);
    }

    #[test]
    fn test_pool_max_size() {
        let pool = QueryBufferPool::new();

        // Add more than MAX_POOLED_BUFFERS
        for _ in 0..(MAX_POOLED_BUFFERS + 10) {
            let buffer = Vec::with_capacity(10);
            pool.return_row_buffer(buffer);
        }

        let stats = pool.stats();
        assert_eq!(stats.row_buffers_pooled, MAX_POOLED_BUFFERS);
    }

    #[test]
    fn test_concurrent_access_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let pool = Arc::new(QueryBufferPool::new());
        let mut handles = vec![];

        // Spawn multiple threads that access the pool concurrently
        for _ in 0..4 {
            let pool = Arc::clone(&pool);
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    let buffer = pool.get_row_buffer(10);
                    assert!(buffer.capacity() >= 10);
                    pool.return_row_buffer(buffer);
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Each thread has its own pool, so stats show only current thread's pool
        let stats = pool.stats();
        assert!(stats.row_buffers_pooled <= MAX_POOLED_BUFFERS);
    }
}
