// ============================================================================
// SharedDatabase - Thread-Safe Database Wrapper for Concurrent Access
// ============================================================================
//
// This module provides a thread-safe wrapper around Database that enables
// concurrent read queries while maintaining exclusive access for writes.

use super::core::Database;
use crate::{Row, StorageError};
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(target_arch = "wasm32")]
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// A thread-safe wrapper around `Database` that enables concurrent read queries.
///
/// `SharedDatabase` wraps a `Database` in an `Arc<RwLock<>>`, allowing:
/// - **Multiple concurrent readers**: SELECT queries can execute simultaneously
/// - **Exclusive writer**: INSERT/UPDATE/DELETE require exclusive access
///
/// # Usage
///
/// ## Creating a SharedDatabase
///
/// ```rust
/// use vibesql_storage::{Database, SharedDatabase};
///
/// // Create from an existing database
/// let db = Database::new();
/// let shared_db = SharedDatabase::new(db);
///
/// // Or create with default configuration
/// let shared_db = SharedDatabase::default();
/// ```
///
/// ## Concurrent Read Queries
///
/// ```rust,ignore
/// use std::thread;
/// use vibesql_storage::SharedDatabase;
///
/// let shared_db = SharedDatabase::default();
/// // ... set up tables and data ...
///
/// // Clone the Arc for sharing across threads
/// let db1 = shared_db.clone();
/// let db2 = shared_db.clone();
///
/// // Execute queries concurrently
/// let handle1 = thread::spawn(move || {
///     let db = db1.read();
///     // Execute SELECT query using db reference
/// });
///
/// let handle2 = thread::spawn(move || {
///     let db = db2.read();
///     // Execute another SELECT query concurrently
/// });
///
/// handle1.join().unwrap();
/// handle2.join().unwrap();
/// ```
///
/// ## Write Operations
///
/// ```rust,ignore
/// let shared_db = SharedDatabase::default();
///
/// // Acquire exclusive write access
/// {
///     let mut db = shared_db.write();
///     db.create_table(schema)?;
///     db.insert_row("table", row)?;
/// } // Write lock released here
/// ```
///
/// # Performance Considerations
///
/// - Read operations can proceed in parallel without contention
/// - Write operations block all other operations until complete
/// - For write-heavy workloads, consider batching writes to minimize lock contention
/// - Clone operations are cheap (just incrementing an Arc reference count)
#[derive(Clone)]
pub struct SharedDatabase {
    inner: Arc<RwLock<Database>>,
}

impl SharedDatabase {
    /// Create a new SharedDatabase from an existing Database.
    ///
    /// This takes ownership of the Database and wraps it for concurrent access.
    pub fn new(db: Database) -> Self {
        Self {
            inner: Arc::new(RwLock::new(db)),
        }
    }

    /// Acquire a read lock for concurrent read access.
    ///
    /// Multiple threads can hold read locks simultaneously, enabling
    /// concurrent SELECT query execution.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let shared_db = SharedDatabase::default();
    /// let db = shared_db.read();
    /// let table = db.get_table("users");
    /// ```
    #[inline]
    #[cfg(not(target_arch = "wasm32"))]
    pub fn read(&self) -> RwLockReadGuard<'_, Database> {
        self.inner.read()
    }

    #[inline]
    #[cfg(target_arch = "wasm32")]
    pub fn read(&self) -> RwLockReadGuard<'_, Database> {
        self.inner.read().expect("RwLock poisoned")
    }

    /// Try to acquire a read lock without blocking.
    ///
    /// Returns `Some(guard)` if the lock was acquired, `None` if a writer
    /// currently holds the lock.
    #[inline]
    #[cfg(not(target_arch = "wasm32"))]
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, Database>> {
        self.inner.try_read()
    }

    #[inline]
    #[cfg(target_arch = "wasm32")]
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, Database>> {
        self.inner.try_read().ok()
    }

    /// Acquire a write lock for exclusive access.
    ///
    /// Only one thread can hold a write lock at a time. This blocks all
    /// readers and other writers.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let shared_db = SharedDatabase::default();
    /// {
    ///     let mut db = shared_db.write();
    ///     db.insert_row("users", row)?;
    /// }
    /// ```
    #[inline]
    #[cfg(not(target_arch = "wasm32"))]
    pub fn write(&self) -> RwLockWriteGuard<'_, Database> {
        self.inner.write()
    }

    #[inline]
    #[cfg(target_arch = "wasm32")]
    pub fn write(&self) -> RwLockWriteGuard<'_, Database> {
        self.inner.write().expect("RwLock poisoned")
    }

    /// Try to acquire a write lock without blocking.
    ///
    /// Returns `Some(guard)` if the lock was acquired, `None` if another
    /// thread currently holds any lock (read or write).
    #[inline]
    #[cfg(not(target_arch = "wasm32"))]
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, Database>> {
        self.inner.try_write()
    }

    #[inline]
    #[cfg(target_arch = "wasm32")]
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, Database>> {
        self.inner.try_write().ok()
    }

    /// Get the underlying Arc for sharing.
    ///
    /// This is useful when you need to share the database across threads
    /// using your own synchronization strategy.
    #[inline]
    pub fn inner(&self) -> &Arc<RwLock<Database>> {
        &self.inner
    }

    /// Check if this is the only reference to the database.
    ///
    /// Returns true if this SharedDatabase is the only reference,
    /// meaning it's safe to unwrap and take ownership of the inner Database.
    #[inline]
    pub fn is_unique(&self) -> bool {
        Arc::strong_count(&self.inner) == 1
    }

    /// Try to unwrap the SharedDatabase and return the inner Database.
    ///
    /// This only succeeds if this is the only reference. If there are
    /// other clones, returns `Err(self)`.
    pub fn try_unwrap(self) -> Result<Database, Self> {
        match Arc::try_unwrap(self.inner) {
            Ok(lock) => {
                #[cfg(not(target_arch = "wasm32"))]
                {
                    Ok(lock.into_inner())
                }
                #[cfg(target_arch = "wasm32")]
                {
                    Ok(lock.into_inner().expect("RwLock poisoned"))
                }
            }
            Err(arc) => Err(Self { inner: arc }),
        }
    }
}

impl Default for SharedDatabase {
    fn default() -> Self {
        Self::new(Database::new())
    }
}

impl std::fmt::Debug for SharedDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedDatabase")
            .field("ref_count", &Arc::strong_count(&self.inner))
            .finish()
    }
}

/// Convenience methods for common database operations.
///
/// These methods acquire the appropriate lock internally, making it easier
/// to perform simple operations without explicit lock management.
impl SharedDatabase {
    /// Execute a read-only query using a closure.
    ///
    /// This is a convenience method that acquires a read lock, executes
    /// the closure, and returns the result.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let row_count = shared_db.with_read(|db| {
    ///     db.get_table("users").map(|t| t.row_count()).unwrap_or(0)
    /// });
    /// ```
    #[inline]
    pub fn with_read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Database) -> R,
    {
        let guard = self.read();
        f(&guard)
    }

    /// Execute a write operation using a closure.
    ///
    /// This is a convenience method that acquires a write lock, executes
    /// the closure, and returns the result.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// shared_db.with_write(|db| {
    ///     db.insert_row("users", row)
    /// })?;
    /// ```
    #[inline]
    pub fn with_write<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Database) -> R,
    {
        let mut guard = self.write();
        f(&mut guard)
    }

    /// Check if a table exists.
    ///
    /// Acquires a read lock internally.
    #[inline]
    pub fn table_exists(&self, name: &str) -> bool {
        self.with_read(|db| db.get_table(name).is_some())
    }

    /// Get the row count of a table.
    ///
    /// Returns `None` if the table doesn't exist.
    #[inline]
    pub fn table_row_count(&self, name: &str) -> Option<usize> {
        self.with_read(|db| db.get_table(name).map(|t| t.row_count()))
    }

    /// Insert a row into a table.
    ///
    /// Acquires a write lock internally.
    #[inline]
    pub fn insert_row(&self, table_name: &str, row: Row) -> Result<(), StorageError> {
        self.with_write(|db| db.insert_row(table_name, row))
    }

    /// Insert multiple rows into a table.
    ///
    /// Acquires a write lock internally. More efficient than calling
    /// `insert_row` multiple times.
    #[inline]
    pub fn insert_rows_batch(&self, table_name: &str, rows: Vec<Row>) -> Result<usize, StorageError> {
        self.with_write(|db| db.insert_rows_batch(table_name, rows))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    fn create_test_db() -> SharedDatabase {
        let mut db = Database::new();

        // Create users table
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ];
        let schema =
            TableSchema::with_primary_key("users".to_string(), columns, vec!["id".to_string()]);
        db.create_table(schema).unwrap();

        // Insert test data
        for i in 1..=3 {
            let row = Row::new(vec![
                SqlValue::Integer(i),
                SqlValue::Varchar(arcstr::ArcStr::from(format!("User {}", i))),
            ]);
            db.insert_row("users", row).unwrap();
        }

        SharedDatabase::new(db)
    }

    #[test]
    fn test_shared_database_read() {
        let shared_db = create_test_db();

        // Can read through the guard
        let db = shared_db.read();
        assert!(db.get_table("users").is_some());
        assert_eq!(db.get_table("users").unwrap().row_count(), 3);
    }

    #[test]
    fn test_shared_database_write() {
        let shared_db = create_test_db();

        // Can write through the guard
        {
            let mut db = shared_db.write();
            let row = Row::new(vec![
                SqlValue::Integer(4),
                SqlValue::Varchar(arcstr::ArcStr::from("User 4")),
            ]);
            db.insert_row("users", row).unwrap();
        }

        // Verify write succeeded
        let db = shared_db.read();
        assert_eq!(db.get_table("users").unwrap().row_count(), 4);
    }

    #[test]
    fn test_shared_database_with_read() {
        let shared_db = create_test_db();

        let count = shared_db.with_read(|db| {
            db.get_table("users").map(|t| t.row_count()).unwrap_or(0)
        });

        assert_eq!(count, 3);
    }

    #[test]
    fn test_shared_database_with_write() {
        let shared_db = create_test_db();

        shared_db.with_write(|db| {
            let row = Row::new(vec![
                SqlValue::Integer(4),
                SqlValue::Varchar(arcstr::ArcStr::from("User 4")),
            ]);
            db.insert_row("users", row).unwrap();
        });

        assert_eq!(shared_db.table_row_count("users"), Some(4));
    }

    #[test]
    fn test_shared_database_clone() {
        let shared_db = create_test_db();
        let cloned = shared_db.clone();

        // Both share the same underlying database
        assert!(!shared_db.is_unique());
        assert!(!cloned.is_unique());

        // Writes through one are visible in the other
        shared_db.with_write(|db| {
            let row = Row::new(vec![
                SqlValue::Integer(4),
                SqlValue::Varchar(arcstr::ArcStr::from("User 4")),
            ]);
            db.insert_row("users", row).unwrap();
        });

        assert_eq!(cloned.table_row_count("users"), Some(4));
    }

    #[test]
    fn test_shared_database_try_unwrap() {
        let shared_db = create_test_db();

        // With only one reference, try_unwrap succeeds
        let db = shared_db.try_unwrap().expect("Should succeed with single reference");
        assert!(db.get_table("users").is_some());
    }

    #[test]
    fn test_shared_database_try_unwrap_fails_with_clone() {
        let shared_db = create_test_db();
        let _cloned = shared_db.clone();

        // With multiple references, try_unwrap fails
        let result = shared_db.try_unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn test_shared_database_convenience_methods() {
        let shared_db = create_test_db();

        // table_exists
        assert!(shared_db.table_exists("users"));
        assert!(!shared_db.table_exists("nonexistent"));

        // table_row_count
        assert_eq!(shared_db.table_row_count("users"), Some(3));
        assert_eq!(shared_db.table_row_count("nonexistent"), None);

        // insert_row
        let row = Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar(arcstr::ArcStr::from("User 4")),
        ]);
        shared_db.insert_row("users", row).unwrap();
        assert_eq!(shared_db.table_row_count("users"), Some(4));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_shared_database_concurrent_reads() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;

        let shared_db = create_test_db();
        let read_count = Arc::new(AtomicUsize::new(0));

        // Spawn multiple reader threads
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let db = shared_db.clone();
                let counter = read_count.clone();
                thread::spawn(move || {
                    let guard = db.read();
                    // Simulate some work
                    let count = guard.get_table("users").map(|t| t.row_count()).unwrap_or(0);
                    counter.fetch_add(1, Ordering::SeqCst);
                    count
                })
            })
            .collect();

        // Wait for all readers
        for handle in handles {
            let count = handle.join().unwrap();
            assert_eq!(count, 3);
        }

        // All readers completed
        assert_eq!(read_count.load(Ordering::SeqCst), 4);
    }
}
