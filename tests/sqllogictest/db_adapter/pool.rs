//! Thread-local database pooling for test file reuse.
//!
//! This module provides a thread-local Database pool that allows reuse across test files
//! within the same worker thread. This avoids the overhead of creating a new Database for
//! each test file (622 files in full suite). Each worker thread gets its own cached Database
//! that is reset between files.

use std::cell::RefCell;
use vibesql_storage::Database;

// Thread-local Database pool for reuse across test files within the same worker thread.
// This avoids the overhead of creating a new Database for each test file (622 files in full suite).
// Each worker thread gets its own cached Database that is reset between files.
thread_local! {
    static DB_POOL: RefCell<Option<Database>> = RefCell::new(None);
}

/// Get a reset Database from the thread-local pool.
/// First call creates a new Database, subsequent calls reuse and reset the existing one.
/// Uses take/replace pattern to avoid cloning overhead.
pub fn get_pooled_database() -> Database {
    DB_POOL.with(|pool| {
        let mut pool_ref = pool.borrow_mut();
        match pool_ref.take() {
            Some(mut db) => {
                // Reuse existing database after resetting it (no clone)
                db.reset();
                db
            }
            None => {
                // First use - create new database with MySQL mode (default)
                // The dolthub/sqllogictest corpus was regenerated against MySQL 8.x
                // and expects MySQL semantics including decimal division
                vibesql_storage::Database::new()
            }
        }
    })
}

/// Return a Database to the thread-local pool for reuse.
/// Only returns if pool is empty to avoid conflicts with multiple instances.
pub fn return_to_pool(db: Database) {
    DB_POOL.with(|pool| {
        let mut pool_ref = pool.borrow_mut();
        if pool_ref.is_none() {
            *pool_ref = Some(db);
        }
    });
}
