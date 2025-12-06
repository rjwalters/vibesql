//! Database connection implementation
//!
//! This module provides the Database class for establishing connections
//! to the in-memory vibesql database following DB-API 2.0 conventions.

use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::prelude::*;

use crate::cursor::Cursor;

/// Database connection object
///
/// Represents a connection to an in-memory vibesql database.
/// Follows DB-API 2.0 conventions for database connections.
///
/// # Example
/// ```python
/// db = vibesql.connect()
/// cursor = db.cursor()
/// cursor.execute("SELECT 1")
/// ```
#[pyclass]
pub struct Database {
    pub(crate) db: Arc<Mutex<vibesql_storage::Database>>,
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl Database {
    /// Create a new database connection
    ///
    /// # Returns
    /// A new Database instance with an empty in-memory database.
    #[new]
    pub fn new() -> Self {
        Database { db: Arc::new(Mutex::new(vibesql_storage::Database::new())) }
    }

    /// Create a cursor for executing queries
    ///
    /// # Returns
    /// A new Cursor object for executing SQL statements and fetching results.
    fn cursor(&self) -> PyResult<Cursor> {
        Cursor::new(Arc::clone(&self.db))
    }

    /// Close the database connection
    ///
    /// For in-memory databases, this is a no-op but provided for DB-API 2.0 compatibility.
    fn close(&self) -> PyResult<()> {
        // In-memory database, no cleanup needed
        Ok(())
    }

    /// Commit the current transaction
    ///
    /// For in-memory databases without transaction support, this is a no-op
    /// but provided for DB-API 2.0 compatibility.
    fn commit(&self) -> PyResult<()> {
        // In-memory database, no transaction needed
        Ok(())
    }

    /// Get version string
    ///
    /// # Returns
    /// A string containing the version identifier.
    fn version(&self) -> String {
        "vibesql-py 0.1.0".to_string()
    }

    /// Save database to SQL dump file
    ///
    /// Generates a SQL dump file containing all schemas, tables, indexes,
    /// roles, and data needed to recreate the current database state.
    ///
    /// # Arguments
    /// * `path` - Path where the SQL dump file will be created
    ///
    /// # Example
    /// ```python
    /// db = vibesql.connect()
    /// cursor = db.cursor()
    /// cursor.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    /// cursor.execute("INSERT INTO users VALUES (1, 'Alice')")
    /// db.save("mydata.vbsql")
    /// ```
    fn save(&self, path: &str) -> PyResult<()> {
        // Use binary format to preserve sequences and column defaults (for AUTOINCREMENT)
        self.db.lock().save(path).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Failed to save database: {}", e))
        })
    }

    /// Load database from binary file
    ///
    /// Creates a new Database instance by loading binary database file.
    /// This preserves sequences and column defaults for AUTOINCREMENT support.
    /// This is a static method that returns a new Database.
    ///
    /// # Arguments
    /// * `path` - Path to the binary database file (.vbsql)
    ///
    /// # Returns
    /// A new Database instance with the loaded state
    ///
    /// # Example
    /// ```python
    /// # Save a database
    /// db1 = vibesql.connect()
    /// cursor = db1.cursor()
    /// cursor.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    /// db1.save("mydata.vbsql")
    ///
    /// # Load it later
    /// db2 = vibesql.Database.load("mydata.vbsql")
    /// cursor2 = db2.cursor()
    /// cursor2.execute("SELECT * FROM users")
    /// ```
    #[staticmethod]
    fn load(path: &str) -> PyResult<Database> {
        // Use default format (compressed or uncompressed binary) to preserve
        // sequences and column defaults (for AUTOINCREMENT)
        let db = vibesql_storage::Database::load(path).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Failed to load database: {}", e))
        })?;
        Ok(Database { db: Arc::new(Mutex::new(db)) })
    }
}
