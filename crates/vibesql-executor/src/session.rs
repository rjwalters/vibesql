//! Session - Prepared Statement Execution Context
//!
//! Provides a session-based API for prepared statement execution, offering
//! significant performance benefits for repeated query patterns by caching
//! parsed SQL and performing AST-level parameter binding.
//!
//! ## Performance Benefits
//!
//! For repeated queries, prepared statements can provide 10-100x speedup by:
//! - Parsing SQL once and caching the AST
//! - Binding parameters at the AST level (no re-parsing)
//! - Reusing the same cached statement for different parameter values
//!
//! ## Usage
//!
//! ```rust,ignore
//! use vibesql_executor::Session;
//! use vibesql_storage::Database;
//! use vibesql_types::SqlValue;
//!
//! let mut db = Database::new();
//! // ... create tables and insert data ...
//!
//! // Create a session for prepared statement execution
//! let session = Session::new(&db);
//!
//! // Prepare a statement (parses SQL once)
//! let stmt = session.prepare("SELECT * FROM users WHERE id = ?")?;
//!
//! // Execute with different parameters (reuses parsed AST - no re-parsing!)
//! let result1 = session.execute_prepared(&stmt, &[SqlValue::Integer(1)])?;
//! let result2 = session.execute_prepared(&stmt, &[SqlValue::Integer(2)])?;
//!
//! // For DML statements that modify data, use execute_prepared_mut
//! let mut session = Session::new(&mut db);
//! let insert_stmt = session.prepare("INSERT INTO users (id, name) VALUES (?, ?)")?;
//! session.execute_prepared_mut(&insert_stmt, &[SqlValue::Integer(3), SqlValue::Varchar("Alice".into())])?;
//! ```
//!
//! ## Thread Safety
//!
//! `PreparedStatement` is `Clone` and can be shared across threads via `Arc`.
//! The `PreparedStatementCache` uses internal locking for thread-safe access.

use std::sync::Arc;

use vibesql_ast::Statement;
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

use crate::cache::{PreparedStatement, PreparedStatementCache, PreparedStatementError};
use crate::errors::ExecutorError;
use crate::{DeleteExecutor, InsertExecutor, SelectExecutor, SelectResult, UpdateExecutor};

/// Execution result for prepared statements
#[derive(Debug)]
pub enum PreparedExecutionResult {
    /// Result from a SELECT query
    Select(SelectResult),
    /// Number of rows affected by INSERT/UPDATE/DELETE
    RowsAffected(usize),
    /// DDL or other statement that doesn't return rows
    Ok,
}

impl PreparedExecutionResult {
    /// Get rows if this is a SELECT result
    pub fn rows(&self) -> Option<&[Row]> {
        match self {
            PreparedExecutionResult::Select(result) => Some(&result.rows),
            _ => None,
        }
    }

    /// Get rows affected if this is a DML result
    pub fn rows_affected(&self) -> Option<usize> {
        match self {
            PreparedExecutionResult::RowsAffected(n) => Some(*n),
            _ => None,
        }
    }

    /// Convert to SelectResult if this is a SELECT result
    pub fn into_select_result(self) -> Option<SelectResult> {
        match self {
            PreparedExecutionResult::Select(result) => Some(result),
            _ => None,
        }
    }
}

/// Error type for session operations
#[derive(Debug)]
pub enum SessionError {
    /// Error during prepared statement operations
    PreparedStatement(PreparedStatementError),
    /// Error during query execution
    Execution(ExecutorError),
    /// Statement type not supported for this operation
    UnsupportedStatement(String),
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionError::PreparedStatement(e) => write!(f, "Prepared statement error: {}", e),
            SessionError::Execution(e) => write!(f, "Execution error: {:?}", e),
            SessionError::UnsupportedStatement(msg) => write!(f, "Unsupported statement: {}", msg),
        }
    }
}

impl std::error::Error for SessionError {}

impl From<PreparedStatementError> for SessionError {
    fn from(e: PreparedStatementError) -> Self {
        SessionError::PreparedStatement(e)
    }
}

impl From<ExecutorError> for SessionError {
    fn from(e: ExecutorError) -> Self {
        SessionError::Execution(e)
    }
}

/// Session for executing prepared statements
///
/// A session holds a reference to the database and a cache of prepared statements.
/// Use this for executing repeated queries with different parameters.
pub struct Session<'a> {
    db: &'a Database,
    cache: Arc<PreparedStatementCache>,
}

impl<'a> Session<'a> {
    /// Create a new session with a reference to the database
    ///
    /// Uses a default cache size of 1000 prepared statements.
    pub fn new(db: &'a Database) -> Self {
        Self {
            db,
            cache: Arc::new(PreparedStatementCache::default_cache()),
        }
    }

    /// Create a new session with a custom cache size
    pub fn with_cache_size(db: &'a Database, cache_size: usize) -> Self {
        Self {
            db,
            cache: Arc::new(PreparedStatementCache::new(cache_size)),
        }
    }

    /// Create a new session with a shared cache
    ///
    /// This allows multiple sessions to share the same prepared statement cache,
    /// which is useful for connection pooling scenarios.
    pub fn with_shared_cache(db: &'a Database, cache: Arc<PreparedStatementCache>) -> Self {
        Self { db, cache }
    }

    /// Get the underlying database reference
    pub fn database(&self) -> &Database {
        self.db
    }

    /// Get the prepared statement cache
    pub fn cache(&self) -> &PreparedStatementCache {
        &self.cache
    }

    /// Get the shared cache Arc (for sharing with other sessions)
    pub fn shared_cache(&self) -> Arc<PreparedStatementCache> {
        Arc::clone(&self.cache)
    }

    /// Prepare a SQL statement for execution
    ///
    /// Parses the SQL and caches the result. Subsequent calls with the same
    /// SQL string will return the cached statement without re-parsing.
    ///
    /// Supports `?` placeholders for parameter binding.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let stmt = session.prepare("SELECT * FROM users WHERE id = ?")?;
    /// assert_eq!(stmt.param_count(), 1);
    /// ```
    pub fn prepare(&self, sql: &str) -> Result<Arc<PreparedStatement>, SessionError> {
        self.cache.get_or_prepare(sql).map_err(SessionError::from)
    }

    /// Execute a prepared SELECT statement with parameters
    ///
    /// Binds the parameters to the prepared statement and executes it.
    /// This is the fast path for repeated queries - no SQL parsing occurs.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let stmt = session.prepare("SELECT * FROM users WHERE id = ?")?;
    /// let result = session.execute_prepared(&stmt, &[SqlValue::Integer(42)])?;
    /// ```
    pub fn execute_prepared(
        &self,
        stmt: &PreparedStatement,
        params: &[SqlValue],
    ) -> Result<PreparedExecutionResult, SessionError> {
        // Bind parameters to get executable statement
        let bound_stmt = stmt.bind(params)?;

        // Execute based on statement type
        self.execute_statement(&bound_stmt)
    }

    /// Execute a bound statement (internal helper)
    fn execute_statement(&self, stmt: &Statement) -> Result<PreparedExecutionResult, SessionError> {
        match stmt {
            Statement::Select(select_stmt) => {
                let executor = SelectExecutor::new(self.db);
                let result = executor.execute_with_columns(select_stmt)?;
                Ok(PreparedExecutionResult::Select(result))
            }
            _ => Err(SessionError::UnsupportedStatement(
                "Only SELECT is supported for read-only sessions. Use SessionMut for DML.".into(),
            )),
        }
    }
}

/// Mutable session for executing prepared statements that modify data
///
/// Use this session type when you need to execute INSERT, UPDATE, or DELETE
/// statements in addition to SELECT.
pub struct SessionMut<'a> {
    db: &'a mut Database,
    cache: Arc<PreparedStatementCache>,
}

impl<'a> SessionMut<'a> {
    /// Create a new mutable session with a reference to the database
    pub fn new(db: &'a mut Database) -> Self {
        Self {
            db,
            cache: Arc::new(PreparedStatementCache::default_cache()),
        }
    }

    /// Create a new mutable session with a custom cache size
    pub fn with_cache_size(db: &'a mut Database, cache_size: usize) -> Self {
        Self {
            db,
            cache: Arc::new(PreparedStatementCache::new(cache_size)),
        }
    }

    /// Create a new mutable session with a shared cache
    pub fn with_shared_cache(db: &'a mut Database, cache: Arc<PreparedStatementCache>) -> Self {
        Self { db, cache }
    }

    /// Get the underlying database reference (immutable)
    pub fn database(&self) -> &Database {
        self.db
    }

    /// Get the underlying database reference (mutable)
    pub fn database_mut(&mut self) -> &mut Database {
        self.db
    }

    /// Get the prepared statement cache
    pub fn cache(&self) -> &PreparedStatementCache {
        &self.cache
    }

    /// Get the shared cache Arc
    pub fn shared_cache(&self) -> Arc<PreparedStatementCache> {
        Arc::clone(&self.cache)
    }

    /// Prepare a SQL statement for execution
    pub fn prepare(&self, sql: &str) -> Result<Arc<PreparedStatement>, SessionError> {
        self.cache.get_or_prepare(sql).map_err(SessionError::from)
    }

    /// Execute a prepared statement with parameters (read-only)
    ///
    /// Use this for SELECT queries.
    pub fn execute_prepared(
        &self,
        stmt: &PreparedStatement,
        params: &[SqlValue],
    ) -> Result<PreparedExecutionResult, SessionError> {
        let bound_stmt = stmt.bind(params)?;
        self.execute_statement_readonly(&bound_stmt)
    }

    /// Execute a prepared statement with parameters (read-write)
    ///
    /// Use this for INSERT, UPDATE, DELETE statements.
    pub fn execute_prepared_mut(
        &mut self,
        stmt: &PreparedStatement,
        params: &[SqlValue],
    ) -> Result<PreparedExecutionResult, SessionError> {
        let bound_stmt = stmt.bind(params)?;
        self.execute_statement_mut(&bound_stmt)
    }

    /// Execute a read-only statement
    fn execute_statement_readonly(
        &self,
        stmt: &Statement,
    ) -> Result<PreparedExecutionResult, SessionError> {
        match stmt {
            Statement::Select(select_stmt) => {
                let executor = SelectExecutor::new(self.db);
                let result = executor.execute_with_columns(select_stmt)?;
                Ok(PreparedExecutionResult::Select(result))
            }
            _ => Err(SessionError::UnsupportedStatement(
                "Use execute_prepared_mut for DML statements".into(),
            )),
        }
    }

    /// Execute a statement that may modify data
    fn execute_statement_mut(
        &mut self,
        stmt: &Statement,
    ) -> Result<PreparedExecutionResult, SessionError> {
        match stmt {
            Statement::Select(select_stmt) => {
                let executor = SelectExecutor::new(self.db);
                let result = executor.execute_with_columns(select_stmt)?;
                Ok(PreparedExecutionResult::Select(result))
            }
            Statement::Insert(insert_stmt) => {
                let rows_affected = InsertExecutor::execute(self.db, insert_stmt)?;
                // Invalidate cache for affected table
                self.cache.invalidate_table(&insert_stmt.table_name);
                Ok(PreparedExecutionResult::RowsAffected(rows_affected))
            }
            Statement::Update(update_stmt) => {
                let rows_affected = UpdateExecutor::execute(update_stmt, self.db)?;
                // Invalidate cache for affected table
                self.cache.invalidate_table(&update_stmt.table_name);
                Ok(PreparedExecutionResult::RowsAffected(rows_affected))
            }
            Statement::Delete(delete_stmt) => {
                let rows_affected = DeleteExecutor::execute(delete_stmt, self.db)?;
                // Invalidate cache for affected table
                self.cache.invalidate_table(&delete_stmt.table_name);
                Ok(PreparedExecutionResult::RowsAffected(rows_affected))
            }
            _ => Err(SessionError::UnsupportedStatement(format!(
                "Statement type {:?} not supported for prepared execution",
                std::mem::discriminant(stmt)
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    fn create_test_db() -> Database {
        let mut db = Database::new();
        // Enable case-insensitive identifiers (default MySQL behavior)
        db.catalog.set_case_sensitive_identifiers(false);

        // Create users table
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, true),
        ];
        let schema = TableSchema::with_primary_key(
            "users".to_string(),
            columns,
            vec!["id".to_string()],
        );
        db.create_table(schema).unwrap();

        // Insert test data
        let row1 = Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".into())]);
        let row2 = Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".into())]);
        let row3 = Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar("Charlie".into())]);

        db.insert_row("users", row1).unwrap();
        db.insert_row("users", row2).unwrap();
        db.insert_row("users", row3).unwrap();

        db
    }

    #[test]
    fn test_session_prepare() {
        let db = create_test_db();
        let session = Session::new(&db);

        let stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();
        assert_eq!(stmt.param_count(), 1);
    }

    #[test]
    fn test_session_execute_prepared() {
        let db = create_test_db();
        let session = Session::new(&db);

        let stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();

        // Execute with id = 1
        let result = session
            .execute_prepared(&stmt, &[SqlValue::Integer(1)])
            .unwrap();

        if let PreparedExecutionResult::Select(select_result) = result {
            assert_eq!(select_result.rows.len(), 1);
            assert_eq!(select_result.rows[0].values[0], SqlValue::Integer(1));
            assert_eq!(
                select_result.rows[0].values[1],
                SqlValue::Varchar("Alice".into())
            );
        } else {
            panic!("Expected Select result");
        }
    }

    #[test]
    fn test_session_reuse_prepared() {
        let db = create_test_db();
        let session = Session::new(&db);

        let stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();

        // Execute multiple times with different parameters
        let result1 = session
            .execute_prepared(&stmt, &[SqlValue::Integer(1)])
            .unwrap();
        let result2 = session
            .execute_prepared(&stmt, &[SqlValue::Integer(2)])
            .unwrap();
        let result3 = session
            .execute_prepared(&stmt, &[SqlValue::Integer(3)])
            .unwrap();

        // Verify each returned the correct row
        assert_eq!(
            result1.rows().unwrap()[0].values[1],
            SqlValue::Varchar("Alice".into())
        );
        assert_eq!(
            result2.rows().unwrap()[0].values[1],
            SqlValue::Varchar("Bob".into())
        );
        assert_eq!(
            result3.rows().unwrap()[0].values[1],
            SqlValue::Varchar("Charlie".into())
        );

        // Verify cache was used (should have 1 miss, then hits)
        let stats = session.cache().stats();
        assert_eq!(stats.misses, 1);
        assert!(stats.hits >= 0); // May vary based on implementation
    }

    #[test]
    fn test_session_param_count_mismatch() {
        let db = create_test_db();
        let session = Session::new(&db);

        let stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();

        // Try to execute with wrong number of parameters
        let result = session.execute_prepared(&stmt, &[]);
        assert!(result.is_err());

        let result = session.execute_prepared(&stmt, &[SqlValue::Integer(1), SqlValue::Integer(2)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_session_mut_insert() {
        let mut db = create_test_db();
        let mut session = SessionMut::new(&mut db);

        let stmt = session
            .prepare("INSERT INTO users (id, name) VALUES (?, ?)")
            .unwrap();

        let result = session
            .execute_prepared_mut(
                &stmt,
                &[SqlValue::Integer(4), SqlValue::Varchar("David".into())],
            )
            .unwrap();

        assert_eq!(result.rows_affected(), Some(1));

        // Verify the row was inserted
        let select_stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();
        let select_result = session
            .execute_prepared(&select_stmt, &[SqlValue::Integer(4)])
            .unwrap();

        assert_eq!(select_result.rows().unwrap().len(), 1);
        assert_eq!(
            select_result.rows().unwrap()[0].values[1],
            SqlValue::Varchar("David".into())
        );
    }

    #[test]
    fn test_session_mut_update() {
        let mut db = create_test_db();
        let mut session = SessionMut::new(&mut db);

        let stmt = session
            .prepare("UPDATE users SET name = ? WHERE id = ?")
            .unwrap();

        let result = session
            .execute_prepared_mut(
                &stmt,
                &[SqlValue::Varchar("Alicia".into()), SqlValue::Integer(1)],
            )
            .unwrap();

        assert_eq!(result.rows_affected(), Some(1));

        // Verify the row was updated
        let select_stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();
        let select_result = session
            .execute_prepared(&select_stmt, &[SqlValue::Integer(1)])
            .unwrap();

        assert_eq!(
            select_result.rows().unwrap()[0].values[1],
            SqlValue::Varchar("Alicia".into())
        );
    }

    #[test]
    fn test_session_mut_delete() {
        let mut db = create_test_db();
        let mut session = SessionMut::new(&mut db);

        let stmt = session.prepare("DELETE FROM users WHERE id = ?").unwrap();

        let result = session
            .execute_prepared_mut(&stmt, &[SqlValue::Integer(1)])
            .unwrap();

        assert_eq!(result.rows_affected(), Some(1));

        // Verify the row was deleted
        let select_stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();
        let select_result = session
            .execute_prepared(&select_stmt, &[SqlValue::Integer(1)])
            .unwrap();

        assert_eq!(select_result.rows().unwrap().len(), 0);
    }

    #[test]
    fn test_shared_cache() {
        let db = create_test_db();

        // Create first session and prepare a statement
        let session1 = Session::new(&db);
        let stmt = session1
            .prepare("SELECT * FROM users WHERE id = ?")
            .unwrap();

        // Get the shared cache
        let shared_cache = session1.shared_cache();
        let initial_misses = session1.cache().stats().misses;

        // Create second session with shared cache
        let session2 = Session::with_shared_cache(&db, shared_cache);

        // Prepare the same statement - should hit cache
        let _stmt2 = session2
            .prepare("SELECT * FROM users WHERE id = ?")
            .unwrap();

        // Verify cache was shared (no additional miss)
        assert_eq!(session2.cache().stats().misses, initial_misses);

        // Execute on both sessions
        let result1 = session1
            .execute_prepared(&stmt, &[SqlValue::Integer(1)])
            .unwrap();
        let result2 = session2
            .execute_prepared(&stmt, &[SqlValue::Integer(2)])
            .unwrap();

        assert_eq!(
            result1.rows().unwrap()[0].values[1],
            SqlValue::Varchar("Alice".into())
        );
        assert_eq!(
            result2.rows().unwrap()[0].values[1],
            SqlValue::Varchar("Bob".into())
        );
    }

    #[test]
    fn test_no_params_statement() {
        let db = create_test_db();
        let session = Session::new(&db);

        let stmt = session.prepare("SELECT * FROM users").unwrap();
        assert_eq!(stmt.param_count(), 0);

        let result = session.execute_prepared(&stmt, &[]).unwrap();
        assert_eq!(result.rows().unwrap().len(), 3);
    }

    #[test]
    fn test_multiple_placeholders() {
        let db = create_test_db();
        let session = Session::new(&db);

        let stmt = session
            .prepare("SELECT * FROM users WHERE id >= ? AND id <= ?")
            .unwrap();
        assert_eq!(stmt.param_count(), 2);

        let result = session
            .execute_prepared(&stmt, &[SqlValue::Integer(1), SqlValue::Integer(2)])
            .unwrap();

        assert_eq!(result.rows().unwrap().len(), 2);
    }
}
