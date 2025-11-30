use std::sync::Arc;

use anyhow::Result;
use vibesql_executor::{PreparedStatement, PreparedStatementCache, PreparedStatementCacheStats};
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Session state for a database connection
pub struct Session {
    /// Database name
    #[allow(dead_code)]
    pub database: String,
    /// User name
    #[allow(dead_code)]
    pub user: String,
    /// Database instance
    db: Database,
    /// Transaction state
    #[allow(dead_code)]
    pub in_transaction: bool,
    /// Prepared statement cache for reduced parsing overhead
    stmt_cache: Arc<PreparedStatementCache>,
}

/// Simplified execution result for wire protocol
#[derive(Debug)]
pub enum ExecutionResult {
    Select {
        rows: Vec<Row>,
        columns: Vec<Column>,
    },
    Insert {
        rows_affected: usize,
    },
    Update {
        rows_affected: usize,
    },
    Delete {
        rows_affected: usize,
    },
    CreateTable,
    CreateIndex,
    CreateView,
    DropTable,
    DropIndex,
    DropView,
    Other {
        message: String,
    },
}

impl ExecutionResult {
    /// Get the statement type as a string for metrics
    pub fn statement_type(&self) -> &str {
        match self {
            ExecutionResult::Select { .. } => "SELECT",
            ExecutionResult::Insert { .. } => "INSERT",
            ExecutionResult::Update { .. } => "UPDATE",
            ExecutionResult::Delete { .. } => "DELETE",
            ExecutionResult::CreateTable => "CREATE_TABLE",
            ExecutionResult::CreateIndex => "CREATE_INDEX",
            ExecutionResult::CreateView => "CREATE_VIEW",
            ExecutionResult::DropTable => "DROP_TABLE",
            ExecutionResult::DropIndex => "DROP_INDEX",
            ExecutionResult::DropView => "DROP_VIEW",
            ExecutionResult::Other { .. } => "OTHER",
        }
    }

    /// Get the number of rows affected
    pub fn rows_affected(&self) -> u64 {
        match self {
            ExecutionResult::Select { rows, .. } => rows.len() as u64,
            ExecutionResult::Insert { rows_affected } => *rows_affected as u64,
            ExecutionResult::Update { rows_affected } => *rows_affected as u64,
            ExecutionResult::Delete { rows_affected } => *rows_affected as u64,
            _ => 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<vibesql_types::SqlValue>,
}

impl Session {
    /// Create a new session
    pub fn new(database: String, user: String) -> Result<Self> {
        let db = Database::new();

        Ok(Self {
            database,
            user,
            db,
            in_transaction: false,
            stmt_cache: Arc::new(PreparedStatementCache::default_cache()),
        })
    }

    /// Create a new session with a shared statement cache
    #[allow(dead_code)]
    pub fn with_cache(
        database: String,
        user: String,
        cache: Arc<PreparedStatementCache>,
    ) -> Result<Self> {
        let db = Database::new();

        Ok(Self {
            database,
            user,
            db,
            in_transaction: false,
            stmt_cache: cache,
        })
    }

    /// Prepare a SQL statement for repeated execution
    ///
    /// The statement is parsed once and cached. Subsequent calls with the same
    /// SQL will return the cached prepared statement without re-parsing.
    ///
    /// # Example
    /// ```ignore
    /// let stmt = session.prepare("SELECT * FROM users WHERE id = ?")?;
    /// let result = session.execute_prepared(&stmt, &[SqlValue::Integer(1)])?;
    /// ```
    #[allow(dead_code)]
    pub fn prepare(&self, sql: &str) -> Result<Arc<PreparedStatement>> {
        self.stmt_cache.get_or_prepare(sql).map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// Execute a prepared statement with parameters
    ///
    /// Binds the provided parameters to the prepared statement and executes it.
    /// This avoids the parsing overhead of the original SQL.
    #[allow(dead_code)]
    pub fn execute_prepared(
        &mut self,
        stmt: &PreparedStatement,
        params: &[SqlValue],
    ) -> Result<ExecutionResult> {
        // Bind parameters to get executable statement
        let bound_stmt = stmt.bind(params).map_err(|e| anyhow::anyhow!("{}", e))?;

        // Execute the bound statement
        self.execute_statement(&bound_stmt)
    }

    /// Execute a SQL query with auto-caching
    ///
    /// This method automatically caches parsed statements for performance.
    /// For repeated queries, use `prepare()` + `execute_prepared()` for best performance.
    pub fn execute(&mut self, sql: &str) -> Result<ExecutionResult> {
        // Try to get from cache or prepare
        let prepared = self.stmt_cache.get_or_prepare(sql).map_err(|e| anyhow::anyhow!("{}", e))?;

        // For non-parameterized queries, execute directly from cached AST
        self.execute_statement(prepared.statement())
    }

    /// Execute a SQL query with parameters (convenience method)
    ///
    /// Combines prepare + execute_prepared in one call.
    #[allow(dead_code)]
    pub fn execute_with_params(&mut self, sql: &str, params: &[SqlValue]) -> Result<ExecutionResult> {
        let prepared = self.prepare(sql)?;
        self.execute_prepared(&prepared, params)
    }

    /// Execute a parsed statement
    fn execute_statement(&mut self, statement: &vibesql_ast::Statement) -> Result<ExecutionResult> {
        match statement {
            vibesql_ast::Statement::Select(select_stmt) => {
                let executor = vibesql_executor::SelectExecutor::new(&self.db);
                let rows = executor.execute(select_stmt)?;

                // Convert to our result format
                let result_rows: Vec<Row> =
                    rows.iter().map(|r| Row { values: r.values.clone() }).collect();

                // TODO: Get actual column names from select statement
                let columns = if !rows.is_empty() {
                    (0..rows[0].values.len()).map(|i| Column { name: format!("col{}", i) }).collect()
                } else {
                    Vec::new()
                };

                Ok(ExecutionResult::Select { rows: result_rows, columns })
            }

            vibesql_ast::Statement::Insert(insert_stmt) => {
                let affected = vibesql_executor::InsertExecutor::execute(&mut self.db, insert_stmt)?;
                // Invalidate cache for modified table
                self.stmt_cache.invalidate_table(&insert_stmt.table_name);
                Ok(ExecutionResult::Insert { rows_affected: affected })
            }

            vibesql_ast::Statement::Update(update_stmt) => {
                let affected = vibesql_executor::UpdateExecutor::execute(update_stmt, &mut self.db)?;
                // Invalidate cache for modified table
                self.stmt_cache.invalidate_table(&update_stmt.table_name);
                Ok(ExecutionResult::Update { rows_affected: affected })
            }

            vibesql_ast::Statement::Delete(delete_stmt) => {
                let affected = vibesql_executor::DeleteExecutor::execute(delete_stmt, &mut self.db)?;
                // Invalidate cache for modified table
                self.stmt_cache.invalidate_table(&delete_stmt.table_name);
                Ok(ExecutionResult::Delete { rows_affected: affected })
            }

            vibesql_ast::Statement::CreateTable(create_stmt) => {
                vibesql_executor::CreateTableExecutor::execute(create_stmt, &mut self.db)?;
                Ok(ExecutionResult::CreateTable)
            }

            vibesql_ast::Statement::CreateIndex(index_stmt) => {
                vibesql_executor::CreateIndexExecutor::execute(index_stmt, &mut self.db)?;
                Ok(ExecutionResult::CreateIndex)
            }

            vibesql_ast::Statement::CreateView(view_stmt) => {
                vibesql_executor::advanced_objects::execute_create_view(view_stmt, &mut self.db)?;
                Ok(ExecutionResult::CreateView)
            }

            vibesql_ast::Statement::DropTable(drop_stmt) => {
                vibesql_executor::DropTableExecutor::execute(drop_stmt, &mut self.db)?;
                // Invalidate cache for dropped table
                self.stmt_cache.invalidate_table(&drop_stmt.table_name);
                Ok(ExecutionResult::DropTable)
            }

            vibesql_ast::Statement::DropIndex(drop_stmt) => {
                vibesql_executor::DropIndexExecutor::execute(drop_stmt, &mut self.db)?;
                Ok(ExecutionResult::DropIndex)
            }

            vibesql_ast::Statement::DropView(drop_stmt) => {
                vibesql_executor::advanced_objects::execute_drop_view(drop_stmt, &mut self.db)?;
                Ok(ExecutionResult::DropView)
            }

            _ => {
                // For now, return a generic success for other statements
                Ok(ExecutionResult::Other { message: "Command completed successfully".to_string() })
            }
        }
    }

    /// Get prepared statement cache statistics
    #[allow(dead_code)]
    pub fn cache_stats(&self) -> PreparedStatementCacheStats {
        self.stmt_cache.stats()
    }

    /// Clear the prepared statement cache
    #[allow(dead_code)]
    pub fn clear_cache(&self) {
        self.stmt_cache.clear();
    }

    /// Begin a transaction
    #[allow(dead_code)]
    pub fn begin_transaction(&mut self) -> Result<()> {
        if self.in_transaction {
            return Err(anyhow::anyhow!("Already in transaction"));
        }
        self.in_transaction = true;
        Ok(())
    }

    /// Commit the current transaction
    #[allow(dead_code)]
    pub fn commit(&mut self) -> Result<()> {
        if !self.in_transaction {
            return Err(anyhow::anyhow!("No transaction in progress"));
        }
        self.in_transaction = false;
        Ok(())
    }

    /// Rollback the current transaction
    #[allow(dead_code)]
    pub fn rollback(&mut self) -> Result<()> {
        if !self.in_transaction {
            return Err(anyhow::anyhow!("No transaction in progress"));
        }
        self.in_transaction = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_creation() {
        let session = Session::new("testdb".to_string(), "testuser".to_string());
        assert!(session.is_ok());
        let session = session.unwrap();
        assert_eq!(session.database, "testdb");
        assert_eq!(session.user, "testuser");
        assert!(!session.in_transaction);
    }

    #[test]
    fn test_transaction_state() {
        let mut session = Session::new("testdb".to_string(), "testuser".to_string()).unwrap();

        // Not in transaction initially
        assert!(!session.in_transaction);

        // Begin transaction
        assert!(session.begin_transaction().is_ok());
        assert!(session.in_transaction);

        // Can't begin twice
        assert!(session.begin_transaction().is_err());

        // Commit
        assert!(session.commit().is_ok());
        assert!(!session.in_transaction);

        // Can't commit when not in transaction
        assert!(session.commit().is_err());
    }

    #[test]
    fn test_prepare_and_execute() {
        let mut session = Session::new("testdb".to_string(), "testuser".to_string()).unwrap();

        // Create a table first
        session.execute("CREATE TABLE users (id INT, name VARCHAR(100))").unwrap();

        // Prepare a statement (note: parser doesn't support ?, so use literal value)
        let stmt = session.prepare("SELECT * FROM users WHERE id = 1").unwrap();
        assert_eq!(stmt.param_count(), 0);

        // Execute the prepared statement
        let result = session.execute_prepared(&stmt, &[]);
        assert!(result.is_ok());

        // Verify we get a Select result
        match result.unwrap() {
            ExecutionResult::Select { .. } => (),
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_cache_hit() {
        let session = Session::new("testdb".to_string(), "testuser".to_string()).unwrap();

        // First prepare - cache miss
        let _stmt1 = session.prepare("SELECT 1").unwrap();
        let stats = session.cache_stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 0);

        // Second prepare - cache hit
        let _stmt2 = session.prepare("SELECT 1").unwrap();
        let stats = session.cache_stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 1);
    }

    #[test]
    fn test_auto_caching_in_execute() {
        let mut session = Session::new("testdb".to_string(), "testuser".to_string()).unwrap();

        // First execute - cache miss
        session.execute("SELECT 1").unwrap();
        let stats = session.cache_stats();
        assert_eq!(stats.misses, 1);

        // Second execute - cache hit
        session.execute("SELECT 1").unwrap();
        let stats = session.cache_stats();
        assert_eq!(stats.hits, 1);
    }
}
