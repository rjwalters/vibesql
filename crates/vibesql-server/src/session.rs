use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use vibesql_executor::{
    CursorExecutor, CursorStore, FetchResult as CursorFetchResult, PreparedStatement,
    PreparedStatementCache, PreparedStatementCacheStats,
};
use vibesql_types::SqlValue;

use crate::registry::SharedDatabase;
use crate::transaction::SessionTransactionManager;

/// Session state for a database connection
pub struct Session {
    /// Database name
    #[allow(dead_code)]
    pub database: String,
    /// User name
    #[allow(dead_code)]
    pub user: String,
    /// Shared database instance (shared across all connections to the same database)
    db: SharedDatabase,
    /// Prepared statement cache for reduced parsing overhead
    stmt_cache: Arc<PreparedStatementCache>,
    /// Named prepared statements (from PREPARE SQL syntax)
    named_statements: HashMap<String, Arc<PreparedStatement>>,
    /// Cursor storage for DECLARE/OPEN/FETCH/CLOSE operations
    cursors: CursorStore,
    /// Session-level transaction manager for READ COMMITTED isolation
    /// Tracks uncommitted changes that should only be visible to this session
    txn_manager: SessionTransactionManager,
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
    Analyze {
        tables_analyzed: usize,
    },
    /// Statement prepared successfully
    Prepare {
        statement_name: String,
    },
    /// Prepared statement deallocated
    Deallocate {
        statement_name: String,
    },
    /// Cursor declared successfully
    DeclareCursor {
        cursor_name: String,
    },
    /// Cursor opened successfully
    OpenCursor {
        cursor_name: String,
    },
    /// Cursor fetched rows
    Fetch {
        rows: Vec<Row>,
        columns: Vec<Column>,
    },
    /// Cursor closed successfully
    CloseCursor {
        cursor_name: String,
    },
    /// Transaction started
    Begin,
    /// Transaction committed
    Commit,
    /// Transaction rolled back
    Rollback,
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
            ExecutionResult::Analyze { .. } => "ANALYZE",
            ExecutionResult::Prepare { .. } => "PREPARE",
            ExecutionResult::Deallocate { .. } => "DEALLOCATE",
            ExecutionResult::DeclareCursor { .. } => "DECLARE_CURSOR",
            ExecutionResult::OpenCursor { .. } => "OPEN_CURSOR",
            ExecutionResult::Fetch { .. } => "FETCH",
            ExecutionResult::CloseCursor { .. } => "CLOSE_CURSOR",
            ExecutionResult::Begin => "BEGIN",
            ExecutionResult::Commit => "COMMIT",
            ExecutionResult::Rollback => "ROLLBACK",
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
            ExecutionResult::Fetch { rows, .. } => rows.len() as u64,
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
    /// Create a new session with a shared database
    ///
    /// This constructor is used for wire protocol connections where multiple
    /// connections to the same database should share data.
    pub fn new(database: String, user: String, db: SharedDatabase) -> Self {
        Self {
            database,
            user,
            db,
            stmt_cache: Arc::new(PreparedStatementCache::default_cache()),
            named_statements: HashMap::new(),
            cursors: CursorStore::new(),
            txn_manager: SessionTransactionManager::new(),
        }
    }

    /// Create a new standalone session with its own isolated database
    ///
    /// This constructor is used for HTTP API requests and other stateless
    /// operations where data isolation between requests is acceptable.
    pub fn new_standalone(database: String, user: String) -> Self {
        let db = Arc::new(tokio::sync::RwLock::new(vibesql_storage::Database::new()));
        Self::new(database, user, db)
    }

    /// Check if this session is currently in a transaction
    pub fn in_transaction(&self) -> bool {
        self.txn_manager.in_transaction()
    }

    /// Get the shared database handle
    pub fn shared_database(&self) -> &SharedDatabase {
        &self.db
    }

    /// Create a new session with a shared statement cache
    #[allow(dead_code)]
    pub fn with_cache(
        database: String,
        user: String,
        db: SharedDatabase,
        cache: Arc<PreparedStatementCache>,
    ) -> Self {
        Self {
            database,
            user,
            db,
            stmt_cache: cache,
            named_statements: HashMap::new(),
            cursors: CursorStore::new(),
            txn_manager: SessionTransactionManager::new(),
        }
    }

    /// Prepare a SQL statement for repeated execution
    ///
    /// The statement is parsed once and cached. Subsequent calls with the same
    /// SQL will return the cached prepared statement without re-parsing.
    ///
    /// # Example
    /// ```text
    /// let stmt = session.prepare("SELECT * FROM users WHERE id = ?")?;
    /// let result = session.execute_prepared(&stmt, &[SqlValue::Integer(1)]).await?;
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
    pub async fn execute_prepared(
        &mut self,
        stmt: &PreparedStatement,
        params: &[SqlValue],
    ) -> Result<ExecutionResult> {
        // Bind parameters to get executable statement
        let bound_stmt = stmt.bind(params).map_err(|e| anyhow::anyhow!("{}", e))?;

        // Execute the bound statement
        self.execute_statement(&bound_stmt).await
    }

    /// Execute a SQL query with auto-caching
    ///
    /// This method automatically caches parsed statements for performance.
    /// For repeated queries, use `prepare()` + `execute_prepared()` for best performance.
    pub async fn execute(&mut self, sql: &str) -> Result<ExecutionResult> {
        // Try to get from cache or prepare
        let prepared = self.stmt_cache.get_or_prepare(sql).map_err(|e| anyhow::anyhow!("{}", e))?;

        // For non-parameterized queries, execute directly from cached AST
        self.execute_statement(prepared.statement()).await
    }

    /// Execute a SQL query with parameters (convenience method)
    ///
    /// Combines prepare + execute_prepared in one call.
    #[allow(dead_code)]
    pub async fn execute_with_params(
        &mut self,
        sql: &str,
        params: &[SqlValue],
    ) -> Result<ExecutionResult> {
        let prepared = self.prepare(sql)?;
        self.execute_prepared(&prepared, params).await
    }

    /// Execute a parsed statement
    async fn execute_statement(
        &mut self,
        statement: &vibesql_ast::Statement,
    ) -> Result<ExecutionResult> {
        // Acquire write lock for most operations (read-only operations could use read lock,
        // but for simplicity we use write lock for all to avoid potential deadlocks)
        let mut db = self.db.write().await;

        match statement {
            vibesql_ast::Statement::Select(select_stmt) => {
                let executor = vibesql_executor::SelectExecutor::new(&db);
                let rows = executor.execute(select_stmt)?;

                // Convert to our result format
                let result_rows: Vec<Row> =
                    rows.iter().map(|r| Row { values: r.values.to_vec() }).collect();

                // TODO: Get actual column names from select statement
                let columns = if !rows.is_empty() {
                    (0..rows[0].values.len())
                        .map(|i| Column { name: format!("col{}", i) })
                        .collect()
                } else {
                    Vec::new()
                };

                Ok(ExecutionResult::Select { rows: result_rows, columns })
            }

            vibesql_ast::Statement::Insert(insert_stmt) => {
                let affected =
                    vibesql_executor::InsertExecutor::execute(&mut db, insert_stmt)?;
                // Invalidate cache for modified table
                self.stmt_cache.invalidate_table(&insert_stmt.table_name);
                Ok(ExecutionResult::Insert { rows_affected: affected })
            }

            vibesql_ast::Statement::Update(update_stmt) => {
                let affected =
                    vibesql_executor::UpdateExecutor::execute(update_stmt, &mut db)?;
                // Invalidate cache for modified table
                self.stmt_cache.invalidate_table(&update_stmt.table_name);
                Ok(ExecutionResult::Update { rows_affected: affected })
            }

            vibesql_ast::Statement::Delete(delete_stmt) => {
                let affected =
                    vibesql_executor::DeleteExecutor::execute(delete_stmt, &mut db)?;
                // Invalidate cache for modified table
                self.stmt_cache.invalidate_table(&delete_stmt.table_name);
                Ok(ExecutionResult::Delete { rows_affected: affected })
            }

            vibesql_ast::Statement::CreateTable(create_stmt) => {
                vibesql_executor::CreateTableExecutor::execute(create_stmt, &mut db)?;
                Ok(ExecutionResult::CreateTable)
            }

            vibesql_ast::Statement::CreateIndex(index_stmt) => {
                vibesql_executor::CreateIndexExecutor::execute(index_stmt, &mut db)?;
                Ok(ExecutionResult::CreateIndex)
            }

            vibesql_ast::Statement::CreateView(view_stmt) => {
                vibesql_executor::advanced_objects::execute_create_view(view_stmt, &mut db)?;
                Ok(ExecutionResult::CreateView)
            }

            vibesql_ast::Statement::DropTable(drop_stmt) => {
                vibesql_executor::DropTableExecutor::execute(drop_stmt, &mut db)?;
                // Invalidate cache for dropped table
                self.stmt_cache.invalidate_table(&drop_stmt.table_name);
                Ok(ExecutionResult::DropTable)
            }

            vibesql_ast::Statement::DropIndex(drop_stmt) => {
                vibesql_executor::DropIndexExecutor::execute(drop_stmt, &mut db)?;
                Ok(ExecutionResult::DropIndex)
            }

            vibesql_ast::Statement::DropView(drop_stmt) => {
                vibesql_executor::advanced_objects::execute_drop_view(drop_stmt, &mut db)?;
                Ok(ExecutionResult::DropView)
            }

            vibesql_ast::Statement::Analyze(analyze_stmt) => {
                let message =
                    vibesql_executor::AnalyzeExecutor::execute(analyze_stmt, &mut db)?;
                // Extract table count from message - the executor returns a message like
                // "ANALYZE completed - N table(s) analyzed"
                let tables_analyzed =
                    if analyze_stmt.table_name.is_some() { 1 } else { db.list_tables().len() };
                let _ = message; // Message is informational, we track count
                Ok(ExecutionResult::Analyze { tables_analyzed })
            }

            vibesql_ast::Statement::Prepare(prepare_stmt) => {
                // Release lock before calling helper (doesn't need db)
                drop(db);
                self.execute_prepare(prepare_stmt)
            }

            vibesql_ast::Statement::Execute(execute_stmt) => {
                // Release lock before calling helper (will reacquire if needed)
                drop(db);
                self.execute_execute(execute_stmt).await
            }

            vibesql_ast::Statement::Deallocate(deallocate_stmt) => {
                // Release lock before calling helper (doesn't need db)
                drop(db);
                self.execute_deallocate(deallocate_stmt)
            }

            vibesql_ast::Statement::DeclareCursor(declare_stmt) => {
                // Release lock - declare doesn't need db
                drop(db);
                self.execute_declare_cursor(declare_stmt)
            }

            vibesql_ast::Statement::OpenCursor(open_stmt) => {
                // Keep lock for open cursor (needs db)
                CursorExecutor::open(&mut self.cursors, open_stmt, &db)
                    .map_err(|e| anyhow::anyhow!("{}", e))?;
                Ok(ExecutionResult::OpenCursor { cursor_name: open_stmt.cursor_name.clone() })
            }

            vibesql_ast::Statement::Fetch(fetch_stmt) => {
                // Release lock - fetch doesn't need db
                drop(db);
                self.execute_fetch(fetch_stmt)
            }

            vibesql_ast::Statement::CloseCursor(close_stmt) => {
                // Release lock - close doesn't need db
                drop(db);
                self.execute_close_cursor(close_stmt)
            }

            vibesql_ast::Statement::BeginTransaction(_) => {
                // Release lock - transaction management doesn't need db lock
                drop(db);
                self.begin_transaction().await
            }

            vibesql_ast::Statement::Commit(_) => {
                // Release lock first, commit() will reacquire if needed
                drop(db);
                self.commit().await
            }

            vibesql_ast::Statement::Rollback(_) => {
                // Release lock - rollback discards buffered changes, no db write needed
                drop(db);
                self.rollback().await
            }

            vibesql_ast::Statement::RollbackToSavepoint(_savepoint_stmt) => {
                // TODO: Implement savepoints in SessionTransactionManager
                Ok(ExecutionResult::Other { message: "ROLLBACK TO SAVEPOINT".to_string() })
            }

            vibesql_ast::Statement::Savepoint(_savepoint_stmt) => {
                // TODO: Implement savepoints in SessionTransactionManager
                Ok(ExecutionResult::Other { message: "SAVEPOINT".to_string() })
            }

            vibesql_ast::Statement::ReleaseSavepoint(_release_stmt) => {
                // TODO: Implement savepoints in SessionTransactionManager
                Ok(ExecutionResult::Other { message: "RELEASE SAVEPOINT".to_string() })
            }

            _ => {
                // For now, return a generic success for other statements
                Ok(ExecutionResult::Other { message: "Command completed successfully".to_string() })
            }
        }
    }

    /// Execute PREPARE statement - registers a named prepared statement
    fn execute_prepare(
        &mut self,
        prepare_stmt: &vibesql_ast::PrepareStmt,
    ) -> Result<ExecutionResult> {
        use vibesql_ast::PreparedStatementBody;

        let name = prepare_stmt.name.clone();

        // Get the SQL string to prepare
        let sql = match &prepare_stmt.statement {
            PreparedStatementBody::SqlString(s) => s.clone(),
            PreparedStatementBody::ParsedStatement(_stmt) => {
                // For parsed statements, we need to re-serialize to SQL for caching
                // For now, we'll create a prepared statement directly from the AST
                // This is a simplified approach - a full implementation would
                // need to serialize the AST back to SQL or work with AST directly
                return Err(anyhow::anyhow!(
                    "PREPARE ... AS syntax not yet supported. Use PREPARE ... FROM 'sql_string' instead"
                ));
            }
        };

        // Create the prepared statement
        let prepared = self
            .stmt_cache
            .get_or_prepare(&sql)
            .map_err(|e| anyhow::anyhow!("Failed to prepare statement: {}", e))?;

        // Store in named statements registry
        self.named_statements.insert(name.clone(), prepared);

        Ok(ExecutionResult::Prepare { statement_name: name })
    }

    /// Execute EXECUTE statement - runs a named prepared statement
    fn execute_execute(
        &mut self,
        execute_stmt: &vibesql_ast::ExecuteStmt,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ExecutionResult>> + Send + '_>>
    {
        let name = execute_stmt.name.clone();
        let param_exprs = execute_stmt.params.clone();

        Box::pin(async move {
            // Look up the named statement
            let prepared = self
                .named_statements
                .get(&name)
                .ok_or_else(|| anyhow::anyhow!("Prepared statement '{}' not found", name))?
                .clone();

            // Evaluate parameter expressions to get values
            let params: Vec<SqlValue> =
                param_exprs.iter().map(evaluate_expression).collect::<Result<Vec<_>>>()?;

            // Execute the prepared statement with parameters
            self.execute_prepared(&prepared, &params).await
        })
    }

    /// Execute DEALLOCATE statement - removes a named prepared statement
    fn execute_deallocate(
        &mut self,
        deallocate_stmt: &vibesql_ast::DeallocateStmt,
    ) -> Result<ExecutionResult> {
        use vibesql_ast::DeallocateTarget;

        match &deallocate_stmt.target {
            DeallocateTarget::Name(name) => {
                if self.named_statements.remove(name).is_none() {
                    return Err(anyhow::anyhow!("Prepared statement '{}' not found", name));
                }
                Ok(ExecutionResult::Deallocate { statement_name: name.clone() })
            }
            DeallocateTarget::All => {
                let count = self.named_statements.len();
                self.named_statements.clear();
                Ok(ExecutionResult::Other {
                    message: format!("Deallocated {} prepared statement(s)", count),
                })
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
    ///
    /// Starts a new transaction for this session. While in a transaction:
    /// - Changes are tracked for potential rollback
    /// - The storage layer manages transaction state
    pub async fn begin_transaction(&mut self) -> Result<ExecutionResult> {
        // Track session-level transaction state
        self.txn_manager.begin().map_err(|e| anyhow::anyhow!("{}", e))?;

        // Also start transaction in the shared database storage layer
        let mut db = self.db.write().await;
        db.begin_transaction()
            .map_err(|e| anyhow::anyhow!("Failed to begin transaction: {}", e))?;

        Ok(ExecutionResult::Begin)
    }

    /// Commit the current transaction
    ///
    /// Commits all changes made during this transaction.
    /// After commit, changes are permanent and visible to all sessions.
    pub async fn commit(&mut self) -> Result<ExecutionResult> {
        // Commit session-level transaction state (clears buffered change tracking)
        let _changes = self.txn_manager.commit().map_err(|e| anyhow::anyhow!("{}", e))?;

        // Commit in the shared database storage layer
        let mut db = self.db.write().await;
        db.commit_transaction()
            .map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;

        Ok(ExecutionResult::Commit)
    }

    /// Rollback the current transaction
    ///
    /// Discards all changes made during this transaction.
    /// The database state is restored to what it was before BEGIN.
    pub async fn rollback(&mut self) -> Result<ExecutionResult> {
        // Rollback session-level transaction state
        self.txn_manager.rollback().map_err(|e| anyhow::anyhow!("{}", e))?;

        // Rollback in the shared database storage layer
        let mut db = self.db.write().await;
        db.rollback_transaction()
            .map_err(|e| anyhow::anyhow!("Failed to rollback transaction: {}", e))?;

        Ok(ExecutionResult::Rollback)
    }

    /// Execute DECLARE CURSOR statement
    fn execute_declare_cursor(
        &mut self,
        stmt: &vibesql_ast::DeclareCursorStmt,
    ) -> Result<ExecutionResult> {
        CursorExecutor::declare(&mut self.cursors, stmt).map_err(|e| anyhow::anyhow!("{}", e))?;
        Ok(ExecutionResult::DeclareCursor { cursor_name: stmt.cursor_name.clone() })
    }

    /// Execute FETCH statement
    fn execute_fetch(&mut self, stmt: &vibesql_ast::FetchStmt) -> Result<ExecutionResult> {
        let fetch_result: CursorFetchResult =
            CursorExecutor::fetch(&mut self.cursors, stmt).map_err(|e| anyhow::anyhow!("{}", e))?;

        // Convert cursor rows to session rows
        let rows: Vec<Row> =
            fetch_result.rows.iter().map(|r| Row { values: r.values.to_vec() }).collect();
        let columns: Vec<Column> =
            fetch_result.columns.iter().map(|name| Column { name: name.clone() }).collect();

        Ok(ExecutionResult::Fetch { rows, columns })
    }

    /// Execute CLOSE CURSOR statement
    fn execute_close_cursor(
        &mut self,
        stmt: &vibesql_ast::CloseCursorStmt,
    ) -> Result<ExecutionResult> {
        CursorExecutor::close(&mut self.cursors, stmt).map_err(|e| anyhow::anyhow!("{}", e))?;
        Ok(ExecutionResult::CloseCursor { cursor_name: stmt.cursor_name.clone() })
    }
}

/// Evaluate an expression to a SqlValue (for EXECUTE parameters)
fn evaluate_expression(expr: &vibesql_ast::Expression) -> Result<SqlValue> {
    use vibesql_ast::Expression;

    match expr {
        // Expression::Literal wraps SqlValue directly
        Expression::Literal(val) => Ok(val.clone()),
        Expression::UnaryOp { op, expr: operand } => {
            // Handle negative numbers
            if let vibesql_ast::UnaryOperator::Minus = op {
                let val = evaluate_expression(operand)?;
                match val {
                    SqlValue::Integer(n) => Ok(SqlValue::Integer(-n)),
                    SqlValue::Bigint(n) => Ok(SqlValue::Bigint(-n)),
                    SqlValue::Float(n) => Ok(SqlValue::Float(-n)),
                    SqlValue::Double(n) => Ok(SqlValue::Double(-n)),
                    SqlValue::Numeric(n) => Ok(SqlValue::Numeric(-n)),
                    _ => Err(anyhow::anyhow!("Cannot negate non-numeric value")),
                }
            } else {
                Err(anyhow::anyhow!("Unsupported unary operator in EXECUTE parameter"))
            }
        }
        _ => Err(anyhow::anyhow!(
            "Unsupported expression type in EXECUTE parameters. Only literals are currently supported."
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::RwLock;
    use vibesql_storage::Database;

    fn create_shared_db() -> SharedDatabase {
        Arc::new(RwLock::new(Database::new()))
    }

    #[test]
    fn test_session_creation() {
        let db = create_shared_db();
        let session = Session::new("testdb".to_string(), "testuser".to_string(), db);
        assert_eq!(session.database, "testdb");
        assert_eq!(session.user, "testuser");
        assert!(!session.in_transaction());
    }

    #[tokio::test]
    async fn test_transaction_state() {
        let db = create_shared_db();
        let mut session = Session::new("testdb".to_string(), "testuser".to_string(), db);

        // Not in transaction initially
        assert!(!session.in_transaction());

        // Begin transaction
        assert!(session.begin_transaction().await.is_ok());
        assert!(session.in_transaction());

        // Can't begin twice
        assert!(session.begin_transaction().await.is_err());

        // Commit
        assert!(session.commit().await.is_ok());
        assert!(!session.in_transaction());

        // Can't commit when not in transaction
        assert!(session.commit().await.is_err());
    }

    #[tokio::test]
    async fn test_prepare_and_execute() {
        let db = create_shared_db();
        let mut session = Session::new("testdb".to_string(), "testuser".to_string(), db);

        // Create a table first
        session.execute("CREATE TABLE users (id INT, name VARCHAR(100))").await.unwrap();

        // Prepare a statement (note: parser doesn't support ?, so use literal value)
        let stmt = session.prepare("SELECT * FROM users WHERE id = 1").unwrap();
        assert_eq!(stmt.param_count(), 0);

        // Execute the prepared statement
        let result = session.execute_prepared(&stmt, &[]).await;
        assert!(result.is_ok());

        // Verify we get a Select result
        match result.unwrap() {
            ExecutionResult::Select { .. } => (),
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_cache_hit() {
        let db = create_shared_db();
        let session = Session::new("testdb".to_string(), "testuser".to_string(), db);

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

    #[tokio::test]
    async fn test_auto_caching_in_execute() {
        let db = create_shared_db();
        let mut session = Session::new("testdb".to_string(), "testuser".to_string(), db);

        // First execute - cache miss
        session.execute("SELECT 1").await.unwrap();
        let stats = session.cache_stats();
        assert_eq!(stats.misses, 1);

        // Second execute - cache hit
        session.execute("SELECT 1").await.unwrap();
        let stats = session.cache_stats();
        assert_eq!(stats.hits, 1);
    }

    #[tokio::test]
    async fn test_analyze_single_table() {
        let db = create_shared_db();
        let mut session = Session::new("testdb".to_string(), "testuser".to_string(), db);

        // Create a table and insert data
        session.execute("CREATE TABLE users (id INT, name VARCHAR(100))").await.unwrap();
        session.execute("INSERT INTO users VALUES (1, 'Alice')").await.unwrap();
        session.execute("INSERT INTO users VALUES (2, 'Bob')").await.unwrap();

        // Analyze the table
        let result = session.execute("ANALYZE users").await.unwrap();

        // Verify we get an Analyze result
        match result {
            ExecutionResult::Analyze { tables_analyzed } => {
                assert_eq!(tables_analyzed, 1);
            }
            other => panic!("Expected Analyze result, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_analyze_all_tables() {
        let db = create_shared_db();
        let mut session = Session::new("testdb".to_string(), "testuser".to_string(), db);

        // Create multiple tables
        session.execute("CREATE TABLE users (id INT, name VARCHAR(100))").await.unwrap();
        session.execute("CREATE TABLE products (id INT, price INT)").await.unwrap();
        session.execute("INSERT INTO users VALUES (1, 'Alice')").await.unwrap();
        session.execute("INSERT INTO products VALUES (1, 100)").await.unwrap();

        // Analyze all tables (no table name)
        let result = session.execute("ANALYZE").await.unwrap();

        // Verify we get an Analyze result with 2 tables
        match result {
            ExecutionResult::Analyze { tables_analyzed } => {
                assert_eq!(tables_analyzed, 2);
            }
            other => panic!("Expected Analyze result, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_analyze_with_columns() {
        let db = create_shared_db();
        let mut session = Session::new("testdb".to_string(), "testuser".to_string(), db);

        // Create a table
        session.execute("CREATE TABLE users (id INT, name VARCHAR(100), age INT)").await.unwrap();
        session.execute("INSERT INTO users VALUES (1, 'Alice', 30)").await.unwrap();

        // Analyze specific columns
        let result = session.execute("ANALYZE users (id, name)").await.unwrap();

        // Verify we get an Analyze result
        match result {
            ExecutionResult::Analyze { tables_analyzed } => {
                assert_eq!(tables_analyzed, 1);
            }
            other => panic!("Expected Analyze result, got {:?}", other),
        }
    }

    #[test]
    fn test_analyze_statement_type() {
        let result = ExecutionResult::Analyze { tables_analyzed: 1 };
        assert_eq!(result.statement_type(), "ANALYZE");
    }

    #[tokio::test]
    async fn test_shared_database_across_sessions() {
        // Create a shared database
        let db = create_shared_db();

        // Create two sessions using the same shared database
        let mut session1 = Session::new("testdb".to_string(), "user1".to_string(), Arc::clone(&db));
        let mut session2 = Session::new("testdb".to_string(), "user2".to_string(), Arc::clone(&db));

        // Create a table through session 1
        session1
            .execute("CREATE TABLE shared_test (id INT, value VARCHAR(100))")
            .await
            .unwrap();

        // Insert data through session 1
        session1
            .execute("INSERT INTO shared_test VALUES (1, 'from session 1')")
            .await
            .unwrap();

        // Should be visible through session 2
        let result = session2.execute("SELECT * FROM shared_test").await.unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Expected Select result"),
        }

        // Insert through session 2
        session2
            .execute("INSERT INTO shared_test VALUES (2, 'from session 2')")
            .await
            .unwrap();

        // Should be visible through session 1
        let result = session1.execute("SELECT * FROM shared_test").await.unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Select result"),
        }
    }
}
