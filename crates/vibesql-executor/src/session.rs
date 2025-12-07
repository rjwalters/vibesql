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
//! ```text
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
//! session.execute_prepared_mut(&insert_stmt, &[SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))])?;
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

use crate::cache::{
    ArenaParseError, ArenaPreparedStatement, CachedPlan, PkPointLookupPlan, PreparedStatement,
    PreparedStatementCache, PreparedStatementError, ProjectionPlan, ResolvedProjection,
};
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
        Self { db, cache: Arc::new(PreparedStatementCache::default_cache()) }
    }

    /// Create a new session with a custom cache size
    pub fn with_cache_size(db: &'a Database, cache_size: usize) -> Self {
        Self { db, cache: Arc::new(PreparedStatementCache::new(cache_size)) }
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
    /// ```text
    /// let stmt = session.prepare("SELECT * FROM users WHERE id = ?")?;
    /// assert_eq!(stmt.param_count(), 1);
    /// ```
    pub fn prepare(&self, sql: &str) -> Result<Arc<PreparedStatement>, SessionError> {
        self.cache.get_or_prepare(sql).map_err(SessionError::from)
    }

    /// Prepare a SQL SELECT statement using arena allocation
    ///
    /// This is optimized for SELECT statements and provides better cache locality.
    /// Arena-based statements store the parsed AST in contiguous memory, which
    /// can improve performance for frequently executed queries.
    ///
    /// For non-SELECT statements, this will return an error - use `prepare()` instead.
    ///
    /// # Performance Benefits
    ///
    /// Arena allocation provides:
    /// - Better cache locality (contiguous memory layout)
    /// - Lower allocation overhead (single arena vs multiple heap allocations)
    /// - Potential for zero-copy parameter binding in future phases
    ///
    /// # Example
    ///
    /// ```text
    /// let stmt = session.prepare_arena("SELECT * FROM users WHERE id = ?")?;
    /// assert_eq!(stmt.param_count(), 1);
    /// ```
    pub fn prepare_arena(&self, sql: &str) -> Result<Arc<ArenaPreparedStatement>, ArenaParseError> {
        self.cache.get_or_prepare_arena(sql)
    }

    /// Execute a prepared SELECT statement with parameters
    ///
    /// Binds the parameters to the prepared statement and executes it.
    /// This is the fast path for repeated queries - no SQL parsing occurs.
    ///
    /// For simple PK point lookups, uses cached execution plan to bypass
    /// the full query execution pipeline.
    ///
    /// # Example
    ///
    /// ```text
    /// let stmt = session.prepare("SELECT * FROM users WHERE id = ?")?;
    /// let result = session.execute_prepared(&stmt, &[SqlValue::Integer(42)])?;
    /// ```
    pub fn execute_prepared(
        &self,
        stmt: &PreparedStatement,
        params: &[SqlValue],
    ) -> Result<PreparedExecutionResult, SessionError> {
        // Check parameter count first
        if params.len() != stmt.param_count() {
            return Err(SessionError::PreparedStatement(
                PreparedStatementError::ParameterCountMismatch {
                    expected: stmt.param_count(),
                    actual: params.len(),
                },
            ));
        }

        // Try fast-path execution using cached plan
        match stmt.cached_plan() {
            CachedPlan::PkPointLookup(plan) => {
                if let Some(result) = self.try_execute_pk_lookup(plan, params)? {
                    return Ok(result);
                }
                // Fall through to standard execution if fast path fails
            }
            CachedPlan::SimpleFastPath(plan) => {
                // SimpleFastPath caches both the result of is_simple_point_query()
                // AND the column names derived from the SELECT list (#3780)
                let bound_stmt = stmt.bind(params)?;
                if let Statement::Select(select_stmt) = &bound_stmt {
                    let executor = SelectExecutor::new(self.db);

                    // Use cached column names if available, otherwise derive and cache them
                    let columns = plan.get_or_resolve_columns(|| {
                        executor.derive_fast_path_column_names(select_stmt).ok()
                    });

                    match columns {
                        Some(cached_columns) => {
                            // Fast path: use cached column names
                            let rows = executor.execute_fast_path(select_stmt)?;
                            return Ok(PreparedExecutionResult::Select(SelectResult {
                                columns: cached_columns.iter().cloned().collect(),
                                rows,
                            }));
                        }
                        None => {
                            // First execution or resolution failed: derive columns
                            let result = executor.execute_fast_path_with_columns(select_stmt)?;
                            return Ok(PreparedExecutionResult::Select(result));
                        }
                    }
                }
                // Fall through for non-SELECT (shouldn't happen for SimpleFastPath)
            }
            CachedPlan::PkDelete(_) => {
                // DELETE requires mutable access, use execute_prepared_mut instead
                // Fall through to standard execution which will return an error
            }
            CachedPlan::Standard => {
                // Fall through to standard execution
            }
        }

        // Bind parameters to get executable statement
        let bound_stmt = stmt.bind(params)?;

        // Execute based on statement type
        self.execute_statement(&bound_stmt)
    }

    /// Try to execute a PK point lookup using the cached plan
    ///
    /// Returns `Ok(Some(result))` if execution succeeded via fast path,
    /// `Ok(None)` if we need to fall back to standard execution,
    /// or `Err` if a real error occurred.
    fn try_execute_pk_lookup(
        &self,
        plan: &PkPointLookupPlan,
        params: &[SqlValue],
    ) -> Result<Option<PreparedExecutionResult>, SessionError> {
        // Get the table
        let table = match self.db.get_table(&plan.table_name) {
            Some(t) => t,
            None => return Ok(None), // Table doesn't exist or it's a view - fall back
        };

        // Verify PK columns match what we expected
        let actual_pk_columns = match &table.schema.primary_key {
            Some(cols) if cols.len() == plan.pk_columns.len() => cols,
            _ => return Ok(None), // PK structure changed - fall back
        };

        // Verify column mappings and extract parameter values
        // Use borrowing to avoid cloning when possible
        for (param_idx, pk_col_idx) in &plan.param_to_pk_col {
            if *param_idx >= params.len() || *pk_col_idx >= plan.pk_columns.len() {
                return Ok(None); // Invalid mapping - fall back
            }

            // Verify the column name still matches
            let expected_col = &plan.pk_columns[*pk_col_idx];
            let actual_col = &actual_pk_columns[*pk_col_idx];
            if !expected_col.eq_ignore_ascii_case(actual_col) {
                return Ok(None); // Column mismatch - fall back
            }
        }

        // Get or resolve projection info (cached after first execution)
        let resolved = match plan.get_or_resolve(|proj| {
            self.resolve_projection(proj, &table.schema.columns)
        }) {
            Some(r) => r,
            None => return Ok(None), // Resolution failed - fall back
        };

        // Perform the PK lookup using borrowed parameter
        // For single-column PK, we can pass the reference directly
        let row = if plan.param_to_pk_col.len() == 1 {
            let (param_idx, _) = plan.param_to_pk_col[0];
            self.db
                .get_row_by_pk(&plan.table_name, &params[param_idx])
                .map_err(|e| SessionError::Execution(ExecutorError::StorageError(e.to_string())))?
        } else {
            // For composite PK, we need to collect values
            let pk_values: Vec<SqlValue> = plan
                .param_to_pk_col
                .iter()
                .map(|(param_idx, _)| params[*param_idx].clone())
                .collect();
            self.db
                .get_row_by_composite_pk(&plan.table_name, &pk_values)
                .map_err(|e| SessionError::Execution(ExecutorError::StorageError(e.to_string())))?
        };

        // Build result with cached column names
        let columns: Vec<String> = resolved.column_names.iter().cloned().collect();

        let rows = match row {
            Some(r) => {
                // Project directly from source row without cloning entire row first
                if resolved.column_indices.is_empty() {
                    // Wildcard - clone entire row
                    vec![r.clone()]
                } else {
                    // Specific columns - only clone needed values
                    let projected_values: Vec<SqlValue> = resolved
                        .column_indices
                        .iter()
                        .map(|&i| r.values[i].clone())
                        .collect();
                    vec![Row::new(projected_values)]
                }
            }
            None => vec![],
        };

        Ok(Some(PreparedExecutionResult::Select(SelectResult {
            columns,
            rows,
        })))
    }

    /// Resolve projection plan to column indices and names
    ///
    /// This is called once per plan and the result is cached.
    fn resolve_projection(
        &self,
        proj: &ProjectionPlan,
        schema_columns: &[vibesql_catalog::ColumnSchema],
    ) -> Option<ResolvedProjection> {
        match proj {
            ProjectionPlan::Wildcard => {
                // For wildcard, indices are empty (we clone entire row)
                // but we cache the column names
                let column_names: Arc<[String]> = schema_columns
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();
                Some(ResolvedProjection {
                    column_indices: vec![],
                    column_names,
                })
            }
            ProjectionPlan::Columns(projections) => {
                let mut col_indices = Vec::with_capacity(projections.len());
                let mut column_names = Vec::with_capacity(projections.len());

                for proj in projections {
                    let idx = schema_columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&proj.column_name))?;

                    col_indices.push(idx);
                    column_names.push(
                        proj.alias
                            .clone()
                            .unwrap_or_else(|| proj.column_name.clone()),
                    );
                }

                Some(ResolvedProjection {
                    column_indices: col_indices,
                    column_names: column_names.into(),
                })
            }
        }
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
        Self { db, cache: Arc::new(PreparedStatementCache::default_cache()) }
    }

    /// Create a new mutable session with a custom cache size
    pub fn with_cache_size(db: &'a mut Database, cache_size: usize) -> Self {
        Self { db, cache: Arc::new(PreparedStatementCache::new(cache_size)) }
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

    /// Prepare a SQL SELECT statement using arena allocation
    ///
    /// See [`Session::prepare_arena`] for details.
    pub fn prepare_arena(&self, sql: &str) -> Result<Arc<ArenaPreparedStatement>, ArenaParseError> {
        self.cache.get_or_prepare_arena(sql)
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
        // Try fast-path execution using cached plan
        if let CachedPlan::PkDelete(plan) = stmt.cached_plan() {
            if let Some(result) = self.try_execute_pk_delete(plan, params)? {
                return Ok(result);
            }
            // Fall through to standard execution if fast path fails
        }

        let bound_stmt = stmt.bind(params)?;
        self.execute_statement_mut(&bound_stmt)
    }

    /// Try to execute a PK delete using the cached plan
    ///
    /// Returns `Ok(Some(result))` if execution succeeded via fast path,
    /// `Ok(None)` if we need to fall back to standard execution.
    fn try_execute_pk_delete(
        &mut self,
        plan: &crate::cache::PkDeletePlan,
        params: &[SqlValue],
    ) -> Result<Option<PreparedExecutionResult>, SessionError> {
        // Check cached validation first (fast path for repeated executions)
        if let Some(valid) = plan.is_fast_path_valid() {
            if !valid {
                return Ok(None); // Cached as invalid, fall back immediately
            }
            // Cached as valid, skip expensive checks and execute directly
        } else {
            // Not cached yet - do the expensive validation and cache result
            let valid = self.validate_delete_fast_path(plan);
            plan.set_fast_path_valid(valid);
            if !valid {
                return Ok(None);
            }
        }

        // Build PK values from parameters
        let pk_values = plan.build_pk_values(params);

        // Execute the fast delete
        match self.db.delete_by_pk_fast(&plan.table_name, &pk_values) {
            Ok(deleted) => Ok(Some(PreparedExecutionResult::RowsAffected(if deleted {
                1
            } else {
                0
            }))),
            Err(_) => Ok(None), // Fall back to standard path on error
        }
    }

    /// Validate whether fast delete path can be used for this table
    /// This is expensive (iterates triggers and FKs) so result is cached
    fn validate_delete_fast_path(&self, plan: &crate::cache::PkDeletePlan) -> bool {
        // Check for triggers - if any exist, we must use standard path
        let has_triggers = self
            .db
            .catalog
            .get_triggers_for_table(&plan.table_name, Some(vibesql_ast::TriggerEvent::Delete))
            .next()
            .is_some();

        if has_triggers {
            return false;
        }

        // Check for referencing FKs - if any exist, we must use standard path
        let schema = match self.db.catalog.get_table(&plan.table_name) {
            Some(s) => s,
            None => return false, // Table not found
        };

        let has_pk = schema.get_primary_key_indices().is_some();
        if has_pk {
            let has_referencing_fks = self.db.catalog.list_tables().iter().any(|t| {
                self.db
                    .catalog
                    .get_table(t)
                    .map(|s| {
                        s.foreign_keys
                            .iter()
                            .any(|fk| fk.parent_table.eq_ignore_ascii_case(&plan.table_name))
                    })
                    .unwrap_or(false)
            });

            if has_referencing_fks {
                return false;
            }
        }

        true // No blockers, fast path is valid
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
                // Note: We don't invalidate prepared statement cache for DML operations.
                // Prepared statements (parsed AST) don't depend on data values.
                // Only schema changes (DDL) require cache invalidation.
                // Query result caches are handled separately by IntegrationCache.
                Ok(PreparedExecutionResult::RowsAffected(rows_affected))
            }
            Statement::Update(update_stmt) => {
                let rows_affected = UpdateExecutor::execute(update_stmt, self.db)?;
                // Note: We don't invalidate prepared statement cache for DML operations.
                // Prepared statements (parsed AST) don't depend on data values.
                // Only schema changes (DDL) require cache invalidation.
                // Query result caches are handled separately by IntegrationCache.
                Ok(PreparedExecutionResult::RowsAffected(rows_affected))
            }
            Statement::Delete(delete_stmt) => {
                let rows_affected = DeleteExecutor::execute(delete_stmt, self.db)?;
                // Note: We don't invalidate prepared statement cache for DML operations.
                // Prepared statements (parsed AST) don't depend on data values.
                // Only schema changes (DDL) require cache invalidation.
                // Query result caches are handled separately by IntegrationCache.
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
        let row1 = Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]);
        let row2 = Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]);
        let row3 = Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))]);

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
        let result = session.execute_prepared(&stmt, &[SqlValue::Integer(1)]).unwrap();

        if let PreparedExecutionResult::Select(select_result) = result {
            assert_eq!(select_result.rows.len(), 1);
            assert_eq!(select_result.rows[0].values[0], SqlValue::Integer(1));
            assert_eq!(select_result.rows[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
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
        let result1 = session.execute_prepared(&stmt, &[SqlValue::Integer(1)]).unwrap();
        let result2 = session.execute_prepared(&stmt, &[SqlValue::Integer(2)]).unwrap();
        let result3 = session.execute_prepared(&stmt, &[SqlValue::Integer(3)]).unwrap();

        // Verify each returned the correct row
        assert_eq!(result1.rows().unwrap()[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
        assert_eq!(result2.rows().unwrap()[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Bob")));
        assert_eq!(result3.rows().unwrap()[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Charlie")));

        // Verify cache was used (should have 1 miss, then hits)
        let stats = session.cache().stats();
        assert_eq!(stats.misses, 1);
        // hits is always >= 0 since it's a u64, just ensure it exists
        let _hits = stats.hits;
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

        let stmt = session.prepare("INSERT INTO users (id, name) VALUES (?, ?)").unwrap();

        let result = session
            .execute_prepared_mut(&stmt, &[SqlValue::Integer(4), SqlValue::Varchar(arcstr::ArcStr::from("David"))])
            .unwrap();

        assert_eq!(result.rows_affected(), Some(1));

        // Verify the row was inserted
        let select_stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();
        let select_result =
            session.execute_prepared(&select_stmt, &[SqlValue::Integer(4)]).unwrap();

        assert_eq!(select_result.rows().unwrap().len(), 1);
        assert_eq!(select_result.rows().unwrap()[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("David")));
    }

    #[test]
    fn test_session_mut_update() {
        let mut db = create_test_db();
        let mut session = SessionMut::new(&mut db);

        let stmt = session.prepare("UPDATE users SET name = ? WHERE id = ?").unwrap();

        let result = session
            .execute_prepared_mut(
                &stmt,
                &[SqlValue::Varchar(arcstr::ArcStr::from("Alicia")), SqlValue::Integer(1)],
            )
            .unwrap();

        assert_eq!(result.rows_affected(), Some(1));

        // Verify the row was updated
        let select_stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();
        let select_result =
            session.execute_prepared(&select_stmt, &[SqlValue::Integer(1)]).unwrap();

        assert_eq!(select_result.rows().unwrap()[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Alicia")));
    }

    #[test]
    fn test_session_mut_delete() {
        let mut db = create_test_db();
        let mut session = SessionMut::new(&mut db);

        let stmt = session.prepare("DELETE FROM users WHERE id = ?").unwrap();

        let result = session.execute_prepared_mut(&stmt, &[SqlValue::Integer(1)]).unwrap();

        assert_eq!(result.rows_affected(), Some(1));

        // Verify the row was deleted
        let select_stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();
        let select_result =
            session.execute_prepared(&select_stmt, &[SqlValue::Integer(1)]).unwrap();

        assert_eq!(select_result.rows().unwrap().len(), 0);
    }

    #[test]
    fn test_shared_cache() {
        let db = create_test_db();

        // Create first session and prepare a statement
        let session1 = Session::new(&db);
        let stmt = session1.prepare("SELECT * FROM users WHERE id = ?").unwrap();

        // Get the shared cache
        let shared_cache = session1.shared_cache();
        let initial_misses = session1.cache().stats().misses;

        // Create second session with shared cache
        let session2 = Session::with_shared_cache(&db, shared_cache);

        // Prepare the same statement - should hit cache
        let _stmt2 = session2.prepare("SELECT * FROM users WHERE id = ?").unwrap();

        // Verify cache was shared (no additional miss)
        assert_eq!(session2.cache().stats().misses, initial_misses);

        // Execute on both sessions
        let result1 = session1.execute_prepared(&stmt, &[SqlValue::Integer(1)]).unwrap();
        let result2 = session2.execute_prepared(&stmt, &[SqlValue::Integer(2)]).unwrap();

        assert_eq!(result1.rows().unwrap()[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
        assert_eq!(result2.rows().unwrap()[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Bob")));
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

        let stmt = session.prepare("SELECT * FROM users WHERE id >= ? AND id <= ?").unwrap();
        assert_eq!(stmt.param_count(), 2);

        let result =
            session.execute_prepared(&stmt, &[SqlValue::Integer(1), SqlValue::Integer(2)]).unwrap();

        assert_eq!(result.rows().unwrap().len(), 2);
    }

    #[test]
    fn test_session_prepare_arena() {
        let db = create_test_db();
        let session = Session::new(&db);

        // Test prepare_arena for SELECT statement
        let stmt = session.prepare_arena("SELECT * FROM users WHERE id = ?").unwrap();
        assert_eq!(stmt.param_count(), 1);

        // Verify tables were extracted (arena parser uses uppercase)
        assert!(stmt.tables().contains("USERS"));

        // Test caching - second call should hit cache
        let stmt2 = session.prepare_arena("SELECT * FROM users WHERE id = ?").unwrap();
        assert_eq!(stmt2.param_count(), 1);

        // Verify it's the same cached statement
        assert!(std::sync::Arc::ptr_eq(&stmt, &stmt2));
    }

    #[test]
    fn test_session_prepare_arena_no_params() {
        let db = create_test_db();
        let session = Session::new(&db);

        let stmt = session.prepare_arena("SELECT * FROM users").unwrap();
        assert_eq!(stmt.param_count(), 0);
    }

    #[test]
    fn test_session_prepare_arena_join() {
        let db = create_test_db();
        let session = Session::new(&db);

        // Create orders table first
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;

        let orders_columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("user_id".to_string(), DataType::Integer, false),
        ];
        let _orders_schema = TableSchema::with_primary_key(
            "orders".to_string(),
            orders_columns,
            vec!["id".to_string()],
        );

        // We need a mutable db for this test, so we'll just test the parse works
        // without actually querying
        let stmt = session
            .prepare_arena("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id")
            .unwrap();

        // Both tables should be tracked
        let tables = stmt.tables();
        assert!(tables.contains("USERS"), "Expected USERS in {:?}", tables);
        assert!(tables.contains("ORDERS"), "Expected ORDERS in {:?}", tables);
    }

    #[test]
    fn test_session_mut_prepare_arena() {
        let mut db = create_test_db();
        let session = SessionMut::new(&mut db);

        // Test prepare_arena for SELECT statement
        let stmt = session.prepare_arena("SELECT * FROM users WHERE id = ?").unwrap();
        assert_eq!(stmt.param_count(), 1);
    }

    #[test]
    fn test_delete_fast_path_plan() {
        use crate::cache::CachedPlan;

        let mut db = create_test_db();
        let mut session = SessionMut::new(&mut db);

        // Prepare DELETE statement
        let stmt = session.prepare("DELETE FROM users WHERE id = ?").unwrap();

        // Verify it creates a PkDelete plan
        match stmt.cached_plan() {
            CachedPlan::PkDelete(plan) => {
                // Table name should be uppercase
                assert_eq!(plan.table_name, "USERS");
                // Should have one PK column
                assert_eq!(plan.pk_columns, vec!["ID"]);
                // Param 0 maps to PK column 0
                assert_eq!(plan.param_to_pk_col, vec![(0, 0)]);
                // Fast path should not be validated yet
                assert!(plan.is_fast_path_valid().is_none());
            }
            other => panic!("Expected PkDelete plan, got {:?}", other),
        }

        // Execute DELETE - this should validate and use fast path
        let result = session.execute_prepared_mut(&stmt, &[SqlValue::Integer(1)]).unwrap();
        assert_eq!(result.rows_affected(), Some(1));

        // After execution, fast path should be cached as valid (no triggers/FKs on users table)
        match stmt.cached_plan() {
            CachedPlan::PkDelete(plan) => {
                assert_eq!(plan.is_fast_path_valid(), Some(true), "Fast path should be valid after execution");
            }
            _ => panic!("Plan should still be PkDelete"),
        }

        // Verify the row was deleted
        let select_stmt = session.prepare("SELECT * FROM users WHERE id = ?").unwrap();
        let select_result = session.execute_prepared(&select_stmt, &[SqlValue::Integer(1)]).unwrap();
        assert_eq!(select_result.rows().unwrap().len(), 0);
    }
}
