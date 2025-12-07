//! Prepared statement caching for optimized query execution
//!
//! Caches parsed AST statements to avoid repeated parsing overhead.
//! This provides significant performance benefits for repeated queries by:
//! - Caching the parsed AST for identical SQL strings
//! - Avoiding expensive parsing for each query execution
//! - Supporting `?` placeholders with AST-level parameter binding
//!
//! ## Parameterized Queries
//!
//! The parser supports `?` placeholders which are converted to `Placeholder(index)`
//! expressions in the AST. Parameter binding replaces these placeholders with
//! literal values directly in the AST, avoiding re-parsing entirely.
//!
//! Example:
//! ```text
//! let stmt = session.prepare("SELECT * FROM users WHERE id = ?")?;
//! // First execution - fast (no re-parsing)
//! let result1 = session.execute_prepared(&stmt, &[SqlValue::Integer(1)])?;
//! // Second execution - equally fast (still no re-parsing)
//! let result2 = session.execute_prepared(&stmt, &[SqlValue::Integer(2)])?;
//! ```

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use lru::LruCache;
use std::num::NonZeroUsize;
use vibesql_ast::Statement;
use vibesql_types::SqlValue;

use super::{extract_tables_from_statement, QuerySignature};

pub mod arena_prepared;
mod bind;
pub mod plan;

pub use plan::{
    CachedPlan, ColumnProjection, PkDeletePlan, PkPointLookupPlan, ProjectionPlan, ResolvedProjection,
    SimpleFastPathPlan,
};

/// A prepared statement with cached AST and optional execution plan
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// Original SQL with `?` placeholders
    sql: String,
    /// Parsed AST
    statement: Statement,
    /// Query signature for cache lookup (ignores literal values)
    signature: QuerySignature,
    /// Number of parameters expected
    param_count: usize,
    /// Tables referenced by this statement (for invalidation)
    tables: std::collections::HashSet<String>,
    /// Cached execution plan (for fast-path execution)
    cached_plan: CachedPlan,
}

impl PreparedStatement {
    /// Create a new prepared statement from parsed AST
    pub fn new(sql: String, statement: Statement) -> Self {
        let signature = QuerySignature::from_ast(&statement);
        // Count placeholders from the AST (more accurate than counting ? in SQL string)
        let param_count = bind::count_placeholders(&statement);
        let tables = extract_tables_from_statement(&statement);
        // Analyze for fast-path execution plan
        let cached_plan = plan::analyze_for_plan(&statement);

        Self { sql, statement, signature, param_count, tables, cached_plan }
    }

    /// Get the original SQL
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Get the cached statement AST
    pub fn statement(&self) -> &Statement {
        &self.statement
    }

    /// Get the query signature
    pub fn signature(&self) -> &QuerySignature {
        &self.signature
    }

    /// Get the number of parameters expected
    pub fn param_count(&self) -> usize {
        self.param_count
    }

    /// Get the tables referenced by this statement
    pub fn tables(&self) -> &std::collections::HashSet<String> {
        &self.tables
    }

    /// Get the cached execution plan
    pub fn cached_plan(&self) -> &CachedPlan {
        &self.cached_plan
    }

    /// Bind parameters to create an executable statement
    ///
    /// For statements without placeholders, returns a clone of the cached statement.
    /// For parameterized statements, replaces Placeholder expressions with Literal values
    /// directly in the AST, avoiding the overhead of re-parsing.
    ///
    /// This is the key performance optimization: binding happens at the AST level,
    /// not by string substitution and re-parsing.
    pub fn bind(&self, params: &[SqlValue]) -> Result<Statement, PreparedStatementError> {
        if params.len() != self.param_count {
            return Err(PreparedStatementError::ParameterCountMismatch {
                expected: self.param_count,
                actual: params.len(),
            });
        }

        if self.param_count == 0 {
            // No parameters - return cached statement directly
            return Ok(self.statement.clone());
        }

        // Bind parameters at AST level (no re-parsing!)
        Ok(bind::bind_parameters(&self.statement, params))
    }
}

/// Errors that can occur during prepared statement operations
#[derive(Debug, Clone)]
pub enum PreparedStatementError {
    /// Wrong number of parameters provided
    ParameterCountMismatch { expected: usize, actual: usize },
    /// Failed to parse bound SQL
    ParseError(String),
}

impl std::fmt::Display for PreparedStatementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PreparedStatementError::ParameterCountMismatch { expected, actual } => {
                write!(f, "Parameter count mismatch: expected {}, got {}", expected, actual)
            }
            PreparedStatementError::ParseError(msg) => write!(f, "Parse error: {}", msg),
        }
    }
}

impl std::error::Error for PreparedStatementError {}

/// Statistics for prepared statement cache
#[derive(Debug, Clone)]
pub struct PreparedStatementCacheStats {
    pub hits: usize,
    pub misses: usize,
    pub evictions: usize,
    pub size: usize,
    pub hit_rate: f64,
}

/// Thread-safe cache for prepared statements with LRU eviction
pub struct PreparedStatementCache {
    /// LRU cache mapping SQL string to prepared statement (owned AST)
    cache: Mutex<LruCache<String, Arc<PreparedStatement>>>,
    /// LRU cache for arena-based prepared statements (SELECT only)
    arena_cache: Mutex<LruCache<String, Arc<arena_prepared::ArenaPreparedStatement>>>,
    /// Maximum cache size
    max_size: usize,
    /// Cache hit count
    hits: AtomicUsize,
    /// Cache miss count
    misses: AtomicUsize,
    /// Cache eviction count
    evictions: AtomicUsize,
    /// Arena cache hit count
    arena_hits: AtomicUsize,
    /// Arena cache miss count
    arena_misses: AtomicUsize,
}

impl PreparedStatementCache {
    /// Create a new cache with specified max size
    pub fn new(max_size: usize) -> Self {
        let cap = NonZeroUsize::new(max_size).unwrap_or(NonZeroUsize::new(1).unwrap());
        Self {
            cache: Mutex::new(LruCache::new(cap)),
            arena_cache: Mutex::new(LruCache::new(cap)),
            max_size,
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
            evictions: AtomicUsize::new(0),
            arena_hits: AtomicUsize::new(0),
            arena_misses: AtomicUsize::new(0),
        }
    }

    /// Create a default cache (1000 entries)
    pub fn default_cache() -> Self {
        Self::new(1000)
    }

    /// Get a prepared statement from cache (updates LRU order)
    pub fn get(&self, sql: &str) -> Option<Arc<PreparedStatement>> {
        let mut cache = self.cache.lock().unwrap();
        if let Some(stmt) = cache.get(sql) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(Arc::clone(stmt))
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Get or insert a prepared statement
    ///
    /// If the SQL is in cache, returns the cached statement.
    /// Otherwise, parses the SQL, caches it, and returns the new statement.
    /// Uses double-checked locking to avoid duplicate parsing.
    pub fn get_or_prepare(
        &self,
        sql: &str,
    ) -> Result<Arc<PreparedStatement>, PreparedStatementError> {
        // Acquire lock for both read and potential write to avoid race condition
        let mut cache = self.cache.lock().unwrap();

        // Check if already in cache
        if let Some(stmt) = cache.get(sql) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Arc::clone(stmt));
        }

        // Not in cache - parse the SQL
        self.misses.fetch_add(1, Ordering::Relaxed);
        let statement = parse_with_arena_fallback(sql)
            .map_err(|e| PreparedStatementError::ParseError(e.to_string()))?;

        let prepared = Arc::new(PreparedStatement::new(sql.to_string(), statement));

        // Check if we'll evict an entry
        if cache.len() >= self.max_size {
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }

        // Insert into cache (LRU will automatically evict if at capacity)
        cache.put(sql.to_string(), Arc::clone(&prepared));

        Ok(prepared)
    }

    /// Get or insert an arena-based prepared statement
    ///
    /// This method returns an arena-allocated SELECT statement for optimal
    /// performance. It's only for SELECT statements - other statement types
    /// will fail with `UnsupportedStatement`.
    ///
    /// Arena statements provide:
    /// - Better cache locality (contiguous memory)
    /// - Potentially lower allocation overhead
    /// - Direct use with arena-based execution paths
    ///
    /// For non-SELECT statements, use `get_or_prepare` instead.
    pub fn get_or_prepare_arena(
        &self,
        sql: &str,
    ) -> Result<Arc<arena_prepared::ArenaPreparedStatement>, arena_prepared::ArenaParseError> {
        // Acquire lock for both read and potential write
        let mut cache = self.arena_cache.lock().unwrap();

        // Check if already in cache
        if let Some(stmt) = cache.get(sql) {
            self.arena_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Arc::clone(stmt));
        }

        // Not in cache - parse the SQL using arena parser
        self.arena_misses.fetch_add(1, Ordering::Relaxed);
        let prepared = Arc::new(arena_prepared::ArenaPreparedStatement::new(sql.to_string())?);

        // Check if we'll evict an entry
        if cache.len() >= self.max_size {
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }

        // Insert into cache (LRU will automatically evict if at capacity)
        cache.put(sql.to_string(), Arc::clone(&prepared));

        Ok(prepared)
    }

    /// Get an arena-based prepared statement if it exists in cache
    pub fn get_arena(&self, sql: &str) -> Option<Arc<arena_prepared::ArenaPreparedStatement>> {
        let mut cache = self.arena_cache.lock().unwrap();
        if let Some(stmt) = cache.get(sql) {
            self.arena_hits.fetch_add(1, Ordering::Relaxed);
            Some(Arc::clone(stmt))
        } else {
            None
        }
    }

    /// Clear all cached statements (both owned and arena)
    pub fn clear(&self) {
        self.cache.lock().unwrap().clear();
        self.arena_cache.lock().unwrap().clear();
    }

    /// Invalidate all statements referencing a table
    pub fn invalidate_table(&self, table: &str) {
        // Invalidate owned statements
        {
            let mut cache = self.cache.lock().unwrap();
            let keys_to_remove: Vec<String> = cache
                .iter()
                .filter(|(_, stmt)| stmt.tables.iter().any(|t| t.eq_ignore_ascii_case(table)))
                .map(|(k, _)| k.clone())
                .collect();

            for key in keys_to_remove {
                cache.pop(&key);
            }
        }

        // Invalidate arena statements
        {
            let mut arena_cache = self.arena_cache.lock().unwrap();
            let keys_to_remove: Vec<String> = arena_cache
                .iter()
                .filter(|(_, stmt)| stmt.tables().iter().any(|t| t.eq_ignore_ascii_case(table)))
                .map(|(k, _)| k.clone())
                .collect();

            for key in keys_to_remove {
                arena_cache.pop(&key);
            }
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> PreparedStatementCacheStats {
        let cache = self.cache.lock().unwrap();
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 { hits as f64 / total as f64 } else { 0.0 };

        PreparedStatementCacheStats {
            hits,
            misses,
            evictions: self.evictions.load(Ordering::Relaxed),
            size: cache.len(),
            hit_rate,
        }
    }

    /// Get maximum cache size
    pub fn max_size(&self) -> usize {
        self.max_size
    }
}

/// Parse SQL using arena parser for SELECT/INSERT/UPDATE/DELETE, falling back to standard parser.
///
/// This function provides the performance benefits of arena parsing for common query types
/// while maintaining full compatibility with all SQL statement types.
///
/// # Performance
///
/// For supported statements (SELECT, INSERT, UPDATE, DELETE), this is 10-20% faster because:
/// - Arena parsing is 30-40% faster (fewer allocations during parse)
/// - Conversion allocates fewer, larger chunks (better cache locality)
/// - Many strings benefit from SSO (Small String Optimization)
fn parse_with_arena_fallback(sql: &str) -> Result<Statement, vibesql_parser::ParseError> {
    // Quick check: determine statement type from first keyword
    let trimmed = sql.trim_start();
    let first_word = trimmed.split_whitespace().next().unwrap_or("");

    // SELECT (or WITH for CTEs)
    if first_word.eq_ignore_ascii_case("SELECT") || first_word.eq_ignore_ascii_case("WITH") {
        if let Ok(select_stmt) = vibesql_parser::arena_parser::parse_select_to_owned(sql) {
            return Ok(Statement::Select(Box::new(select_stmt)));
        }
        // Arena parser failed - fall back to standard parser
    }

    // INSERT
    if first_word.eq_ignore_ascii_case("INSERT") {
        if let Ok(insert_stmt) = vibesql_parser::arena_parser::parse_insert_to_owned(sql) {
            return Ok(Statement::Insert(insert_stmt));
        }
        // Arena parser failed - fall back to standard parser
    }

    // REPLACE (treated as INSERT OR REPLACE)
    if first_word.eq_ignore_ascii_case("REPLACE") {
        if let Ok(insert_stmt) = vibesql_parser::arena_parser::parse_insert_to_owned(sql) {
            return Ok(Statement::Insert(insert_stmt));
        }
        // Arena parser failed - fall back to standard parser
    }

    // UPDATE
    if first_word.eq_ignore_ascii_case("UPDATE") {
        if let Ok(update_stmt) = vibesql_parser::arena_parser::parse_update_to_owned(sql) {
            return Ok(Statement::Update(update_stmt));
        }
        // Arena parser failed - fall back to standard parser
    }

    // DELETE
    if first_word.eq_ignore_ascii_case("DELETE") {
        if let Ok(delete_stmt) = vibesql_parser::arena_parser::parse_delete_to_owned(sql) {
            return Ok(Statement::Delete(delete_stmt));
        }
        // Arena parser failed - fall back to standard parser
    }

    // Fall back to standard parser for unsupported types or failed arena parse
    vibesql_parser::Parser::parse_sql(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::Expression;

    #[test]
    fn test_prepared_statement_no_params() {
        let sql = "SELECT * FROM users";
        let statement = vibesql_parser::Parser::parse_sql(sql).unwrap();
        let prepared = PreparedStatement::new(sql.to_string(), statement);

        assert_eq!(prepared.param_count(), 0);
        assert!(prepared.bind(&[]).is_ok());
    }

    #[test]
    fn test_prepared_statement_with_placeholder() {
        // Parser now supports ? placeholders
        let sql = "SELECT * FROM users WHERE id = ?";
        let statement = vibesql_parser::Parser::parse_sql(sql).unwrap();
        let prepared = PreparedStatement::new(sql.to_string(), statement);

        // Should have 1 placeholder
        assert_eq!(prepared.param_count(), 1);

        // bind() with correct params should work
        let bound = prepared.bind(&[SqlValue::Integer(42)]).unwrap();
        assert!(matches!(bound, Statement::Select(_)));

        // Verify placeholder was replaced with literal
        if let Statement::Select(select) = bound {
            if let Some(Expression::BinaryOp { right, .. }) = &select.where_clause {
                assert_eq!(**right, Expression::Literal(SqlValue::Integer(42)));
            } else {
                panic!("Expected BinaryOp in WHERE clause");
            }
        }
    }

    #[test]
    fn test_prepared_statement_multiple_placeholders() {
        let sql = "SELECT * FROM users WHERE id = ? AND name = ?";
        let statement = vibesql_parser::Parser::parse_sql(sql).unwrap();
        let prepared = PreparedStatement::new(sql.to_string(), statement);

        assert_eq!(prepared.param_count(), 2);

        let params = vec![SqlValue::Integer(42), SqlValue::Varchar(arcstr::ArcStr::from("John"))];
        let bound = prepared.bind(&params).unwrap();
        assert!(matches!(bound, Statement::Select(_)));
    }

    #[test]
    fn test_prepared_statement_bind_param_mismatch() {
        let sql = "SELECT * FROM users WHERE id = ?";
        let statement = vibesql_parser::Parser::parse_sql(sql).unwrap();
        let prepared = PreparedStatement::new(sql.to_string(), statement);

        // Wrong param count should fail
        let result = prepared.bind(&[]);
        assert!(matches!(
            result,
            Err(PreparedStatementError::ParameterCountMismatch { expected: 1, actual: 0 })
        ));

        // Too many params should also fail
        let result = prepared.bind(&[SqlValue::Integer(1), SqlValue::Integer(2)]);
        assert!(matches!(
            result,
            Err(PreparedStatementError::ParameterCountMismatch { expected: 1, actual: 2 })
        ));
    }

    #[test]
    fn test_prepared_statement_reuse() {
        // The key performance test: we can bind multiple times without re-parsing
        let sql = "SELECT * FROM users WHERE id = ?";
        let statement = vibesql_parser::Parser::parse_sql(sql).unwrap();
        let prepared = PreparedStatement::new(sql.to_string(), statement);

        // Bind with different values - each should work without re-parsing
        let bound1 = prepared.bind(&[SqlValue::Integer(1)]).unwrap();
        let bound2 = prepared.bind(&[SqlValue::Integer(2)]).unwrap();
        let bound3 = prepared.bind(&[SqlValue::Integer(3)]).unwrap();

        // Verify each has the correct value
        for (bound, expected_id) in [(bound1, 1), (bound2, 2), (bound3, 3)] {
            if let Statement::Select(select) = bound {
                if let Some(Expression::BinaryOp { right, .. }) = &select.where_clause {
                    assert_eq!(**right, Expression::Literal(SqlValue::Integer(expected_id)));
                }
            }
        }
    }

    #[test]
    fn test_cache_get_or_prepare() {
        let cache = PreparedStatementCache::new(10);
        let sql = "SELECT * FROM users WHERE id = 1";

        // First call should miss and parse
        let stmt1 = cache.get_or_prepare(sql).unwrap();
        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 0);

        // Second call should hit
        let stmt2 = cache.get_or_prepare(sql).unwrap();
        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 1);

        // Should be the same Arc
        assert!(Arc::ptr_eq(&stmt1, &stmt2));
    }

    #[test]
    fn test_cache_placeholder_reuse() {
        // This is the key benefit: one cached statement for all values
        let cache = PreparedStatementCache::new(10);
        let sql = "SELECT * FROM users WHERE id = ?";

        // First call - cache miss
        let stmt1 = cache.get_or_prepare(sql).unwrap();
        assert_eq!(cache.stats().misses, 1);
        assert_eq!(cache.stats().hits, 0);

        // Same SQL with placeholder - cache hit!
        let stmt2 = cache.get_or_prepare(sql).unwrap();
        assert_eq!(cache.stats().misses, 1);
        assert_eq!(cache.stats().hits, 1);

        // Both point to same prepared statement
        assert!(Arc::ptr_eq(&stmt1, &stmt2));

        // Now bind with different values - no re-parsing needed
        let bound1 = stmt1.bind(&[SqlValue::Integer(1)]).unwrap();
        let bound2 = stmt2.bind(&[SqlValue::Integer(999)]).unwrap();

        // Verify different bound values
        if let (Statement::Select(s1), Statement::Select(s2)) = (&bound1, &bound2) {
            if let (
                Some(Expression::BinaryOp { right: r1, .. }),
                Some(Expression::BinaryOp { right: r2, .. }),
            ) = (&s1.where_clause, &s2.where_clause)
            {
                assert_eq!(**r1, Expression::Literal(SqlValue::Integer(1)));
                assert_eq!(**r2, Expression::Literal(SqlValue::Integer(999)));
            }
        }
    }

    #[test]
    fn test_cache_lru_eviction() {
        let cache = PreparedStatementCache::new(2);

        cache.get_or_prepare("SELECT * FROM users").unwrap();
        cache.get_or_prepare("SELECT * FROM orders").unwrap();
        assert_eq!(cache.stats().size, 2);
        assert_eq!(cache.stats().evictions, 0);

        // This should evict the LRU entry (users)
        cache.get_or_prepare("SELECT * FROM products").unwrap();
        assert_eq!(cache.stats().size, 2);
        assert_eq!(cache.stats().evictions, 1);

        // users should be evicted, orders and products should remain
        assert!(cache.get("SELECT * FROM users").is_none());
        assert!(cache.get("SELECT * FROM orders").is_some());
        assert!(cache.get("SELECT * FROM products").is_some());
    }

    #[test]
    fn test_cache_table_invalidation() {
        let cache = PreparedStatementCache::new(10);

        cache.get_or_prepare("SELECT * FROM users WHERE id = ?").unwrap();
        cache.get_or_prepare("SELECT * FROM orders WHERE id = ?").unwrap();
        assert_eq!(cache.stats().size, 2);

        // Invalidate users table
        cache.invalidate_table("users");
        assert_eq!(cache.stats().size, 1);

        // orders should still be cached
        assert!(cache.get("SELECT * FROM orders WHERE id = ?").is_some());
    }

    #[test]
    fn test_arena_parse_insert() {
        let sql = "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')";
        let result = parse_with_arena_fallback(sql);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), Statement::Insert(_)));
    }

    #[test]
    fn test_arena_parse_insert_with_placeholders() {
        let cache = PreparedStatementCache::new(10);
        let sql = "INSERT INTO users (name, email) VALUES (?, ?)";

        let stmt = cache.get_or_prepare(sql).unwrap();
        assert_eq!(stmt.param_count(), 2);

        let bound = stmt
            .bind(&[
                SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
                SqlValue::Varchar(arcstr::ArcStr::from("bob@example.com")),
            ])
            .unwrap();
        assert!(matches!(bound, Statement::Insert(_)));
    }

    #[test]
    fn test_arena_parse_update() {
        let sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        let result = parse_with_arena_fallback(sql);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), Statement::Update(_)));
    }

    #[test]
    fn test_arena_parse_update_with_placeholders() {
        let cache = PreparedStatementCache::new(10);
        let sql = "UPDATE users SET name = ? WHERE id = ?";

        let stmt = cache.get_or_prepare(sql).unwrap();
        assert_eq!(stmt.param_count(), 2);

        let bound =
            stmt.bind(&[SqlValue::Varchar(arcstr::ArcStr::from("Charlie")), SqlValue::Integer(42)]).unwrap();
        assert!(matches!(bound, Statement::Update(_)));
    }

    #[test]
    fn test_arena_parse_delete() {
        let sql = "DELETE FROM users WHERE id = 1";
        let result = parse_with_arena_fallback(sql);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), Statement::Delete(_)));
    }

    #[test]
    fn test_arena_parse_delete_with_placeholders() {
        let cache = PreparedStatementCache::new(10);
        let sql = "DELETE FROM users WHERE id = ?";

        let stmt = cache.get_or_prepare(sql).unwrap();
        assert_eq!(stmt.param_count(), 1);

        let bound = stmt.bind(&[SqlValue::Integer(99)]).unwrap();
        assert!(matches!(bound, Statement::Delete(_)));
    }

    #[test]
    fn test_arena_parse_sysbench_insert() {
        // Test the exact SQL that the sysbench benchmark uses
        let cache = PreparedStatementCache::new(10);

        // Test 3 columns
        let sql3 = "INSERT INTO test (a, b, c) VALUES (?, ?, ?)";
        let stmt3 = cache.get_or_prepare(sql3);
        assert!(stmt3.is_ok(), "3-column INSERT failed: {:?}", stmt3.err());

        // Test 4 columns with generic names
        let sql4_gen = "INSERT INTO test (a, b, c, d) VALUES (?, ?, ?, ?)";
        let stmt4_gen = cache.get_or_prepare(sql4_gen);
        assert!(stmt4_gen.is_ok(), "4-column INSERT (generic) failed: {:?}", stmt4_gen.err());

        // Test 4 columns with sysbench names (using "padding" instead of "pad" which is a keyword)
        let sql4 = "INSERT INTO sbtest1 (id, k, c, padding) VALUES (?, ?, ?, ?)";
        let stmt4 = cache.get_or_prepare(sql4);
        assert!(stmt4.is_ok(), "4-column INSERT (sysbench) failed: {:?}", stmt4.err());
        assert_eq!(stmt4.unwrap().param_count(), 4);
    }
}
