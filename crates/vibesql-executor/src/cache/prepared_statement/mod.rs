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
//! ```ignore
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

mod bind;
pub mod plan;

pub use plan::{CachedPlan, PkPointLookupPlan, ProjectionPlan, ColumnProjection};

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

        Self {
            sql,
            statement,
            signature,
            param_count,
            tables,
            cached_plan,
        }
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
    /// LRU cache mapping SQL string to prepared statement
    cache: Mutex<LruCache<String, Arc<PreparedStatement>>>,
    /// Maximum cache size
    max_size: usize,
    /// Cache hit count
    hits: AtomicUsize,
    /// Cache miss count
    misses: AtomicUsize,
    /// Cache eviction count
    evictions: AtomicUsize,
}

impl PreparedStatementCache {
    /// Create a new cache with specified max size
    pub fn new(max_size: usize) -> Self {
        let cap = NonZeroUsize::new(max_size).unwrap_or(NonZeroUsize::new(1).unwrap());
        Self {
            cache: Mutex::new(LruCache::new(cap)),
            max_size,
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
            evictions: AtomicUsize::new(0),
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
        let statement = vibesql_parser::Parser::parse_sql(sql)
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

    /// Clear all cached statements
    pub fn clear(&self) {
        self.cache.lock().unwrap().clear();
    }

    /// Invalidate all statements referencing a table
    pub fn invalidate_table(&self, table: &str) {
        let mut cache = self.cache.lock().unwrap();
        // Collect keys to remove (can't modify while iterating)
        let keys_to_remove: Vec<String> = cache
            .iter()
            .filter(|(_, stmt)| stmt.tables.iter().any(|t| t.eq_ignore_ascii_case(table)))
            .map(|(k, _)| k.clone())
            .collect();

        for key in keys_to_remove {
            cache.pop(&key);
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

        let params = vec![SqlValue::Integer(42), SqlValue::Varchar("John".to_string())];
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
}
