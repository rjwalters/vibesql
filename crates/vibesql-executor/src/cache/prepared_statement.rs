//! Prepared statement caching for optimized query execution
//!
//! Caches parsed AST statements to avoid repeated parsing overhead.
//! This provides significant performance benefits for repeated queries by:
//! - Caching the parsed AST for identical SQL strings
//! - Avoiding expensive parsing for each query execution
//!
//! ## Current Limitations
//!
//! The SQL parser does not currently support `?` placeholders directly.
//! Parameterized queries should substitute values before calling `prepare()`.
//! The cache key is the exact SQL string, so queries with different literal
//! values will be cached separately.
//!
//! ## Future Enhancement
//!
//! True parameterized query support would require parser changes to:
//! - Accept `?` as valid expression tokens
//! - Store placeholder nodes in the AST
//! - Bind parameters at execution time without re-parsing

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use lru::LruCache;
use std::num::NonZeroUsize;
use vibesql_ast::Statement;
use vibesql_types::SqlValue;

use super::{extract_tables_from_statement, QuerySignature};

/// A prepared statement with cached AST
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
}

impl PreparedStatement {
    /// Create a new prepared statement from parsed AST
    pub fn new(sql: String, statement: Statement) -> Self {
        let signature = QuerySignature::from_ast(&statement);
        let param_count = sql.matches('?').count();
        let tables = extract_tables_from_statement(&statement);

        Self {
            sql,
            statement,
            signature,
            param_count,
            tables,
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

    /// Bind parameters to create an executable statement
    ///
    /// For statements without placeholders, returns a clone of the cached statement.
    /// For parameterized statements, substitutes `?` with actual values and re-parses.
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

        // Substitute parameters into SQL and re-parse
        let bound_sql = substitute_placeholders(&self.sql, params);
        vibesql_parser::Parser::parse_sql(&bound_sql)
            .map_err(|e| PreparedStatementError::ParseError(e.to_string()))
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

/// Substitute `?` placeholders with actual SQL values
fn substitute_placeholders(sql: &str, params: &[SqlValue]) -> String {
    let mut result = String::with_capacity(sql.len() + params.len() * 16);
    let mut param_idx = 0;
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '?' && param_idx < params.len() {
            // Substitute the placeholder with the parameter value
            result.push_str(&sql_value_to_sql(&params[param_idx]));
            param_idx += 1;
        } else if c == '\'' {
            // Handle string literals - don't substitute ? inside them
            result.push(c);
            while let Some(&next) = chars.peek() {
                chars.next();
                result.push(next);
                if next == '\'' {
                    // Check for escaped quote
                    if chars.peek() == Some(&'\'') {
                        chars.next();
                        result.push('\'');
                    } else {
                        break;
                    }
                }
            }
        } else {
            result.push(c);
        }
    }

    result
}

/// Convert SqlValue to SQL string representation
fn sql_value_to_sql(value: &SqlValue) -> String {
    match value {
        SqlValue::Integer(n) => n.to_string(),
        SqlValue::Smallint(n) => n.to_string(),
        SqlValue::Bigint(n) => n.to_string(),
        SqlValue::Unsigned(n) => n.to_string(),
        SqlValue::Numeric(n) => n.to_string(),
        SqlValue::Float(n) => n.to_string(),
        SqlValue::Real(n) => n.to_string(),
        SqlValue::Double(n) => n.to_string(),
        SqlValue::Character(s) | SqlValue::Varchar(s) => {
            format!("'{}'", s.replace('\'', "''"))
        }
        SqlValue::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        SqlValue::Date(d) => format!("DATE '{}'", d),
        SqlValue::Time(t) => format!("TIME '{}'", t),
        SqlValue::Timestamp(ts) => format!("TIMESTAMP '{}'", ts),
        SqlValue::Interval(i) => format!("INTERVAL '{}'", i),
        SqlValue::Null => "NULL".to_string(),
    }
}

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

    #[test]
    fn test_substitute_placeholders_simple() {
        let sql = "SELECT * FROM users WHERE id = ?";
        let params = vec![SqlValue::Integer(42)];
        let result = substitute_placeholders(sql, &params);
        assert_eq!(result, "SELECT * FROM users WHERE id = 42");
    }

    #[test]
    fn test_substitute_placeholders_multiple() {
        let sql = "SELECT * FROM users WHERE id = ? AND name = ?";
        let params = vec![SqlValue::Integer(42), SqlValue::Varchar("John".to_string())];
        let result = substitute_placeholders(sql, &params);
        assert_eq!(result, "SELECT * FROM users WHERE id = 42 AND name = 'John'");
    }

    #[test]
    fn test_substitute_placeholders_string_escape() {
        let sql = "SELECT * FROM users WHERE name = ?";
        let params = vec![SqlValue::Varchar("O'Brien".to_string())];
        let result = substitute_placeholders(sql, &params);
        assert_eq!(result, "SELECT * FROM users WHERE name = 'O''Brien'");
    }

    #[test]
    fn test_substitute_placeholders_in_string_literal() {
        // ? inside string literal should not be substituted
        let sql = "SELECT '?' AS question, id FROM users WHERE id = ?";
        let params = vec![SqlValue::Integer(1)];
        let result = substitute_placeholders(sql, &params);
        assert_eq!(result, "SELECT '?' AS question, id FROM users WHERE id = 1");
    }

    #[test]
    fn test_prepared_statement_no_params() {
        let sql = "SELECT * FROM users";
        let statement = vibesql_parser::Parser::parse_sql(sql).unwrap();
        let prepared = PreparedStatement::new(sql.to_string(), statement);

        assert_eq!(prepared.param_count(), 0);
        assert!(prepared.bind(&[]).is_ok());
    }

    #[test]
    fn test_prepared_statement_with_literal() {
        // Parser doesn't support ? placeholders, so we test with literal values
        let sql = "SELECT * FROM users WHERE id = 42";
        let statement = vibesql_parser::Parser::parse_sql(sql).unwrap();
        let prepared = PreparedStatement::new(sql.to_string(), statement);

        // No ? placeholders means param_count is 0
        assert_eq!(prepared.param_count(), 0);

        // bind() with empty params returns the cached statement
        let bound = prepared.bind(&[]).unwrap();
        assert!(matches!(bound, Statement::Select(_)));
    }

    #[test]
    fn test_prepared_statement_bind_no_params() {
        let sql = "SELECT * FROM users";
        let statement = vibesql_parser::Parser::parse_sql(sql).unwrap();
        let prepared = PreparedStatement::new(sql.to_string(), statement);

        // Wrong param count should fail
        let result = prepared.bind(&[SqlValue::Integer(42)]);
        assert!(matches!(
            result,
            Err(PreparedStatementError::ParameterCountMismatch { expected: 0, actual: 1 })
        ));
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
    fn test_cache_different_literals_different_entries() {
        let cache = PreparedStatementCache::new(10);

        // Different literal values = different cache entries
        cache.get_or_prepare("SELECT * FROM users WHERE id = 1").unwrap();
        cache.get_or_prepare("SELECT * FROM users WHERE id = 2").unwrap();
        assert_eq!(cache.stats().size, 2);
        assert_eq!(cache.stats().misses, 2);
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
    fn test_cache_lru_access_updates_order() {
        let cache = PreparedStatementCache::new(2);

        cache.get_or_prepare("SELECT * FROM users").unwrap();
        cache.get_or_prepare("SELECT * FROM orders").unwrap();

        // Access users to make it recently used
        cache.get("SELECT * FROM users");

        // Now insert products - orders should be evicted (it's now LRU)
        cache.get_or_prepare("SELECT * FROM products").unwrap();

        // users should still be in cache (was accessed), orders should be evicted
        assert!(cache.get("SELECT * FROM users").is_some());
        assert!(cache.get("SELECT * FROM orders").is_none());
        assert!(cache.get("SELECT * FROM products").is_some());
    }

    #[test]
    fn test_cache_table_invalidation() {
        let cache = PreparedStatementCache::new(10);

        cache.get_or_prepare("SELECT * FROM users WHERE id = 1").unwrap();
        cache.get_or_prepare("SELECT * FROM orders WHERE id = 1").unwrap();
        assert_eq!(cache.stats().size, 2);

        // Invalidate users table
        cache.invalidate_table("users");
        assert_eq!(cache.stats().size, 1);

        // orders should still be cached
        assert!(cache.get("SELECT * FROM orders WHERE id = 1").is_some());
    }
}
