//! Cache management and integration utilities

use std::{collections::HashSet, sync::Arc};

use vibesql_storage::Row;

use super::{QueryPlanCache, QueryResultCache, QuerySignature};
use crate::schema::CombinedSchema;

/// Simplified cache manager for common operations
pub struct CacheManager {
    plan_cache: Arc<QueryPlanCache>,
    result_cache: Arc<QueryResultCache>,
}

impl CacheManager {
    /// Create a new cache manager with separate plan and result caches
    pub fn new(max_size: usize) -> Self {
        Self::with_separate_limits(max_size, max_size)
    }

    /// Create cache manager with different limits for plans and results
    pub fn with_separate_limits(plan_cache_size: usize, result_cache_size: usize) -> Self {
        Self {
            plan_cache: Arc::new(QueryPlanCache::new(plan_cache_size)),
            result_cache: Arc::new(QueryResultCache::new(result_cache_size)),
        }
    }

    /// Get or create cached query plan
    /// Note: The creator function should return the parsed query, not execute it
    pub fn get_or_create<F>(&self, query: &str, creator: F) -> Result<String, String>
    where
        F: FnOnce() -> Result<String, String>,
    {
        let signature = QuerySignature::from_sql(query);

        // Try cache hit
        if let Some(cached) = self.plan_cache.get(&signature) {
            return Ok(cached);
        }

        // Cache miss - create new plan
        let plan = creator()?;
        self.plan_cache.insert(signature, plan.clone());
        Ok(plan)
    }

    /// Get cached query result
    pub fn get_result(&self, signature: &QuerySignature) -> Option<(Vec<Row>, CombinedSchema)> {
        self.result_cache.get(signature)
    }

    /// Insert query result into cache
    pub fn insert_result(
        &self,
        signature: QuerySignature,
        rows: Vec<Row>,
        schema: CombinedSchema,
        tables: HashSet<String>,
    ) {
        self.result_cache.insert(signature, rows, schema, tables);
    }

    /// Invalidate all plans and results referencing a table
    pub fn invalidate_table(&self, table_name: &str) {
        self.plan_cache.invalidate_table(table_name);
        self.result_cache.invalidate_table(table_name);
    }

    /// Clear all cached plans and results
    pub fn clear(&self) {
        self.plan_cache.clear();
        self.result_cache.clear();
    }

    /// Get plan cache statistics
    pub fn plan_stats(&self) -> super::CacheStats {
        self.plan_cache.stats()
    }

    /// Get result cache statistics
    pub fn result_stats(&self) -> super::CacheStats {
        self.result_cache.stats()
    }

    /// Get combined cache statistics (for backwards compatibility)
    pub fn stats(&self) -> super::CacheStats {
        self.plan_stats()
    }
}

/// Query execution context with caching
pub struct CachedQueryContext {
    manager: CacheManager,
}

impl CachedQueryContext {
    /// Create a new cached query context
    pub fn new(max_cache_size: usize) -> Self {
        Self { manager: CacheManager::new(max_cache_size) }
    }

    /// Execute a query using cache
    pub fn execute<F>(&self, query: &str, executor: F) -> Result<Vec<String>, String>
    where
        F: FnOnce(&str) -> Result<Vec<String>, String>,
    {
        let plan = self.manager.get_or_create(query, || Ok(format!("plan_for_{}", query)))?;

        executor(&plan)
    }

    /// Get cache statistics
    pub fn stats(&self) -> super::CacheStats {
        self.manager.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::{LiteralValue, ParameterizedPlan},
        *,
    };

    #[test]
    fn test_cache_manager_get_or_create() {
        let manager = CacheManager::new(10);

        let result1 = manager.get_or_create("SELECT * FROM users", || Ok("plan1".to_string()));

        assert!(result1.is_ok());
        assert_eq!(result1.unwrap(), "plan1");

        // Second call should hit cache
        let result2 = manager.get_or_create("SELECT * FROM users", || Ok("plan2".to_string()));

        assert!(result2.is_ok());
        assert_eq!(result2.unwrap(), "plan1"); // Same as first
    }

    #[test]
    fn test_cache_manager_invalidate_table() {
        let manager = CacheManager::new(10);

        // Note: get_or_create doesn't track table dependencies automatically
        // Table invalidation only works when using insert_with_tables directly
        // or when the cache implementation extracts table names from queries

        // Insert with explicit table tracking
        let signature = super::QuerySignature::from_sql("SELECT * FROM users");
        let mut tables = std::collections::HashSet::new();
        tables.insert("users".to_string());
        manager.plan_cache.insert_with_tables(signature, "SELECT * FROM users".to_string(), tables);

        assert_eq!(manager.stats().size, 1);

        manager.invalidate_table("users");
        let stats = manager.stats();
        assert_eq!(stats.size, 0);
    }

    #[test]
    fn test_cache_manager_no_table_tracking_by_default() {
        // Document that get_or_create doesn't automatically track table dependencies
        let manager = CacheManager::new(10);

        manager
            .get_or_create("SELECT * FROM users", || Ok("SELECT * FROM users".to_string()))
            .unwrap();

        // Invalidation won't work because tables aren't tracked
        manager.invalidate_table("users");
        let stats = manager.stats();

        // Entry still exists (size = 1) because no tables were associated
        assert_eq!(stats.size, 1);
    }

    #[test]
    fn test_cache_manager_clear() {
        let manager = CacheManager::new(10);

        manager.get_or_create("SELECT * FROM users", || Ok("plan".to_string())).unwrap();

        assert_eq!(manager.stats().size, 1);
        manager.clear();
        assert_eq!(manager.stats().size, 0);
    }

    #[test]
    fn test_cached_query_context() {
        let ctx = CachedQueryContext::new(10);

        let result = ctx.execute("SELECT * FROM users", |plan| Ok(vec![plan.to_string()]));

        assert!(result.is_ok());
    }

    #[test]
    fn test_repeated_query_patterns() {
        let manager = CacheManager::new(100);
        let mut call_count = 0;

        // First query
        manager
            .get_or_create("SELECT col0 FROM tab WHERE col1 > 5", || {
                call_count += 1;
                Ok("plan1".to_string())
            })
            .unwrap();

        // Same query - should hit cache
        manager
            .get_or_create("SELECT col0 FROM tab WHERE col1 > 5", || {
                call_count += 1;
                Ok("plan1".to_string())
            })
            .unwrap();

        // Should hit cache (exact match)
        assert_eq!(call_count, 1);
        assert_eq!(manager.stats().hits, 1);
    }

    #[test]
    fn test_cache_manager_with_error() {
        let manager = CacheManager::new(10);

        let result =
            manager.get_or_create("SELECT * FROM users", || Err("creation failed".to_string()));

        assert!(result.is_err());
    }

    #[test]
    fn test_parameterized_plan_example() {
        let plan = ParameterizedPlan::new(
            "SELECT * FROM users WHERE age > ?".to_string(),
            vec![super::super::ParameterPosition { position: 40, context: "age".to_string() }],
            vec![LiteralValue::Integer(25)],
        );

        let bound1 = plan.bind(&[LiteralValue::Integer(30)]).unwrap();
        let bound2 = plan.bind(&[LiteralValue::Integer(40)]).unwrap();

        assert_eq!(bound1, "SELECT * FROM users WHERE age > 30");
        assert_eq!(bound2, "SELECT * FROM users WHERE age > 40");
    }
}
