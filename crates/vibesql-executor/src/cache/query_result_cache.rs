//! Thread-safe query result cache with LRU eviction
//!
//! Caches actual query results (rows + schema) to avoid re-executing
//! identical read-only queries. This is particularly effective for
//! SQLLogicTest workloads with repeated query patterns.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        RwLock,
    },
};

use vibesql_storage::Row;

use super::{CacheStats, QuerySignature};
use crate::schema::CombinedSchema;

/// Cached query result with metadata
#[derive(Clone)]
struct CachedResult {
    /// Query result rows
    rows: Vec<Row>,
    /// Schema for the result
    schema: CombinedSchema,
    /// Tables accessed by this query (for invalidation)
    tables: HashSet<String>,
}

/// Thread-safe cache for query results
///
/// Caches the actual result rows and schema from SELECT queries.
/// Results are invalidated when any referenced table is modified.
pub struct QueryResultCache {
    cache: RwLock<HashMap<QuerySignature, CachedResult>>,
    max_size: usize,
    hits: AtomicUsize,
    misses: AtomicUsize,
    evictions: AtomicUsize,
}

impl QueryResultCache {
    /// Create a new result cache with specified max size
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            max_size,
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
            evictions: AtomicUsize::new(0),
        }
    }

    /// Try to get cached result for a query
    pub fn get(&self, signature: &QuerySignature) -> Option<(Vec<Row>, CombinedSchema)> {
        let cache = self.cache.read().unwrap();
        if let Some(entry) = cache.get(signature) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some((entry.rows.clone(), entry.schema.clone()))
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert query result into cache with table dependencies
    pub fn insert(
        &self,
        signature: QuerySignature,
        rows: Vec<Row>,
        schema: CombinedSchema,
        tables: HashSet<String>,
    ) {
        let entry = CachedResult { rows, schema, tables };
        let mut cache = self.cache.write().unwrap();

        // Simple LRU: evict first entry if at capacity
        if cache.len() >= self.max_size {
            if let Some(key) = cache.keys().next().cloned() {
                cache.remove(&key);
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }

        cache.insert(signature, entry);
    }

    /// Check if signature is cached
    pub fn contains(&self, signature: &QuerySignature) -> bool {
        self.cache.read().unwrap().contains_key(signature)
    }

    /// Clear all cached results
    pub fn clear(&self) {
        self.cache.write().unwrap().clear();
    }

    /// Invalidate all queries touching a specific table
    ///
    /// This should be called when a table is modified (INSERT/UPDATE/DELETE)
    pub fn invalidate_table(&self, table: &str) {
        let mut cache = self.cache.write().unwrap();
        cache.retain(|_, entry| !entry.tables.iter().any(|t| t.eq_ignore_ascii_case(table)));
    }

    /// Invalidate entire cache (e.g., on any write when not tracking dependencies)
    pub fn invalidate_all(&self) {
        self.clear();
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let cache = self.cache.read().unwrap();
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 { hits as f64 / total as f64 } else { 0.0 };

        CacheStats {
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
    use vibesql_types::SqlValue;

    fn make_test_row(values: Vec<SqlValue>) -> Row {
        Row::new(values)
    }

    fn make_test_schema() -> CombinedSchema {
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;

        let columns = vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "name".to_string(),
                data_type: DataType::Varchar { max_length: Some(255) },
                nullable: true,
                default_value: None,
            },
        ];

        let schema = TableSchema::new("users".to_string(), columns);
        CombinedSchema::from_table("users".to_string(), schema)
    }

    #[test]
    fn test_cache_hit() {
        let cache = QueryResultCache::new(10);
        let sig = QuerySignature::from_sql("SELECT * FROM users");
        let rows = vec![
            make_test_row(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
            make_test_row(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
        ];
        let schema = make_test_schema();
        let mut tables = HashSet::new();
        tables.insert("users".to_string());

        cache.insert(sig.clone(), rows.clone(), schema.clone(), tables);
        let result = cache.get(&sig);

        assert!(result.is_some());
        let (cached_rows, _cached_schema) = result.unwrap();
        assert_eq!(cached_rows.len(), 2);
        assert_eq!(cached_rows[0].values.len(), 2);
    }

    #[test]
    fn test_cache_miss() {
        let cache = QueryResultCache::new(10);
        let sig = QuerySignature::from_sql("SELECT * FROM users");

        let result = cache.get(&sig);
        assert!(result.is_none());
    }

    #[test]
    fn test_lru_eviction() {
        let cache = QueryResultCache::new(2);
        let schema = make_test_schema();
        let rows = vec![make_test_row(vec![SqlValue::Integer(1)])];
        let tables = HashSet::new();

        let sig1 = QuerySignature::from_sql("SELECT * FROM users");
        let sig2 = QuerySignature::from_sql("SELECT * FROM orders");
        let sig3 = QuerySignature::from_sql("SELECT * FROM products");

        cache.insert(sig1, rows.clone(), schema.clone(), tables.clone());
        cache.insert(sig2, rows.clone(), schema.clone(), tables.clone());
        assert_eq!(cache.stats().size, 2);

        cache.insert(sig3, rows, schema, tables);
        assert_eq!(cache.stats().size, 2);
        assert_eq!(cache.stats().evictions, 1);
    }

    #[test]
    fn test_cache_clear() {
        let cache = QueryResultCache::new(10);
        let sig = QuerySignature::from_sql("SELECT * FROM users");
        let rows = vec![make_test_row(vec![SqlValue::Integer(1)])];
        let schema = make_test_schema();
        let tables = HashSet::new();

        cache.insert(sig.clone(), rows, schema, tables);
        assert!(cache.contains(&sig));

        cache.clear();
        assert!(!cache.contains(&sig));
    }

    #[test]
    fn test_table_invalidation() {
        let cache = QueryResultCache::new(10);
        let sig = QuerySignature::from_sql("SELECT * FROM users WHERE id = 1");
        let rows = vec![make_test_row(vec![SqlValue::Integer(1)])];
        let schema = make_test_schema();
        let mut tables = HashSet::new();
        tables.insert("users".to_string());

        cache.insert(sig.clone(), rows, schema, tables);
        assert!(cache.contains(&sig));

        cache.invalidate_table("users");
        assert!(!cache.contains(&sig));
    }

    #[test]
    fn test_table_invalidation_case_insensitive() {
        let cache = QueryResultCache::new(10);
        let sig = QuerySignature::from_sql("SELECT * FROM users");
        let rows = vec![make_test_row(vec![SqlValue::Integer(1)])];
        let schema = make_test_schema();
        let mut tables = HashSet::new();
        tables.insert("users".to_string());

        cache.insert(sig.clone(), rows, schema, tables);
        assert!(cache.contains(&sig));

        // Invalidate with different case
        cache.invalidate_table("USERS");
        assert!(!cache.contains(&sig));
    }

    #[test]
    fn test_cache_stats() {
        let cache = QueryResultCache::new(10);
        let sig = QuerySignature::from_sql("SELECT * FROM users");
        let rows = vec![make_test_row(vec![SqlValue::Integer(1)])];
        let schema = make_test_schema();
        let tables = HashSet::new();

        cache.insert(sig.clone(), rows, schema, tables);

        // Generate hits
        cache.get(&sig);
        cache.get(&sig);

        // Generate miss
        let other_sig = QuerySignature::from_sql("SELECT * FROM orders");
        cache.get(&other_sig);

        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
        assert!((stats.hit_rate - 2.0 / 3.0).abs() < 0.01);
    }
}
