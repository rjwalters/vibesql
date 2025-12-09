//! Database registry for shared database instances across connections.
//!
//! This module provides a registry that maps database names to shared database
//! instances, allowing multiple connections to the same database to share data.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use vibesql_storage::Database;

/// Shared database handle that can be cloned across connections.
pub type SharedDatabase = Arc<RwLock<Database>>;

/// Registry managing shared database instances.
///
/// When a connection requests a database by name, the registry either returns
/// an existing shared instance or creates a new one. This ensures all connections
/// to the same database name share the same data.
#[derive(Clone)]
pub struct DatabaseRegistry {
    databases: Arc<RwLock<HashMap<String, SharedDatabase>>>,
}

impl DatabaseRegistry {
    /// Create a new empty database registry.
    pub fn new() -> Self {
        Self { databases: Arc::new(RwLock::new(HashMap::new())) }
    }

    /// Get or create a shared database instance for the given name.
    ///
    /// If a database with the given name already exists, returns a clone of
    /// its shared handle. Otherwise, creates a new database and returns it.
    pub async fn get_or_create(&self, name: &str) -> SharedDatabase {
        // First try read lock to check if database exists
        {
            let databases = self.databases.read().await;
            if let Some(db) = databases.get(name) {
                return Arc::clone(db);
            }
        }

        // Need to create - acquire write lock
        let mut databases = self.databases.write().await;

        // Double-check after acquiring write lock (another task may have created it)
        if let Some(db) = databases.get(name) {
            return Arc::clone(db);
        }

        // Create new database
        let db = Arc::new(RwLock::new(Database::new()));
        databases.insert(name.to_string(), Arc::clone(&db));
        db
    }

    /// Get a shared database instance if it exists.
    ///
    /// Returns None if no database with the given name exists.
    #[allow(dead_code)]
    pub async fn get(&self, name: &str) -> Option<SharedDatabase> {
        let databases = self.databases.read().await;
        databases.get(name).cloned()
    }

    /// List all database names in the registry.
    #[allow(dead_code)]
    pub async fn list_databases(&self) -> Vec<String> {
        let databases = self.databases.read().await;
        databases.keys().cloned().collect()
    }

    /// Get the number of databases in the registry.
    #[allow(dead_code)]
    pub async fn database_count(&self) -> usize {
        let databases = self.databases.read().await;
        databases.len()
    }

    /// Register a pre-built database instance.
    ///
    /// This is useful for benchmarks where the database is pre-loaded with data
    /// before starting the server.
    pub async fn register_database(&self, name: &str, db: Database) {
        let mut databases = self.databases.write().await;
        databases.insert(name.to_string(), Arc::new(RwLock::new(db)));
    }
}

impl Default for DatabaseRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_or_create_new_database() {
        let registry = DatabaseRegistry::new();

        let db1 = registry.get_or_create("testdb").await;
        assert_eq!(registry.database_count().await, 1);

        // Same name should return the same database
        let db2 = registry.get_or_create("testdb").await;
        assert_eq!(registry.database_count().await, 1);

        // Verify they point to the same database
        assert!(Arc::ptr_eq(&db1, &db2));
    }

    #[tokio::test]
    async fn test_different_databases() {
        let registry = DatabaseRegistry::new();

        let db1 = registry.get_or_create("db1").await;
        let db2 = registry.get_or_create("db2").await;

        assert_eq!(registry.database_count().await, 2);
        assert!(!Arc::ptr_eq(&db1, &db2));
    }

    #[tokio::test]
    async fn test_shared_data_across_connections() {
        let registry = DatabaseRegistry::new();

        // Simulate two connections to the same database
        let db1 = registry.get_or_create("shared").await;
        let db2 = registry.get_or_create("shared").await;

        // Create a table through first "connection"
        {
            let mut db = db1.write().await;
            let schema = vibesql_catalog::TableSchema::new(
                "users".to_string(),
                vec![vibesql_catalog::ColumnSchema::new(
                    "id".to_string(),
                    vibesql_types::DataType::Integer,
                    true,
                )],
            );
            db.create_table(schema).unwrap();
        }

        // Should be visible through second "connection"
        {
            let db = db2.read().await;
            assert!(db.get_table("users").is_some());
        }
    }

    #[tokio::test]
    async fn test_list_databases() {
        let registry = DatabaseRegistry::new();

        registry.get_or_create("alpha").await;
        registry.get_or_create("beta").await;
        registry.get_or_create("gamma").await;

        let names = registry.list_databases().await;
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"alpha".to_string()));
        assert!(names.contains(&"beta".to_string()));
        assert!(names.contains(&"gamma".to_string()));
    }
}
