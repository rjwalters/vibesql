// ============================================================================
// Index Manager - Core coordination and query methods
// ============================================================================

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use vibesql_types::{DataType, SqlValue};

use super::index_metadata::{acquire_btree_lock, normalize_index_name, IndexData, IndexMetadata};
use crate::btree::{BTreeIndex, Key};
use crate::database::{DatabaseConfig, ResourceTracker};
use crate::page::PageManager;
use crate::{Row, StorageBackend, StorageError};

#[cfg(not(target_arch = "wasm32"))]
use crate::NativeStorage;

#[cfg(target_arch = "wasm32")]
use crate::OpfsStorage;

#[cfg(target_arch = "wasm32")]
use crate::backend::MemoryStorage;

/// Manages user-defined indexes (CREATE INDEX statements)
///
/// This component encapsulates all user-defined index operations, maintaining
/// index metadata and data structures for efficient query optimization.
///
/// Supports adaptive index management with resource budgets and LRU eviction,
/// enabling efficient operation in both browser (limited memory) and server
/// (abundant memory) environments.
#[derive(Clone)]
pub struct IndexManager {
    /// Index metadata storage (normalized_index_name -> metadata)
    pub(super) indexes: HashMap<String, IndexMetadata>,
    /// Actual index data (normalized_index_name -> data)
    pub(super) index_data: HashMap<String, IndexData>,
    /// Resource budget configuration
    pub(super) config: DatabaseConfig,
    /// Resource usage tracker for budget enforcement
    pub(crate) resource_tracker: ResourceTracker,
    /// Database directory path for index file storage
    pub(super) database_path: Option<PathBuf>,
    /// Storage backend for file operations
    pub(super) storage: Arc<dyn StorageBackend>,
}

impl std::fmt::Debug for IndexManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexManager")
            .field("indexes", &self.indexes)
            .field("index_data", &self.index_data)
            .field("config", &self.config)
            .field("resource_tracker", &self.resource_tracker)
            .field("database_path", &self.database_path)
            .finish()
    }
}

impl IndexManager {
    /// Create a new empty IndexManager with default configuration
    pub fn new() -> Self {
        // Create a default in-memory storage (will be replaced when database_path is set)
        #[cfg(not(target_arch = "wasm32"))]
        let storage = Arc::new(NativeStorage::new(".").unwrap());
        #[cfg(target_arch = "wasm32")]
        let storage = Arc::new(MemoryStorage::new());

        IndexManager {
            indexes: HashMap::new(),
            index_data: HashMap::new(),
            config: DatabaseConfig::default(),
            resource_tracker: ResourceTracker::new(),
            database_path: None,
            storage,
        }
    }

    /// Create a new IndexManager with custom configuration
    pub fn with_config(config: DatabaseConfig) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        let storage = Arc::new(NativeStorage::new(".").unwrap());
        #[cfg(target_arch = "wasm32")]
        let storage = Arc::new(MemoryStorage::new());

        IndexManager {
            indexes: HashMap::new(),
            index_data: HashMap::new(),
            config,
            resource_tracker: ResourceTracker::new(),
            database_path: None,
            storage,
        }
    }

    /// Set the database directory path for index file storage
    pub fn set_database_path(&mut self, path: PathBuf) {
        // Update storage backend to use the correct path
        #[cfg(not(target_arch = "wasm32"))]
        {
            if let Ok(storage) = NativeStorage::new(&path) {
                self.storage = Arc::new(storage);
            }
        }
        #[cfg(target_arch = "wasm32")]
        {
            // OPFS doesn't use directory paths the same way
            // Keep the existing storage (will be initialized via init_opfs_async)
            let _ = path; // Suppress unused variable warning
        }
        self.database_path = Some(path);
    }

    /// Initialize OPFS storage asynchronously (WASM only)
    ///
    /// This replaces the temporary in-memory storage with persistent OPFS storage.
    /// Must be called from an async context.
    ///
    /// # Returns
    /// Ok on successful initialization, Err if OPFS is not supported or initialization fails
    #[cfg(target_arch = "wasm32")]
    pub async fn init_opfs_async(&mut self) -> Result<(), StorageError> {
        let opfs_storage = OpfsStorage::new_async().await.map_err(|e| StorageError::from(e))?;

        self.storage = Arc::new(opfs_storage);
        Ok(())
    }

    /// Set the resource budget configuration
    pub fn set_config(&mut self, config: DatabaseConfig) {
        self.config = config;
    }

    /// Reset the index manager to empty state (clears all indexes).
    ///
    /// Clears all index metadata and data but preserves configuration
    /// (database path, storage backend, and resource budgets).
    /// This is more efficient than creating a new instance and ensures
    /// disk-backed indexes continue to work after reset.
    pub fn reset(&mut self) {
        self.indexes.clear();
        self.index_data.clear();
        self.resource_tracker = ResourceTracker::new();
    }

    /// Check if an index exists
    pub fn index_exists(&self, index_name: &str) -> bool {
        let normalized = normalize_index_name(index_name);
        self.indexes.contains_key(&normalized)
    }

    /// Check if any indexes exist for a specific table
    ///
    /// This is an O(n) operation over all indexes but is useful for
    /// optimizing bulk insert operations when no indexes need updating.
    pub fn has_indexes_for_table(&self, table_name: &str) -> bool {
        let search_name_upper = table_name.to_uppercase();
        let search_table_only = search_name_upper.rsplit('.').next().unwrap_or(&search_name_upper);

        self.indexes.values().any(|metadata| {
            let stored_name_upper = metadata.table_name.to_uppercase();
            let stored_table_only = stored_name_upper.rsplit('.').next().unwrap_or(&stored_name_upper);
            stored_table_only == search_table_only
        })
    }

    /// Get index metadata
    pub fn get_index(&self, index_name: &str) -> Option<&IndexMetadata> {
        let normalized = normalize_index_name(index_name);
        self.indexes.get(&normalized)
    }

    /// Get index data
    pub fn get_index_data(&self, index_name: &str) -> Option<&IndexData> {
        let normalized = normalize_index_name(index_name);

        // Record access for LRU tracking (uses interior mutability)
        self.resource_tracker.record_access(&normalized);

        self.index_data.get(&normalized)
    }

    /// Check unique constraints for user-defined indexes before insert
    /// This should be called BEFORE adding the row to the table
    pub fn check_unique_constraints_for_insert(
        &self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        row: &Row,
    ) -> Result<(), StorageError> {
        for (index_name, metadata) in &self.indexes {
            if metadata.table_name == table_name && metadata.unique {
                if let Some(index_data) = self.index_data.get(index_name) {
                    // Build composite key from the indexed columns
                    // Apply prefix truncation and normalize numeric types to ensure consistent comparison
                    let key_values: Vec<SqlValue> = metadata
                        .columns
                        .iter()
                        .map(|col| {
                            let col_idx = table_schema
                                .get_column_index(&col.column_name)
                                .expect("Index column should exist");
                            let value = &row.values[col_idx];
                            let truncated = super::index_maintenance::apply_prefix_truncation(
                                value,
                                col.prefix_length,
                            );
                            crate::database::indexes::index_operations::normalize_for_comparison(
                                &truncated,
                            )
                        })
                        .collect();

                    // Check if key already exists (skip NULLs)
                    if !key_values.contains(&SqlValue::Null) {
                        match index_data {
                            IndexData::InMemory { data, .. } => {
                                if data.contains_key(&key_values) {
                                    let column_names: Vec<String> = metadata
                                        .columns
                                        .iter()
                                        .map(|c| c.column_name.clone())
                                        .collect();
                                    return Err(StorageError::UniqueConstraintViolation(format!(
                                        "UNIQUE constraint '{}' violated: duplicate key value for ({})",
                                        index_name,
                                        column_names.join(", ")
                                    )));
                                }
                            }
                            IndexData::DiskBacked { btree, .. } => {
                                // Safely acquire lock and check if key exists in B+tree
                                let guard = acquire_btree_lock(btree)?;
                                if let Ok(row_ids) = guard.lookup(&key_values) {
                                    if !row_ids.is_empty() {
                                        let column_names: Vec<String> = metadata
                                            .columns
                                            .iter()
                                            .map(|c| c.column_name.clone())
                                            .collect();
                                        return Err(StorageError::UniqueConstraintViolation(format!(
                                            "UNIQUE constraint '{}' violated: duplicate key value for ({})",
                                            index_name,
                                            column_names.join(", ")
                                        )));
                                    }
                                }
                            }
                            IndexData::IVFFlat { .. } => {
                                // IVFFlat indexes don't support unique constraints
                                // Vector indexes are for similarity search, not uniqueness
                            }
                            IndexData::Hnsw { .. } => {
                                // HNSW indexes don't support unique constraints
                                // Vector indexes are for similarity search, not uniqueness
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<String> {
        self.indexes.keys().cloned().collect()
    }

    // ========================================================================
    // Resource Budget and Eviction Methods
    // ========================================================================

    /// Get the file path for an index file
    pub(super) fn get_index_file_path(
        &self,
        table_name: &str,
        index_name: &str,
    ) -> Result<PathBuf, StorageError> {
        let index_dir = self
            .database_path
            .as_ref()
            .map(|p| p.join("indexes"))
            .unwrap_or_else(|| std::env::temp_dir().join("vibesql_indexes"));

        // Create indexes directory if needed
        std::fs::create_dir_all(&index_dir).map_err(|e| {
            StorageError::IoError(format!("Failed to create index directory: {}", e))
        })?;

        // Sanitize names for filesystem
        let safe_table = table_name.replace('/', "_");
        let safe_index = index_name.replace('/', "_");
        Ok(index_dir.join(format!("{}_{}.idx", safe_table, safe_index)))
    }

    /// Estimate memory usage for an index
    pub(super) fn estimate_index_memory(&self, row_count: usize, key_size: usize) -> usize {
        // Rough estimate: (key_size + Vec<usize> overhead) * row_count
        // Add BTreeMap overhead (~32 bytes per entry)
        (key_size + std::mem::size_of::<Vec<usize>>() + 32) * row_count
    }

    /// Enforce memory budget by evicting cold indexes if needed
    pub fn enforce_memory_budget(&mut self) -> Result<(), StorageError> {
        use crate::database::SpillPolicy;

        // Track previous memory to detect lack of progress (avoid infinite loop)
        let mut last_memory_used = self.resource_tracker.memory_used();

        while self.resource_tracker.memory_used() > self.config.memory_budget {
            match self.config.spill_policy {
                SpillPolicy::Reject => {
                    return Err(StorageError::MemoryBudgetExceeded {
                        used: self.resource_tracker.memory_used(),
                        budget: self.config.memory_budget,
                    });
                }
                SpillPolicy::SpillToDisk => {
                    // Find coldest in-memory index and spill it
                    let coldest = self
                        .resource_tracker
                        .find_coldest_in_memory_index()
                        .ok_or(StorageError::NoIndexToEvict)?;

                    self.spill_index_to_disk(&coldest.0)?;

                    // Check if we made progress - if memory didn't decrease, break to avoid infinite loop
                    let current_memory = self.resource_tracker.memory_used();
                    if current_memory >= last_memory_used {
                        // No progress made (index was already disk-backed or spill failed)
                        // This can happen if the only remaining in-memory index is the one
                        // we just created and it's already been spilled
                        break;
                    }
                    last_memory_used = current_memory;
                }
                SpillPolicy::BestEffort => {
                    // Try to spill, but don't fail if we can't
                    if let Some((coldest, _)) = self.resource_tracker.find_coldest_in_memory_index()
                    {
                        let _ = self.spill_index_to_disk(&coldest);
                    } else {
                        // No more indexes to evict, give up
                        break;
                    }

                    // Check for progress in BestEffort mode too
                    let current_memory = self.resource_tracker.memory_used();
                    if current_memory >= last_memory_used {
                        break;
                    }
                    last_memory_used = current_memory;
                }
            }
        }

        Ok(())
    }

    /// Convert an InMemory index to DiskBacked (eviction/spilling)
    fn spill_index_to_disk(&mut self, index_name: &str) -> Result<(), StorageError> {
        // Get the index data
        let index_data = self
            .index_data
            .remove(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        // Extract InMemory data, or return if already DiskBacked or IVFFlat
        let data = match index_data {
            IndexData::InMemory { data, .. } => data,
            IndexData::DiskBacked { .. } => {
                // Already disk-backed, just put it back
                self.index_data.insert(index_name.to_string(), index_data);
                return Ok(());
            }
            IndexData::IVFFlat { .. } => {
                // IVFFlat indexes can't be spilled to disk-backed B-tree format
                // They have a different structure (inverted lists + centroids)
                self.index_data.insert(index_name.to_string(), index_data);
                return Ok(());
            }
            IndexData::Hnsw { .. } => {
                // HNSW indexes can't be spilled to disk-backed B-tree format
                // They have a different structure (multi-layer proximity graph)
                self.index_data.insert(index_name.to_string(), index_data);
                return Ok(());
            }
        };

        // Get metadata for this index
        let metadata = self
            .indexes
            .get(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?
            .clone();

        // Create disk-backed version
        let index_file = self.get_index_file_path(&metadata.table_name, index_name)?;
        let index_file_str = index_file
            .to_str()
            .ok_or_else(|| StorageError::IoError("Invalid index file path".to_string()))?;

        let page_manager =
            Arc::new(PageManager::new(index_file_str, self.storage.clone()).map_err(|e| {
                StorageError::IoError(format!("Failed to create index file: {}", e))
            })?);

        // Convert BTreeMap to sorted entries for bulk_load
        // Use native duplicate key support - don't extend keys with row_id
        let mut sorted_entries: Vec<(Key, usize)> = Vec::new();
        for (key, row_indices) in &data {
            for &row_idx in row_indices {
                sorted_entries.push((key.clone(), row_idx));
            }
        }
        sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Build key schema from metadata
        // Note: We need access to table schema to get column data types
        // For now, we'll estimate based on SqlValue types in the data
        let key_schema: Vec<DataType> = if let Some((first_key, _)) = sorted_entries.first() {
            first_key
                .iter()
                .map(|v| match v {
                    SqlValue::Null => DataType::Integer, // Placeholder
                    SqlValue::Integer(_)
                    | SqlValue::Smallint(_)
                    | SqlValue::Bigint(_)
                    | SqlValue::Unsigned(_) => DataType::Integer,
                    SqlValue::Real(_)
                    | SqlValue::Float(_)
                    | SqlValue::Double(_)
                    | SqlValue::Numeric(_) => DataType::Real,
                    SqlValue::Character(_) | SqlValue::Varchar(_) => {
                        DataType::Varchar { max_length: None }
                    }
                    _ => DataType::Integer, // Fallback for other types
                })
                .collect()
        } else {
            // Empty index, use Integer as placeholder
            vec![DataType::Integer; metadata.columns.len()]
        };

        // Bulk load into B+ tree
        let btree = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager.clone())
            .map_err(|e| StorageError::IoError(format!("Failed to bulk load index: {}", e)))?;

        // Calculate disk size (approximate)
        let disk_bytes = if let Ok(file_meta) = std::fs::metadata(&index_file) {
            file_meta.len() as usize
        } else {
            0
        };

        // Replace with disk-backed version
        #[cfg(not(target_arch = "wasm32"))]
        let disk_backed =
            IndexData::DiskBacked { btree: Arc::new(parking_lot::Mutex::new(btree)), page_manager };

        #[cfg(target_arch = "wasm32")]
        let disk_backed =
            IndexData::DiskBacked { btree: Arc::new(std::sync::Mutex::new(btree)), page_manager };

        self.index_data.insert(index_name.to_string(), disk_backed);

        // Update resource tracking
        self.resource_tracker.mark_spilled(index_name, disk_bytes);

        Ok(())
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}
