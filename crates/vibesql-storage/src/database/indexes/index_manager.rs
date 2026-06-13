// ============================================================================
// Index Manager - Core coordination and query methods
// ============================================================================

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use vibesql_types::{DataType, SqlValue};

use super::index_metadata::{acquire_btree_lock, normalize_index_name, IndexData, IndexMetadata};
#[cfg(target_arch = "wasm32")]
use crate::backend::MemoryStorage;
#[cfg(not(target_arch = "wasm32"))]
use crate::NativeStorage;
#[cfg(target_arch = "wasm32")]
use crate::OpfsStorage;
use crate::{
    btree::{BTreeIndex, Key},
    database::{DatabaseConfig, ResourceTracker},
    page::PageManager,
    Row, StorageBackend, StorageError,
};

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

    // ========================================================================
    // Disk-backed index transaction undo-logging (issue #5425)
    // ========================================================================
    //
    // `IndexData::DiskBacked` clones as a shallow `Arc<Mutex<BTreeIndex>>` bump,
    // so the copy-on-write `Operations` rollback snapshot (#5413/#5419) shares
    // the *same* live B+ tree as the transaction — restoring the shallow clone
    // does not undo spilled-index mutations. Instead, when a mutating
    // transaction begins we arm a per-tree undo-log; ROLLBACK reverses it and
    // COMMIT discards it. Cost is proportional to the transaction's own
    // mutations, not to the (by definition large) spilled index size.

    /// Arm transaction undo-logging on every disk-backed B+ tree.
    ///
    /// Called at the copy-on-write snapshot chokepoint, just before the first
    /// index mutation inside a transaction. In-memory / vector indexes are
    /// unaffected (they are restored by the snapshot clone as before).
    pub fn begin_disk_undo_logging(&mut self) {
        for index_data in self.index_data.values_mut() {
            if let IndexData::DiskBacked { btree, .. } = index_data {
                if let Ok(mut guard) = acquire_btree_lock(btree) {
                    guard.begin_undo_logging();
                }
            }
        }
    }

    /// Reverse and disable the undo-log on every disk-backed B+ tree
    /// (ROLLBACK path), restoring each spilled index to its pre-transaction
    /// state.
    pub fn rollback_disk_undo_logs(&mut self) {
        for index_data in self.index_data.values_mut() {
            if let IndexData::DiskBacked { btree, .. } = index_data {
                if let Ok(mut guard) = acquire_btree_lock(btree) {
                    if let Err(e) = guard.rollback_undo_log() {
                        log::warn!("Failed to roll back disk-backed index undo-log: {:?}", e);
                    }
                }
            }
        }
    }

    /// Disable and discard the undo-log on every disk-backed B+ tree
    /// (COMMIT path — the mutations are already persisted in the trees).
    pub fn clear_disk_undo_logs(&mut self) {
        for index_data in self.index_data.values_mut() {
            if let IndexData::DiskBacked { btree, .. } = index_data {
                if let Ok(mut guard) = acquire_btree_lock(btree) {
                    guard.clear_undo_log();
                }
            }
        }
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
        let search_name_lower = table_name.to_lowercase();
        let search_table_only = search_name_lower.rsplit('.').next().unwrap_or(&search_name_lower);

        self.indexes.values().any(|metadata| {
            let stored_name_lower = metadata.table_name.to_lowercase();
            let stored_table_only =
                stored_name_lower.rsplit('.').next().unwrap_or(&stored_name_lower);
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
    ///
    /// Partial UNIQUE indexes are skipped here — storage cannot evaluate the
    /// WHERE predicate to know whether the candidate row should even be in
    /// the index. The executor crate must perform partial-aware uniqueness
    /// checks before calling this method (see
    /// `partial_index_maintenance::check_partial_unique_for_insert`).
    pub fn check_unique_constraints_for_insert(
        &self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        row: &Row,
    ) -> Result<(), StorageError> {
        for (index_name, metadata) in &self.indexes {
            if metadata.table_name == table_name && metadata.unique && !metadata.is_partial() {
                // Skip expression indexes: storage cannot evaluate expressions
                // to build the key (expect_column_name would panic). The
                // executor crate maintains expression indexes separately (see
                // expression_index_maintenance). Observed via upsert1-800
                // where a UNIQUE expression index caused a panic on INSERT.
                if metadata.columns.iter().any(|col| col.is_expression()) {
                    continue;
                }
                if let Some(index_data) = self.index_data.get(index_name) {
                    // Build composite key from the indexed columns
                    // Apply prefix truncation and normalize numeric types to ensure consistent
                    // comparison
                    let key_values: Vec<SqlValue> = metadata
                        .columns
                        .iter()
                        .map(|col| {
                            let col_idx = table_schema
                                .get_column_index(col.expect_column_name())
                                .expect("Index column should exist");
                            let value = &row.values[col_idx];
                            let truncated = super::index_maintenance::apply_prefix_truncation(
                                value,
                                col.prefix_length(),
                            );
                            crate::database::indexes::index_operations::normalize_for_comparison(
                                &truncated,
                            )
                        })
                        .collect();

                    // Check if key already exists (skip NULLs)
                    if !key_values.contains(&SqlValue::Null) {
                        // Build SQLite-compatible column list: "table.col1, table.col2"
                        let columns_str = metadata
                            .columns
                            .iter()
                            .map(|col| {
                                format!("{}.{}", metadata.table_name, col.expect_column_name())
                            })
                            .collect::<Vec<_>>()
                            .join(", ");

                        match index_data {
                            IndexData::InMemory { data, .. } => {
                                if data.contains_key(&key_values) {
                                    // SQLite format: "UNIQUE constraint failed: table.col1, table.col2"
                                    return Err(StorageError::UniqueConstraintViolation(format!(
                                        "UNIQUE constraint failed: {}",
                                        columns_str
                                    )));
                                }
                            }
                            IndexData::DiskBacked { btree, .. } => {
                                // Safely acquire lock and check if key exists in B+tree
                                let guard = acquire_btree_lock(btree)?;
                                if let Ok(row_ids) = guard.lookup(&key_values) {
                                    if !row_ids.is_empty() {
                                        // SQLite format: "UNIQUE constraint failed: table.col1, table.col2"
                                        return Err(StorageError::UniqueConstraintViolation(
                                            format!("UNIQUE constraint failed: {}", columns_str),
                                        ));
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

    /// Attach (or clear) a partial-index WHERE clause on an existing
    /// storage-side index. Used by persistence/recovery paths that
    /// recreate indexes through the no-WHERE-clause path and then need to
    /// graft the partial predicate on afterwards. Returns `true` if a
    /// matching index was found and updated.
    ///
    /// Note: this does NOT re-evaluate the predicate against existing rows
    /// — the index body is left untouched. Callers that need to ensure the
    /// body only contains matching rows must rebuild the index (e.g.
    /// through the executor's CREATE INDEX path, or REINDEX).
    pub fn set_index_where_clause(
        &mut self,
        index_name: &str,
        where_clause: Option<Box<vibesql_ast::Expression>>,
    ) -> bool {
        let normalized = super::index_metadata::normalize_index_name(index_name);
        if let Some(meta) = self.indexes.get_mut(&normalized) {
            meta.where_clause = where_clause;
            true
        } else {
            false
        }
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

                    // Check if we made progress - if memory didn't decrease, break to avoid
                    // infinite loop
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
                    // Temporal keys must keep their temporal type: the
                    // persisted key_schema is what `first_key_value_sample`
                    // reports for disk-backed indexes, and a mis-typed
                    // (Integer) schema would silently disable temporal
                    // probe-bound coercion for spilled indexes (issue #5337).
                    SqlValue::Date(_) => DataType::Date,
                    SqlValue::Timestamp(_) => DataType::Timestamp { with_timezone: false },
                    SqlValue::Time(_) => DataType::Time { with_timezone: false },
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::{collections::BTreeMap, str::FromStr};

    use tempfile::TempDir;

    use super::*;

    /// Regression test for issue #5337 (caveat 1): `spill_index_to_disk`
    /// inferred the persisted key schema from the first key's `SqlValue`
    /// with an `_ => Integer` fallback, mis-typing temporal keys. A spilled
    /// temporal index would then report no temporal key type via
    /// `first_key_value_sample`, silently disabling probe-bound coercion.
    #[test]
    fn spill_preserves_temporal_key_schema() {
        let temp_dir = TempDir::new().unwrap();
        let mut manager = IndexManager::new();
        manager.set_database_path(temp_dir.path().to_path_buf());

        let index_name = "idx_ts";
        let ts = |s: &str| SqlValue::Timestamp(vibesql_types::Timestamp::from_str(s).unwrap());

        let mut data: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
        data.insert(vec![ts("2024-01-01 12:00:00.5")], vec![0]);
        data.insert(vec![ts("2024-06-15 08:30:00")], vec![1]);

        manager.indexes.insert(
            index_name.to_string(),
            IndexMetadata {
                index_name: index_name.to_string(),
                table_name: "events".to_string(),
                unique: false,
                columns: vec![vibesql_ast::IndexColumn::new_column(
                    "created_at".to_string(),
                    vibesql_ast::OrderDirection::Asc,
                )],
                where_clause: None,
            },
        );
        manager.index_data.insert(
            index_name.to_string(),
            IndexData::InMemory { data, pending_deletions: vec![] },
        );

        manager.spill_index_to_disk(index_name).unwrap();

        let spilled = manager.index_data.get(index_name).unwrap();
        match spilled {
            IndexData::DiskBacked { btree, .. } => {
                let guard = acquire_btree_lock(btree).unwrap();
                assert_eq!(
                    guard.key_schema(),
                    &[DataType::Timestamp { with_timezone: false }],
                    "spilled temporal index must persist a temporal key schema"
                );
            }
            other => panic!("expected DiskBacked after spill, got {:?}", other),
        }

        // The key type must remain detectable for probe-bound coercion.
        assert!(matches!(spilled.first_key_value_sample(0), Some(SqlValue::Timestamp(_))));
    }

    #[test]
    fn spill_preserves_date_and_time_key_schema() {
        let temp_dir = TempDir::new().unwrap();
        let mut manager = IndexManager::new();
        manager.set_database_path(temp_dir.path().to_path_buf());

        let cases: Vec<(&str, Vec<SqlValue>, DataType)> = vec![
            (
                "idx_date",
                vec![SqlValue::Date(vibesql_types::Date::from_str("2024-01-01").unwrap())],
                DataType::Date,
            ),
            (
                "idx_time",
                vec![SqlValue::Time(vibesql_types::Time::from_str("12:00:00").unwrap())],
                DataType::Time { with_timezone: false },
            ),
        ];

        for (index_name, key, expected_type) in cases {
            let mut data: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
            data.insert(key, vec![0]);

            manager.indexes.insert(
                index_name.to_string(),
                IndexMetadata {
                    index_name: index_name.to_string(),
                    table_name: "events".to_string(),
                    unique: false,
                    columns: vec![vibesql_ast::IndexColumn::new_column(
                        "c".to_string(),
                        vibesql_ast::OrderDirection::Asc,
                    )],
                    where_clause: None,
                },
            );
            manager.index_data.insert(
                index_name.to_string(),
                IndexData::InMemory { data, pending_deletions: vec![] },
            );

            manager.spill_index_to_disk(index_name).unwrap();

            match manager.index_data.get(index_name).unwrap() {
                IndexData::DiskBacked { btree, .. } => {
                    let guard = acquire_btree_lock(btree).unwrap();
                    assert_eq!(
                        guard.key_schema(),
                        std::slice::from_ref(&expected_type),
                        "{}",
                        index_name
                    );
                }
                other => panic!("expected DiskBacked after spill, got {:?}", other),
            }
        }
    }

    // ========================================================================
    // Spilled-index transaction rollback / commit (issue #5425)
    // ========================================================================

    use vibesql_catalog::{ColumnSchema, TableSchema};

    use crate::database::indexes::index_operations::normalize_for_comparison;

    /// Normalize an integer key the same way the insert/maintenance path does.
    fn normalize_int(k: i64) -> SqlValue {
        normalize_for_comparison(&SqlValue::Integer(k))
    }

    /// Build a manager holding a single-column integer index on table `t`,
    /// seeded with `(key, row_id)` entries, then spill it to disk. Returns the
    /// manager, the table schema, and the TempDir backing the index file.
    fn spilled_index_manager(
        index_name: &str,
        seed: &[(i64, usize)],
    ) -> (IndexManager, TableSchema, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let mut manager = IndexManager::new();
        manager.set_database_path(temp_dir.path().to_path_buf());

        let schema = TableSchema::new(
            "t".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );

        // Seed with normalized keys, matching how the real insert/maintenance
        // path stores them (integers normalize to Double — see
        // `normalize_for_comparison`). This keeps the seeded B+ tree, its
        // inferred key schema, and the maintenance-path mutations consistent.
        let mut data: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
        for (k, row_id) in seed {
            data.entry(vec![normalize_int(*k)]).or_default().push(*row_id);
        }

        manager.indexes.insert(
            index_name.to_string(),
            IndexMetadata {
                index_name: index_name.to_string(),
                table_name: "t".to_string(),
                unique: false,
                columns: vec![vibesql_ast::IndexColumn::new_column(
                    "id".to_string(),
                    vibesql_ast::OrderDirection::Asc,
                )],
                where_clause: None,
            },
        );
        manager.index_data.insert(
            index_name.to_string(),
            IndexData::InMemory { data, pending_deletions: vec![] },
        );

        manager.spill_index_to_disk(index_name).unwrap();
        match manager.index_data.get(index_name).unwrap() {
            IndexData::DiskBacked { .. } => {}
            other => panic!("expected DiskBacked after spill, got {:?}", other),
        }

        (manager, schema, temp_dir)
    }

    /// Read the row_ids stored under `key` in a disk-backed index.
    fn disk_lookup(manager: &IndexManager, index_name: &str, key: i64) -> Vec<usize> {
        match manager.index_data.get(index_name).unwrap() {
            IndexData::DiskBacked { btree, .. } => {
                let guard = acquire_btree_lock(btree).unwrap();
                let mut rows = guard.lookup(&vec![normalize_int(key)]).unwrap();
                rows.sort_unstable();
                rows
            }
            other => panic!("expected DiskBacked, got {:?}", other),
        }
    }

    fn row(id: i64) -> Row {
        Row::new(vec![SqlValue::Integer(id)])
    }

    /// #5425 core: a transaction that mutates a *spilled* (disk-backed) index
    /// and then ROLLBACKs must leave the index identical to its pre-transaction
    /// state. Before the undo-log fix the shallow `Arc<Mutex<BTreeIndex>>` clone
    /// meant rollback was a no-op for spilled indexes and these mutations
    /// survived.
    #[test]
    fn spilled_index_rollback_restores_state() {
        let (mut manager, schema, _dir) = spilled_index_manager("idx_t_id", &[(10, 0), (20, 1)]);

        // Begin the (simulated) transaction: arm undo-logging on disk-backed
        // indexes — exactly what the COW snapshot chokepoint does on the first
        // index mutation.
        manager.begin_disk_undo_logging();

        // Mutate the spilled index through the real maintenance entry points.
        manager.add_to_indexes_for_insert("t", &schema, &row(30), 2);
        manager.add_to_indexes_for_insert("t", &schema, &row(10), 3); // duplicate key
        manager.update_indexes_for_delete_with_values("t", &schema, &[SqlValue::Integer(20)], 1);

        // Sanity: mutations are visible mid-transaction.
        assert_eq!(disk_lookup(&manager, "idx_t_id", 30), vec![2]);
        assert_eq!(disk_lookup(&manager, "idx_t_id", 10), vec![0, 3]);
        assert!(disk_lookup(&manager, "idx_t_id", 20).is_empty());

        // ROLLBACK.
        manager.rollback_disk_undo_logs();

        assert!(
            disk_lookup(&manager, "idx_t_id", 30).is_empty(),
            "inserted key must be undone on ROLLBACK"
        );
        assert_eq!(
            disk_lookup(&manager, "idx_t_id", 10),
            vec![0],
            "duplicate row inserted in txn must be undone, original preserved"
        );
        assert_eq!(
            disk_lookup(&manager, "idx_t_id", 20),
            vec![1],
            "deleted key must be restored on ROLLBACK"
        );
    }

    /// #5425: COMMIT of a spilled-index transaction must persist the mutations
    /// (clearing the undo-log must not reverse anything).
    #[test]
    fn spilled_index_commit_persists_state() {
        let (mut manager, schema, _dir) = spilled_index_manager("idx_t_id", &[(10, 0), (20, 1)]);

        manager.begin_disk_undo_logging();
        manager.add_to_indexes_for_insert("t", &schema, &row(30), 2);
        manager.update_indexes_for_delete_with_values("t", &schema, &[SqlValue::Integer(20)], 1);

        // COMMIT: discard the undo-log without reversing.
        manager.clear_disk_undo_logs();

        assert_eq!(
            disk_lookup(&manager, "idx_t_id", 30),
            vec![2],
            "committed insert must persist on a spilled index"
        );
        assert!(
            disk_lookup(&manager, "idx_t_id", 20).is_empty(),
            "committed delete must persist on a spilled index"
        );
        assert_eq!(disk_lookup(&manager, "idx_t_id", 10), vec![0]);
    }

    /// #5425 + #5413/#5419: a transaction that mutates BOTH an in-memory index
    /// and a spilled (disk-backed) index on the same table must restore both on
    /// ROLLBACK. The in-memory index is restored by the copy-on-write snapshot
    /// clone (modeled here by cloning before mutating and restoring after);
    /// the disk-backed index is restored by the undo-log. This mirrors the
    /// `rollback_transaction` sequence (reverse disk undo-log, then restore the
    /// snapshot clone).
    #[test]
    fn mixed_in_memory_and_disk_backed_indexes_both_roll_back() {
        let temp_dir = TempDir::new().unwrap();
        let mut manager = IndexManager::new();
        manager.set_database_path(temp_dir.path().to_path_buf());

        let schema = TableSchema::new(
            "t".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );

        let mk_meta = |name: &str| IndexMetadata {
            index_name: name.to_string(),
            table_name: "t".to_string(),
            unique: false,
            columns: vec![vibesql_ast::IndexColumn::new_column(
                "id".to_string(),
                vibesql_ast::OrderDirection::Asc,
            )],
            where_clause: None,
        };

        // In-memory index (normalized keys, matching the maintenance path).
        let mut mem_data: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
        mem_data.entry(vec![normalize_int(10)]).or_default().push(0);
        manager.indexes.insert("idx_mem".to_string(), mk_meta("idx_mem"));
        manager.index_data.insert(
            "idx_mem".to_string(),
            IndexData::InMemory { data: mem_data, pending_deletions: vec![] },
        );

        // Disk-backed (spilled) index.
        let mut disk_data: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
        disk_data.entry(vec![normalize_int(10)]).or_default().push(0);
        manager.indexes.insert("idx_disk".to_string(), mk_meta("idx_disk"));
        manager.index_data.insert(
            "idx_disk".to_string(),
            IndexData::InMemory { data: disk_data, pending_deletions: vec![] },
        );
        manager.spill_index_to_disk("idx_disk").unwrap();

        // BEGIN: copy-on-write snapshot of the whole manager (restores in-memory
        // indexes), and arm undo-logging on disk-backed indexes.
        let snapshot = manager.clone();
        manager.begin_disk_undo_logging();

        // Mutate both indexes (one insert hits every matching index for the table).
        manager.add_to_indexes_for_insert("t", &schema, &row(30), 1);

        // Mid-txn both are mutated.
        match manager.index_data.get("idx_mem").unwrap() {
            IndexData::InMemory { data, .. } => {
                assert!(data.contains_key(&vec![normalize_int(30)]), "in-memory mutated");
            }
            other => panic!("expected InMemory, got {:?}", other),
        }
        assert_eq!(disk_lookup(&manager, "idx_disk", 30), vec![1], "disk-backed mutated");

        // ROLLBACK: reverse the disk-backed undo-log first, then restore the
        // snapshot clone (this is exactly what `rollback_transaction` does).
        manager.rollback_disk_undo_logs();
        manager = snapshot;

        // In-memory index restored by the snapshot clone.
        match manager.index_data.get("idx_mem").unwrap() {
            IndexData::InMemory { data, .. } => {
                assert!(!data.contains_key(&vec![normalize_int(30)]), "in-memory rolled back");
                assert!(data.contains_key(&vec![normalize_int(10)]));
            }
            other => panic!("expected InMemory, got {:?}", other),
        }
        // Disk-backed index restored by the undo-log (the snapshot clone shares
        // the same now-reverted B+ tree Arc).
        assert!(disk_lookup(&manager, "idx_disk", 30).is_empty(), "disk-backed rolled back");
        assert_eq!(disk_lookup(&manager, "idx_disk", 10), vec![0]);
    }

    // ========================================================================
    // Spilled-index full-tree rebuild rollback (issue #5435)
    // ========================================================================

    /// #5435: a full-tree `rebuild_indexes` on a *spilled* (disk-backed) index
    /// that runs *inside* a transaction must be reversible. This is reachable
    /// today: a DELETE that compacts away >50% of a table's rows triggers
    /// `Database::delete_row`'s `delete_result.compacted` branch, which calls
    /// `rebuild_indexes` while the disk undo-log is armed.
    ///
    /// Before this fix the rebuild swapped in a freshly `bulk_load`ed tree
    /// (`*guard = new_btree`), discarding the armed undo-log and replacing the
    /// live tree wholesale, so ROLLBACK could not restore the pre-rebuild
    /// contents. The fix rebuilds the same tree in place via logged
    /// delete/insert when undo-logging is armed.
    #[test]
    fn spilled_index_rebuild_in_txn_rolls_back() {
        // Seed: keys 10,20,30 at row positions 0,1,2.
        let (mut manager, schema, _dir) =
            spilled_index_manager("idx_t_id", &[(10, 0), (20, 1), (30, 2)]);

        // BEGIN: arm undo-logging (what the COW snapshot chokepoint does on the
        // first index mutation of a transaction).
        manager.begin_disk_undo_logging();

        // Simulate a DELETE that compacts the table: rows 20 and 30 were
        // deleted, so the live table now holds only key 10 — and compaction
        // shifted its position to row 0. `rebuild_indexes` rebuilds the index
        // from the post-compaction table rows.
        let post_compaction_rows = vec![row(10)];
        manager.rebuild_indexes("t", &schema, &post_compaction_rows);

        // Mid-transaction: the index reflects the rebuilt (post-compaction)
        // contents.
        assert_eq!(disk_lookup(&manager, "idx_t_id", 10), vec![0]);
        assert!(
            disk_lookup(&manager, "idx_t_id", 20).is_empty(),
            "rebuilt index drops the deleted key mid-txn"
        );
        assert!(
            disk_lookup(&manager, "idx_t_id", 30).is_empty(),
            "rebuilt index drops the deleted key mid-txn"
        );

        // ROLLBACK: the in-place logged rebuild must be fully reversed.
        manager.rollback_disk_undo_logs();

        assert_eq!(
            disk_lookup(&manager, "idx_t_id", 10),
            vec![0],
            "key 10 restored to its pre-rebuild position"
        );
        assert_eq!(
            disk_lookup(&manager, "idx_t_id", 20),
            vec![1],
            "key 20 dropped by the rebuild must be restored on ROLLBACK"
        );
        assert_eq!(
            disk_lookup(&manager, "idx_t_id", 30),
            vec![2],
            "key 30 dropped by the rebuild must be restored on ROLLBACK"
        );
    }

    /// #5435: COMMIT of a transaction whose spilled index was rebuilt must
    /// persist the rebuilt contents (clearing the undo-log must not reverse the
    /// rebuild).
    #[test]
    fn spilled_index_rebuild_in_txn_commits() {
        let (mut manager, schema, _dir) =
            spilled_index_manager("idx_t_id", &[(10, 0), (20, 1), (30, 2)]);

        manager.begin_disk_undo_logging();

        let post_compaction_rows = vec![row(10), row(30)];
        manager.rebuild_indexes("t", &schema, &post_compaction_rows);

        // COMMIT: discard the undo-log without reversing.
        manager.clear_disk_undo_logs();

        assert_eq!(disk_lookup(&manager, "idx_t_id", 10), vec![0]);
        assert!(
            disk_lookup(&manager, "idx_t_id", 20).is_empty(),
            "committed rebuild must persist the dropped key"
        );
        assert_eq!(
            disk_lookup(&manager, "idx_t_id", 30),
            vec![1],
            "committed rebuild must persist the new row position"
        );
    }

    /// #5435: outside a transaction (no undo-log armed) the rebuild still uses
    /// the fast wholesale `bulk_load` swap and produces correct contents.
    #[test]
    fn spilled_index_rebuild_without_txn_uses_fast_path() {
        let (mut manager, schema, _dir) =
            spilled_index_manager("idx_t_id", &[(10, 0), (20, 1), (30, 2)]);

        // No begin_disk_undo_logging(): the index is not undo-logging.
        match manager.index_data.get("idx_t_id").unwrap() {
            IndexData::DiskBacked { btree, .. } => {
                assert!(!acquire_btree_lock(btree).unwrap().is_undo_logging());
            }
            other => panic!("expected DiskBacked, got {:?}", other),
        }

        let new_rows = vec![row(10), row(40)];
        manager.rebuild_indexes("t", &schema, &new_rows);

        assert_eq!(disk_lookup(&manager, "idx_t_id", 10), vec![0]);
        assert_eq!(disk_lookup(&manager, "idx_t_id", 40), vec![1]);
        assert!(disk_lookup(&manager, "idx_t_id", 20).is_empty());
        assert!(disk_lookup(&manager, "idx_t_id", 30).is_empty());
    }

    // ========================================================================
    // Vector-index (IVFFlat / HNSW) rebuild after compaction (issue #5446)
    // ========================================================================

    use vibesql_ast::VectorDistanceMetric;

    /// Schema for a table `v` with an id column and a 2-D vector column `vec`.
    fn vector_schema() -> TableSchema {
        TableSchema::new(
            "v".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("vec".to_string(), DataType::Vector { dimensions: 2 }, true),
            ],
        )
    }

    /// Build a table row `(id, vec)` where `vec` is a 2-D vector.
    fn vrow(id: i64, x: f32, y: f32) -> Row {
        Row::new(vec![SqlValue::Integer(id), SqlValue::Vector(vec![x, y])])
    }

    /// `(row_id, vector)` pairs the create path expects, for the given table rows.
    fn vectors_from(rows: &[Row]) -> Vec<(usize, Vec<f64>)> {
        rows.iter()
            .enumerate()
            .map(|(i, r)| match &r.values[1] {
                SqlValue::Vector(v) => (i, v.iter().map(|&f| f as f64).collect::<Vec<f64>>()),
                other => panic!("expected vector, got {:?}", other),
            })
            .collect()
    }

    /// #5446: after a compacting DELETE renumbers rows, `rebuild_indexes` must
    /// rebuild an IVFFlat index from the post-compaction rows so its row_id
    /// references point at the *new* positions, not the stale pre-compaction
    /// ones.
    #[test]
    fn ivfflat_index_rebuilt_after_compaction() {
        let schema = vector_schema();
        let mut manager = IndexManager::new();

        // Pre-compaction table: 4 rows at positions 0..3.
        let pre_rows =
            vec![vrow(1, 0.0, 0.0), vrow(2, 10.0, 10.0), vrow(3, 0.1, 0.0), vrow(4, 10.0, 9.9)];

        manager
            .create_ivfflat_index_with_vectors(
                "idx_vec".to_string(),
                "v".to_string(),
                "vec".to_string(),
                2,
                2,
                VectorDistanceMetric::L2,
                vectors_from(&pre_rows),
            )
            .unwrap();
        manager.set_ivfflat_probes("idx_vec", 2).unwrap();

        // Simulate a DELETE that compacts away the two "far" rows (old positions
        // 1 and 3). The surviving rows (old ids 1 and 3) are renumbered to
        // positions 0 and 1.
        let post_rows = vec![vrow(1, 0.0, 0.0), vrow(3, 0.1, 0.0)];
        manager.rebuild_indexes("v", &schema, &post_rows);

        // Search near the origin: results must be the new positions {0, 1},
        // never the stale pre-compaction positions {2, 3}.
        let results = manager.search_ivfflat_index("idx_vec", &[0.05, 0.0], 2).unwrap();
        let mut ids: Vec<usize> = results.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![0, 1], "IVFFlat rebuild must map to compacted positions");

        // The index must hold exactly the two surviving vectors.
        match manager.index_data.get("idx_vec").unwrap() {
            IndexData::IVFFlat { index } => assert_eq!(index.len(), 2),
            other => panic!("expected IVFFlat, got {:?}", other),
        }
    }

    /// #5446: same as above for HNSW.
    #[test]
    fn hnsw_index_rebuilt_after_compaction() {
        let schema = vector_schema();
        let mut manager = IndexManager::new();

        let pre_rows =
            vec![vrow(1, 0.0, 0.0), vrow(2, 10.0, 10.0), vrow(3, 0.1, 0.0), vrow(4, 10.0, 9.9)];

        manager
            .create_hnsw_index_with_vectors(
                "idx_vec".to_string(),
                "v".to_string(),
                "vec".to_string(),
                2,
                16,
                64,
                VectorDistanceMetric::L2,
                vectors_from(&pre_rows),
            )
            .unwrap();

        let post_rows = vec![vrow(1, 0.0, 0.0), vrow(3, 0.1, 0.0)];
        manager.rebuild_indexes("v", &schema, &post_rows);

        let results = manager.search_hnsw_index("idx_vec", &[0.05, 0.0], 2).unwrap();
        let mut ids: Vec<usize> = results.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![0, 1], "HNSW rebuild must map to compacted positions");

        match manager.index_data.get("idx_vec").unwrap() {
            IndexData::Hnsw { index } => assert_eq!(index.len(), 2),
            other => panic!("expected Hnsw, got {:?}", other),
        }
    }

    /// #5446: vector indexes are in-memory and part of the COW snapshot of
    /// `Operations` (#5419/#5425). A rebuild that runs inside a transaction and
    /// then ROLLBACKs must restore the pre-rebuild index. This is modeled the
    /// same way the in-memory leg of `mixed_in_memory_and_disk_backed_indexes_*`
    /// is: clone the manager before the rebuild (the COW snapshot), then restore
    /// the clone on ROLLBACK.
    #[test]
    fn ivfflat_rebuild_in_txn_rolls_back_via_snapshot() {
        let schema = vector_schema();
        let mut manager = IndexManager::new();

        let pre_rows =
            vec![vrow(1, 0.0, 0.0), vrow(2, 10.0, 10.0), vrow(3, 0.1, 0.0), vrow(4, 10.0, 9.9)];
        manager
            .create_ivfflat_index_with_vectors(
                "idx_vec".to_string(),
                "v".to_string(),
                "vec".to_string(),
                2,
                2,
                VectorDistanceMetric::L2,
                vectors_from(&pre_rows),
            )
            .unwrap();
        manager.set_ivfflat_probes("idx_vec", 2).unwrap();

        // BEGIN: COW snapshot (deep clone of the whole manager, incl. the
        // in-memory vector index).
        let snapshot = manager.clone();

        // A compacting DELETE rebuilds the index mid-transaction.
        let post_rows = vec![vrow(1, 0.0, 0.0), vrow(3, 0.1, 0.0)];
        manager.rebuild_indexes("v", &schema, &post_rows);
        match manager.index_data.get("idx_vec").unwrap() {
            IndexData::IVFFlat { index } => assert_eq!(index.len(), 2, "rebuilt mid-txn"),
            other => panic!("expected IVFFlat, got {:?}", other),
        }

        // ROLLBACK: restore the snapshot clone.
        manager = snapshot;
        match manager.index_data.get("idx_vec").unwrap() {
            IndexData::IVFFlat { index } => {
                assert_eq!(index.len(), 4, "ROLLBACK restores the pre-rebuild vector index");
            }
            other => panic!("expected IVFFlat, got {:?}", other),
        }
    }

    /// #5446: COMMIT of a transaction whose vector index was rebuilt persists the
    /// rebuilt contents (the COW snapshot is simply dropped on COMMIT).
    #[test]
    fn ivfflat_rebuild_in_txn_commits() {
        let schema = vector_schema();
        let mut manager = IndexManager::new();

        let pre_rows =
            vec![vrow(1, 0.0, 0.0), vrow(2, 10.0, 10.0), vrow(3, 0.1, 0.0), vrow(4, 10.0, 9.9)];
        manager
            .create_ivfflat_index_with_vectors(
                "idx_vec".to_string(),
                "v".to_string(),
                "vec".to_string(),
                2,
                2,
                VectorDistanceMetric::L2,
                vectors_from(&pre_rows),
            )
            .unwrap();

        // BEGIN + rebuild; on COMMIT the snapshot is dropped (modeled by not
        // restoring it).
        let _snapshot = manager.clone();
        let post_rows = vec![vrow(1, 0.0, 0.0), vrow(3, 0.1, 0.0)];
        manager.rebuild_indexes("v", &schema, &post_rows);
        drop(_snapshot);

        match manager.index_data.get("idx_vec").unwrap() {
            IndexData::IVFFlat { index } => {
                assert_eq!(index.len(), 2, "COMMIT persists the rebuilt vector index");
            }
            other => panic!("expected IVFFlat, got {:?}", other),
        }
    }
}
