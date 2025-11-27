// ============================================================================
// Index Maintenance - CRUD operations for indexes
// ============================================================================

#![allow(clippy::doc_overindented_list_items)]

use std::collections::BTreeMap;
use std::sync::Arc;

use vibesql_types::{DataType, SqlValue};

use super::index_metadata::{acquire_btree_lock, normalize_index_name, IndexData, IndexMetadata, DISK_BACKED_THRESHOLD};
use super::index_manager::IndexManager;
use crate::btree::{BTreeIndex, Key};
use crate::page::PageManager;
use crate::{Row, StorageError};

/// Apply prefix truncation to a SqlValue if prefix_length is specified
///
/// For string types (Varchar, Char, Text), truncates to first N characters.
/// For other types, returns the value unchanged (prefix indexing only applies to strings).
///
/// # Arguments
/// * `value` - The value to potentially truncate
/// * `prefix_length` - Optional prefix length in characters
///
/// # Returns
/// Truncated value if applicable, otherwise the original value
pub(super) fn apply_prefix_truncation(value: &SqlValue, prefix_length: Option<u64>) -> SqlValue {
    // If no prefix length specified, return value as-is
    let Some(prefix_len) = prefix_length else {
        return value.clone();
    };

    // Only apply truncation to string types
    match value {
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // Take first N characters (UTF-8 aware)
            let truncated: String = s.chars().take(prefix_len as usize).collect();
            // Return same type as input
            match value {
                SqlValue::Varchar(_) => SqlValue::Varchar(truncated),
                SqlValue::Character(_) => SqlValue::Character(truncated),
                _ => unreachable!(),
            }
        }
        // For non-string types, prefix indexing doesn't apply
        _ => value.clone(),
    }
}

impl IndexManager {
    /// Create an index
    pub fn create_index(
        &mut self,
        index_name: String,
        table_name: String,
        table_schema: &vibesql_catalog::TableSchema,
        table_rows: &[Row],
        unique: bool,
        columns: Vec<vibesql_ast::IndexColumn>,
    ) -> Result<(), StorageError> {
        // Normalize index name for case-insensitive comparison
        let normalized_name = normalize_index_name(&index_name);

        // Check if index already exists
        if self.indexes.contains_key(&normalized_name) {
            return Err(StorageError::IndexAlreadyExists(index_name));
        }

        // Get column indices in the table for all indexed columns
        let mut column_indices = Vec::new();
        for index_col in &columns {
            let column_idx =
                table_schema.get_column_index(&index_col.column_name).ok_or_else(|| {
                    StorageError::ColumnNotFound {
                        column_name: index_col.column_name.clone(),
                        table_name: table_name.clone(),
                    }
                })?;
            column_indices.push(column_idx);
        }

        // Store index metadata (use normalized name as key)
        let metadata =
            IndexMetadata { index_name: index_name.clone(), table_name: table_name.clone(), unique, columns: columns.clone() };

        self.indexes.insert(normalized_name.clone(), metadata);

        // Choose backend based on table size
        // In test builds, DISK_BACKED_THRESHOLD is usize::MAX to disable disk-backed indexes
        #[allow(clippy::absurd_extreme_comparisons)]
        let use_disk_backed = table_rows.len() >= DISK_BACKED_THRESHOLD;

        let (index_data, memory_bytes, disk_bytes, backend) = if use_disk_backed {
            // Create disk-backed B+ tree index using proper database path
            let index_file = self.get_index_file_path(&table_name, &index_name)?;
            let index_file_str = index_file.to_str()
                .ok_or_else(|| StorageError::IoError("Invalid index file path".to_string()))?;

            let page_manager = Arc::new(PageManager::new(index_file_str, self.storage.clone())
                .map_err(|e| StorageError::IoError(format!("Failed to create index file: {}", e)))?);

            // Build key schema from indexed columns
            let key_schema: Vec<DataType> = column_indices
                .iter()
                .map(|&idx| table_schema.columns[idx].data_type.clone())
                .collect();

            // Prepare sorted entries for bulk loading
            // The BTreeIndex has native duplicate key support via Vec<RowId> per key,
            // so we don't need to extend keys with row_id for non-unique indexes
            let mut sorted_entries: Vec<(Key, usize)> = Vec::new();
            for (row_idx, row) in table_rows.iter().enumerate() {
                let key_values: Vec<SqlValue> = column_indices
                    .iter()
                    .zip(columns.iter())
                    .map(|(&idx, col)| {
                        let value = &row.values[idx];
                        let truncated = apply_prefix_truncation(value, col.prefix_length);
                        // Normalize numeric types to ensure consistent comparison with query bounds
                        super::index_operations::normalize_for_comparison(&truncated)
                    })
                    .collect();
                sorted_entries.push((key_values, row_idx));
            }
            // Sort by key for bulk_load
            sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

            // Use the same key schema for both unique and non-unique indexes
            // The BTreeIndex handles duplicates internally via Vec<RowId>
            let btree_key_schema = key_schema;

            // Use bulk_load for efficient index creation
            let btree = BTreeIndex::bulk_load(sorted_entries, btree_key_schema, page_manager.clone())
                .map_err(|e| StorageError::IoError(format!("Failed to bulk load index: {}", e)))?;

            // Calculate disk size
            let disk_bytes = if let Ok(file_meta) = std::fs::metadata(&index_file) {
                file_meta.len() as usize
            } else {
                0
            };

            #[cfg(not(target_arch = "wasm32"))]
            let data = IndexData::DiskBacked {
                btree: Arc::new(parking_lot::Mutex::new(btree)),
                page_manager,
            };

            #[cfg(target_arch = "wasm32")]
            let data = IndexData::DiskBacked {
                btree: Arc::new(std::sync::Mutex::new(btree)),
                page_manager,
            };

            (data, 0, disk_bytes, crate::database::IndexBackend::DiskBacked)
        } else {
            // Build the index data in-memory for small tables
            let mut index_data_map: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
            for (row_idx, row) in table_rows.iter().enumerate() {
                let key_values: Vec<SqlValue> = column_indices
                    .iter()
                    .zip(columns.iter())
                    .map(|(&idx, col)| {
                        let value = &row.values[idx];
                        let truncated = apply_prefix_truncation(value, col.prefix_length);
                        // Normalize numeric types to ensure consistent comparison with query bounds
                        super::index_operations::normalize_for_comparison(&truncated)
                    })
                    .collect();
                index_data_map.entry(key_values).or_default().push(row_idx);
            }

            // Estimate memory usage
            let key_size = std::mem::size_of::<Vec<SqlValue>>(); // Rough estimate
            let memory_bytes = self.estimate_index_memory(table_rows.len(), key_size);

            let data = IndexData::InMemory { data: index_data_map };

            (data, memory_bytes, 0, crate::database::IndexBackend::InMemory)
        };

        // Register the index with resource tracker
        self.resource_tracker.register_index(
            normalized_name.clone(),
            memory_bytes,
            disk_bytes,
            backend,
        );

        self.index_data.insert(normalized_name.clone(), index_data);

        // Enforce memory budget after creating index
        self.enforce_memory_budget()?;

        Ok(())
    }

    /// Add row to user-defined indexes after insert
    /// This should be called AFTER the row has been added to the table
    pub fn add_to_indexes_for_insert(
        &mut self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        row: &Row,
        row_index: usize,
    ) {
        for (index_name, metadata) in &self.indexes {
            if metadata.table_name == table_name {
                if let Some(index_data) = self.index_data.get_mut(index_name) {
                    // Build composite key from the indexed columns
                    // Normalize numeric types to ensure consistent comparison
                    let key_values: Vec<SqlValue> = metadata
                        .columns
                        .iter()
                        .map(|col| {
                            let col_idx = table_schema
                                .get_column_index(&col.column_name)
                                .expect("Index column should exist");
                            let value = &row.values[col_idx];
                            let truncated = apply_prefix_truncation(value, col.prefix_length);
                            // Normalize numeric types for consistent ordering/comparison
                            crate::database::indexes::index_operations::normalize_for_comparison(&truncated)
                        })
                        .collect();

                    // Insert into the index data
                    match index_data {
                        IndexData::InMemory { data } => {
                            data.entry(key_values).or_insert_with(Vec::new).push(row_index);
                        }
                        IndexData::DiskBacked { btree, .. } => {
                            // Safely acquire lock and insert into B+tree
                            // BTreeIndex now supports duplicate keys for non-unique indexes
                            match acquire_btree_lock(btree) {
                                Ok(mut guard) => {
                                    if let Err(e) = guard.insert(key_values, row_index) {
                                        // Log error if insert fails for other reasons
                                        log::warn!("Failed to insert into disk-backed index '{}': {:?}", index_name, e);
                                    }
                                }
                                Err(e) => {
                                    log::warn!("BTreeIndex lock acquisition failed in add_to_indexes_for_insert: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Update user-defined indexes for update operation
    pub fn update_indexes_for_update(
        &mut self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        old_row: &Row,
        new_row: &Row,
        row_index: usize,
    ) {
        for (index_name, metadata) in &self.indexes {
            if metadata.table_name == table_name {
                if let Some(index_data) = self.index_data.get_mut(index_name) {
                    // Build keys from old and new rows
                    // Normalize numeric types to ensure consistent comparison
                    let old_key_values: Vec<SqlValue> = metadata
                        .columns
                        .iter()
                        .map(|col| {
                            let col_idx = table_schema
                                .get_column_index(&col.column_name)
                                .expect("Index column should exist");
                            let value = &old_row.values[col_idx];
                            let truncated = apply_prefix_truncation(value, col.prefix_length);
                            crate::database::indexes::index_operations::normalize_for_comparison(&truncated)
                        })
                        .collect();

                    let new_key_values: Vec<SqlValue> = metadata
                        .columns
                        .iter()
                        .map(|col| {
                            let col_idx = table_schema
                                .get_column_index(&col.column_name)
                                .expect("Index column should exist");
                            let value = &new_row.values[col_idx];
                            let truncated = apply_prefix_truncation(value, col.prefix_length);
                            crate::database::indexes::index_operations::normalize_for_comparison(&truncated)
                        })
                        .collect();

                    // If keys are different, remove old and add new
                    if old_key_values != new_key_values {
                        match index_data {
                            IndexData::InMemory { data } => {
                                // Remove old key
                                if let Some(row_indices) = data.get_mut(&old_key_values) {
                                    row_indices.retain(|&idx| idx != row_index);
                                    // Remove empty entries
                                    if row_indices.is_empty() {
                                        data.remove(&old_key_values);
                                    }
                                }

                                // Add new key
                                data
                                    .entry(new_key_values)
                                    .or_insert_with(Vec::new)
                                    .push(row_index);
                            }
                            IndexData::DiskBacked { btree, .. } => {
                                // Safely acquire lock and update B+tree: delete old key, insert new key
                                match acquire_btree_lock(btree) {
                                    Ok(mut guard) => {
                                        let _ = guard.delete(&old_key_values);
                                        if let Err(e) = guard.insert(new_key_values, row_index) {
                                            log::warn!("Failed to update disk-backed index '{}': {:?}", index_name, e);
                                        }
                                    }
                                    Err(e) => {
                                        log::warn!("BTreeIndex lock acquisition failed in update_indexes_for_update: {}", e);
                                    }
                                }
                            }
                        }
                    }
                    // If keys are the same, no change needed
                }
            }
        }
    }

    /// Update user-defined indexes for delete operation
    pub fn update_indexes_for_delete(
        &mut self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        row: &Row,
        row_index: usize,
    ) {
        for (index_name, metadata) in &self.indexes {
            if metadata.table_name == table_name {
                if let Some(index_data) = self.index_data.get_mut(index_name) {
                    // Build key from the row
                    let key_values: Vec<SqlValue> = metadata
                        .columns
                        .iter()
                        .map(|col| {
                            let col_idx = table_schema
                                .get_column_index(&col.column_name)
                                .expect("Index column should exist");
                            let value = &row.values[col_idx];
                            let truncated = apply_prefix_truncation(value, col.prefix_length);
                            // Normalize numeric types for consistent ordering/comparison
                            crate::database::indexes::index_operations::normalize_for_comparison(&truncated)
                        })
                        .collect();

                    // Remove the row index from this key
                    match index_data {
                        IndexData::InMemory { data } => {
                            if let Some(row_indices) = data.get_mut(&key_values) {
                                row_indices.retain(|&idx| idx != row_index);
                                // Remove empty entries
                                if row_indices.is_empty() {
                                    data.remove(&key_values);
                                }
                            }
                        }
                        IndexData::DiskBacked { btree, .. } => {
                            // Safely acquire lock and delete from B+tree
                            match acquire_btree_lock(btree) {
                                Ok(mut guard) => {
                                    let _ = guard.delete(&key_values);
                                }
                                Err(e) => {
                                    log::warn!("BTreeIndex lock acquisition failed in update_indexes_for_delete: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Rebuild user-defined indexes after bulk operations that change row indices
    pub fn rebuild_indexes(
        &mut self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        table_rows: &[Row],
    ) {
        // Collect index names that need rebuilding
        let indexes_to_rebuild: Vec<String> = self
            .indexes
            .iter()
            .filter(|(_, metadata)| metadata.table_name == table_name)
            .map(|(name, _)| name.clone())
            .collect();

        // Rebuild each index
        for index_name in indexes_to_rebuild {
            if let Some(index_data) = self.index_data.get_mut(&index_name) {
                if let Some(metadata) = self.indexes.get(&index_name) {
                    match index_data {
                        IndexData::InMemory { data } => {
                            // Clear existing data
                            data.clear();

                            // Rebuild from current table rows
                            for (row_index, row) in table_rows.iter().enumerate() {
                                let key_values: Vec<SqlValue> = metadata
                                    .columns
                                    .iter()
                                    .map(|col| {
                                        let col_idx = table_schema
                                            .get_column_index(&col.column_name)
                                            .expect("Index column should exist");
                                        let value = &row.values[col_idx];
                                        let truncated = apply_prefix_truncation(value, col.prefix_length);
                                        // Normalize numeric types for consistent ordering/comparison
                                        crate::database::indexes::index_operations::normalize_for_comparison(&truncated)
                                    })
                                    .collect();

                                data.entry(key_values).or_insert_with(Vec::new).push(row_index);
                            }
                        }
                        IndexData::DiskBacked { btree, page_manager } => {
                            // For rebuild, we need to create a new B+tree from scratch
                            // First, collect all entries
                            let mut sorted_entries = Vec::new();
                            for (row_index, row) in table_rows.iter().enumerate() {
                                let key_values: Vec<SqlValue> = metadata
                                    .columns
                                    .iter()
                                    .map(|col| {
                                        let col_idx = table_schema
                                            .get_column_index(&col.column_name)
                                            .expect("Index column should exist");
                                        let value = &row.values[col_idx];
                                        let truncated = apply_prefix_truncation(value, col.prefix_length);
                                        // Normalize numeric types for consistent ordering/comparison
                                        crate::database::indexes::index_operations::normalize_for_comparison(&truncated)
                                    })
                                    .collect();
                                sorted_entries.push((key_values, row_index));
                            }

                            // Sort entries by key for bulk_load
                            sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

                            // Get key schema from metadata
                            let key_schema: Vec<DataType> = metadata
                                .columns
                                .iter()
                                .map(|col| {
                                    let col_idx = table_schema
                                        .get_column_index(&col.column_name)
                                        .expect("Index column should exist");
                                    table_schema.columns[col_idx].data_type.clone()
                                })
                                .collect();

                            // Use bulk_load to create new B+tree
                            if let Ok(new_btree) = BTreeIndex::bulk_load(
                                sorted_entries,
                                key_schema,
                                page_manager.clone(),
                            ) {
                                // Safely acquire lock and replace old btree with new one
                                match acquire_btree_lock(btree) {
                                    Ok(mut guard) => {
                                        *guard = new_btree;
                                    }
                                    Err(e) => {
                                        log::warn!("BTreeIndex lock acquisition failed in rebuild_indexes: {}", e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Drop an index
    pub fn drop_index(&mut self, index_name: &str) -> Result<(), StorageError> {
        // Normalize index name for case-insensitive comparison
        let normalized = normalize_index_name(index_name);

        if self.indexes.remove(&normalized).is_none() {
            return Err(StorageError::IndexNotFound(index_name.to_string()));
        }
        // Also remove the index data
        self.index_data.remove(&normalized);

        // Unregister from resource tracker
        self.resource_tracker.unregister_index(&normalized);

        Ok(())
    }

    /// Drop all indexes associated with a table (CASCADE behavior)
    ///
    /// This is called automatically when dropping a table to maintain
    /// referential integrity. Indexes are tied to specific tables and
    /// cannot exist without their parent table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The table name, which may be qualified (e.g., "public.users") or
    ///                  unqualified (e.g., "users"). Matching is case-insensitive and handles
    ///                  both qualified and unqualified names.
    ///
    /// # Returns
    ///
    /// Vector of index names that were dropped (for logging/debugging)
    pub fn drop_indexes_for_table(&mut self, table_name: &str) -> Vec<String> {
        // Normalize for case-insensitive comparison
        let search_name_upper = table_name.to_uppercase();

        // Extract just the table name part if qualified (e.g., "public.users" -> "users")
        let search_table_only = search_name_upper
            .rsplit('.')
            .next()
            .unwrap_or(&search_name_upper);

        // Collect index names to drop (can't modify while iterating)
        // Match if:
        // 1. Exact match (case-insensitive), OR
        // 2. Index's unqualified table name matches our unqualified search name
        let indexes_to_drop: Vec<String> = self
            .indexes
            .iter()
            .filter(|(_, metadata)| {
                let stored_upper = metadata.table_name.to_uppercase();
                let stored_table_only = stored_upper
                    .rsplit('.')
                    .next()
                    .unwrap_or(&stored_upper);

                // Match if full names match OR unqualified parts match
                stored_upper == search_name_upper || stored_table_only == search_table_only
            })
            .map(|(name, _)| name.clone())
            .collect();

        // Drop each index
        for index_name in &indexes_to_drop {
            self.indexes.remove(index_name);
            self.index_data.remove(index_name);

            // Unregister from resource tracker
            self.resource_tracker.unregister_index(index_name);
        }

        indexes_to_drop
    }
}
