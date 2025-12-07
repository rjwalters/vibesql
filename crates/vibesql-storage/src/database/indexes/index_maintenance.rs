// ============================================================================
// Index Maintenance - CRUD operations for indexes
// ============================================================================

#![allow(clippy::doc_overindented_list_items)]

use std::collections::BTreeMap;
use std::sync::Arc;

use vibesql_types::{DataType, SqlValue};

use super::index_manager::IndexManager;
use super::index_metadata::{
    acquire_btree_lock, normalize_index_name, IndexData, IndexMetadata, DISK_BACKED_THRESHOLD,
};
use super::ivfflat::IVFFlatIndex;
use crate::btree::{BTreeIndex, Key};
use crate::page::PageManager;
use crate::progress::ProgressTracker;
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
            let truncated = arcstr::ArcStr::from(truncated.as_str());
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
        let metadata = IndexMetadata {
            index_name: index_name.clone(),
            table_name: table_name.clone(),
            unique,
            columns: columns.clone(),
        };

        self.indexes.insert(normalized_name.clone(), metadata);

        // Choose backend based on table size
        // In test builds, DISK_BACKED_THRESHOLD is usize::MAX to disable disk-backed indexes
        #[allow(clippy::absurd_extreme_comparisons)]
        let use_disk_backed = table_rows.len() >= DISK_BACKED_THRESHOLD;

        let (index_data, memory_bytes, disk_bytes, backend) = if use_disk_backed {
            // Create disk-backed B+ tree index using proper database path
            let index_file = self.get_index_file_path(&table_name, &index_name)?;
            let index_file_str = index_file
                .to_str()
                .ok_or_else(|| StorageError::IoError("Invalid index file path".to_string()))?;

            let page_manager =
                Arc::new(PageManager::new(index_file_str, self.storage.clone()).map_err(|e| {
                    StorageError::IoError(format!("Failed to create index file: {}", e))
                })?);

            // Build key schema from indexed columns
            let key_schema: Vec<DataType> = column_indices
                .iter()
                .map(|&idx| table_schema.columns[idx].data_type.clone())
                .collect();

            // Prepare sorted entries for bulk loading
            // The BTreeIndex has native duplicate key support via Vec<RowId> per key,
            // so we don't need to extend keys with row_id for non-unique indexes
            let mut sorted_entries: Vec<(Key, usize)> = Vec::new();
            let mut progress = ProgressTracker::new(
                format!("Creating index '{}'", index_name),
                Some(table_rows.len()),
            );
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
                progress.update(row_idx + 1);
            }
            progress.finish();
            // Sort by key for bulk_load
            sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

            // Use the same key schema for both unique and non-unique indexes
            // The BTreeIndex handles duplicates internally via Vec<RowId>
            let btree_key_schema = key_schema;

            // Use bulk_load for efficient index creation
            let btree =
                BTreeIndex::bulk_load(sorted_entries, btree_key_schema, page_manager.clone())
                    .map_err(|e| {
                        StorageError::IoError(format!("Failed to bulk load index: {}", e))
                    })?;

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
            // Build the index data in-memory using bulk-load optimization
            // This is significantly faster than incremental BTreeMap insertion for large tables
            // because sorted insertion has better cache locality and fewer tree rebalances
            let mut progress = ProgressTracker::new(
                format!("Creating index '{}'", index_name),
                Some(table_rows.len()),
            );

            // Phase 1: Extract all (key, row_idx) pairs
            let mut entries: Vec<(Vec<SqlValue>, usize)> = Vec::with_capacity(table_rows.len());
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
                entries.push((key_values, row_idx));
                progress.update(row_idx + 1);
            }

            // Phase 2: Sort by key for optimal BTreeMap construction
            entries.sort_by(|a, b| a.0.cmp(&b.0));

            // Phase 3: Group entries by key and build BTreeMap
            // Using sorted iteration results in more balanced tree construction
            let mut index_data_map: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
            for (key, row_idx) in entries {
                index_data_map.entry(key).or_default().push(row_idx);
            }
            progress.finish();

            // Estimate memory usage
            let key_size = std::mem::size_of::<Vec<SqlValue>>(); // Rough estimate
            let memory_bytes = self.estimate_index_memory(table_rows.len(), key_size);

            let data = IndexData::InMemory {
                data: index_data_map,
                pending_deletions: Vec::new(),
            };

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
            // Case-insensitive comparison for table name matching
            // SQL parser normalizes identifiers to uppercase, but table/index metadata
            // may store the original case from DDL statements
            if metadata.table_name.eq_ignore_ascii_case(table_name) {
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
                            crate::database::indexes::index_operations::normalize_for_comparison(
                                &truncated,
                            )
                        })
                        .collect();

                    // Insert into the index data
                    match index_data {
                        IndexData::InMemory { data, .. } => {
                            data.entry(key_values).or_insert_with(Vec::new).push(row_index);
                        }
                        IndexData::DiskBacked { btree, .. } => {
                            // Safely acquire lock and insert into B+tree
                            // BTreeIndex now supports duplicate keys for non-unique indexes
                            match acquire_btree_lock(btree) {
                                Ok(mut guard) => {
                                    if let Err(e) = guard.insert(key_values, row_index) {
                                        // Log error if insert fails for other reasons
                                        log::warn!(
                                            "Failed to insert into disk-backed index '{}': {:?}",
                                            index_name,
                                            e
                                        );
                                    }
                                }
                                Err(e) => {
                                    log::warn!("BTreeIndex lock acquisition failed in add_to_indexes_for_insert: {}", e);
                                }
                            }
                        }
                        IndexData::IVFFlat { index } => {
                            // IVFFlat indexes are maintained separately via rebuild
                            // Incremental inserts would require re-clustering which is expensive
                            // For now, log a warning - users should rebuild the index after bulk inserts
                            log::debug!(
                                "IVFFlat index '{}' does not support incremental inserts. Consider rebuilding after bulk operations.",
                                index_name
                            );
                            let _ = index; // Suppress unused warning
                        }
                        IndexData::Hnsw { index } => {
                            // HNSW indexes support incremental inserts
                            // The index is self-organizing and doesn't require rebuilding
                            log::debug!(
                                "HNSW index '{}' does not support incremental inserts via standard index maintenance. Use search_hnsw_index API.",
                                index_name
                            );
                            let _ = index; // Suppress unused warning
                        }
                    }
                }
            }
        }
    }

    /// Batch add rows to user-defined indexes after insert
    ///
    /// This is significantly more efficient than calling `add_to_indexes_for_insert` in a loop
    /// because it:
    /// 1. Pre-computes column indices once per index (not per row)
    /// 2. Builds all keys in a single pass per index
    /// 3. Batch-inserts entries into each index
    ///
    /// # Arguments
    /// * `table_name` - The table name
    /// * `table_schema` - The table schema (for column lookups)
    /// * `rows_to_insert` - Vec of (row_index, row) pairs to insert
    pub fn batch_add_to_indexes_for_insert(
        &mut self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        rows_to_insert: &[(usize, &Row)],
    ) {
        if rows_to_insert.is_empty() {
            return;
        }

        // Collect indexes that need updating for this table
        // Pre-compute column indices once per index (not per row)
        #[allow(clippy::type_complexity)]
        let indexes_to_update: Vec<(String, Vec<(usize, Option<u64>)>)> = self
            .indexes
            .iter()
            .filter(|(_, metadata)| metadata.table_name.eq_ignore_ascii_case(table_name))
            .map(|(index_name, metadata)| {
                // Pre-compute column indices and prefix lengths for this index
                let column_info: Vec<(usize, Option<u64>)> = metadata
                    .columns
                    .iter()
                    .map(|col| {
                        let col_idx = table_schema
                            .get_column_index(&col.column_name)
                            .expect("Index column should exist");
                        (col_idx, col.prefix_length)
                    })
                    .collect();
                (index_name.clone(), column_info)
            })
            .collect();

        // Process each index
        for (index_name, column_info) in indexes_to_update {
            if let Some(index_data) = self.index_data.get_mut(&index_name) {
                match index_data {
                    IndexData::InMemory { data, .. } => {
                        // Build all keys and insert in batch
                        for &(row_index, row) in rows_to_insert {
                            let key_values: Vec<SqlValue> = column_info
                                .iter()
                                .map(|&(col_idx, prefix_length)| {
                                    let value = &row.values[col_idx];
                                    let truncated = apply_prefix_truncation(value, prefix_length);
                                    crate::database::indexes::index_operations::normalize_for_comparison(&truncated)
                                })
                                .collect();

                            data.entry(key_values).or_default().push(row_index);
                        }
                    }
                    IndexData::DiskBacked { btree, .. } => {
                        // Acquire lock once and batch insert
                        match acquire_btree_lock(btree) {
                            Ok(mut guard) => {
                                for &(row_index, row) in rows_to_insert {
                                    let key_values: Vec<SqlValue> = column_info
                                        .iter()
                                        .map(|&(col_idx, prefix_length)| {
                                            let value = &row.values[col_idx];
                                            let truncated = apply_prefix_truncation(value, prefix_length);
                                            crate::database::indexes::index_operations::normalize_for_comparison(&truncated)
                                        })
                                        .collect();
                                    if let Err(e) = guard.insert(key_values, row_index) {
                                        log::warn!(
                                            "Failed to insert into disk-backed index '{}': {:?}",
                                            index_name,
                                            e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                log::warn!("BTreeIndex lock acquisition failed in batch_add_to_indexes_for_insert: {}", e);
                            }
                        }
                    }
                    IndexData::IVFFlat { .. } | IndexData::Hnsw { .. } => {
                        // Vector indexes don't support incremental inserts via this path
                        // They need to be rebuilt after bulk operations
                    }
                }
            }
        }
    }

    /// Update user-defined indexes for update operation
    ///
    /// # Arguments
    /// * `table_name` - Name of the table being updated
    /// * `table_schema` - Schema of the table
    /// * `old_row` - Row data before the update
    /// * `new_row` - Row data after the update
    /// * `row_index` - Index of the row in the table
    /// * `changed_columns` - Optional set of column indices that were modified.
    ///   If provided, indexes that don't involve any changed columns will be skipped.
    ///   If None, all indexes are processed (backward compatible).
    pub fn update_indexes_for_update(
        &mut self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        old_row: &Row,
        new_row: &Row,
        row_index: usize,
        changed_columns: Option<&std::collections::HashSet<usize>>,
    ) {
        for (index_name, metadata) in &self.indexes {
            // Case-insensitive comparison for table name matching
            // SQL parser normalizes identifiers to uppercase, but table/index metadata
            // may store the original case from DDL statements
            if metadata.table_name.eq_ignore_ascii_case(table_name) {
                // OPTIMIZATION: Skip indexes that don't involve any changed columns
                // This avoids building key vectors and comparing them for unaffected indexes
                if let Some(changed) = changed_columns {
                    let index_affected = metadata.columns.iter().any(|col| {
                        table_schema
                            .get_column_index(&col.column_name)
                            .map(|idx| changed.contains(&idx))
                            .unwrap_or(false)
                    });
                    if !index_affected {
                        continue; // Skip this index - none of its columns were changed
                    }
                }

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
                            crate::database::indexes::index_operations::normalize_for_comparison(
                                &truncated,
                            )
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
                            crate::database::indexes::index_operations::normalize_for_comparison(
                                &truncated,
                            )
                        })
                        .collect();

                    // If keys are different, remove old and add new
                    if old_key_values != new_key_values {
                        match index_data {
                            IndexData::InMemory { data, .. } => {
                                // Remove old key
                                if let Some(row_indices) = data.get_mut(&old_key_values) {
                                    row_indices.retain(|&idx| idx != row_index);
                                    // Remove empty entries
                                    if row_indices.is_empty() {
                                        data.remove(&old_key_values);
                                    }
                                }

                                // Add new key
                                data.entry(new_key_values).or_insert_with(Vec::new).push(row_index);
                            }
                            IndexData::DiskBacked { btree, .. } => {
                                // Safely acquire lock and update B+tree: delete old key, insert new key
                                // Use delete_specific to only remove the specific row_index, not all rows
                                // with this key (important for non-unique indexes with duplicate keys)
                                match acquire_btree_lock(btree) {
                                    Ok(mut guard) => {
                                        let _ = guard.delete_specific(&old_key_values, row_index);
                                        if let Err(e) = guard.insert(new_key_values, row_index) {
                                            log::warn!(
                                                "Failed to update disk-backed index '{}': {:?}",
                                                index_name,
                                                e
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        log::warn!("BTreeIndex lock acquisition failed in update_indexes_for_update: {}", e);
                                    }
                                }
                            }
                            IndexData::IVFFlat { index } => {
                                // IVFFlat indexes are maintained separately via rebuild
                                // Incremental updates would require re-clustering which is expensive
                                log::debug!(
                                    "IVFFlat index '{}' does not support incremental updates. Consider rebuilding after bulk operations.",
                                    index_name
                                );
                                let _ = index; // Suppress unused warning
                            }
                            IndexData::Hnsw { index } => {
                                // HNSW indexes would need remove + insert for updates
                                log::debug!(
                                    "HNSW index '{}' does not support incremental updates. Consider rebuilding after bulk operations.",
                                    index_name
                                );
                                let _ = index; // Suppress unused warning
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
        self.update_indexes_for_delete_with_values(table_name, table_schema, &row.values, row_index);
    }

    /// Update user-defined indexes for delete operation using raw values slice
    ///
    /// This is an optimization over `update_indexes_for_delete` that avoids requiring
    /// a full Row struct. Useful when you already have a values slice and want to
    /// avoid the overhead of wrapping it in a Row.
    pub fn update_indexes_for_delete_with_values(
        &mut self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        values: &[SqlValue],
        row_index: usize,
    ) {
        for (index_name, metadata) in &self.indexes {
            // Case-insensitive comparison for table name matching
            // SQL parser normalizes identifiers to uppercase, but table/index metadata
            // may store the original case from DDL statements
            if metadata.table_name.eq_ignore_ascii_case(table_name) {
                if let Some(index_data) = self.index_data.get_mut(index_name) {
                    // Build key from the values slice
                    let key_values: Vec<SqlValue> = metadata
                        .columns
                        .iter()
                        .map(|col| {
                            let col_idx = table_schema
                                .get_column_index(&col.column_name)
                                .expect("Index column should exist");
                            let value = &values[col_idx];
                            let truncated = apply_prefix_truncation(value, col.prefix_length);
                            // Normalize numeric types for consistent ordering/comparison
                            crate::database::indexes::index_operations::normalize_for_comparison(
                                &truncated,
                            )
                        })
                        .collect();

                    // Remove the row index from this key
                    match index_data {
                        IndexData::InMemory { data, .. } => {
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
                            // Use delete_specific to only remove the specific row_index, not all rows
                            // with this key (important for non-unique indexes with duplicate keys)
                            match acquire_btree_lock(btree) {
                                Ok(mut guard) => {
                                    let _ = guard.delete_specific(&key_values, row_index);
                                }
                                Err(e) => {
                                    log::warn!("BTreeIndex lock acquisition failed in update_indexes_for_delete: {}", e);
                                }
                            }
                        }
                        IndexData::IVFFlat { index } => {
                            // IVFFlat indexes are maintained separately via rebuild
                            // Incremental deletes would require re-clustering which is expensive
                            log::debug!(
                                "IVFFlat index '{}' does not support incremental deletes. Consider rebuilding after bulk operations.",
                                index_name
                            );
                            let _ = index; // Suppress unused warning
                        }
                        IndexData::Hnsw { index } => {
                            // HNSW indexes support incremental deletes
                            // But we're not tracking which row maps to which vector
                            log::debug!(
                                "HNSW index '{}' does not support incremental deletes. Consider rebuilding after bulk operations.",
                                index_name
                            );
                            let _ = index; // Suppress unused warning
                        }
                    }
                }
            }
        }
    }

    /// Batch update user-defined indexes for delete operation
    ///
    /// This is significantly more efficient than calling `update_indexes_for_delete` in a loop
    /// because it:
    /// 1. Pre-computes column indices once per index (not per row)
    /// 2. Builds all keys in a single pass
    /// 3. Batch-removes entries from each index
    ///
    /// # Arguments
    /// * `table_name` - The table name
    /// * `table_schema` - The table schema (for column lookups)
    /// * `rows_to_delete` - Vec of (row_index, row) pairs to delete
    pub fn batch_update_indexes_for_delete(
        &mut self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        rows_to_delete: &[(usize, &Row)],
    ) {
        if rows_to_delete.is_empty() {
            return;
        }

        // Collect indexes that need updating for this table
        // Pre-compute column indices once per index (not per row)
        #[allow(clippy::type_complexity)]
        let indexes_to_update: Vec<(String, Vec<(usize, Option<u64>)>)> = self
            .indexes
            .iter()
            .filter(|(_, metadata)| metadata.table_name.eq_ignore_ascii_case(table_name))
            .map(|(index_name, metadata)| {
                // Pre-compute column indices and prefix lengths for this index
                let column_info: Vec<(usize, Option<u64>)> = metadata
                    .columns
                    .iter()
                    .map(|col| {
                        let col_idx = table_schema
                            .get_column_index(&col.column_name)
                            .expect("Index column should exist");
                        (col_idx, col.prefix_length)
                    })
                    .collect();
                (index_name.clone(), column_info)
            })
            .collect();

        // Process each index
        for (index_name, column_info) in indexes_to_update {
            if let Some(index_data) = self.index_data.get_mut(&index_name) {
                match index_data {
                    IndexData::InMemory { data, .. } => {
                        // Build all keys and remove in batch
                        for &(row_index, row) in rows_to_delete {
                            let key_values: Vec<SqlValue> = column_info
                                .iter()
                                .map(|&(col_idx, prefix_length)| {
                                    let value = &row.values[col_idx];
                                    let truncated = apply_prefix_truncation(value, prefix_length);
                                    crate::database::indexes::index_operations::normalize_for_comparison(&truncated)
                                })
                                .collect();

                            if let Some(row_indices) = data.get_mut(&key_values) {
                                row_indices.retain(|&idx| idx != row_index);
                                if row_indices.is_empty() {
                                    data.remove(&key_values);
                                }
                            }
                        }
                    }
                    IndexData::DiskBacked { btree, .. } => {
                        // Build all (key, row_id) pairs first for batch deletion
                        let entries_to_delete: Vec<(Vec<SqlValue>, usize)> = rows_to_delete
                            .iter()
                            .map(|&(row_index, row)| {
                                let key_values: Vec<SqlValue> = column_info
                                    .iter()
                                    .map(|&(col_idx, prefix_length)| {
                                        let value = &row.values[col_idx];
                                        let truncated = apply_prefix_truncation(value, prefix_length);
                                        crate::database::indexes::index_operations::normalize_for_comparison(&truncated)
                                    })
                                    .collect();
                                (key_values, row_index)
                            })
                            .collect();

                        // Use batch delete for better performance
                        // This sorts keys internally and traverses leaves sequentially
                        match acquire_btree_lock(btree) {
                            Ok(mut guard) => {
                                let _ = guard.delete_batch(&entries_to_delete);
                            }
                            Err(e) => {
                                log::warn!("BTreeIndex lock acquisition failed in batch_update_indexes_for_delete: {}", e);
                            }
                        }
                    }
                    IndexData::IVFFlat { .. } | IndexData::Hnsw { .. } => {
                        // Vector indexes don't support incremental deletes
                        // They need to be rebuilt after bulk operations
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
        // Case-insensitive comparison for table name matching
        let indexes_to_rebuild: Vec<String> = self
            .indexes
            .iter()
            .filter(|(_, metadata)| metadata.table_name.eq_ignore_ascii_case(table_name))
            .map(|(name, _)| name.clone())
            .collect();

        // Rebuild each index
        for index_name in indexes_to_rebuild {
            if let Some(index_data) = self.index_data.get_mut(&index_name) {
                if let Some(metadata) = self.indexes.get(&index_name) {
                    match index_data {
                        IndexData::InMemory { data, pending_deletions } => {
                            // Clear existing data and pending deletions
                            data.clear();
                            pending_deletions.clear();

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
                        IndexData::IVFFlat { index } => {
                            // IVFFlat indexes need to be rebuilt via build()
                            // This requires extracting vectors from the table
                            log::debug!(
                                "IVFFlat index rebuild not yet implemented. Index '{}' needs manual rebuild.",
                                index_name
                            );
                            let _ = index; // Suppress unused warning
                        }
                        IndexData::Hnsw { index } => {
                            // HNSW indexes need to be rebuilt via build()
                            // This requires extracting vectors from the table
                            log::debug!(
                                "HNSW index rebuild not yet implemented. Index '{}' needs manual rebuild.",
                                index_name
                            );
                            let _ = index; // Suppress unused warning
                        }
                    }
                }
            }
        }
    }

    /// Adjust row indices after row deletions for user-defined indexes
    ///
    /// For in-memory indexes, this uses lazy adjustment: instead of immediately adjusting
    /// all row indices (O(n) for table size), we store the deleted indices in a pending
    /// list and apply the adjustment lazily during lookups. This makes single-row deletes
    /// O(1) instead of O(n).
    ///
    /// For disk-backed indexes, we still use the immediate adjustment approach since
    /// the B+tree has its own row ID adjustment mechanism.
    ///
    /// # Arguments
    /// * `table_name` - The table whose indexes need adjustment
    /// * `deleted_indices` - Sorted list of deleted row indices (ascending order)
    pub fn adjust_indexes_after_delete(&mut self, table_name: &str, deleted_indices: &[usize]) {
        if deleted_indices.is_empty() {
            return;
        }

        // Find all indexes for this table
        let index_names: Vec<String> = self
            .indexes
            .iter()
            .filter(|(_, metadata)| metadata.table_name.eq_ignore_ascii_case(table_name))
            .map(|(name, _)| name.clone())
            .collect();

        for index_name in index_names {
            if let Some(index_data) = self.index_data.get_mut(&index_name) {
                match index_data {
                    IndexData::InMemory { pending_deletions, .. } => {
                        // Lazy adjustment: merge deleted_indices into pending_deletions
                        // This is O(d) where d = number of deletes, instead of O(n) for table size
                        //
                        // Note: deleted_indices are raw indices that haven't been adjusted yet.
                        // We need to adjust them based on existing pending_deletions before merging.
                        let adjusted_deletions: Vec<usize> = deleted_indices
                            .iter()
                            .map(|&idx| {
                                // The deleted index needs to be adjusted for previously pending deletions
                                // that are less than it, since those deletions affect the raw row indices
                                let adjustment = pending_deletions.partition_point(|&d| d < idx);
                                idx - adjustment
                            })
                            .collect();

                        // Merge adjusted deletions into pending_deletions (maintaining sorted order)
                        if pending_deletions.is_empty() {
                            *pending_deletions = adjusted_deletions;
                        } else {
                            // Merge two sorted lists
                            let mut merged = Vec::with_capacity(
                                pending_deletions.len() + adjusted_deletions.len(),
                            );
                            let mut i = 0;
                            let mut j = 0;
                            while i < pending_deletions.len() && j < adjusted_deletions.len() {
                                if pending_deletions[i] <= adjusted_deletions[j] {
                                    merged.push(pending_deletions[i]);
                                    i += 1;
                                } else {
                                    merged.push(adjusted_deletions[j]);
                                    j += 1;
                                }
                            }
                            merged.extend_from_slice(&pending_deletions[i..]);
                            merged.extend_from_slice(&adjusted_deletions[j..]);
                            *pending_deletions = merged;
                        }

                        // Compact if needed (apply pending deletions when list gets too large)
                        if index_data.needs_compaction() {
                            index_data.compact_pending_deletions();
                        }
                    }
                    IndexData::DiskBacked { btree, .. } => {
                        // For disk-backed indexes, we still use immediate adjustment
                        // since the B+tree has its own efficient row ID adjustment
                        match acquire_btree_lock(btree) {
                            Ok(mut guard) => {
                                guard.adjust_row_ids_after_delete(deleted_indices);
                            }
                            Err(e) => {
                                log::warn!(
                                    "BTreeIndex lock acquisition failed in adjust_indexes_after_delete: {}",
                                    e
                                );
                            }
                        }
                    }
                    IndexData::IVFFlat { index } => {
                        // IVFFlat indexes store row IDs in inverted lists
                        // This would require iterating through all lists and adjusting row IDs
                        log::debug!(
                            "IVFFlat index '{}' row ID adjustment not yet implemented. Consider rebuilding.",
                            index_name
                        );
                        let _ = index; // Suppress unused warning
                    }
                    IndexData::Hnsw { index } => {
                        // HNSW indexes store row IDs in the proximity graph
                        // This would require iterating through all nodes and adjusting row IDs
                        log::debug!(
                            "HNSW index '{}' row ID adjustment not yet implemented. Consider rebuilding.",
                            index_name
                        );
                        let _ = index; // Suppress unused warning
                    }
                }
            }
        }
    }

    /// Create an IVFFlat index for approximate nearest neighbor search on vector columns
    ///
    /// This method creates an IVFFlat (Inverted File with Flat quantization) index
    /// for efficient approximate nearest neighbor search on vector data.
    ///
    /// # Arguments
    /// * `index_name` - Name for the new index
    /// * `table_name` - Name of the table containing the vector column
    /// * `table_schema` - Schema of the table
    /// * `table_rows` - Current rows in the table
    /// * `column_name` - Name of the vector column to index
    /// * `col_idx` - Column index in the table schema
    /// * `dimensions` - Number of dimensions in the vectors
    /// * `lists` - Number of clusters for the IVFFlat algorithm
    /// * `metric` - Distance metric to use (L2, Cosine, InnerProduct)
    #[allow(clippy::too_many_arguments)]
    pub fn create_ivfflat_index(
        &mut self,
        index_name: String,
        table_name: String,
        column_name: String,
        col_idx: usize,
        dimensions: usize,
        lists: usize,
        metric: vibesql_ast::VectorDistanceMetric,
    ) -> Result<(), StorageError> {
        // Normalize index name for case-insensitive comparison
        let normalized_name = normalize_index_name(&index_name);

        // Check if index already exists
        if self.indexes.contains_key(&normalized_name) {
            return Err(StorageError::IndexAlreadyExists(index_name));
        }

        // Create IVFFlat index
        let mut ivfflat = IVFFlatIndex::new(dimensions, lists as u32, metric);

        // Extract vectors from table rows
        let mut vectors: Vec<(usize, Vec<f64>)> = Vec::new();
        for (row_idx, row) in self.get_table_rows_for_ivfflat(&table_name)?.iter().enumerate() {
            if col_idx < row.values.len() {
                if let Some(vec_data) = Self::extract_vector(&row.values[col_idx]) {
                    vectors.push((row_idx, vec_data));
                }
            }
        }

        // Save count before moving
        let vector_count = vectors.len();

        // Build the index using k-means clustering
        ivfflat
            .build(vectors)
            .map_err(|e| StorageError::IoError(format!("Failed to build IVFFlat index: {}", e)))?;

        // Store index metadata
        let metadata = IndexMetadata {
            index_name: index_name.clone(),
            table_name: table_name.clone(),
            unique: false, // IVFFlat indexes are never unique
            columns: vec![vibesql_ast::IndexColumn {
                column_name,
                direction: vibesql_ast::OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        self.indexes.insert(normalized_name.clone(), metadata);

        // Store index data
        self.index_data.insert(normalized_name.clone(), IndexData::IVFFlat { index: ivfflat });

        // Register with resource tracker (estimate memory based on vector count and dimensions)
        let estimated_memory = vector_count * dimensions * std::mem::size_of::<f64>() * 2; // vectors + centroids
        self.resource_tracker.register_index(
            normalized_name,
            estimated_memory,
            0,
            crate::database::IndexBackend::InMemory,
        );

        Ok(())
    }

    /// Helper method to get table rows for IVFFlat index building
    /// Note: This is a temporary solution - the actual rows should be passed from the caller
    fn get_table_rows_for_ivfflat(&self, _table_name: &str) -> Result<Vec<Row>, StorageError> {
        // This method shouldn't be called - rows should be extracted by the caller
        // Return empty for now; the actual implementation passes rows directly
        Ok(Vec::new())
    }

    /// Create an IVFFlat index with pre-extracted vectors
    ///
    /// This is the main entry point for creating IVFFlat indexes when the
    /// table rows have already been accessed by the caller (executor layer).
    #[allow(clippy::too_many_arguments)]
    pub fn create_ivfflat_index_with_vectors(
        &mut self,
        index_name: String,
        table_name: String,
        column_name: String,
        dimensions: usize,
        lists: usize,
        metric: vibesql_ast::VectorDistanceMetric,
        vectors: Vec<(usize, Vec<f64>)>,
    ) -> Result<(), StorageError> {
        // Normalize index name for case-insensitive comparison
        let normalized_name = normalize_index_name(&index_name);

        // Check if index already exists
        if self.indexes.contains_key(&normalized_name) {
            return Err(StorageError::IndexAlreadyExists(index_name));
        }

        // Create IVFFlat index
        let mut ivfflat = IVFFlatIndex::new(dimensions, lists as u32, metric);

        let vector_count = vectors.len();

        // Build the index using k-means clustering
        ivfflat
            .build(vectors)
            .map_err(|e| StorageError::IoError(format!("Failed to build IVFFlat index: {}", e)))?;

        // Store index metadata
        let metadata = IndexMetadata {
            index_name: index_name.clone(),
            table_name: table_name.clone(),
            unique: false, // IVFFlat indexes are never unique
            columns: vec![vibesql_ast::IndexColumn {
                column_name,
                direction: vibesql_ast::OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        self.indexes.insert(normalized_name.clone(), metadata);

        // Store index data
        self.index_data.insert(normalized_name.clone(), IndexData::IVFFlat { index: ivfflat });

        // Register with resource tracker (estimate memory based on vector count and dimensions)
        let estimated_memory = vector_count * dimensions * std::mem::size_of::<f64>() * 2; // vectors + centroids
        self.resource_tracker.register_index(
            normalized_name,
            estimated_memory,
            0,
            crate::database::IndexBackend::InMemory,
        );

        Ok(())
    }

    /// Extract a vector from a SqlValue, converting f32 to f64
    ///
    /// Note: SqlValue::Vector stores f32 for storage efficiency,
    /// but IVFFlat uses f64 for precision in k-means clustering.
    fn extract_vector(value: &vibesql_types::SqlValue) -> Option<Vec<f64>> {
        match value {
            vibesql_types::SqlValue::Vector(data) => {
                // Convert f32 vector to f64 for IVFFlat processing
                Some(data.iter().map(|&v| v as f64).collect())
            }
            vibesql_types::SqlValue::Null => None,
            _ => None,
        }
    }

    /// Search an IVFFlat index for approximate nearest neighbors
    ///
    /// # Arguments
    /// * `index_name` - Name of the IVFFlat index
    /// * `query_vector` - The query vector (f64)
    /// * `k` - Maximum number of nearest neighbors to return
    ///
    /// # Returns
    /// * `Ok(Vec<(usize, f64)>)` - Vector of (row_id, distance) pairs, ordered by distance
    /// * `Err(StorageError)` - If index not found or not an IVFFlat index
    pub fn search_ivfflat_index(
        &self,
        index_name: &str,
        query_vector: &[f64],
        k: usize,
    ) -> Result<Vec<(usize, f64)>, StorageError> {
        let normalized_name = normalize_index_name(index_name);

        let index_data = self
            .index_data
            .get(&normalized_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        match index_data {
            IndexData::IVFFlat { index } => index
                .search(query_vector, k)
                .map_err(|e| StorageError::Other(format!("IVFFlat search error: {}", e))),
            _ => {
                Err(StorageError::Other(format!("Index '{}' is not an IVFFlat index", index_name)))
            }
        }
    }

    /// Get all IVFFlat indexes for a specific table
    ///
    /// Returns index metadata and access to search for each IVFFlat index on the table.
    pub fn get_ivfflat_indexes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<(&IndexMetadata, &IVFFlatIndex)> {
        let search_name_upper = table_name.to_uppercase();
        let search_table_only = search_name_upper.rsplit('.').next().unwrap_or(&search_name_upper);

        self.indexes
            .iter()
            .filter_map(|(normalized_name, metadata)| {
                let stored_upper = metadata.table_name.to_uppercase();
                let stored_table_only = stored_upper.rsplit('.').next().unwrap_or(&stored_upper);

                // Check if table matches
                if stored_upper != search_name_upper && stored_table_only != search_table_only {
                    return None;
                }

                // Check if it's an IVFFlat index
                if let Some(IndexData::IVFFlat { index }) = self.index_data.get(normalized_name) {
                    Some((metadata, index))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Set the number of probes for an IVFFlat index
    ///
    /// Probes controls how many clusters are searched during a query.
    /// Higher values improve recall but increase search time.
    pub fn set_ivfflat_probes(
        &mut self,
        index_name: &str,
        probes: usize,
    ) -> Result<(), StorageError> {
        let normalized_name = normalize_index_name(index_name);

        let index_data = self
            .index_data
            .get_mut(&normalized_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        match index_data {
            IndexData::IVFFlat { index } => {
                index.set_probes(probes);
                Ok(())
            }
            _ => {
                Err(StorageError::Other(format!("Index '{}' is not an IVFFlat index", index_name)))
            }
        }
    }

    // ============================================================================
    // HNSW Index Methods
    // ============================================================================

    /// Create an HNSW index with pre-extracted vectors
    ///
    /// This is the main entry point for creating HNSW indexes when the
    /// table rows have already been accessed by the caller (executor layer).
    #[allow(clippy::too_many_arguments)]
    pub fn create_hnsw_index_with_vectors(
        &mut self,
        index_name: String,
        table_name: String,
        column_name: String,
        dimensions: usize,
        m: u32,
        ef_construction: u32,
        metric: vibesql_ast::VectorDistanceMetric,
        vectors: Vec<(usize, Vec<f64>)>,
    ) -> Result<(), StorageError> {
        use super::hnsw::HnswIndex;

        // Normalize index name for case-insensitive comparison
        let normalized_name = normalize_index_name(&index_name);

        // Check if index already exists
        if self.indexes.contains_key(&normalized_name) {
            return Err(StorageError::IndexAlreadyExists(index_name));
        }

        // Create HNSW index
        let mut hnsw = HnswIndex::new(dimensions, m, ef_construction, metric);

        let vector_count = vectors.len();

        // Build the index
        hnsw.build(vectors)
            .map_err(|e| StorageError::IoError(format!("Failed to build HNSW index: {}", e)))?;

        // Store index metadata
        let metadata = IndexMetadata {
            index_name: index_name.clone(),
            table_name: table_name.clone(),
            unique: false, // HNSW indexes are never unique
            columns: vec![vibesql_ast::IndexColumn {
                column_name,
                direction: vibesql_ast::OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        self.indexes.insert(normalized_name.clone(), metadata);

        // Store index data
        self.index_data.insert(normalized_name.clone(), IndexData::Hnsw { index: hnsw });

        // Register with resource tracker (estimate memory based on vector count and dimensions)
        // HNSW has more overhead due to graph structure: ~m*2 neighbors per node
        let estimated_memory = vector_count
            * (dimensions * std::mem::size_of::<f64>()
                + m as usize * 2 * std::mem::size_of::<usize>());
        self.resource_tracker.register_index(
            normalized_name,
            estimated_memory,
            0,
            crate::database::IndexBackend::InMemory,
        );

        Ok(())
    }

    /// Search an HNSW index for approximate nearest neighbors
    ///
    /// # Arguments
    /// * `index_name` - Name of the HNSW index
    /// * `query_vector` - The query vector (f64)
    /// * `k` - Maximum number of nearest neighbors to return
    ///
    /// # Returns
    /// * `Ok(Vec<(usize, f64)>)` - Vector of (row_id, distance) pairs, ordered by distance
    /// * `Err(StorageError)` - If index not found or not an HNSW index
    pub fn search_hnsw_index(
        &self,
        index_name: &str,
        query_vector: &[f64],
        k: usize,
    ) -> Result<Vec<(usize, f64)>, StorageError> {
        let normalized_name = normalize_index_name(index_name);

        let index_data = self
            .index_data
            .get(&normalized_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        match index_data {
            IndexData::Hnsw { index } => index
                .search(query_vector, k)
                .map_err(|e| StorageError::Other(format!("HNSW search error: {}", e))),
            _ => Err(StorageError::Other(format!("Index '{}' is not an HNSW index", index_name))),
        }
    }

    /// Get all HNSW indexes for a specific table
    ///
    /// Returns index metadata and access to search for each HNSW index on the table.
    pub fn get_hnsw_indexes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<(&IndexMetadata, &super::hnsw::HnswIndex)> {
        let search_name_upper = table_name.to_uppercase();
        let search_table_only = search_name_upper.rsplit('.').next().unwrap_or(&search_name_upper);

        self.indexes
            .iter()
            .filter_map(|(normalized_name, metadata)| {
                let stored_upper = metadata.table_name.to_uppercase();
                let stored_table_only = stored_upper.rsplit('.').next().unwrap_or(&stored_upper);

                // Check if table matches
                if stored_upper != search_name_upper && stored_table_only != search_table_only {
                    return None;
                }

                // Check if it's an HNSW index
                if let Some(IndexData::Hnsw { index }) = self.index_data.get(normalized_name) {
                    Some((metadata, index))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Set the ef_search parameter for an HNSW index
    ///
    /// ef_search controls the search accuracy/speed tradeoff.
    /// Higher values improve recall but increase search time.
    pub fn set_hnsw_ef_search(
        &mut self,
        index_name: &str,
        ef_search: usize,
    ) -> Result<(), StorageError> {
        let normalized_name = normalize_index_name(index_name);

        let index_data = self
            .index_data
            .get_mut(&normalized_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        match index_data {
            IndexData::Hnsw { index } => {
                index.set_ef_search(ef_search);
                Ok(())
            }
            _ => Err(StorageError::Other(format!("Index '{}' is not an HNSW index", index_name))),
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
        let search_table_only = search_name_upper.rsplit('.').next().unwrap_or(&search_name_upper);

        // Collect index names to drop (can't modify while iterating)
        // Match if:
        // 1. Exact match (case-insensitive), OR
        // 2. Index's unqualified table name matches our unqualified search name
        let indexes_to_drop: Vec<String> = self
            .indexes
            .iter()
            .filter(|(_, metadata)| {
                let stored_upper = metadata.table_name.to_uppercase();
                let stored_table_only = stored_upper.rsplit('.').next().unwrap_or(&stored_upper);

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
