// ============================================================================
// Index Deletion Operations
// ============================================================================

use vibesql_types::SqlValue;

use super::prefix::apply_prefix_truncation;
use crate::{
    database::indexes::{
        index_manager::IndexManager,
        index_metadata::{acquire_btree_lock, normalize_index_name, IndexData},
    },
    Row,
};

impl IndexManager {
    /// Update user-defined indexes for delete operation
    pub fn update_indexes_for_delete(
        &mut self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        row: &Row,
        row_index: usize,
    ) {
        self.update_indexes_for_delete_with_values(
            table_name,
            table_schema,
            &row.values,
            row_index,
        );
    }

    /// Update user-defined indexes for delete operation using raw values slice
    ///
    /// This is an optimization over `update_indexes_for_delete` that avoids requiring
    /// a full Row struct. Useful when you already have a values slice and want to
    /// avoid the overhead of wrapping it in a Row.
    ///
    /// Note: Expression indexes are skipped - use `update_expression_indexes_for_delete` for them.
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
            if metadata.matches_table(table_name) {
                // Skip expression indexes - they need pre-computed keys
                // Expression indexes are handled by update_expression_indexes_for_delete
                if metadata.columns.iter().any(|col| col.is_expression()) {
                    continue;
                }

                // Skip partial indexes - the executor must evaluate the WHERE
                // predicate for the (deleted) row to decide whether the entry
                // was ever in the index. Handled by
                // update_partial_indexes_for_delete.
                if metadata.is_partial() {
                    continue;
                }

                if let Some(index_data) = self.index_data.get_mut(index_name) {
                    // Build key from the values slice
                    let key_values: Vec<SqlValue> = metadata
                        .columns
                        .iter()
                        .map(|col| {
                            // Safe: we checked above that no columns are expressions
                            let col_name =
                                col.column_name().expect("Column index should have column name");
                            let col_idx = table_schema
                                .get_column_index(col_name)
                                .expect("Index column should exist");
                            let value = &values[col_idx];
                            let truncated = apply_prefix_truncation(value, col.prefix_length());
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
                            // Use delete_specific to only remove the specific row_index, not all
                            // rows with this key (important for
                            // non-unique indexes with duplicate keys)
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
                            // Drop the deleted row's vector from its posting list so
                            // a subsequent nearest-neighbor search cannot return the
                            // deleted row. Tables tombstone deletes via a bitmap (no
                            // row renumbering) until a compaction, which triggers a
                            // full rebuild (#5446) — so removing by absolute row_id
                            // is sufficient and correct on the non-compacting path.
                            index.remove(row_index);
                        }
                        IndexData::Hnsw { index } => {
                            // HnswIndex::remove unlinks the node from every graph
                            // layer (and repairs the entry point), so the deleted
                            // row is no longer reachable by search. Lazy unlinks
                            // erode graph connectivity / recall over time, so
                            // remove also tracks a tombstone counter and auto-
                            // rebuilds the graph from the live vectors once the
                            // deleted ratio crosses its compaction threshold
                            // (#5454). The rebuilt graph is in-memory and part of
                            // the COW snapshot of `Operations`, so an
                            // in-transaction rebuild is reversed on ROLLBACK.
                            index.remove(row_index);
                        }
                    }
                }
            }
        }
    }

    /// Update expression indexes for delete operation with pre-computed keys
    ///
    /// This method handles expression indexes which require pre-computed key values
    /// since the storage layer cannot evaluate expressions.
    ///
    /// # Arguments
    /// * `table_name` - The table name
    /// * `row_index` - The index of the row in the table
    /// * `expression_keys` - Map of index name to pre-computed key values
    pub fn update_expression_indexes_for_delete(
        &mut self,
        table_name: &str,
        row_index: usize,
        expression_keys: &std::collections::HashMap<String, Vec<SqlValue>>,
    ) {
        for (index_name, metadata) in &self.indexes {
            if !metadata.matches_table(table_name) {
                continue;
            }

            // Only process expression indexes
            if !metadata.columns.iter().any(|col| col.is_expression()) {
                continue;
            }

            // Get pre-computed key for this index
            let normalized_name = normalize_index_name(index_name);
            let key_values = match expression_keys
                .get(&normalized_name)
                .or_else(|| expression_keys.get(index_name))
            {
                Some(keys) => keys.clone(),
                None => {
                    log::warn!(
                        "No pre-computed keys provided for expression index '{}' during delete",
                        index_name
                    );
                    continue;
                }
            };

            // Normalize key values
            let normalized_key: Vec<SqlValue> = key_values
                .iter()
                .map(|v| crate::database::indexes::index_operations::normalize_for_comparison(v))
                .collect();

            if let Some(index_data) = self.index_data.get_mut(&normalized_name) {
                match index_data {
                    IndexData::InMemory { data, .. } => {
                        if let Some(row_indices) = data.get_mut(&normalized_key) {
                            row_indices.retain(|&idx| idx != row_index);
                            if row_indices.is_empty() {
                                data.remove(&normalized_key);
                            }
                        }
                    }
                    IndexData::DiskBacked { btree, .. } => match acquire_btree_lock(btree) {
                        Ok(mut guard) => {
                            let _ = guard.delete_specific(&normalized_key, row_index);
                        }
                        Err(e) => {
                            log::warn!(
                                    "BTreeIndex lock acquisition failed in update_expression_indexes_for_delete: {}",
                                    e
                                );
                        }
                    },
                    IndexData::IVFFlat { .. } | IndexData::Hnsw { .. } => {
                        // Vector indexes don't support expression indexing
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
    /// Note: Expression indexes are skipped - use `batch_update_expression_indexes_for_delete` for
    /// them.
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
        // Skip expression indexes - they need pre-computed keys
        // Skip partial indexes - they need pre-evaluated predicates (handled by
        // batch_update_partial_indexes_for_delete on the executor side).
        #[allow(clippy::type_complexity)]
        let indexes_to_update: Vec<(String, Vec<(usize, Option<u64>)>)> = self
            .indexes
            .iter()
            .filter(|(_, metadata)| {
                metadata.matches_table(table_name)
                    && !metadata.columns.iter().any(|col| col.is_expression())
                    && !metadata.is_partial()
            })
            .map(|(index_name, metadata)| {
                // Pre-compute column indices and prefix lengths for this index
                let column_info: Vec<(usize, Option<u64>)> = metadata
                    .columns
                    .iter()
                    .map(|col| {
                        // Safe: we filtered out expression indexes above
                        let col_name =
                            col.column_name().expect("Column index should have column name");
                        let col_idx = table_schema
                            .get_column_index(col_name)
                            .expect("Index column should exist");
                        (col_idx, col.prefix_length())
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
                    IndexData::IVFFlat { index } => {
                        // Remove each deleted row's vector by absolute row_id.
                        for &(row_index, _) in rows_to_delete {
                            index.remove(row_index);
                        }
                    }
                    IndexData::Hnsw { index } => {
                        for &(row_index, _) in rows_to_delete {
                            index.remove(row_index);
                        }
                    }
                }
            }
        }
    }
}
