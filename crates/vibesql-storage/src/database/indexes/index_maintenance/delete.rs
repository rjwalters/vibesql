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
            if metadata.table_name.eq_ignore_ascii_case(table_name) {
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
            if !metadata.table_name.eq_ignore_ascii_case(table_name) {
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
                metadata.table_name.eq_ignore_ascii_case(table_name)
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
                        // We need to adjust them based on existing pending_deletions before
                        // merging.
                        let adjusted_deletions: Vec<usize> = deleted_indices
                            .iter()
                            .map(|&idx| {
                                // The deleted index needs to be adjusted for previously pending
                                // deletions that are less than it,
                                // since those deletions affect the raw row indices
                                let adjustment = pending_deletions.partition_point(|&d| d < idx);
                                idx - adjustment
                            })
                            .collect();

                        // Merge adjusted deletions into pending_deletions (maintaining sorted
                        // order)
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
                    IndexData::IVFFlat { .. } | IndexData::Hnsw { .. } => {
                        // No row-id adjustment is needed for vector indexes here.
                        //
                        // Deletes tombstone rows via the table's deletion bitmap
                        // and do NOT renumber surviving row positions, so the
                        // absolute row_ids stored in the vector index remain
                        // valid. The deleted row's vector was already removed in
                        // `update_indexes_for_delete_with_values`. Row positions
                        // only change on compaction, which renumbers rows and
                        // triggers a full vector-index rebuild (#5446) via
                        // `rebuild_indexes`.
                    }
                }
            }
        }
    }
}
