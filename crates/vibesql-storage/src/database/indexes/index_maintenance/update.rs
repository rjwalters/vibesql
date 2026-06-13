// ============================================================================
// Index Update and Rebuild Operations
// ============================================================================

use vibesql_types::{DataType, SqlValue};

use super::prefix::apply_prefix_truncation;
use crate::{
    btree::BTreeIndex,
    database::indexes::{
        index_manager::IndexManager,
        index_metadata::{acquire_btree_lock, normalize_index_name, IndexData, IndexMetadata},
    },
    Row,
};

impl IndexManager {
    /// Update user-defined indexes for update operation
    ///
    /// Note: Expression indexes require pre-computed keys via
    /// `update_expression_indexes_for_update`. This method skips expression indexes since it
    /// cannot evaluate expressions.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table being updated
    /// * `table_schema` - Schema of the table
    /// * `old_row` - Row data before the update
    /// * `new_row` - Row data after the update
    /// * `row_index` - Index of the row in the table
    /// * `changed_columns` - Optional set of column indices that were modified. If provided,
    ///   indexes that don't involve any changed columns will be skipped. If None, all indexes are
    ///   processed (backward compatible).
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
                // Skip expression indexes - they need pre-computed keys
                // Expression indexes are handled by update_expression_indexes_for_update
                if metadata.columns.iter().any(|col| col.is_expression()) {
                    continue;
                }

                // Skip partial indexes - the executor must evaluate the WHERE
                // predicate for both the old and new rows to decide whether
                // to add, remove, or move the index entry. Handled by
                // update_partial_indexes_for_update.
                if metadata.is_partial() {
                    continue;
                }

                // OPTIMIZATION: Skip indexes that don't involve any changed columns
                // This avoids building key vectors and comparing them for unaffected indexes
                if let Some(changed) = changed_columns {
                    let index_affected = metadata.columns.iter().any(|col| {
                        // Safe: we checked above that no columns are expressions
                        col.column_name()
                            .and_then(|name| table_schema.get_column_index(name))
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
                            // Safe: we checked above that no columns are expressions
                            let col_name =
                                col.column_name().expect("Column index should have column name");
                            let col_idx = table_schema
                                .get_column_index(col_name)
                                .expect("Index column should exist");
                            let value = &old_row.values[col_idx];
                            let truncated = apply_prefix_truncation(value, col.prefix_length());
                            crate::database::indexes::index_operations::normalize_for_comparison(
                                &truncated,
                            )
                        })
                        .collect();

                    let new_key_values: Vec<SqlValue> = metadata
                        .columns
                        .iter()
                        .map(|col| {
                            // Safe: we checked above that no columns are expressions
                            let col_name =
                                col.column_name().expect("Column index should have column name");
                            let col_idx = table_schema
                                .get_column_index(col_name)
                                .expect("Index column should exist");
                            let value = &new_row.values[col_idx];
                            let truncated = apply_prefix_truncation(value, col.prefix_length());
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
                                // Safely acquire lock and update B+tree: delete old key, insert new
                                // key Use delete_specific to only
                                // remove the specific row_index, not all rows
                                // with this key (important for non-unique indexes with duplicate
                                // keys)
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
                                // The vector for this row changed: drop the old
                                // entry and re-insert at the new vector's nearest
                                // centroid so search reflects the updated value.
                                // (This arm only runs when old_key != new_key.)
                                index.remove(row_index);
                                if let Some(col_idx) = Self::vector_index_column_idx(
                                    metadata,
                                    table_schema,
                                    index_name,
                                ) {
                                    if let Some(vector) =
                                        Self::extract_vector(&new_row.values[col_idx])
                                    {
                                        if let Err(e) = index.insert(row_index, vector) {
                                            log::warn!(
                                                "Failed to re-insert into IVFFlat index '{}' on update: {}",
                                                index_name,
                                                e
                                            );
                                        }
                                    }
                                    // If the new value is NULL/non-vector, the row
                                    // is correctly left out of the index.
                                }
                            }
                            IndexData::Hnsw { index } => {
                                index.remove(row_index);
                                if let Some(col_idx) = Self::vector_index_column_idx(
                                    metadata,
                                    table_schema,
                                    index_name,
                                ) {
                                    if let Some(vector) =
                                        Self::extract_vector(&new_row.values[col_idx])
                                    {
                                        if let Err(e) = index.insert(row_index, vector) {
                                            log::warn!(
                                                "Failed to re-insert into HNSW index '{}' on update: {}",
                                                index_name,
                                                e
                                            );
                                        }
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

    /// Update expression indexes for update operation with pre-computed keys
    ///
    /// This method handles expression indexes which require pre-computed key values
    /// since the storage layer cannot evaluate expressions.
    ///
    /// # Arguments
    /// * `table_name` - The table name
    /// * `row_index` - The index of the row in the table
    /// * `old_expression_keys` - Map of index name to pre-computed old key values
    /// * `new_expression_keys` - Map of index name to pre-computed new key values
    pub fn update_expression_indexes_for_update(
        &mut self,
        table_name: &str,
        row_index: usize,
        old_expression_keys: &std::collections::HashMap<String, Vec<SqlValue>>,
        new_expression_keys: &std::collections::HashMap<String, Vec<SqlValue>>,
    ) {
        for (index_name, metadata) in &self.indexes {
            if !metadata.table_name.eq_ignore_ascii_case(table_name) {
                continue;
            }

            // Only process expression indexes
            if !metadata.columns.iter().any(|col| col.is_expression()) {
                continue;
            }

            let normalized_name = normalize_index_name(index_name);

            // Get pre-computed keys for this index
            let old_key = match old_expression_keys
                .get(&normalized_name)
                .or_else(|| old_expression_keys.get(index_name))
            {
                Some(keys) => keys,
                None => {
                    log::warn!(
                        "No pre-computed old keys provided for expression index '{}' during update",
                        index_name
                    );
                    continue;
                }
            };

            let new_key = match new_expression_keys
                .get(&normalized_name)
                .or_else(|| new_expression_keys.get(index_name))
            {
                Some(keys) => keys,
                None => {
                    log::warn!(
                        "No pre-computed new keys provided for expression index '{}' during update",
                        index_name
                    );
                    continue;
                }
            };

            // Normalize keys
            let old_key_normalized: Vec<SqlValue> = old_key
                .iter()
                .map(|v| crate::database::indexes::index_operations::normalize_for_comparison(v))
                .collect();
            let new_key_normalized: Vec<SqlValue> = new_key
                .iter()
                .map(|v| crate::database::indexes::index_operations::normalize_for_comparison(v))
                .collect();

            // Only update if keys are different
            if old_key_normalized == new_key_normalized {
                continue;
            }

            if let Some(index_data) = self.index_data.get_mut(&normalized_name) {
                match index_data {
                    IndexData::InMemory { data, .. } => {
                        // Remove old key
                        if let Some(row_indices) = data.get_mut(&old_key_normalized) {
                            row_indices.retain(|&idx| idx != row_index);
                            if row_indices.is_empty() {
                                data.remove(&old_key_normalized);
                            }
                        }
                        // Add new key
                        data.entry(new_key_normalized).or_default().push(row_index);
                    }
                    IndexData::DiskBacked { btree, .. } => match acquire_btree_lock(btree) {
                        Ok(mut guard) => {
                            let _ = guard.delete_specific(&old_key_normalized, row_index);
                            if let Err(e) = guard.insert(new_key_normalized, row_index) {
                                log::warn!(
                                    "Failed to update disk-backed expression index '{}': {:?}",
                                    index_name,
                                    e
                                );
                            }
                        }
                        Err(e) => {
                            log::warn!(
                                    "BTreeIndex lock acquisition failed in update_expression_indexes_for_update: {}",
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

    /// Rebuild user-defined indexes after bulk operations that change row indices
    ///
    /// Note: Expression indexes are SKIPPED by this method because they require
    /// expression evaluation which the storage layer cannot perform. Expression
    /// indexes must be rebuilt by the executor layer calling
    /// `maintain_expression_indexes_for_insert` for each row.
    pub fn rebuild_indexes(
        &mut self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        table_rows: &[Row],
    ) {
        // Collect index names that need rebuilding
        // Case-insensitive comparison for table name matching
        // Skip expression indexes - they need to be rebuilt by executor with expression evaluation
        // Skip partial indexes - they need predicate evaluation by the executor
        let indexes_to_rebuild: Vec<String> = self
            .indexes
            .iter()
            .filter(|(_, metadata)| {
                metadata.table_name.eq_ignore_ascii_case(table_name)
                    && !metadata.columns.iter().any(|col| col.is_expression())
                    && !metadata.is_partial()
            })
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
                                            .get_column_index(col.expect_column_name())
                                            .expect("Index column should exist");
                                        let value = &row.values[col_idx];
                                        let truncated = apply_prefix_truncation(value, col.prefix_length());
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
                                            .get_column_index(col.expect_column_name())
                                            .expect("Index column should exist");
                                        let value = &row.values[col_idx];
                                        let truncated = apply_prefix_truncation(value, col.prefix_length());
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
                                        .get_column_index(col.expect_column_name())
                                        .expect("Index column should exist");
                                    table_schema.columns[col_idx].data_type.clone()
                                })
                                .collect();

                            // Acquire the lock once and decide on the rebuild
                            // strategy based on whether a transaction undo-log
                            // is armed (issue #5435).
                            //
                            // A wholesale `bulk_load` + `*guard = new_btree`
                            // swap is the fast path, but it replaces the live
                            // tree object and discards any armed undo-log — so a
                            // rebuild that runs *inside* a transaction (e.g. a
                            // DELETE that compacts away >50% of a table's rows,
                            // see `Database::delete_row`'s
                            // `delete_result.compacted` branch) would not be
                            // reversed on ROLLBACK. When undo-logging is armed
                            // we instead rebuild the *same* tree in place
                            // through the logged delete/insert paths, so the
                            // inverse of the rebuild is captured and ROLLBACK
                            // restores the exact pre-rebuild contents (matching
                            // the #5425 soundness bar). Outside a transaction
                            // the undo-log is `None` and we keep the fast swap.
                            match acquire_btree_lock(btree) {
                                Ok(mut guard) => {
                                    if guard.is_undo_logging() {
                                        if let Err(e) =
                                            guard.rebuild_in_place_logged(sorted_entries)
                                        {
                                            log::warn!(
                                                "Logged in-place rebuild failed for index '{}': {}",
                                                index_name,
                                                e
                                            );
                                        }
                                    } else {
                                        match BTreeIndex::bulk_load(
                                            sorted_entries,
                                            key_schema,
                                            page_manager.clone(),
                                        ) {
                                            Ok(new_btree) => {
                                                *guard = new_btree;
                                            }
                                            Err(e) => {
                                                log::warn!(
                                                    "bulk_load failed for index '{}': {}",
                                                    index_name,
                                                    e
                                                );
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    log::warn!(
                                        "BTreeIndex lock acquisition failed in rebuild_indexes: {}",
                                        e
                                    );
                                }
                            }
                        }
                        IndexData::IVFFlat { index } => {
                            // Rebuild the IVFFlat index from the post-compaction table
                            // rows so its internal row_id references match the new
                            // (renumbered) row positions. A full rebuild re-runs
                            // k-means clustering; compaction is rare, so correctness
                            // is preferred over incremental maintenance here.
                            //
                            // Vector indexes are in-memory and part of the COW
                            // snapshot of `Operations` (#5419/#5425): a rebuild that
                            // runs inside a transaction is restored on ROLLBACK by
                            // the snapshot clone — no disk undo-log is involved.
                            let col_idx = match table_schema
                                .get_column_index(metadata.columns[0].expect_column_name())
                            {
                                Some(idx) => idx,
                                None => {
                                    log::warn!(
                                        "IVFFlat index '{}' column not found during rebuild; skipping",
                                        index_name
                                    );
                                    continue;
                                }
                            };

                            let vectors = Self::extract_vectors_for_rebuild(table_rows, col_idx);

                            // Preserve the configured parameters of the existing index.
                            let mut rebuilt = crate::database::indexes::ivfflat::IVFFlatIndex::new(
                                index.dimensions(),
                                index.num_lists() as u32,
                                index.metric(),
                            );
                            rebuilt.set_probes(index.probes());

                            if let Err(e) = rebuilt.build(vectors) {
                                log::warn!(
                                    "Failed to rebuild IVFFlat index '{}': {}",
                                    index_name,
                                    e
                                );
                            } else {
                                *index = rebuilt;
                            }
                        }
                        IndexData::Hnsw { index } => {
                            // Rebuild the HNSW graph from the post-compaction table
                            // rows so its internal row_id references match the new
                            // (renumbered) row positions. HNSW graph construction is
                            // O(n log n); compaction is rare, so a full rebuild is
                            // acceptable in exchange for correctness.
                            //
                            // Like IVFFlat, HNSW indexes are in-memory and part of
                            // the COW snapshot of `Operations`, so an in-transaction
                            // rebuild is reversed on ROLLBACK by the snapshot clone.
                            let col_idx = match table_schema
                                .get_column_index(metadata.columns[0].expect_column_name())
                            {
                                Some(idx) => idx,
                                None => {
                                    log::warn!(
                                        "HNSW index '{}' column not found during rebuild; skipping",
                                        index_name
                                    );
                                    continue;
                                }
                            };

                            let vectors = Self::extract_vectors_for_rebuild(table_rows, col_idx);

                            // Preserve the configured parameters of the existing index.
                            let mut rebuilt = crate::database::indexes::hnsw::HnswIndex::new(
                                index.dimensions(),
                                index.m() as u32,
                                index.ef_construction() as u32,
                                index.metric(),
                            );
                            rebuilt.set_ef_search(index.ef_search());

                            if let Err(e) = rebuilt.build(vectors) {
                                log::warn!("Failed to rebuild HNSW index '{}': {}", index_name, e);
                            } else {
                                *index = rebuilt;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Extract `(row_id, vector)` pairs from the given table rows for the vector
    /// column at `col_idx`, for rebuilding an IVFFlat/HNSW index after compaction.
    ///
    /// `row_id` is the row's position in `table_rows` (the post-compaction
    /// position), which is exactly what vector search must return so callers can
    /// look the row back up in the compacted table. NULL / non-vector cells are
    /// skipped, mirroring the create path.
    fn extract_vectors_for_rebuild(table_rows: &[Row], col_idx: usize) -> Vec<(usize, Vec<f64>)> {
        let mut vectors = Vec::new();
        for (row_index, row) in table_rows.iter().enumerate() {
            if col_idx < row.values.len() {
                if let Some(vec_data) = IndexManager::extract_vector(&row.values[col_idx]) {
                    vectors.push((row_index, vec_data));
                }
            }
        }
        vectors
    }

    // ============================================================================
    // Expression Index Query Methods
    // ============================================================================

    /// Get expression indexes for a specific table
    ///
    /// Returns metadata for all expression indexes on the table. Expression indexes
    /// are indexes where at least one column is an expression rather than a simple
    /// column reference.
    ///
    /// This is used by the executor layer to identify which indexes need expression
    /// evaluation during DML operations.
    ///
    /// # Arguments
    /// * `table_name` - The table name (case-insensitive)
    ///
    /// # Returns
    /// Vector of (normalized_index_name, IndexMetadata) pairs for expression indexes
    pub fn get_expression_indexes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<(String, &IndexMetadata)> {
        let search_name_lower = table_name.to_lowercase();
        let search_table_only = search_name_lower.rsplit('.').next().unwrap_or(&search_name_lower);

        self.indexes
            .iter()
            .filter_map(|(normalized_name, metadata)| {
                let stored_lower = metadata.table_name.to_lowercase();
                let stored_table_only = stored_lower.rsplit('.').next().unwrap_or(&stored_lower);

                // Check if table matches
                if stored_lower != search_name_lower && stored_table_only != search_table_only {
                    return None;
                }

                // Check if it's an expression index
                if metadata.columns.iter().any(|col| col.is_expression()) {
                    Some((normalized_name.clone(), metadata))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Check if a table has any expression indexes
    ///
    /// This is a fast check used to determine whether DML operations need to
    /// evaluate expressions for index maintenance.
    pub fn has_expression_indexes(&self, table_name: &str) -> bool {
        let search_name_lower = table_name.to_lowercase();
        let search_table_only = search_name_lower.rsplit('.').next().unwrap_or(&search_name_lower);

        self.indexes.iter().any(|(_, metadata)| {
            let stored_lower = metadata.table_name.to_lowercase();
            let stored_table_only = stored_lower.rsplit('.').next().unwrap_or(&stored_lower);

            (stored_lower == search_name_lower || stored_table_only == search_table_only)
                && metadata.columns.iter().any(|col| col.is_expression())
        })
    }

    /// Clear expression index data for a table (for rebuilding after compaction)
    ///
    /// This clears the index data (but keeps metadata) for all expression indexes
    /// on the given table. Used before rebuilding expression indexes.
    pub fn clear_expression_index_data(&mut self, table_name: &str) {
        let search_name_lower = table_name.to_lowercase();
        let search_table_only = search_name_lower.rsplit('.').next().unwrap_or(&search_name_lower);

        // Find expression indexes for this table
        let indexes_to_clear: Vec<String> = self
            .indexes
            .iter()
            .filter_map(|(name, metadata)| {
                let stored_lower = metadata.table_name.to_lowercase();
                let stored_table_only = stored_lower.rsplit('.').next().unwrap_or(&stored_lower);

                if (stored_lower == search_name_lower || stored_table_only == search_table_only)
                    && metadata.columns.iter().any(|col| col.is_expression())
                {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect();

        // Clear the data for each expression index
        for index_name in indexes_to_clear {
            if let Some(index_data) = self.index_data.get_mut(&index_name) {
                match index_data {
                    IndexData::InMemory { data, pending_deletions } => {
                        data.clear();
                        pending_deletions.clear();
                    }
                    IndexData::DiskBacked { .. } => {
                        // Expression indexes currently use InMemory storage
                        // Disk-backed clearing would need BTreeIndex::clear() implementation
                        log::warn!(
                            "Disk-backed expression index '{}' clearing not yet supported",
                            index_name
                        );
                    }
                    IndexData::IVFFlat { .. } | IndexData::Hnsw { .. } => {
                        // Vector indexes don't support expression indexing
                    }
                }
            }
        }
    }
}
