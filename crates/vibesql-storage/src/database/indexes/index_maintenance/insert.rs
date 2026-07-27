// ============================================================================
// Index Insertion Operations
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
    /// Add row to user-defined indexes after insert
    /// This should be called AFTER the row has been added to the table
    ///
    /// Note: Expression indexes require pre-computed keys via
    /// `add_to_expression_indexes_for_insert`. This method skips expression indexes since it
    /// cannot evaluate expressions.
    ///
    /// Partial indexes are also skipped here. The storage layer cannot evaluate
    /// the WHERE predicate, so the executor crate must call
    /// `add_to_partial_indexes_for_insert` with a pre-computed inclusion set.
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
                // Skip expression indexes - they need pre-computed keys
                // Expression indexes are handled by add_to_expression_indexes_for_insert
                if metadata.columns.iter().any(|col| col.is_expression()) {
                    continue;
                }

                // Skip partial indexes - they need pre-evaluated predicates
                // Partial indexes are handled by add_to_partial_indexes_for_insert
                if metadata.is_partial() {
                    continue;
                }

                if let Some(index_data) = self.index_data.get_mut(index_name) {
                    // Build composite key from the indexed columns
                    // Normalize numeric types to ensure consistent comparison
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
                            let value = &row.values[col_idx];
                            let truncated = apply_prefix_truncation(value, col.prefix_length());
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
                            // Incrementally add this row's vector so the index
                            // stays in sync without a full rebuild. The vector is
                            // assigned to its nearest centroid (O(probe)); if the
                            // index is not yet trained it lands in the staging
                            // list, matching IVFFlatIndex::insert semantics.
                            if let Some(col_idx) = Self::vector_index_column_idx(
                                metadata,
                                table_schema,
                                index_name,
                            ) {
                                if let Some(vector) = Self::extract_vector(&row.values[col_idx]) {
                                    if let Err(e) = index.insert(row_index, vector) {
                                        log::warn!(
                                            "Failed to insert into IVFFlat index '{}': {}",
                                            index_name,
                                            e
                                        );
                                    }
                                }
                                // NULL / non-vector cells are not indexed, mirroring
                                // the build path.
                            }
                        }
                        IndexData::Hnsw { index } => {
                            // HNSW supports incremental inserts natively (no
                            // training step); wire the row's vector into the graph.
                            if let Some(col_idx) = Self::vector_index_column_idx(
                                metadata,
                                table_schema,
                                index_name,
                            ) {
                                if let Some(vector) = Self::extract_vector(&row.values[col_idx]) {
                                    if let Err(e) = index.insert(row_index, vector) {
                                        log::warn!(
                                            "Failed to insert into HNSW index '{}': {}",
                                            index_name,
                                            e
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Add row to expression indexes after insert with pre-computed keys
    ///
    /// This method handles expression indexes which require pre-computed key values
    /// since the storage layer cannot evaluate expressions.
    ///
    /// # Arguments
    /// * `table_name` - The table name
    /// * `row_index` - The index of the row in the table
    /// * `expression_keys` - Map of index name to pre-computed key values
    pub fn add_to_expression_indexes_for_insert(
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
                        "No pre-computed keys provided for expression index '{}' during insert",
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
                        data.entry(normalized_key).or_default().push(row_index);
                    }
                    IndexData::DiskBacked { btree, .. } => match acquire_btree_lock(btree) {
                        Ok(mut guard) => {
                            if let Err(e) = guard.insert(normalized_key, row_index) {
                                log::warn!(
                                    "Failed to insert into disk-backed expression index '{}': {:?}",
                                    index_name,
                                    e
                                );
                            }
                        }
                        Err(e) => {
                            log::warn!(
                                    "BTreeIndex lock acquisition failed in add_to_expression_indexes_for_insert: {}",
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

    /// Batch add rows to user-defined indexes after insert
    ///
    /// This is significantly more efficient than calling `add_to_indexes_for_insert` in a loop
    /// because it:
    /// 1. Pre-computes column indices once per index (not per row)
    /// 2. Builds all keys in a single pass per index
    /// 3. Batch-inserts entries into each index
    ///
    /// Note: Expression indexes are skipped - use `batch_add_to_expression_indexes_for_insert` for
    /// them.
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
        // Skip expression indexes - they need pre-computed keys
        // Skip partial indexes - they need pre-evaluated predicates
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
                    IndexData::IVFFlat { index } => {
                        // Incrementally add each inserted row's vector. The single
                        // indexed vector column lives at column_info[0].
                        if let Some(&(col_idx, _)) = column_info.first() {
                            for &(row_index, row) in rows_to_insert {
                                if let Some(vector) = Self::extract_vector(&row.values[col_idx]) {
                                    if let Err(e) = index.insert(row_index, vector) {
                                        log::warn!(
                                            "Failed to batch-insert into IVFFlat index '{}': {}",
                                            index_name,
                                            e
                                        );
                                    }
                                }
                            }
                        }
                    }
                    IndexData::Hnsw { index } => {
                        if let Some(&(col_idx, _)) = column_info.first() {
                            for &(row_index, row) in rows_to_insert {
                                if let Some(vector) = Self::extract_vector(&row.values[col_idx]) {
                                    if let Err(e) = index.insert(row_index, vector) {
                                        log::warn!(
                                            "Failed to batch-insert into HNSW index '{}': {}",
                                            index_name,
                                            e
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
