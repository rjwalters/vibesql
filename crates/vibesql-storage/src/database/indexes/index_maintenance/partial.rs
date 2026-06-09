// ============================================================================
// Partial-Index Maintenance Operations
// ============================================================================
//
// Partial indexes (`CREATE INDEX ... WHERE predicate`) require evaluating the
// predicate to decide whether each row belongs in the index. The storage
// layer cannot evaluate expressions, so the executor crate evaluates the
// predicate per row and then calls these methods with a pre-computed set of
// index names that should include the row (or, for updates, sets describing
// the predicate's truthy value before and after the update).
//
// The standard insert/update/delete maintenance methods (in `insert.rs`,
// `update.rs`, `delete.rs`) explicitly skip partial indexes — they only
// know how to maintain full-coverage indexes. The methods in this module
// fill in the partial-index path.

use std::collections::HashSet;

use vibesql_types::SqlValue;

use super::prefix::apply_prefix_truncation;
use crate::{
    database::indexes::{
        index_manager::IndexManager,
        index_metadata::{acquire_btree_lock, IndexData},
    },
    Row, StorageError,
};

impl IndexManager {
    /// Maintain partial indexes after inserting a row.
    ///
    /// `included_partial_indexes` is the set of normalized partial-index
    /// names whose WHERE predicate evaluated to truthy for this row. Only
    /// those indexes get the new entry.
    pub fn add_to_partial_indexes_for_insert(
        &mut self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        row: &Row,
        row_index: usize,
        included_partial_indexes: &HashSet<String>,
    ) {
        for (index_name, metadata) in &self.indexes {
            if !metadata.table_name.eq_ignore_ascii_case(table_name) {
                continue;
            }
            if !metadata.is_partial() {
                continue;
            }
            // Expression-based partial indexes are not yet supported; the
            // executor passes column-based partial indexes only.
            if metadata.columns.iter().any(|col| col.is_expression()) {
                continue;
            }
            if !included_partial_indexes.contains(index_name) {
                continue;
            }

            if let Some(index_data) = self.index_data.get_mut(index_name) {
                let key_values: Vec<SqlValue> = metadata
                    .columns
                    .iter()
                    .map(|col| {
                        let col_name = col
                            .column_name()
                            .expect("Partial-index column should have a column name");
                        let col_idx = table_schema
                            .get_column_index(col_name)
                            .expect("Index column should exist");
                        let value = &row.values[col_idx];
                        let truncated = apply_prefix_truncation(value, col.prefix_length());
                        crate::database::indexes::index_operations::normalize_for_comparison(
                            &truncated,
                        )
                    })
                    .collect();

                match index_data {
                    IndexData::InMemory { data, .. } => {
                        data.entry(key_values).or_default().push(row_index);
                    }
                    IndexData::DiskBacked { btree, .. } => match acquire_btree_lock(btree) {
                        Ok(mut guard) => {
                            if let Err(e) = guard.insert(key_values, row_index) {
                                log::warn!(
                                    "Failed to insert into disk-backed partial index '{}': {:?}",
                                    index_name,
                                    e
                                );
                            }
                        }
                        Err(e) => {
                            log::warn!(
                                "BTreeIndex lock acquisition failed in add_to_partial_indexes_for_insert: {}",
                                e
                            );
                        }
                    },
                    IndexData::IVFFlat { .. } | IndexData::Hnsw { .. } => {
                        // Partial vector indexes are not currently supported.
                    }
                }
            }
        }
    }

    /// Maintain partial indexes after updating a row.
    ///
    /// `old_included` / `new_included` are the sets of normalized partial-index
    /// names whose WHERE predicate evaluated to truthy on the old and new rows
    /// respectively. For each partial index, the four possible transitions are
    /// handled separately:
    ///   - was-out → is-out: no work.
    ///   - was-out → is-in: insert the new key.
    ///   - was-in → is-out: remove the old key.
    ///   - was-in → is-in: if keys differ, remove old + insert new; else no-op.
    #[allow(clippy::too_many_arguments)]
    pub fn update_partial_indexes_for_update(
        &mut self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        old_row: &Row,
        new_row: &Row,
        row_index: usize,
        old_included: &HashSet<String>,
        new_included: &HashSet<String>,
    ) {
        let target_indexes: Vec<String> = self
            .indexes
            .iter()
            .filter(|(_, metadata)| {
                metadata.table_name.eq_ignore_ascii_case(table_name)
                    && metadata.is_partial()
                    && !metadata.columns.iter().any(|col| col.is_expression())
            })
            .map(|(name, _)| name.clone())
            .collect();

        for index_name in target_indexes {
            let was_in = old_included.contains(&index_name);
            let is_in = new_included.contains(&index_name);

            // Snapshot metadata fields we need before taking the mutable
            // borrow on `index_data`.
            let columns = match self.indexes.get(&index_name) {
                Some(meta) => meta.columns.clone(),
                None => continue,
            };

            let build_key = |source: &Row| -> Vec<SqlValue> {
                columns
                    .iter()
                    .map(|col| {
                        let col_name = col
                            .column_name()
                            .expect("Partial-index column should have a column name");
                        let col_idx = table_schema
                            .get_column_index(col_name)
                            .expect("Index column should exist");
                        let value = &source.values[col_idx];
                        let truncated = apply_prefix_truncation(value, col.prefix_length());
                        crate::database::indexes::index_operations::normalize_for_comparison(
                            &truncated,
                        )
                    })
                    .collect()
            };

            let old_key = if was_in { Some(build_key(old_row)) } else { None };
            let new_key = if is_in { Some(build_key(new_row)) } else { None };

            // No-op if keys are present and identical (was-in → is-in with
            // same key value).
            if let (Some(ok), Some(nk)) = (&old_key, &new_key) {
                if ok == nk {
                    continue;
                }
            }

            if let Some(index_data) = self.index_data.get_mut(&index_name) {
                match index_data {
                    IndexData::InMemory { data, .. } => {
                        if let Some(ok) = &old_key {
                            if let Some(row_indices) = data.get_mut(ok) {
                                row_indices.retain(|&idx| idx != row_index);
                                if row_indices.is_empty() {
                                    data.remove(ok);
                                }
                            }
                        }
                        if let Some(nk) = new_key {
                            data.entry(nk).or_default().push(row_index);
                        }
                    }
                    IndexData::DiskBacked { btree, .. } => match acquire_btree_lock(btree) {
                        Ok(mut guard) => {
                            if let Some(ok) = &old_key {
                                let _ = guard.delete_specific(ok, row_index);
                            }
                            if let Some(nk) = new_key {
                                if let Err(e) = guard.insert(nk, row_index) {
                                    log::warn!(
                                        "Failed to insert into disk-backed partial index '{}': {:?}",
                                        index_name,
                                        e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            log::warn!(
                                "BTreeIndex lock acquisition failed in update_partial_indexes_for_update: {}",
                                e
                            );
                        }
                    },
                    IndexData::IVFFlat { .. } | IndexData::Hnsw { .. } => {
                        // Partial vector indexes are not currently supported.
                    }
                }
            }
        }
    }

    /// Maintain partial indexes after deleting a row.
    ///
    /// `included_partial_indexes` is the set of normalized partial-index
    /// names whose WHERE predicate evaluated to truthy for the (about to be)
    /// deleted row. Only those indexes need an entry removed.
    pub fn update_partial_indexes_for_delete(
        &mut self,
        table_name: &str,
        table_schema: &vibesql_catalog::TableSchema,
        values: &[SqlValue],
        row_index: usize,
        included_partial_indexes: &HashSet<String>,
    ) {
        for (index_name, metadata) in &self.indexes {
            if !metadata.table_name.eq_ignore_ascii_case(table_name) {
                continue;
            }
            if !metadata.is_partial() {
                continue;
            }
            if metadata.columns.iter().any(|col| col.is_expression()) {
                continue;
            }
            if !included_partial_indexes.contains(index_name) {
                continue;
            }

            if let Some(index_data) = self.index_data.get_mut(index_name) {
                let key_values: Vec<SqlValue> = metadata
                    .columns
                    .iter()
                    .map(|col| {
                        let col_name = col
                            .column_name()
                            .expect("Partial-index column should have a column name");
                        let col_idx = table_schema
                            .get_column_index(col_name)
                            .expect("Index column should exist");
                        let value = &values[col_idx];
                        let truncated = apply_prefix_truncation(value, col.prefix_length());
                        crate::database::indexes::index_operations::normalize_for_comparison(
                            &truncated,
                        )
                    })
                    .collect();

                match index_data {
                    IndexData::InMemory { data, .. } => {
                        if let Some(row_indices) = data.get_mut(&key_values) {
                            row_indices.retain(|&idx| idx != row_index);
                            if row_indices.is_empty() {
                                data.remove(&key_values);
                            }
                        }
                    }
                    IndexData::DiskBacked { btree, .. } => match acquire_btree_lock(btree) {
                        Ok(mut guard) => {
                            let _ = guard.delete_specific(&key_values, row_index);
                        }
                        Err(e) => {
                            log::warn!(
                                "BTreeIndex lock acquisition failed in update_partial_indexes_for_delete: {}",
                                e
                            );
                        }
                    },
                    IndexData::IVFFlat { .. } | IndexData::Hnsw { .. } => {}
                }
            }
        }
    }

    /// Check whether inserting `key_values` would violate the uniqueness of
    /// partial UNIQUE index `index_name`.
    ///
    /// The caller is responsible for having already evaluated the index's
    /// WHERE predicate against the candidate row; only call this when the
    /// predicate is truthy. Storage's index body only contains rows that
    /// satisfy the predicate, so any colliding key here corresponds to an
    /// existing row that also satisfied the predicate — exactly the SQLite
    /// semantics for partial UNIQUE indexes.
    pub fn check_partial_unique_conflict(
        &self,
        index_name: &str,
        key_values: &[SqlValue],
    ) -> Result<bool, StorageError> {
        let normalized = crate::database::indexes::index_metadata::normalize_index_name(index_name);
        let Some(index_data) = self.index_data.get(&normalized) else {
            return Ok(false);
        };
        // SQLite ignores all-NULL keys for UNIQUE checks.
        if key_values.iter().all(|v| matches!(v, SqlValue::Null)) {
            return Ok(false);
        }
        match index_data {
            IndexData::InMemory { data, .. } => Ok(data.contains_key(key_values)),
            IndexData::DiskBacked { btree, .. } => {
                let guard = acquire_btree_lock(btree)?;
                let key_vec = key_values.to_vec();
                Ok(guard.lookup(&key_vec).map(|ids| !ids.is_empty()).unwrap_or(false))
            }
            _ => Ok(false),
        }
    }

    /// Whether the given table has any partial indexes.
    pub fn has_partial_indexes(&self, table_name: &str) -> bool {
        self.indexes.values().any(|metadata| {
            metadata.table_name.eq_ignore_ascii_case(table_name) && metadata.is_partial()
        })
    }

    /// Get all partial indexes for a specific table.
    ///
    /// Returns `(normalized_index_name, IndexMetadata)` pairs for use by the
    /// executor crate (which needs the `where_clause` to evaluate predicates).
    pub fn get_partial_indexes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<(String, &crate::database::indexes::index_metadata::IndexMetadata)> {
        self.indexes
            .iter()
            .filter(|(_, metadata)| {
                metadata.table_name.eq_ignore_ascii_case(table_name) && metadata.is_partial()
            })
            .map(|(name, metadata)| (name.clone(), metadata))
            .collect()
    }
}
