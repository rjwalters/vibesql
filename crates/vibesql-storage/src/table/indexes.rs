// ============================================================================
// Index Manager - Hash-based index management for primary keys and unique constraints
// ============================================================================

use std::collections::{HashMap, HashSet};

use vibesql_types::SqlValue;

use crate::Row;

/// Represents different types of indexes that can be affected by column updates
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IndexType {
    /// Primary key index
    PrimaryKey,
    /// Unique constraint index (with constraint index)
    UniqueConstraint(usize),
}

/// Manages hash-based indexes for primary keys and unique constraints
///
/// This component encapsulates all index-related operations, maintaining
/// hash maps that provide O(1) lookup for constraint validation.
#[derive(Debug, Clone)]
pub(crate) struct IndexManager {
    primary_key_index: Option<HashMap<Vec<SqlValue>, usize>>,
    unique_indexes: Vec<HashMap<Vec<SqlValue>, usize>>,
}

impl IndexManager {
    /// Create a new IndexManager initialized for the given schema
    pub(crate) fn new(schema: &vibesql_catalog::TableSchema) -> Self {
        let primary_key_index =
            if schema.primary_key.is_some() { Some(HashMap::new()) } else { None };

        let unique_indexes = (0..schema.unique_constraints.len()).map(|_| HashMap::new()).collect();

        IndexManager { primary_key_index, unique_indexes }
    }

    /// Get reference to primary key index
    #[inline]
    pub(crate) fn primary_key_index(&self) -> Option<&HashMap<Vec<SqlValue>, usize>> {
        self.primary_key_index.as_ref()
    }

    /// Get reference to unique constraint indexes
    #[inline]
    pub(crate) fn unique_indexes(&self) -> &[HashMap<Vec<SqlValue>, usize>] {
        &self.unique_indexes
    }

    /// Update hash indexes when inserting a row
    #[inline]
    pub(crate) fn update_for_insert(
        &mut self,
        schema: &vibesql_catalog::TableSchema,
        row: &Row,
        row_index: usize,
    ) {
        // Update primary key index
        if let Some(ref mut pk_index) = self.primary_key_index {
            if let Some(pk_indices) = schema.get_primary_key_indices() {
                let pk_values: Vec<SqlValue> =
                    pk_indices.iter().map(|&idx| row.values[idx].clone()).collect();
                pk_index.insert(pk_values, row_index);
            }
        }

        // Update unique constraint indexes
        let unique_constraint_indices = schema.get_unique_constraint_indices();
        for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
            let unique_values: Vec<SqlValue> =
                unique_indices.iter().map(|&idx| row.values[idx].clone()).collect();

            // Skip if any value in the unique constraint is NULL
            if unique_values.contains(&SqlValue::Null) {
                continue;
            }

            if let Some(unique_index) = self.unique_indexes.get_mut(constraint_idx) {
                unique_index.insert(unique_values, row_index);
            }
        }
    }

    /// Update hash indexes when updating a row
    #[inline]
    pub(crate) fn update_for_update(
        &mut self,
        schema: &vibesql_catalog::TableSchema,
        old_row: &Row,
        new_row: &Row,
        row_index: usize,
    ) {
        // Update primary key index
        if let Some(ref mut pk_index) = self.primary_key_index {
            if let Some(pk_indices) = schema.get_primary_key_indices() {
                let old_pk_values: Vec<SqlValue> =
                    pk_indices.iter().map(|&idx| old_row.values[idx].clone()).collect();
                let new_pk_values: Vec<SqlValue> =
                    pk_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

                // Remove old key if different from new key
                if old_pk_values != new_pk_values {
                    pk_index.remove(&old_pk_values);
                    pk_index.insert(new_pk_values, row_index);
                }
            }
        }

        // Update unique constraint indexes
        let unique_constraint_indices = schema.get_unique_constraint_indices();
        for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
            let old_unique_values: Vec<SqlValue> =
                unique_indices.iter().map(|&idx| old_row.values[idx].clone()).collect();
            let new_unique_values: Vec<SqlValue> =
                unique_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

            if let Some(unique_index) = self.unique_indexes.get_mut(constraint_idx) {
                // Remove old key if it's different and not NULL
                if old_unique_values != new_unique_values
                    && !old_unique_values.contains(&SqlValue::Null)
                {
                    unique_index.remove(&old_unique_values);
                }

                // Insert new key if not NULL
                if !new_unique_values.contains(&SqlValue::Null) {
                    unique_index.insert(new_unique_values, row_index);
                }
            }
        }
    }

    /// Determine which indexes are affected by column changes
    ///
    /// Returns an enum describing which indexes need updating based on the changed columns.
    /// This allows selective index maintenance for performance optimization.
    ///
    /// # Arguments
    /// * `schema` - Table schema containing index definitions
    /// * `changed_columns` - Set of column indices that were modified
    ///
    /// # Returns
    /// Vector of index types that are affected by the changes
    pub(crate) fn get_affected_indexes(
        &self,
        schema: &vibesql_catalog::TableSchema,
        changed_columns: &HashSet<usize>,
    ) -> Vec<IndexType> {
        let mut affected = Vec::new();

        // Check if primary key affected
        if let Some(pk_indices) = schema.get_primary_key_indices() {
            if pk_indices.iter().any(|idx| changed_columns.contains(idx)) {
                affected.push(IndexType::PrimaryKey);
            }
        }

        // Check which unique constraints affected
        let unique_constraint_indices = schema.get_unique_constraint_indices();
        for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
            if unique_indices.iter().any(|col_idx| changed_columns.contains(col_idx)) {
                affected.push(IndexType::UniqueConstraint(constraint_idx));
            }
        }

        affected
    }

    /// Update only affected indexes after a row update
    ///
    /// This is a performance optimization that only updates indexes that reference
    /// changed columns, rather than updating all indexes unconditionally.
    ///
    /// # Arguments
    /// * `schema` - Table schema containing index definitions
    /// * `old_row` - Row data before the update
    /// * `new_row` - Row data after the update
    /// * `row_index` - Index of the row in the table
    /// * `affected_indexes` - List of indexes that need updating
    #[inline]
    pub(crate) fn update_selective(
        &mut self,
        schema: &vibesql_catalog::TableSchema,
        old_row: &Row,
        new_row: &Row,
        row_index: usize,
        affected_indexes: &[IndexType],
    ) {
        for index_type in affected_indexes {
            match index_type {
                IndexType::PrimaryKey => {
                    // Update primary key index
                    if let Some(ref mut pk_index) = self.primary_key_index {
                        if let Some(pk_indices) = schema.get_primary_key_indices() {
                            let old_pk_values: Vec<SqlValue> =
                                pk_indices.iter().map(|&idx| old_row.values[idx].clone()).collect();
                            let new_pk_values: Vec<SqlValue> =
                                pk_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

                            // Remove old key if different from new key
                            if old_pk_values != new_pk_values {
                                pk_index.remove(&old_pk_values);
                                pk_index.insert(new_pk_values, row_index);
                            }
                        }
                    }
                }
                IndexType::UniqueConstraint(constraint_idx) => {
                    // Update specific unique constraint index
                    let unique_constraint_indices = schema.get_unique_constraint_indices();
                    if let Some(unique_indices) = unique_constraint_indices.get(*constraint_idx) {
                        let old_unique_values: Vec<SqlValue> =
                            unique_indices.iter().map(|&idx| old_row.values[idx].clone()).collect();
                        let new_unique_values: Vec<SqlValue> =
                            unique_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

                        if let Some(unique_index) = self.unique_indexes.get_mut(*constraint_idx) {
                            // Remove old key if it's different and not NULL
                            if old_unique_values != new_unique_values
                                && !old_unique_values.contains(&SqlValue::Null)
                            {
                                unique_index.remove(&old_unique_values);
                            }

                            // Insert new key if not NULL
                            if !new_unique_values.contains(&SqlValue::Null) {
                                unique_index.insert(new_unique_values, row_index);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Update hash indexes when deleting a row
    ///
    /// Removes the row's key values from primary key and unique constraint indexes.
    /// Note: After deleting rows, if row indices change, you must call `rebuild()`.
    ///
    /// # Arguments
    /// * `schema` - Table schema containing index definitions
    /// * `row` - Row being deleted
    #[inline]
    pub(crate) fn update_for_delete(&mut self, schema: &vibesql_catalog::TableSchema, row: &Row) {
        // Update primary key index
        if let Some(ref mut pk_index) = self.primary_key_index {
            if let Some(pk_indices) = schema.get_primary_key_indices() {
                let pk_values: Vec<SqlValue> =
                    pk_indices.iter().map(|&idx| row.values[idx].clone()).collect();
                pk_index.remove(&pk_values);
            }
        }

        // Update unique constraint indexes
        let unique_constraint_indices = schema.get_unique_constraint_indices();
        for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
            let unique_values: Vec<SqlValue> =
                unique_indices.iter().map(|&idx| row.values[idx].clone()).collect();

            // Skip if any value in the unique constraint is NULL
            if unique_values.contains(&SqlValue::Null) {
                continue;
            }

            if let Some(unique_index) = self.unique_indexes.get_mut(constraint_idx) {
                unique_index.remove(&unique_values);
            }
        }
    }

    /// Batch update hash indexes when deleting multiple rows
    ///
    /// This is significantly more efficient than calling `update_for_delete` in a loop
    /// because it pre-computes column indices once for all rows instead of per-row.
    ///
    /// # Performance
    /// - O(1) schema lookups (vs O(d) for d deletes with single-row method)
    /// - Reduces CPU overhead by ~30-40% for multi-row deletes
    ///
    /// # Arguments
    /// * `schema` - Table schema containing index definitions
    /// * `rows` - Slice of rows being deleted
    #[inline]
    pub(crate) fn batch_update_for_delete(
        &mut self,
        schema: &vibesql_catalog::TableSchema,
        rows: &[&Row],
    ) {
        if rows.is_empty() {
            return;
        }

        // Pre-compute PK column indices once (O(1) instead of O(d))
        let pk_indices = schema.get_primary_key_indices();

        // Pre-compute unique constraint column indices once
        let unique_constraint_indices = schema.get_unique_constraint_indices();

        // Update primary key index for all rows
        if let (Some(ref mut pk_index), Some(pk_cols)) = (&mut self.primary_key_index, pk_indices) {
            for row in rows {
                let pk_values: Vec<SqlValue> =
                    pk_cols.iter().map(|&idx| row.values[idx].clone()).collect();
                pk_index.remove(&pk_values);
            }
        }

        // Update unique constraint indexes for all rows
        for (constraint_idx, unique_cols) in unique_constraint_indices.iter().enumerate() {
            if let Some(unique_index) = self.unique_indexes.get_mut(constraint_idx) {
                for row in rows {
                    let unique_values: Vec<SqlValue> =
                        unique_cols.iter().map(|&idx| row.values[idx].clone()).collect();

                    // Skip if any value in the unique constraint is NULL
                    if unique_values.contains(&SqlValue::Null) {
                        continue;
                    }

                    unique_index.remove(&unique_values);
                }
            }
        }
    }

    /// Rebuild all hash indexes from scratch
    ///
    /// Used after bulk operations that change row indices (e.g., deletes that shift rows).
    /// Clears all indexes and rebuilds them from the current rows.
    ///
    /// # Arguments
    /// * `schema` - Table schema containing index definitions
    /// * `rows` - All current rows in the table
    pub(crate) fn rebuild(&mut self, schema: &vibesql_catalog::TableSchema, rows: &[Row]) {
        self.clear();

        // Rebuild from current rows
        for (row_index, row) in rows.iter().enumerate() {
            self.update_for_insert(schema, row, row_index);
        }
    }

    // NOTE: adjust_after_delete and adjust_after_multi_delete methods were removed
    // as part of the deletion bitmap optimization (issue #3779). With the bitmap approach,
    // rows are marked as deleted rather than physically removed, so index entry positions
    // no longer need adjustment. Indexes are only rebuilt during compaction.

    /// Clear all indexes
    ///
    /// Removes all entries from primary key and unique constraint indexes.
    pub(crate) fn clear(&mut self) {
        if let Some(ref mut pk_index) = self.primary_key_index {
            pk_index.clear();
        }
        for unique_index in &mut self.unique_indexes {
            unique_index.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    use super::*;

    #[test]
    fn test_primary_key_insert() {
        let schema = TableSchema::with_primary_key(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
            ],
            vec!["id".to_string()],
        );

        let mut index_mgr = IndexManager::new(&schema);

        // Insert a row
        let row = Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]);
        index_mgr.update_for_insert(&schema, &row, 0);

        // Verify primary key index has the entry
        assert!(index_mgr.primary_key_index().is_some());
        let pk_index = index_mgr.primary_key_index().unwrap();
        assert_eq!(pk_index.len(), 1);
        assert_eq!(pk_index.get(&vec![SqlValue::Integer(1)]), Some(&0));
    }

    #[test]
    fn test_unique_constraint_insert() {
        let schema = TableSchema::with_unique_constraints(
            "products".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "sku".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
            ],
            vec![vec!["sku".to_string()]],
        );

        let mut index_mgr = IndexManager::new(&schema);

        // Insert a row
        let row = Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("SKU001"))]);
        index_mgr.update_for_insert(&schema, &row, 0);

        // Verify unique index has the entry
        assert_eq!(index_mgr.unique_indexes().len(), 1);
        let unique_index = &index_mgr.unique_indexes()[0];
        assert_eq!(unique_index.len(), 1);
        assert_eq!(unique_index.get(&vec![SqlValue::Varchar(arcstr::ArcStr::from("SKU001"))]), Some(&0));
    }

    #[test]
    fn test_null_handling_in_unique_index() {
        let schema = TableSchema::with_unique_constraints(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "email".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    true, // nullable
                ),
            ],
            vec![vec!["email".to_string()]],
        );

        let mut index_mgr = IndexManager::new(&schema);

        // Insert a row with NULL email
        let row = Row::new(vec![SqlValue::Integer(1), SqlValue::Null]);
        index_mgr.update_for_insert(&schema, &row, 0);

        // Verify NULL is NOT in the unique index
        let unique_index = &index_mgr.unique_indexes()[0];
        assert_eq!(unique_index.len(), 0, "NULL values should not be indexed");
    }

    #[test]
    fn test_selective_update_detection() {
        let schema = TableSchema::with_all_constraints(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "email".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
            ],
            Some(vec!["id".to_string()]),
            vec![vec!["email".to_string()]],
        );

        let index_mgr = IndexManager::new(&schema);

        // Test 1: Only non-indexed column changed (name)
        let mut changed = HashSet::new();
        changed.insert(2); // name column
        let affected = index_mgr.get_affected_indexes(&schema, &changed);
        assert_eq!(affected.len(), 0, "Non-indexed column should not affect indexes");

        // Test 2: Primary key column changed
        let mut changed = HashSet::new();
        changed.insert(0); // id column
        let affected = index_mgr.get_affected_indexes(&schema, &changed);
        assert_eq!(affected.len(), 1);
        assert_eq!(affected[0], IndexType::PrimaryKey);

        // Test 3: Unique constraint column changed
        let mut changed = HashSet::new();
        changed.insert(1); // email column
        let affected = index_mgr.get_affected_indexes(&schema, &changed);
        assert_eq!(affected.len(), 1);
        assert_eq!(affected[0], IndexType::UniqueConstraint(0));

        // Test 4: Multiple indexed columns changed
        let mut changed = HashSet::new();
        changed.insert(0); // id
        changed.insert(1); // email
        let affected = index_mgr.get_affected_indexes(&schema, &changed);
        assert_eq!(affected.len(), 2);
    }

    #[test]
    fn test_primary_key_update() {
        let schema = TableSchema::with_primary_key(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
            ],
            vec!["id".to_string()],
        );

        let mut index_mgr = IndexManager::new(&schema);

        // Insert initial row
        let old_row = Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]);
        index_mgr.update_for_insert(&schema, &old_row, 0);

        // Update primary key
        let new_row = Row::new(vec![SqlValue::Integer(10), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]);
        index_mgr.update_for_update(&schema, &old_row, &new_row, 0);

        // Verify old key removed and new key added
        let pk_index = index_mgr.primary_key_index().unwrap();
        assert_eq!(pk_index.get(&vec![SqlValue::Integer(1)]), None, "Old key should be removed");
        assert_eq!(pk_index.get(&vec![SqlValue::Integer(10)]), Some(&0), "New key should be added");
    }
}
