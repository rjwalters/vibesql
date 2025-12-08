// ============================================================================
// Database Index Operations
// ============================================================================

use super::core::Database;
use super::operations::SpatialIndexMetadata;
use crate::{Row, StorageError};
use vibesql_ast::IndexColumn;

// ============================================================================
// DELETE Profiling Statistics (thread-local aggregates)
// ============================================================================

/// Aggregate statistics for DELETE profiling.
///
/// # Environment Variables
///
/// - `DELETE_PROFILE=1` - Enable timing collection and print summary on thread exit
/// - `DELETE_PROFILE_VERBOSE=1` - Also print per-delete breakdown to stderr
///
/// When `DELETE_PROFILE=1` is set, aggregate statistics are automatically printed
/// when the thread-local stats are dropped (typically at thread exit).
#[derive(Default)]
pub struct DeleteProfileStats {
    pub count: u64,
    pub total_ns: u128,
    pub pk_lookup_ns: u128,
    pub row_clone_ns: u128,
    pub wal_ns: u128,
    pub index_update_ns: u128,
    pub row_remove_ns: u128,
    pub cache_ns: u128,
}

impl DeleteProfileStats {
    /// Add timing data from a single delete operation
    pub fn record(&mut self, phase_times: &[u128; 6], total_ns: u128) {
        self.count += 1;
        self.total_ns += total_ns;
        self.pk_lookup_ns += phase_times[0];
        self.row_clone_ns += phase_times[1];
        self.wal_ns += phase_times[2];
        self.index_update_ns += phase_times[3];
        self.row_remove_ns += phase_times[4];
        self.cache_ns += phase_times[5];
    }

    /// Print a summary of the aggregate statistics
    pub fn print_summary(&self) {
        if self.count == 0 {
            return;
        }
        let total = self.total_ns;
        let avg_us = (total as f64 / self.count as f64) / 1000.0;
        eprintln!("\n=== DELETE PROFILE SUMMARY ({} deletes) ===", self.count);
        eprintln!("Average DELETE time: {:.1}µs", avg_us);
        eprintln!(
            "  pk_lookup:    {:>8.1}µs ({:>5.1}%)",
            (self.pk_lookup_ns as f64 / self.count as f64) / 1000.0,
            if total > 0 { self.pk_lookup_ns as f64 / total as f64 * 100.0 } else { 0.0 }
        );
        eprintln!(
            "  value_clone:  {:>8.1}µs ({:>5.1}%)",
            (self.row_clone_ns as f64 / self.count as f64) / 1000.0,
            if total > 0 { self.row_clone_ns as f64 / total as f64 * 100.0 } else { 0.0 }
        );
        eprintln!(
            "  wal:          {:>8.1}µs ({:>5.1}%)",
            (self.wal_ns as f64 / self.count as f64) / 1000.0,
            if total > 0 { self.wal_ns as f64 / total as f64 * 100.0 } else { 0.0 }
        );
        eprintln!(
            "  index_update: {:>8.1}µs ({:>5.1}%)",
            (self.index_update_ns as f64 / self.count as f64) / 1000.0,
            if total > 0 { self.index_update_ns as f64 / total as f64 * 100.0 } else { 0.0 }
        );
        eprintln!(
            "  row_remove:   {:>8.1}µs ({:>5.1}%)",
            (self.row_remove_ns as f64 / self.count as f64) / 1000.0,
            if total > 0 { self.row_remove_ns as f64 / total as f64 * 100.0 } else { 0.0 }
        );
        eprintln!(
            "  cache:        {:>8.1}µs ({:>5.1}%)",
            (self.cache_ns as f64 / self.count as f64) / 1000.0,
            if total > 0 { self.cache_ns as f64 / total as f64 * 100.0 } else { 0.0 }
        );
        eprintln!("==========================================\n");
    }
}

impl Drop for DeleteProfileStats {
    fn drop(&mut self) {
        // Print summary if DELETE_PROFILE=1 (auto-summary) or DELETE_PROFILE_SUMMARY=1 (explicit)
        if self.count > 0
            && (std::env::var("DELETE_PROFILE").is_ok()
                || std::env::var("DELETE_PROFILE_SUMMARY").is_ok())
        {
            self.print_summary();
        }
    }
}

thread_local! {
    /// Thread-local aggregate statistics for DELETE profiling
    pub static DELETE_PROFILE_STATS: std::cell::RefCell<DeleteProfileStats> =
        std::cell::RefCell::new(DeleteProfileStats::default());
}

/// Print the DELETE profile summary for the current thread.
/// Call this at the end of a benchmark to see aggregate statistics.
pub fn print_delete_profile_summary() {
    DELETE_PROFILE_STATS.with(|stats| {
        stats.borrow().print_summary();
    });
}

/// Reset the DELETE profile statistics for the current thread.
pub fn reset_delete_profile_stats() {
    DELETE_PROFILE_STATS.with(|stats| {
        *stats.borrow_mut() = DeleteProfileStats::default();
    });
}

impl Database {
    // ============================================================================
    // Index Management
    // ============================================================================

    /// Create an index
    pub fn create_index(
        &mut self,
        index_name: String,
        table_name: String,
        unique: bool,
        columns: Vec<IndexColumn>,
    ) -> Result<(), StorageError> {
        self.operations.create_index(
            &self.catalog,
            &self.tables,
            index_name,
            table_name,
            unique,
            columns,
        )
    }

    /// Check if an index exists
    pub fn index_exists(&self, index_name: &str) -> bool {
        self.operations.index_exists(index_name)
    }

    /// Get index metadata
    pub fn get_index(&self, index_name: &str) -> Option<&super::indexes::IndexMetadata> {
        self.operations.get_index(index_name)
    }

    /// Get index data
    pub fn get_index_data(&self, index_name: &str) -> Option<&super::indexes::IndexData> {
        self.operations.get_index_data(index_name)
    }

    /// Update user-defined indexes for update operation
    ///
    /// # Arguments
    /// * `table_name` - Name of the table being updated
    /// * `old_row` - Row data before the update
    /// * `new_row` - Row data after the update
    /// * `row_index` - Index of the row in the table
    /// * `changed_columns` - Optional set of column indices that were modified.
    ///   If provided, indexes that don't involve any changed columns will be skipped.
    pub fn update_indexes_for_update(
        &mut self,
        table_name: &str,
        old_row: &Row,
        new_row: &Row,
        row_index: usize,
        changed_columns: Option<&std::collections::HashSet<usize>>,
    ) {
        self.operations.update_indexes_for_update(
            &self.catalog,
            table_name,
            old_row,
            new_row,
            row_index,
            changed_columns,
        );
    }

    /// Update user-defined indexes for delete operation
    pub fn update_indexes_for_delete(&mut self, table_name: &str, row: &Row, row_index: usize) {
        self.operations.update_indexes_for_delete(&self.catalog, table_name, row, row_index);
    }

    /// Batch update user-defined indexes for delete operation
    ///
    /// This is significantly more efficient than calling `update_indexes_for_delete` in a loop
    /// because it pre-computes column indices once per index rather than once per row.
    pub fn batch_update_indexes_for_delete(
        &mut self,
        table_name: &str,
        rows_to_delete: &[(usize, &Row)],
    ) {
        self.operations.batch_update_indexes_for_delete(&self.catalog, table_name, rows_to_delete);
    }

    /// Rebuild user-defined indexes after bulk operations that change row indices
    pub fn rebuild_indexes(&mut self, table_name: &str) {
        self.operations.rebuild_indexes(&self.catalog, &self.tables, table_name);
    }

    /// Adjust user-defined indexes after row deletions
    ///
    /// This is more efficient than rebuild_indexes when only a few rows are deleted,
    /// as it adjusts row indices in place rather than rebuilding from scratch.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table whose indexes need adjustment
    /// * `deleted_indices` - Sorted list of deleted row indices (ascending order)
    pub fn adjust_indexes_after_delete(&mut self, table_name: &str, deleted_indices: &[usize]) {
        self.operations.adjust_indexes_after_delete(table_name, deleted_indices);
    }

    /// Drop an index
    pub fn drop_index(&mut self, index_name: &str) -> Result<(), StorageError> {
        self.operations.drop_index(index_name)
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<String> {
        self.operations.list_indexes()
    }

    /// List all indexes for a specific table
    pub fn list_indexes_for_table(&self, table_name: &str) -> Vec<String> {
        self.operations.list_indexes_for_table(table_name)
    }

    /// Check if a column has any user-defined index
    ///
    /// This is used to determine if updates to a column require index maintenance.
    /// Returns true if any user-defined index (B-tree or spatial) includes this column.
    #[inline]
    pub fn has_index_on_column(&self, table_name: &str, column_name: &str) -> bool {
        self.operations.has_index_on_column(table_name, column_name)
    }

    // ============================================================================
    // Spatial Index Methods
    // ============================================================================

    /// Create a spatial index
    pub fn create_spatial_index(
        &mut self,
        metadata: SpatialIndexMetadata,
        spatial_index: crate::index::SpatialIndex,
    ) -> Result<(), StorageError> {
        self.operations.create_spatial_index(metadata, spatial_index)
    }

    /// Create an IVFFlat index for approximate nearest neighbor search on vector columns
    ///
    /// This method creates an IVFFlat (Inverted File with Flat quantization) index
    /// for efficient approximate nearest neighbor search on vector data.
    ///
    /// # Arguments
    /// * `index_name` - Name for the new index
    /// * `table_name` - Name of the table containing the vector column
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
        self.operations.create_ivfflat_index(
            &self.catalog,
            &self.tables,
            index_name,
            table_name,
            column_name,
            col_idx,
            dimensions,
            lists,
            metric,
        )
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
        self.operations.search_ivfflat_index(index_name, query_vector, k)
    }

    /// Get all IVFFlat indexes for a specific table
    pub fn get_ivfflat_indexes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<(&super::indexes::IndexMetadata, &super::indexes::ivfflat::IVFFlatIndex)> {
        self.operations.get_ivfflat_indexes_for_table(table_name)
    }

    /// Set the number of probes for an IVFFlat index
    pub fn set_ivfflat_probes(
        &mut self,
        index_name: &str,
        probes: usize,
    ) -> Result<(), StorageError> {
        self.operations.set_ivfflat_probes(index_name, probes)
    }

    // ============================================================================
    // HNSW Index Methods
    // ============================================================================

    /// Create an HNSW index for approximate nearest neighbor search on vector columns
    ///
    /// This method creates an HNSW (Hierarchical Navigable Small World) index
    /// for efficient approximate nearest neighbor search on vector data.
    ///
    /// # Arguments
    /// * `index_name` - Name for the new index
    /// * `table_name` - Name of the table containing the vector column
    /// * `column_name` - Name of the vector column to index
    /// * `col_idx` - Column index in the table schema
    /// * `dimensions` - Number of dimensions in the vectors
    /// * `m` - Maximum number of connections per node (default 16)
    /// * `ef_construction` - Size of dynamic candidate list during construction (default 64)
    /// * `metric` - Distance metric to use (L2, Cosine, InnerProduct)
    #[allow(clippy::too_many_arguments)]
    pub fn create_hnsw_index(
        &mut self,
        index_name: String,
        table_name: String,
        column_name: String,
        col_idx: usize,
        dimensions: usize,
        m: u32,
        ef_construction: u32,
        metric: vibesql_ast::VectorDistanceMetric,
    ) -> Result<(), StorageError> {
        self.operations.create_hnsw_index(
            &self.catalog,
            &self.tables,
            index_name,
            table_name,
            column_name,
            col_idx,
            dimensions,
            m,
            ef_construction,
            metric,
        )
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
        self.operations.search_hnsw_index(index_name, query_vector, k)
    }

    /// Get all HNSW indexes for a specific table
    pub fn get_hnsw_indexes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<(&super::indexes::IndexMetadata, &super::indexes::hnsw::HnswIndex)> {
        self.operations.get_hnsw_indexes_for_table(table_name)
    }

    /// Set the ef_search parameter for an HNSW index
    pub fn set_hnsw_ef_search(
        &mut self,
        index_name: &str,
        ef_search: usize,
    ) -> Result<(), StorageError> {
        self.operations.set_hnsw_ef_search(index_name, ef_search)
    }

    /// Check if a spatial index exists
    pub fn spatial_index_exists(&self, index_name: &str) -> bool {
        self.operations.spatial_index_exists(index_name)
    }

    /// Get spatial index metadata
    pub fn get_spatial_index_metadata(&self, index_name: &str) -> Option<&SpatialIndexMetadata> {
        self.operations.get_spatial_index_metadata(index_name)
    }

    /// Get spatial index (immutable)
    pub fn get_spatial_index(&self, index_name: &str) -> Option<&crate::index::SpatialIndex> {
        self.operations.get_spatial_index(index_name)
    }

    /// Get spatial index (mutable)
    pub fn get_spatial_index_mut(
        &mut self,
        index_name: &str,
    ) -> Option<&mut crate::index::SpatialIndex> {
        self.operations.get_spatial_index_mut(index_name)
    }

    /// Get all spatial indexes for a specific table
    pub fn get_spatial_indexes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<(&SpatialIndexMetadata, &crate::index::SpatialIndex)> {
        self.operations.get_spatial_indexes_for_table(table_name)
    }

    /// Get all spatial indexes for a specific table (mutable)
    pub fn get_spatial_indexes_for_table_mut(
        &mut self,
        table_name: &str,
    ) -> Vec<(&SpatialIndexMetadata, &mut crate::index::SpatialIndex)> {
        self.operations.get_spatial_indexes_for_table_mut(table_name)
    }

    /// Drop a spatial index
    pub fn drop_spatial_index(&mut self, index_name: &str) -> Result<(), StorageError> {
        self.operations.drop_spatial_index(index_name)
    }

    /// Drop all spatial indexes associated with a table (CASCADE behavior)
    pub fn drop_spatial_indexes_for_table(&mut self, table_name: &str) -> Vec<String> {
        self.operations.drop_spatial_indexes_for_table(table_name)
    }

    /// List all spatial indexes
    pub fn list_spatial_indexes(&self) -> Vec<String> {
        self.operations.list_spatial_indexes()
    }

    // ============================================================================
    // Direct Index Lookup API (High-Performance OLTP)
    // ============================================================================

    /// Look up rows by index name and key values - bypasses SQL parsing for maximum performance
    ///
    /// This method provides direct B+ tree index lookups, completely bypassing SQL parsing
    /// and the query execution pipeline. Use this for performance-critical OLTP workloads
    /// where you know the exact index and key values.
    ///
    /// # Arguments
    /// * `index_name` - Name of the index (as created with CREATE INDEX)
    /// * `key_values` - Key values to look up (must match index column order)
    ///
    /// # Returns
    /// * `Ok(Some(Vec<&Row>))` - The rows matching the key
    /// * `Ok(None)` - No rows match the key
    /// * `Err(StorageError)` - Index not found or other error
    ///
    /// # Performance
    /// This is ~100-300x faster than executing a SQL SELECT query because it:
    /// - Skips SQL parsing (~300µs saved)
    /// - Skips query planning and optimization
    /// - Uses direct B+ tree lookup on the index
    ///
    /// # Example
    /// ```text
    /// // Single-column index lookup
    /// let rows = db.lookup_by_index("idx_users_pk", &[SqlValue::Integer(42)])?;
    ///
    /// // Composite key lookup
    /// let rows = db.lookup_by_index("idx_orders_pk", &[
    ///     SqlValue::Integer(warehouse_id),
    ///     SqlValue::Integer(district_id),
    ///     SqlValue::Integer(order_id),
    /// ])?;
    /// ```
    pub fn lookup_by_index(
        &self,
        index_name: &str,
        key_values: &[vibesql_types::SqlValue],
    ) -> Result<Option<Vec<&Row>>, StorageError> {
        // Get index metadata to find the table
        let metadata = self
            .get_index(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;
        let table_name = metadata.table_name.clone();

        // Get the index data
        let index_data = self
            .get_index_data(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        // Perform the lookup
        let row_indices = match index_data.get(key_values) {
            Some(indices) => indices,
            None => return Ok(None),
        };

        // Get the table
        let table = self
            .get_table(&table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

        // Collect the rows (use get_row() to skip deleted rows)
        let mut result = Vec::with_capacity(row_indices.len());
        for &idx in &row_indices {
            // Use get_row() which returns None for deleted rows
            if let Some(row) = table.get_row(idx) {
                result.push(row);
            }
        }

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }

    /// Look up the first row by index - optimized for unique indexes
    ///
    /// This is a convenience method for unique indexes where you expect exactly one row.
    /// Returns only the first matching row.
    ///
    /// # Arguments
    /// * `index_name` - Name of the index
    /// * `key_values` - Key values to look up
    ///
    /// # Returns
    /// * `Ok(Some(&Row))` - The first matching row
    /// * `Ok(None)` - No row matches the key
    /// * `Err(StorageError)` - Index not found or other error
    pub fn lookup_one_by_index(
        &self,
        index_name: &str,
        key_values: &[vibesql_types::SqlValue],
    ) -> Result<Option<&Row>, StorageError> {
        // Get index metadata to find the table
        let metadata = self
            .get_index(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;
        let table_name = metadata.table_name.clone();

        // Get the index data
        let index_data = self
            .get_index_data(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        // Perform the lookup
        let row_indices = match index_data.get(key_values) {
            Some(indices) => indices,
            None => return Ok(None),
        };

        // Get the first row index
        let first_idx = match row_indices.first() {
            Some(&idx) => idx,
            None => return Ok(None),
        };

        // Get the table and return the row using O(1) direct access
        let table = self
            .get_table(&table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

        Ok(table.get_row(first_idx))
    }

    /// Batch lookup by index - look up multiple keys in a single call
    ///
    /// This method is optimized for batch point lookups where you need to retrieve
    /// multiple rows by their index keys. It's more efficient than calling
    /// `lookup_by_index` in a loop.
    ///
    /// # Arguments
    /// * `index_name` - Name of the index
    /// * `keys` - List of key value tuples to look up
    ///
    /// # Returns
    /// * `Ok(Vec<Option<Vec<&Row>>>)` - For each key, the matching rows (or None if not found)
    /// * `Err(StorageError)` - Index not found or other error
    ///
    /// # Example
    /// ```text
    /// // Batch lookup multiple items
    /// let results = db.lookup_by_index_batch("idx_items_pk", &[
    ///     vec![SqlValue::Integer(1)],
    ///     vec![SqlValue::Integer(2)],
    ///     vec![SqlValue::Integer(3)],
    /// ])?;
    ///
    /// for (key_idx, rows) in results.iter().enumerate() {
    ///     if let Some(rows) = rows {
    ///         println!("Key {} matched {} rows", key_idx, rows.len());
    ///     }
    /// }
    /// ```
    pub fn lookup_by_index_batch<'a>(
        &'a self,
        index_name: &str,
        keys: &[Vec<vibesql_types::SqlValue>],
    ) -> Result<Vec<Option<Vec<&'a Row>>>, StorageError> {
        // Get index metadata to find the table
        let metadata = self
            .get_index(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;
        let table_name = metadata.table_name.clone();

        // Get the index data
        let index_data = self
            .get_index_data(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        // Get the table once for O(1) row access
        let table = self
            .get_table(&table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

        // Look up each key using direct row access
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let row_indices = index_data.get(key);
            match row_indices {
                Some(indices) if !indices.is_empty() => {
                    let matched_rows: Vec<_> =
                        indices.iter().filter_map(|&idx| table.get_row(idx)).collect();
                    if matched_rows.is_empty() {
                        results.push(None);
                    } else {
                        results.push(Some(matched_rows));
                    }
                }
                _ => results.push(None),
            }
        }

        Ok(results)
    }

    /// Batch lookup returning first row only - optimized for unique indexes
    ///
    /// Like `lookup_by_index_batch` but returns only the first matching row for each key.
    /// More efficient when you know the index is unique.
    ///
    /// # Arguments
    /// * `index_name` - Name of the index
    /// * `keys` - List of key value tuples to look up
    ///
    /// # Returns
    /// * `Ok(Vec<Option<&Row>>)` - For each key, the first matching row (or None)
    pub fn lookup_one_by_index_batch<'a>(
        &'a self,
        index_name: &str,
        keys: &[Vec<vibesql_types::SqlValue>],
    ) -> Result<Vec<Option<&'a Row>>, StorageError> {
        // Get index metadata to find the table
        let metadata = self
            .get_index(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;
        let table_name = metadata.table_name.clone();

        // Get the index data
        let index_data = self
            .get_index_data(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        // Get the table once for O(1) row access
        let table = self
            .get_table(&table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

        // Look up each key using direct row access
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let row_indices = index_data.get(key);
            match row_indices {
                Some(indices) if !indices.is_empty() => {
                    results.push(table.get_row(indices[0]));
                }
                _ => results.push(None),
            }
        }

        Ok(results)
    }

    // ============================================================================
    // Prefix Index Lookup API (Multi-column indexes)
    // ============================================================================

    /// Look up rows by index using prefix matching - for multi-column indexes
    ///
    /// This method performs prefix matching on multi-column indexes. For example,
    /// with an index on (a, b, c), you can look up all rows where (a, b) match
    /// a specific value, regardless of c.
    ///
    /// # Arguments
    /// * `index_name` - Name of the index (as created with CREATE INDEX)
    /// * `prefix` - Prefix key values to match (must be a prefix of index columns)
    ///
    /// # Returns
    /// * `Ok(Vec<&Row>)` - The rows matching the prefix (empty if none found)
    /// * `Err(StorageError)` - Index not found or other error
    ///
    /// # Performance
    /// Uses efficient B+ tree range scan: O(log n + k) where n is total keys, k is matches.
    ///
    /// # Example
    /// ```text
    /// // Index on (warehouse_id, district_id, order_id) - 3 columns
    /// // Find all orders for warehouse 1, district 5 (2-column prefix)
    /// let rows = db.lookup_by_index_prefix("idx_orders_pk", &[
    ///     SqlValue::Integer(1),  // warehouse_id
    ///     SqlValue::Integer(5),  // district_id
    /// ])?;
    /// ```
    pub fn lookup_by_index_prefix(
        &self,
        index_name: &str,
        prefix: &[vibesql_types::SqlValue],
    ) -> Result<Vec<&Row>, StorageError> {
        // Get index metadata to find the table
        let metadata = self
            .get_index(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;
        let table_name = metadata.table_name.clone();

        // Get the index data
        let index_data = self
            .get_index_data(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        // Perform the prefix scan
        let row_indices = index_data.prefix_scan(prefix);
        if row_indices.is_empty() {
            return Ok(vec![]);
        }

        // Get the table
        let table = self
            .get_table(&table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

        // Collect the rows using O(1) direct access
        let rows: Vec<_> = row_indices.iter().filter_map(|&idx| table.get_row(idx)).collect();

        Ok(rows)
    }

    /// Batch prefix lookup - look up multiple prefixes in a single call
    ///
    /// This method is optimized for batch prefix lookups on multi-column indexes.
    /// For each prefix, returns all rows where the key prefix matches.
    ///
    /// # Arguments
    /// * `index_name` - Name of the index
    /// * `prefixes` - List of prefix key tuples to look up
    ///
    /// # Returns
    /// * `Ok(Vec<Vec<&Row>>)` - For each prefix, the matching rows (empty vec if none)
    /// * `Err(StorageError)` - Index not found or other error
    ///
    /// # Example
    /// ```text
    /// // Index on (w_id, d_id, o_id) - find new orders for all 10 districts
    /// let prefixes: Vec<Vec<SqlValue>> = (1..=10)
    ///     .map(|d| vec![SqlValue::Integer(w_id), SqlValue::Integer(d)])
    ///     .collect();
    /// let results = db.lookup_by_index_prefix_batch("idx_new_order_pk", &prefixes)?;
    /// // results[0] = rows for district 1, results[1] = rows for district 2, etc.
    /// ```
    pub fn lookup_by_index_prefix_batch<'a>(
        &'a self,
        index_name: &str,
        prefixes: &[Vec<vibesql_types::SqlValue>],
    ) -> Result<Vec<Vec<&'a Row>>, StorageError> {
        // Get index metadata to find the table
        let metadata = self
            .get_index(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;
        let table_name = metadata.table_name.clone();

        // Get the index data
        let index_data = self
            .get_index_data(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        // Get the table once for O(1) row access
        let table = self
            .get_table(&table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

        // Look up each prefix and collect results
        let mut results = Vec::with_capacity(prefixes.len());
        for prefix in prefixes {
            let row_indices = index_data.prefix_scan(prefix);
            let matched_rows: Vec<_> =
                row_indices.iter().filter_map(|&idx| table.get_row(idx)).collect();
            results.push(matched_rows);
        }

        Ok(results)
    }

    // ============================================================================
    // Fast Delete API (High-Performance OLTP)
    // ============================================================================

    /// Delete a single row by PK value - fast path that skips unnecessary overhead
    ///
    /// This method provides a highly optimized DELETE path for single-row PK deletes.
    /// It bypasses the full DELETE executor overhead when:
    /// - There are no triggers on the table
    /// - There are no foreign key constraints referencing this table
    /// - The WHERE clause is a simple PK equality (`id = ?`)
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `pk_values` - Primary key values to match
    ///
    /// # Returns
    /// * `Ok(true)` - Row was deleted
    /// * `Ok(false)` - No row found with this PK
    /// * `Err(StorageError)` - Table not found or other error
    ///
    /// # Performance
    /// This is ~2-3x faster than the full DELETE executor because it:
    /// - Uses direct PK index lookup (O(1))
    /// - Avoids cloning row data
    /// - Skips ExpressionEvaluator creation
    /// - Performs minimal index maintenance
    ///
    /// # Profiling
    /// Set environment variables to enable profiling:
    /// - `DELETE_PROFILE=1` - Enable timing collection and auto-print summary on thread exit
    /// - `DELETE_PROFILE_VERBOSE=1` - Also print per-delete breakdown to stderr
    ///
    /// Use `print_delete_profile_summary()` to manually print aggregate stats.
    /// Use `reset_delete_profile_stats()` to reset the stats before a benchmark.
    ///
    /// # Safety
    /// Caller must ensure:
    /// - No triggers exist on this table for DELETE
    /// - No foreign key constraints reference this table
    ///
    /// Note: WAL logging is handled internally by this method.
    ///
    /// # Example
    /// ```text
    /// // Fast delete by PK
    /// let deleted = db.delete_by_pk_fast("users", &[SqlValue::Integer(42)])?;
    /// if deleted {
    ///     println!("User 42 deleted");
    /// }
    /// ```
    pub fn delete_by_pk_fast(
        &mut self,
        table_name: &str,
        pk_values: &[vibesql_types::SqlValue],
    ) -> Result<bool, StorageError> {
        use std::time::Instant;

        // Check if profiling is enabled
        let profile = std::env::var("DELETE_PROFILE").is_ok();
        let start = if profile { Some(Instant::now()) } else { None };
        let mut phase_times: [u128; 6] = [0; 6]; // pk_lookup, value_clone, wal, index_update, row_remove, cache

        // First, find the row index and clone only the values (not the full Row struct)
        // This avoids double-cloning: previously we cloned the Row, then cloned its values for WAL.
        // Now we clone values only once and use a reference for index updates.
        let (row_index, values) = {
            let phase_start = start.map(|_| Instant::now());
            let table = self
                .get_table(table_name)
                .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;

            let row_index = match table.primary_key_index() {
                Some(pk_index) => match pk_index.get(pk_values) {
                    Some(&idx) => idx,
                    None => return Ok(false), // Row not found
                },
                None => return Err(StorageError::Other("Table has no primary key".to_string())),
            };
            if let Some(ps) = phase_start {
                phase_times[0] = ps.elapsed().as_nanos(); // pk_lookup
            }

            // Get the row values - clone once for both WAL and index updates
            let phase_start = start.map(|_| Instant::now());
            let values = match table.get_row(row_index) {
                Some(r) => r.values.clone(),
                None => return Ok(false), // Row already deleted
            };
            if let Some(ps) = phase_start {
                phase_times[1] = ps.elapsed().as_nanos(); // value_clone
            }

            (row_index, values)
        };

        // Update user-defined indexes first (using reference to values)
        // This must happen before we move ownership of values to WAL
        let phase_start = start.map(|_| Instant::now());
        self.operations
            .update_indexes_for_delete_with_values(&self.catalog, table_name, &values, row_index);
        if let Some(ps) = phase_start {
            phase_times[3] = ps.elapsed().as_nanos(); // index_update
        }

        // Emit WAL entry before deleting (needed for crash recovery)
        // Only emit if persistence is enabled to avoid unnecessary work
        // Move ownership of values to avoid a second clone
        let phase_start = start.map(|_| Instant::now());
        if self.persistence_enabled() {
            self.emit_wal_delete(table_name, row_index as u64, values.to_vec());
        }
        if let Some(ps) = phase_start {
            phase_times[2] = ps.elapsed().as_nanos(); // wal
        }

        // Now delete the row (this updates the internal PK hash index)
        let phase_start = start.map(|_| Instant::now());
        let table_mut = self
            .get_table_mut(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;

        let delete_result = table_mut.delete_by_indices(&[row_index]);

        // If compaction occurred, rebuild user-defined indexes since row indices changed
        if delete_result.compacted {
            self.rebuild_indexes(table_name);
        }
        if let Some(ps) = phase_start {
            phase_times[4] = ps.elapsed().as_nanos(); // row_remove
        }

        // Phase 5: Invalidate columnar cache
        let phase_start = start.map(|_| Instant::now());
        if delete_result.deleted_count > 0 {
            self.invalidate_columnar_cache(table_name);
        }
        if let Some(ps) = phase_start {
            phase_times[5] = ps.elapsed().as_nanos(); // cache
        }

        // Record and optionally print profile summary
        if let Some(s) = start {
            let total = s.elapsed().as_nanos();

            // Record to thread-local aggregate stats
            DELETE_PROFILE_STATS.with(|stats| {
                stats.borrow_mut().record(&phase_times, total);
            });

            // Print per-delete output only if DELETE_PROFILE_VERBOSE is set
            if std::env::var("DELETE_PROFILE_VERBOSE").is_ok() {
                let total_us = total as f64 / 1000.0;
                eprintln!(
                    "DELETE_PROFILE: total={:.1}µs | pk_lookup={:.1}µs ({:.0}%) | value_clone={:.1}µs ({:.0}%) | wal={:.1}µs ({:.0}%) | index_update={:.1}µs ({:.0}%) | row_remove={:.1}µs ({:.0}%) | cache={:.1}µs ({:.0}%)",
                    total_us,
                    phase_times[0] as f64 / 1000.0,
                    if total > 0 { phase_times[0] as f64 / total as f64 * 100.0 } else { 0.0 },
                    phase_times[1] as f64 / 1000.0,
                    if total > 0 { phase_times[1] as f64 / total as f64 * 100.0 } else { 0.0 },
                    phase_times[2] as f64 / 1000.0,
                    if total > 0 { phase_times[2] as f64 / total as f64 * 100.0 } else { 0.0 },
                    phase_times[3] as f64 / 1000.0,
                    if total > 0 { phase_times[3] as f64 / total as f64 * 100.0 } else { 0.0 },
                    phase_times[4] as f64 / 1000.0,
                    if total > 0 { phase_times[4] as f64 / total as f64 * 100.0 } else { 0.0 },
                    phase_times[5] as f64 / 1000.0,
                    if total > 0 { phase_times[5] as f64 / total as f64 * 100.0 } else { 0.0 },
                );
            }
        }

        Ok(delete_result.deleted_count > 0)
    }

    // ============================================================================
    // Table Index Info for DML Cost Estimation
    // ============================================================================

    /// Get table index information for DML cost estimation
    ///
    /// This method collects all the metadata needed by `CostEstimator::estimate_insert()`,
    /// `estimate_update()`, and `estimate_delete()` to compute accurate DML operation costs.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to get index info for
    ///
    /// # Returns
    /// * `Some(TableIndexInfo)` - Index information if table exists
    /// * `None` - If table doesn't exist
    ///
    /// # Example
    /// ```text
    /// let info = db.get_table_index_info("users")?;
    /// let insert_cost = cost_estimator.estimate_insert(&info);
    /// ```
    pub fn get_table_index_info(
        &self,
        table_name: &str,
    ) -> Option<crate::statistics::TableIndexInfo> {
        // Get the table
        let table = self.get_table(table_name)?;

        // Count hash indexes: 1 for PK (if exists) + 1 per unique constraint
        let has_primary_key = table.schema.primary_key.is_some();
        let unique_constraint_count = table.schema.unique_constraints.len();
        let hash_index_count =
            if has_primary_key { 1 } else { 0 } + unique_constraint_count;

        // Count B-tree indexes (user-defined indexes managed at Database level)
        let btree_index_count = self.list_indexes_for_table(table_name).len();

        // Calculate deleted ratio
        let total_rows = table.physical_row_count();
        let deleted_ratio = if total_rows > 0 {
            table.deleted_count() as f64 / total_rows as f64
        } else {
            0.0
        };

        // Check if table uses native columnar storage
        let is_native_columnar = table.is_native_columnar();

        // Get average row size: prefer actual statistics over schema-based heuristics
        //
        // When statistics are available (from ANALYZE), use the actual avg_row_bytes
        // which accounts for real string fill ratios, NULL prevalence, and actual
        // BLOB sizes. This provides more accurate WAL cost estimation.
        //
        // Fall back to schema-based estimation when no statistics are available.
        // See issue #3980 for details.
        let avg_row_size = table
            .get_statistics()
            .and_then(|stats| stats.avg_row_bytes)
            .map(|bytes| bytes as usize)
            .unwrap_or_else(|| {
                // Fall back to schema-based estimation
                let column_types: Vec<_> =
                    table.schema.columns.iter().map(|col| col.data_type.clone()).collect();
                crate::statistics::estimate_row_size(&column_types)
            });

        Some(crate::statistics::TableIndexInfo::new(
            hash_index_count,
            btree_index_count,
            is_native_columnar,
            deleted_ratio,
            avg_row_size,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    #[test]
    fn test_get_table_index_info_basic() {
        let mut db = Database::new();

        // Create a table with primary key and one unique constraint
        let schema = TableSchema::with_all_constraints(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("email".to_string(), DataType::Varchar { max_length: Some(100) }, false),
                ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, true),
            ],
            Some(vec!["id".to_string()]),
            vec![vec!["email".to_string()]],
        );
        db.create_table(schema).unwrap();

        // Get table index info
        let info = db.get_table_index_info("users").unwrap();

        // Should have 2 hash indexes: 1 PK + 1 unique constraint
        assert_eq!(info.hash_index_count, 2);
        // No B-tree indexes yet
        assert_eq!(info.btree_index_count, 0);
        // Not native columnar
        assert!(!info.is_native_columnar);
        // No deleted rows
        assert_eq!(info.deleted_ratio, 0.0);
    }

    #[test]
    fn test_get_table_index_info_with_btree_index() {
        use vibesql_ast::IndexColumn;

        let mut db = Database::new();

        // Create a table with primary key
        let schema = TableSchema::with_primary_key(
            "products".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, false),
                ColumnSchema::new("price".to_string(), DataType::Decimal { precision: 10, scale: 2 }, false),
            ],
            vec!["id".to_string()],
        );
        db.create_table(schema).unwrap();

        // Create a B-tree index on price
        db.create_index(
            "idx_products_price".to_string(),
            "products".to_string(),
            false,
            vec![IndexColumn {
                column_name: "price".to_string(),
                direction: vibesql_ast::OrderDirection::Asc,
                prefix_length: None,
            }],
        ).unwrap();

        // Get table index info
        let info = db.get_table_index_info("products").unwrap();

        // Should have 1 hash index (PK)
        assert_eq!(info.hash_index_count, 1);
        // Should have 1 B-tree index
        assert_eq!(info.btree_index_count, 1);
    }

    #[test]
    fn test_get_table_index_info_with_deleted_rows() {
        let mut db = Database::new();

        // Create a table with primary key
        let schema = TableSchema::with_primary_key(
            "items".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, false),
            ],
            vec!["id".to_string()],
        );
        db.create_table(schema).unwrap();

        // Insert 10 rows
        for i in 0..10 {
            let row = Row::new(vec![
                SqlValue::Integer(i),
                SqlValue::Varchar(format!("Item {}", i).into()),
            ]);
            db.insert_row("items", row).unwrap();
        }

        // Get initial info - no deleted rows
        let info = db.get_table_index_info("items").unwrap();
        assert_eq!(info.deleted_ratio, 0.0);

        // Delete 3 rows (30% deletion)
        db.delete_by_pk_fast("items", &[SqlValue::Integer(0)]).unwrap();
        db.delete_by_pk_fast("items", &[SqlValue::Integer(1)]).unwrap();
        db.delete_by_pk_fast("items", &[SqlValue::Integer(2)]).unwrap();

        // Get updated info - should show deleted ratio
        let info = db.get_table_index_info("items").unwrap();
        // 3 deleted out of 10 = 0.3
        assert!((info.deleted_ratio - 0.3).abs() < 0.01);
    }

    #[test]
    fn test_get_table_index_info_nonexistent_table() {
        let db = Database::new();

        // Should return None for nonexistent table
        let info = db.get_table_index_info("nonexistent");
        assert!(info.is_none());
    }

    #[test]
    fn test_get_table_index_info_no_primary_key() {
        let mut db = Database::new();

        // Create a table without primary key
        let schema = TableSchema::new(
            "logs".to_string(),
            vec![
                ColumnSchema::new("message".to_string(), DataType::Varchar { max_length: Some(500) }, false),
                ColumnSchema::new("level".to_string(), DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();

        // Get table index info
        let info = db.get_table_index_info("logs").unwrap();

        // Should have 0 hash indexes (no PK, no unique constraints)
        assert_eq!(info.hash_index_count, 0);
        // No B-tree indexes
        assert_eq!(info.btree_index_count, 0);
    }

    #[test]
    fn test_get_table_index_info_multiple_unique_constraints() {
        let mut db = Database::new();

        // Create a table with PK and multiple unique constraints
        let schema = TableSchema::with_all_constraints(
            "accounts".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("email".to_string(), DataType::Varchar { max_length: Some(100) }, false),
                ColumnSchema::new("username".to_string(), DataType::Varchar { max_length: Some(50) }, false),
                ColumnSchema::new("phone".to_string(), DataType::Varchar { max_length: Some(20) }, true),
            ],
            Some(vec!["id".to_string()]),
            vec![
                vec!["email".to_string()],
                vec!["username".to_string()],
            ],
        );
        db.create_table(schema).unwrap();

        // Get table index info
        let info = db.get_table_index_info("accounts").unwrap();

        // Should have 3 hash indexes: 1 PK + 2 unique constraints
        assert_eq!(info.hash_index_count, 3);
    }

    // ============================================================================
    // avg_row_size Statistics Tests (Issue #3980)
    // ============================================================================

    #[test]
    fn test_get_table_index_info_uses_schema_estimate_without_stats() {
        let mut db = Database::new();

        // Create a table with VARCHAR columns
        let schema = TableSchema::with_primary_key(
            "items".to_string(),
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
        db.create_table(schema).unwrap();

        // Without ANALYZE, should use schema-based estimation
        let info = db.get_table_index_info("items").unwrap();

        // avg_row_size should be computed from schema heuristics
        // INTEGER (4) + VARCHAR(100) estimate (32) + overhead (8) = 44, min 64
        assert!(
            info.avg_row_size >= 64,
            "Schema-based avg_row_size should be at least base size: {}",
            info.avg_row_size
        );
    }

    #[test]
    fn test_get_table_index_info_prefers_actual_statistics() {
        let mut db = Database::new();

        // Create a table with VARCHAR columns
        let schema = TableSchema::with_primary_key(
            "items".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "description".to_string(),
                    DataType::Varchar { max_length: Some(1000) },
                    false,
                ),
            ],
            vec!["id".to_string()],
        );
        db.create_table(schema).unwrap();

        // Insert rows with LONG strings (filling the VARCHAR)
        for i in 0..10 {
            let long_description = "x".repeat(800); // Much longer than schema heuristic
            let row = Row::new(vec![
                SqlValue::Integer(i),
                SqlValue::Varchar(long_description.into()),
            ]);
            db.insert_row("items", row).unwrap();
        }

        // Get info WITHOUT statistics - uses schema heuristic
        let info_without_stats = db.get_table_index_info("items").unwrap();
        let schema_estimate = info_without_stats.avg_row_size;

        // Run ANALYZE to compute actual statistics
        db.get_table_mut("items").unwrap().analyze();

        // Get info WITH statistics - should prefer actual avg_row_bytes
        let info_with_stats = db.get_table_index_info("items").unwrap();
        let actual_estimate = info_with_stats.avg_row_size;

        // Actual statistics should show larger row size due to long strings
        // Schema heuristic: VARCHAR(1000) → min(500, 32) = 32 bytes
        // Actual data: 800 bytes per string
        assert!(
            actual_estimate > schema_estimate,
            "Actual statistics ({}) should show larger row size than schema heuristic ({}) for long strings",
            actual_estimate,
            schema_estimate
        );
    }

    #[test]
    fn test_get_table_index_info_statistics_vs_schema_short_strings() {
        let mut db = Database::new();

        // Create a table with VARCHAR columns
        let schema = TableSchema::with_primary_key(
            "items".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "code".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
            ],
            vec!["id".to_string()],
        );
        db.create_table(schema).unwrap();

        // Insert rows with SHORT strings (much shorter than heuristic)
        for i in 0..10 {
            let short_code = format!("A{}", i); // 2-3 chars, much shorter than 32-byte heuristic
            let row = Row::new(vec![
                SqlValue::Integer(i),
                SqlValue::Varchar(short_code.into()),
            ]);
            db.insert_row("items", row).unwrap();
        }

        // Run ANALYZE to compute actual statistics
        db.get_table_mut("items").unwrap().analyze();

        // Get info WITH statistics
        let info = db.get_table_index_info("items").unwrap();

        // Should have valid avg_row_size (from actual statistics)
        assert!(
            info.avg_row_size > 0,
            "avg_row_size should be positive: {}",
            info.avg_row_size
        );
    }
}
