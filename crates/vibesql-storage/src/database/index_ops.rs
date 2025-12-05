// ============================================================================
// Database Index Operations
// ============================================================================

use super::core::Database;
use super::operations::SpatialIndexMetadata;
use crate::{Row, StorageError};
use vibesql_ast::IndexColumn;

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
    pub fn update_indexes_for_update(
        &mut self,
        table_name: &str,
        old_row: &Row,
        new_row: &Row,
        row_index: usize,
    ) {
        self.operations.update_indexes_for_update(
            &self.catalog,
            table_name,
            old_row,
            new_row,
            row_index,
        );
    }

    /// Update user-defined indexes for delete operation
    pub fn update_indexes_for_delete(&mut self, table_name: &str, row: &Row, row_index: usize) {
        self.operations.update_indexes_for_delete(&self.catalog, table_name, row, row_index);
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
    /// ```rust,ignore
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

        // Collect the rows
        let rows = table.scan();
        let mut result = Vec::with_capacity(row_indices.len());
        for &idx in &row_indices {
            if idx < rows.len() {
                result.push(&rows[idx]);
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
    /// ```rust,ignore
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
    /// ```rust,ignore
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
    /// ```rust,ignore
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
}
