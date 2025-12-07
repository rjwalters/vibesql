// ============================================================================
// Table and Index Operations
// ============================================================================

use super::indexes::IndexManager;
use crate::index::{extract_mbr_from_sql_value, SpatialIndex};
use crate::progress::ProgressTracker;
use crate::{Row, StorageError, Table};
use std::collections::HashMap;
use vibesql_ast::IndexColumn;

/// Metadata for a spatial index
#[derive(Debug, Clone)]
pub struct SpatialIndexMetadata {
    pub index_name: String,
    pub table_name: String,
    pub column_name: String,
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Manages table and index operations
#[derive(Debug, Clone)]
pub struct Operations {
    /// User-defined index manager (B-tree indexes)
    index_manager: IndexManager,
    /// Spatial indexes (R-tree) - stored separately from B-tree indexes
    /// Key: normalized index name (uppercase)
    /// Value: (metadata, spatial index)
    spatial_indexes: HashMap<String, (SpatialIndexMetadata, SpatialIndex)>,
}

impl Operations {
    /// Create a new operations manager
    pub fn new() -> Self {
        Operations { index_manager: IndexManager::new(), spatial_indexes: HashMap::new() }
    }

    /// Set the database path for index storage
    pub fn set_database_path(&mut self, path: std::path::PathBuf) {
        self.index_manager.set_database_path(path);
    }

    /// Set the database configuration (memory budgets, spill policy)
    pub fn set_config(&mut self, config: super::DatabaseConfig) {
        self.index_manager.set_config(config);
    }

    /// Initialize OPFS storage asynchronously (WASM only)
    ///
    /// This replaces the temporary in-memory storage with persistent OPFS storage.
    /// Must be called from an async context.
    #[cfg(target_arch = "wasm32")]
    pub async fn init_opfs_async(&mut self) -> Result<(), crate::StorageError> {
        self.index_manager.init_opfs_async().await
    }

    // ============================================================================
    // Table Operations
    // ============================================================================

    /// Create a table in the catalog and storage
    pub fn create_table(
        &mut self,
        catalog: &mut vibesql_catalog::Catalog,
        schema: vibesql_catalog::TableSchema,
    ) -> Result<(), StorageError> {
        let _table_name = schema.name.clone();

        // Add to catalog
        catalog
            .create_table(schema.clone())
            .map_err(|e| StorageError::CatalogError(e.to_string()))?;

        Ok(())
    }

    /// Drop a table from the catalog
    pub fn drop_table(
        &mut self,
        catalog: &mut vibesql_catalog::Catalog,
        tables: &mut HashMap<String, Table>,
        name: &str,
    ) -> Result<(), StorageError> {
        // Normalize table name for lookup (matches catalog normalization)
        let normalized_name = if catalog.is_case_sensitive_identifiers() {
            name.to_string()
        } else {
            name.to_uppercase()
        };

        // Get qualified table name for index cleanup
        let qualified_name = if normalized_name.contains('.') {
            normalized_name.clone()
        } else {
            let current_schema = catalog.get_current_schema();
            format!("{}.{}", current_schema, normalized_name)
        };

        // Drop associated indexes BEFORE dropping table (CASCADE behavior)
        self.index_manager.drop_indexes_for_table(&qualified_name);

        // Drop associated spatial indexes too
        self.drop_spatial_indexes_for_table(&qualified_name);

        // Remove from catalog
        catalog.drop_table(name).map_err(|e| StorageError::CatalogError(e.to_string()))?;

        // Remove table data - try normalized name first, then try with schema prefix
        if tables.remove(&normalized_name).is_none() {
            tables.remove(&qualified_name);
        }

        Ok(())
    }

    /// Insert a row into a table
    pub fn insert_row(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        tables: &mut HashMap<String, Table>,
        table_name: &str,
        row: Row,
    ) -> Result<usize, StorageError> {
        // Normalize table name for lookup (matches catalog normalization)
        let normalized_name = if catalog.is_case_sensitive_identifiers() {
            table_name.to_string()
        } else {
            table_name.to_uppercase()
        };

        // First try direct lookup, then try with schema prefix if needed
        let table = if let Some(tbl) = tables.get_mut(&normalized_name) {
            tbl
        } else if !table_name.contains('.') {
            // Try with schema prefix
            let current_schema = catalog.get_current_schema();
            let qualified_name = format!("{}.{}", current_schema, normalized_name);
            tables
                .get_mut(&qualified_name)
                .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?
        } else {
            return Err(StorageError::TableNotFound(table_name.to_string()));
        };

        let row_index = table.row_count();

        // Check user-defined unique indexes BEFORE inserting
        if let Some(table_schema) = catalog.get_table(table_name) {
            self.index_manager.check_unique_constraints_for_insert(
                table_name,
                table_schema,
                &row,
            )?;
        }

        // Insert the row (this validates table-level constraints like PK, UNIQUE)
        table.insert(row.clone())?;

        // Update user-defined indexes
        if let Some(table_schema) = catalog.get_table(table_name) {
            self.index_manager.add_to_indexes_for_insert(table_name, table_schema, &row, row_index);
        }

        // Update spatial indexes
        self.update_spatial_indexes_for_insert(catalog, table_name, &row, row_index);

        Ok(row_index)
    }

    /// Insert multiple rows into a table in a single batch
    ///
    /// This method is optimized for bulk data loading. It uses `Table::insert_batch()`
    /// internally which provides significant performance improvements:
    ///
    /// - Pre-allocates vector capacity
    /// - Validates all rows before inserting any
    /// - Rebuilds indexes once after all inserts (vs per-row updates)
    /// - Invalidates caches only once at the end
    ///
    /// # Returns
    ///
    /// Row indices of all inserted rows (starting from the first new row)
    pub fn insert_rows_batch(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        tables: &mut HashMap<String, Table>,
        table_name: &str,
        rows: Vec<Row>,
    ) -> Result<Vec<usize>, StorageError> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        // Normalize table name for lookup (matches catalog normalization)
        let normalized_name = if catalog.is_case_sensitive_identifiers() {
            table_name.to_string()
        } else {
            table_name.to_uppercase()
        };

        // First try direct lookup, then try with schema prefix if needed
        let table = if let Some(tbl) = tables.get_mut(&normalized_name) {
            tbl
        } else if !table_name.contains('.') {
            // Try with schema prefix
            let current_schema = catalog.get_current_schema();
            let qualified_name = format!("{}.{}", current_schema, normalized_name);
            tables
                .get_mut(&qualified_name)
                .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?
        } else {
            return Err(StorageError::TableNotFound(table_name.to_string()));
        };

        // Get table schema once for all rows
        let table_schema = catalog.get_table(table_name);

        // Check user-defined unique indexes BEFORE inserting any rows
        // This is separate from the table-level constraint checks in Table::insert_batch
        if let Some(schema) = table_schema {
            for row in &rows {
                self.index_manager.check_unique_constraints_for_insert(table_name, schema, row)?;
            }
        }

        // Record start index for return value
        let start_index = table.row_count();

        // Check if we have any user-defined or spatial indexes for this table
        // Only clone rows if we actually need them for index updates
        let has_btree_indexes = self.index_manager.has_indexes_for_table(table_name);
        let has_spatial_indexes = self.has_spatial_indexes_for_table(table_name);
        let needs_index_updates = has_btree_indexes || has_spatial_indexes;

        // Conditionally clone rows only if index updates are needed
        // This avoids expensive cloning during bulk data loading when no indexes exist
        let rows_for_indexes = if needs_index_updates {
            Some(rows.clone())
        } else {
            None
        };

        // Use optimized batch insert
        let count = table.insert_batch(rows)?;

        // Generate row indices for return
        let row_indices: Vec<usize> = (start_index..start_index + count).collect();

        // Update user-defined indexes for all inserted rows using batch optimization
        // This pre-computes column indices once per index rather than once per row
        if let Some(rows_ref) = rows_for_indexes {
            let rows_to_insert: Vec<(usize, &Row)> = rows_ref
                .iter()
                .enumerate()
                .map(|(i, row)| (start_index + i, row))
                .collect();
            self.batch_add_to_indexes_for_insert(catalog, table_name, &rows_to_insert);
        }

        Ok(row_indices)
    }

    /// Insert rows from an iterator in a streaming fashion
    ///
    /// This method processes rows in batches for memory efficiency when loading
    /// very large datasets. Rows are committed batch-by-batch.
    ///
    /// # Arguments
    ///
    /// * `catalog` - The database catalog
    /// * `tables` - Map of table names to tables
    /// * `table_name` - Name of the table to insert into
    /// * `rows` - Iterator yielding rows to insert
    /// * `batch_size` - Number of rows per batch (default: 1000)
    ///
    /// # Returns
    ///
    /// Total number of rows successfully inserted
    ///
    /// # Note
    ///
    /// Unlike `insert_rows_batch`, this method commits in batches, so a failure
    /// partway through will leave previously committed rows in the table.
    #[allow(dead_code)] // Available for internal use; public API is via Database::insert_rows_iter
    pub fn insert_rows_iter<I>(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        tables: &mut HashMap<String, Table>,
        table_name: &str,
        rows: I,
        batch_size: usize,
    ) -> Result<usize, StorageError>
    where
        I: Iterator<Item = Row>,
    {
        let batch_size = if batch_size == 0 { 1000 } else { batch_size };
        let mut total_inserted = 0;
        let mut batch = Vec::with_capacity(batch_size);

        for row in rows {
            batch.push(row);

            if batch.len() >= batch_size {
                let indices = self.insert_rows_batch(
                    catalog,
                    tables,
                    table_name,
                    std::mem::take(&mut batch),
                )?;
                total_inserted += indices.len();
                batch = Vec::with_capacity(batch_size);
            }
        }

        // Insert any remaining rows
        if !batch.is_empty() {
            let indices = self.insert_rows_batch(catalog, tables, table_name, batch)?;
            total_inserted += indices.len();
        }

        Ok(total_inserted)
    }

    // ============================================================================
    // Index Management - Delegates to IndexManager
    // ============================================================================

    /// Validate prefix lengths for indexed columns
    ///
    /// Checks:
    /// 1. Prefix lengths are only used on string/binary types
    /// 2. Prefix lengths don't exceed column width (for fixed-width types)
    fn validate_prefix_lengths(
        table_schema: &vibesql_catalog::TableSchema,
        columns: &[IndexColumn],
    ) -> Result<(), StorageError> {
        use vibesql_types::DataType;

        for index_col in columns {
            if let Some(prefix_length) = index_col.prefix_length {
                // Find the column in the table schema
                let column_schema = table_schema
                    .columns
                    .iter()
                    .find(|col| col.name == index_col.column_name)
                    .ok_or_else(|| StorageError::ColumnNotFound {
                        column_name: index_col.column_name.clone(),
                        table_name: table_schema.name.clone(),
                    })?;

                // Check if the column type supports prefix indexing
                match &column_schema.data_type {
                    // String types that support prefix indexing
                    DataType::Character { length } => {
                        // Check if prefix exceeds column width
                        if prefix_length as usize > *length {
                            eprintln!(
                                "Warning: Key part '{}' prefix length ({}) exceeds column width ({})",
                                index_col.column_name, prefix_length, length
                            );
                        }
                    }
                    DataType::Varchar { max_length } => {
                        // Check if prefix exceeds column width (if specified)
                        if let Some(max_len) = max_length {
                            if prefix_length as usize > *max_len {
                                eprintln!(
                                    "Warning: Key part '{}' prefix length ({}) exceeds column width ({})",
                                    index_col.column_name, prefix_length, max_len
                                );
                            }
                        }
                    }
                    DataType::CharacterLargeObject | DataType::Name => {
                        // CLOB/TEXT and NAME types support prefix indexing without width check
                    }
                    DataType::BinaryLargeObject => {
                        // BLOB supports prefix indexing
                    }
                    // All other types do not support prefix indexing
                    _ => {
                        return Err(StorageError::InvalidIndexColumn(format!(
                            "Incorrect prefix key; the used key part '{}' isn't a string or binary type (type: {:?})",
                            index_col.column_name, column_schema.data_type
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Create an index
    pub fn create_index(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        tables: &HashMap<String, Table>,
        index_name: String,
        table_name: String,
        unique: bool,
        columns: Vec<IndexColumn>,
    ) -> Result<(), StorageError> {
        // Normalize table name for lookup (matches catalog normalization)
        let normalized_name = if catalog.is_case_sensitive_identifiers() {
            table_name.clone()
        } else {
            table_name.to_uppercase()
        };

        // Try to find the table with normalized name or qualified name
        let table = if let Some(tbl) = tables.get(&normalized_name) {
            tbl
        } else if !table_name.contains('.') {
            // Try with schema prefix
            let current_schema = catalog.get_current_schema();
            let qualified_name = format!("{}.{}", current_schema, normalized_name);
            tables
                .get(&qualified_name)
                .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?
        } else {
            return Err(StorageError::TableNotFound(table_name.clone()));
        };

        let table_schema = catalog
            .get_table(&table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

        // Validate prefix lengths against column types and widths
        Self::validate_prefix_lengths(table_schema, &columns)?;

        // Pass table rows directly by reference - avoid cloning all rows
        // This is critical for performance at scale (O(n) clone was causing major slowdown)
        self.index_manager.create_index(
            index_name,
            table_name,
            table_schema,
            table.scan(),
            unique,
            columns,
        )
    }

    /// Check if an index exists
    pub fn index_exists(&self, index_name: &str) -> bool {
        self.index_manager.index_exists(index_name)
    }

    /// Get index metadata
    pub fn get_index(&self, index_name: &str) -> Option<&super::indexes::IndexMetadata> {
        self.index_manager.get_index(index_name)
    }

    /// Get index data
    pub fn get_index_data(&self, index_name: &str) -> Option<&super::indexes::IndexData> {
        self.index_manager.get_index_data(index_name)
    }

    /// Update user-defined indexes for update operation
    ///
    /// # Arguments
    /// * `catalog` - Database catalog for schema lookup
    /// * `table_name` - Name of the table being updated
    /// * `old_row` - Row data before the update
    /// * `new_row` - Row data after the update
    /// * `row_index` - Index of the row in the table
    /// * `changed_columns` - Optional set of column indices that were modified.
    ///   If provided, indexes that don't involve any changed columns will be skipped.
    pub fn update_indexes_for_update(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        table_name: &str,
        old_row: &Row,
        new_row: &Row,
        row_index: usize,
        changed_columns: Option<&std::collections::HashSet<usize>>,
    ) {
        if let Some(table_schema) = catalog.get_table(table_name) {
            self.index_manager.update_indexes_for_update(
                table_name,
                table_schema,
                old_row,
                new_row,
                row_index,
                changed_columns,
            );
        }

        self.update_spatial_indexes_for_update(catalog, table_name, old_row, new_row, row_index);
    }

    /// Update user-defined indexes for delete operation
    pub fn update_indexes_for_delete(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        table_name: &str,
        row: &Row,
        row_index: usize,
    ) {
        self.update_indexes_for_delete_with_values(catalog, table_name, &row.values, row_index);
    }

    /// Update user-defined indexes for delete operation using raw values slice
    ///
    /// This is an optimization over `update_indexes_for_delete` that avoids requiring
    /// a full Row struct. Useful in the fast delete path where we already have a values
    /// slice and want to avoid wrapping overhead.
    pub fn update_indexes_for_delete_with_values(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        table_name: &str,
        values: &[vibesql_types::SqlValue],
        row_index: usize,
    ) {
        if let Some(table_schema) = catalog.get_table(table_name) {
            self.index_manager
                .update_indexes_for_delete_with_values(table_name, table_schema, values, row_index);
        }

        self.update_spatial_indexes_for_delete_with_values(catalog, table_name, values, row_index);
    }

    /// Batch update user-defined indexes for delete operation
    ///
    /// This is significantly more efficient than calling `update_indexes_for_delete` in a loop
    /// because it pre-computes column indices once per index rather than once per row.
    pub fn batch_update_indexes_for_delete(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        table_name: &str,
        rows_to_delete: &[(usize, &Row)],
    ) {
        if let Some(table_schema) = catalog.get_table(table_name) {
            self.index_manager.batch_update_indexes_for_delete(table_name, table_schema, rows_to_delete);
        }

        // Batch update spatial indexes (pre-computes column indices once per index)
        self.batch_update_spatial_indexes_for_delete(catalog, table_name, rows_to_delete);
    }

    /// Batch add to user-defined indexes for insert operation
    ///
    /// This is significantly more efficient than calling `add_to_indexes_for_insert` in a loop
    /// because it pre-computes column indices once per index rather than once per row.
    pub fn batch_add_to_indexes_for_insert(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        table_name: &str,
        rows_to_insert: &[(usize, &Row)],
    ) {
        if let Some(table_schema) = catalog.get_table(table_name) {
            self.index_manager.batch_add_to_indexes_for_insert(table_name, table_schema, rows_to_insert);
        }

        // Update spatial indexes in batch
        self.batch_update_spatial_indexes_for_insert(catalog, table_name, rows_to_insert);
    }

    /// Rebuild user-defined indexes after bulk operations that change row indices
    pub fn rebuild_indexes(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        tables: &HashMap<String, Table>,
        table_name: &str,
    ) {
        // Normalize table name for lookup (matches catalog normalization)
        let normalized_name = if catalog.is_case_sensitive_identifiers() {
            table_name.to_string()
        } else {
            table_name.to_uppercase()
        };

        // First try direct lookup, then try with schema prefix if needed
        let table_rows: Vec<Row> = if let Some(table) = tables.get(&normalized_name) {
            table.scan().to_vec()
        } else if !table_name.contains('.') {
            // Try with schema prefix
            let current_schema = catalog.get_current_schema();
            let qualified_name = format!("{}.{}", current_schema, normalized_name);
            if let Some(table) = tables.get(&qualified_name) {
                table.scan().to_vec()
            } else {
                return;
            }
        } else {
            return;
        };

        let table_schema = match catalog.get_table(table_name) {
            Some(schema) => schema,
            None => return,
        };

        self.index_manager.rebuild_indexes(table_name, table_schema, &table_rows);
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
        self.index_manager.adjust_indexes_after_delete(table_name, deleted_indices);
    }

    /// Drop an index
    pub fn drop_index(&mut self, index_name: &str) -> Result<(), StorageError> {
        self.index_manager.drop_index(index_name)
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<String> {
        self.index_manager.list_indexes()
    }

    /// List all indexes for a specific table
    pub fn list_indexes_for_table(&self, table_name: &str) -> Vec<String> {
        // Normalize for case-insensitive comparison
        let normalized_search = table_name.to_uppercase();

        self.index_manager
            .list_indexes()
            .into_iter()
            .filter(|index_name| {
                self.index_manager
                    .get_index(index_name)
                    .map(|metadata| {
                        // Normalize both sides for comparison
                        metadata.table_name.to_uppercase() == normalized_search
                    })
                    .unwrap_or(false)
            })
            .collect()
    }

    /// Check if a column has any user-defined index (B-tree or spatial)
    #[inline]
    pub fn has_index_on_column(&self, table_name: &str, column_name: &str) -> bool {
        let normalized_table = table_name.to_uppercase();
        let normalized_column = column_name.to_uppercase();

        // Check B-tree indexes
        for index_name in self.index_manager.list_indexes() {
            if let Some(metadata) = self.index_manager.get_index(&index_name) {
                if metadata.table_name.to_uppercase() == normalized_table {
                    for col in &metadata.columns {
                        if col.column_name.to_uppercase() == normalized_column {
                            return true;
                        }
                    }
                }
            }
        }

        // Check spatial indexes
        for (metadata, _) in self.spatial_indexes.values() {
            if metadata.table_name.to_uppercase() == normalized_table
                && metadata.column_name.to_uppercase() == normalized_column
            {
                return true;
            }
        }

        false
    }

    // ========================================================================
    // Spatial Index Methods
    // ========================================================================

    /// Normalize an index name to uppercase for case-insensitive comparison
    fn normalize_index_name(name: &str) -> String {
        name.to_uppercase()
    }

    /// Create a spatial index
    pub fn create_spatial_index(
        &mut self,
        metadata: SpatialIndexMetadata,
        spatial_index: SpatialIndex,
    ) -> Result<(), StorageError> {
        let normalized_name = Self::normalize_index_name(&metadata.index_name);

        if self.index_manager.index_exists(&metadata.index_name) {
            return Err(StorageError::IndexAlreadyExists(metadata.index_name.clone()));
        }
        if self.spatial_indexes.contains_key(&normalized_name) {
            return Err(StorageError::IndexAlreadyExists(metadata.index_name.clone()));
        }

        self.spatial_indexes.insert(normalized_name, (metadata, spatial_index));
        Ok(())
    }

    /// Create an IVFFlat index for approximate nearest neighbor search
    ///
    /// Extracts vectors from the specified table and builds an IVFFlat index
    /// using k-means clustering.
    #[allow(clippy::too_many_arguments)]
    pub fn create_ivfflat_index(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        tables: &std::collections::HashMap<String, crate::Table>,
        index_name: String,
        table_name: String,
        column_name: String,
        col_idx: usize,
        dimensions: usize,
        lists: usize,
        metric: vibesql_ast::VectorDistanceMetric,
    ) -> Result<(), StorageError> {
        // Normalize table name for lookup (matches catalog normalization)
        let normalized_name = if catalog.is_case_sensitive_identifiers() {
            table_name.clone()
        } else {
            table_name.to_uppercase()
        };

        // Try to find the table with normalized name or qualified name
        let table = if let Some(tbl) = tables.get(&normalized_name) {
            tbl
        } else if !table_name.contains('.') {
            // Try with schema prefix
            let current_schema = catalog.get_current_schema();
            let qualified_name = format!("{}.{}", current_schema, normalized_name);
            tables
                .get(&qualified_name)
                .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?
        } else {
            return Err(StorageError::TableNotFound(table_name.clone()));
        };

        // Extract vectors from the table
        // Note: SqlValue::Vector stores f32, but IVFFlat uses f64 for precision in clustering
        let rows = table.scan();
        let total_rows = rows.len();
        let mut vectors: Vec<(usize, Vec<f64>)> = Vec::new();
        let mut progress = ProgressTracker::new(
            format!("Creating IVFFlat index '{}'", index_name),
            Some(total_rows),
        );
        for (row_idx, row) in rows.iter().enumerate() {
            if col_idx < row.values.len() {
                if let vibesql_types::SqlValue::Vector(vec_data) = &row.values[col_idx] {
                    // Convert f32 vector to f64 for IVFFlat processing
                    let vec_f64: Vec<f64> = vec_data.iter().map(|&v| v as f64).collect();
                    vectors.push((row_idx, vec_f64));
                }
            }
            progress.update(row_idx + 1);
        }
        progress.finish();

        // Create the IVFFlat index with the extracted vectors
        self.index_manager.create_ivfflat_index_with_vectors(
            index_name,
            table_name,
            column_name,
            dimensions,
            lists,
            metric,
            vectors,
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
        self.index_manager.search_ivfflat_index(index_name, query_vector, k)
    }

    /// Get all IVFFlat indexes for a specific table
    pub fn get_ivfflat_indexes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<(&super::indexes::IndexMetadata, &super::indexes::ivfflat::IVFFlatIndex)> {
        self.index_manager.get_ivfflat_indexes_for_table(table_name)
    }

    /// Set the number of probes for an IVFFlat index
    pub fn set_ivfflat_probes(
        &mut self,
        index_name: &str,
        probes: usize,
    ) -> Result<(), StorageError> {
        self.index_manager.set_ivfflat_probes(index_name, probes)
    }

    // ============================================================================
    // HNSW Index Methods
    // ============================================================================

    /// Create an HNSW index for approximate nearest neighbor search
    ///
    /// Extracts vectors from the specified table and builds an HNSW index
    /// using the hierarchical navigable small world algorithm.
    #[allow(clippy::too_many_arguments)]
    pub fn create_hnsw_index(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        tables: &std::collections::HashMap<String, crate::Table>,
        index_name: String,
        table_name: String,
        column_name: String,
        col_idx: usize,
        dimensions: usize,
        m: u32,
        ef_construction: u32,
        metric: vibesql_ast::VectorDistanceMetric,
    ) -> Result<(), StorageError> {
        // Normalize table name for lookup (matches catalog normalization)
        let normalized_name = if catalog.is_case_sensitive_identifiers() {
            table_name.clone()
        } else {
            table_name.to_uppercase()
        };

        // Try to find the table with normalized name or qualified name
        let table = if let Some(tbl) = tables.get(&normalized_name) {
            tbl
        } else if !table_name.contains('.') {
            // Try with schema prefix
            let current_schema = catalog.get_current_schema();
            let qualified_name = format!("{}.{}", current_schema, normalized_name);
            tables
                .get(&qualified_name)
                .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?
        } else {
            return Err(StorageError::TableNotFound(table_name.clone()));
        };

        // Extract vectors from the table
        // Note: SqlValue::Vector stores f32, but HNSW uses f64 for precision
        let rows = table.scan();
        let total_rows = rows.len();
        let mut vectors: Vec<(usize, Vec<f64>)> = Vec::new();
        let mut progress = ProgressTracker::new(
            format!("Creating HNSW index '{}'", index_name),
            Some(total_rows),
        );
        for (row_idx, row) in rows.iter().enumerate() {
            if col_idx < row.values.len() {
                if let vibesql_types::SqlValue::Vector(vec_data) = &row.values[col_idx] {
                    // Convert f32 vector to f64 for HNSW processing
                    let vec_f64: Vec<f64> = vec_data.iter().map(|&v| v as f64).collect();
                    vectors.push((row_idx, vec_f64));
                }
            }
            progress.update(row_idx + 1);
        }
        progress.finish();

        // Create the HNSW index with the extracted vectors
        self.index_manager.create_hnsw_index_with_vectors(
            index_name,
            table_name,
            column_name,
            dimensions,
            m,
            ef_construction,
            metric,
            vectors,
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
        self.index_manager.search_hnsw_index(index_name, query_vector, k)
    }

    /// Get all HNSW indexes for a specific table
    pub fn get_hnsw_indexes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<(&super::indexes::IndexMetadata, &super::indexes::hnsw::HnswIndex)> {
        self.index_manager.get_hnsw_indexes_for_table(table_name)
    }

    /// Set the ef_search parameter for an HNSW index
    pub fn set_hnsw_ef_search(
        &mut self,
        index_name: &str,
        ef_search: usize,
    ) -> Result<(), StorageError> {
        self.index_manager.set_hnsw_ef_search(index_name, ef_search)
    }

    /// Check if a spatial index exists
    pub fn spatial_index_exists(&self, index_name: &str) -> bool {
        let normalized = Self::normalize_index_name(index_name);
        self.spatial_indexes.contains_key(&normalized)
    }

    /// Get spatial index metadata
    pub fn get_spatial_index_metadata(&self, index_name: &str) -> Option<&SpatialIndexMetadata> {
        let normalized = Self::normalize_index_name(index_name);
        self.spatial_indexes.get(&normalized).map(|(metadata, _)| metadata)
    }

    /// Get spatial index (immutable)
    pub fn get_spatial_index(&self, index_name: &str) -> Option<&SpatialIndex> {
        let normalized = Self::normalize_index_name(index_name);
        self.spatial_indexes.get(&normalized).map(|(_, index)| index)
    }

    /// Get spatial index (mutable)
    pub fn get_spatial_index_mut(&mut self, index_name: &str) -> Option<&mut SpatialIndex> {
        let normalized = Self::normalize_index_name(index_name);
        self.spatial_indexes.get_mut(&normalized).map(|(_, index)| index)
    }

    /// Get all spatial indexes for a specific table
    pub fn get_spatial_indexes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<(&SpatialIndexMetadata, &SpatialIndex)> {
        self.spatial_indexes
            .values()
            .filter(|(metadata, _)| metadata.table_name == table_name)
            .map(|(metadata, index)| (metadata, index))
            .collect()
    }

    /// Get all spatial indexes for a specific table (mutable)
    pub fn get_spatial_indexes_for_table_mut(
        &mut self,
        table_name: &str,
    ) -> Vec<(&SpatialIndexMetadata, &mut SpatialIndex)> {
        self.spatial_indexes
            .iter_mut()
            .filter(|(_, (metadata, _))| metadata.table_name == table_name)
            .map(|(_, (metadata, index))| (metadata as &SpatialIndexMetadata, index))
            .collect()
    }

    /// Drop a spatial index
    pub fn drop_spatial_index(&mut self, index_name: &str) -> Result<(), StorageError> {
        let normalized = Self::normalize_index_name(index_name);

        if self.spatial_indexes.remove(&normalized).is_none() {
            return Err(StorageError::IndexNotFound(index_name.to_string()));
        }

        Ok(())
    }

    /// Drop all spatial indexes associated with a table (CASCADE behavior)
    ///
    /// Matching is case-insensitive and handles both qualified ("schema.table")
    /// and unqualified ("table") names.
    pub fn drop_spatial_indexes_for_table(&mut self, table_name: &str) -> Vec<String> {
        // Normalize for case-insensitive comparison
        let search_name_upper = table_name.to_uppercase();

        // Extract just the table name part if qualified (e.g., "public.users" -> "users")
        let search_table_only = search_name_upper.rsplit('.').next().unwrap_or(&search_name_upper);

        let indexes_to_drop: Vec<String> = self
            .spatial_indexes
            .iter()
            .filter(|(_, (metadata, _))| {
                let stored_upper = metadata.table_name.to_uppercase();
                let stored_table_only = stored_upper.rsplit('.').next().unwrap_or(&stored_upper);

                // Match if full names match OR unqualified parts match
                stored_upper == search_name_upper || stored_table_only == search_table_only
            })
            .map(|(name, _)| name.clone())
            .collect();

        for index_name in &indexes_to_drop {
            self.spatial_indexes.remove(index_name);
        }

        indexes_to_drop
    }

    /// List all spatial indexes
    pub fn list_spatial_indexes(&self) -> Vec<String> {
        self.spatial_indexes.keys().cloned().collect()
    }

    /// Check if any spatial indexes exist for a specific table
    ///
    /// This is an O(n) operation over all spatial indexes but is useful for
    /// optimizing bulk insert operations when no indexes need updating.
    fn has_spatial_indexes_for_table(&self, table_name: &str) -> bool {
        self.spatial_indexes.values().any(|(metadata, _)| metadata.table_name == table_name)
    }

    /// Update spatial indexes for insert operation
    fn update_spatial_indexes_for_insert(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        table_name: &str,
        row: &Row,
        row_index: usize,
    ) {
        let table_schema = match catalog.get_table(table_name) {
            Some(schema) => schema,
            None => return,
        };

        let indexes_to_update: Vec<(String, usize)> = self
            .spatial_indexes
            .iter()
            .filter(|(_, (metadata, _))| metadata.table_name == table_name)
            .filter_map(|(index_name, (metadata, _))| {
                table_schema
                    .get_column_index(&metadata.column_name)
                    .map(|col_idx| (index_name.clone(), col_idx))
            })
            .collect();

        for (index_name, col_idx) in indexes_to_update {
            let geom_value = &row.values[col_idx];

            if let Some(mbr) = extract_mbr_from_sql_value(geom_value) {
                if let Some((_, index)) = self.spatial_indexes.get_mut(&index_name) {
                    index.insert(row_index, mbr);
                }
            }
        }
    }

    /// Batch update spatial indexes for insert operation
    ///
    /// This is more efficient than calling `update_spatial_indexes_for_insert` in a loop
    /// because it pre-computes column indices once per index rather than once per row.
    ///
    /// # Arguments
    /// * `catalog` - The database catalog
    /// * `table_name` - The table name
    /// * `rows_to_insert` - Vec of (row_index, row) pairs to insert
    fn batch_update_spatial_indexes_for_insert(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        table_name: &str,
        rows_to_insert: &[(usize, &Row)],
    ) {
        if rows_to_insert.is_empty() {
            return;
        }

        let table_schema = match catalog.get_table(table_name) {
            Some(schema) => schema,
            None => return,
        };

        // Pre-compute indexes and column indices once
        let indexes_to_update: Vec<(String, usize)> = self
            .spatial_indexes
            .iter()
            .filter(|(_, (metadata, _))| metadata.table_name == table_name)
            .filter_map(|(index_name, (metadata, _))| {
                table_schema
                    .get_column_index(&metadata.column_name)
                    .map(|col_idx| (index_name.clone(), col_idx))
            })
            .collect();

        // Process each index
        for (index_name, col_idx) in indexes_to_update {
            if let Some((_, index)) = self.spatial_indexes.get_mut(&index_name) {
                for &(row_index, row) in rows_to_insert {
                    let geom_value = &row.values[col_idx];
                    if let Some(mbr) = extract_mbr_from_sql_value(geom_value) {
                        index.insert(row_index, mbr);
                    }
                }
            }
        }
    }

    /// Update spatial indexes for update operation
    fn update_spatial_indexes_for_update(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        table_name: &str,
        old_row: &Row,
        new_row: &Row,
        row_index: usize,
    ) {
        let table_schema = match catalog.get_table(table_name) {
            Some(schema) => schema,
            None => return,
        };

        let indexes_to_update: Vec<(String, usize)> = self
            .spatial_indexes
            .iter()
            .filter(|(_, (metadata, _))| metadata.table_name == table_name)
            .filter_map(|(index_name, (metadata, _))| {
                table_schema
                    .get_column_index(&metadata.column_name)
                    .map(|col_idx| (index_name.clone(), col_idx))
            })
            .collect();

        for (index_name, col_idx) in indexes_to_update {
            let old_geom = &old_row.values[col_idx];
            let new_geom = &new_row.values[col_idx];

            if old_geom != new_geom {
                if let Some((_, index)) = self.spatial_indexes.get_mut(&index_name) {
                    if let Some(old_mbr) = extract_mbr_from_sql_value(old_geom) {
                        index.remove(row_index, &old_mbr);
                    }

                    if let Some(new_mbr) = extract_mbr_from_sql_value(new_geom) {
                        index.insert(row_index, new_mbr);
                    }
                }
            }
        }
    }

    fn update_spatial_indexes_for_delete_with_values(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        table_name: &str,
        values: &[vibesql_types::SqlValue],
        row_index: usize,
    ) {
        let table_schema = match catalog.get_table(table_name) {
            Some(schema) => schema,
            None => return,
        };

        let indexes_to_update: Vec<(String, usize)> = self
            .spatial_indexes
            .iter()
            .filter(|(_, (metadata, _))| metadata.table_name == table_name)
            .filter_map(|(index_name, (metadata, _))| {
                table_schema
                    .get_column_index(&metadata.column_name)
                    .map(|col_idx| (index_name.clone(), col_idx))
            })
            .collect();

        for (index_name, col_idx) in indexes_to_update {
            let geom_value = &values[col_idx];

            if let Some(mbr) = extract_mbr_from_sql_value(geom_value) {
                if let Some((_, index)) = self.spatial_indexes.get_mut(&index_name) {
                    index.remove(row_index, &mbr);
                }
            }
        }
    }

    /// Batch update spatial indexes for delete operation
    ///
    /// This is significantly more efficient than calling `update_spatial_indexes_for_delete_with_values`
    /// in a loop because it pre-computes column indices once per index rather than once per row.
    fn batch_update_spatial_indexes_for_delete(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        table_name: &str,
        rows_to_delete: &[(usize, &Row)],
    ) {
        if rows_to_delete.is_empty() {
            return;
        }

        let table_schema = match catalog.get_table(table_name) {
            Some(schema) => schema,
            None => return,
        };

        // Pre-compute which spatial indexes apply to this table and their column indices
        let indexes_to_update: Vec<(String, usize)> = self
            .spatial_indexes
            .iter()
            .filter(|(_, (metadata, _))| metadata.table_name == table_name)
            .filter_map(|(index_name, (metadata, _))| {
                table_schema
                    .get_column_index(&metadata.column_name)
                    .map(|col_idx| (index_name.clone(), col_idx))
            })
            .collect();

        if indexes_to_update.is_empty() {
            return;
        }

        // Process each spatial index - batch remove entries for all rows
        for (index_name, col_idx) in indexes_to_update {
            if let Some((_, index)) = self.spatial_indexes.get_mut(&index_name) {
                for &(row_index, row) in rows_to_delete {
                    let geom_value = &row.values[col_idx];
                    if let Some(mbr) = extract_mbr_from_sql_value(geom_value) {
                        index.remove(row_index, &mbr);
                    }
                }
            }
        }
    }

    /// Reset the operations manager to empty state (clears all indexes).
    ///
    /// Clears all index data but preserves configuration (database path, storage backend, config).
    /// This is more efficient than creating a new instance and ensures indexes work after reset.
    pub fn reset(&mut self) {
        // Clear all user-defined indexes (preserves database_path, storage, config)
        self.index_manager.reset();

        // Clear all spatial indexes
        self.spatial_indexes.clear();
    }
}

impl Default for Operations {
    fn default() -> Self {
        Self::new()
    }
}
