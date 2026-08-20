// ============================================================================
// Table and Index Operations
// ============================================================================

use std::collections::HashMap;

use vibesql_ast::IndexColumn;

use super::indexes::IndexManager;
use crate::{
    index::{extract_mbr_from_sql_value, SpatialIndex},
    progress::ProgressTracker,
    Row, StorageError, Table,
};

/// Metadata for a spatial index
#[derive(Debug, Clone)]
pub struct SpatialIndexMetadata {
    pub index_name: String,
    pub table_name: String,
    pub column_name: String,
    /// Owning schema of this index (e.g. `main` or a session temp schema like
    /// `temp_42`). Stored so a temp-table spatial index and a main-table spatial
    /// index can share a bare name without colliding in the `spatial_indexes`
    /// map — mirroring the B-tree change in #5540 (storage) / #5513 (catalog).
    /// See issue #5558.
    pub schema: String,
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Build the storage key used to key spatial indexes in the `spatial_indexes`
/// map.
///
/// Mirrors the B-tree `make_index_key` introduced in #5540: a `main`-schema
/// spatial index keeps a *bare* (normalized) key so the common case is
/// byte-for-byte backward compatible, while a non-`main` (e.g. session temp)
/// schema gets a `schema.name` prefix. This lets `main.ix` and `temp.ix`
/// coexist as distinct spatial indexes — matching SQLite and the B-tree
/// behavior. See issue #5558.
fn make_spatial_index_key(schema: &str, index_name: &str) -> String {
    let normalized_name = index_name.to_lowercase();
    if schema.eq_ignore_ascii_case(vibesql_catalog::DEFAULT_SCHEMA) {
        normalized_name
    } else {
        format!("{}.{}", schema.to_lowercase(), normalized_name)
    }
}

/// Manages table and index operations
#[derive(Debug, Clone)]
pub struct Operations {
    /// User-defined index manager (B-tree indexes)
    index_manager: IndexManager,
    /// Spatial indexes (R-tree) - stored separately from B-tree indexes
    /// Key: schema-aware index key (bare normalized name for the `main` schema,
    ///   `schema.name` for non-`main` schemas — see [`make_spatial_index_key`])
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

    /// Find a table by name with fallback lookups for quoted identifiers.
    ///
    /// This tries multiple lookup strategies to handle both quoted and unquoted identifiers:
    /// 1. Resolve "temp" schema to session's temp schema (SQLite compatibility)
    /// 2. Direct lookup as-is (for quoted identifiers that preserve case)
    /// 3. Normalized (lowercase) lookup
    /// 4. Temp schema lookup (SQLite semantics - temp tables shadow main tables)
    /// 5. Schema-qualified with original case
    /// 6. Schema-qualified with normalized case
    fn find_table_mut<'a>(
        catalog: &vibesql_catalog::Catalog,
        tables: &'a mut HashMap<String, Table>,
        table_name: &str,
    ) -> Result<&'a mut Table, StorageError> {
        // For qualified names with "temp" schema, resolve to session's temp schema
        // This enables `INSERT INTO temp.t1 VALUES(...)` syntax
        let resolved_name = if let Some((schema_part, table_part)) = table_name.split_once('.') {
            if schema_part.eq_ignore_ascii_case(vibesql_catalog::TEMP_SCHEMA) {
                Some(format!("{}.{}", catalog.temp_schema_name(), table_part))
            } else {
                None
            }
        } else {
            None
        };
        let table_name = resolved_name.as_deref().unwrap_or(table_name);

        // Try 1: Direct lookup as-is (handles quoted identifiers correctly)
        if tables.contains_key(table_name) {
            return Ok(tables.get_mut(table_name).unwrap());
        }

        let normalized_name = if catalog.is_case_sensitive_identifiers() {
            table_name.to_string()
        } else {
            table_name.to_lowercase()
        };

        // Try 2: Normalized name (for unquoted identifiers)
        if normalized_name != table_name && tables.contains_key(&normalized_name) {
            return Ok(tables.get_mut(&normalized_name).unwrap());
        }

        // Try with schema prefix if not already qualified
        if !table_name.contains('.') {
            // Try 3: Session's temp schema first (SQLite semantics - temp tables shadow main tables)
            let temp_qualified = format!("{}.{}", catalog.temp_schema_name(), normalized_name);
            if tables.contains_key(&temp_qualified) {
                return Ok(tables.get_mut(&temp_qualified).unwrap());
            }

            let current_schema = catalog.get_current_schema();

            // Try 4: Schema-qualified with original case (for quoted identifiers)
            let qualified_original = format!("{}.{}", current_schema, table_name);
            if tables.contains_key(&qualified_original) {
                return Ok(tables.get_mut(&qualified_original).unwrap());
            }

            // Try 5: Schema-qualified with normalized case
            if normalized_name != table_name {
                let qualified_normalized = format!("{}.{}", current_schema, normalized_name);
                if tables.contains_key(&qualified_normalized) {
                    return Ok(tables.get_mut(&qualified_normalized).unwrap());
                }
            }

            // Try 6: Attached databases in attachment order (SQLite searches
            // temp, then main, then each ATTACHed database — #6310).
            for attached in catalog.attached_databases() {
                let attached_qualified = format!("{}.{}", attached.name, normalized_name);
                if tables.contains_key(&attached_qualified) {
                    return Ok(tables.get_mut(&attached_qualified).unwrap());
                }
            }
        }

        Err(StorageError::TableNotFound(table_name.to_string()))
    }

    /// Drop a table from the catalog
    ///
    /// SQLite Compatibility: The "temp" schema name is mapped to the session's
    /// temp schema, allowing `DROP TABLE temp.tablename` syntax.
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
            name.to_lowercase()
        };

        // Resolve "temp" schema to session's temp schema for storage lookup
        let resolved_name = if let Some((schema_part, table_part)) = normalized_name.split_once('.')
        {
            if schema_part.eq_ignore_ascii_case(vibesql_catalog::TEMP_SCHEMA) {
                format!("{}.{}", catalog.temp_schema_name(), table_part)
            } else {
                normalized_name.clone()
            }
        } else {
            normalized_name.clone()
        };

        // Get qualified table name for index cleanup and storage removal.
        //
        // For a bare (unqualified) name, resolve the owning schema with SQLite's
        // temp-shadows-main order so a temp table is removed from its temp schema
        // key (`temp_<n>.<table>`) rather than wrongly assuming `main`. Without
        // this, dropping a temp table would leak its storage entry (and miss its
        // temp-schema indexes). Resolution must run while the catalog entry still
        // exists, i.e. before `catalog.drop_table` below. See #5596.
        let qualified_name = if resolved_name.contains('.') {
            resolved_name.clone()
        } else {
            let owning_schema = catalog
                .resolve_table_schema_name(&resolved_name)
                .unwrap_or_else(|| catalog.get_current_schema().to_string());
            format!("{}.{}", owning_schema, resolved_name)
        };

        // Drop associated indexes BEFORE dropping table (CASCADE behavior)
        self.index_manager.drop_indexes_for_table(&qualified_name);

        // Drop associated spatial indexes too
        self.drop_spatial_indexes_for_table(&qualified_name);

        // Remove from catalog
        catalog.drop_table(name).map_err(|e| StorageError::CatalogError(e.to_string()))?;

        // Remove table data - try resolved name first, then try qualified name
        if tables.remove(&resolved_name).is_none() {
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
        // Use the helper function for proper table lookup with fallbacks for quoted identifiers
        let table = Self::find_table_mut(catalog, tables, table_name)?;

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

        // Use the helper function for proper table lookup with fallbacks for quoted identifiers
        let table = Self::find_table_mut(catalog, tables, table_name)?;

        // Get table schema once for all rows
        let table_schema = catalog.get_table(table_name);

        // Check user-defined unique indexes BEFORE inserting any rows
        // This is separate from the table-level constraint checks in Table::insert_batch.
        // The batch variant also tracks keys claimed by earlier rows of this
        // same batch (issue #6346): the index bodies are only rebuilt after
        // the bulk append, so without in-batch tracking two colliding rows
        // in one batch would both pass and both be written.
        if let Some(schema) = table_schema {
            self.index_manager.check_unique_constraints_for_insert_batch(
                table_name,
                schema,
                &rows,
            )?;
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
        let rows_for_indexes = if needs_index_updates { Some(rows.clone()) } else { None };

        // Use optimized batch insert
        let count = table.insert_batch(rows)?;

        // Generate row indices for return
        let row_indices: Vec<usize> = (start_index..start_index + count).collect();

        // Update user-defined indexes for all inserted rows using batch optimization
        // This pre-computes column indices once per index rather than once per row
        if let Some(rows_ref) = rows_for_indexes {
            let rows_to_insert: Vec<(usize, &Row)> =
                rows_ref.iter().enumerate().map(|(i, row)| (start_index + i, row)).collect();
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
            if let Some(prefix_length) = index_col.prefix_length() {
                // Find the column in the table schema
                let column_schema = table_schema
                    .columns
                    .iter()
                    .find(|col| col.name == index_col.expect_column_name())
                    .ok_or_else(|| StorageError::ColumnNotFound {
                        column_name: index_col.expect_column_name().to_string(),
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
                                index_col.expect_column_name(), prefix_length, length
                            );
                        }
                    }
                    DataType::Varchar { max_length } => {
                        // Check if prefix exceeds column width (if specified)
                        if let Some(max_len) = max_length {
                            if prefix_length as usize > *max_len {
                                eprintln!(
                                    "Warning: Key part '{}' prefix length ({}) exceeds column width ({})",
                                    index_col.expect_column_name(), prefix_length, max_len
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
                            index_col.expect_column_name(), column_schema.data_type
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Create an index
    ///
    /// `where_clause` and `included_row_indices` together describe the
    /// partial-index build path. When both are `None`, this builds a normal
    /// full-coverage index. When set, `where_clause` is stashed in the index
    /// metadata (so subsequent insert/update maintenance can distinguish
    /// partial indexes) and `included_row_indices` controls which existing
    /// table rows make it into the initial index body. The executor crate
    /// is responsible for evaluating the predicate to produce that set;
    /// storage never evaluates expressions.
    #[allow(clippy::too_many_arguments)]
    pub fn create_index(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        tables: &HashMap<String, Table>,
        index_name: String,
        table_name: String,
        unique: bool,
        columns: Vec<IndexColumn>,
        where_clause: Option<Box<vibesql_ast::Expression>>,
        included_row_indices: Option<&std::collections::HashSet<usize>>,
    ) -> Result<(), StorageError> {
        // Normalize table name for lookup (matches catalog normalization)
        let normalized_name = if catalog.is_case_sensitive_identifiers() {
            table_name.clone()
        } else {
            table_name.to_lowercase()
        };

        // Try to find the table with normalized name or qualified name.
        //
        // For an unqualified name, follow SQLite name resolution: the session
        // temp schema shadows `main`. A TEMP table is stored under the
        // `temp_<id>.<table>` physical key, so an unqualified index target that
        // names a temp table must be looked up there before falling back to the
        // current (main) schema. Previously only the current schema was tried,
        // so CREATE INDEX on a temp table failed with `TableNotFound`. See #5505.
        let table = if let Some(tbl) = tables.get(&normalized_name) {
            tbl
        } else if !table_name.contains('.') {
            // Temp schema first (temp tables shadow main).
            let temp_qualified = format!("{}.{}", catalog.temp_schema_name(), normalized_name);
            if let Some(tbl) = tables.get(&temp_qualified) {
                tbl
            } else {
                let current_schema = catalog.get_current_schema();
                let qualified_name = format!("{}.{}", current_schema, normalized_name);
                if let Some(tbl) = tables.get(&qualified_name) {
                    tbl
                } else {
                    // Attached databases in attachment order (SQLite searches
                    // temp, then main, then each ATTACHed database — #6310).
                    catalog
                        .attached_databases()
                        .iter()
                        .find_map(|attached| {
                            tables.get(&format!("{}.{}", attached.name, normalized_name))
                        })
                        .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?
                }
            }
        } else {
            return Err(StorageError::TableNotFound(table_name.clone()));
        };

        let table_schema = catalog
            .get_table(&table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

        // Validate prefix lengths against column types and widths
        Self::validate_prefix_lengths(table_schema, &columns)?;

        // Resolve the owning schema for the storage-side index key (#5540).
        // `resolve_table_schema_name` follows SQLite name resolution (temp shadows
        // main for unqualified names; explicit `schema.table` honored), matching
        // the schema the catalog tags the index with in #5513. Falls back to the
        // default (main) schema when the table can't be resolved.
        let index_schema = catalog
            .resolve_table_schema_name(&table_name)
            .unwrap_or_else(|| vibesql_catalog::DEFAULT_SCHEMA.to_string());

        // Pass table rows directly by reference - avoid cloning all rows
        // This is critical for performance at scale (O(n) clone was causing major slowdown)
        self.index_manager.create_index(
            index_name,
            table_name,
            &index_schema,
            table_schema,
            table.scan(),
            unique,
            columns,
            where_clause,
            included_row_indices,
        )
    }

    /// Create an index with pre-computed keys (for expression indexes)
    ///
    /// This method is used when the caller has already evaluated the expressions
    /// and computed the key values for each row. This is necessary for expression
    /// indexes where the key values are derived from evaluating expressions on rows.
    pub fn create_index_with_keys(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        index_name: String,
        table_name: String,
        unique: bool,
        columns: Vec<vibesql_ast::IndexColumn>,
        keys: Vec<(Vec<vibesql_types::SqlValue>, usize)>,
    ) -> Result<(), StorageError> {
        // Get the table schema for key type inference
        let table_schema = catalog
            .get_table(&table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

        // Resolve the owning schema for the storage-side index key (#5540).
        let index_schema = catalog
            .resolve_table_schema_name(&table_name)
            .unwrap_or_else(|| vibesql_catalog::DEFAULT_SCHEMA.to_string());

        self.index_manager.create_index_with_keys(
            index_name,
            table_name,
            &index_schema,
            table_schema,
            unique,
            columns,
            keys,
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

    /// Whether the index needs an expression-index rebuild after a snapshot
    /// reload (issue #5784).
    pub fn is_index_pending_rebuild(&self, index_name: &str) -> bool {
        self.index_manager.is_index_pending_rebuild(index_name)
    }

    /// List reloaded expression indexes needing rebuild as `(index, table)`.
    pub fn pending_expression_rebuilds(&self) -> Vec<(String, String)> {
        self.index_manager.pending_expression_rebuilds()
    }

    /// Repopulate a reloaded expression index body from executor-computed keys.
    pub fn populate_expression_index(
        &mut self,
        index_name: &str,
        keys: Vec<(Vec<vibesql_types::SqlValue>, usize)>,
    ) -> Result<(), StorageError> {
        self.index_manager.populate_expression_index(index_name, keys)
    }

    /// Update user-defined indexes for update operation
    ///
    /// # Arguments
    /// * `catalog` - Database catalog for schema lookup
    /// * `table_name` - Name of the table being updated
    /// * `old_row` - Row data before the update
    /// * `new_row` - Row data after the update
    /// * `row_index` - Index of the row in the table
    /// * `changed_columns` - Optional set of column indices that were modified. If provided,
    ///   indexes that don't involve any changed columns will be skipped.
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
            self.index_manager.update_indexes_for_delete_with_values(
                table_name,
                table_schema,
                values,
                row_index,
            );
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
            self.index_manager.batch_update_indexes_for_delete(
                table_name,
                table_schema,
                rows_to_delete,
            );
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
            self.index_manager.batch_add_to_indexes_for_insert(
                table_name,
                table_schema,
                rows_to_insert,
            );
        }

        // Update spatial indexes in batch
        self.batch_update_spatial_indexes_for_insert(catalog, table_name, rows_to_insert);
    }

    // ============================================================================
    // Partial Index Methods (operations layer)
    // ============================================================================

    /// Maintain partial indexes after inserting a row.
    pub fn add_to_partial_indexes_for_insert(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        table_name: &str,
        row: &Row,
        row_index: usize,
        included_partial_indexes: &std::collections::HashSet<String>,
    ) {
        if let Some(table_schema) = catalog.get_table(table_name) {
            self.index_manager.add_to_partial_indexes_for_insert(
                table_name,
                table_schema,
                row,
                row_index,
                included_partial_indexes,
            );
        }
    }

    /// Maintain partial indexes after updating a row.
    #[allow(clippy::too_many_arguments)]
    pub fn update_partial_indexes_for_update(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        table_name: &str,
        old_row: &Row,
        new_row: &Row,
        row_index: usize,
        old_included: &std::collections::HashSet<String>,
        new_included: &std::collections::HashSet<String>,
    ) {
        if let Some(table_schema) = catalog.get_table(table_name) {
            self.index_manager.update_partial_indexes_for_update(
                table_name,
                table_schema,
                old_row,
                new_row,
                row_index,
                old_included,
                new_included,
            );
        }
    }

    /// Maintain partial indexes after deleting a row.
    pub fn update_partial_indexes_for_delete_with_values(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        table_name: &str,
        values: &[vibesql_types::SqlValue],
        row_index: usize,
        included_partial_indexes: &std::collections::HashSet<String>,
    ) {
        if let Some(table_schema) = catalog.get_table(table_name) {
            self.index_manager.update_partial_indexes_for_delete(
                table_name,
                table_schema,
                values,
                row_index,
                included_partial_indexes,
            );
        }
    }

    /// Check whether a candidate key would violate the uniqueness of a
    /// partial UNIQUE index. Caller must have already verified that the
    /// partial-index WHERE predicate is truthy for the candidate row.
    pub fn check_partial_unique_conflict(
        &self,
        index_name: &str,
        key_values: &[vibesql_types::SqlValue],
    ) -> Result<bool, StorageError> {
        self.index_manager.check_partial_unique_conflict(index_name, key_values)
    }

    /// Whether the given table has any partial indexes.
    pub fn has_partial_indexes(&self, table_name: &str) -> bool {
        self.index_manager.has_partial_indexes(table_name)
    }

    /// Get all partial indexes for a specific table.
    pub fn get_partial_indexes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<(String, &super::indexes::IndexMetadata)> {
        self.index_manager.get_partial_indexes_for_table(table_name)
    }

    // ============================================================================
    // Expression Index Methods
    // ============================================================================

    /// Add row to expression indexes after insert with pre-computed keys
    ///
    /// This method handles expression indexes which require pre-computed key values
    /// since the storage layer cannot evaluate expressions.
    pub fn add_to_expression_indexes_for_insert(
        &mut self,
        table_name: &str,
        row_index: usize,
        expression_keys: &std::collections::HashMap<String, Vec<vibesql_types::SqlValue>>,
    ) {
        self.index_manager.add_to_expression_indexes_for_insert(
            table_name,
            row_index,
            expression_keys,
        );
    }

    /// Update expression indexes for update operation with pre-computed keys
    pub fn update_expression_indexes_for_update(
        &mut self,
        table_name: &str,
        row_index: usize,
        old_expression_keys: &std::collections::HashMap<String, Vec<vibesql_types::SqlValue>>,
        new_expression_keys: &std::collections::HashMap<String, Vec<vibesql_types::SqlValue>>,
    ) {
        self.index_manager.update_expression_indexes_for_update(
            table_name,
            row_index,
            old_expression_keys,
            new_expression_keys,
        );
    }

    /// Update expression indexes for delete operation with pre-computed keys
    pub fn update_expression_indexes_for_delete(
        &mut self,
        table_name: &str,
        row_index: usize,
        expression_keys: &std::collections::HashMap<String, Vec<vibesql_types::SqlValue>>,
    ) {
        self.index_manager.update_expression_indexes_for_delete(
            table_name,
            row_index,
            expression_keys,
        );
    }

    /// Get expression indexes for a specific table
    ///
    /// Returns metadata for all expression indexes on the table. Used by executor
    /// to determine which indexes need expression evaluation during DML operations.
    pub fn get_expression_indexes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<(String, &super::indexes::IndexMetadata)> {
        self.index_manager.get_expression_indexes_for_table(table_name)
    }

    /// Check if a table has any expression indexes
    pub fn has_expression_indexes(&self, table_name: &str) -> bool {
        self.index_manager.has_expression_indexes(table_name)
    }

    /// Clear expression index data for a table (for rebuilding after compaction)
    pub fn clear_expression_index_data(&mut self, table_name: &str) {
        self.index_manager.clear_expression_index_data(table_name);
    }

    /// Clear partial-index data for a table (for rebuilding after compaction).
    pub fn clear_partial_index_data(&mut self, table_name: &str) {
        self.index_manager.clear_partial_index_data(table_name);
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
            table_name.to_lowercase()
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

    /// Drop an index
    pub fn drop_index(&mut self, index_name: &str) -> Result<(), StorageError> {
        self.index_manager.drop_index(index_name)
    }

    /// Patch a storage-side index's WHERE clause. See
    /// `IndexManager::set_index_where_clause` for details. Used by
    /// persistence/recovery paths.
    pub fn set_index_where_clause(
        &mut self,
        index_name: &str,
        where_clause: Option<Box<vibesql_ast::Expression>>,
    ) -> bool {
        self.index_manager.set_index_where_clause(index_name, where_clause)
    }

    /// Propagate a column rename into the storage-side metadata of every
    /// index on `table_name`. See
    /// `IndexManager::rename_column_in_table_indexes` for details (issue
    /// #5877). Returns the number of indexes whose metadata changed.
    pub fn rename_column_in_table_indexes(
        &mut self,
        table_name: &str,
        old_column: &str,
        new_column: &str,
    ) -> usize {
        self.index_manager.rename_column_in_table_indexes(table_name, old_column, new_column)
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<String> {
        self.index_manager.list_indexes()
    }

    /// List all indexes for a specific table
    pub fn list_indexes_for_table(&self, table_name: &str) -> Vec<String> {
        // Normalize for case-insensitive comparison
        let normalized_search = table_name.to_lowercase();

        self.index_manager
            .list_indexes()
            .into_iter()
            .filter(|index_name| {
                self.index_manager
                    .get_index(index_name)
                    .map(|metadata| {
                        // Normalize both sides for comparison
                        metadata.table_name.to_lowercase() == normalized_search
                    })
                    .unwrap_or(false)
            })
            .collect()
    }

    /// Check if a column has any user-defined index (B-tree or spatial)
    /// Note: Expression indexes are NOT checked here - they don't have named columns
    #[inline]
    pub fn has_index_on_column(&self, table_name: &str, column_name: &str) -> bool {
        let normalized_table = table_name.to_lowercase();
        let normalized_column = column_name.to_lowercase();

        // Check B-tree indexes
        for index_name in self.index_manager.list_indexes() {
            if let Some(metadata) = self.index_manager.get_index(&index_name) {
                if metadata.table_name.to_lowercase() == normalized_table {
                    for col in &metadata.columns {
                        // Use column_name() instead of expect_column_name() to handle
                        // expression indexes gracefully - they return None for column_name
                        if let Some(col_name) = col.column_name() {
                            if col_name.to_lowercase() == normalized_column {
                                return true;
                            }
                        }
                        // Skip expression indexes - they don't have named columns
                    }
                }
            }
        }

        // Check spatial indexes
        for (metadata, _) in self.spatial_indexes.values() {
            if metadata.table_name.to_lowercase() == normalized_table
                && metadata.column_name.to_lowercase() == normalized_column
            {
                return true;
            }
        }

        false
    }

    // ========================================================================
    // Spatial Index Methods
    // ========================================================================

    /// Normalize an index name to lowercase for case-insensitive comparison
    fn normalize_index_name(name: &str) -> String {
        name.to_lowercase()
    }

    /// Resolve a (possibly bare) spatial-index name to the map key it is stored
    /// under, following SQLite name resolution.
    ///
    /// Mirrors the B-tree `IndexManager::resolve_index_key` from #5540: an
    /// explicit `schema.index` form targets exactly that schema's index; an
    /// unqualified lookup prefers a non-`main` (temp) schema index over a
    /// same-named `main` index (temp shadows main), so `DROP INDEX ix` resolves
    /// to `temp.ix` when both exist. See issue #5558.
    fn resolve_spatial_index_key(&self, index_name: &str) -> Option<String> {
        // Explicit schema-qualified form: target exactly that schema's index.
        if let Some((schema_part, name_part)) = index_name.split_once('.') {
            let key = make_spatial_index_key(schema_part, name_part);
            if self.spatial_indexes.contains_key(&key) {
                return Some(key);
            }
            // Fall through: maybe the caller passed a dotted *index name* (rare)
            // rather than a schema qualifier; try the whole thing as a bare name.
        }

        let normalized = Self::normalize_index_name(index_name);

        // Temp schema shadows main: prefer a non-main-schema index with this name.
        if let Some((key, _)) = self.spatial_indexes.iter().find(|(_, (meta, _))| {
            !meta.schema.eq_ignore_ascii_case(vibesql_catalog::DEFAULT_SCHEMA)
                && Self::normalize_index_name(&meta.index_name) == normalized
        }) {
            return Some(key.clone());
        }

        // Otherwise the main-schema (bare) key.
        if self.spatial_indexes.contains_key(&normalized) {
            return Some(normalized);
        }

        None
    }

    /// Create a spatial index
    pub fn create_spatial_index(
        &mut self,
        metadata: SpatialIndexMetadata,
        spatial_index: SpatialIndex,
    ) -> Result<(), StorageError> {
        // Schema-aware storage key (#5558): bare for `main`, `schema.name`
        // otherwise — so a temp-table spatial index and a same-named main-table
        // spatial index coexist as distinct entries.
        let key = make_spatial_index_key(&metadata.schema, &metadata.index_name);

        // Collision check is scoped to the owning schema: an existing B-tree or
        // spatial index *in the same schema* blocks creation, but a same-named
        // index in another schema does not.
        if self.index_manager.index_exists(&key) {
            return Err(StorageError::IndexAlreadyExists(metadata.index_name.clone()));
        }
        if self.spatial_indexes.contains_key(&key) {
            return Err(StorageError::IndexAlreadyExists(metadata.index_name.clone()));
        }

        self.spatial_indexes.insert(key, (metadata, spatial_index));
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
            table_name.to_lowercase()
        };

        // Try to find the table with normalized name or qualified name.
        //
        // For an unqualified name, follow SQLite name resolution: the session
        // temp schema shadows `main`. A TEMP table is stored under the
        // `temp_<id>.<table>` physical key, so an unqualified index target that
        // names a temp table must be looked up there before falling back to the
        // current (main) schema. Previously only the current schema was tried,
        // so CREATE INDEX on a temp table failed with `TableNotFound`. See #5505.
        let table = if let Some(tbl) = tables.get(&normalized_name) {
            tbl
        } else if !table_name.contains('.') {
            // Temp schema first (temp tables shadow main).
            let temp_qualified = format!("{}.{}", catalog.temp_schema_name(), normalized_name);
            if let Some(tbl) = tables.get(&temp_qualified) {
                tbl
            } else {
                let current_schema = catalog.get_current_schema();
                let qualified_name = format!("{}.{}", current_schema, normalized_name);
                if let Some(tbl) = tables.get(&qualified_name) {
                    tbl
                } else {
                    // Attached databases in attachment order (SQLite searches
                    // temp, then main, then each ATTACHed database — #6310).
                    catalog
                        .attached_databases()
                        .iter()
                        .find_map(|attached| {
                            tables.get(&format!("{}.{}", attached.name, normalized_name))
                        })
                        .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?
                }
            }
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
            table_name.to_lowercase()
        };

        // Try to find the table with normalized name or qualified name.
        //
        // For an unqualified name, follow SQLite name resolution: the session
        // temp schema shadows `main`. A TEMP table is stored under the
        // `temp_<id>.<table>` physical key, so an unqualified index target that
        // names a temp table must be looked up there before falling back to the
        // current (main) schema. Previously only the current schema was tried,
        // so CREATE INDEX on a temp table failed with `TableNotFound`. See #5505.
        let table = if let Some(tbl) = tables.get(&normalized_name) {
            tbl
        } else if !table_name.contains('.') {
            // Temp schema first (temp tables shadow main).
            let temp_qualified = format!("{}.{}", catalog.temp_schema_name(), normalized_name);
            if let Some(tbl) = tables.get(&temp_qualified) {
                tbl
            } else {
                let current_schema = catalog.get_current_schema();
                let qualified_name = format!("{}.{}", current_schema, normalized_name);
                if let Some(tbl) = tables.get(&qualified_name) {
                    tbl
                } else {
                    // Attached databases in attachment order (SQLite searches
                    // temp, then main, then each ATTACHed database — #6310).
                    catalog
                        .attached_databases()
                        .iter()
                        .find_map(|attached| {
                            tables.get(&format!("{}.{}", attached.name, normalized_name))
                        })
                        .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?
                }
            }
        } else {
            return Err(StorageError::TableNotFound(table_name.clone()));
        };

        // Extract vectors from the table
        // Note: SqlValue::Vector stores f32, but HNSW uses f64 for precision
        let rows = table.scan();
        let total_rows = rows.len();
        let mut vectors: Vec<(usize, Vec<f64>)> = Vec::new();
        let mut progress =
            ProgressTracker::new(format!("Creating HNSW index '{}'", index_name), Some(total_rows));
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
        self.resolve_spatial_index_key(index_name).is_some()
    }

    /// Get spatial index metadata
    pub fn get_spatial_index_metadata(&self, index_name: &str) -> Option<&SpatialIndexMetadata> {
        let key = self.resolve_spatial_index_key(index_name)?;
        self.spatial_indexes.get(&key).map(|(metadata, _)| metadata)
    }

    /// Get spatial index (immutable)
    pub fn get_spatial_index(&self, index_name: &str) -> Option<&SpatialIndex> {
        let key = self.resolve_spatial_index_key(index_name)?;
        self.spatial_indexes.get(&key).map(|(_, index)| index)
    }

    /// Get spatial index (mutable)
    pub fn get_spatial_index_mut(&mut self, index_name: &str) -> Option<&mut SpatialIndex> {
        let key = self.resolve_spatial_index_key(index_name)?;
        self.spatial_indexes.get_mut(&key).map(|(_, index)| index)
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
        // Resolve schema-aware (#5558): an unqualified name drops the temp index
        // first (temp shadows main); an explicit `schema.index` targets exactly.
        let Some(key) = self.resolve_spatial_index_key(index_name) else {
            return Err(StorageError::IndexNotFound(index_name.to_string()));
        };

        if self.spatial_indexes.remove(&key).is_none() {
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
        let search_name_lower = table_name.to_lowercase();

        // Extract just the table name part if qualified (e.g., "public.users" -> "users")
        let search_table_only = search_name_lower.rsplit('.').next().unwrap_or(&search_name_lower);

        let indexes_to_drop: Vec<String> = self
            .spatial_indexes
            .iter()
            .filter(|(_, (metadata, _))| {
                let stored_lower = metadata.table_name.to_lowercase();
                let stored_table_only = stored_lower.rsplit('.').next().unwrap_or(&stored_lower);

                // Match if full names match OR unqualified parts match
                stored_lower == search_name_lower || stored_table_only == search_table_only
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
    /// This is significantly more efficient than calling
    /// `update_spatial_indexes_for_delete_with_values` in a loop because it pre-computes column
    /// indices once per index rather than once per row.
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

    // ========================================================================
    // Disk-backed index transaction undo-logging (issue #5425)
    // ========================================================================

    /// Arm transaction undo-logging on all disk-backed (spilled) indexes.
    ///
    /// Disk-backed indexes are not undone by the copy-on-write `Operations`
    /// rollback snapshot (the snapshot shares the same `Arc<Mutex<BTreeIndex>>`),
    /// so a per-tree undo-log is recorded for the duration of a mutating
    /// transaction. See [`super::indexes::IndexManager::begin_disk_undo_logging`].
    pub fn begin_disk_undo_logging(&mut self) {
        self.index_manager.begin_disk_undo_logging();
    }

    /// Reverse the disk-backed index undo-logs (ROLLBACK path), restoring each
    /// spilled index to its pre-transaction state.
    pub fn rollback_disk_undo_logs(&mut self) {
        self.index_manager.rollback_disk_undo_logs();
    }

    /// Discard the disk-backed index undo-logs (COMMIT path — already persisted).
    pub fn clear_disk_undo_logs(&mut self) {
        self.index_manager.clear_disk_undo_logs();
    }

    /// Capture per-tree disk undo-log markers for a statement-level savepoint
    /// (issue #5434). See [`super::indexes::IndexManager::mark_disk_undo_logs`].
    pub fn mark_disk_undo_logs(&self) -> HashMap<String, usize> {
        self.index_manager.mark_disk_undo_logs()
    }

    /// Reverse the disk-backed index undo-log suffix recorded since the given
    /// statement-savepoint markers (issue #5434 — `RAISE(ABORT)` scope),
    /// leaving the enclosing transaction's undo-log intact. See
    /// [`super::indexes::IndexManager::rollback_disk_undo_logs_to`].
    pub fn rollback_disk_undo_logs_to(&mut self, markers: &HashMap<String, usize>) {
        self.index_manager.rollback_disk_undo_logs_to(markers);
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

#[cfg(test)]
mod spatial_schema_tests {
    use super::*;
    use crate::index::SpatialIndex;

    fn meta(index: &str, table: &str, schema: &str) -> SpatialIndexMetadata {
        SpatialIndexMetadata {
            index_name: index.to_string(),
            table_name: table.to_string(),
            column_name: "g".to_string(),
            schema: schema.to_string(),
            created_at: None,
        }
    }

    /// #5558: a same-named spatial index in `main` and in a temp schema coexist
    /// as distinct storage entries, mirroring the B-tree behavior from #5540.
    #[test]
    fn same_named_spatial_index_across_schemas_coexist() {
        let mut ops = Operations::new();

        ops.create_spatial_index(meta("ix", "t", "main"), SpatialIndex::new("g".to_string()))
            .expect("create main.ix");
        // Same bare name on a temp-schema table must NOT collide with main.ix.
        ops.create_spatial_index(
            meta("ix", "t", "temp_42"),
            SpatialIndex::new("g".to_string()),
        )
        .expect("create temp_42.ix should not collide with main.ix");

        // Both keys are present: bare `ix` (main) and `temp_42.ix` (temp).
        let keys = ops.list_spatial_indexes();
        assert!(keys.contains(&"ix".to_string()), "main.ix keyed bare: {keys:?}");
        assert!(keys.contains(&"temp_42.ix".to_string()), "temp.ix keyed qualified: {keys:?}");

        // Explicit schema-qualified lookups target each one precisely.
        assert_eq!(ops.get_spatial_index_metadata("main.ix").unwrap().schema, "main");
        assert_eq!(ops.get_spatial_index_metadata("temp_42.ix").unwrap().schema, "temp_42");
    }

    /// An unqualified lookup/drop resolves temp-shadows-main (#5558), matching
    /// the B-tree `resolve_index_key` semantics.
    #[test]
    fn unqualified_spatial_lookup_and_drop_resolves_temp_first() {
        let mut ops = Operations::new();
        ops.create_spatial_index(meta("ix", "t", "main"), SpatialIndex::new("g".to_string()))
            .unwrap();
        ops.create_spatial_index(
            meta("ix", "t", "temp_42"),
            SpatialIndex::new("g".to_string()),
        )
        .unwrap();

        // Bare name resolves to the temp index (temp shadows main).
        assert_eq!(ops.get_spatial_index_metadata("ix").unwrap().schema, "temp_42");

        // Unqualified DROP removes the temp index first; main.ix survives.
        ops.drop_spatial_index("ix").expect("drop resolves temp.ix");
        assert!(!ops.spatial_index_exists("temp_42.ix"), "temp.ix dropped");
        assert!(ops.spatial_index_exists("main.ix"), "main.ix survives");
        assert_eq!(ops.get_spatial_index_metadata("ix").unwrap().schema, "main");

        // A second unqualified DROP now removes main.ix.
        ops.drop_spatial_index("ix").expect("drop resolves main.ix");
        assert!(!ops.spatial_index_exists("ix"));
        assert!(ops.list_spatial_indexes().is_empty());
    }

    /// Main-schema spatial indexes keep their bare key (backward compatible).
    #[test]
    fn main_schema_spatial_index_uses_bare_key() {
        let mut ops = Operations::new();
        ops.create_spatial_index(meta("loc", "places", "main"), SpatialIndex::new("g".to_string()))
            .unwrap();
        assert_eq!(ops.list_spatial_indexes(), vec!["loc".to_string()]);
        assert!(ops.spatial_index_exists("loc"));
        assert!(ops.spatial_index_exists("main.loc"));
    }
}
