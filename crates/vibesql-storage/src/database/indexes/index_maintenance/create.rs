// ============================================================================
// Index Creation and Drop Operations
// ============================================================================

use std::{collections::BTreeMap, sync::Arc};

use vibesql_types::{DataType, SqlValue};

use super::prefix::apply_prefix_truncation;
use crate::{
    btree::{BTreeIndex, Key},
    database::indexes::{
        hnsw::HnswIndex,
        index_manager::IndexManager,
        index_metadata::{
            make_index_key, normalize_index_name, split_schema_qualifier, IndexData, IndexMetadata,
            DEFAULT_INDEX_SCHEMA, DISK_BACKED_THRESHOLD,
        },
        ivfflat::IVFFlatIndex,
    },
    page::PageManager,
    progress::ProgressTracker,
    Row, StorageError,
};

impl IndexManager {
    /// Create an index
    ///
    /// The optional `included_row_indices` parameter restricts the set of
    /// table rows that get inserted into the index body. This is the
    /// build-time hook for partial indexes (`CREATE INDEX ... WHERE expr`):
    /// the executor evaluates the predicate against each row and passes the
    /// set of row indices whose predicate is truthy. When `None`, every row
    /// from `table_rows` is indexed (the full-index path).
    #[allow(clippy::too_many_arguments)]
    pub fn create_index(
        &mut self,
        index_name: String,
        table_name: String,
        schema: &str,
        table_schema: &vibesql_catalog::TableSchema,
        table_rows: &[Row],
        unique: bool,
        columns: Vec<vibesql_ast::IndexColumn>,
        where_clause: Option<Box<vibesql_ast::Expression>>,
        included_row_indices: Option<&std::collections::HashSet<usize>>,
    ) -> Result<(), StorageError> {
        // Schema-qualified storage key (#5540): a temp index and a main index can
        // share a bare name; only the owning schema disambiguates them. A
        // main-schema index keeps a bare key for backward compatibility.
        let normalized_name = make_index_key(schema, &index_name);

        // Check if index already exists (per-schema namespace)
        if self.indexes.contains_key(&normalized_name) {
            return Err(StorageError::IndexAlreadyExists(index_name));
        }

        // Expression (functional) indexes cannot be rebuilt through this
        // column-oriented path: an expression column has no single source
        // column, so `expect_column_name()` on it would panic. This path is
        // reached when a persisted expression index is reloaded from a binary
        // or JSON snapshot (the executor's CREATE INDEX path uses
        // `create_index_with_keys` after evaluating the expressions, and never
        // calls this method for an expression index). Rather than panicking,
        // register the index metadata with an **empty body** and mark it
        // pending-rebuild.
        //
        // The empty body is NOT correct for reads (the binary format persists
        // table rows only, not index bodies — see `persistence/binary/data.rs`
        // — so nothing repopulates it on load). Expression indexes ARE a
        // first-class selectable read path (`select/scan/index_scan`,
        // `optimizer/index_planner::can_use_index`), so consulting an empty
        // body would silently return zero rows. To prevent that:
        //   1. The index is recorded in `pending_expression_rebuilds`, and the query planner
        //      declines any index for which `is_index_pending_rebuild` is true — falling back to a
        //      full-table scan (correct results, just slower).
        //   2. `rebuild_pending_expression_indexes` in the executor evaluates the index expression
        //      over the table rows and repopulates the body via `populate_expression_index`,
        //      clearing the flag so the index becomes a fully functional (and used) read path
        //      again.
        // The CLI open path runs the rebuild after every snapshot load, so a
        // reopened database keeps its expression indexes functional. Keeping the
        // metadata also means the catalog and ALTER/DROP-column validations
        // still see the index. See issue #5784.
        if columns.iter().any(|c| c.is_expression()) {
            let metadata = IndexMetadata {
                index_name: index_name.clone(),
                table_name: table_name.clone(),
                schema: schema.to_string(),
                unique,
                columns,
                where_clause,
            };
            self.insert_index(normalized_name.clone(), metadata);
            self.resource_tracker.register_index(
                normalized_name.clone(),
                0,
                0,
                crate::database::IndexBackend::InMemory,
            );
            self.index_data
                .insert(normalized_name.clone(), IndexData::InMemory { data: BTreeMap::new() });
            self.pending_expression_rebuilds.insert(normalized_name);
            return Ok(());
        }

        // Get column indices in the table for all indexed columns
        let mut column_indices = Vec::new();
        for index_col in &columns {
            let column_idx = table_schema
                .get_column_index(index_col.expect_column_name())
                .ok_or_else(|| StorageError::ColumnNotFound {
                    column_name: index_col.expect_column_name().to_string(),
                    table_name: table_name.clone(),
                })?;
            column_indices.push(column_idx);
        }

        // Store index metadata (keyed by the schema-qualified storage key)
        let metadata = IndexMetadata {
            index_name: index_name.clone(),
            table_name: table_name.clone(),
            schema: schema.to_string(),
            unique,
            columns: columns.clone(),
            where_clause,
        };

        self.insert_index(normalized_name.clone(), metadata);

        // Choose backend based on table size
        // In test builds, DISK_BACKED_THRESHOLD is usize::MAX to disable disk-backed indexes
        #[allow(clippy::absurd_extreme_comparisons)]
        let use_disk_backed = table_rows.len() >= DISK_BACKED_THRESHOLD;

        let (index_data, memory_bytes, disk_bytes, backend) = if use_disk_backed {
            // Create disk-backed B+ tree index using proper database path
            let index_file = self.get_index_file_path(&table_name, &index_name)?;
            let index_file_str = index_file
                .to_str()
                .ok_or_else(|| StorageError::IoError("Invalid index file path".to_string()))?;

            let page_manager =
                Arc::new(PageManager::new(index_file_str, self.storage.clone()).map_err(|e| {
                    StorageError::IoError(format!("Failed to create index file: {}", e))
                })?);

            // Build key schema from indexed columns
            let key_schema: Vec<DataType> = column_indices
                .iter()
                .map(|&idx| table_schema.columns[idx].data_type.clone())
                .collect();

            // Prepare sorted entries for bulk loading
            // The BTreeIndex has native duplicate key support via Vec<RowId> per key,
            // so we don't need to extend keys with row_id for non-unique indexes
            //
            // When `included_row_indices` is Some, only rows whose row_idx is in
            // the set are added to the index body (partial-index build path).
            let mut sorted_entries: Vec<(Key, usize)> = Vec::new();
            let mut progress = ProgressTracker::new(
                format!("Creating index '{}'", index_name),
                Some(table_rows.len()),
            );
            for (row_idx, row) in table_rows.iter().enumerate() {
                if let Some(included) = included_row_indices {
                    if !included.contains(&row_idx) {
                        progress.update(row_idx + 1);
                        continue;
                    }
                }
                let key_values: Vec<SqlValue> = column_indices
                    .iter()
                    .zip(columns.iter())
                    .map(|(&idx, col)| {
                        let value = &row.values[idx];
                        let truncated = apply_prefix_truncation(value, col.prefix_length());
                        // Normalize numeric types to ensure consistent comparison with query bounds
                        crate::database::indexes::index_operations::normalize_for_comparison(
                            &truncated,
                        )
                    })
                    .collect();
                sorted_entries.push((key_values, row_idx));
                progress.update(row_idx + 1);
            }
            progress.finish();
            // Sort by key for bulk_load
            sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

            // Use the same key schema for both unique and non-unique indexes
            // The BTreeIndex handles duplicates internally via Vec<RowId>
            let btree_key_schema = key_schema;

            // Use bulk_load for efficient index creation
            let btree =
                BTreeIndex::bulk_load(sorted_entries, btree_key_schema, page_manager.clone())
                    .map_err(|e| {
                        StorageError::IoError(format!("Failed to bulk load index: {}", e))
                    })?;

            // Calculate disk size
            let disk_bytes = if let Ok(file_meta) = std::fs::metadata(&index_file) {
                file_meta.len() as usize
            } else {
                0
            };

            #[cfg(not(target_arch = "wasm32"))]
            let data = IndexData::DiskBacked {
                btree: Arc::new(parking_lot::Mutex::new(btree)),
                page_manager,
            };

            #[cfg(target_arch = "wasm32")]
            let data = IndexData::DiskBacked {
                btree: Arc::new(std::sync::Mutex::new(btree)),
                page_manager,
            };

            (data, 0, disk_bytes, crate::database::IndexBackend::DiskBacked)
        } else {
            // Build the index data in-memory using bulk-load optimization
            // This is significantly faster than incremental BTreeMap insertion for large tables
            // because sorted insertion has better cache locality and fewer tree rebalances
            let mut progress = ProgressTracker::new(
                format!("Creating index '{}'", index_name),
                Some(table_rows.len()),
            );

            // Phase 1: Extract all (key, row_idx) pairs
            //
            // For partial indexes, skip rows not in `included_row_indices`
            // so that the in-memory body only carries matching rows.
            let mut entries: Vec<(Vec<SqlValue>, usize)> = Vec::with_capacity(table_rows.len());
            for (row_idx, row) in table_rows.iter().enumerate() {
                if let Some(included) = included_row_indices {
                    if !included.contains(&row_idx) {
                        progress.update(row_idx + 1);
                        continue;
                    }
                }
                let key_values: Vec<SqlValue> = column_indices
                    .iter()
                    .zip(columns.iter())
                    .map(|(&idx, col)| {
                        let value = &row.values[idx];
                        let truncated = apply_prefix_truncation(value, col.prefix_length());
                        // Normalize numeric types to ensure consistent comparison with query bounds
                        crate::database::indexes::index_operations::normalize_for_comparison(
                            &truncated,
                        )
                    })
                    .collect();
                entries.push((key_values, row_idx));
                progress.update(row_idx + 1);
            }

            // Phase 2: Sort by key for optimal BTreeMap construction
            entries.sort_by(|a, b| a.0.cmp(&b.0));

            // Phase 3: Group entries by key and build BTreeMap
            // Using sorted iteration results in more balanced tree construction
            let mut index_data_map: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
            for (key, row_idx) in entries {
                index_data_map.entry(key).or_default().push(row_idx);
            }
            progress.finish();

            // Estimate memory usage
            let key_size = std::mem::size_of::<Vec<SqlValue>>(); // Rough estimate
            let memory_bytes = self.estimate_index_memory(table_rows.len(), key_size);

            let data = IndexData::InMemory { data: index_data_map };

            (data, memory_bytes, 0, crate::database::IndexBackend::InMemory)
        };

        // Register the index with resource tracker
        self.resource_tracker.register_index(
            normalized_name.clone(),
            memory_bytes,
            disk_bytes,
            backend,
        );

        self.index_data.insert(normalized_name.clone(), index_data);

        // Enforce memory budget after creating index
        self.enforce_memory_budget()?;

        Ok(())
    }

    /// Create an index with pre-computed keys (for expression indexes)
    ///
    /// This method is used when the caller has already evaluated the expressions
    /// and computed the key values for each row. This is necessary for expression
    /// indexes where the key values are derived from evaluating expressions on rows.
    pub fn create_index_with_keys(
        &mut self,
        index_name: String,
        table_name: String,
        schema: &str,
        _table_schema: &vibesql_catalog::TableSchema,
        unique: bool,
        columns: Vec<vibesql_ast::IndexColumn>,
        keys: Vec<(Vec<SqlValue>, usize)>,
    ) -> Result<(), StorageError> {
        use std::collections::BTreeMap;

        // Schema-qualified storage key (#5540).
        let normalized_name = make_index_key(schema, &index_name);

        // Check if index already exists (per-schema namespace)
        if self.indexes.contains_key(&normalized_name) {
            return Err(StorageError::IndexAlreadyExists(index_name));
        }

        // Store index metadata (keyed by the schema-qualified storage key)
        // Expression indexes do not currently use partial-index semantics; the
        // caller is responsible for filtering rows via `create_index_with_keys`'s
        // `keys` parameter (so passing only matching rows is the equivalent of
        // applying a partial-index predicate at build time).
        let metadata = IndexMetadata {
            index_name: index_name.clone(),
            table_name: table_name.clone(),
            schema: schema.to_string(),
            unique,
            columns: columns.clone(),
            where_clause: None,
        };

        self.insert_index(normalized_name.clone(), metadata);

        // For expression indexes, always use in-memory storage for now
        // (disk-backed expression index support can be added later)
        let mut entries: Vec<(Key, usize)> = Vec::new();

        for (key_values, row_idx) in keys {
            // Normalize key values for consistent comparison
            let normalized_key: Vec<SqlValue> = key_values
                .iter()
                .map(|v| crate::database::indexes::index_operations::normalize_for_comparison(v))
                .collect();
            entries.push((normalized_key, row_idx));
        }

        // Sort by key for optimal BTreeMap construction
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Group entries by key and build BTreeMap
        let mut index_data_map: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
        for (key, row_idx) in entries {
            index_data_map.entry(key).or_default().push(row_idx);
        }

        // Estimate memory usage
        let key_size = std::mem::size_of::<Vec<SqlValue>>(); // Rough estimate
        let memory_bytes = self.estimate_index_memory(index_data_map.len(), key_size);

        let index_data = IndexData::InMemory { data: index_data_map };

        // Register the index with resource tracker
        self.resource_tracker.register_index(
            normalized_name.clone(),
            memory_bytes,
            0, // No disk usage for in-memory
            crate::database::IndexBackend::InMemory,
        );

        self.index_data.insert(normalized_name.clone(), index_data);

        // Enforce memory budget after creating index
        self.enforce_memory_budget()?;

        Ok(())
    }

    // ============================================================================
    // IVFFlat Index Creation
    // ============================================================================

    /// Create an IVFFlat index for approximate nearest neighbor search on vector columns
    ///
    /// This method creates an IVFFlat (Inverted File with Flat quantization) index
    /// for efficient approximate nearest neighbor search on vector data.
    ///
    /// # Arguments
    /// * `index_name` - Name for the new index
    /// * `table_name` - Name of the table containing the vector column
    /// * `table_schema` - Schema of the table
    /// * `table_rows` - Current rows in the table
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
        // Normalize index name for case-insensitive comparison
        let normalized_name = normalize_index_name(&index_name);

        // Check if index already exists
        if self.indexes.contains_key(&normalized_name) {
            return Err(StorageError::IndexAlreadyExists(index_name));
        }

        // Create IVFFlat index
        let mut ivfflat = IVFFlatIndex::new(dimensions, lists as u32, metric);

        // Extract vectors from table rows
        let mut vectors: Vec<(usize, Vec<f64>)> = Vec::new();
        for (row_idx, row) in self.get_table_rows_for_ivfflat(&table_name)?.iter().enumerate() {
            if col_idx < row.values.len() {
                if let Some(vec_data) = Self::extract_vector(&row.values[col_idx]) {
                    vectors.push((row_idx, vec_data));
                }
            }
        }

        // Save count before moving
        let vector_count = vectors.len();

        // Build the index using k-means clustering
        ivfflat
            .build(vectors)
            .map_err(|e| StorageError::IoError(format!("Failed to build IVFFlat index: {}", e)))?;

        // Store index metadata
        let metadata = IndexMetadata {
            index_name: index_name.clone(),
            table_name: table_name.clone(),
            schema: DEFAULT_INDEX_SCHEMA.to_string(),
            unique: false, // IVFFlat indexes are never unique
            columns: vec![vibesql_ast::IndexColumn::Column {
                column_name,
                direction: vibesql_ast::OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
        };

        self.insert_index(normalized_name.clone(), metadata);

        // Store index data
        self.index_data.insert(normalized_name.clone(), IndexData::IVFFlat { index: ivfflat });

        // Register with resource tracker (estimate memory based on vector count and dimensions)
        let estimated_memory = vector_count * dimensions * std::mem::size_of::<f64>() * 2; // vectors + centroids
        self.resource_tracker.register_index(
            normalized_name,
            estimated_memory,
            0,
            crate::database::IndexBackend::InMemory,
        );

        Ok(())
    }

    /// Helper method to get table rows for IVFFlat index building
    /// Note: This is a temporary solution - the actual rows should be passed from the caller
    fn get_table_rows_for_ivfflat(&self, _table_name: &str) -> Result<Vec<Row>, StorageError> {
        // This method shouldn't be called - rows should be extracted by the caller
        // Return empty for now; the actual implementation passes rows directly
        Ok(Vec::new())
    }

    /// Create an IVFFlat index with pre-extracted vectors
    ///
    /// This is the main entry point for creating IVFFlat indexes when the
    /// table rows have already been accessed by the caller (executor layer).
    #[allow(clippy::too_many_arguments)]
    pub fn create_ivfflat_index_with_vectors(
        &mut self,
        index_name: String,
        table_name: String,
        column_name: String,
        dimensions: usize,
        lists: usize,
        metric: vibesql_ast::VectorDistanceMetric,
        vectors: Vec<(usize, Vec<f64>)>,
    ) -> Result<(), StorageError> {
        // Normalize index name for case-insensitive comparison
        let normalized_name = normalize_index_name(&index_name);

        // Check if index already exists
        if self.indexes.contains_key(&normalized_name) {
            return Err(StorageError::IndexAlreadyExists(index_name));
        }

        // Create IVFFlat index
        let mut ivfflat = IVFFlatIndex::new(dimensions, lists as u32, metric);

        let vector_count = vectors.len();

        // Build the index using k-means clustering
        ivfflat
            .build(vectors)
            .map_err(|e| StorageError::IoError(format!("Failed to build IVFFlat index: {}", e)))?;

        // Store index metadata
        let metadata = IndexMetadata {
            index_name: index_name.clone(),
            table_name: table_name.clone(),
            schema: DEFAULT_INDEX_SCHEMA.to_string(),
            unique: false, // IVFFlat indexes are never unique
            columns: vec![vibesql_ast::IndexColumn::Column {
                column_name,
                direction: vibesql_ast::OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
        };

        self.insert_index(normalized_name.clone(), metadata);

        // Store index data
        self.index_data.insert(normalized_name.clone(), IndexData::IVFFlat { index: ivfflat });

        // Register with resource tracker (estimate memory based on vector count and dimensions)
        let estimated_memory = vector_count * dimensions * std::mem::size_of::<f64>() * 2; // vectors + centroids
        self.resource_tracker.register_index(
            normalized_name,
            estimated_memory,
            0,
            crate::database::IndexBackend::InMemory,
        );

        Ok(())
    }

    /// Resolve the table-column index of a vector index's single indexed column.
    ///
    /// IVFFlat / HNSW indexes always index exactly one (non-expression) column.
    /// Returns `None` (with a warning) if the metadata or schema can't be
    /// resolved, so callers can skip per-row maintenance for that index rather
    /// than panic.
    pub(crate) fn vector_index_column_idx(
        metadata: &IndexMetadata,
        table_schema: &vibesql_catalog::TableSchema,
        index_name: &str,
    ) -> Option<usize> {
        let col = metadata.columns.first()?;
        let col_name = col.column_name()?;
        match table_schema.get_column_index(col_name) {
            Some(idx) => Some(idx),
            None => {
                log::warn!(
                    "Vector index '{}' column '{}' not found in schema; skipping per-row maintenance",
                    index_name,
                    col_name
                );
                None
            }
        }
    }

    /// Extract a vector from a SqlValue, converting f32 to f64
    ///
    /// Note: SqlValue::Vector stores f32 for storage efficiency,
    /// but IVFFlat uses f64 for precision in k-means clustering.
    pub(crate) fn extract_vector(value: &vibesql_types::SqlValue) -> Option<Vec<f64>> {
        match value {
            vibesql_types::SqlValue::Vector(data) => {
                // Convert f32 vector to f64 for IVFFlat processing
                Some(data.iter().map(|&v| v as f64).collect())
            }
            vibesql_types::SqlValue::Null => None,
            _ => None,
        }
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
        let normalized_name = normalize_index_name(index_name);

        let index_data = self
            .index_data
            .get(&normalized_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        match index_data {
            IndexData::IVFFlat { index } => index
                .search(query_vector, k)
                .map_err(|e| StorageError::Other(format!("IVFFlat search error: {}", e))),
            _ => {
                Err(StorageError::Other(format!("Index '{}' is not an IVFFlat index", index_name)))
            }
        }
    }

    /// Get all IVFFlat indexes for a specific table
    ///
    /// Returns index metadata and access to search for each IVFFlat index on the table.
    pub fn get_ivfflat_indexes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<(&IndexMetadata, &IVFFlatIndex)> {
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

                // Check if it's an IVFFlat index
                if let Some(IndexData::IVFFlat { index }) = self.index_data.get(normalized_name) {
                    Some((metadata, index))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Set the number of probes for an IVFFlat index
    ///
    /// Probes controls how many clusters are searched during a query.
    /// Higher values improve recall but increase search time.
    pub fn set_ivfflat_probes(
        &mut self,
        index_name: &str,
        probes: usize,
    ) -> Result<(), StorageError> {
        let normalized_name = normalize_index_name(index_name);

        let index_data = self
            .index_data
            .get_mut(&normalized_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        match index_data {
            IndexData::IVFFlat { index } => {
                index.set_probes(probes);
                Ok(())
            }
            _ => {
                Err(StorageError::Other(format!("Index '{}' is not an IVFFlat index", index_name)))
            }
        }
    }

    // ============================================================================
    // HNSW Index Creation
    // ============================================================================

    /// Create an HNSW index with pre-extracted vectors
    ///
    /// This is the main entry point for creating HNSW indexes when the
    /// table rows have already been accessed by the caller (executor layer).
    #[allow(clippy::too_many_arguments)]
    pub fn create_hnsw_index_with_vectors(
        &mut self,
        index_name: String,
        table_name: String,
        column_name: String,
        dimensions: usize,
        m: u32,
        ef_construction: u32,
        metric: vibesql_ast::VectorDistanceMetric,
        vectors: Vec<(usize, Vec<f64>)>,
    ) -> Result<(), StorageError> {
        // Normalize index name for case-insensitive comparison
        let normalized_name = normalize_index_name(&index_name);

        // Check if index already exists
        if self.indexes.contains_key(&normalized_name) {
            return Err(StorageError::IndexAlreadyExists(index_name));
        }

        // Create HNSW index
        let mut hnsw = HnswIndex::new(dimensions, m, ef_construction, metric);

        let vector_count = vectors.len();

        // Build the index
        hnsw.build(vectors)
            .map_err(|e| StorageError::IoError(format!("Failed to build HNSW index: {}", e)))?;

        // Store index metadata
        let metadata = IndexMetadata {
            index_name: index_name.clone(),
            table_name: table_name.clone(),
            schema: DEFAULT_INDEX_SCHEMA.to_string(),
            unique: false, // HNSW indexes are never unique
            columns: vec![vibesql_ast::IndexColumn::Column {
                column_name,
                direction: vibesql_ast::OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
        };

        self.insert_index(normalized_name.clone(), metadata);

        // Store index data
        self.index_data.insert(normalized_name.clone(), IndexData::Hnsw { index: hnsw });

        // Register with resource tracker (estimate memory based on vector count and dimensions)
        // HNSW has more overhead due to graph structure: ~m*2 neighbors per node
        let estimated_memory = vector_count
            * (dimensions * std::mem::size_of::<f64>()
                + m as usize * 2 * std::mem::size_of::<usize>());
        self.resource_tracker.register_index(
            normalized_name,
            estimated_memory,
            0,
            crate::database::IndexBackend::InMemory,
        );

        Ok(())
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
        let normalized_name = normalize_index_name(index_name);

        let index_data = self
            .index_data
            .get(&normalized_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        match index_data {
            IndexData::Hnsw { index } => index
                .search(query_vector, k)
                .map_err(|e| StorageError::Other(format!("HNSW search error: {}", e))),
            _ => Err(StorageError::Other(format!("Index '{}' is not an HNSW index", index_name))),
        }
    }

    /// Get all HNSW indexes for a specific table
    ///
    /// Returns index metadata and access to search for each HNSW index on the table.
    pub fn get_hnsw_indexes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<(&IndexMetadata, &HnswIndex)> {
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

                // Check if it's an HNSW index
                if let Some(IndexData::Hnsw { index }) = self.index_data.get(normalized_name) {
                    Some((metadata, index))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Set the ef_search parameter for an HNSW index
    ///
    /// ef_search controls the search accuracy/speed tradeoff.
    /// Higher values improve recall but increase search time.
    pub fn set_hnsw_ef_search(
        &mut self,
        index_name: &str,
        ef_search: usize,
    ) -> Result<(), StorageError> {
        let normalized_name = normalize_index_name(index_name);

        let index_data = self
            .index_data
            .get_mut(&normalized_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        match index_data {
            IndexData::Hnsw { index } => {
                index.set_ef_search(ef_search);
                Ok(())
            }
            _ => Err(StorageError::Other(format!("Index '{}' is not an HNSW index", index_name))),
        }
    }

    // ============================================================================
    // Drop Index Operations
    // ============================================================================

    /// Drop an index.
    ///
    /// `index_name` may be a bare name (resolved temp-shadows-main, #5540) or an
    /// explicit `schema.index` qualifier for exact targeting.
    pub fn drop_index(&mut self, index_name: &str) -> Result<(), StorageError> {
        // Resolve the schema-qualified storage key (temp shadows main).
        let normalized = self
            .resolve_index_key(index_name)
            .ok_or_else(|| StorageError::IndexNotFound(index_name.to_string()))?;

        if self.remove_index(&normalized).is_none() {
            return Err(StorageError::IndexNotFound(index_name.to_string()));
        }
        // Also remove the index data
        self.index_data.remove(&normalized);
        self.pending_expression_rebuilds.remove(&normalized);

        // Unregister from resource tracker
        self.resource_tracker.unregister_index(&normalized);

        Ok(())
    }

    /// Drop all indexes associated with a table (CASCADE behavior)
    ///
    /// This is called automatically when dropping a table to maintain
    /// referential integrity. Indexes are tied to specific tables and
    /// cannot exist without their parent table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The table name, which may be qualified (e.g., "public.users") or
    ///   unqualified (e.g., "users"). Matching is case-insensitive and handles both qualified and
    ///   unqualified names.
    ///
    /// # Returns
    ///
    /// Vector of index names that were dropped (for logging/debugging)
    pub fn drop_indexes_for_table(&mut self, table_name: &str) -> Vec<String> {
        // Normalize for case-insensitive comparison
        let search_name_lower = table_name.to_lowercase();

        // Extract just the table name part if qualified (e.g., "public.users" -> "users")
        let search_table_only = search_name_lower.rsplit('.').next().unwrap_or(&search_name_lower);

        // Collect index names to drop (can't modify while iterating)
        // Match if:
        // 1. Exact match (case-insensitive), OR
        // 2. Index's unqualified table name matches our unqualified search name
        let indexes_to_drop: Vec<String> = self
            .indexes
            .iter()
            .filter(|(_, metadata)| {
                let stored_lower = metadata.table_name.to_lowercase();
                let stored_table_only = stored_lower.rsplit('.').next().unwrap_or(&stored_lower);

                // Match if full names match OR unqualified parts match
                stored_lower == search_name_lower || stored_table_only == search_table_only
            })
            .map(|(name, _)| name.clone())
            .collect();

        // Drop each index
        for index_name in &indexes_to_drop {
            self.remove_index(index_name);
            self.index_data.remove(index_name);
            self.pending_expression_rebuilds.remove(index_name);

            // Unregister from resource tracker
            self.resource_tracker.unregister_index(index_name);
        }

        indexes_to_drop
    }

    /// Detach and return the indexes belonging to `table_name`, together with
    /// their physical bodies, WITHOUT discarding the data or touching
    /// resource-tracker stats (unlike [`Self::drop_indexes_for_table`]).
    ///
    /// `ALTER TABLE ... RENAME TO ...` is implemented as drop-old +
    /// create-new (see `execute_rename_table` in `vibesql-executor`).
    /// Calling [`Self::drop_indexes_for_table`] for that CASCADE would
    /// permanently discard every index (including `UNIQUE` indexes) on the
    /// renamed table — the physical body has no other owner and nothing
    /// re-creates it afterward, so the index (and any constraint it
    /// enforces) silently disappears from both the live session and any
    /// later persisted snapshot (issue #6599). This is the counterpart to
    /// [`Self::restore_indexes_for_table`], which reattaches the returned
    /// entries under the table's new identity.
    ///
    /// Resource-tracker stats are keyed on the index name alone (not the
    /// table name), so they stay valid across the rename and are
    /// deliberately left untouched here — unregistering and re-registering
    /// would just churn the same accounting for no behavioral difference.
    pub fn take_indexes_for_table(&mut self, table_name: &str) -> Vec<(IndexMetadata, IndexData)> {
        let search_name_lower = table_name.to_lowercase();
        let search_table_only = search_name_lower.rsplit('.').next().unwrap_or(&search_name_lower);

        let keys_to_take: Vec<String> = self
            .indexes
            .iter()
            .filter(|(_, metadata)| {
                let stored_lower = metadata.table_name.to_lowercase();
                let stored_table_only = stored_lower.rsplit('.').next().unwrap_or(&stored_lower);
                stored_lower == search_name_lower || stored_table_only == search_table_only
            })
            .map(|(name, _)| name.clone())
            .collect();

        let mut taken = Vec::with_capacity(keys_to_take.len());
        for key in &keys_to_take {
            let Some(meta) = self.remove_index(key) else { continue };
            let Some(data) = self.index_data.remove(key) else { continue };
            taken.push((meta, data));
        }
        taken
    }

    /// Reattach indexes previously detached by [`Self::take_indexes_for_table`]
    /// under `new_table_name`.
    ///
    /// Each index's `table_name` is rewritten to `new_table_name` (preserving
    /// any schema qualifier the original stored name carried, e.g. `aux.t` ->
    /// `aux.<new_table_name>`), and the physical body is reinserted
    /// unchanged: a table rename does not touch row data or column
    /// positions, so no index rebuild is necessary. The storage key
    /// (`schema.index_name`) is derived from the index's own schema and
    /// name, neither of which change on a table rename, so this restores the
    /// index under the exact same physical key it was taken from.
    pub fn restore_indexes_for_table(
        &mut self,
        indexes: Vec<(IndexMetadata, IndexData)>,
        new_table_name: &str,
    ) {
        for (mut meta, data) in indexes {
            meta.table_name = match split_schema_qualifier(&meta.table_name) {
                Some((schema, _)) => format!("{schema}.{new_table_name}"),
                None => new_table_name.to_string(),
            };
            let key = make_index_key(&meta.schema, &meta.index_name);
            self.insert_index(key.clone(), meta);
            self.index_data.insert(key, data);
        }
    }
}
