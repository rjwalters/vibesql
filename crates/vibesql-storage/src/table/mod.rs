// ============================================================================
// Table - In-Memory Storage Layer
// ============================================================================
//
// This module provides the core Table abstraction for in-memory row storage.
// The table implementation follows a delegation pattern, where specialized
// components handle distinct concerns:
//
// ## Architecture
//
// ```
// Table (Orchestration Layer)
//   ├─> IndexManager        - Hash-based indexing for PK/UNIQUE constraints
//   ├─> RowNormalizer       - Value normalization and validation
//   └─> AppendModeTracker   - Sequential insert detection for optimization
// ```
//
// ### Component Responsibilities
//
// **IndexManager** (`indexes.rs`):
// - Maintains hash indexes for primary key and unique constraints
// - Provides O(1) lookups for duplicate detection
// - Handles index updates on INSERT/UPDATE/DELETE
// - Supports selective index maintenance for performance
//
// **RowNormalizer** (`normalization.rs`):
// - CHAR padding/truncation to fixed length
// - Type validation (ensures values match column types)
// - NULL constraint validation
// - Column count verification
//
// **AppendModeTracker** (`append_mode.rs`):
// - Detects sequential primary key insertion patterns
// - Enables executor-level optimizations when sequential inserts detected
// - Maintains O(1) tracking overhead
// - Activates after threshold of consecutive sequential inserts
//
// ### Design Principles
//
// 1. **Separation of Concerns**: Each component handles one specific responsibility
// 2. **Delegation Pattern**: Table orchestrates, components execute
// 3. **Performance First**: Optimizations built into architecture (append mode, selective updates)
// 4. **Clean API**: Public interface remains simple despite internal complexity
//
// ### Refactoring History
//
// This module structure is the result of a systematic refactoring effort (#842)
// that extracted specialized components from a monolithic table.rs file:
//
// - **Phase 1** (PR #853): IndexManager extraction
// - **Phase 3** (PR #856): RowNormalizer extraction
// - **Phase 4** (PR #858): AppendModeTracker extraction
// - **Phase 5** (PR #859): Documentation and finalization
//
// Note: Phase 2 (Constraint Validation) was closed as invalid - constraint
// validation properly belongs in the executor layer, not the storage layer.

mod append_mode;
mod indexes;
mod normalization;

use append_mode::AppendModeTracker;
use indexes::IndexManager;
use normalization::RowNormalizer;
use vibesql_types::SqlValue;

use crate::{Row, StorageError};

/// In-memory table - stores rows with optimized indexing and validation
///
/// # Architecture
///
/// The `Table` struct acts as an orchestration layer, delegating specialized
/// operations to dedicated components:
///
/// - **Row Storage**: Direct Vec storage for sequential access (table scans)
/// - **Columnar Storage**: Native columnar storage for OLAP-optimized tables
/// - **Indexing**: `IndexManager` maintains hash indexes for constraint checks
/// - **Normalization**: `RowNormalizer` handles value transformation and validation
/// - **Optimization**: Append mode tracking for sequential insert performance
///
/// # Storage Formats
///
/// Tables support two storage formats:
/// - **Row-oriented (default)**: Traditional row storage, optimized for OLTP
/// - **Columnar**: Native column storage, optimized for OLAP with zero conversion overhead
///
/// ## Columnar Storage Limitations
///
/// **IMPORTANT**: Columnar tables are optimized for read-heavy analytical workloads.
/// Each INSERT/UPDATE/DELETE operation triggers a full rebuild of the columnar
/// representation (O(n) cost). This makes columnar tables unsuitable for:
/// - High-frequency INSERT workloads
/// - OLTP use cases with frequent writes
/// - Streaming inserts
///
/// **Recommended use cases for columnar tables**:
/// - Bulk-loaded analytical data (load once, query many times)
/// - Reporting tables with infrequent updates
/// - Data warehouse fact tables
///
/// For mixed workloads, use row-oriented storage with the columnar cache
/// (via `scan_columnar()`), which provides SIMD acceleration with caching.
///
/// # Performance Characteristics
///
/// - **INSERT**: O(1) amortized for row append + O(1) for index updates
/// - **UPDATE**: O(1) for row update + O(k) for k affected indexes (selective mode)
/// - **DELETE**: O(n) for scan + O(m) for m deletes + O(n) for index rebuild
/// - **SCAN**: O(n) direct vector iteration
/// - **COLUMNAR SCAN**: O(n) with SIMD acceleration (no conversion overhead for native columnar)
/// - **PK/UNIQUE lookup**: O(1) via hash indexes
///
/// # Example
///
/// ```rust,ignore
/// use vibesql_catalog::TableSchema;
/// use vibesql_storage::Table;
///
/// let schema = TableSchema::new("users", columns);
/// let mut table = Table::new(schema);
///
/// // Insert automatically validates and indexes
/// table.insert(row)?;
///
/// // Scan returns all rows
/// for row in table.scan() {
///     // Process row...
/// }
/// ```
#[derive(Debug)]
pub struct Table {
    /// Table schema defining structure and constraints
    pub schema: vibesql_catalog::TableSchema,

    /// Row storage - direct vector for sequential access (row-oriented tables only)
    rows: Vec<Row>,

    /// Native columnar storage - primary storage for columnar tables
    /// For columnar tables, this is the authoritative data source
    /// For row tables, this is None (use columnar_cache for converted data)
    native_columnar: Option<crate::ColumnarTable>,

    /// Hash indexes for constraint validation (managed by IndexManager)
    /// Provides O(1) lookups for primary key and unique constraints
    indexes: IndexManager,

    /// Append mode optimization tracking (managed by AppendModeTracker)
    /// Detects sequential primary key inserts for executor-level optimizations
    append_tracker: AppendModeTracker,

    /// Cached statistics for query optimization (computed lazily)
    statistics: Option<crate::statistics::TableStatistics>,

    /// Counter for modifications since last statistics update
    modifications_since_stats: usize,

    /// Cached columnar representation for SIMD-accelerated queries (row tables only)
    /// Invalidated on any table modification (INSERT/UPDATE/DELETE)
    /// Uses RwLock for thread-safe interior mutability since scan_columnar takes &self
    /// Not used for native columnar tables (they use native_columnar directly)
    columnar_cache: std::sync::RwLock<Option<crate::ColumnarTable>>,
}

impl Clone for Table {
    fn clone(&self) -> Self {
        // Clone the columnar cache if present
        let cached = self.columnar_cache.read().unwrap().clone();
        Table {
            schema: self.schema.clone(),
            rows: self.rows.clone(),
            native_columnar: self.native_columnar.clone(),
            indexes: self.indexes.clone(),
            append_tracker: self.append_tracker.clone(),
            statistics: self.statistics.clone(),
            modifications_since_stats: self.modifications_since_stats,
            columnar_cache: std::sync::RwLock::new(cached),
        }
    }
}

impl Table {
    /// Create a new empty table with given schema
    ///
    /// The storage format is determined by the schema's storage_format field:
    /// - Row: Traditional row-oriented storage (default)
    /// - Columnar: Native columnar storage for analytical workloads
    pub fn new(schema: vibesql_catalog::TableSchema) -> Self {
        let indexes = IndexManager::new(&schema);
        let is_columnar = schema.is_columnar();

        // For columnar tables, initialize empty native columnar storage
        let native_columnar = if is_columnar {
            // Create empty columnar table with column names from schema
            let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
            Some(
                crate::ColumnarTable::from_rows(&[], &column_names)
                    .expect("Creating empty columnar table should never fail"),
            )
        } else {
            None
        };

        Table {
            schema,
            rows: Vec::new(),
            native_columnar,
            indexes,
            append_tracker: AppendModeTracker::new(),
            statistics: None,
            modifications_since_stats: 0,
            columnar_cache: std::sync::RwLock::new(None),
        }
    }

    /// Check if this table uses native columnar storage
    pub fn is_native_columnar(&self) -> bool {
        self.native_columnar.is_some()
    }

    /// Insert a row into the table
    ///
    /// For row-oriented tables, rows are stored directly in a Vec.
    /// For columnar tables, rows are buffered and the columnar data is rebuilt.
    pub fn insert(&mut self, row: Row) -> Result<(), StorageError> {
        // Normalize and validate row (column count, type checking, NULL checking, value
        // normalization)
        let normalizer = RowNormalizer::new(&self.schema);
        let normalized_row = normalizer.normalize_and_validate(row)?;

        // Detect sequential append pattern before inserting
        if let Some(pk_indices) = self.schema.get_primary_key_indices() {
            let pk_values: Vec<SqlValue> =
                pk_indices.iter().map(|&idx| normalized_row.values[idx].clone()).collect();
            self.append_tracker.update(&pk_values);
        }

        // Add row to table (always stored for indexing and potential row access)
        let row_index = self.rows.len();
        self.rows.push(normalized_row.clone());

        // Update indexes (delegate to IndexManager)
        self.indexes.update_for_insert(&self.schema, &normalized_row, row_index);

        // Track modifications for statistics staleness
        self.modifications_since_stats += 1;

        // Mark stats stale if significant changes (> 10% of table)
        if let Some(stats) = &mut self.statistics {
            if self.modifications_since_stats > stats.row_count / 10 {
                stats.mark_stale();
            }
        }

        // For native columnar tables, rebuild columnar data
        // For row tables, invalidate the cache
        if self.native_columnar.is_some() {
            self.rebuild_native_columnar()?;
        } else {
            *self.columnar_cache.write().unwrap() = None;
        }

        Ok(())
    }

    /// Rebuild native columnar storage from rows
    fn rebuild_native_columnar(&mut self) -> Result<(), StorageError> {
        let column_names: Vec<String> =
            self.schema.columns.iter().map(|c| c.name.clone()).collect();

        let columnar = crate::ColumnarTable::from_rows(&self.rows, &column_names)
            .map_err(|e| StorageError::Other(format!("Columnar rebuild failed: {}", e)))?;

        self.native_columnar = Some(columnar);
        Ok(())
    }

    /// Insert multiple rows into the table in a single batch operation
    ///
    /// This method is optimized for bulk data loading and provides significant
    /// performance improvements over repeated single-row inserts:
    ///
    /// - **Pre-allocation**: Vector capacity is reserved upfront
    /// - **Batch normalization**: Rows are validated/normalized together
    /// - **Deferred index updates**: Indexes are rebuilt once after all inserts
    /// - **Single cache invalidation**: Columnar cache invalidated once at end
    /// - **Statistics update once**: Stats marked stale only at completion
    ///
    /// # Arguments
    ///
    /// * `rows` - Vector of rows to insert
    ///
    /// # Returns
    ///
    /// * `Ok(usize)` - Number of rows successfully inserted
    /// * `Err(StorageError)` - If any row fails validation (no rows inserted on error)
    ///
    /// # Performance
    ///
    /// For large batches (1000+ rows), this method is typically 10-50x faster
    /// than equivalent single-row inserts due to reduced per-row overhead.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let rows = vec![
    ///     Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]),
    ///     Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())]),
    ///     Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar("Charlie".to_string())]),
    /// ];
    /// let count = table.insert_batch(rows)?;
    /// assert_eq!(count, 3);
    /// ```
    pub fn insert_batch(&mut self, rows: Vec<Row>) -> Result<usize, StorageError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let row_count = rows.len();
        let normalizer = RowNormalizer::new(&self.schema);

        // Phase 1: Normalize and validate all rows upfront
        // This ensures we fail fast before modifying any state
        let mut normalized_rows = Vec::with_capacity(row_count);
        for row in rows {
            let normalized = normalizer.normalize_and_validate(row)?;
            normalized_rows.push(normalized);
        }

        // Phase 2: Pre-allocate capacity for rows vector
        self.rows.reserve(row_count);

        // Phase 3: Insert all rows into storage
        for row in normalized_rows {
            self.rows.push(row);
        }

        // Phase 4: Rebuild indexes from scratch (more efficient for large batches)
        // For small batches, incremental would be faster, but rebuild is simpler
        // and the threshold here (any batch) ensures consistency
        self.indexes.rebuild(&self.schema, &self.rows);

        // Phase 5: Update append mode tracker with last inserted row
        // (We only track the final state, not intermediate states)
        if let Some(pk_indices) = self.schema.get_primary_key_indices() {
            if let Some(last_row) = self.rows.last() {
                let pk_values: Vec<SqlValue> =
                    pk_indices.iter().map(|&idx| last_row.values[idx].clone()).collect();
                // Reset tracker and set to last value (bulk insert breaks sequential pattern)
                self.append_tracker.reset();
                self.append_tracker.update(&pk_values);
            }
        }

        // Phase 6: Update statistics tracking
        self.modifications_since_stats += row_count;
        if let Some(stats) = &mut self.statistics {
            if self.modifications_since_stats > stats.row_count / 10 {
                stats.mark_stale();
            }
        }

        // Phase 7: Handle columnar storage
        // For native columnar tables, rebuild columnar data
        // For row tables, invalidate the cache
        if self.native_columnar.is_some() {
            self.rebuild_native_columnar()?;
        } else {
            *self.columnar_cache.write().unwrap() = None;
        }

        Ok(row_count)
    }

    /// Insert rows from an iterator in a streaming fashion
    ///
    /// This method is optimized for very large datasets that may not fit
    /// in memory all at once. Rows are processed in configurable batch sizes.
    ///
    /// # Arguments
    ///
    /// * `rows` - Iterator yielding rows to insert
    /// * `batch_size` - Number of rows to process per batch (default: 1000)
    ///
    /// # Returns
    ///
    /// * `Ok(usize)` - Total number of rows successfully inserted
    /// * `Err(StorageError)` - If any row fails validation
    ///
    /// # Note
    ///
    /// Unlike `insert_batch`, this method commits rows in batches, so a failure
    /// partway through will leave previously committed batches in the table.
    /// Use `insert_batch` if you need all-or-nothing semantics.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Stream rows from a file reader
    /// let rows_iter = csv_reader.rows().map(|r| Row::from_csv_record(r));
    /// let count = table.insert_from_iter(rows_iter, 1000)?;
    /// ```
    pub fn insert_from_iter<I>(&mut self, rows: I, batch_size: usize) -> Result<usize, StorageError>
    where
        I: Iterator<Item = Row>,
    {
        let batch_size = if batch_size == 0 { 1000 } else { batch_size };
        let mut total_inserted = 0;
        let mut batch = Vec::with_capacity(batch_size);

        for row in rows {
            batch.push(row);

            if batch.len() >= batch_size {
                let count = self.insert_batch(std::mem::take(&mut batch))?;
                total_inserted += count;
                batch = Vec::with_capacity(batch_size);
            }
        }

        // Insert any remaining rows
        if !batch.is_empty() {
            let count = self.insert_batch(batch)?;
            total_inserted += count;
        }

        Ok(total_inserted)
    }

    /// Get all rows (for scanning)
    pub fn scan(&self) -> &[Row] {
        &self.rows
    }

    /// Get a single row by index position (O(1) access)
    ///
    /// This is more efficient than `scan().get(idx)` when you only need one row,
    /// as it avoids returning a reference to the entire row slice.
    ///
    /// # Arguments
    /// * `idx` - The row index position
    ///
    /// # Returns
    /// * `Some(&Row)` - The row at the given index
    /// * `None` - If the index is out of bounds
    #[inline]
    pub fn get_row(&self, idx: usize) -> Option<&Row> {
        self.rows.get(idx)
    }

    /// Scan table data in columnar format for SIMD-accelerated processing
    ///
    /// This method returns columnar data suitable for high-performance analytical queries.
    /// Unlike `scan()` which returns row-oriented data, this method returns column-oriented
    /// data that enables:
    ///
    /// - **SIMD vectorization**: Process 4-8 values per CPU instruction
    /// - **Cache efficiency**: Contiguous column data improves memory access patterns
    /// - **Type specialization**: Avoid SqlValue enum matching overhead
    ///
    /// # Performance
    ///
    /// For **native columnar tables**: Zero conversion overhead - returns data directly.
    /// For **row tables**: O(n * m) conversion cost, cached for subsequent queries.
    ///
    /// # Returns
    ///
    /// * `Ok(ColumnarTable)` - Columnar representation of the table data
    /// * `Err(StorageError)` - If conversion fails due to type mismatches
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let columnar = table.scan_columnar()?;
    /// // Process with SIMD-accelerated operations
    /// if let Some(ColumnData::Int64 { values, nulls }) = columnar.get_column("quantity") {
    ///     // SIMD filtering on values slice
    /// }
    /// ```
    pub fn scan_columnar(&self) -> Result<crate::ColumnarTable, StorageError> {
        // For native columnar tables, return data directly (zero conversion overhead)
        if let Some(ref native) = self.native_columnar {
            return Ok(native.clone());
        }

        // For row tables, check cache first (read lock)
        {
            let cache = self.columnar_cache.read().unwrap();
            if let Some(cached) = cache.as_ref() {
                return Ok(cached.clone());
            }
        }

        // Get column names from schema
        let column_names: Vec<String> =
            self.schema.columns.iter().map(|c| c.name.clone()).collect();

        // Convert rows to columnar format
        let columnar = crate::ColumnarTable::from_rows(&self.rows, &column_names)
            .map_err(|e| StorageError::Other(format!("Columnar conversion failed: {}", e)))?;

        // Cache the result for future queries (write lock)
        *self.columnar_cache.write().unwrap() = Some(columnar.clone());

        Ok(columnar)
    }

    /// Get number of rows
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Get table statistics, computing if necessary
    ///
    /// Statistics are computed lazily on first access and cached.
    /// They are marked stale after significant data changes (> 10% of rows).
    pub fn statistics(&mut self) -> &crate::statistics::TableStatistics {
        if self.statistics.is_none() || self.statistics.as_ref().unwrap().needs_refresh() {
            self.statistics =
                Some(crate::statistics::TableStatistics::compute(&self.rows, &self.schema));
            self.modifications_since_stats = 0;
        }

        self.statistics.as_ref().unwrap()
    }

    /// Get cached table statistics without computing
    ///
    /// Returns None if statistics have never been computed or are stale.
    /// Use `statistics()` if you want to compute/refresh statistics.
    pub fn get_statistics(&self) -> Option<&crate::statistics::TableStatistics> {
        self.statistics.as_ref()
    }

    /// Force recomputation of statistics (ANALYZE command)
    pub fn analyze(&mut self) {
        self.statistics =
            Some(crate::statistics::TableStatistics::compute(&self.rows, &self.schema));
        self.modifications_since_stats = 0;
    }

    /// Check if table is in append mode (sequential inserts detected)
    /// When true, constraint checks can skip duplicate lookups for optimization
    pub fn is_in_append_mode(&self) -> bool {
        self.append_tracker.is_active()
    }

    /// Clear all rows
    pub fn clear(&mut self) {
        self.rows.clear();
        // Clear indexes (delegate to IndexManager)
        self.indexes.clear();
        // Reset append mode tracking
        self.append_tracker.reset();
        // Clear native columnar if present, or invalidate cache for row tables
        if self.native_columnar.is_some() {
            let column_names: Vec<String> =
                self.schema.columns.iter().map(|c| c.name.clone()).collect();
            self.native_columnar = Some(
                crate::ColumnarTable::from_rows(&[], &column_names)
                    .expect("Creating empty columnar table should never fail"),
            );
        } else {
            *self.columnar_cache.write().unwrap() = None;
        }
    }

    /// Update a row at the specified index
    pub fn update_row(&mut self, index: usize, row: Row) -> Result<(), StorageError> {
        if index >= self.rows.len() {
            return Err(StorageError::ColumnIndexOutOfBounds { index });
        }

        // Normalize and validate row
        let normalizer = RowNormalizer::new(&self.schema);
        let normalized_row = normalizer.normalize_and_validate(row)?;

        // Get old row for index updates (clone to avoid borrow issues)
        let old_row = self.rows[index].clone();

        // Update the row
        self.rows[index] = normalized_row.clone();

        // Update indexes (delegate to IndexManager)
        self.indexes.update_for_update(&self.schema, &old_row, &normalized_row, index);

        // For native columnar tables, rebuild columnar data
        // For row tables, invalidate the cache
        if self.native_columnar.is_some() {
            self.rebuild_native_columnar()?;
        } else {
            *self.columnar_cache.write().unwrap() = None;
        }

        Ok(())
    }

    /// Update a row with selective index maintenance
    ///
    /// Only updates indexes that reference changed columns, providing significant
    /// performance improvement for tables with many indexes when updating non-indexed columns.
    ///
    /// # Arguments
    /// * `index` - Row index to update
    /// * `row` - New row data
    /// * `changed_columns` - Set of column indices that were modified
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(StorageError)` if index out of bounds or column count mismatch
    pub fn update_row_selective(
        &mut self,
        index: usize,
        row: Row,
        changed_columns: &std::collections::HashSet<usize>,
    ) -> Result<(), StorageError> {
        if index >= self.rows.len() {
            return Err(StorageError::ColumnIndexOutOfBounds { index });
        }

        // Normalize and validate row
        let normalizer = RowNormalizer::new(&self.schema);
        let normalized_row = normalizer.normalize_and_validate(row)?;

        // Get old row for index updates (clone to avoid borrow issues)
        let old_row = self.rows[index].clone();

        // Update the row
        self.rows[index] = normalized_row.clone();

        // Determine which indexes are affected by the changed columns (delegate to IndexManager)
        let affected_indexes = self.indexes.get_affected_indexes(&self.schema, changed_columns);

        // Update only affected indexes (delegate to IndexManager)
        self.indexes.update_selective(
            &self.schema,
            &old_row,
            &normalized_row,
            index,
            &affected_indexes,
        );

        // For native columnar tables, rebuild columnar data
        // For row tables, invalidate the cache
        if self.native_columnar.is_some() {
            self.rebuild_native_columnar()?;
        } else {
            *self.columnar_cache.write().unwrap() = None;
        }

        Ok(())
    }

    /// Delete rows matching a predicate
    /// Returns number of rows deleted
    pub fn delete_where<F>(&mut self, mut predicate: F) -> usize
    where
        F: FnMut(&Row) -> bool,
    {
        // Collect indices and rows to delete (only call predicate once per row)
        let mut indices_and_rows_to_delete: Vec<(usize, Row)> = Vec::new();
        for (index, row) in self.rows.iter().enumerate() {
            if predicate(row) {
                indices_and_rows_to_delete.push((index, row.clone()));
            }
        }

        if indices_and_rows_to_delete.is_empty() {
            return 0;
        }

        // Update indexes for deleted rows BEFORE removing (while indices are still valid)
        for (_, deleted_row) in &indices_and_rows_to_delete {
            self.indexes.update_for_delete(&self.schema, deleted_row);
        }

        // Extract just the indices for adjustment
        let deleted_indices: Vec<usize> =
            indices_and_rows_to_delete.iter().map(|(idx, _)| *idx).collect();

        // Delete rows in reverse order to maintain correct indices during removal
        for (index, _) in indices_and_rows_to_delete.iter().rev() {
            self.rows.remove(*index);
        }

        // Adjust remaining index entries instead of full rebuild
        // This is O(num_entries) instead of O(n) for rebuild
        self.indexes.adjust_after_multi_delete(&deleted_indices);

        // For native columnar tables, rebuild columnar data
        // For row tables, invalidate the cache
        if self.native_columnar.is_some() {
            // Note: Using expect here since delete_where returns usize, not Result
            let _ = self.rebuild_native_columnar();
        } else {
            *self.columnar_cache.write().unwrap() = None;
        }

        indices_and_rows_to_delete.len()
    }

    /// Remove a specific row (used for transaction undo)
    /// Returns error if row not found
    pub fn remove_row(&mut self, target_row: &Row) -> Result<(), StorageError> {
        // Find and remove the first matching row
        if let Some(pos) = self.rows.iter().position(|row| row == target_row) {
            // Update indexes before removing (delegate to IndexManager)
            self.indexes.update_for_delete(&self.schema, target_row);
            self.rows.remove(pos);
            // Adjust remaining index entries instead of full rebuild
            self.indexes.adjust_after_delete(pos);
            // For native columnar tables, rebuild columnar data
            // For row tables, invalidate the cache
            if self.native_columnar.is_some() {
                self.rebuild_native_columnar()?;
            } else {
                *self.columnar_cache.write().unwrap() = None;
            }
            Ok(())
        } else {
            Err(StorageError::RowNotFound)
        }
    }

    /// Delete rows by known indices (fast path - no scanning required)
    ///
    /// This is more efficient than `delete_where` when row indices are already known
    /// (e.g., from a primary key lookup). Avoids O(n) table scan.
    ///
    /// # Arguments
    /// * `indices` - Indices of rows to delete, need not be sorted
    ///
    /// # Returns
    /// Number of rows deleted
    pub fn delete_by_indices(&mut self, indices: &[usize]) -> usize {
        if indices.is_empty() {
            return 0;
        }

        // Sort indices for consistent processing
        let mut sorted_indices: Vec<usize> = indices.to_vec();
        sorted_indices.sort_unstable();

        // Validate all indices before modifying
        if sorted_indices.last().is_some_and(|&max| max >= self.rows.len()) {
            return 0; // Invalid index, return early
        }

        // Update indexes for deleted rows BEFORE removing (while indices are still valid)
        for &idx in &sorted_indices {
            let row = &self.rows[idx];
            self.indexes.update_for_delete(&self.schema, row);
        }

        // Delete rows in reverse order to maintain correct indices during removal
        for &idx in sorted_indices.iter().rev() {
            self.rows.remove(idx);
        }

        // Adjust remaining index entries
        self.indexes.adjust_after_multi_delete(&sorted_indices);

        // For native columnar tables, rebuild columnar data
        // For row tables, invalidate the cache
        if self.native_columnar.is_some() {
            let _ = self.rebuild_native_columnar();
        } else {
            *self.columnar_cache.write().unwrap() = None;
        }

        sorted_indices.len()
    }

    /// Get mutable reference to rows
    pub fn rows_mut(&mut self) -> &mut Vec<Row> {
        &mut self.rows
    }

    /// Get mutable reference to schema
    pub fn schema_mut(&mut self) -> &mut vibesql_catalog::TableSchema {
        &mut self.schema
    }

    /// Get reference to primary key index
    pub fn primary_key_index(&self) -> Option<&std::collections::HashMap<Vec<SqlValue>, usize>> {
        self.indexes.primary_key_index()
    }

    /// Get reference to unique constraint indexes
    pub fn unique_indexes(&self) -> &[std::collections::HashMap<Vec<SqlValue>, usize>] {
        self.indexes.unique_indexes()
    }

    /// Rebuild all hash indexes from scratch
    /// Used after schema changes that add constraints (e.g., ALTER TABLE ADD PRIMARY KEY)
    pub fn rebuild_indexes(&mut self) {
        // Recreate the IndexManager to match the current schema
        // (in case constraints were added that didn't exist before)
        self.indexes = IndexManager::new(&self.schema);

        // Rebuild indexes from existing rows
        self.indexes.rebuild(&self.schema, &self.rows);
    }
}

#[cfg(test)]
mod tests {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    use super::*;

    fn create_test_table() -> Table {
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, true),
        ];
        let schema = TableSchema::with_primary_key(
            "test_table".to_string(),
            columns,
            vec!["id".to_string()],
        );
        Table::new(schema)
    }

    fn create_row(id: i64, name: &str) -> Row {
        Row { values: vec![SqlValue::Integer(id), SqlValue::Varchar(name.to_string())] }
    }

    #[test]
    fn test_append_mode_integration() {
        let mut table = create_test_table();
        assert!(!table.is_in_append_mode());

        // Sequential inserts should activate append mode
        table.insert(create_row(1, "Alice")).unwrap();
        table.insert(create_row(2, "Bob")).unwrap();
        table.insert(create_row(3, "Charlie")).unwrap();
        table.insert(create_row(4, "David")).unwrap();
        assert!(table.is_in_append_mode());

        // Clear should reset
        table.clear();
        assert!(!table.is_in_append_mode());
    }

    #[test]
    fn test_scan_columnar() {
        let mut table = create_test_table();

        // Insert test data
        table.insert(create_row(1, "Alice")).unwrap();
        table.insert(create_row(2, "Bob")).unwrap();
        table.insert(create_row(3, "Charlie")).unwrap();

        // Convert to columnar format
        let columnar = table.scan_columnar().unwrap();

        // Verify row count
        assert_eq!(columnar.row_count(), 3);
        assert_eq!(columnar.column_count(), 2);

        // Verify column data - id column
        let id_col = columnar.get_column("id").expect("id column should exist");
        assert_eq!(id_col.len(), 3);
        assert!(!id_col.is_null(0));
        assert!(!id_col.is_null(1));
        assert!(!id_col.is_null(2));

        // Verify column data - name column
        let name_col = columnar.get_column("name").expect("name column should exist");
        assert_eq!(name_col.len(), 3);
    }

    #[test]
    fn test_scan_columnar_empty_table() {
        let table = create_test_table();

        // Convert empty table to columnar format
        let columnar = table.scan_columnar().unwrap();

        // Verify empty result
        assert_eq!(columnar.row_count(), 0);
        assert_eq!(columnar.column_count(), 2); // Schema defines 2 columns
    }

    #[test]
    fn test_scan_columnar_with_nulls() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("value".to_string(), DataType::Integer, true), // nullable
        ];
        let schema = TableSchema::new("test_nulls".to_string(), columns);
        let mut table = Table::new(schema);

        // Insert rows with NULL values
        table.insert(Row { values: vec![SqlValue::Integer(1), SqlValue::Integer(100)] }).unwrap();
        table.insert(Row { values: vec![SqlValue::Integer(2), SqlValue::Null] }).unwrap();
        table.insert(Row { values: vec![SqlValue::Integer(3), SqlValue::Integer(300)] }).unwrap();

        // Convert to columnar format
        let columnar = table.scan_columnar().unwrap();

        // Verify NULL handling
        let value_col = columnar.get_column("value").expect("value column should exist");
        assert!(!value_col.is_null(0)); // 100
        assert!(value_col.is_null(1)); // NULL
        assert!(!value_col.is_null(2)); // 300
    }

    // ========================================================================
    // Bulk Insert Tests
    // ========================================================================

    #[test]
    fn test_insert_batch_basic() {
        let mut table = create_test_table();

        let rows = vec![create_row(1, "Alice"), create_row(2, "Bob"), create_row(3, "Charlie")];

        let count = table.insert_batch(rows).unwrap();

        assert_eq!(count, 3);
        assert_eq!(table.row_count(), 3);

        // Verify data
        let scanned: Vec<_> = table.scan().to_vec();
        assert_eq!(scanned[0].values[0], SqlValue::Integer(1));
        assert_eq!(scanned[1].values[0], SqlValue::Integer(2));
        assert_eq!(scanned[2].values[0], SqlValue::Integer(3));
    }

    #[test]
    fn test_insert_batch_empty() {
        let mut table = create_test_table();

        let count = table.insert_batch(Vec::new()).unwrap();

        assert_eq!(count, 0);
        assert_eq!(table.row_count(), 0);
    }

    #[test]
    fn test_insert_batch_preserves_indexes() {
        let mut table = create_test_table();

        let rows = vec![create_row(1, "Alice"), create_row(2, "Bob"), create_row(3, "Charlie")];

        table.insert_batch(rows).unwrap();

        // Primary key index should exist and have 3 entries
        assert!(table.primary_key_index().is_some());
        let pk_index = table.primary_key_index().unwrap();
        assert_eq!(pk_index.len(), 3);

        // Each PK should map to correct row index
        assert_eq!(pk_index.get(&vec![SqlValue::Integer(1)]), Some(&0));
        assert_eq!(pk_index.get(&vec![SqlValue::Integer(2)]), Some(&1));
        assert_eq!(pk_index.get(&vec![SqlValue::Integer(3)]), Some(&2));
    }

    #[test]
    fn test_insert_batch_invalidates_columnar_cache() {
        let mut table = create_test_table();

        // Insert some initial rows and build columnar cache
        table.insert(create_row(1, "Alice")).unwrap();
        let _ = table.scan_columnar().unwrap();

        // Batch insert more rows
        let rows = vec![create_row(2, "Bob"), create_row(3, "Charlie")];
        table.insert_batch(rows).unwrap();

        // Columnar cache should reflect all rows after rebuild
        let columnar = table.scan_columnar().unwrap();
        assert_eq!(columnar.row_count(), 3);
    }

    #[test]
    fn test_insert_batch_validation_failure_is_atomic() {
        let mut table = create_test_table();

        // Insert valid row first
        table.insert(create_row(1, "Alice")).unwrap();

        // Try to batch insert with one invalid row (wrong column count)
        let rows = vec![
            Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())]),
            Row::new(vec![SqlValue::Integer(3)]), // Invalid - missing column
        ];

        let result = table.insert_batch(rows);
        assert!(result.is_err());

        // Table should still have only 1 row (atomic failure)
        assert_eq!(table.row_count(), 1);
    }

    #[test]
    fn test_insert_batch_large() {
        let mut table = create_test_table();

        // Insert 10000 rows in a batch
        let rows: Vec<Row> = (0..10_000).map(|i| create_row(i, &format!("User{}", i))).collect();

        let count = table.insert_batch(rows).unwrap();

        assert_eq!(count, 10_000);
        assert_eq!(table.row_count(), 10_000);

        // Verify first and last rows
        let scanned = table.scan();
        assert_eq!(scanned[0].values[0], SqlValue::Integer(0));
        assert_eq!(scanned[9999].values[0], SqlValue::Integer(9999));
    }

    #[test]
    fn test_insert_from_iter_basic() {
        let mut table = create_test_table();

        let rows = (0..100).map(|i| create_row(i, &format!("User{}", i)));

        let count = table.insert_from_iter(rows, 10).unwrap();

        assert_eq!(count, 100);
        assert_eq!(table.row_count(), 100);
    }

    #[test]
    fn test_insert_from_iter_default_batch_size() {
        let mut table = create_test_table();

        let rows = (0..50).map(|i| create_row(i, &format!("User{}", i)));

        // batch_size=0 should use default of 1000
        let count = table.insert_from_iter(rows, 0).unwrap();

        assert_eq!(count, 50);
        assert_eq!(table.row_count(), 50);
    }

    #[test]
    fn test_insert_from_iter_partial_final_batch() {
        let mut table = create_test_table();

        // 25 rows with batch size 10 = 2 full batches + 5 remaining
        let rows = (0..25).map(|i| create_row(i, &format!("User{}", i)));

        let count = table.insert_from_iter(rows, 10).unwrap();

        assert_eq!(count, 25);
        assert_eq!(table.row_count(), 25);
    }

    #[test]
    fn test_insert_batch_after_single_inserts() {
        let mut table = create_test_table();

        // Single inserts first
        table.insert(create_row(1, "Alice")).unwrap();
        table.insert(create_row(2, "Bob")).unwrap();

        // Then batch insert
        let rows = vec![create_row(3, "Charlie"), create_row(4, "David")];
        table.insert_batch(rows).unwrap();

        assert_eq!(table.row_count(), 4);

        // Verify indexes are correct
        let pk_index = table.primary_key_index().unwrap();
        assert_eq!(pk_index.get(&vec![SqlValue::Integer(1)]), Some(&0));
        assert_eq!(pk_index.get(&vec![SqlValue::Integer(2)]), Some(&1));
        assert_eq!(pk_index.get(&vec![SqlValue::Integer(3)]), Some(&2));
        assert_eq!(pk_index.get(&vec![SqlValue::Integer(4)]), Some(&3));
    }
}
