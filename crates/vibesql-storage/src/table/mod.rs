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

/// Result of a delete operation, indicating how many rows were deleted
/// and whether table compaction occurred.
///
/// # Important
///
/// When `compacted` is true, all row indices in the table have changed.
/// User-defined indexes (B-tree indexes managed at the Database level)
/// must be rebuilt after compaction to maintain correctness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteResult {
    /// Number of rows that were deleted
    pub deleted_count: usize,
    /// Whether table compaction occurred (row indices changed)
    pub compacted: bool,
}

impl DeleteResult {
    /// Create a new DeleteResult
    pub fn new(deleted_count: usize, compacted: bool) -> Self {
        Self { deleted_count, compacted }
    }
}

/// In-memory table - stores rows with optimized indexing and validation
///
/// # Architecture
///
/// The `Table` struct acts as an orchestration layer, delegating specialized
/// operations to dedicated components:
///
/// - **Row Storage**: Direct Vec storage for sequential access (table scans)
/// - **Deletion Bitmap**: O(1) deletion via bitmap marking instead of Vec::remove()
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
/// - **DELETE**: O(1) per row via bitmap marking (amortized O(n) for compaction)
/// - **SCAN**: O(n) direct vector iteration (skipping deleted rows)
/// - **COLUMNAR SCAN**: O(n) with SIMD acceleration (no conversion overhead for native columnar)
/// - **PK/UNIQUE lookup**: O(1) via hash indexes
///
/// # Example
///
/// ```text
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

    /// Deletion bitmap - tracks which rows are logically deleted
    /// Uses O(1) bit operations instead of O(n) Vec::remove()
    /// Compaction occurs when deleted_count > rows.len() / 2
    deleted: Vec<bool>,

    /// Count of deleted rows (cached to avoid counting bits)
    deleted_count: usize,

    /// Native columnar storage - primary storage for columnar tables
    /// For columnar tables, this is the authoritative data source
    /// For row tables, this is None (use Database::get_columnar() for cached columnar data)
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

    // Note: Table-level columnar caching was removed in #3892 to eliminate duplicate
    // caching with Database::columnar_cache. All columnar caching now goes through
    // Database::get_columnar() which provides LRU eviction and Arc-based sharing.
    // Table::scan_columnar() performs fresh conversion on each call.
}

impl Clone for Table {
    fn clone(&self) -> Self {
        Table {
            schema: self.schema.clone(),
            rows: self.rows.clone(),
            deleted: self.deleted.clone(),
            deleted_count: self.deleted_count,
            native_columnar: self.native_columnar.clone(),
            indexes: self.indexes.clone(),
            append_tracker: self.append_tracker.clone(),
            statistics: self.statistics.clone(),
            modifications_since_stats: self.modifications_since_stats,
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
            deleted: Vec::new(),
            deleted_count: 0,
            native_columnar,
            indexes,
            append_tracker: AppendModeTracker::new(),
            statistics: None,
            modifications_since_stats: 0,
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
        self.deleted.push(false);

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
        // Note: Database-level columnar cache invalidation is handled by the executor
        if self.native_columnar.is_some() {
            self.rebuild_native_columnar()?;
        }

        Ok(())
    }

    /// Rebuild native columnar storage from rows (excluding deleted rows)
    fn rebuild_native_columnar(&mut self) -> Result<(), StorageError> {
        let column_names: Vec<String> =
            self.schema.columns.iter().map(|c| c.name.clone()).collect();

        // Collect only live rows for columnar conversion
        let live_rows: Vec<&Row> = self
            .rows
            .iter()
            .enumerate()
            .filter(|(idx, _)| !self.deleted[*idx])
            .map(|(_, row)| row)
            .collect();

        let columnar = crate::ColumnarTable::from_row_refs(&live_rows, &column_names)
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
    /// ```text
    /// let rows = vec![
    ///     Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
    ///     Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
    ///     Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))]),
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

        // Phase 2: Pre-allocate capacity for rows and deleted vectors
        self.rows.reserve(row_count);
        self.deleted.reserve(row_count);

        // Record starting index for incremental index updates
        let start_index = self.rows.len();

        // Phase 3: Insert all rows into storage
        for row in normalized_rows {
            self.rows.push(row);
            self.deleted.push(false);
        }

        // Phase 4: Incrementally update indexes for only the new rows
        // This is O(batch_size) instead of O(total_rows), avoiding O(n²) behavior
        // when doing multiple batch inserts
        for (i, row) in self.rows[start_index..].iter().enumerate() {
            self.indexes.update_for_insert(&self.schema, row, start_index + i);
        }

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
        // Note: Database-level columnar cache invalidation is handled by the executor
        if self.native_columnar.is_some() {
            self.rebuild_native_columnar()?;
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
    /// ```text
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

    /// Get all rows for scanning
    ///
    /// Returns a slice of all rows in the table. For tables with a deletion bitmap,
    /// this returns the raw storage which may include deleted rows.
    ///
    /// **Important**: For operations that need to skip deleted rows, use `scan_live()`
    /// which filters deleted rows automatically.
    pub fn scan(&self) -> &[Row] {
        &self.rows
    }

    /// Check if a row at the given index is deleted
    #[inline]
    pub fn is_row_deleted(&self, idx: usize) -> bool {
        idx < self.deleted.len() && self.deleted[idx]
    }

    /// Iterate over live (non-deleted) rows with their physical indices
    ///
    /// This is the preferred way to scan table data, as it automatically
    /// skips rows that have been deleted but not yet compacted.
    ///
    /// # Returns
    /// An iterator yielding `(physical_index, &Row)` pairs for all live rows.
    ///
    /// # Example
    /// ```text
    /// for (idx, row) in table.scan_live() {
    ///     // idx is the physical index, can be used with get_row() or delete_by_indices()
    ///     process_row(idx, row);
    /// }
    /// ```
    #[inline]
    pub fn scan_live(&self) -> impl Iterator<Item = (usize, &Row)> {
        self.rows
            .iter()
            .enumerate()
            .filter(|(idx, _)| !self.deleted[*idx])
    }

    /// Scan only live (non-deleted) rows, returning an owned Vec.
    ///
    /// This method provides an efficient way to get all live rows as a Vec<Row>
    /// for executor paths that need owned data. Unlike `scan()` which returns
    /// all rows including deleted ones, this method filters out deleted rows.
    ///
    /// # Performance
    /// O(n) time and space where n is the number of live rows.
    /// Pre-allocates the exact capacity needed based on `row_count()`.
    ///
    /// # Returns
    /// A Vec containing clones of all non-deleted rows.
    ///
    /// # Example
    /// ```text
    /// // For SELECT queries that need a Vec<Row>
    /// let rows = table.scan_live_vec();
    /// ```
    #[inline]
    pub fn scan_live_vec(&self) -> Vec<Row> {
        let mut result = Vec::with_capacity(self.row_count());
        for (idx, row) in self.rows.iter().enumerate() {
            if !self.deleted[idx] {
                result.push(row.clone());
            }
        }
        result
    }

    /// Get a single row by index position (O(1) access)
    ///
    /// Returns None if the row is deleted or index is out of bounds.
    ///
    /// # Arguments
    /// * `idx` - The row index position (physical index)
    ///
    /// # Returns
    /// * `Some(&Row)` - The row at the given index if it exists and is not deleted
    /// * `None` - If the index is out of bounds or row is deleted
    #[inline]
    pub fn get_row(&self, idx: usize) -> Option<&Row> {
        if idx < self.deleted.len() && self.deleted[idx] {
            return None;
        }
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
    /// For **row tables**: O(n * m) conversion cost per call.
    ///
    /// # Caching
    ///
    /// This method does not cache results. For cached columnar access with LRU eviction,
    /// use `Database::get_columnar()` which provides Arc-based sharing across queries.
    ///
    /// # Returns
    ///
    /// * `Ok(ColumnarTable)` - Columnar representation of the table data
    /// * `Err(StorageError)` - If conversion fails due to type mismatches
    ///
    /// # Example
    ///
    /// ```text
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

        // For row tables, perform fresh conversion each time
        // Note: Caching is now handled at the Database level via Database::get_columnar()
        // which provides LRU eviction and Arc-based sharing across queries.

        // Get column names from schema
        let column_names: Vec<String> =
            self.schema.columns.iter().map(|c| c.name.clone()).collect();

        // Collect only live rows for columnar conversion
        let live_rows: Vec<&Row> = self
            .rows
            .iter()
            .enumerate()
            .filter(|(idx, _)| !self.deleted[*idx])
            .map(|(_, row)| row)
            .collect();

        // Convert rows to columnar format
        crate::ColumnarTable::from_row_refs(&live_rows, &column_names)
            .map_err(|e| StorageError::Other(format!("Columnar conversion failed: {}", e)))
    }

    /// Get number of live (non-deleted) rows
    pub fn row_count(&self) -> usize {
        self.rows.len() - self.deleted_count
    }

    /// Get total number of rows including deleted ones (physical storage size)
    #[inline]
    pub fn physical_row_count(&self) -> usize {
        self.rows.len()
    }

    /// Get count of deleted (logically removed) rows
    ///
    /// This is used for DML cost estimation, as tables with many deleted rows
    /// may have degraded performance for UPDATE/DELETE operations.
    #[inline]
    pub fn deleted_count(&self) -> usize {
        self.deleted_count
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
        self.deleted.clear();
        self.deleted_count = 0;
        // Clear indexes (delegate to IndexManager)
        self.indexes.clear();
        // Reset append mode tracking
        self.append_tracker.reset();
        // Clear native columnar if present
        // Note: Database-level columnar cache invalidation is handled by the executor
        if self.native_columnar.is_some() {
            let column_names: Vec<String> =
                self.schema.columns.iter().map(|c| c.name.clone()).collect();
            self.native_columnar = Some(
                crate::ColumnarTable::from_rows(&[], &column_names)
                    .expect("Creating empty columnar table should never fail"),
            );
        }
    }

    /// Update a row at the specified index
    pub fn update_row(&mut self, index: usize, row: Row) -> Result<(), StorageError> {
        if index >= self.rows.len() {
            return Err(StorageError::ColumnIndexOutOfBounds { index });
        }

        // Cannot update a deleted row
        if self.deleted[index] {
            return Err(StorageError::RowNotFound);
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
        // Note: Database-level columnar cache invalidation is handled by the executor
        if self.native_columnar.is_some() {
            self.rebuild_native_columnar()?;
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

        // Cannot update a deleted row
        if self.deleted[index] {
            return Err(StorageError::RowNotFound);
        }

        // Normalize and validate row
        let normalizer = RowNormalizer::new(&self.schema);
        let normalized_row = normalizer.normalize_and_validate(row)?;

        // Get old row for index updates (clone to avoid borrow issues)
        let old_row = self.rows[index].clone();

        // Determine which indexes are affected by the changed columns (delegate to IndexManager)
        let affected_indexes = self.indexes.get_affected_indexes(&self.schema, changed_columns);

        // Update only affected indexes BEFORE replacing row (delegate to IndexManager)
        self.indexes.update_selective(
            &self.schema,
            &old_row,
            &normalized_row,
            index,
            &affected_indexes,
        );

        // Update the row (move ownership, no clone needed)
        self.rows[index] = normalized_row;

        // For native columnar tables, rebuild columnar data
        // Note: Database-level columnar cache invalidation is handled by the executor
        if self.native_columnar.is_some() {
            self.rebuild_native_columnar()?;
        }

        Ok(())
    }

    /// Fast path update for pre-validated rows
    ///
    /// This variant skips normalization/validation, assuming the caller has already
    /// validated the row data. Use for performance-critical UPDATE paths where
    /// validation was done at the executor level.
    ///
    /// # Arguments
    /// * `index` - Row index to update
    /// * `new_row` - Pre-validated new row data (ownership transferred)
    /// * `old_row` - Reference to old row for index updates
    /// * `changed_columns` - Set of column indices that were modified
    ///
    /// # Safety
    /// Caller must ensure row data is valid (correct column count, types, constraints)
    #[inline]
    pub fn update_row_unchecked(
        &mut self,
        index: usize,
        new_row: Row,
        old_row: &Row,
        changed_columns: &std::collections::HashSet<usize>,
    ) {
        // Determine which indexes are affected by the changed columns
        let affected_indexes = self.indexes.get_affected_indexes(&self.schema, changed_columns);

        // Update affected indexes BEFORE replacing row
        self.indexes.update_selective(
            &self.schema,
            old_row,
            &new_row,
            index,
            &affected_indexes,
        );

        // Update the row (direct move, no validation)
        self.rows[index] = new_row;

        // Note: Database-level columnar cache invalidation is handled by the executor
    }

    /// Update a single column value in-place without cloning the row
    ///
    /// This is the fastest possible update path for non-indexed columns:
    /// - No row cloning (direct in-place modification)
    /// - No index updates (caller must verify column is not indexed)
    /// - No validation (caller must pre-validate the value)
    ///
    /// # Arguments
    ///
    /// * `row_index` - Index of the row to update
    /// * `col_index` - Index of the column to update
    /// * `new_value` - The new value for the column
    ///
    /// # Safety
    ///
    /// Caller must ensure:
    /// - The column is NOT indexed (no internal or user-defined indexes)
    /// - The value satisfies all constraints (NOT NULL, type, etc.)
    #[inline]
    pub fn update_column_inplace(
        &mut self,
        row_index: usize,
        col_index: usize,
        new_value: vibesql_types::SqlValue,
    ) {
        self.rows[row_index].values[col_index] = new_value;

        // Note: Database-level columnar cache invalidation is handled by the executor
    }

    /// Delete rows matching a predicate
    ///
    /// Uses O(1) bitmap marking for each deleted row instead of O(n) Vec::remove().
    ///
    /// # Returns
    /// [`DeleteResult`] containing the count of deleted rows and whether compaction occurred.
    pub fn delete_where<F>(&mut self, mut predicate: F) -> DeleteResult
    where
        F: FnMut(&Row) -> bool,
    {
        // Collect indices of rows to delete (skip already-deleted rows)
        let mut indices_to_delete: Vec<usize> = Vec::new();
        for (index, row) in self.rows.iter().enumerate() {
            if !self.deleted[index] && predicate(row) {
                indices_to_delete.push(index);
            }
        }

        if indices_to_delete.is_empty() {
            return DeleteResult::new(0, false);
        }

        // Use the optimized delete_by_indices which uses bitmap marking
        self.delete_by_indices(&indices_to_delete)
    }

    /// Remove a specific row (used for transaction undo)
    /// Returns error if row not found
    ///
    /// Uses O(1) bitmap marking instead of O(n) Vec::remove().
    ///
    /// Note: This method does not return compaction status since it's used
    /// internally for transaction rollback where index consistency is handled
    /// at a higher level.
    pub fn remove_row(&mut self, target_row: &Row) -> Result<(), StorageError> {
        // Find the first matching non-deleted row
        for (idx, row) in self.rows.iter().enumerate() {
            if !self.deleted[idx] && row == target_row {
                // Use delete_by_indices for consistent behavior
                // Note: We ignore compaction status here since transaction rollback
                // handles index consistency at the transaction layer
                let _ = self.delete_by_indices(&[idx]);
                return Ok(());
            }
        }
        Err(StorageError::RowNotFound)
    }

    /// Delete rows by known indices (fast path - no scanning required)
    ///
    /// Uses O(1) bitmap marking instead of O(n) Vec::remove(). Rows are marked
    /// as deleted but remain in the vector until compaction is triggered.
    ///
    /// # Arguments
    /// * `indices` - Indices of rows to delete, need not be sorted
    ///
    /// # Returns
    /// [`DeleteResult`] containing:
    /// - `deleted_count`: Number of rows deleted
    /// - `compacted`: Whether compaction occurred (row indices changed)
    ///
    /// # Important
    ///
    /// When `compacted` is true, all row indices in the table have changed.
    /// User-defined indexes (B-tree indexes managed at the Database level)
    /// must be rebuilt after compaction to maintain correctness.
    ///
    /// # Performance
    /// O(d) where d = number of rows to delete, compared to O(d * n) for Vec::remove()
    pub fn delete_by_indices(&mut self, indices: &[usize]) -> DeleteResult {
        if indices.is_empty() {
            return DeleteResult::new(0, false);
        }

        // Count valid, non-already-deleted indices
        let mut deleted = 0;
        for &idx in indices {
            // Skip invalid or already-deleted indices
            if idx >= self.rows.len() || self.deleted[idx] {
                continue;
            }

            // Update indexes for this row BEFORE marking as deleted
            let row = &self.rows[idx];
            self.indexes.update_for_delete(&self.schema, row);

            // Mark row as deleted - O(1) operation
            self.deleted[idx] = true;
            self.deleted_count += 1;
            deleted += 1;
        }

        if deleted == 0 {
            return DeleteResult::new(0, false);
        }

        // Check if compaction is needed (> 50% deleted)
        // Compaction rebuilds the vectors without deleted rows
        // NOTE: When compaction occurs, all row indices change and user-defined
        // indexes (B-tree indexes) must be rebuilt by the caller
        let compacted = if self.should_compact() {
            self.compact();
            true
        } else {
            false
        };

        // For native columnar tables, rebuild columnar data
        // Note: Database-level columnar cache invalidation is handled by the executor
        if self.native_columnar.is_some() {
            let _ = self.rebuild_native_columnar();
        }

        DeleteResult::new(deleted, compacted)
    }

    /// Delete rows by known indices with batch-optimized internal index updates
    ///
    /// This is an optimized version of `delete_by_indices` that pre-computes
    /// schema lookups for internal hash indexes, reducing overhead for multi-row
    /// deletes by ~30-40%.
    ///
    /// # Arguments
    /// * `indices` - Indices of rows to delete, need not be sorted
    ///
    /// # Returns
    /// [`DeleteResult`] containing:
    /// - `deleted_count`: Number of rows deleted
    /// - `compacted`: Whether compaction occurred (row indices changed)
    ///
    /// # Performance
    /// - Pre-computes PK/unique column indices once (O(1) vs O(d) schema lookups)
    /// - Uses batch index updates for internal hash indexes
    /// - Best for multi-row deletes; single-row deletes use `delete_by_indices`
    pub fn delete_by_indices_batch(&mut self, indices: &[usize]) -> DeleteResult {
        if indices.is_empty() {
            return DeleteResult::new(0, false);
        }

        // For single-row deletes, use the standard path (no batch overhead)
        if indices.len() == 1 {
            return self.delete_by_indices(indices);
        }

        // Phase 1: Collect valid rows to delete and their references
        // This avoids repeated bounds/deleted checks
        let mut valid_indices: Vec<usize> = Vec::with_capacity(indices.len());
        let mut rows_to_delete: Vec<&Row> = Vec::with_capacity(indices.len());

        for &idx in indices {
            if idx < self.rows.len() && !self.deleted[idx] {
                valid_indices.push(idx);
                rows_to_delete.push(&self.rows[idx]);
            }
        }

        if valid_indices.is_empty() {
            return DeleteResult::new(0, false);
        }

        // Phase 2: Batch update internal hash indexes (pre-computes column indices once)
        self.indexes.batch_update_for_delete(&self.schema, &rows_to_delete);

        // Phase 3: Mark rows as deleted
        let deleted = valid_indices.len();
        for idx in valid_indices {
            self.deleted[idx] = true;
            self.deleted_count += 1;
        }

        // Phase 4: Check compaction and handle columnar
        let compacted = if self.should_compact() {
            self.compact();
            true
        } else {
            false
        };

        // For native columnar tables, rebuild columnar data
        // (Row tables use Database::columnar_cache which is invalidated by executors)
        if self.native_columnar.is_some() {
            let _ = self.rebuild_native_columnar();
        }

        DeleteResult::new(deleted, compacted)
    }

    /// Check if the table should be compacted
    ///
    /// Compaction is triggered when more than 50% of rows are deleted.
    /// This prevents unbounded growth of deleted row storage.
    #[inline]
    fn should_compact(&self) -> bool {
        // Only compact if we have at least some rows and > 50% are deleted
        !self.rows.is_empty() && self.deleted_count > self.rows.len() / 2
    }

    /// Compact the table by removing deleted rows
    ///
    /// This rebuilds the rows vector without deleted entries and rebuilds
    /// all indexes to point to the new positions.
    fn compact(&mut self) {
        if self.deleted_count == 0 {
            return;
        }

        // Build new vectors with only live rows
        let mut new_rows = Vec::with_capacity(self.rows.len() - self.deleted_count);
        for (idx, row) in self.rows.iter().enumerate() {
            if !self.deleted[idx] {
                new_rows.push(row.clone());
            }
        }

        // Replace old vectors with compacted ones
        self.rows = new_rows;
        self.deleted = vec![false; self.rows.len()];
        self.deleted_count = 0;

        // Rebuild all indexes since row positions have changed
        self.indexes.rebuild(&self.schema, &self.rows);
    }

    /// Check if a row at the given index is deleted
    #[inline]
    pub fn is_deleted(&self, idx: usize) -> bool {
        idx < self.deleted.len() && self.deleted[idx]
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
        Row::from_vec(vec![SqlValue::Integer(id), SqlValue::Varchar(arcstr::ArcStr::from(name))])
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
        table.insert(Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Integer(100)])).unwrap();
        table.insert(Row::from_vec(vec![SqlValue::Integer(2), SqlValue::Null])).unwrap();
        table.insert(Row::from_vec(vec![SqlValue::Integer(3), SqlValue::Integer(300)])).unwrap();

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
    fn test_insert_batch_columnar_scan_includes_new_rows() {
        let mut table = create_test_table();

        // Insert some initial rows
        table.insert(create_row(1, "Alice")).unwrap();
        let _ = table.scan_columnar().unwrap();

        // Batch insert more rows
        let rows = vec![create_row(2, "Bob"), create_row(3, "Charlie")];
        table.insert_batch(rows).unwrap();

        // Columnar scan should reflect all rows
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
            Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
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
