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

/// Compute the columnar index for a given physical row index.
///
/// The columnar table only stores live (non-deleted) rows, so the columnar
/// index is the physical index minus the count of deleted rows before it.
///
/// This is a free function to avoid borrow conflicts when both `self.deleted`
/// and `self.native_columnar` need to be accessed simultaneously.
#[inline]
fn columnar_index_in(deleted: &[bool], physical_index: usize) -> usize {
    let deleted_before = deleted[..physical_index].iter().filter(|&&d| d).count();
    physical_index - deleted_before
}

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
/// ## Columnar Storage
///
/// Columnar tables maintain their columnar representation incrementally:
/// - **INSERT**: O(m) append per row where m = number of columns (no full rebuild)
/// - **UPDATE**: O(m) in-place column update (no full rebuild)
/// - **DELETE**: Full rebuild (O(n * m)) since deletion requires row compaction
/// - **TRUNCATE**: O(1) clear
///
/// This makes columnar tables viable for moderate write workloads, not just
/// bulk-loaded analytical data. The row store is maintained alongside the
/// columnar store for indexing and constraint validation.
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

    /// Largest *signed* effective rowid ever assigned in this table
    /// (issue #5835). `None` until the first row is inserted.
    ///
    /// SQLite rowids are signed 64-bit integers (they can be negative), and
    /// `Row::row_id` stores the two's-complement bit pattern of that i64.
    /// This field tracks the maximum under *signed* interpretation:
    /// - explicit rowids contribute `row_id as i64` (so `-1`, stored as
    ///   `u64::MAX`, contributes `-1` — it can never poison allocation);
    /// - implicit rows (no `row_id`) contribute their effective rowid,
    ///   `physical position + 1`.
    ///
    /// Updated on `insert` / `insert_batch` / `update_row` (including rows
    /// reloaded from a v13+ snapshot or replayed from a v3+ WAL). Monotone:
    /// deletes never decrease it, so rowid allocation via
    /// [`Table::next_rowid`] can never collide with a rowid that is (or was)
    /// in use. Not serialized — rebuilt naturally on load because every
    /// reloaded row passes through `insert`.
    max_assigned_rowid: Option<i64>,
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
            max_assigned_rowid: self.max_assigned_rowid,
        }
    }
}

/// Rowid allocation exhausted the signed 64-bit rowid space (issue #5894).
///
/// sqlite3 surfaces this as `SQLITE_FULL` ("database or disk is full"): when
/// the maximum rowid is already `i64::MAX`, sqlite3 probes random unused
/// rowids rather than reusing `i64::MAX` (a silent duplicate) or overflowing;
/// if every probe collides with an existing rowid it gives up with this error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowidExhausted;

impl std::fmt::Display for RowidExhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Matches sqlite3's SQLITE_FULL text so callers can surface it verbatim.
        write!(f, "database or disk is full")
    }
}

impl std::error::Error for RowidExhausted {}

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
            max_assigned_rowid: None,
        }
    }

    /// Track a row's effective *signed* rowid for allocation (issue #5835,
    /// extended by issue #6173).
    ///
    /// For a table with an INTEGER PRIMARY KEY rowid alias
    /// (`schema.rowid_alias_column`), the alias COLUMN'S value IS the rowid —
    /// `row.row_id` is deliberately left unset for such rows (see the INSERT
    /// executor's "Skip INTEGER PRIMARY KEY (rowid alias) tables" comment),
    /// so falling back to `row.row_id`/physical position here would silently
    /// under-track the true max whenever the alias column's value diverges
    /// from insertion order — e.g. an `INSERT INTO t(b) VALUES(...)` that
    /// omits the named IPK column, following an earlier row whose IPK value
    /// was itself non-sequential (an explicit large value, a gap left by
    /// DELETE, or — the case that surfaced this — a BEFORE/AFTER INSERT
    /// trigger recursively inserting extra rows into the same table). Without
    /// this, a later NULL/omitted-IPK insert could recompute the SAME "next"
    /// value twice and collide with a row already written (autoinc-3928).
    ///
    /// For any other table, `row_id` is the explicit rowid bit pattern if the
    /// row carries one; `position` is the physical index the row occupies,
    /// from which an implicit rowid (`position + 1`) is derived otherwise.
    #[inline]
    fn track_effective_rowid(&mut self, row: &Row, position: usize) {
        let alias_value = self
            .schema
            .rowid_alias_column
            .and_then(|idx| row.values.get(idx))
            .and_then(|v| if let SqlValue::Integer(i) = v { Some(*i) } else { None });
        let effective: i64 = match alias_value {
            Some(v) => v,
            // SQLite rowids are signed; reinterpret the stored bit pattern.
            None => match row.row_id {
                Some(rid) => rid as i64,
                None => position as i64 + 1,
            },
        };
        self.max_assigned_rowid =
            Some(self.max_assigned_rowid.map_or(effective, |m| m.max(effective)));
    }

    /// Check if this table uses native columnar storage
    pub fn is_native_columnar(&self) -> bool {
        self.native_columnar.is_some()
    }

    /// Insert a row into the table
    ///
    /// For row-oriented tables, rows are stored directly in a Vec.
    /// For columnar tables, the row is appended to the columnar data incrementally
    /// (O(m) where m = columns, instead of O(n * m) full rebuild).
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

        // Track the largest effective rowid ever assigned (issue #5835) so
        // future implicit-rowid allocation never collides with it.
        self.track_effective_rowid(&normalized_row, self.rows.len());

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

        // For native columnar tables, incrementally append to columnar data
        // This is O(m) per row instead of O(n*m) full rebuild
        if let Some(ref mut columnar) = self.native_columnar {
            columnar
                .append_row(&normalized_row)
                .map_err(|e| StorageError::Other(format!("Columnar append failed: {}", e)))?;
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
    /// - **Incremental columnar**: For native columnar tables, appends O(batch * m) instead of O(n * m) rebuild
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
            // Track the largest effective rowid ever assigned (issue #5835).
            self.track_effective_rowid(&row, self.rows.len());
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
        // For native columnar tables, incrementally append new rows to columnar data
        // This is O(batch_size * m) instead of O(n * m) full rebuild
        if let Some(ref mut columnar) = self.native_columnar {
            for row in &self.rows[start_index..] {
                columnar
                    .append_row(row)
                    .map_err(|e| StorageError::Other(format!("Columnar append failed: {}", e)))?;
            }
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
        self.rows.iter().enumerate().filter(|(idx, _)| !self.deleted[*idx])
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
                let mut cloned = row.clone();
                // Set row_id for ROWID pseudo-column support (SQLite compatibility)
                // If the row already has an explicit row_id (from INSERT INTO t(rowid,...)),
                // preserve it. Otherwise, use 1-indexed physical index.
                if cloned.row_id.is_none() {
                    cloned.row_id = Some((idx + 1) as u64);
                }
                result.push(cloned);
            }
        }
        result
    }

    /// Scan only live (non-deleted) rows that are also **MVCC-visible**
    /// to `snapshot`, returning an owned Vec.
    ///
    /// # Phase 1d of #5136
    ///
    /// This is the read-path chokepoint that wires
    /// [`Row::visible_to`](crate::Row::visible_to) into the SELECT scan
    /// boundary.
    ///
    /// - With the `mvcc_enabled` feature **OFF** (default), this method
    ///   behaves identically to [`scan_live_vec`](Self::scan_live_vec):
    ///   the snapshot argument is ignored and every live row is returned.
    ///   This preserves bit-for-bit pre-MVCC behavior for builds without
    ///   the feature flag.
    /// - With the `mvcc_enabled` feature **ON**, rows additionally have
    ///   `Row::visible_to(snapshot)` applied. Rows that fail visibility
    ///   are filtered out. The `snapshot` is typically the transaction's
    ///   BEGIN-time snapshot (from
    ///   [`crate::Database::current_snapshot`]); auto-commit reads pass
    ///   [`TxnSnapshot::empty`](crate::mvcc::TxnSnapshot::empty) which
    ///   under Phase 1c's write semantics is equivalent to "show only
    ///   pre-MVCC rows + my-own writes" — but under the empty snapshot,
    ///   no MVCC rows are visible. The executor must be careful to pass
    ///   the right snapshot here; see `select::scan::table::execute_table_scan`
    ///   for the canonical wiring.
    ///
    /// # Performance
    /// O(n) time and space where n is the number of live rows.
    /// With the feature OFF, this is exactly the same code path as
    /// `scan_live_vec`; no extra branch is executed per row.
    #[inline]
    pub fn scan_visible_vec(&self, snapshot: &crate::mvcc::TxnSnapshot) -> Vec<Row> {
        // With the feature OFF, the snapshot argument is dead; defer to
        // the pre-MVCC scan_live_vec path so we are guaranteed bit-for-bit
        // identical behavior to today.
        #[cfg(not(feature = "mvcc_enabled"))]
        {
            let _ = snapshot;
            return self.scan_live_vec();
        }

        #[cfg(feature = "mvcc_enabled")]
        {
            let mut result = Vec::with_capacity(self.row_count());
            for (idx, row) in self.rows.iter().enumerate() {
                if self.deleted[idx] {
                    continue;
                }
                if !row.visible_to(snapshot) {
                    continue;
                }
                let mut cloned = row.clone();
                if cloned.row_id.is_none() {
                    cloned.row_id = Some((idx + 1) as u64);
                }
                result.push(cloned);
            }
            result
        }
    }

    /// Iterate over live rows that are also MVCC-visible to `snapshot`.
    ///
    /// See [`scan_visible_vec`](Self::scan_visible_vec) for the
    /// feature-flag semantics. With the `mvcc_enabled` feature OFF, this
    /// is exactly [`scan_live`](Self::scan_live) and the snapshot is
    /// ignored.
    #[inline]
    pub fn scan_visible<'a>(
        &'a self,
        snapshot: &'a crate::mvcc::TxnSnapshot,
    ) -> impl Iterator<Item = (usize, &'a Row)> + 'a {
        self.rows.iter().enumerate().filter(move |(idx, row)| {
            if self.deleted[*idx] {
                return false;
            }
            #[cfg(feature = "mvcc_enabled")]
            {
                row.visible_to(snapshot)
            }
            #[cfg(not(feature = "mvcc_enabled"))]
            {
                let _ = (row, snapshot);
                true
            }
        })
    }

    /// Check whether `row` (at physical index `idx`) is visible to
    /// `snapshot`, accounting for both the deletion bitmap and MVCC
    /// `xmin`/`xmax`.
    ///
    /// Returns `false` if the row is deleted via the bitmap. With the
    /// `mvcc_enabled` feature OFF, the snapshot is ignored and the
    /// answer is purely "is this row not deletion-bitmap-tombstoned?".
    /// With the feature ON, the row must also pass
    /// [`Row::visible_to`](crate::Row::visible_to).
    #[inline]
    pub fn is_row_visible(&self, idx: usize, snapshot: &crate::mvcc::TxnSnapshot) -> bool {
        if idx >= self.deleted.len() || self.deleted[idx] {
            return false;
        }
        #[cfg(feature = "mvcc_enabled")]
        {
            match self.rows.get(idx) {
                Some(row) => row.visible_to(snapshot),
                None => false,
            }
        }
        #[cfg(not(feature = "mvcc_enabled"))]
        {
            let _ = snapshot;
            idx < self.rows.len()
        }
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

    /// Largest *signed* effective rowid ever assigned, or `None` if no row
    /// was ever inserted (issue #5835).
    ///
    /// SQLite rowids are signed 64-bit integers; `Row::row_id` stores the
    /// two's-complement bit pattern. This is the signed maximum over every
    /// effective rowid ever assigned (explicit rowids as `row_id as i64`,
    /// implicit rows as `physical position + 1`). Monotone across deletes.
    #[inline]
    pub fn max_rowid_signed(&self) -> Option<i64> {
        self.max_assigned_rowid
    }

    /// Next rowid to allocate for an implicit-rowid insert, as a *signed*
    /// value (issue #5835).
    ///
    /// SQLite semantics (verified against sqlite3): the next implicit rowid
    /// is the signed maximum existing rowid + 1, or 1 for a table that never
    /// held a row. Negative maxima yield negative-or-zero allocations (after
    /// `INSERT INTO t(rowid,x) VALUES(-1,5)`, the next implicit rowid is 0),
    /// exactly matching sqlite3.
    ///
    /// `max_assigned_rowid` covers both implicit rowids (tracked as
    /// physical position + 1 at insert time, preserving the pre-existing
    /// `physical_row_count + 1` allocation for purely implicit tables) and
    /// explicit rowids — including rows reloaded from disk, whose persisted
    /// rowids can exceed the (compacted) physical count. Without the latter,
    /// a reloaded table with rowids {1, 3} would hand out rowid 3 again.
    ///
    /// Saturating: a table whose max rowid is `i64::MAX` allocates
    /// `i64::MAX` again rather than overflowing (SQLite reports SQLITE_FULL
    /// here; a duplicate-rowid error is the closest safe behavior).
    #[inline]
    pub fn next_rowid_signed(&self) -> i64 {
        self.max_assigned_rowid.map_or(1, |m| m.saturating_add(1))
    }

    /// [`Table::next_rowid_signed`] as the u64 bit pattern stored in
    /// `Row::row_id` (two's complement — e.g. an allocation of `-4` is
    /// returned as `(-4i64) as u64`).
    #[inline]
    pub fn next_rowid(&self) -> u64 {
        self.next_rowid_signed() as u64
    }

    /// Allocate the next rowid for an auto-assigned insert, matching sqlite3
    /// exactly — including the `i64::MAX` corner that [`Table::next_rowid_signed`]
    /// only saturates over (issue #5894).
    ///
    /// This is the fallible allocator that insert paths must use for any rowid
    /// that gets *stored* (implicit rowids, explicit NULL/DEFAULT rowids, and
    /// `INTEGER PRIMARY KEY` NULL auto-assign). Semantics:
    ///
    /// - empty table (never held a row): `1`;
    /// - otherwise the signed maximum rowid `+ 1` (negative maxima included —
    ///   a table whose max rowid is `-5` allocates `-4`, matching sqlite3);
    /// - when the maximum rowid is already `i64::MAX`, sqlite3 does not reuse it
    ///   (a silent duplicate) or overflow — it probes up to 100 random positive
    ///   rowids for one not currently in use, returning [`RowidExhausted`]
    ///   (`SQLITE_FULL`) only if every probe collides.
    ///
    /// The random-probe fallback is inherently nondeterministic (as it is in
    /// sqlite3): callers get *some* unused rowid, not a predictable one.
    pub fn allocate_rowid(&self) -> Result<i64, RowidExhausted> {
        match self.max_assigned_rowid {
            None => Ok(1),
            // `m < i64::MAX` guarantees `m + 1` cannot overflow.
            Some(m) if m < i64::MAX => Ok(m + 1),
            Some(_) => self.probe_random_rowid(),
        }
    }

    /// [`Table::allocate_rowid`] as the u64 bit pattern stored in `Row::row_id`
    /// (two's complement — an allocation of `-4` is returned as `(-4i64) as u64`).
    #[inline]
    pub fn allocate_rowid_u64(&self) -> Result<u64, RowidExhausted> {
        self.allocate_rowid().map(|r| r as u64)
    }

    /// sqlite3's random-rowid fallback: probe positive rowids in `[1, i64::MAX)`
    /// for one not currently in use, giving up after 100 attempts (issue #5894).
    ///
    /// The set of in-use effective rowids is snapshotted once (live rows only —
    /// deleted rowids are free) so each probe is an O(1) membership test.
    fn probe_random_rowid(&self) -> Result<i64, RowidExhausted> {
        use rand::RngExt;

        let in_use: std::collections::HashSet<i64> = self
            .scan_live()
            .map(|(pos, row)| row.row_id.map_or(pos as i64 + 1, |rid| rid as i64))
            .collect();

        let mut rng = rand::rng();
        for _ in 0..100 {
            let candidate = rng.random_range(1..i64::MAX);
            if !in_use.contains(&candidate) {
                return Ok(candidate);
            }
        }
        Err(RowidExhausted)
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
    ///
    /// Used by the full-table clear paths (`TRUNCATE TABLE` and the no-`WHERE`
    /// `DELETE FROM t` fast path). Resets rowid allocation: an emptied table's
    /// next rowid is `1`, matching sqlite3 (`max(rowid)` of an empty table is
    /// NULL, so allocation restarts at 1) and the AUTO_INCREMENT-reset contract
    /// (issue #5894). Since `allocate_rowid` now reads `max_assigned_rowid`, the
    /// counter must be cleared here — otherwise a truncated table would keep
    /// allocating past its pre-truncate maximum.
    pub fn clear(&mut self) {
        self.rows.clear();
        self.deleted.clear();
        self.deleted_count = 0;
        self.max_assigned_rowid = None;
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

        // Track the largest explicit rowid ever assigned (issue #5835), or
        // (issue #6173) the largest INTEGER PRIMARY KEY alias value: an
        // UPDATE replaces a row in place, so a row with neither an explicit
        // `row_id` NOR a rowid-alias column cannot introduce a new effective
        // rowid beyond what its insert already tracked — but `UPDATE t SET
        // <ipk_col>=<bigger value>` on a rowid-alias table changes the
        // row's *effective* rowid without ever touching `row_id`, and that
        // new high-water mark must still be tracked (same reasoning as
        // `track_effective_rowid`'s doc comment on the INSERT paths).
        if normalized_row.row_id.is_some() || self.schema.rowid_alias_column.is_some() {
            self.track_effective_rowid(&normalized_row, index);
        }

        // Get old row for index updates (clone to avoid borrow issues)
        let old_row = self.rows[index].clone();

        // Update the row
        self.rows[index] = normalized_row.clone();

        // Update indexes (delegate to IndexManager)
        self.indexes.update_for_update(&self.schema, &old_row, &normalized_row, index);

        // For native columnar tables, incrementally update the columnar row
        if let Some(ref mut columnar) = self.native_columnar {
            let columnar_idx = columnar_index_in(&self.deleted, index);
            columnar
                .update_row_at(columnar_idx, &normalized_row)
                .map_err(|e| StorageError::Other(format!("Columnar update failed: {}", e)))?;
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

        // Update the row
        self.rows[index] = normalized_row;

        // For native columnar tables, incrementally update the columnar row
        if let Some(ref mut columnar) = self.native_columnar {
            let columnar_idx = columnar_index_in(&self.deleted, index);
            columnar
                .update_row_at(columnar_idx, &self.rows[index])
                .map_err(|e| StorageError::Other(format!("Columnar update failed: {}", e)))?;
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
        self.indexes.update_selective(&self.schema, old_row, &new_row, index, &affected_indexes);

        // Update the row (direct move, no validation)
        self.rows[index] = new_row;

        // For native columnar tables, incrementally update the columnar row
        // (Row-oriented tables rely on Database::invalidate_columnar_cache from the executor)
        if let Some(ref mut columnar) = self.native_columnar {
            let columnar_idx = columnar_index_in(&self.deleted, index);
            // Ignore errors in unchecked path (matching the no-validation contract)
            let _ = columnar.update_row_at(columnar_idx, &self.rows[index]);
        }
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

        // For native columnar tables, update the specific column value incrementally
        // (Row-oriented tables rely on Database::invalidate_columnar_cache from the executor)
        if let Some(ref mut columnar) = self.native_columnar {
            let columnar_idx = columnar_index_in(&self.deleted, row_index);
            let col_name = &self.schema.columns[col_index].name;
            if let Some(column) = columnar.get_column_mut(col_name) {
                let _ = column.set_value(columnar_idx, &self.rows[row_index].values[col_index]);
            }
        }
    }

    /// Stamp the xmin field of an in-storage row in place.
    ///
    /// Used by Phase 1c (#5150 of #5136) super-fast UPDATE paths that
    /// mutate individual column values in-place (no row clone) and so
    /// can't go through the normal `update_row*` flow which stamps the
    /// new row before insertion.
    ///
    /// The caller is responsible for gating on the `mvcc_enabled` feature
    /// (see [`crate::mvcc::stamp_xmin_for_write`]) — this method
    /// unconditionally writes the field. It is a no-op if `row_index` is
    /// out of bounds, mirroring the unchecked-style contract of
    /// `update_column_inplace`.
    #[inline]
    pub fn stamp_row_xmin_inplace(&mut self, row_index: usize, txn_id: crate::row::TxnId) {
        if let Some(row) = self.rows.get_mut(row_index) {
            row.xmin = txn_id;
        }
    }

    /// Stamp the xmax field of an in-storage row in place.
    ///
    /// Used by Phase 1c (#5150 of #5136) DELETE paths that bitmap-mark
    /// rows as deleted (rather than physically removing them) — the xmax
    /// stamp is the MVCC-visible deletion record that Phase 1d's
    /// visibility filter will consult. With the `mvcc_enabled` feature
    /// off this method should not be called (callers branch on the
    /// feature flag); with the feature on it records `xmax = Some(txn_id)`
    /// so the row, while still bitmap-deleted today, also carries a
    /// proper MVCC tombstone.
    ///
    /// The caller is responsible for gating on the feature flag. The
    /// method is a no-op if `row_index` is out of bounds.
    #[inline]
    pub fn stamp_row_xmax_inplace(&mut self, row_index: usize, txn_id: crate::row::TxnId) {
        if let Some(row) = self.rows.get_mut(row_index) {
            row.xmax = Some(txn_id);
        }
    }

    /// Bitmap-delete a single row by index, updating internal indexes, WITHOUT
    /// triggering compaction.
    ///
    /// This is used by the interleaved per-row DELETE trigger path (#5486),
    /// where row triggers fire BEFORE -> delete -> AFTER for each row in turn.
    /// Per-row deletion must not compact mid-loop, because compaction shifts
    /// every row index and would invalidate the indices of the not-yet-processed
    /// rows the caller still holds. The caller is responsible for calling
    /// [`Table::compact_if_needed`] once after the loop completes.
    ///
    /// Returns `true` if the row was newly deleted, `false` if the index was
    /// out of bounds or the row was already deleted.
    pub fn mark_deleted_inplace(&mut self, idx: usize) -> bool {
        if idx >= self.rows.len() || self.deleted[idx] {
            return false;
        }

        // Update internal hash indexes for this row BEFORE marking deleted,
        // mirroring `delete_by_indices`.
        let row = &self.rows[idx];
        self.indexes.update_for_delete(&self.schema, row);

        self.deleted[idx] = true;
        self.deleted_count += 1;

        // For native columnar tables, rebuild columnar data so reads (e.g. a
        // trigger body's SELECT) observe the deletion immediately.
        if self.native_columnar.is_some() {
            let _ = self.rebuild_native_columnar();
        }

        true
    }

    /// Compact the table if it has crossed the deletion threshold, returning
    /// whether compaction actually occurred.
    ///
    /// Companion to [`Table::mark_deleted_inplace`]: the interleaved per-row
    /// DELETE path defers compaction until after all rows are processed, then
    /// calls this once. When it returns `true` the caller must rebuild
    /// user-defined / expression / partial indexes since all row indices moved.
    pub fn compact_if_needed(&mut self) -> bool {
        if self.should_compact() {
            self.compact();
            if self.native_columnar.is_some() {
                let _ = self.rebuild_native_columnar();
            }
            true
        } else {
            false
        }
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

        // Build new vectors with only live rows.
        //
        // Rowid stability (issue #5835): a row without an explicit rowid
        // derives its implicit rowid from its physical position (idx + 1).
        // Compaction shifts physical positions, so materialize each
        // surviving implicit rowid BEFORE the shift — otherwise `WHERE
        // rowid=N` would silently target a different row after compaction.
        let mut new_rows = Vec::with_capacity(self.rows.len() - self.deleted_count);
        let mut max_effective: Option<i64> = self.max_assigned_rowid;
        for (idx, row) in self.rows.iter().enumerate() {
            if !self.deleted[idx] {
                let mut row = row.clone();
                let effective = row.row_id.unwrap_or(idx as u64 + 1);
                row.row_id = Some(effective);
                // Signed interpretation (issue #5835): negative rowids are
                // stored as two's-complement bit patterns and must never
                // poison allocation tracking.
                let signed = effective as i64;
                max_effective = Some(max_effective.map_or(signed, |m| m.max(signed)));
                new_rows.push(row);
            }
        }
        self.max_assigned_rowid = max_effective;

        // Replace old vectors with compacted ones
        self.rows = new_rows;
        self.deleted = vec![false; self.rows.len()];
        self.deleted_count = 0;

        // Rebuild all indexes since row positions have changed
        self.indexes.rebuild(&self.schema, &self.rows);
    }

    /// MVCC garbage collection: physically remove row versions whose
    /// deletion is committed and provably invisible to every active
    /// reader.
    ///
    /// # Phase 1d follow-up (#5208)
    ///
    /// Walks every row currently in storage and, for any row whose
    /// `xmax = Some(t)` with `t < horizon`, marks it for bitmap
    /// deletion. `horizon` is computed by
    /// [`TransactionManager::compute_gc_horizon`] — see its docs for
    /// the meaning of "horizon."
    ///
    /// **What does NOT count as reclaimable:**
    /// - Rows with `xmax = None` (still live) — never removed by GC.
    /// - Rows with `xmax = Some(t)` where `t >= horizon` — some active
    ///   or future reader may still need to see this row, so we leave
    ///   it alone.
    /// - Rows already in the deletion bitmap — counted as "already
    ///   reclaimed."
    ///
    /// **Off-state (`mvcc_enabled` feature OFF):** this method is still
    /// callable, but rows constructed by the executor have
    /// `xmax = None` in the off-state, so the sweep finds nothing and
    /// returns 0. Calling this is therefore harmless when MVCC is
    /// disabled — useful for keeping the public API surface stable
    /// across feature configurations.
    ///
    /// # Returns
    ///
    /// The number of rows newly marked deletable by this call. If that
    /// crossed the compaction threshold (> 50% deleted), the underlying
    /// row vector is compacted before returning, and the
    /// [`DeleteResult::compacted`] bit is implicit in the caller's
    /// follow-up: the caller should call
    /// [`Database::rebuild_indexes`] on the table when this returns
    /// non-zero, to keep user-defined B-tree indexes in sync.
    ///
    /// # Performance
    ///
    /// O(n) scan over physical rows. There is no incremental
    /// bookkeeping — v1 GC is a single sweep. Future phases can add
    /// per-table "needs-GC" flags / smaller per-page sweeps.
    ///
    /// [`TransactionManager::compute_gc_horizon`]: crate::database::transactions::TransactionManager::compute_gc_horizon
    /// [`Database::rebuild_indexes`]: crate::Database
    /// [`DeleteResult::compacted`]: crate::table::DeleteResult
    pub fn gc_old_versions(&mut self, horizon: crate::row::TxnId) -> usize {
        // Off-state: no row in the off-state carries an xmax stamp
        // (the executor's `stamp_xmax_for_write` is a no-op when the
        // feature is off), so the sweep predicate would find nothing.
        // Short-circuit to skip the iteration entirely; this keeps the
        // public API surface stable while paying zero cost when MVCC
        // is compiled out.
        if !cfg!(feature = "mvcc_enabled") {
            let _ = horizon;
            return 0;
        }

        // PRE_MVCC_TXN_ID (= 0) is the "no deleter / always committed"
        // sentinel. The horizon is always >= 1 in any non-empty manager
        // (next_transaction_id starts at 1), so the strict `<` check
        // below treats sentinel-stamped deletes (which shouldn't happen
        // in practice) as not-reclaimable — defensive against bizarre
        // states.
        let mut indices_to_gc: Vec<usize> = Vec::new();
        for (idx, row) in self.rows.iter().enumerate() {
            if self.deleted[idx] {
                continue;
            }
            if let Some(xmax) = row.xmax {
                if xmax != crate::row::PRE_MVCC_TXN_ID && xmax < horizon {
                    indices_to_gc.push(idx);
                }
            }
        }

        if indices_to_gc.is_empty() {
            return 0;
        }

        let result = self.delete_by_indices(&indices_to_gc);
        result.deleted_count
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

    // ========================================================================
    // GC tests (#5208 — MVCC Phase 1d follow-up)
    // ========================================================================

    #[test]
    fn gc_off_state_returns_zero() {
        // Off-state: no rows carry an xmax stamp, so GC has nothing to do.
        // The early-return in `gc_old_versions` should fire and report 0.
        let mut table = create_test_table();
        table.insert(create_row(1, "Alice")).unwrap();
        table.insert(create_row(2, "Bob")).unwrap();
        let reclaimed = table.gc_old_versions(100);
        assert_eq!(reclaimed, 0);
        // No rows should have been removed.
        assert_eq!(table.row_count(), 2);
    }

    #[cfg(feature = "mvcc_enabled")]
    #[test]
    fn gc_reclaims_only_rows_with_xmax_below_horizon() {
        // Three rows: one with xmax = 5 (committed before horizon),
        // one with xmax = 10 (still in horizon — must be retained),
        // one with xmax = None (live — must be retained).
        let mut table = create_test_table();
        table.insert(create_row(1, "Alice")).unwrap();
        table.insert(create_row(2, "Bob")).unwrap();
        table.insert(create_row(3, "Charlie")).unwrap();

        // Stamp xmax directly on the underlying rows to simulate
        // Phase 1c's UPDATE/DELETE having stamped tombstones.
        table.stamp_row_xmax_inplace(0, 5);
        table.stamp_row_xmax_inplace(1, 10);
        // Row 2 left with xmax = None.

        let reclaimed = table.gc_old_versions(8);
        // Only row 0 (xmax = 5 < horizon = 8) should have been reclaimed.
        assert_eq!(reclaimed, 1);
        // The other two should still be present (one tombstoned, one live).
        assert_eq!(table.row_count(), 2);
    }

    #[cfg(feature = "mvcc_enabled")]
    #[test]
    fn gc_reclaims_nothing_when_horizon_is_zero() {
        // Horizon = 0 means "no reader can possibly be done with anything"
        // — every committed deletion must be retained. The strict `<`
        // comparison combined with the sentinel check guarantees nothing
        // is reclaimed even if a row was somehow stamped with xmax = 0
        // (which would be a bug; the sentinel means "no deleter").
        let mut table = create_test_table();
        table.insert(create_row(1, "Alice")).unwrap();
        table.stamp_row_xmax_inplace(0, 5);

        let reclaimed = table.gc_old_versions(0);
        assert_eq!(reclaimed, 0);
        assert_eq!(table.row_count(), 1);
    }

    #[cfg(feature = "mvcc_enabled")]
    #[test]
    fn gc_skips_rows_already_in_deletion_bitmap() {
        // Row 0 is in the bitmap from a prior DELETE; row 1 has xmax
        // stamped but no bitmap mark (the deferred case Phase 1e will
        // produce). GC should reclaim row 1 only.
        let mut table = create_test_table();
        table.insert(create_row(1, "Alice")).unwrap();
        table.insert(create_row(2, "Bob")).unwrap();
        // Bitmap-delete row 0 the "normal" way (also stamps it in the
        // index manager, so PK lookup is consistent).
        let _ = table.delete_by_indices(&[0]);
        // Row 1: stamp tombstone but don't bitmap-delete.
        table.stamp_row_xmax_inplace(1, 4);

        let reclaimed = table.gc_old_versions(10);
        // Row 0 was already deleted — not counted again. Row 1 is now
        // bitmap-deleted by GC.
        assert_eq!(reclaimed, 1);
        // Both physical slots are now in the deletion bitmap; row_count
        // (live rows) is 0.
        assert_eq!(table.row_count(), 0);
    }

    #[cfg(feature = "mvcc_enabled")]
    #[test]
    fn gc_respects_horizon_boundary() {
        // Boundary: xmax == horizon is NOT reclaimed (only strictly less).
        // This matches the semantics of `xmin_active`: a row whose
        // deleter id equals an active reader's xmin_active could still
        // be in that reader's snapshot under some future visibility
        // rule, so we err on the side of retaining it.
        let mut table = create_test_table();
        table.insert(create_row(1, "Alice")).unwrap();
        table.stamp_row_xmax_inplace(0, 7);

        // Horizon == 7: row is NOT reclaimed.
        let reclaimed = table.gc_old_versions(7);
        assert_eq!(reclaimed, 0);

        // Horizon == 8: row IS reclaimed.
        let reclaimed = table.gc_old_versions(8);
        assert_eq!(reclaimed, 1);
    }
}
