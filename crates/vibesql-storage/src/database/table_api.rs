// ============================================================================
// Table Operations API
// ============================================================================
//
// This module provides table management methods for the Database struct.
// Includes create, drop, insert, update operations.

use vibesql_catalog::{TableIdentifier, TableSchema};

use super::Database;
use crate::{
    change_events::{ChangeEvent, ChangeEventPk},
    wal::WalOp,
    Row, StorageError, Table,
};

/// Extract the single-column primary-key identity (column name + value) of `row`
/// for the given `schema`, for stamping onto a [`ChangeEvent`] (#5472).
///
/// Returns `None` — disabling PK pruning, so consumers re-query — when:
/// - the table has no primary key, or
/// - the primary key is composite (more than one column), or
/// - the PK column cannot be resolved / is out of bounds in `row`.
///
/// Only single-column PKs are supported by design: the conservative predicate
/// analyzer in the server can only reason about a single key column, and a
/// `None` here is always safe (it just falls back to re-querying).
fn single_pk_identity(schema: &TableSchema, row: &Row) -> Option<ChangeEventPk> {
    let pk_cols = schema.primary_key.as_ref()?;
    if pk_cols.len() != 1 {
        return None;
    }
    let col_name = &pk_cols[0];
    let idx = schema.get_column_index(col_name)?;
    let value = row.values.get(idx)?.clone();
    // Carry the canonical (lower-cased) column name so the server can match it
    // case-insensitively against the subscription's WHERE predicate.
    Some(ChangeEventPk::single(col_name.to_lowercase(), value))
}

impl Database {
    // ============================================================================
    // Table Operations
    // ============================================================================

    /// Check if a table name refers to a session-scoped table that must NOT be
    /// persisted to WAL: a temporary table (in any temp schema) or a table in
    /// an ATTACHed database schema (session-scoped in Phase 1 of #6310 —
    /// attached schemas are never persisted into the main database's WAL or
    /// snapshot; file-backed attachment persistence is #6362).
    ///
    /// This checks if the table is in ANY temp schema (temp_1, temp_2, etc.)
    /// or in any currently attached schema.
    pub(super) fn is_temp_table(&self, table_name: &str) -> bool {
        // Check if the table name is qualified with a temp or attached schema
        // prefix (e.g., "temp_1.foo", "aux.foo")
        if let Some((schema, _)) = table_name.split_once('.') {
            vibesql_catalog::Catalog::is_temp_schema(schema)
                || self.catalog.is_attached_schema(schema)
        } else {
            // Unqualified name - check if it exists in this session's temp schema
            let temp_qualified =
                format!("{}.{}", self.catalog.temp_schema_name(), table_name.to_lowercase());
            self.tables.contains_key(&temp_qualified)
        }
    }

    /// Create a table with SQL:1999 identifier semantics.
    ///
    /// The `identifier` parameter determines how the table name is stored:
    /// - Quoted identifiers: stored with exact case
    /// - Unquoted identifiers: stored with lowercase canonical form
    /// - Qualified identifiers: schema and table have independent case handling
    ///
    /// Temporary tables (in the "temp" schema) are not persisted to WAL.
    pub fn create_table_with_identifier(
        &mut self,
        schema: vibesql_catalog::TableSchema,
        identifier: TableIdentifier,
    ) -> Result<(), StorageError> {
        self.catalog
            .create_table_with_identifier(schema.clone(), identifier.clone())
            .map_err(|e| StorageError::CatalogError(e.to_string()))?;

        // Build qualified name from identifier
        let qualified_name = if identifier.is_qualified() {
            // Identifier already includes schema qualification
            identifier.canonical().to_string()
        } else {
            // Add current schema to unqualified identifier
            let current_schema = &self.catalog.get_current_schema();
            format!("{}.{}", current_schema, identifier.canonical())
        };

        // Check if this is a session-scoped table (in any temp schema, or in
        // an ATTACHed database schema). Neither is persisted to WAL.
        let is_temp = identifier.schema_canonical().is_some_and(|s| {
            vibesql_catalog::Catalog::is_temp_schema(s) || self.catalog.is_attached_schema(s)
        });

        if !is_temp {
            // Assign table ID and emit WAL entry for persistence
            let table_id = self.next_table_id();

            // Serialize schema for WAL (use a simple binary format)
            let schema_data = serialize_table_schema(&schema);

            self.emit_wal_op(WalOp::CreateTable {
                table_id,
                table_name: qualified_name.clone(),
                schema_data,
            });
        }

        let table = Table::new(schema);
        self.tables.insert(qualified_name, table);

        Ok(())
    }

    /// Create a table
    /// Legacy method - uses global case_sensitive_identifiers setting
    pub fn create_table(
        &mut self,
        schema: vibesql_catalog::TableSchema,
    ) -> Result<(), StorageError> {
        let table_name = schema.name.clone();

        self.operations.create_table(&mut self.catalog, schema.clone())?;

        // Normalize table name for storage (matches catalog normalization)
        let normalized_table_name = if self.catalog.is_case_sensitive_identifiers() {
            table_name.clone()
        } else {
            table_name.to_lowercase()
        };

        let current_schema = &self.catalog.get_current_schema();
        let qualified_name = format!("{}.{}", current_schema, normalized_table_name);

        // Assign table ID and emit WAL entry for persistence
        let table_id = self.next_table_id();

        // Serialize schema for WAL (use a simple binary format)
        let schema_data = serialize_table_schema(&schema);

        self.emit_wal_op(WalOp::CreateTable {
            table_id,
            table_name: qualified_name.clone(),
            schema_data,
        });

        let table = Table::new(schema);
        self.tables.insert(qualified_name, table);

        Ok(())
    }

    /// Get a table by identifier using SQL:1999 case semantics.
    ///
    /// Uses the canonical form of the identifier for direct lookup without fallbacks.
    /// Supports both simple and schema-qualified identifiers.
    pub fn get_table_by_identifier(&self, identifier: &TableIdentifier) -> Option<&Table> {
        let qualified_name = if identifier.is_qualified() {
            // Identifier already includes schema qualification
            identifier.canonical().to_string()
        } else {
            // Add current schema to unqualified identifier
            let current_schema = &self.catalog.get_current_schema();
            format!("{}.{}", current_schema, identifier.canonical())
        };
        self.tables.get(&qualified_name)
    }

    /// Get a table for reading
    /// Legacy method with fallback lookups for backward compatibility
    ///
    /// For unqualified names, checks temp schema first (SQLite semantics).
    /// SQLite Compatibility: The "temp" schema name is mapped to the session's
    /// temp schema, allowing `temp.tablename` syntax.
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        // For qualified names with "temp" schema, resolve to session's temp schema
        // This enables `SELECT * FROM temp.t1` syntax
        let resolved_name = if let Some((schema_part, table_part)) = name.split_once('.') {
            if schema_part.eq_ignore_ascii_case(vibesql_catalog::TEMP_SCHEMA) {
                std::borrow::Cow::Owned(format!(
                    "{}.{}",
                    self.catalog.temp_schema_name(),
                    table_part
                ))
            } else {
                std::borrow::Cow::Borrowed(name)
            }
        } else {
            std::borrow::Cow::Borrowed(name)
        };
        let name = resolved_name.as_ref();

        // Try the name as-is first (for delimited identifiers)
        if let Some(table) = self.tables.get(name) {
            return Some(table);
        }

        // Try lowercase normalization (standard for unquoted identifiers)
        let lowercase_name = name.to_lowercase();
        if lowercase_name != name {
            if let Some(table) = self.tables.get(&lowercase_name) {
                return Some(table);
            }
        }

        // Try uppercase normalization (for backward compatibility with old data)
        let uppercase_name = name.to_uppercase();
        if uppercase_name != name && uppercase_name != lowercase_name {
            if let Some(table) = self.tables.get(&uppercase_name) {
                return Some(table);
            }
        }

        // For qualified names (schema.table), try normalizing each part separately
        // This handles the case where storage normalized table name but not schema
        if let Some((schema_part, table_part)) = name.split_once('.') {
            // Try schema lowercase, table uppercase (current storage behavior)
            let mixed_case =
                format!("{}.{}", schema_part.to_lowercase(), table_part.to_uppercase());
            if mixed_case != name && mixed_case != uppercase_name && mixed_case != lowercase_name {
                if let Some(table) = self.tables.get(&mixed_case) {
                    return Some(table);
                }
            }
        }

        // For unqualified names, check session's temp schema first (SQLite semantics)
        // Temp tables shadow tables in the main schema
        if !name.contains('.') {
            // If unqualified resolution is restricted to a single schema
            // (trigger-body execution, #6477), look up ONLY there — mirrors
            // `Catalog::get_table`'s restriction so a trigger's DML physically
            // writes to the same table its body's name resolution found,
            // instead of falling back to `main`/temp/other attachments below.
            if let Some(restrict_schema) = self.catalog.unqualified_resolution_restricted_to() {
                return super::operations::find_restricted_table_key(
                    &self.tables,
                    &restrict_schema,
                    &lowercase_name,
                )
                .and_then(|key| self.tables.get(&key));
            }

            // Check session's temp schema first
            let temp_qualified = format!("{}.{}", self.catalog.temp_schema_name(), lowercase_name);
            if let Some(table) = self.tables.get(&temp_qualified) {
                return Some(table);
            }

            let current_schema = &self.catalog.get_current_schema();

            // Try as-is with schema prefix
            let qualified_name_original = format!("{}.{}", current_schema, name);
            if let Some(table) = self.tables.get(&qualified_name_original) {
                return Some(table);
            }

            // Try uppercase with schema prefix
            let qualified_name_uppercase = format!("{}.{}", current_schema, uppercase_name);
            if qualified_name_uppercase != qualified_name_original {
                if let Some(table) = self.tables.get(&qualified_name_uppercase) {
                    return Some(table);
                }
            }

            // Try lowercase with schema prefix
            let qualified_name_lowercase = format!("{}.{}", current_schema, lowercase_name);
            if qualified_name_lowercase != qualified_name_original
                && qualified_name_lowercase != qualified_name_uppercase
            {
                if let Some(table) = self.tables.get(&qualified_name_lowercase) {
                    return Some(table);
                }
            }

            // Finally, check attached databases in attachment order (SQLite
            // searches temp, then main, then each ATTACHed database — #6310).
            for attached in self.catalog.attached_databases() {
                let attached_qualified = format!("{}.{}", attached.name, lowercase_name);
                if let Some(table) = self.tables.get(&attached_qualified) {
                    return Some(table);
                }
            }
        }

        None
    }

    /// Get a table for writing
    ///
    /// For unqualified names, checks temp schema first (SQLite semantics).
    /// SQLite Compatibility: The "temp" schema name is mapped to the session's
    /// temp schema, allowing `temp.tablename` syntax.
    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        // For qualified names with "temp" schema, resolve to session's temp schema
        // This enables `UPDATE temp.t1 SET ...` syntax
        let resolved_name = if let Some((schema_part, table_part)) = name.split_once('.') {
            if schema_part.eq_ignore_ascii_case(vibesql_catalog::TEMP_SCHEMA) {
                Some(format!("{}.{}", self.catalog.temp_schema_name(), table_part))
            } else {
                None
            }
        } else {
            None
        };
        let name = resolved_name.as_deref().unwrap_or(name);

        // Try the name as-is first (for delimited identifiers)
        if self.tables.contains_key(name) {
            return self.tables.get_mut(name);
        }

        // Try lowercase normalization (standard for unquoted identifiers)
        let lowercase_name = name.to_lowercase();
        if lowercase_name != name && self.tables.contains_key(&lowercase_name) {
            return self.tables.get_mut(&lowercase_name);
        }

        // Try uppercase normalization (for backward compatibility with old data)
        let uppercase_name = name.to_uppercase();
        if uppercase_name != name
            && uppercase_name != lowercase_name
            && self.tables.contains_key(&uppercase_name)
        {
            return self.tables.get_mut(&uppercase_name);
        }

        // For unqualified names, check session's temp schema first (SQLite semantics)
        // Temp tables shadow tables in the main schema
        if !name.contains('.') {
            // If unqualified resolution is restricted to a single schema
            // (trigger-body execution, #6477), look up ONLY there — see the
            // matching restriction in `Self::get_table`.
            if let Some(restrict_schema) =
                self.catalog.unqualified_resolution_restricted_to().map(|s| s.to_string())
            {
                let restricted_key = super::operations::find_restricted_table_key(
                    &self.tables,
                    &restrict_schema,
                    &lowercase_name,
                )?;
                return self.tables.get_mut(&restricted_key);
            }

            // Check session's temp schema first
            let temp_qualified = format!("{}.{}", self.catalog.temp_schema_name(), lowercase_name);
            if self.tables.contains_key(&temp_qualified) {
                return self.tables.get_mut(&temp_qualified);
            }

            let current_schema = &self.catalog.get_current_schema().to_string();

            // Try as-is with schema prefix
            let qualified_name_original = format!("{}.{}", current_schema, name);
            if self.tables.contains_key(&qualified_name_original) {
                return self.tables.get_mut(&qualified_name_original);
            }

            // Try lowercase with schema prefix (standard)
            let qualified_name_lowercase = format!("{}.{}", current_schema, lowercase_name);
            if qualified_name_lowercase != qualified_name_original
                && self.tables.contains_key(&qualified_name_lowercase)
            {
                return self.tables.get_mut(&qualified_name_lowercase);
            }

            // Try uppercase with schema prefix (backward compatibility)
            let qualified_name_uppercase = format!("{}.{}", current_schema, uppercase_name);
            if qualified_name_uppercase != qualified_name_original
                && qualified_name_uppercase != qualified_name_lowercase
                && self.tables.contains_key(&qualified_name_uppercase)
            {
                return self.tables.get_mut(&qualified_name_uppercase);
            }

            // Finally, check attached databases in attachment order (SQLite
            // searches temp, then main, then each ATTACHed database — #6310).
            let attached_names: Vec<String> =
                self.catalog.attached_databases().iter().map(|a| a.name.clone()).collect();
            for attached_name in attached_names {
                let attached_qualified = format!("{}.{}", attached_name, lowercase_name);
                if self.tables.contains_key(&attached_qualified) {
                    return self.tables.get_mut(&attached_qualified);
                }
            }
        }

        None
    }

    /// Drop a table
    ///
    /// Temporary tables (in the "temp" schema) are not persisted to WAL.
    pub fn drop_table(&mut self, name: &str) -> Result<(), StorageError> {
        // Emit WAL entry for persistence before dropping (skip for temp tables)
        if !self.is_temp_table(name) {
            self.emit_wal_op(WalOp::DropTable {
                table_id: self.table_name_to_id(name),
                table_name: name.to_string(),
            });
        }

        // Invalidate columnar cache before dropping
        self.columnar_cache.invalidate(name);
        self.snapshot_operations_for_mutation();
        self.operations.drop_table(&mut self.catalog, &mut self.tables, name)
    }

    /// Insert a row into a table
    ///
    /// Temporary tables (in the "temp" schema) are not persisted to WAL.
    ///
    /// # MVCC (Phase 1c of #5136)
    ///
    /// When the `mvcc_enabled` feature is on and a transaction is active,
    /// the new row is stamped with `xmin = current_txn_id` before storage.
    /// When the feature is off (default) the row keeps the pre-MVCC
    /// sentinel `xmin = 0` and behavior is bit-for-bit identical to
    /// pre-MVCC. See [`crate::mvcc::stamp_xmin_for_write`].
    pub fn insert_row(&mut self, table_name: &str, mut row: Row) -> Result<(), StorageError> {
        // Phase 1c: stamp xmin with the active txn id when MVCC is on.
        // No-op when the feature is off, so the off-state matches main.
        let txn_id = self.transaction_id();
        crate::mvcc::stamp_xmin_for_write(&mut row, txn_id);

        self.snapshot_operations_for_mutation();
        let row_index =
            self.operations.insert_row(&self.catalog, &mut self.tables, table_name, row.clone())?;

        // Emit WAL entry for persistence (skip for temp tables)
        if !self.is_temp_table(table_name) {
            self.emit_wal_op(WalOp::Insert {
                table_id: self.table_name_to_id(table_name),
                table_name: table_name.to_string(),
                row_id: row_index as u64,
                values: row.values.to_vec(),
                // Effective SQLite rowid (issue #5835): explicit when the row
                // carries one, else the implicit physical position + 1.
                rowid: Some(row.row_id.unwrap_or(row_index as u64 + 1)),
            });
        }

        // Broadcast change event to subscribers, stamping the single-column PK
        // identity when available so consumers can prune re-queries (#5472).
        let pk = self.get_table(table_name).and_then(|t| single_pk_identity(&t.schema, &row));
        self.broadcast_change(ChangeEvent::Insert {
            table_name: table_name.to_string(),
            row_index,
            pk,
        });

        // #6199 Phase 3: keep the cached columnar copy in sync incrementally
        // rather than dropping it. If the table is resident, the just-inserted
        // row is appended to the columnar representation in place (no full
        // rebuild on the next scan); if it is not resident (or the append can't
        // be applied), `append_rows` leaves the cache consistent. Native
        // columnar tables are never in this cache, so this is a no-op for them.
        self.columnar_cache.append_rows(table_name, std::slice::from_ref(&row));

        Ok(())
    }

    /// Insert multiple rows into a table in a single batch
    ///
    /// This method is optimized for bulk data loading and provides significant
    /// performance improvements over repeated `insert_row` calls:
    ///
    /// - **Pre-allocation**: Vector capacity reserved upfront
    /// - **Batch validation**: All rows validated before any insertion
    /// - **Deferred index rebuild**: Indexes rebuilt once after all inserts
    /// - **Single cache invalidation**: Columnar cache invalidated once at end
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table to insert into
    /// * `rows` - Vector of rows to insert
    ///
    /// # Returns
    ///
    /// * `Ok(usize)` - Number of rows successfully inserted
    /// * `Err(StorageError)` - If validation fails (no rows inserted on error)
    ///
    /// # Performance
    ///
    /// For large batches (1000+ rows), expect 10-50x speedup vs single-row inserts.
    ///
    /// # Example
    ///
    /// ```text
    /// let rows = vec![
    ///     Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
    ///     Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
    /// ];
    /// let count = db.insert_rows_batch("users", rows)?;
    /// ```
    pub fn insert_rows_batch(
        &mut self,
        table_name: &str,
        mut rows: Vec<Row>,
    ) -> Result<usize, StorageError> {
        if rows.is_empty() {
            return Ok(0);
        }

        // Phase 1c (Issue #5150 / #5136): stamp xmin on every new row with
        // the active txn id when the `mvcc_enabled` feature is on. When
        // the feature is off this is a no-op (rows keep their constructor
        // default `xmin = PRE_MVCC_TXN_ID`), so the off-state matches main.
        let txn_id = self.transaction_id();
        for row in rows.iter_mut() {
            crate::mvcc::stamp_xmin_for_write(row, txn_id);
        }

        self.snapshot_operations_for_mutation();
        let row_indices = self.operations.insert_rows_batch(
            &self.catalog,
            &mut self.tables,
            table_name,
            rows.clone(),
        )?;

        let table_id = self.table_name_to_id(table_name);
        let is_temp = self.is_temp_table(table_name);

        // Resolve the single-column PK position once (if any) so each broadcast
        // can stamp the row's PK identity without re-borrowing the table (#5472).
        let pk_col: Option<(String, usize)> = self.get_table(table_name).and_then(|t| {
            let pk_cols = t.schema.primary_key.as_ref()?;
            if pk_cols.len() != 1 {
                return None;
            }
            let name = &pk_cols[0];
            t.schema.get_column_index(name).map(|idx| (name.to_lowercase(), idx))
        });

        // #6199 Phase 3: keep the cached columnar copy in sync incrementally by
        // appending the whole batch in place, rather than dropping the entry and
        // forcing a full rebuild on the next scan. Done here, before the
        // consuming loop below moves `rows`. No-op when the table is not resident
        // (or native columnar); leaves the cache consistent on an unapplicable
        // append.
        self.columnar_cache.append_rows(table_name, &rows);

        // Emit WAL entries and broadcast events
        for (row, &row_index) in rows.into_iter().zip(row_indices.iter()) {
            // Emit WAL entry for persistence (skip for temp tables)
            if !is_temp {
                self.emit_wal_op(WalOp::Insert {
                    table_id,
                    table_name: table_name.to_string(),
                    row_id: row_index as u64,
                    values: row.values.to_vec(),
                    // Effective SQLite rowid (issue #5835).
                    rowid: Some(row.row_id.unwrap_or(row_index as u64 + 1)),
                });
            }

            // Broadcast change event to subscribers, stamping the PK identity
            // when the table has a single-column primary key (#5472).
            let pk = pk_col.as_ref().and_then(|(name, idx)| {
                row.values.get(*idx).map(|v| ChangeEventPk::single(name.clone(), v.clone()))
            });
            self.broadcast_change(ChangeEvent::Insert {
                table_name: table_name.to_string(),
                row_index,
                pk,
            });
        }

        // Columnar cache already maintained incrementally above (Phase 3).

        Ok(row_indices.len())
    }

    /// Insert rows from an iterator in a streaming fashion
    ///
    /// This method is optimized for very large datasets that may not fit
    /// in memory all at once. Rows are processed in configurable batch sizes,
    /// balancing memory usage with performance.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table to insert into
    /// * `rows` - Iterator yielding rows to insert
    /// * `batch_size` - Number of rows per batch (0 defaults to 1000)
    ///
    /// # Returns
    ///
    /// * `Ok(usize)` - Total number of rows successfully inserted
    /// * `Err(StorageError)` - If any batch fails validation
    ///
    /// # Note
    ///
    /// Unlike `insert_rows_batch`, this method commits rows batch-by-batch.
    /// A failure partway through will leave previously committed batches
    /// in the table. Use `insert_rows_batch` for all-or-nothing semantics.
    ///
    /// # Example
    ///
    /// ```text
    /// // Stream 100K rows in batches of 5000
    /// let rows = (0..100_000).map(|i| Row::new(vec![SqlValue::Integer(i)]));
    /// let count = db.insert_rows_iter("numbers", rows, 5000)?;
    /// ```
    pub fn insert_rows_iter<I>(
        &mut self,
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
                let count = self.insert_rows_batch(table_name, std::mem::take(&mut batch))?;
                total_inserted += count;
                batch = Vec::with_capacity(batch_size);
            }
        }

        // Insert any remaining rows
        if !batch.is_empty() {
            let count = self.insert_rows_batch(table_name, batch)?;
            total_inserted += count;
        }

        Ok(total_inserted)
    }

    /// Update a single row by primary key value (direct API, no SQL parsing)
    ///
    /// This method provides a high-performance update path that bypasses SQL parsing,
    /// making it suitable for benchmarking and performance-critical code paths.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table
    /// * `pk_value` - Primary key value to match (single column PK only)
    /// * `column_updates` - List of (column_name, new_value) pairs to update
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Row was found and updated
    /// * `Ok(false)` - Row was not found (no error)
    /// * `Err(StorageError)` - Table not found, column not found, or constraint violation
    ///
    /// # Example
    ///
    /// ```text
    /// // Update column 'name' for row with id=5
    /// let updated = db.update_row_by_pk(
    ///     "users",
    ///     SqlValue::Integer(5),
    ///     vec![("name", SqlValue::Varchar(arcstr::ArcStr::from("Alice")))],
    /// )?;
    /// ```
    pub fn update_row_by_pk(
        &mut self,
        table_name: &str,
        pk_value: vibesql_types::SqlValue,
        column_updates: Vec<(&str, vibesql_types::SqlValue)>,
    ) -> Result<bool, StorageError> {
        // First phase: read data (immutable borrow)
        let (row_index, old_row, schema, resolved_name) = {
            // Get table using existing lookup logic (handles schema prefixes)
            let table = self
                .get_table(table_name)
                .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;

            // Look up row by PK
            let pk_index = table
                .primary_key_index()
                .ok_or_else(|| StorageError::Other("Table has no primary key index".to_string()))?;

            let row_index = match pk_index.get(&vec![pk_value.clone()]) {
                Some(&idx) => idx,
                None => return Ok(false), // Row not found
            };

            // Get old row and schema
            let old_row = table.scan()[row_index].clone();
            let schema = table.schema.clone();
            let resolved_name = schema.name.clone();

            (row_index, old_row, schema, resolved_name)
        };

        // Second phase: apply updates
        let mut new_row = old_row.clone();
        let mut changed_columns = std::collections::HashSet::new();

        for (col_name, new_value) in &column_updates {
            let col_index =
                schema.get_column_index(col_name).ok_or_else(|| StorageError::ColumnNotFound {
                    column_name: col_name.to_string(),
                    table_name: resolved_name.clone(),
                })?;

            // Check NOT NULL constraint
            let column = &schema.columns[col_index];
            if !column.nullable && *new_value == vibesql_types::SqlValue::Null {
                return Err(StorageError::NullConstraintViolation {
                    table: resolved_name.clone(),
                    column: col_name.to_string(),
                });
            }

            new_row.set(col_index, new_value.clone())?;
            changed_columns.insert(col_index);
        }

        // Phase 1c (Issue #5150 / #5136): stamp the new row's xmin with
        // the active txn id when MVCC is on. The new row is by definition
        // not deleted, so xmax stays `None` regardless of feature state.
        //
        // Note: this fast path overwrites the row in-place. Phase 1c does
        // NOT preserve the old version as a tombstone here — Phase 1d will
        // revisit if true two-version retention is required for snapshot
        // isolation across UPDATE.
        let txn_id = self.transaction_id();
        crate::mvcc::stamp_xmin_for_write(&mut new_row, txn_id);
        new_row.xmax = None;

        // Third phase: write data (mutable borrow)
        let table_mut = self.get_table_mut(table_name).unwrap();
        table_mut.update_row_selective(row_index, new_row.clone(), &changed_columns)?;

        // Update user-defined indexes (pass changed_columns to skip unaffected indexes)
        self.snapshot_operations_for_mutation();
        self.operations.update_indexes_for_update(
            &self.catalog,
            &resolved_name,
            &old_row,
            &new_row,
            row_index,
            Some(&changed_columns),
        );

        // Emit WAL entry for persistence (skip for temp tables)
        if !self.is_temp_table(&resolved_name) {
            self.emit_wal_op(WalOp::Update {
                table_id: self.table_name_to_id(&resolved_name),
                table_name: resolved_name.clone(),
                row_id: row_index as u64,
                old_values: old_row.values.to_vec(),
                new_values: new_row.values.to_vec(),
            });
        }

        // Broadcast change event to subscribers, carrying BOTH the pre-image and
        // post-image single-column PK so consumers can reason about a row that
        // moves into or out of a filter (or whose PK itself changed) (#5472).
        let pk =
            match (single_pk_identity(&schema, &old_row), single_pk_identity(&schema, &new_row)) {
                (Some(old_pk), Some(new_pk)) => {
                    Some(ChangeEventPk::updated(old_pk.column, old_pk.value, new_pk.value))
                }
                // If either image's PK is unavailable, fall back to no PK (re-query).
                _ => None,
            };
        self.broadcast_change(ChangeEvent::Update {
            table_name: resolved_name.clone(),
            row_index,
            pk,
        });

        // Invalidate columnar cache
        self.columnar_cache.invalidate(&resolved_name);

        Ok(true)
    }

    /// Update the first LIVE row matching `predicate`, without requiring a
    /// primary key (direct API, no SQL parsing).
    ///
    /// Unlike [`Database::update_row_by_pk`], this does not consult a PK index
    /// — it scans live rows and updates the first match by content, via the
    /// same `predicate` used to find it. Returns `Ok(false)` when no row
    /// matches. This is intended for small internal system tables that have
    /// no declared PRIMARY KEY, no user indexes, and no triggers — currently
    /// only `sqlite_sequence` (AUTOINCREMENT bookkeeping, issue #6173) —
    /// where the caller already guarantees at most one live row can match.
    pub fn update_row_matching<F>(
        &mut self,
        table_name: &str,
        mut predicate: F,
        column_updates: Vec<(&str, vibesql_types::SqlValue)>,
    ) -> Result<bool, StorageError>
    where
        F: FnMut(&Row) -> bool,
    {
        let (row_index, old_row, schema, resolved_name) = {
            let table = self
                .get_table(table_name)
                .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
            let Some((row_index, row)) = table.scan_live().find(|(_, row)| predicate(row)) else {
                return Ok(false);
            };
            let old_row = row.clone();
            let schema = table.schema.clone();
            let resolved_name = schema.name.clone();
            (row_index, old_row, schema, resolved_name)
        };

        let mut new_row = old_row.clone();
        let mut changed_columns = std::collections::HashSet::new();
        for (col_name, new_value) in &column_updates {
            let col_index =
                schema.get_column_index(col_name).ok_or_else(|| StorageError::ColumnNotFound {
                    column_name: col_name.to_string(),
                    table_name: resolved_name.clone(),
                })?;
            new_row.set(col_index, new_value.clone())?;
            changed_columns.insert(col_index);
        }

        let txn_id = self.transaction_id();
        crate::mvcc::stamp_xmin_for_write(&mut new_row, txn_id);
        new_row.xmax = None;

        let table_mut = self.get_table_mut(table_name).unwrap();
        table_mut.update_row_selective(row_index, new_row.clone(), &changed_columns)?;

        if !self.is_temp_table(&resolved_name) {
            self.emit_wal_op(WalOp::Update {
                table_id: self.table_name_to_id(&resolved_name),
                table_name: resolved_name.clone(),
                row_id: row_index as u64,
                old_values: old_row.values.to_vec(),
                new_values: new_row.values.to_vec(),
            });
        }

        self.columnar_cache.invalidate(&resolved_name);

        Ok(true)
    }

    /// Delete the first LIVE row matching `predicate`, without requiring a
    /// primary key (direct API, no SQL parsing).
    ///
    /// Same restriction and intended use as [`Database::update_row_matching`]:
    /// internal system tables with no PK/indexes/triggers to maintain
    /// (currently only `sqlite_sequence`). Returns `Ok(false)` when no row
    /// matches.
    pub fn delete_row_matching<F>(
        &mut self,
        table_name: &str,
        predicate: F,
    ) -> Result<bool, StorageError>
    where
        F: FnMut(&Row) -> bool + Clone,
    {
        let (row_index, old_values, resolved_name) = {
            let table = self
                .get_table(table_name)
                .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
            let mut p = predicate.clone();
            let Some((row_index, row)) = table.scan_live().find(|(_, row)| p(row)) else {
                return Ok(false);
            };
            (row_index, row.values.to_vec(), table.schema.name.clone())
        };

        if !self.is_temp_table(&resolved_name) {
            self.emit_wal_delete(&resolved_name, row_index as u64, old_values);
        }

        let table_mut = self.get_table_mut(table_name).unwrap();
        table_mut.delete_where(predicate);

        self.columnar_cache.invalidate(&resolved_name);

        Ok(true)
    }

    /// List all table names
    pub fn list_tables(&self) -> Vec<String> {
        self.catalog.list_tables()
    }
}

/// Serialize a TableSchema to bytes for WAL storage
///
/// Base layout: `table_name\0`, column count (u32 LE), then per column
/// `name\0`, `Debug(data_type)\0`, nullable byte.
///
/// A constraint/durability **trailer** (issue #5883) follows the column list,
/// carrying the same per-table durability fields the checkpoint catalog
/// persists — primary key, WITHOUT ROWID flag, and the verbatim CREATE TABLE
/// `sql_source`. Crash recovery (`wal::recovery::deserialize_table_schema`)
/// restores these and then rebuilds CHECK/FK constraints and the INTEGER
/// PRIMARY KEY rowid alias by re-parsing `sql_source`, exactly like the
/// checkpoint load path (issue #5834 / PR #5878). Without the trailer, a
/// table whose CREATE TABLE was logged after the last checkpoint came back
/// from a crash with no PK, no constraints, and no rowid alias.
///
/// Trailer layout (versioned, append-only):
///   `[trailer_version: u8 = 1]`
///   `[has_pk: u8]` — if 1: `[pk_count: u32 LE]` then `pk_count ×  name\0`
///   `[without_rowid: u8]`
///   `[has_sql_source: u8]` — if 1: `[len: u32 LE][utf8 bytes]`
///
/// Compatibility: the blob is length-prefixed inside the WAL entry, and the
/// legacy deserializer stops after the column list (ignoring trailing bytes),
/// so old binaries read new blobs fine; new binaries treat a blob ending at
/// the column list as trailer-absent (legacy) and fall back to the old
/// behavior. No WAL format version bump is required.
pub(crate) fn serialize_table_schema(schema: &vibesql_catalog::TableSchema) -> Vec<u8> {
    // Simple approach: serialize the table name and column info as text
    // Format: table_name\0col1_name\0col1_type\0nullable\0...
    let mut data = Vec::new();

    // Write table name
    data.extend_from_slice(schema.name.as_bytes());
    data.push(0);

    // Write column count
    data.extend_from_slice(&(schema.columns.len() as u32).to_le_bytes());

    // Write each column
    for col in &schema.columns {
        // Column name
        data.extend_from_slice(col.name.as_bytes());
        data.push(0);

        // Data type (as debug string for simplicity)
        let type_str = format!("{:?}", col.data_type);
        data.extend_from_slice(type_str.as_bytes());
        data.push(0);

        // Nullable flag
        data.push(if col.nullable { 1 } else { 0 });
    }

    // ---- Constraint/durability trailer, version 1 (issue #5883) ----
    data.push(1); // trailer version

    match &schema.primary_key {
        Some(pk_cols) => {
            data.push(1);
            data.extend_from_slice(&(pk_cols.len() as u32).to_le_bytes());
            for col_name in pk_cols {
                data.extend_from_slice(col_name.as_bytes());
                data.push(0);
            }
        }
        None => data.push(0),
    }

    data.push(if schema.without_rowid { 1 } else { 0 });

    match &schema.sql_source {
        Some(src) => {
            data.push(1);
            data.extend_from_slice(&(src.len() as u32).to_le_bytes());
            data.extend_from_slice(src.as_bytes());
        }
        None => data.push(0),
    }

    data
}
