use std::collections::HashMap;

/// Storage format for tables
///
/// Tables can be stored in row-oriented (default) or columnar format.
/// Re-exported from vibesql_ast for convenience.
pub use vibesql_ast::StorageFormat;

use crate::{column::ColumnSchema, foreign_key::ForeignKeyConstraint};

/// The storage-type classification of a column in a SQLite STRICT table.
///
/// STRICT tables (<https://sqlite.org/stricttables.html>) allow exactly six
/// declared datatypes. Each maps to a rigid runtime type that INSERT/UPDATE
/// values must match (after the documented lossless coercions). This is kept
/// distinct from [`vibesql_types::DataType`] because the two ends of the map
/// are lossy: `ANY` and `BLOB` both parse to `DataType::BinaryLargeObject`, and
/// `INT` vs `INTEGER` both parse to `DataType::Integer` — yet STRICT must treat
/// each pair differently (ANY accepts everything; the error message echoes the
/// exact declared keyword).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrictType {
    /// `INT`
    Int,
    /// `INTEGER`
    Integer,
    /// `REAL`
    Real,
    /// `TEXT`
    Text,
    /// `BLOB`
    Blob,
    /// `ANY` — accepts any value, stored as-is (no type check, no coercion).
    Any,
}

impl StrictType {
    /// The canonical declared keyword for this strict type, used verbatim in
    /// SQLite's `cannot store <T> value in <KEYWORD> column <tbl>.<col>` errors.
    pub fn keyword(self) -> &'static str {
        match self {
            StrictType::Int => "INT",
            StrictType::Integer => "INTEGER",
            StrictType::Real => "REAL",
            StrictType::Text => "TEXT",
            StrictType::Blob => "BLOB",
            StrictType::Any => "ANY",
        }
    }

    /// Classify a declared datatype's verbatim source text for a STRICT table.
    ///
    /// Returns `Some(strict_type)` only for the six allowed datatypes (matched
    /// case-insensitively, with no size/precision specifier). Any other spelling
    /// — including a parameterized form like `TEXT(50)` — returns `None`, which
    /// the DDL layer reports as `unknown datatype for <tbl>.<col>: "<text>"`.
    pub fn classify(declared_type: &str) -> Option<StrictType> {
        match declared_type.trim().to_ascii_uppercase().as_str() {
            "INT" => Some(StrictType::Int),
            "INTEGER" => Some(StrictType::Integer),
            "REAL" => Some(StrictType::Real),
            "TEXT" => Some(StrictType::Text),
            "BLOB" => Some(StrictType::Blob),
            "ANY" => Some(StrictType::Any),
            _ => None,
        }
    }
}

/// Table schema definition.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    /// Cache for O(1) column name to index lookup
    column_index_cache: HashMap<String, usize>,
    /// Primary key column names (None if no primary key, Some(vec) for single or composite key)
    pub primary_key: Option<Vec<String>>,
    /// Unique constraints - each inner vec represents a unique constraint (can be single or
    /// composite)
    pub unique_constraints: Vec<Vec<String>>,
    /// Explicit per-key-part collation for each PRIMARY KEY column, parallel to
    /// `primary_key`. `Some(vec)` mirrors the PK column list; each element is the
    /// key-part `COLLATE` name (`PRIMARY KEY(a COLLATE nocase)`), or `None` to
    /// fall back to the column's declared collation, then BINARY. Not serialized
    /// in the binary catalog — rederived from `sql_source` on load (like
    /// `strict_types`), so no format bump is needed. Issue #5881.
    pub primary_key_collations: Option<Vec<Option<String>>>,
    /// Explicit per-key-part collation for each UNIQUE constraint, parallel to
    /// `unique_constraints` (outer + inner index alignment). Same fallback and
    /// persistence semantics as `primary_key_collations`. Issue #5881.
    pub unique_constraint_collations: Vec<Vec<Option<String>>>,
    /// Check constraints - each tuple is (constraint_name, check_expression)
    pub check_constraints: Vec<(String, vibesql_ast::Expression)>,
    /// Foreign key constraints
    pub foreign_keys: Vec<ForeignKeyConstraint>,
    /// Storage format for this table (row-oriented or columnar)
    pub storage_format: StorageFormat,
    /// Index of INTEGER PRIMARY KEY column that serves as rowid alias (SQLite compatibility)
    /// When set, this column's value IS the rowid, and references to rowid should return this
    /// column's value. Only set for single-column INTEGER (exact type) PRIMARY KEYs.
    pub rowid_alias_column: Option<usize>,
    /// If true, table was created with WITHOUT ROWID clause (SQLite compatibility).
    pub without_rowid: bool,
    /// If true, table was created with the STRICT clause (SQLite compatibility).
    /// STRICT tables enforce rigid per-column datatypes on INSERT/UPDATE. See
    /// <https://sqlite.org/stricttables.html>.
    pub strict: bool,
    /// Per-column STRICT storage-type classification, parallel to `columns`.
    /// Empty when the table is not STRICT. When non-empty, `strict_types[i]` is
    /// the declared strict type of `columns[i]` (validated at CREATE time to be
    /// one of INT/INTEGER/REAL/TEXT/BLOB/ANY). Not serialized in the binary
    /// catalog — it is rederived from `sql_source` on load (see the storage
    /// crate's constraint rehydration), so no binary format bump is needed.
    pub strict_types: Vec<StrictType>,
    /// If true, this schema is a pseudo-schema standing in for a VIEW rather than
    /// a base table. Views have no implicit rowid, so references to the
    /// `rowid`/`oid`/`_rowid_` pseudo-columns against a view must error with
    /// `no such column: rowid` (SQLite semantics; `allow_rowid_in_view` is off by
    /// default). See issue #5492.
    pub is_view: bool,
    /// Verbatim original `CREATE TABLE` source text, exactly as the user typed
    /// it (whitespace and formatting preserved), with any trailing semicolon
    /// stripped. SQLite stores the byte-for-byte original statement in
    /// `sqlite_master.sql`; when this is `Some`, `sqlite_master` returns it
    /// verbatim instead of a reconstruction from the parsed schema. `None` when
    /// the source text is unavailable (e.g. schema built programmatically), in
    /// which case callers fall back to reconstructing the SQL. See issue #5619.
    pub sql_source: Option<String>,
}

impl TableSchema {
    /// Build the column-name -> index lookup cache, preserving the FIRST
    /// occurrence of each name on duplicates.
    ///
    /// Real tables reject duplicate column names at CREATE TABLE time, but VIEW
    /// pseudo-schemas built from `SELECT *` over a join can legitimately carry
    /// duplicate column names (e.g. two columns named `c`). When that happens,
    /// `get_column_index` must agree with the FIRST-match semantics of
    /// `columns.iter().position(...)` used by the view UPDATE path. A plain
    /// `HashMap` collect is last-write-wins, which disagreed with `position()`
    /// and caused INSTEAD OF UPDATE triggers on such views to read the wrong
    /// `new.<col>` slot (see issue #5703). Using `entry().or_insert()` keeps the
    /// first index, matching `position()`.
    fn build_column_index_cache(columns: &[ColumnSchema]) -> HashMap<String, usize> {
        columns.iter().enumerate().fold(HashMap::new(), |mut map, (idx, col)| {
            map.entry(col.name.clone()).or_insert(idx);
            map
        })
    }

    pub fn new(name: String, columns: Vec<ColumnSchema>) -> Self {
        // Store columns by exact name for case-sensitive lookups.
        // The parser normalizes unquoted identifiers to uppercase, so case-insensitive
        // matching for regular identifiers works automatically. Delimited identifiers
        // (quoted with "") preserve exact case per SQL:1999 Section 5.2.
        let column_index_cache: HashMap<String, usize> = Self::build_column_index_cache(&columns);

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key: None,
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            storage_format: StorageFormat::default(),
            rowid_alias_column: None,
            without_rowid: false,
            is_view: false,
            sql_source: None,
            strict: false,
            strict_types: Vec::new(),
            primary_key_collations: None,
            unique_constraint_collations: Vec::new(),
        }
    }

    /// The STRICT storage type of the column at `col_idx`, or `None` when the
    /// table is not STRICT (or `col_idx` is out of range). Callers use this to
    /// gate STRICT type enforcement on INSERT/UPDATE.
    pub fn strict_type_of(&self, col_idx: usize) -> Option<StrictType> {
        if !self.strict {
            return None;
        }
        self.strict_types.get(col_idx).copied()
    }

    /// Create a table schema with a primary key
    pub fn with_primary_key(
        name: String,
        columns: Vec<ColumnSchema>,
        primary_key: Vec<String>,
    ) -> Self {
        let column_index_cache: HashMap<String, usize> = Self::build_column_index_cache(&columns);

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key: Some(primary_key),
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            storage_format: StorageFormat::default(),
            rowid_alias_column: None,
            without_rowid: false,
            is_view: false,
            sql_source: None,
            strict: false,
            strict_types: Vec::new(),
            primary_key_collations: None,
            unique_constraint_collations: Vec::new(),
        }
    }

    /// Create a table schema with unique constraints
    pub fn with_unique_constraints(
        name: String,
        columns: Vec<ColumnSchema>,
        unique_constraints: Vec<Vec<String>>,
    ) -> Self {
        let column_index_cache: HashMap<String, usize> = Self::build_column_index_cache(&columns);

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key: None,
            unique_constraints,
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            storage_format: StorageFormat::default(),
            rowid_alias_column: None,
            without_rowid: false,
            is_view: false,
            sql_source: None,
            strict: false,
            strict_types: Vec::new(),
            primary_key_collations: None,
            unique_constraint_collations: Vec::new(),
        }
    }

    /// Create a table schema with foreign key constraints
    pub fn with_foreign_keys(
        name: String,
        columns: Vec<ColumnSchema>,
        foreign_keys: Vec<ForeignKeyConstraint>,
    ) -> Self {
        let column_index_cache: HashMap<String, usize> = Self::build_column_index_cache(&columns);

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key: None,
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys,
            storage_format: StorageFormat::default(),
            rowid_alias_column: None,
            without_rowid: false,
            is_view: false,
            sql_source: None,
            strict: false,
            strict_types: Vec::new(),
            primary_key_collations: None,
            unique_constraint_collations: Vec::new(),
        }
    }

    /// Create a table schema with both primary key and unique constraints
    pub fn with_all_constraints(
        name: String,
        columns: Vec<ColumnSchema>,
        primary_key: Option<Vec<String>>,
        unique_constraints: Vec<Vec<String>>,
    ) -> Self {
        let column_index_cache: HashMap<String, usize> = Self::build_column_index_cache(&columns);

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key,
            unique_constraints,
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            storage_format: StorageFormat::default(),
            rowid_alias_column: None,
            without_rowid: false,
            is_view: false,
            sql_source: None,
            strict: false,
            strict_types: Vec::new(),
            primary_key_collations: None,
            unique_constraint_collations: Vec::new(),
        }
    }

    /// Create a table schema with all constraint types
    pub fn with_all_constraint_types(
        name: String,
        columns: Vec<ColumnSchema>,
        primary_key: Option<Vec<String>>,
        unique_constraints: Vec<Vec<String>>,
        check_constraints: Vec<(String, vibesql_ast::Expression)>,
        foreign_keys: Vec<ForeignKeyConstraint>,
    ) -> Self {
        let column_index_cache: HashMap<String, usize> = Self::build_column_index_cache(&columns);

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key,
            unique_constraints,
            check_constraints,
            foreign_keys,
            storage_format: StorageFormat::default(),
            rowid_alias_column: None,
            without_rowid: false,
            is_view: false,
            sql_source: None,
            strict: false,
            strict_types: Vec::new(),
            primary_key_collations: None,
            unique_constraint_collations: Vec::new(),
        }
    }

    /// Create a table schema with storage format
    pub fn with_storage_format(
        name: String,
        columns: Vec<ColumnSchema>,
        storage_format: StorageFormat,
    ) -> Self {
        let column_index_cache: HashMap<String, usize> = Self::build_column_index_cache(&columns);

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key: None,
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            storage_format,
            rowid_alias_column: None,
            without_rowid: false,
            is_view: false,
            sql_source: None,
            strict: false,
            strict_types: Vec::new(),
            primary_key_collations: None,
            unique_constraint_collations: Vec::new(),
        }
    }

    /// Set the storage format for this table
    pub fn set_storage_format(&mut self, storage_format: StorageFormat) {
        self.storage_format = storage_format;
    }

    /// Set the verbatim original `CREATE TABLE` source text (see `sql_source`).
    /// Any trailing semicolon and surrounding whitespace are stripped so the
    /// stored text matches SQLite's `sqlite_master.sql` (which excludes the
    /// terminating `;`). See issue #5619.
    pub fn set_sql_source(&mut self, sql_source: impl Into<String>) {
        let s = sql_source.into();
        let trimmed = s.trim();
        let trimmed = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
        self.sql_source = Some(trimmed.to_string());
    }

    /// Discard any captured verbatim `CREATE TABLE` source text (see
    /// `sql_source`). Must be called whenever the schema is structurally mutated
    /// (e.g. by ALTER TABLE) so the stale original text is not re-emitted in
    /// `sqlite_master.sql` or the SQL-dump persistence path — which would no
    /// longer match the live schema and could even fail to reload (a renamed
    /// table whose verbatim text still names the old table). After invalidation,
    /// callers fall back to reconstructing the SQL. See issue #5619.
    pub fn invalidate_sql_source(&mut self) {
        self.sql_source = None;
    }

    /// Set the rowid alias column (for INTEGER PRIMARY KEY columns)
    /// This column's value IS the rowid in SQLite compatibility mode
    pub fn set_rowid_alias_column(&mut self, column_index: Option<usize>) {
        self.rowid_alias_column = column_index;
    }

    /// Mark this schema as a VIEW pseudo-schema (no implicit rowid). See #5492.
    pub fn set_is_view(&mut self, is_view: bool) {
        self.is_view = is_view;
    }

    /// Check if this table uses columnar storage
    pub fn is_columnar(&self) -> bool {
        matches!(self.storage_format, StorageFormat::Columnar)
    }

    /// Get column by name.
    /// Uses exact case matching. The parser normalizes unquoted identifiers to uppercase,
    /// so case-insensitive matching for regular identifiers works automatically.
    /// Delimited identifiers preserve exact case per SQL:1999 Section 5.2.
    pub fn get_column(&self, name: &str) -> Option<&ColumnSchema> {
        self.get_column_index(name).map(|idx| &self.columns[idx])
    }

    /// Get column index by name.
    /// First tries exact case match to support delimited identifiers (SQL:1999 Section 5.2).
    /// Falls back to case-insensitive search for backward compatibility with tests
    /// that create schemas directly without parser normalization.
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        // First, try exact case match (supports delimited identifiers correctly)
        if let Some(&idx) = self.column_index_cache.get(name) {
            return Some(idx);
        }
        // Fallback: case-insensitive search for backward compatibility
        // This handles cases where tests create columns with lowercase names
        // but SQL queries normalize to uppercase
        let name_lower = name.to_lowercase();
        self.column_index_cache
            .iter()
            .find(|(k, _)| k.to_lowercase() == name_lower)
            .map(|(_, &idx)| idx)
    }

    /// Get number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get the indices of primary key columns
    pub fn get_primary_key_indices(&self) -> Option<Vec<usize>> {
        self.primary_key.as_ref().map(|pk_cols| {
            pk_cols.iter().filter_map(|col_name| self.get_column_index(col_name)).collect()
        })
    }

    /// Get the indices for all unique constraints
    /// Returns a vector where each element is a vector of column indices for one unique constraint
    pub fn get_unique_constraint_indices(&self) -> Vec<Vec<usize>> {
        self.unique_constraints
            .iter()
            .map(|constraint_cols| {
                constraint_cols
                    .iter()
                    .filter_map(|col_name| self.get_column_index(col_name))
                    .collect()
            })
            .collect()
    }

    /// Effective per-key-part collation for the PRIMARY KEY, aligned with
    /// [`get_primary_key_indices`](Self::get_primary_key_indices).
    ///
    /// Resolution per key part follows SQLite semantics (issue #5881):
    /// explicit key-part `COLLATE` (`PRIMARY KEY(a COLLATE nocase)`), else the
    /// column's declared collation, else `None` (BINARY, case-sensitive). The
    /// key-part vector is looked up by position with a safe fallback so a stale
    /// or short `primary_key_collations` (e.g. after DROP COLUMN) degrades to
    /// the column's declared collation rather than panicking.
    ///
    /// Returns `None` when the table has no primary key.
    pub fn primary_key_effective_collations(&self) -> Option<Vec<Option<String>>> {
        let pk_cols = self.primary_key.as_ref()?;
        Some(
            pk_cols
                .iter()
                .enumerate()
                .map(|(i, col_name)| {
                    let key_part = self
                        .primary_key_collations
                        .as_ref()
                        .and_then(|v| v.get(i).cloned().flatten());
                    self.resolve_key_part_collation(key_part, col_name)
                })
                .collect(),
        )
    }

    /// Effective per-key-part collation for the UNIQUE constraint at
    /// `constraint_idx`, aligned with its entry in
    /// [`get_unique_constraint_indices`](Self::get_unique_constraint_indices).
    /// Same key-part → column → BINARY resolution as
    /// [`primary_key_effective_collations`](Self::primary_key_effective_collations).
    pub fn unique_constraint_effective_collations(
        &self,
        constraint_idx: usize,
    ) -> Vec<Option<String>> {
        let Some(cols) = self.unique_constraints.get(constraint_idx) else {
            return Vec::new();
        };
        cols.iter()
            .enumerate()
            .map(|(i, col_name)| {
                let key_part = self
                    .unique_constraint_collations
                    .get(constraint_idx)
                    .and_then(|v| v.get(i).cloned().flatten());
                self.resolve_key_part_collation(key_part, col_name)
            })
            .collect()
    }

    /// Resolve a single key part's effective collation: the explicit key-part
    /// `COLLATE` if present, otherwise the named column's declared collation,
    /// otherwise `None` (BINARY).
    fn resolve_key_part_collation(
        &self,
        key_part: Option<String>,
        col_name: &str,
    ) -> Option<String> {
        key_part.or_else(|| {
            self.get_column_index(col_name).and_then(|idx| self.columns[idx].collation.clone())
        })
    }

    /// Add a column to the table schema
    pub fn add_column(&mut self, column: ColumnSchema) -> Result<(), crate::CatalogError> {
        if self.get_column(&column.name).is_some() {
            return Err(crate::CatalogError::ColumnAlreadyExists(column.name));
        }
        let index = self.columns.len();
        self.column_index_cache.insert(column.name.clone(), index);
        self.columns.push(column);
        Ok(())
    }

    /// Remove a column from the table schema by index
    pub fn remove_column(&mut self, index: usize) -> Result<(), crate::CatalogError> {
        if index >= self.columns.len() {
            return Err(crate::CatalogError::ColumnNotFound {
                column_name: "index out of bounds".to_string(),
                table_name: self.name.clone(),
            });
        }
        let removed_column = self.columns.remove(index);

        // Rebuild the column index cache since indices have shifted
        self.column_index_cache.clear();
        for (idx, col) in self.columns.iter().enumerate() {
            self.column_index_cache.insert(col.name.clone(), idx);
        }

        // Remove from primary key if present
        if let Some(ref mut pk) = self.primary_key {
            pk.retain(|col_name| col_name != &removed_column.name);
            if pk.is_empty() {
                self.primary_key = None;
            }
        }

        // The per-key-part collation vectors (issue #5881) are positionally
        // aligned with `primary_key` / `unique_constraints`; a column removal
        // shifts those lists, so drop the explicit key-part collations rather
        // than risk stale misalignment. Enforcement then falls back to each
        // column's declared collation, which remains correct.
        self.primary_key_collations = None;
        self.unique_constraint_collations = Vec::new();

        // Remove from unique constraints
        self.unique_constraints = self
            .unique_constraints
            .iter()
            .filter_map(|constraint| {
                let filtered: Vec<String> = constraint
                    .iter()
                    .filter(|col_name| *col_name != &removed_column.name)
                    .cloned()
                    .collect();
                if filtered.is_empty() {
                    None
                } else {
                    Some(filtered)
                }
            })
            .collect();

        // Remove foreign keys that reference the removed column
        self.foreign_keys = self
            .foreign_keys
            .iter()
            .filter(|fk| !fk.column_names.contains(&removed_column.name))
            .cloned()
            .collect();

        // Remove check constraints that reference the removed column
        self.check_constraints.retain(|(_name, expr)| {
            !Self::expression_references_column(expr, &removed_column.name)
        });

        Ok(())
    }

    /// Rename a column at `index`, keeping the column-index cache and any
    /// constraint references (primary key, unique, foreign keys) consistent.
    ///
    /// Mutating `columns[i].name` directly leaves the `column_index_cache` stale,
    /// so callers that rename a column must go through this method.
    pub fn rename_column(
        &mut self,
        index: usize,
        new_name: &str,
    ) -> Result<(), crate::CatalogError> {
        if index >= self.columns.len() {
            return Err(crate::CatalogError::ColumnNotFound {
                column_name: "index out of bounds".to_string(),
                table_name: self.name.clone(),
            });
        }
        let old_name = self.columns[index].name.clone();
        self.columns[index].name = new_name.to_string();

        // Rebuild the column index cache to reflect the new name.
        self.column_index_cache.clear();
        for (idx, col) in self.columns.iter().enumerate() {
            self.column_index_cache.insert(col.name.clone(), idx);
        }

        // Update constraint references that named the old column.
        if let Some(ref mut pk) = self.primary_key {
            for col in pk.iter_mut() {
                if *col == old_name {
                    *col = new_name.to_string();
                }
            }
        }
        for constraint in self.unique_constraints.iter_mut() {
            for col in constraint.iter_mut() {
                if *col == old_name {
                    *col = new_name.to_string();
                }
            }
        }
        for fk in self.foreign_keys.iter_mut() {
            for col in fk.column_names.iter_mut() {
                if *col == old_name {
                    *col = new_name.to_string();
                }
            }
        }

        Ok(())
    }

    /// Check if a column exists
    pub fn has_column(&self, name: &str) -> bool {
        self.get_column(name).is_some()
    }

    /// Check if a column is part of the primary key
    pub fn is_column_in_primary_key(&self, column_name: &str) -> bool {
        self.primary_key.as_ref().is_some_and(|pk| pk.contains(&column_name.to_string()))
    }

    /// Get the column index of the INTEGER PRIMARY KEY column (SQLite semantics)
    ///
    /// In SQLite, when a column is declared as `INTEGER PRIMARY KEY`:
    /// 1. It becomes an alias for the internal `rowid`
    /// 2. Inserting NULL auto-generates the next rowid value
    /// 3. The auto-generated value is `max(rowid) + 1` (or 1 if table is empty)
    ///
    /// This method returns the column index if:
    /// - There is exactly one primary key column
    /// - That column's type is exactly `DataType::Integer` (not INT, not BIGINT)
    ///
    /// Returns `None` if the table doesn't have an INTEGER PRIMARY KEY.
    pub fn get_integer_primary_key_index(&self) -> Option<usize> {
        // Must have a single-column primary key
        let pk_cols = self.primary_key.as_ref()?;
        if pk_cols.len() != 1 {
            return None;
        }

        // Get the column index
        let col_idx = self.get_column_index(&pk_cols[0])?;
        let col = &self.columns[col_idx];

        // Must be exactly INTEGER type (not Bigint, not Smallint)
        if matches!(col.data_type, vibesql_types::DataType::Integer) {
            Some(col_idx)
        } else {
            None
        }
    }

    /// Set nullable property for a column by index
    pub fn set_column_nullable(
        &mut self,
        index: usize,
        nullable: bool,
    ) -> Result<(), crate::CatalogError> {
        if index >= self.columns.len() {
            return Err(crate::CatalogError::ColumnNotFound {
                column_name: "index out of bounds".to_string(),
                table_name: self.name.clone(),
            });
        }
        self.columns[index].set_nullable(nullable);
        Ok(())
    }

    /// Set default value for a column by index
    pub fn set_column_default(
        &mut self,
        index: usize,
        default: vibesql_ast::Expression,
    ) -> Result<(), crate::CatalogError> {
        if index >= self.columns.len() {
            return Err(crate::CatalogError::ColumnNotFound {
                column_name: "index out of bounds".to_string(),
                table_name: self.name.clone(),
            });
        }
        self.columns[index].set_default(default);
        Ok(())
    }

    /// Drop default value for a column by index
    pub fn drop_column_default(&mut self, index: usize) -> Result<(), crate::CatalogError> {
        if index >= self.columns.len() {
            return Err(crate::CatalogError::ColumnNotFound {
                column_name: "index out of bounds".to_string(),
                table_name: self.name.clone(),
            });
        }
        self.columns[index].drop_default();
        Ok(())
    }

    /// Add a check constraint
    pub fn add_check_constraint(
        &mut self,
        name: String,
        expr: vibesql_ast::Expression,
    ) -> Result<(), crate::CatalogError> {
        // Check if constraint name already exists
        if self.check_constraints.iter().any(|(n, _)| n == &name) {
            return Err(crate::CatalogError::ConstraintAlreadyExists(name));
        }
        self.check_constraints.push((name, expr));
        Ok(())
    }

    /// Add a unique constraint
    pub fn add_unique_constraint(
        &mut self,
        columns: Vec<String>,
    ) -> Result<(), crate::CatalogError> {
        // Verify all columns exist
        for col_name in &columns {
            if !self.has_column(col_name) {
                return Err(crate::CatalogError::ColumnNotFound {
                    column_name: col_name.clone(),
                    table_name: self.name.clone(),
                });
            }
        }
        self.unique_constraints.push(columns);
        Ok(())
    }

    /// Add a foreign key constraint
    pub fn add_foreign_key(
        &mut self,
        foreign_key: ForeignKeyConstraint,
    ) -> Result<(), crate::CatalogError> {
        // Verify all columns exist
        for col_name in &foreign_key.column_names {
            if !self.has_column(col_name) {
                return Err(crate::CatalogError::ColumnNotFound {
                    column_name: col_name.clone(),
                    table_name: self.name.clone(),
                });
            }
        }
        self.foreign_keys.push(foreign_key);
        Ok(())
    }

    /// Remove a check constraint by name
    pub fn drop_check_constraint(&mut self, name: &str) -> Result<(), crate::CatalogError> {
        let original_len = self.check_constraints.len();
        self.check_constraints.retain(|(n, _)| n != name);
        if self.check_constraints.len() == original_len {
            return Err(crate::CatalogError::ConstraintNotFound(name.to_string()));
        }
        Ok(())
    }

    /// Remove a unique constraint by column names
    pub fn drop_unique_constraint(
        &mut self,
        columns: &[String],
    ) -> Result<(), crate::CatalogError> {
        let original_len = self.unique_constraints.len();
        self.unique_constraints.retain(|constraint| constraint != columns);
        if self.unique_constraints.len() == original_len {
            return Err(crate::CatalogError::ConstraintNotFound(format!("{:?}", columns)));
        }
        Ok(())
    }

    /// Remove a foreign key constraint by name
    pub fn drop_foreign_key(&mut self, name: &str) -> Result<(), crate::CatalogError> {
        let original_len = self.foreign_keys.len();
        self.foreign_keys.retain(|fk| fk.name.as_deref() != Some(name));
        if self.foreign_keys.len() == original_len {
            return Err(crate::CatalogError::ConstraintNotFound(name.to_string()));
        }
        Ok(())
    }

    /// Check if an expression references a specific column
    fn expression_references_column(expr: &vibesql_ast::Expression, column_name: &str) -> bool {
        match expr {
            vibesql_ast::Expression::ColumnRef(col_id) => col_id.column_canonical() == column_name,
            vibesql_ast::Expression::BinaryOp { left, right, .. } => {
                Self::expression_references_column(left, column_name)
                    || Self::expression_references_column(right, column_name)
            }
            vibesql_ast::Expression::Conjunction(children)
            | vibesql_ast::Expression::Disjunction(children) => {
                children.iter().any(|child| Self::expression_references_column(child, column_name))
            }
            vibesql_ast::Expression::UnaryOp { expr, .. } => {
                Self::expression_references_column(expr, column_name)
            }
            vibesql_ast::Expression::Function { args, .. }
            | vibesql_ast::Expression::AggregateFunction { args, .. } => {
                args.iter().any(|arg| Self::expression_references_column(arg, column_name))
            }
            vibesql_ast::Expression::IsNull { expr, .. } => {
                Self::expression_references_column(expr, column_name)
            }
            vibesql_ast::Expression::IsDistinctFrom { left, right, .. } => {
                Self::expression_references_column(left, column_name)
                    || Self::expression_references_column(right, column_name)
            }
            vibesql_ast::Expression::IsTruthValue { expr, .. } => {
                Self::expression_references_column(expr, column_name)
            }
            vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
                // Check operand
                if let Some(op) = operand {
                    if Self::expression_references_column(op, column_name) {
                        return true;
                    }
                }
                // Check when clauses
                for clause in when_clauses {
                    // Check all conditions in this clause
                    if clause
                        .conditions
                        .iter()
                        .any(|cond| Self::expression_references_column(cond, column_name))
                    {
                        return true;
                    }
                    // Check result
                    if Self::expression_references_column(&clause.result, column_name) {
                        return true;
                    }
                }
                // Check else result
                if let Some(else_expr) = else_result {
                    if Self::expression_references_column(else_expr, column_name) {
                        return true;
                    }
                }
                false
            }
            vibesql_ast::Expression::ScalarSubquery(_) | vibesql_ast::Expression::Exists { .. } => {
                // Subqueries can reference columns, but for now we'll be conservative
                // and not remove check constraints with subqueries
                false
            }
            vibesql_ast::Expression::In { expr, .. }
            | vibesql_ast::Expression::InList { expr, .. } => {
                Self::expression_references_column(expr, column_name)
            }
            vibesql_ast::Expression::Between { expr, low, high, .. } => {
                Self::expression_references_column(expr, column_name)
                    || Self::expression_references_column(low, column_name)
                    || Self::expression_references_column(high, column_name)
            }
            vibesql_ast::Expression::WindowFunction { function, over } => {
                // Check function arguments
                let func_refs_column = match function {
                    vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                    | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                    | vibesql_ast::WindowFunctionSpec::Value { args, .. } => {
                        args.iter().any(|arg| Self::expression_references_column(arg, column_name))
                    }
                };
                if func_refs_column {
                    return true;
                }

                // Check partition by
                if let Some(partition_exprs) = &over.partition_by {
                    if partition_exprs
                        .iter()
                        .any(|expr| Self::expression_references_column(expr, column_name))
                    {
                        return true;
                    }
                }

                // Check order by
                if let Some(order_items) = &over.order_by {
                    if order_items
                        .iter()
                        .any(|item| Self::expression_references_column(&item.expr, column_name))
                    {
                        return true;
                    }
                }

                false
            }
            vibesql_ast::Expression::Cast { expr, .. } => {
                Self::expression_references_column(expr, column_name)
            }
            vibesql_ast::Expression::Position { substring, string, .. } => {
                Self::expression_references_column(substring, column_name)
                    || Self::expression_references_column(string, column_name)
            }
            vibesql_ast::Expression::Trim { removal_char, string, .. } => {
                removal_char
                    .as_ref()
                    .is_some_and(|e| Self::expression_references_column(e, column_name))
                    || Self::expression_references_column(string, column_name)
            }
            vibesql_ast::Expression::Extract { expr, .. } => {
                Self::expression_references_column(expr, column_name)
            }
            vibesql_ast::Expression::Like { expr, pattern, .. }
            | vibesql_ast::Expression::Glob { expr, pattern, .. } => {
                Self::expression_references_column(expr, column_name)
                    || Self::expression_references_column(pattern, column_name)
            }
            vibesql_ast::Expression::QuantifiedComparison { expr, .. } => {
                Self::expression_references_column(expr, column_name)
            }
            vibesql_ast::Expression::DuplicateKeyValue { column } => column == column_name,
            vibesql_ast::Expression::PseudoVariable { column, .. } => column == column_name,
            // These don't reference columns
            vibesql_ast::Expression::Interval { value, .. } => {
                // INTERVAL expressions may contain column references in the value
                Self::expression_references_column(value, column_name)
            }
            vibesql_ast::Expression::Literal(_)
            | vibesql_ast::Expression::CollatedLiteral { .. }
            | vibesql_ast::Expression::Placeholder(_)
            | vibesql_ast::Expression::NumberedPlaceholder(_)
            | vibesql_ast::Expression::NamedPlaceholder(_)
            | vibesql_ast::Expression::Wildcard
            | vibesql_ast::Expression::CurrentDate
            | vibesql_ast::Expression::CurrentTime { .. }
            | vibesql_ast::Expression::CurrentTimestamp { .. }
            | vibesql_ast::Expression::Default
            | vibesql_ast::Expression::NextValue { .. }
            | vibesql_ast::Expression::SessionVariable { .. }
            | vibesql_ast::Expression::MatchAgainst { .. } => false,

            vibesql_ast::Expression::RowValueConstructor(values) => {
                values.iter().any(|val| Self::expression_references_column(val, column_name))
            }

            vibesql_ast::Expression::Collate { expr, .. } => {
                Self::expression_references_column(expr, column_name)
            }

            vibesql_ast::Expression::Raise { error_message, .. } => error_message
                .as_ref()
                .is_some_and(|msg| Self::expression_references_column(msg, column_name)),
        }
    }
}
