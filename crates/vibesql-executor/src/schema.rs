use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    fmt,
    hash::{Hash, Hasher},
    ops::Deref,
};

use vibesql_catalog::TableIdentifier;

/// A normalized table/alias key for case-insensitive lookups.
/// Always stored as lowercase, making case-insensitive handling impossible to get wrong.
#[derive(Debug, Clone, Eq)]
pub struct TableKey(String);

impl TableKey {
    /// Create a new TableKey, normalizing to lowercase.
    #[inline]
    pub fn new(name: impl AsRef<str>) -> Self {
        TableKey(name.as_ref().to_lowercase())
    }

    /// Get the normalized key as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume the TableKey and return the inner String.
    #[inline]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl PartialEq for TableKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Hash for TableKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl Deref for TableKey {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for TableKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for TableKey {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TableKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for TableKey {
    fn from(s: String) -> Self {
        TableKey::new(s)
    }
}

impl From<&str> for TableKey {
    fn from(s: &str) -> Self {
        TableKey::new(s)
    }
}

impl From<TableKey> for String {
    fn from(key: TableKey) -> Self {
        key.0
    }
}

impl From<&TableKey> for TableKey {
    fn from(key: &TableKey) -> Self {
        key.clone()
    }
}

impl From<&String> for TableKey {
    fn from(s: &String) -> Self {
        TableKey::new(s)
    }
}

/// Represents the combined schema from multiple tables (for JOINs)
#[derive(Debug, Clone)]
pub struct CombinedSchema {
    /// Map from table identifier to (start_index, TableSchema)
    /// start_index is where this table's columns begin in the combined row
    /// TableIdentifier provides both canonical form for lookups and display form for output
    pub table_schemas: HashMap<TableIdentifier, (usize, vibesql_catalog::TableSchema)>,
    /// Total number of columns across all tables
    pub total_columns: usize,
    /// Columns that are hidden from `SELECT *` expansion due to NATURAL JOIN deduplication.
    /// These columns are still accessible via qualified references like `table.*`.
    /// This allows `SELECT t1.*` to return all columns from t1, while `SELECT *`
    /// correctly deduplicates columns for NATURAL JOIN.
    pub hidden_columns: HashSet<usize>,
    /// Columns that are ALWAYS hidden from `SELECT *` / `table.*` expansion,
    /// with no replacement or COALESCE partner (unlike `hidden_columns`, which is
    /// join-deduplication and always pairs the hidden column with a replacement).
    ///
    /// This models SQLite's `SQLITE_HIDDEN` virtual-table columns: the `json` and
    /// `root` columns of `json_each`/`json_tree` are resolvable by explicit
    /// reference (`jx.json`, `WHERE root = '$'`) but are excluded from wildcard
    /// expansion and `pragma_table_info`. The three `SELECT *` expansion call
    /// sites short-circuit these to *excluded* unconditionally, distinct from the
    /// join-dedup `hidden_columns` fallback that treats a no-replacement hidden
    /// column as visible (issue #6050).
    pub always_hidden_columns: HashSet<usize>,
    /// Reference to outer scope schema for nested subquery column resolution (issue #4493)
    /// Forms a linked-list chain similar to SQLite's NameContext.pNext
    /// Enables resolution of columns from multiple nesting levels
    pub outer_schema: Option<Box<CombinedSchema>>,
    /// Table aliases/names that appear more than once in the FROM clause (issue #4507)
    /// Used to detect ambiguous qualified column references like "A.f1" when table "A" appears
    /// twice Stores normalized (lowercase) table identifiers for case-insensitive matching
    pub duplicate_aliases: HashSet<TableIdentifier>,
    /// Column names that have been joined via NATURAL JOIN or USING clause (issue #4517)
    /// These columns exist in multiple tables but should NOT be considered ambiguous
    /// because they are logically the same column after the join.
    /// Stored as lowercase for case-insensitive matching.
    pub joined_columns: HashSet<String>,
    /// For USING columns in RIGHT/FULL OUTER JOINs, maps the unqualified column name
    /// (lowercase) to a list of column indices for N-way COALESCE resolution.
    /// When an unqualified reference is made to a USING column, we apply COALESCE
    /// semantics: return the first non-NULL value from the list of columns.
    /// This supports chained joins like `t1 NATURAL FULL JOIN t2 NATURAL FULL JOIN t3`
    /// where we need COALESCE(t1.id, t2.id, t3.id).
    /// Issue #4783, #4903: USING column semantics differ from SQLite in OUTER JOINs
    pub using_coalesce_indices: HashMap<String, Vec<usize>>,
    /// Column replacement map for RIGHT/FULL OUTER NATURAL JOINs in SELECT * expansion.
    /// Maps hidden_column_index -> replacement_column_index.
    /// When expanding SELECT *, if a column is hidden but has a replacement, output
    /// the replacement column's value instead of skipping. This maintains the column
    /// ordering from the left table while using values from the right table.
    /// Example: In `t5 NATURAL RIGHT JOIN t4`, t5.id is hidden but should be replaced
    /// by t4.id to maintain the output order (id, y, x) instead of (y, id, x).
    pub column_replacement_map: HashMap<usize, usize>,
    /// Alias tables that are added for parenthesized join expressions (issue #4905).
    /// These are virtual tables that point to the same columns as existing tables.
    /// They exist for column resolution (e.g., `j1.id` in `FROM t1 JOIN (...) AS j1 ON j1.id = t1.id`).
    /// Stores the table identifiers of alias tables.
    pub alias_tables: HashSet<TableIdentifier>,
    /// Tables that are shadowed by an alias table (issue #4786).
    /// When a parenthesized join has an alias, the underlying tables are shadowed
    /// and should be skipped in SELECT * expansion. Instead, the alias table's columns
    /// should be used.
    /// Maps: aliased table name -> tables shadowed by that alias
    pub shadowed_tables: HashMap<TableIdentifier, HashSet<TableIdentifier>>,
}

impl CombinedSchema {
    /// Create an empty combined schema with no tables
    ///
    /// Used for SELECT statements without a FROM clause when the expression
    /// evaluation needs outer context for column resolution.
    pub fn empty() -> Self {
        CombinedSchema {
            table_schemas: HashMap::new(),
            total_columns: 0,
            hidden_columns: HashSet::new(),
            always_hidden_columns: HashSet::new(),
            outer_schema: None,
            duplicate_aliases: HashSet::new(),
            joined_columns: HashSet::new(),
            using_coalesce_indices: HashMap::new(),
            column_replacement_map: HashMap::new(),
            alias_tables: HashSet::new(),
            shadowed_tables: HashMap::new(),
        }
    }

    /// Create a new combined schema from a single table
    ///
    /// Note: Table name is automatically normalized via TableIdentifier for case-insensitive
    /// lookups
    pub fn from_table(table_name: String, schema: vibesql_catalog::TableSchema) -> Self {
        let total_columns = schema.columns.len();
        let mut table_schemas = HashMap::new();
        let table_id = TableIdentifier::unquoted(&table_name);
        table_schemas.insert(table_id, (0, schema));
        CombinedSchema {
            table_schemas,
            total_columns,
            hidden_columns: HashSet::new(),
            always_hidden_columns: HashSet::new(),
            outer_schema: None,
            duplicate_aliases: HashSet::new(),
            joined_columns: HashSet::new(),
            using_coalesce_indices: HashMap::new(),
            column_replacement_map: HashMap::new(),
            alias_tables: HashSet::new(),
            shadowed_tables: HashMap::new(),
        }
    }

    /// Create a new combined schema from a derived table (subquery result)
    ///
    /// Note: Alias is automatically normalized via TableIdentifier for case-insensitive lookups
    pub fn from_derived_table(
        alias: String,
        column_names: Vec<String>,
        column_types: Vec<vibesql_types::DataType>,
    ) -> Self {
        let total_columns = column_names.len();

        // Build column definitions
        let columns: Vec<vibesql_catalog::ColumnSchema> = column_names
            .into_iter()
            .zip(column_types)
            .map(|(name, data_type)| vibesql_catalog::ColumnSchema {
                name,
                data_type,
                nullable: true,       // Derived table columns are always nullable
                default_value: None,  // Derived table columns have no defaults
                generated_expr: None, // Derived table columns are not generated
                collation: None,      // Derived table columns don't inherit collation
                is_exact_integer_type: false, // Derived columns don't preserve exact type
            })
            .collect();

        // A derived table (FROM subquery) has no implicit rowid, just like a
        // view: `SELECT rowid FROM (SELECT ...)` must error with
        // `no such column: rowid` (sqlite3 parity, #5492). Mark the synthetic
        // schema as a view so the rowid pseudo-column is not exposed.
        let mut schema = vibesql_catalog::TableSchema::new(alias.clone(), columns);
        schema.set_is_view(true);
        let mut table_schemas = HashMap::new();
        let table_id = TableIdentifier::unquoted(&alias);
        table_schemas.insert(table_id, (0, schema));
        CombinedSchema {
            table_schemas,
            total_columns,
            hidden_columns: HashSet::new(),
            always_hidden_columns: HashSet::new(),
            outer_schema: None,
            duplicate_aliases: HashSet::new(),
            joined_columns: HashSet::new(),
            using_coalesce_indices: HashMap::new(),
            column_replacement_map: HashMap::new(),
            alias_tables: HashSet::new(),
            shadowed_tables: HashMap::new(),
        }
    }

    /// Create a combined schema for a table-valued function FROM item
    /// (`json_each(...)`, `json_tree(...)`).
    ///
    /// Unlike [`Self::from_derived_table`], the synthesized schema is **not**
    /// marked as a view. In SQLite these TVFs expose an implicit `rowid`
    /// pseudo-column (`SELECT jx.rowid FROM json_tree(...) AS jx` succeeds),
    /// whereas a FROM-subquery has no rowid and must error on `rowid` (#5492).
    /// Because a TVF row carries no tracked row-id, the pseudo-column resolves to
    /// NULL rather than erroring — sufficient for json101-5.3..5.8, whose WHERE
    /// clauses filter every row out so the projected `rowid` is never
    /// materialized (#6019).
    pub fn from_table_function(
        alias: String,
        column_names: Vec<String>,
        column_types: Vec<vibesql_types::DataType>,
    ) -> Self {
        let total_columns = column_names.len();

        let columns: Vec<vibesql_catalog::ColumnSchema> = column_names
            .into_iter()
            .zip(column_types)
            .map(|(name, data_type)| vibesql_catalog::ColumnSchema {
                name,
                data_type,
                nullable: true,
                default_value: None,
                generated_expr: None,
                collation: None,
                is_exact_integer_type: false,
            })
            .collect();

        // NOTE: deliberately not `set_is_view(true)` — see doc comment above.
        let schema = vibesql_catalog::TableSchema::new(alias.clone(), columns);
        let mut table_schemas = HashMap::new();
        let table_id = TableIdentifier::unquoted(&alias);
        table_schemas.insert(table_id, (0, schema));
        CombinedSchema {
            table_schemas,
            total_columns,
            hidden_columns: HashSet::new(),
            always_hidden_columns: HashSet::new(),
            outer_schema: None,
            duplicate_aliases: HashSet::new(),
            joined_columns: HashSet::new(),
            using_coalesce_indices: HashMap::new(),
            column_replacement_map: HashMap::new(),
            alias_tables: HashSet::new(),
            shadowed_tables: HashMap::new(),
        }
    }

    /// Add an alias for a parenthesized join expression.
    ///
    /// This is used for expressions like `(t1 JOIN t2) AS j1` where `j1` becomes an
    /// alias for the combined result. The alias is added as a virtual table containing
    /// all visible columns, allowing references like `j1.column` to work.
    ///
    /// The alias table is marked as "alias-only" and will not be expanded in SELECT *.
    ///
    /// **Important**: The alias table uses start_idx=0 and stores the original column
    /// indices in a mapping, so that `j1.column` resolves to the correct index in the
    /// actual row data.
    pub fn add_join_alias(mut self, alias: &str) -> Self {
        // Build the alias table columns to match what SELECT * would output:
        // 1. First: USING/NATURAL JOIN columns (from joined_columns)
        // 2. Then: Other visible columns
        //
        // For USING columns, use the first index from using_coalesce_indices
        // so that j1.id resolves to the correct column position.

        // Collect USING column schemas (from joined_columns)
        let mut joined_col_entries: Vec<(usize, vibesql_catalog::ColumnSchema)> = Vec::new();
        for joined_col in &self.joined_columns {
            // Find the first column with this name (it will be the leftmost in the join)
            if let Some(indices) = self.using_coalesce_indices.get(joined_col) {
                if let Some(&first_idx) = indices.first() {
                    // Find the column schema for this index
                    for (table_id, (start_idx, table_schema)) in &self.table_schemas {
                        if self.alias_tables.contains(table_id) {
                            continue;
                        }
                        for (col_idx, col) in table_schema.columns.iter().enumerate() {
                            let absolute_idx = *start_idx + col_idx;
                            if absolute_idx == first_idx {
                                // Use the column name (lowercase matched the joined_col)
                                // but the index should be the first coalesce index
                                joined_col_entries.push((first_idx, col.clone()));
                                break;
                            }
                        }
                    }
                }
            }
        }

        // Collect all visible non-USING columns
        let mut other_columns: Vec<(usize, vibesql_catalog::ColumnSchema)> = Vec::new();
        for (table_id, (start_idx, table_schema)) in &self.table_schemas {
            if self.alias_tables.contains(table_id) {
                continue;
            }
            for (col_idx, col) in table_schema.columns.iter().enumerate() {
                let absolute_idx = *start_idx + col_idx;
                // Skip hidden columns
                if self.hidden_columns.contains(&absolute_idx) {
                    continue;
                }
                // Skip USING columns (already handled above)
                let is_joined = self.joined_columns.contains(&col.name.to_lowercase());
                if is_joined {
                    continue;
                }
                other_columns.push((absolute_idx, col.clone()));
            }
        }

        // Sort other columns by index
        other_columns.sort_by_key(|(idx, _)| *idx);

        // Combine: USING columns first (sorted by their index), then other columns
        joined_col_entries.sort_by_key(|(idx, _)| *idx);
        let mut all_columns = joined_col_entries;
        all_columns.extend(other_columns);

        let columns: Vec<vibesql_catalog::ColumnSchema> =
            all_columns.iter().map(|(_, col)| col.clone()).collect();

        let schema = vibesql_catalog::TableSchema::new(alias.to_string(), columns);
        let table_id = TableIdentifier::unquoted(alias);
        // Use start_idx = 0 so column resolution returns indices 0, 1, 2, ...
        // The order matches the all_columns order, so j1.id maps to index 0 if id is first
        self.table_schemas.insert(table_id.clone(), (0, schema));
        self.alias_tables.insert(table_id.clone());

        // Issue #4786: Mark all existing non-alias tables as shadowed by this alias.
        // This ensures that in SELECT *, the alias table's columns are used instead of
        // the individual base tables' columns. This is especially important for outer
        // joins with ON clause, where the aliased join needs to appear as a single table.
        let shadowed: HashSet<TableIdentifier> = self
            .table_schemas
            .keys()
            .filter(|t| !self.alias_tables.contains(*t) && *t != &table_id)
            .cloned()
            .collect();
        self.shadowed_tables.insert(table_id, shadowed);

        self
    }

    /// Combine two schemas (for JOIN operations)
    ///
    /// Note: Right table name is automatically normalized via TableIdentifier for case-insensitive
    /// lookups
    pub fn combine(
        left: CombinedSchema,
        right_table_name: String,
        right_schema: vibesql_catalog::TableSchema,
    ) -> Self {
        let mut table_schemas = left.table_schemas;
        let mut duplicate_aliases = left.duplicate_aliases;
        let left_total = left.total_columns;
        let right_columns = right_schema.columns.len();
        let right_id = TableIdentifier::unquoted(&right_table_name);

        // Track duplicate table alias/name - but NOT for self-joins
        // A self-join is when the same table (identical schema) is joined to itself.
        // In SQLite, self-joins like `FROM t1 JOIN t1 USING(a,b)` allow unambiguous
        // references to `t1.column` because it's the same underlying table.
        // Only mark as duplicate if it's a different table with the same alias.
        if let Some((_, existing_schema)) = table_schemas.get(&right_id) {
            // Check if it's a true self-join (same table) or alias conflict (different tables)
            // For self-joins, the schemas are identical (same table name, same columns)
            if existing_schema != &right_schema {
                // Different tables with same alias - this is ambiguous
                duplicate_aliases.insert(right_id.clone());
            }
            // If same schema, it's a self-join - don't mark as duplicate
        }

        // Always insert/overwrite the table
        table_schemas.insert(right_id, (left_total, right_schema));
        CombinedSchema {
            table_schemas,
            total_columns: left_total + right_columns,
            hidden_columns: left.hidden_columns,
            always_hidden_columns: left.always_hidden_columns,
            outer_schema: left.outer_schema,
            duplicate_aliases,
            joined_columns: left.joined_columns,
            using_coalesce_indices: left.using_coalesce_indices,
            column_replacement_map: left.column_replacement_map,
            alias_tables: left.alias_tables,
            shadowed_tables: left.shadowed_tables,
        }
    }

    /// Merge two CombinedSchemas (for JOIN operations with nested joins)
    ///
    /// Unlike `combine` which adds a single table, this method merges ALL tables
    /// from the right schema into the left schema. This is essential for nested
    /// joins like `t1 JOIN (t2 JOIN t3 USING(a)) USING(a)` where the right side
    /// contains multiple tables that must all remain visible.
    ///
    /// The right schema's tables have their start indices adjusted to account
    /// for the left schema's total column count.
    pub fn merge(left: CombinedSchema, right: CombinedSchema) -> Self {
        let mut table_schemas = left.table_schemas;
        let mut duplicate_aliases = left.duplicate_aliases;
        let left_total = left.total_columns;

        // Add all tables from right schema with adjusted start indices
        for (table_id, (start_index, schema)) in right.table_schemas {
            let adjusted_start = left_total + start_index;

            // Check if this table already exists in the left schema
            if let Some((_, existing_schema)) = table_schemas.get(&table_id) {
                // Only mark as duplicate if it's a different table with the same alias
                if existing_schema != &schema {
                    duplicate_aliases.insert(table_id.clone());
                }

                // For self-joins (same table appearing twice), we need to keep BOTH
                // entries so that USING/NATURAL join conditions can distinguish between
                // the left and right instances. Use a synthetic suffix to create a
                // unique key for the right-side instance.
                // The synthetic key format "__selfjoin_right_<original_name>_<start_idx>"
                // ensures uniqueness even for multi-way self-joins.
                let synthetic_key = TableIdentifier::unquoted(&format!(
                    "__selfjoin_right_{}_{}",
                    table_id.canonical(),
                    adjusted_start
                ));
                table_schemas.insert(synthetic_key, (adjusted_start, schema));
            } else {
                // No conflict - insert normally
                table_schemas.insert(table_id, (adjusted_start, schema));
            }
        }

        // Merge hidden columns, adjusting right side indices
        let mut hidden_columns = left.hidden_columns;
        for idx in right.hidden_columns {
            hidden_columns.insert(left_total + idx);
        }

        // Merge always-hidden (TVF SQLITE_HIDDEN) columns, adjusting right indices
        let mut always_hidden_columns = left.always_hidden_columns;
        for idx in right.always_hidden_columns {
            always_hidden_columns.insert(left_total + idx);
        }

        // Merge duplicate aliases from both sides
        duplicate_aliases.extend(right.duplicate_aliases);

        // Merge joined columns from both sides
        let mut joined_columns = left.joined_columns;
        joined_columns.extend(right.joined_columns);

        // Merge using_coalesce_indices from both sides (adjusting right side indices)
        // For N-way coalescing, we extend existing Vec entries rather than overwriting
        let mut using_coalesce_indices = left.using_coalesce_indices;
        for (col_name, indices) in right.using_coalesce_indices {
            let adjusted_indices: Vec<usize> = indices.iter().map(|idx| left_total + idx).collect();
            using_coalesce_indices
                .entry(col_name)
                .or_insert_with(Vec::new)
                .extend(adjusted_indices);
        }

        // Merge column_replacement_map from both sides (adjusting right side indices)
        let mut column_replacement_map = left.column_replacement_map;
        for (hidden_idx, replacement_idx) in right.column_replacement_map {
            column_replacement_map.insert(left_total + hidden_idx, left_total + replacement_idx);
        }

        // Merge alias_tables from both sides
        let mut alias_tables = left.alias_tables;
        alias_tables.extend(right.alias_tables);

        // Merge shadowed_tables from both sides
        let mut shadowed_tables = left.shadowed_tables;
        shadowed_tables.extend(right.shadowed_tables);

        CombinedSchema {
            table_schemas,
            total_columns: left_total + right.total_columns,
            hidden_columns,
            always_hidden_columns,
            outer_schema: left.outer_schema,
            duplicate_aliases,
            joined_columns,
            using_coalesce_indices,
            column_replacement_map,
            alias_tables,
            shadowed_tables,
        }
    }

    /// Look up a column by name (optionally qualified with table name)
    /// Uses case-insensitive matching for table/alias and column names
    ///
    /// Searches the current schema level first, then follows the outer_schema
    /// chain to search enclosing scopes (similar to SQLite's NameContext.pNext).
    /// This enables correlated subqueries to reference columns from outer queries.
    pub fn get_column_index(&self, table: Option<&str>, column: &str) -> Option<usize> {
        // Try current level first
        let current_result = if let Some(table_name) = table {
            // Qualified column reference (table.column)
            // TableIdentifier normalizes to lowercase, so lookup is case-insensitive
            let table_id = TableIdentifier::unquoted(table_name);
            if let Some((start_index, schema)) = self.table_schemas.get(&table_id) {
                // Special handling for alias tables (issue #4905)
                // For alias tables, we need to find the actual column index in the
                // underlying schema, not use start_index + idx (which would be wrong
                // since alias tables have start_index=0 but columns are non-contiguous).
                if self.alias_tables.contains(&table_id) {
                    // Check if it's a USING column - use coalesce index
                    let col_lower = column.to_lowercase();
                    if let Some(indices) = self.using_coalesce_indices.get(&col_lower) {
                        return indices.first().copied();
                    }
                    // Otherwise, find this column in the underlying (non-alias) tables
                    return self.get_column_index(None, column);
                }
                schema.get_column_index(column).map(|idx| start_index + idx)
            } else {
                None
            }
        } else {
            // Unqualified column reference - search all tables
            // IMPORTANT: For LEFT JOINs, we must resolve to the LEFTMOST table
            // that has the column. Since HashMap iteration order is non-deterministic,
            // we find ALL matches and pick the one with the lowest start_index.
            //
            // Issue #4781: For NATURAL/USING joins, prefer NON-HIDDEN columns.
            // In RIGHT JOIN USING(a), the left-side `a` is hidden, so we should
            // pick the right-side `a` (which is not hidden). This ensures
            // COALESCE semantics where the USING column picks the non-NULL value.
            let column_lower = column.to_lowercase();
            let is_joined_column = self.joined_columns.contains(&column_lower);

            let mut best_match: Option<usize> = None;
            let mut best_match_is_hidden = false;

            for (table_id, (start_index, schema)) in &self.table_schemas {
                // Skip alias tables for unqualified column resolution (issue #4905)
                // Alias tables are virtual tables that should only be accessed via
                // qualified references like `j1.column`, not unqualified references.
                if self.alias_tables.contains(table_id) {
                    continue;
                }
                if let Some(idx) = schema.get_column_index(column) {
                    let absolute_idx = start_index + idx;
                    let is_hidden = self.hidden_columns.contains(&absolute_idx);

                    // For joined columns, prefer non-hidden columns
                    // For regular columns, prefer leftmost (lowest index) as before
                    let should_update = match (best_match, is_joined_column) {
                        (None, _) => true,
                        // For joined columns: prefer non-hidden over hidden
                        (Some(_), true) if best_match_is_hidden && !is_hidden => true,
                        // For all columns: prefer lower index if same hidden status
                        (Some(current_best), _)
                            if absolute_idx < current_best
                                && (!is_joined_column || is_hidden == best_match_is_hidden) =>
                        {
                            true
                        }
                        _ => false,
                    };

                    if should_update {
                        best_match = Some(absolute_idx);
                        best_match_is_hidden = is_hidden;
                    }
                }
            }
            best_match
        };

        // If found at current level, return it
        if current_result.is_some() {
            return current_result;
        }

        // Not found at current level - search outer scopes via chain
        // This enables nested correlated subqueries to reference columns
        // from multiple enclosing scopes (issue #4493)
        //
        // Index convention (window1.test 61.1): the row paired with a chained
        // schema is laid out as `current.values ++ outer.values` (see
        // `build_merged_outer_row`), so indices resolved in the outer chain
        // must be offset by this level's column span. This keeps the current
        // level's 0-based indices valid both against the merged row (current
        // values form the prefix) and against raw current-level rows (as used
        // by the outer-correlated aggregate path over `outer_rows`, #4930 /
        // window1.test 53.0).
        if let Some(outer) = &self.outer_schema {
            return outer.get_column_index(table, column).map(|idx| self.total_columns + idx);
        }

        // Not found anywhere in the chain
        None
    }

    /// Get the declared data type for a column by its combined (absolute) index
    ///
    /// Returns `None` if the index does not fall inside any table at this
    /// schema level (e.g. it resolves into an outer scope, or belongs to an
    /// alias table). Used by columnar predicate extraction to decide whether
    /// a predicate can be pushed down to the columnar comparators (issue
    /// #5335: temporal columns need type-aware pushdown decisions).
    pub fn get_column_type_by_index(&self, idx: usize) -> Option<&vibesql_types::DataType> {
        for (table_id, (start_index, schema)) in &self.table_schemas {
            // Alias tables share start_index 0 with non-contiguous columns;
            // skip them so we only match real, contiguous column ranges.
            if self.alias_tables.contains(table_id) {
                continue;
            }
            if idx >= *start_index && idx < start_index + schema.columns.len() {
                return Some(&schema.columns[idx - start_index].data_type);
            }
        }
        None
    }

    /// Get the declared collation for a column by its combined (absolute)
    /// index
    ///
    /// Returns `None` if the column has no declared collation (default
    /// BINARY) or the index does not fall inside any table at this schema
    /// level. Used by the compiled/vectorized/columnar predicate fast paths
    /// to decline compilation for columns whose comparisons must honor a
    /// non-BINARY collation (issue #5792) — those fall back to the
    /// collation-aware expression evaluator.
    pub fn get_column_collation_by_index(&self, idx: usize) -> Option<&str> {
        for (table_id, (start_index, schema)) in &self.table_schemas {
            // Alias tables share start_index 0 with non-contiguous columns;
            // skip them so we only match real, contiguous column ranges.
            if self.alias_tables.contains(table_id) {
                continue;
            }
            if idx >= *start_index && idx < start_index + schema.columns.len() {
                return schema.columns[idx - start_index].collation.as_deref();
            }
        }
        None
    }

    /// Whether the column at the given combined index has a declared
    /// non-BINARY collation (e.g. NOCASE, RTRIM). Comparisons on such
    /// columns cannot be evaluated by the raw-value fast paths.
    pub fn column_has_non_binary_collation(&self, idx: usize) -> bool {
        matches!(
            self.get_column_collation_by_index(idx),
            Some(c) if !c.eq_ignore_ascii_case("binary")
        )
    }

    /// Get the type affinity for a column by name
    ///
    /// Returns the SQLite type affinity for the column, which determines how
    /// type coercion is performed in comparisons.
    pub fn get_column_affinity(
        &self,
        table: Option<&str>,
        column: &str,
    ) -> Option<vibesql_types::TypeAffinity> {
        if let Some(table_name) = table {
            // Qualified column reference (table.column)
            let table_id = TableIdentifier::unquoted(table_name);
            if let Some((_start_index, schema)) = self.table_schemas.get(&table_id) {
                if let Some(col_idx) = schema.get_column_index(column) {
                    return Some(schema.columns[col_idx].data_type.sqlite_affinity());
                }
            }
        } else {
            // Unqualified column reference - search all tables
            for (table_id, (_start_index, schema)) in &self.table_schemas {
                // Skip alias tables for unqualified column resolution
                if self.alias_tables.contains(table_id) {
                    continue;
                }
                if let Some(col_idx) = schema.get_column_index(column) {
                    return Some(schema.columns[col_idx].data_type.sqlite_affinity());
                }
            }
        }
        None
    }

    /// Check if an unqualified column reference is ambiguous
    /// (i.e., exists in multiple tables in the schema)
    ///
    /// Returns true if the column exists in more than one table,
    /// UNLESS the column is a "joined column" from NATURAL JOIN or USING clause.
    /// Joined columns are deduplicated and should be accessible without qualification.
    ///
    /// Only relevant for unqualified column references - qualified references
    /// (with table prefix) are never ambiguous.
    pub fn is_column_ambiguous(&self, column: &str) -> bool {
        // Columns joined via NATURAL JOIN or USING clause are never ambiguous (issue #4517)
        // They logically represent a single column even though they exist in multiple tables
        let column_lower = column.to_lowercase();
        if self.joined_columns.contains(&column_lower) {
            return false;
        }

        let mut match_count = 0;
        for (table_id, (_start_index, schema)) in &self.table_schemas {
            // Skip alias tables - they're virtual tables for column resolution only
            // and should not cause ambiguity for unqualified column references (issue #4905)
            if self.alias_tables.contains(table_id) {
                continue;
            }
            if schema.get_column_index(column).is_some() {
                match_count += 1;
                if match_count > 1 {
                    return true;
                }
            }
        }
        false
    }

    /// Check if any table in this schema has a column with the given name.
    ///
    /// This is a faster alternative to building a HashSet of all column names
    /// for cases where we just need to check if a column exists.
    /// Used for WHERE clause alias resolution (SQLite compatibility).
    ///
    /// # Arguments
    /// * `column` - The column name (case-insensitive)
    ///
    /// # Returns
    /// `true` if any table in the schema has a column matching this name
    #[inline]
    pub fn has_column(&self, column: &str) -> bool {
        // Case-insensitive search through all tables
        for (_start_index, schema) in self.table_schemas.values() {
            if schema.get_column_index(column).is_some() {
                return true;
            }
        }
        false
    }

    /// Validate that a qualified column reference is not ambiguous.
    ///
    /// This checks if the table identifier appears more than once in the FROM clause,
    /// which would make qualified references like "A.f1" ambiguous (issue #4507).
    ///
    /// # Arguments
    /// * `table` - The table name/alias from the qualified reference
    /// * `column` - The column name (used for error message only)
    ///
    /// # Returns
    /// * `Ok(())` if the reference is unambiguous
    /// * `Err(ExecutorError::AmbiguousColumnName)` if the table appears multiple times
    ///
    /// # Example
    /// ```sql
    /// -- This should fail validation:
    /// SELECT A.f1 FROM test1 A, test2 A;  -- "A" appears twice
    /// ```
    pub fn validate_qualified_reference(
        &self,
        table: &str,
        column: &str,
    ) -> Result<(), crate::errors::ExecutorError> {
        let table_id = TableIdentifier::unquoted(table);
        if self.duplicate_aliases.contains(&table_id) {
            return Err(crate::errors::ExecutorError::AmbiguousColumnName {
                column_name: format!("{}.{}", table, column),
            });
        }
        Ok(())
    }

    /// Get a table schema by name (case-insensitive lookup)
    pub fn get_table(&self, table_name: &str) -> Option<&(usize, vibesql_catalog::TableSchema)> {
        self.table_schemas.get(&TableIdentifier::unquoted(table_name))
    }

    /// Check if a table exists (case-insensitive lookup)
    pub fn contains_table(&self, table_name: &str) -> bool {
        self.table_schemas.contains_key(&TableIdentifier::unquoted(table_name))
    }

    /// Get all table names as strings (using display form)
    pub fn table_names(&self) -> Vec<String> {
        self.table_schemas.keys().map(|table_id| table_id.display().to_string()).collect()
    }

    /// Insert or update a table in the schema
    pub fn insert_table(
        &mut self,
        name: String,
        start_index: usize,
        schema: vibesql_catalog::TableSchema,
    ) {
        let table_id = TableIdentifier::unquoted(&name);
        self.table_schemas.insert(table_id, (start_index, schema));
    }

    /// Get the original column name from the schema for a column reference.
    ///
    /// SQLite preserves the schema column name (not the query identifier case)
    /// when returning column names in results. This method looks up the column
    /// in the schema and returns the original name.
    ///
    /// # Arguments
    /// * `table` - Optional table name for qualified references (e.g., "t1" in "t1.col")
    /// * `column` - Column name to look up (case-insensitive)
    ///
    /// # Returns
    /// The original column name from the schema, or the input column name if not found.
    pub fn get_original_column_name(&self, table: Option<&str>, column: &str) -> String {
        if let Some(table_name) = table {
            // Qualified column reference (table.column)
            let table_id = TableIdentifier::unquoted(table_name);
            if let Some((_start_index, schema)) = self.table_schemas.get(&table_id) {
                if let Some(idx) = schema.get_column_index(column) {
                    return schema.columns[idx].name.clone();
                }
            }
        } else {
            // Unqualified column reference - search all tables
            // Find the match with the lowest start_index (leftmost table)
            let mut best_match: Option<(usize, String)> = None;
            for (start_index, schema) in self.table_schemas.values() {
                if let Some(idx) = schema.get_column_index(column) {
                    let name = schema.columns[idx].name.clone();
                    match &best_match {
                        None => best_match = Some((*start_index, name)),
                        Some((current_start, _)) if *start_index < *current_start => {
                            best_match = Some((*start_index, name));
                        }
                        _ => {}
                    }
                }
            }
            if let Some((_, name)) = best_match {
                return name;
            }
        }
        // Fallback: return the input column name if not found in schema
        column.to_string()
    }

    /// Get the fully qualified column name with original table name prefix.
    ///
    /// This follows SQLite's `full_column_names=ON` behavior where column names
    /// in results are prefixed with the original table name from the schema.
    ///
    /// For example, if a table was created as `CREATE TABLE test1(f1 int)` and
    /// queried with `SELECT a.f1 FROM test1 a`, this returns `test1.f1` (using
    /// the original table name "test1", not the alias "a").
    ///
    /// # Arguments
    /// * `table` - Optional table alias/name for qualified references
    /// * `column` - Column name to look up (case-insensitive)
    ///
    /// # Returns
    /// The fully qualified column name in `table.column` format, or just the
    /// column name if the table is not found.
    pub fn get_full_column_name(&self, table: Option<&str>, column: &str) -> String {
        if let Some(table_name) = table {
            // Qualified column reference (table.column)
            let table_id = TableIdentifier::unquoted(table_name);
            if let Some((_start_index, schema)) = self.table_schemas.get(&table_id) {
                if let Some(idx) = schema.get_column_index(column) {
                    // Use the original table name from the schema
                    return format!("{}.{}", schema.name, schema.columns[idx].name);
                }
            }
        } else {
            // Unqualified column reference - search all tables
            // Find the match with the lowest start_index (leftmost table)
            let mut best_match: Option<(usize, String, String)> = None;
            for (start_index, schema) in self.table_schemas.values() {
                if let Some(idx) = schema.get_column_index(column) {
                    let table_name = schema.name.clone();
                    let col_name = schema.columns[idx].name.clone();
                    match &best_match {
                        None => best_match = Some((*start_index, table_name, col_name)),
                        Some((current_start, _, _)) if *start_index < *current_start => {
                            best_match = Some((*start_index, table_name, col_name));
                        }
                        _ => {}
                    }
                }
            }
            if let Some((_, table_name, col_name)) = best_match {
                return format!("{}.{}", table_name, col_name);
            }
        }
        // Fallback: return the input column name if not found in schema
        column.to_string()
    }

    /// Check if a column index is hidden from `SELECT *` expansion.
    ///
    /// Columns are hidden when they are duplicates in a NATURAL JOIN.
    /// For example, in `SELECT * FROM t1 NATURAL JOIN t2` where both tables
    /// have column `a`, the `t2.a` column is hidden so `SELECT *` only shows
    /// one copy of `a` (from t1).
    ///
    /// However, `SELECT t2.*` should still include `t2.a` because qualified
    /// wildcards expand all columns from that specific table.
    #[inline]
    pub fn is_column_hidden(&self, idx: usize) -> bool {
        self.hidden_columns.contains(&idx)
    }

    /// Mark a column as hidden from `SELECT *` expansion.
    ///
    /// This is used by NATURAL JOIN to hide duplicate columns from the right side.
    pub fn hide_column(&mut self, idx: usize) {
        self.hidden_columns.insert(idx);
    }

    /// Is this column ALWAYS hidden from `SELECT *` / `table.*` expansion?
    ///
    /// True for `SQLITE_HIDDEN`-style TVF columns (`json`/`root` of
    /// `json_each`/`json_tree`) that must be excluded from wildcard expansion
    /// unconditionally — no replacement or COALESCE partner exists. Distinct from
    /// [`is_column_hidden`], which is join-deduplication and pairs the hidden
    /// column with a replacement (issue #6050).
    #[inline]
    pub fn is_column_always_hidden(&self, idx: usize) -> bool {
        self.always_hidden_columns.contains(&idx)
    }

    /// Mark a column as always-hidden from `SELECT *` expansion (see
    /// [`is_column_always_hidden`]).
    pub fn always_hide_column(&mut self, idx: usize) {
        self.always_hidden_columns.insert(idx);
    }

    /// Mark a column name as a "joined column" from NATURAL JOIN or USING clause.
    ///
    /// Joined columns exist in multiple tables but should NOT be considered ambiguous
    /// because they are logically the same column after the join. This allows
    /// unqualified references to these columns without triggering an ambiguity error.
    ///
    /// # Arguments
    /// * `column` - The column name (will be normalized to lowercase)
    pub fn add_joined_column(&mut self, column: &str) {
        self.joined_columns.insert(column.to_lowercase());
    }

    /// Add a USING column coalesce pair for RIGHT/FULL OUTER JOINs.
    ///
    /// For USING columns in OUTER JOINs, unqualified references should use
    /// COALESCE semantics. This method records a column index for later coalesce evaluation.
    /// For chained joins, each call extends the existing Vec with new indices.
    ///
    /// # Arguments
    /// * `column` - The column name (will be normalized to lowercase)
    /// * `left_idx` - Index of the left-side column (first in the chain)
    /// * `right_idx` - Index of the right-side column (added to the chain)
    ///
    /// Issue #4783, #4903: USING column semantics in OUTER JOINs with N-way coalescing
    pub fn add_using_coalesce_pair(&mut self, column: &str, left_idx: usize, right_idx: usize) {
        let indices =
            self.using_coalesce_indices.entry(column.to_lowercase()).or_insert_with(Vec::new);

        // Issue #4909: For chained NATURAL FULL JOINs like `t3 NATURAL FULL JOIN (inner)`,
        // the Vec may already have entries from the inner join (e.g., [t4.id, t5.id]).
        // We must INSERT left_idx at the BEGINNING if not present, to get [t3.id, t4.id, t5.id].
        // This ensures COALESCE picks the leftmost non-NULL value.
        if !indices.contains(&left_idx) {
            indices.insert(0, left_idx);
        }
        // Always add right_idx at the end if not already present
        if !indices.contains(&right_idx) {
            indices.push(right_idx);
        }
    }

    /// Get the coalesce pair for a USING column, if any.
    /// For backwards compatibility, returns the first two indices as a pair.
    ///
    /// Returns Some((left_idx, right_idx)) if this column needs COALESCE
    /// semantics for OUTER JOIN USING, None otherwise.
    ///
    /// # Arguments
    /// * `column` - The column name (will be normalized to lowercase)
    pub fn get_using_coalesce_pair(&self, column: &str) -> Option<(usize, usize)> {
        self.using_coalesce_indices
            .get(&column.to_lowercase())
            .filter(|indices| indices.len() >= 2)
            .map(|indices| (indices[0], indices[1]))
    }

    /// Get all coalesce indices for a USING column (for N-way COALESCE).
    ///
    /// Returns Some(&Vec<usize>) containing all column indices that should be
    /// coalesced for this column name.
    pub fn get_using_coalesce_indices(&self, column: &str) -> Option<&Vec<usize>> {
        self.using_coalesce_indices.get(&column.to_lowercase())
    }

    /// Add a column replacement for SELECT * expansion (for RIGHT/FULL OUTER JOINs).
    ///
    /// When a hidden column has a replacement, SELECT * will output the replacement
    /// column's value at the hidden column's position, maintaining correct column ordering.
    pub fn add_column_replacement(&mut self, hidden_idx: usize, replacement_idx: usize) {
        self.column_replacement_map.insert(hidden_idx, replacement_idx);
    }

    /// Get the replacement column index for a hidden column, if any.
    pub fn get_column_replacement(&self, hidden_idx: usize) -> Option<usize> {
        self.column_replacement_map.get(&hidden_idx).copied()
    }

    /// Get all indices for N-way COALESCE for a left-side USING column (for SELECT *).
    ///
    /// In FULL OUTER JOIN with USING clause, when expanding SELECT *, we need to apply
    /// N-way COALESCE for USING columns. This method returns all indices except the first
    /// (which is the "left" index) for coalescing.
    ///
    /// Returns Some(&[indices]) if the given index is a left-side USING column, None otherwise.
    pub fn get_using_coalesce_rest_for_left(&self, left_idx: usize) -> Option<&[usize]> {
        for indices in self.using_coalesce_indices.values() {
            if !indices.is_empty() && indices[0] == left_idx && indices.len() > 1 {
                return Some(&indices[1..]);
            }
        }
        None
    }

    /// Get the right-side column index for a left-side USING column (for COALESCE in SELECT *).
    /// For backwards compatibility - returns only the second index (first "right" index).
    ///
    /// Returns Some(right_idx) if the given index is a left-side USING column, None otherwise.
    pub fn get_using_coalesce_right_for_left(&self, left_idx: usize) -> Option<usize> {
        for indices in self.using_coalesce_indices.values() {
            if !indices.is_empty() && indices[0] == left_idx && indices.len() > 1 {
                return Some(indices[1]);
            }
        }
        None
    }

    /// Check if the given column index is a right-side of a USING coalesce chain.
    ///
    /// These columns should be skipped in SELECT * output because they're
    /// represented by the first column with COALESCE applied.
    pub fn is_using_coalesce_right_side(&self, idx: usize) -> bool {
        // Check if this index appears in any position other than the first
        for indices in self.using_coalesce_indices.values() {
            if indices.len() > 1 && indices[1..].contains(&idx) {
                return true;
            }
        }
        false
    }

    /// Get all coalesce indices for a column that's anywhere in the chain.
    ///
    /// Unlike `get_using_coalesce_rest_for_left` which only works if the given index
    /// is the FIRST in the chain, this method returns all indices in the chain if
    /// the given index is found ANYWHERE in the chain. This is needed for N-way
    /// coalescing where the visible column might be in the middle of the chain.
    ///
    /// Returns Some(&Vec<usize>) if the given index is part of a coalesce chain.
    pub fn get_all_coalesce_indices_for_column(&self, idx: usize) -> Option<&Vec<usize>> {
        for indices in self.using_coalesce_indices.values() {
            if indices.contains(&idx) && indices.len() > 1 {
                return Some(indices);
            }
        }
        None
    }

    /// Compute the ordered list of output columns produced by `SELECT *`.
    ///
    /// Each entry is the COALESCE chain of absolute row indices that backs one
    /// output column. For ordinary columns the chain is a single index; for
    /// USING/NATURAL columns in OUTER JOINs the chain holds every index that
    /// participates in the N-way COALESCE (in COALESCE priority order).
    ///
    /// This mirrors the column ordering and coalescing applied by
    /// `project_row_combined` for `SELECT *`, so positional references
    /// (`ORDER BY 1`) resolve to the *output* column — including its coalesced
    /// value — rather than a single base-table column. Without this, a
    /// positional ORDER BY over a RIGHT/FULL JOIN sorts by the (often NULL)
    /// left-side column instead of the merged USING column (issue #5657).
    ///
    /// Returns `None` when the schema has no special join structure to mirror
    /// (no hidden columns, no coalesce chains, no alias/shadow tables), letting
    /// callers fall back to the simpler index-based expansion.
    pub fn wildcard_output_chains(&self) -> Option<Vec<Vec<usize>>> {
        // Only take over when there is join-specific structure to account for.
        if self.hidden_columns.is_empty()
            && self.using_coalesce_indices.is_empty()
            && self.column_replacement_map.is_empty()
            && self.alias_tables.is_empty()
            && self.shadowed_tables.is_empty()
        {
            return None;
        }

        // Mirror the aliased-join handling in project_row_combined: when an
        // alias table shadows all non-alias tables, use its column order.
        let alias_covering_all = self.alias_tables.iter().find(|alias_id| {
            if let Some(shadowed) = self.shadowed_tables.get(*alias_id) {
                let non_alias_tables: Vec<_> =
                    self.table_schemas.keys().filter(|t| !self.alias_tables.contains(*t)).collect();
                non_alias_tables.iter().all(|t| shadowed.contains(*t))
            } else {
                false
            }
        });

        let mut chains: Vec<Vec<usize>> = Vec::new();

        if let Some(alias_id) = alias_covering_all {
            if let Some((_, alias_schema)) = self.table_schemas.get(alias_id) {
                let alias_name = alias_id.display().to_string();
                for col_schema in &alias_schema.columns {
                    if let Some(actual_idx) =
                        self.get_column_index(Some(&alias_name), &col_schema.name)
                    {
                        chains.push(self.coalesce_chain_for_index(actual_idx));
                    }
                }
            }
            return Some(chains);
        }

        // No covering alias: iterate tables in start_index order, applying the
        // same skip/include rules as SELECT * projection.
        let mut sorted_tables: Vec<_> = self.table_schemas.iter().collect();
        sorted_tables.sort_by_key(|(_, (start_index, _))| *start_index);

        for (table_id, (start_index, table_schema)) in sorted_tables {
            if self.alias_tables.contains(table_id) {
                continue;
            }
            for (col_idx, _col) in table_schema.columns.iter().enumerate() {
                let abs_idx = start_index + col_idx;

                // Skip replacement targets (emitted via the hidden column's slot).
                if self.column_replacement_map.values().any(|&v| v == abs_idx) {
                    continue;
                }
                // Skip right-side USING columns (emitted via the left-side COALESCE).
                if self.is_using_coalesce_right_side(abs_idx) {
                    continue;
                }

                // Always-hidden (SQLITE_HIDDEN-style TVF) columns are excluded
                // from wildcard expansion unconditionally — no replacement or
                // COALESCE partner exists (issue #6050).
                if self.is_column_always_hidden(abs_idx) {
                    continue;
                }

                let should_include = if self.is_column_hidden(abs_idx) {
                    self.get_column_replacement(abs_idx).is_some()
                        || self.get_using_coalesce_right_for_left(abs_idx).is_some()
                } else {
                    true
                };

                if should_include {
                    chains.push(self.coalesce_chain_for_index(abs_idx));
                }
            }
        }

        Some(chains)
    }

    /// Return the COALESCE chain backing the output column at `idx`.
    ///
    /// For USING/NATURAL OUTER-JOIN columns this is the full N-way chain; for a
    /// simple hidden column with a single replacement it is `[replacement]`;
    /// otherwise it is `[idx]`. Mirrors the value selection in
    /// `project_row_combined`.
    fn coalesce_chain_for_index(&self, idx: usize) -> Vec<usize> {
        if let Some(all_indices) = self.get_all_coalesce_indices_for_column(idx) {
            return all_indices.clone();
        }
        if self.is_column_hidden(idx) {
            if let Some(replacement_idx) = self.get_column_replacement(idx) {
                return vec![replacement_idx];
            }
        }
        vec![idx]
    }

    /// Reverse-lookup the (canonical table name, column name) for an absolute
    /// row index, skipping alias tables. Used to build qualified column
    /// references when resolving positional ORDER BY over `SELECT *`.
    pub fn table_column_for_index(&self, idx: usize) -> Option<(String, String)> {
        for (table_id, (start_index, schema)) in &self.table_schemas {
            if self.alias_tables.contains(table_id) {
                continue;
            }
            if idx >= *start_index && idx < start_index + schema.columns.len() {
                let col = &schema.columns[idx - start_index];
                return Some((table_id.table_canonical().to_string(), col.name.clone()));
            }
        }
        None
    }

    /// Build a map from column names to their indices.
    ///
    /// This is used by window function frame calculations to resolve named column
    /// references in ORDER BY expressions. The map contains both the original case
    /// and lowercase versions of column names for case-insensitive matching.
    ///
    /// For columns that appear in multiple tables, the mapping prefers the leftmost
    /// table (lowest start_index) to match the behavior of unqualified column lookups.
    pub fn build_column_name_map(&self) -> std::collections::HashMap<String, usize> {
        let mut map = std::collections::HashMap::new();

        // Collect entries sorted by start_index to ensure deterministic ordering
        let mut entries: Vec<_> = self
            .table_schemas
            .iter()
            .filter(|(table_id, _)| !self.alias_tables.contains(*table_id))
            .map(|(_, (start_index, schema))| (*start_index, schema))
            .collect();
        entries.sort_by_key(|(start_index, _)| *start_index);

        for (start_index, schema) in entries {
            for (idx, col) in schema.columns.iter().enumerate() {
                let absolute_idx = start_index + idx;
                let name = &col.name;

                // Insert original case if not already present
                if !map.contains_key(name) {
                    map.insert(name.clone(), absolute_idx);
                }

                // Insert lowercase for case-insensitive matching
                let lower = name.to_lowercase();
                if !map.contains_key(&lower) {
                    map.insert(lower, absolute_idx);
                }
            }
        }

        map
    }
}

/// Builder for incrementally constructing a CombinedSchema
///
/// Builds schemas in O(n) time instead of O(n²) by tracking
/// the column offset as tables are added.
#[derive(Debug)]
pub struct SchemaBuilder {
    table_schemas: HashMap<TableIdentifier, (usize, vibesql_catalog::TableSchema)>,
    column_offset: usize,
    hidden_columns: HashSet<usize>,
    always_hidden_columns: HashSet<usize>,
    duplicate_aliases: HashSet<TableIdentifier>,
    joined_columns: HashSet<String>,
    using_coalesce_indices: HashMap<String, Vec<usize>>,
    column_replacement_map: HashMap<usize, usize>,
    alias_tables: HashSet<TableIdentifier>,
    shadowed_tables: HashMap<TableIdentifier, HashSet<TableIdentifier>>,
}

impl SchemaBuilder {
    /// Create a new empty schema builder
    pub fn new() -> Self {
        SchemaBuilder {
            table_schemas: HashMap::new(),
            column_offset: 0,
            hidden_columns: HashSet::new(),
            always_hidden_columns: HashSet::new(),
            duplicate_aliases: HashSet::new(),
            joined_columns: HashSet::new(),
            using_coalesce_indices: HashMap::new(),
            column_replacement_map: HashMap::new(),
            alias_tables: HashSet::new(),
            shadowed_tables: HashMap::new(),
        }
    }

    /// Create a schema builder initialized with an existing CombinedSchema
    ///
    /// Note: Table names are already normalized via TableIdentifier
    pub fn from_schema(schema: CombinedSchema) -> Self {
        let column_offset = schema.total_columns;
        SchemaBuilder {
            table_schemas: schema.table_schemas,
            column_offset,
            hidden_columns: schema.hidden_columns,
            always_hidden_columns: schema.always_hidden_columns,
            duplicate_aliases: schema.duplicate_aliases,
            joined_columns: schema.joined_columns,
            using_coalesce_indices: schema.using_coalesce_indices,
            column_replacement_map: schema.column_replacement_map,
            alias_tables: schema.alias_tables,
            shadowed_tables: schema.shadowed_tables,
        }
    }

    /// Add a table to the schema
    ///
    /// This is an O(1) operation - columns are not copied, just indexed
    /// Note: Table names are automatically normalized via TableIdentifier for case-insensitive
    /// lookups
    pub fn add_table(&mut self, name: String, schema: vibesql_catalog::TableSchema) -> &mut Self {
        let num_columns = schema.columns.len();
        let table_id = TableIdentifier::unquoted(&name);

        // Track duplicate table alias/name - but NOT for self-joins
        // (same logic as CombinedSchema::combine())
        if let Some((_, existing_schema)) = self.table_schemas.get(&table_id) {
            // Only mark as duplicate if it's a different table with the same alias
            if existing_schema != &schema {
                self.duplicate_aliases.insert(table_id.clone());
            }
        }

        self.table_schemas.insert(table_id, (self.column_offset, schema));
        self.column_offset += num_columns;
        self
    }

    /// Build the final CombinedSchema
    ///
    /// This consumes the builder and produces the schema in O(1) time
    pub fn build(self) -> CombinedSchema {
        CombinedSchema {
            table_schemas: self.table_schemas,
            total_columns: self.column_offset,
            hidden_columns: self.hidden_columns,
            always_hidden_columns: self.always_hidden_columns,
            outer_schema: None,
            duplicate_aliases: self.duplicate_aliases,
            joined_columns: self.joined_columns,
            using_coalesce_indices: self.using_coalesce_indices,
            column_replacement_map: self.column_replacement_map,
            alias_tables: self.alias_tables,
            shadowed_tables: self.shadowed_tables,
        }
    }

    /// Add a column replacement for SELECT * expansion (for RIGHT/FULL OUTER JOINs)
    pub fn add_column_replacement(&mut self, hidden_idx: usize, replacement_idx: usize) {
        self.column_replacement_map.insert(hidden_idx, replacement_idx);
    }
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use vibesql_catalog::ColumnSchema;
    use vibesql_types::DataType;

    use super::*;

    /// Helper to create a simple table schema with the given columns
    fn table_schema_with_columns(
        table_name: &str,
        columns: Vec<(&str, DataType)>,
    ) -> vibesql_catalog::TableSchema {
        let cols: Vec<ColumnSchema> = columns
            .into_iter()
            .map(|(name, data_type)| ColumnSchema::new(name.to_string(), data_type, true))
            .collect();
        vibesql_catalog::TableSchema::new(table_name.to_string(), cols)
    }

    /// Helper to create a table schema with a single column
    fn table_schema_with_column(
        table_name: &str,
        column_name: &str,
    ) -> vibesql_catalog::TableSchema {
        table_schema_with_columns(table_name, vec![(column_name, DataType::Integer)])
    }

    // ==========================================================================
    // CombinedSchema::from_table - Case-Insensitive Table Name Tests
    // ==========================================================================

    #[test]
    fn test_from_table_uppercase_insertion_case_insensitive_lookup() {
        // Insert with uppercase table name
        let schema = CombinedSchema::from_table(
            "ITEM".to_string(),
            table_schema_with_column("ITEM", "price"),
        );

        // All case variations should find the column
        assert!(schema.get_column_index(Some("ITEM"), "price").is_some(), "ITEM should find price");
        assert!(schema.get_column_index(Some("item"), "price").is_some(), "item should find price");
        assert!(schema.get_column_index(Some("Item"), "price").is_some(), "Item should find price");
        assert!(schema.get_column_index(Some("iTEM"), "price").is_some(), "iTEM should find price");
    }

    #[test]
    fn test_from_table_lowercase_insertion_case_insensitive_lookup() {
        // Insert with lowercase table name
        let schema = CombinedSchema::from_table(
            "item".to_string(),
            table_schema_with_column("item", "price"),
        );

        // All case variations should find the column
        assert!(schema.get_column_index(Some("ITEM"), "price").is_some());
        assert!(schema.get_column_index(Some("item"), "price").is_some());
        assert!(schema.get_column_index(Some("Item"), "price").is_some());
    }

    #[test]
    fn test_from_table_mixedcase_insertion_case_insensitive_lookup() {
        // Insert with mixed case table name
        let schema = CombinedSchema::from_table(
            "MyTable".to_string(),
            table_schema_with_column("MyTable", "id"),
        );

        // All case variations should find the column
        assert!(schema.get_column_index(Some("MYTABLE"), "id").is_some());
        assert!(schema.get_column_index(Some("mytable"), "id").is_some());
        assert!(schema.get_column_index(Some("MyTable"), "id").is_some());
        assert!(schema.get_column_index(Some("myTable"), "id").is_some());
    }

    // ==========================================================================
    // CombinedSchema::from_derived_table - Case-Insensitive Alias Tests
    // ==========================================================================

    #[test]
    fn test_from_derived_table_case_insensitive_alias() {
        // Derived table with uppercase alias
        let schema = CombinedSchema::from_derived_table(
            "SUBQ".to_string(),
            vec!["col1".to_string(), "col2".to_string()],
            vec![DataType::Integer, DataType::Varchar { max_length: None }],
        );

        // All alias case variations should work
        assert!(schema.get_column_index(Some("SUBQ"), "col1").is_some());
        assert!(schema.get_column_index(Some("subq"), "col1").is_some());
        assert!(schema.get_column_index(Some("Subq"), "col1").is_some());
    }

    /// #6019: a table-valued-function FROM item is NOT marked as a view, so its
    /// implicit `rowid` pseudo-column resolves (to NULL, no tracked row-id)
    /// rather than erroring like a FROM-subquery derived table.
    #[test]
    fn test_from_table_function_is_not_view() {
        let tvf = CombinedSchema::from_table_function(
            "jx".to_string(),
            vec!["key".to_string(), "value".to_string()],
            vec![DataType::Null, DataType::Null],
        );
        let (_, ts) = tvf.table_schemas.get(&TableIdentifier::unquoted("jx")).unwrap();
        assert!(!ts.is_view, "TVF schema must not be a view (rowid must not error)");
        assert!(!ts.without_rowid, "TVF schema must not be WITHOUT ROWID");

        // Contrast: a FROM-subquery derived table IS a view (#5492).
        let derived = CombinedSchema::from_derived_table(
            "subq".to_string(),
            vec!["x".to_string()],
            vec![DataType::Integer],
        );
        let (_, ds) = derived.table_schemas.get(&TableIdentifier::unquoted("subq")).unwrap();
        assert!(ds.is_view, "derived-table schema must remain a view");
    }

    // ==========================================================================
    // CombinedSchema::combine - Multi-Table Case-Insensitive Tests
    // ==========================================================================

    #[test]
    fn test_combine_case_insensitive_both_tables() {
        // Create left schema with uppercase
        let left = CombinedSchema::from_table(
            "ORDERS".to_string(),
            table_schema_with_columns(
                "ORDERS",
                vec![("order_id", DataType::Integer), ("customer_id", DataType::Integer)],
            ),
        );

        // Combine with right table using different case
        let combined = CombinedSchema::combine(
            left,
            "Items".to_string(),
            table_schema_with_columns(
                "Items",
                vec![("item_id", DataType::Integer), ("price", DataType::DoublePrecision)],
            ),
        );

        // Verify left table columns accessible with any case
        assert!(combined.get_column_index(Some("orders"), "order_id").is_some());
        assert!(combined.get_column_index(Some("ORDERS"), "order_id").is_some());
        assert!(combined.get_column_index(Some("Orders"), "customer_id").is_some());

        // Verify right table columns accessible with any case
        assert!(combined.get_column_index(Some("items"), "item_id").is_some());
        assert!(combined.get_column_index(Some("ITEMS"), "item_id").is_some());
        assert!(combined.get_column_index(Some("Items"), "price").is_some());

        // Verify correct indices (left table starts at 0, right at 2)
        assert_eq!(combined.get_column_index(Some("orders"), "order_id"), Some(0));
        assert_eq!(combined.get_column_index(Some("orders"), "customer_id"), Some(1));
        assert_eq!(combined.get_column_index(Some("items"), "item_id"), Some(2));
        assert_eq!(combined.get_column_index(Some("items"), "price"), Some(3));
    }

    #[test]
    fn test_combine_multiple_joins_case_insensitive() {
        // Simulate a 3-way join: orders JOIN customers JOIN items
        let orders = CombinedSchema::from_table(
            "O".to_string(), // short alias
            table_schema_with_column("O", "order_id"),
        );

        let with_customers = CombinedSchema::combine(
            orders,
            "C".to_string(),
            table_schema_with_column("C", "customer_id"),
        );

        let with_items = CombinedSchema::combine(
            with_customers,
            "I".to_string(),
            table_schema_with_column("I", "item_id"),
        );

        // All aliases should be case-insensitive
        assert!(with_items.get_column_index(Some("o"), "order_id").is_some());
        assert!(with_items.get_column_index(Some("O"), "order_id").is_some());
        assert!(with_items.get_column_index(Some("c"), "customer_id").is_some());
        assert!(with_items.get_column_index(Some("C"), "customer_id").is_some());
        assert!(with_items.get_column_index(Some("i"), "item_id").is_some());
        assert!(with_items.get_column_index(Some("I"), "item_id").is_some());
    }

    // ==========================================================================
    // CombinedSchema::get_column_index - Unqualified Column Lookup
    // ==========================================================================

    #[test]
    fn test_unqualified_column_lookup_no_ambiguity() {
        let schema = CombinedSchema::from_table(
            "USERS".to_string(),
            table_schema_with_columns(
                "USERS",
                vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: None })],
            ),
        );

        // Unqualified lookup should work
        assert!(schema.get_column_index(None, "id").is_some());
        assert!(schema.get_column_index(None, "name").is_some());
        assert!(schema.get_column_index(None, "missing").is_none());
    }

    #[test]
    fn test_column_case_sensitive_with_fallback() {
        // Column created with mixed case (simulating a delimited identifier like "UserName")
        let schema = CombinedSchema::from_table(
            "users".to_string(),
            table_schema_with_column("users", "UserName"),
        );

        // Exact case match works
        assert!(schema.get_column_index(Some("users"), "UserName").is_some());
        // Case-insensitive fallback also works for backward compatibility
        assert!(schema.get_column_index(Some("users"), "username").is_some());
        assert!(schema.get_column_index(Some("users"), "USERNAME").is_some());
    }

    /// Test case for issue #4111: TPC-DS Q6 scenario
    /// Schema created with lowercase column names (from data loader)
    /// Query uses uppercase identifiers (from parser normalization)
    #[test]
    fn test_tpcds_q6_case_insensitive_column_lookup_issue_4111() {
        // Simulate TPC-DS item table with lowercase columns (as created by data loader)
        let schema = CombinedSchema::from_table(
            "J".to_string(), // Uppercase alias from parser
            table_schema_with_columns(
                "item",
                vec![
                    ("i_item_sk", DataType::Integer),
                    ("i_current_price", DataType::DoublePrecision), // lowercase!
                    ("i_category", DataType::Varchar { max_length: None }),
                ],
            ),
        );

        // Query uses uppercase column names (from parser normalization)
        // This is the exact pattern that fails in TPC-DS Q6:
        // SELECT AVG(j.i_current_price) FROM item j WHERE j.i_category = i.i_category
        assert!(
            schema.get_column_index(Some("J"), "I_CURRENT_PRICE").is_some(),
            "J.I_CURRENT_PRICE should find i_current_price via case-insensitive lookup"
        );
        assert!(
            schema.get_column_index(Some("J"), "I_CATEGORY").is_some(),
            "J.I_CATEGORY should find i_category via case-insensitive lookup"
        );
        assert!(
            schema.get_column_index(Some("j"), "I_CURRENT_PRICE").is_some(),
            "j.I_CURRENT_PRICE should find i_current_price"
        );
        assert!(
            schema.get_column_index(Some("J"), "i_current_price").is_some(),
            "J.i_current_price should find via exact match"
        );
    }

    #[test]
    fn test_column_distinct_cases_exact_match() {
        // When there are multiple columns with different cases (via delimited identifiers),
        // exact match takes precedence
        let cols: Vec<vibesql_catalog::ColumnSchema> = vec![
            vibesql_catalog::ColumnSchema::new("value".to_string(), DataType::Integer, true),
            vibesql_catalog::ColumnSchema::new("VALUE".to_string(), DataType::Integer, true),
            vibesql_catalog::ColumnSchema::new("Value".to_string(), DataType::Integer, true),
        ];
        let table_schema = vibesql_catalog::TableSchema::new("data".to_string(), cols);
        let schema = CombinedSchema::from_table("data".to_string(), table_schema);

        // Each case variation should find its specific column
        assert_eq!(schema.get_column_index(Some("data"), "value"), Some(0));
        assert_eq!(schema.get_column_index(Some("data"), "VALUE"), Some(1));
        assert_eq!(schema.get_column_index(Some("data"), "Value"), Some(2));
    }

    // ==========================================================================
    // SchemaBuilder - Case-Insensitive Tests
    // ==========================================================================

    #[test]
    fn test_schema_builder_add_table_case_insensitive() {
        let mut builder = SchemaBuilder::new();

        // Add tables with different case
        builder.add_table("ORDERS".to_string(), table_schema_with_column("ORDERS", "order_id"));
        builder.add_table("Items".to_string(), table_schema_with_column("Items", "item_id"));

        let schema = builder.build();

        // All case variations should work
        assert!(schema.get_column_index(Some("orders"), "order_id").is_some());
        assert!(schema.get_column_index(Some("ORDERS"), "order_id").is_some());
        assert!(schema.get_column_index(Some("items"), "item_id").is_some());
        assert!(schema.get_column_index(Some("ITEMS"), "item_id").is_some());
    }

    #[test]
    fn test_schema_builder_from_schema_preserves_case_insensitivity() {
        // Create initial schema with uppercase table name
        let initial = CombinedSchema::from_table(
            "PRODUCTS".to_string(),
            table_schema_with_columns(
                "PRODUCTS",
                vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: None })],
            ),
        );

        // Verify initial schema works
        assert!(initial.get_column_index(Some("products"), "id").is_some());

        // Create builder from schema and add another table
        let mut builder = SchemaBuilder::from_schema(initial);
        builder
            .add_table("Categories".to_string(), table_schema_with_column("Categories", "cat_id"));

        let final_schema = builder.build();

        // Original table should still be case-insensitive
        assert!(final_schema.get_column_index(Some("products"), "id").is_some());
        assert!(final_schema.get_column_index(Some("PRODUCTS"), "id").is_some());
        assert!(final_schema.get_column_index(Some("Products"), "name").is_some());

        // New table should also be case-insensitive
        assert!(final_schema.get_column_index(Some("categories"), "cat_id").is_some());
        assert!(final_schema.get_column_index(Some("CATEGORIES"), "cat_id").is_some());
    }

    #[test]
    fn test_schema_builder_from_schema_multiple_tables() {
        // Create combined schema with multiple tables
        let orders = CombinedSchema::from_table(
            "Orders".to_string(),
            table_schema_with_column("Orders", "order_id"),
        );
        let combined = CombinedSchema::combine(
            orders,
            "Items".to_string(),
            table_schema_with_column("Items", "item_id"),
        );

        // Create builder from combined schema
        let mut builder = SchemaBuilder::from_schema(combined);
        builder
            .add_table("CUSTOMERS".to_string(), table_schema_with_column("CUSTOMERS", "cust_id"));

        let final_schema = builder.build();

        // All tables should be case-insensitive
        assert!(final_schema.get_column_index(Some("orders"), "order_id").is_some());
        assert!(final_schema.get_column_index(Some("ORDERS"), "order_id").is_some());
        assert!(final_schema.get_column_index(Some("items"), "item_id").is_some());
        assert!(final_schema.get_column_index(Some("ITEMS"), "item_id").is_some());
        assert!(final_schema.get_column_index(Some("customers"), "cust_id").is_some());
        assert!(final_schema.get_column_index(Some("CUSTOMERS"), "cust_id").is_some());

        // Verify column offsets are correct
        assert_eq!(final_schema.get_column_index(Some("orders"), "order_id"), Some(0));
        assert_eq!(final_schema.get_column_index(Some("items"), "item_id"), Some(1));
        assert_eq!(final_schema.get_column_index(Some("customers"), "cust_id"), Some(2));
    }

    // ==========================================================================
    // Regression Tests for Issue #3633
    // ==========================================================================

    #[test]
    fn test_issue_3633_correlated_subquery_alias_case() {
        // This test verifies the fix for issue #3633 where correlated subqueries
        // with uppercase aliases (like "J") couldn't find columns because the
        // parser uses uppercase but the schema stored lowercase.

        // Simulate the scenario: outer query has table with alias "J"
        let schema = CombinedSchema::from_table(
            "J".to_string(), // Parser often uppercases aliases
            table_schema_with_columns(
                "items",
                vec![("price", DataType::DoublePrecision), ("quantity", DataType::Integer)],
            ),
        );

        // The correlated subquery should be able to reference J.price
        // regardless of case used by the parser/resolver
        assert!(
            schema.get_column_index(Some("J"), "price").is_some(),
            "Uppercase J should find price (parser case)"
        );
        assert!(
            schema.get_column_index(Some("j"), "price").is_some(),
            "Lowercase j should find price (normalized case)"
        );
    }

    #[test]
    fn test_issue_3633_multi_table_join_with_aliases() {
        // Simulates: SELECT * FROM orders O JOIN items I ON O.id = I.order_id
        let orders = CombinedSchema::from_table(
            "O".to_string(),
            table_schema_with_columns(
                "orders",
                vec![("id", DataType::Integer), ("date", DataType::Date)],
            ),
        );

        let combined = CombinedSchema::combine(
            orders,
            "I".to_string(),
            table_schema_with_columns(
                "items",
                vec![("order_id", DataType::Integer), ("amount", DataType::DoublePrecision)],
            ),
        );

        // Both O and I aliases should work case-insensitively
        // This is critical for correlated subqueries that reference outer aliases
        assert_eq!(combined.get_column_index(Some("O"), "id"), Some(0));
        assert_eq!(combined.get_column_index(Some("o"), "id"), Some(0));
        assert_eq!(combined.get_column_index(Some("O"), "date"), Some(1));
        assert_eq!(combined.get_column_index(Some("I"), "order_id"), Some(2));
        assert_eq!(combined.get_column_index(Some("i"), "order_id"), Some(2));
        assert_eq!(combined.get_column_index(Some("I"), "amount"), Some(3));
    }

    // ==========================================================================
    // Edge Cases
    // ==========================================================================

    #[test]
    fn test_nonexistent_table_returns_none() {
        let schema = CombinedSchema::from_table(
            "users".to_string(),
            table_schema_with_column("users", "id"),
        );

        assert!(schema.get_column_index(Some("nonexistent"), "id").is_none());
        assert!(schema.get_column_index(Some("NONEXISTENT"), "id").is_none());
    }

    #[test]
    fn test_nonexistent_column_returns_none() {
        let schema = CombinedSchema::from_table(
            "users".to_string(),
            table_schema_with_column("users", "id"),
        );

        assert!(schema.get_column_index(Some("users"), "nonexistent").is_none());
        assert!(schema.get_column_index(Some("USERS"), "nonexistent").is_none());
    }

    #[test]
    fn test_empty_table_name() {
        let schema = CombinedSchema::from_table("".to_string(), table_schema_with_column("", "id"));

        // Empty string table should still work
        assert!(schema.get_column_index(Some(""), "id").is_some());
    }

    #[test]
    fn test_total_columns_tracking() {
        let mut builder = SchemaBuilder::new();
        builder.add_table(
            "t1".to_string(),
            table_schema_with_columns(
                "t1",
                vec![("a", DataType::Integer), ("b", DataType::Integer)],
            ),
        );
        builder.add_table(
            "t2".to_string(),
            table_schema_with_columns("t2", vec![("c", DataType::Integer)]),
        );

        let schema = builder.build();
        assert_eq!(schema.total_columns, 3);
    }

    // ==========================================================================
    // Ambiguous Column Detection Tests (Issue #4391)
    // ==========================================================================

    #[test]
    fn test_is_column_ambiguous_single_table() {
        // Single table - no column can be ambiguous
        let schema = CombinedSchema::from_table(
            "test1".to_string(),
            table_schema_with_columns(
                "test1",
                vec![("f1", DataType::Integer), ("f2", DataType::Integer)],
            ),
        );

        assert!(!schema.is_column_ambiguous("f1"));
        assert!(!schema.is_column_ambiguous("f2"));
        assert!(!schema.is_column_ambiguous("nonexistent"));
    }

    #[test]
    fn test_is_column_ambiguous_two_tables_no_overlap() {
        // Two tables with different columns - no ambiguity
        let test1 = CombinedSchema::from_table(
            "test1".to_string(),
            table_schema_with_columns(
                "test1",
                vec![("f1", DataType::Integer), ("f2", DataType::Integer)],
            ),
        );
        let schema = CombinedSchema::combine(
            test1,
            "test2".to_string(),
            table_schema_with_columns(
                "test2",
                vec![("f3", DataType::Integer), ("f4", DataType::Integer)],
            ),
        );

        assert!(!schema.is_column_ambiguous("f1"));
        assert!(!schema.is_column_ambiguous("f2"));
        assert!(!schema.is_column_ambiguous("f3"));
        assert!(!schema.is_column_ambiguous("f4"));
    }

    #[test]
    fn test_is_column_ambiguous_two_tables_with_overlap() {
        // Two tables with same column names - should be ambiguous
        // This is the exact scenario from issue #4391:
        // CREATE TABLE test1(f1, f2);
        // CREATE TABLE test2(f1, f2);
        // SELECT f1 FROM test1, test2;
        let test1 = CombinedSchema::from_table(
            "test1".to_string(),
            table_schema_with_columns(
                "test1",
                vec![("f1", DataType::Integer), ("f2", DataType::Integer)],
            ),
        );
        let schema = CombinedSchema::combine(
            test1,
            "test2".to_string(),
            table_schema_with_columns(
                "test2",
                vec![("f1", DataType::Integer), ("f2", DataType::Integer)],
            ),
        );

        // Both f1 and f2 exist in both tables - should be ambiguous
        assert!(schema.is_column_ambiguous("f1"), "f1 should be ambiguous");
        assert!(schema.is_column_ambiguous("f2"), "f2 should be ambiguous");

        // Nonexistent columns are not ambiguous (they just don't exist)
        assert!(!schema.is_column_ambiguous("f3"));
    }

    #[test]
    fn test_is_column_ambiguous_case_insensitive() {
        // Column names should be matched case-insensitively
        let test1 = CombinedSchema::from_table(
            "test1".to_string(),
            table_schema_with_columns("test1", vec![("F1", DataType::Integer)]),
        );
        let schema = CombinedSchema::combine(
            test1,
            "test2".to_string(),
            table_schema_with_columns("test2", vec![("f1", DataType::Integer)]),
        );

        // F1 and f1 should be considered the same column, so it's ambiguous
        assert!(schema.is_column_ambiguous("f1"));
        assert!(schema.is_column_ambiguous("F1"));
        assert!(schema.is_column_ambiguous("F1")); // Mixed case
    }

    #[test]
    fn test_is_column_ambiguous_partial_overlap() {
        // Two tables where only some columns overlap
        let test1 = CombinedSchema::from_table(
            "test1".to_string(),
            table_schema_with_columns(
                "test1",
                vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: None })],
            ),
        );
        let schema = CombinedSchema::combine(
            test1,
            "test2".to_string(),
            table_schema_with_columns(
                "test2",
                vec![
                    ("id", DataType::Integer),    // Same as test1
                    ("value", DataType::Integer), // Different from test1
                ],
            ),
        );

        // id is in both tables - ambiguous
        assert!(schema.is_column_ambiguous("id"));

        // name only in test1, value only in test2 - not ambiguous
        assert!(!schema.is_column_ambiguous("name"));
        assert!(!schema.is_column_ambiguous("value"));
    }

    #[test]
    fn test_is_column_ambiguous_three_tables() {
        // Three tables where a column appears in multiple (but not all)
        let t1 = CombinedSchema::from_table(
            "t1".to_string(),
            table_schema_with_columns(
                "t1",
                vec![("a", DataType::Integer), ("b", DataType::Integer)],
            ),
        );
        let t1_t2 = CombinedSchema::combine(
            t1,
            "t2".to_string(),
            table_schema_with_columns(
                "t2",
                vec![("b", DataType::Integer), ("c", DataType::Integer)],
            ),
        );
        let schema = CombinedSchema::combine(
            t1_t2,
            "t3".to_string(),
            table_schema_with_columns(
                "t3",
                vec![("c", DataType::Integer), ("d", DataType::Integer)],
            ),
        );

        // a only in t1 - not ambiguous
        assert!(!schema.is_column_ambiguous("a"));

        // b in t1 and t2 - ambiguous
        assert!(schema.is_column_ambiguous("b"));

        // c in t2 and t3 - ambiguous
        assert!(schema.is_column_ambiguous("c"));

        // d only in t3 - not ambiguous
        assert!(!schema.is_column_ambiguous("d"));
    }

    // ==========================================================================
    // Joined Column Tests (Issue #4517 - NATURAL JOIN columns not ambiguous)
    // ==========================================================================

    #[test]
    fn test_joined_column_not_ambiguous_natural_join() {
        // Test that columns marked as "joined" via NATURAL JOIN are not ambiguous
        // This simulates: SELECT b FROM t1 NATURAL JOIN t2 WHERE t1.b = t2.b AND t1.c = t2.c
        let t1 = CombinedSchema::from_table(
            "t1".to_string(),
            table_schema_with_columns(
                "t1",
                vec![("a", DataType::Integer), ("b", DataType::Integer), ("c", DataType::Integer)],
            ),
        );
        let mut schema = CombinedSchema::combine(
            t1,
            "t2".to_string(),
            table_schema_with_columns(
                "t2",
                vec![("b", DataType::Integer), ("c", DataType::Integer), ("d", DataType::Integer)],
            ),
        );

        // Before marking as joined, b and c should be ambiguous
        assert!(
            schema.is_column_ambiguous("b"),
            "b should be ambiguous before NATURAL JOIN processing"
        );
        assert!(
            schema.is_column_ambiguous("c"),
            "c should be ambiguous before NATURAL JOIN processing"
        );

        // Mark b and c as joined columns (as would happen in NATURAL JOIN processing)
        schema.add_joined_column("b");
        schema.add_joined_column("c");

        // After marking as joined, b and c should NOT be ambiguous
        assert!(!schema.is_column_ambiguous("b"), "b should NOT be ambiguous after NATURAL JOIN");
        assert!(!schema.is_column_ambiguous("c"), "c should NOT be ambiguous after NATURAL JOIN");

        // a only in t1, d only in t2 - never ambiguous
        assert!(!schema.is_column_ambiguous("a"));
        assert!(!schema.is_column_ambiguous("d"));
    }

    #[test]
    fn test_joined_column_case_insensitive() {
        // Test that joined column matching is case-insensitive
        let t1 = CombinedSchema::from_table(
            "t1".to_string(),
            table_schema_with_columns("t1", vec![("Name", DataType::Integer)]),
        );
        let mut schema = CombinedSchema::combine(
            t1,
            "t2".to_string(),
            table_schema_with_columns("t2", vec![("NAME", DataType::Integer)]),
        );

        // Before marking as joined, should be ambiguous
        assert!(schema.is_column_ambiguous("name"));
        assert!(schema.is_column_ambiguous("NAME"));
        assert!(schema.is_column_ambiguous("Name"));

        // Mark as joined with lowercase
        schema.add_joined_column("name");

        // All case variants should now be non-ambiguous
        assert!(!schema.is_column_ambiguous("name"));
        assert!(!schema.is_column_ambiguous("NAME"));
        assert!(!schema.is_column_ambiguous("Name"));
    }

    #[test]
    fn test_joined_column_with_using_clause() {
        // Test USING clause behavior (similar to NATURAL JOIN but explicit columns)
        // This simulates: SELECT id FROM t1 JOIN t2 USING(id)
        let t1 = CombinedSchema::from_table(
            "t1".to_string(),
            table_schema_with_columns(
                "t1",
                vec![("id", DataType::Integer), ("value1", DataType::Integer)],
            ),
        );
        let mut schema = CombinedSchema::combine(
            t1,
            "t2".to_string(),
            table_schema_with_columns(
                "t2",
                vec![("id", DataType::Integer), ("value2", DataType::Integer)],
            ),
        );

        // Mark id as joined (as would happen in USING clause processing)
        schema.add_joined_column("id");

        // id should NOT be ambiguous (it's a USING column)
        assert!(!schema.is_column_ambiguous("id"));

        // value1 and value2 are unique to their tables - not ambiguous
        assert!(!schema.is_column_ambiguous("value1"));
        assert!(!schema.is_column_ambiguous("value2"));
    }

    // ==========================================================================
    // wildcard_output_chains / coalesce_chain_for_index (issue #5657)
    // ==========================================================================

    /// A plain join with no USING/NATURAL coalescing has no special structure,
    /// so wildcard_output_chains returns None and callers fall back to the
    /// simple index expansion.
    #[test]
    fn test_wildcard_output_chains_none_for_plain_join() {
        let t1 = CombinedSchema::from_table(
            "t1".to_string(),
            table_schema_with_columns(
                "t1",
                vec![("a", DataType::Integer), ("b", DataType::Integer)],
            ),
        );
        let schema = CombinedSchema::combine(
            t1,
            "t2".to_string(),
            table_schema_with_columns(
                "t2",
                vec![("c", DataType::Integer), ("d", DataType::Integer)],
            ),
        );

        assert!(schema.wildcard_output_chains().is_none());
    }

    /// For a USING/NATURAL OUTER JOIN coalesce chain, the merged column's output
    /// chain holds every participating index (in COALESCE order), and the
    /// right-side member is suppressed from the output sequence. This is what
    /// lets positional ORDER BY sort by the merged value (issue #5657).
    #[test]
    fn test_wildcard_output_chains_coalesces_using_column() {
        // Simulate `t4 NATURAL RIGHT JOIN t5`: both have `id`; the left `id`
        // (index 0) is hidden and coalesces with the right `id` (index 2).
        let t4 = CombinedSchema::from_table(
            "t4".to_string(),
            table_schema_with_columns(
                "t4",
                vec![("id", DataType::Integer), ("x", DataType::Varchar { max_length: None })],
            ),
        );
        let mut schema = CombinedSchema::combine(
            t4,
            "t5".to_string(),
            table_schema_with_columns(
                "t5",
                vec![("id", DataType::Integer), ("y", DataType::Varchar { max_length: None })],
            ),
        );
        // id at index 0 (t4) and index 2 (t5) form the coalesce chain.
        schema.add_joined_column("id");
        schema.add_using_coalesce_pair("id", 0, 2);
        schema.hide_column(0);

        let chains = schema.wildcard_output_chains().expect("join structure present");

        // Output columns: merged id (chain [0,2]), t4.x (idx 1), t5.y (idx 3).
        // The right-side id (idx 2) must not appear as its own output column.
        assert_eq!(chains.len(), 3, "merged id collapses to one output column");
        assert_eq!(chains[0], vec![0, 2], "id coalesces left then right");
        assert_eq!(chains[1], vec![1], "t4.x is a plain column");
        assert_eq!(chains[2], vec![3], "t5.y is a plain column");
    }

    /// table_column_for_index reverse-maps an absolute row index back to its
    /// (canonical table, column) so positional ORDER BY can build qualified
    /// column references for the coalesce chain.
    #[test]
    fn test_table_column_for_index_reverse_lookup() {
        let t1 = CombinedSchema::from_table(
            "t1".to_string(),
            table_schema_with_columns(
                "t1",
                vec![("a", DataType::Integer), ("b", DataType::Integer)],
            ),
        );
        let schema = CombinedSchema::combine(
            t1,
            "t2".to_string(),
            table_schema_with_columns(
                "t2",
                vec![("c", DataType::Integer), ("d", DataType::Integer)],
            ),
        );

        assert_eq!(schema.table_column_for_index(0), Some(("t1".to_string(), "a".to_string())));
        assert_eq!(schema.table_column_for_index(3), Some(("t2".to_string(), "d".to_string())));
        assert_eq!(schema.table_column_for_index(99), None);
    }
}
