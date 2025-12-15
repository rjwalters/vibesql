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
    /// Reference to outer scope schema for nested subquery column resolution (issue #4493)
    /// Forms a linked-list chain similar to SQLite's NameContext.pNext
    /// Enables resolution of columns from multiple nesting levels
    pub outer_schema: Option<Box<CombinedSchema>>,
    /// Table aliases/names that appear more than once in the FROM clause (issue #4507)
    /// Used to detect ambiguous qualified column references like "A.f1" when table "A" appears twice
    /// Stores normalized (lowercase) table identifiers for case-insensitive matching
    pub duplicate_aliases: HashSet<TableIdentifier>,
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
            outer_schema: None,
            duplicate_aliases: HashSet::new(),
        }
    }

    /// Create a new combined schema from a single table
    ///
    /// Note: Table name is automatically normalized via TableIdentifier for case-insensitive lookups
    pub fn from_table(table_name: String, schema: vibesql_catalog::TableSchema) -> Self {
        let total_columns = schema.columns.len();
        let mut table_schemas = HashMap::new();
        let table_id = TableIdentifier::unquoted(&table_name);
        table_schemas.insert(table_id, (0, schema));
        CombinedSchema {
            table_schemas,
            total_columns,
            hidden_columns: HashSet::new(),
            outer_schema: None,
            duplicate_aliases: HashSet::new(),
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
                nullable: true,      // Derived table columns are always nullable
                default_value: None, // Derived table columns have no defaults
                generated_expr: None, // Derived table columns are not generated
            })
            .collect();

        let schema = vibesql_catalog::TableSchema::new(alias.clone(), columns);
        let mut table_schemas = HashMap::new();
        let table_id = TableIdentifier::unquoted(&alias);
        table_schemas.insert(table_id, (0, schema));
        CombinedSchema {
            table_schemas,
            total_columns,
            hidden_columns: HashSet::new(),
            outer_schema: None,
            duplicate_aliases: HashSet::new(),
        }
    }

    /// Combine two schemas (for JOIN operations)
    ///
    /// Note: Right table name is automatically normalized via TableIdentifier for case-insensitive lookups
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
            outer_schema: left.outer_schema,
            duplicate_aliases,
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
            // Track duplicate table alias/name - but NOT for self-joins
            // (same logic as combine())
            if let Some((_, existing_schema)) = table_schemas.get(&table_id) {
                // Only mark as duplicate if it's a different table with the same alias
                if existing_schema != &schema {
                    duplicate_aliases.insert(table_id.clone());
                }
            }

            let adjusted_start = left_total + start_index;
            table_schemas.insert(table_id, (adjusted_start, schema));
        }

        // Merge hidden columns, adjusting right side indices
        let mut hidden_columns = left.hidden_columns;
        for idx in right.hidden_columns {
            hidden_columns.insert(left_total + idx);
        }

        // Merge duplicate aliases from both sides
        duplicate_aliases.extend(right.duplicate_aliases);

        CombinedSchema {
            table_schemas,
            total_columns: left_total + right.total_columns,
            hidden_columns,
            outer_schema: left.outer_schema,
            duplicate_aliases,
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
                schema.get_column_index(column).map(|idx| start_index + idx)
            } else {
                None
            }
        } else {
            // Unqualified column reference - search all tables
            // IMPORTANT: For LEFT JOINs, we must resolve to the LEFTMOST table
            // that has the column. Since HashMap iteration order is non-deterministic,
            // we find ALL matches and pick the one with the lowest start_index.
            let mut best_match: Option<usize> = None;
            for (start_index, schema) in self.table_schemas.values() {
                if let Some(idx) = schema.get_column_index(column) {
                    let absolute_idx = start_index + idx;
                    match best_match {
                        None => best_match = Some(absolute_idx),
                        Some(current_best) if absolute_idx < current_best => {
                            best_match = Some(absolute_idx);
                        }
                        _ => {}
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
        if let Some(outer) = &self.outer_schema {
            return outer.get_column_index(table, column);
        }

        // Not found anywhere in the chain
        None
    }

    /// Check if an unqualified column reference is ambiguous
    /// (i.e., exists in multiple tables in the schema)
    ///
    /// Returns true if the column exists in more than one table.
    /// Only relevant for unqualified column references - qualified references
    /// (with table prefix) are never ambiguous.
    pub fn is_column_ambiguous(&self, column: &str) -> bool {
        let mut match_count = 0;
        for (_start_index, schema) in self.table_schemas.values() {
            if schema.get_column_index(column).is_some() {
                match_count += 1;
                if match_count > 1 {
                    return true;
                }
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
    pub fn validate_qualified_reference(&self, table: &str, column: &str) -> Result<(), crate::errors::ExecutorError> {
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
    duplicate_aliases: HashSet<TableIdentifier>,
}

impl SchemaBuilder {
    /// Create a new empty schema builder
    pub fn new() -> Self {
        SchemaBuilder {
            table_schemas: HashMap::new(),
            column_offset: 0,
            hidden_columns: HashSet::new(),
            duplicate_aliases: HashSet::new(),
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
            duplicate_aliases: schema.duplicate_aliases,
        }
    }

    /// Add a table to the schema
    ///
    /// This is an O(1) operation - columns are not copied, just indexed
    /// Note: Table names are automatically normalized via TableIdentifier for case-insensitive lookups
    pub fn add_table(
        &mut self,
        name: String,
        schema: vibesql_catalog::TableSchema,
    ) -> &mut Self {
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
            outer_schema: None,
            duplicate_aliases: self.duplicate_aliases,
        }
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
                    ("id", DataType::Integer), // Same as test1
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
}
