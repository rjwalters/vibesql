use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

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
    /// Map from table name (normalized via TableKey) to (start_index, TableSchema)
    /// start_index is where this table's columns begin in the combined row
    /// Keys are always lowercase for case-insensitive lookups
    pub table_schemas: HashMap<TableKey, (usize, vibesql_catalog::TableSchema)>,
    /// Total number of columns across all tables
    pub total_columns: usize,
}

impl CombinedSchema {
    /// Create a new combined schema from a single table
    ///
    /// Note: Table name is automatically normalized via TableKey for case-insensitive lookups
    pub fn from_table(table_name: String, schema: vibesql_catalog::TableSchema) -> Self {
        let total_columns = schema.columns.len();
        let mut table_schemas = HashMap::new();
        // TableKey automatically normalizes to lowercase
        table_schemas.insert(TableKey::new(table_name), (0, schema));
        CombinedSchema { table_schemas, total_columns }
    }

    /// Create a new combined schema from a derived table (subquery result)
    ///
    /// Note: Alias is automatically normalized via TableKey for case-insensitive lookups
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
            })
            .collect();

        let schema = vibesql_catalog::TableSchema::new(alias.clone(), columns);
        let mut table_schemas = HashMap::new();
        // TableKey automatically normalizes to lowercase
        table_schemas.insert(TableKey::new(alias), (0, schema));
        CombinedSchema { table_schemas, total_columns }
    }

    /// Combine two schemas (for JOIN operations)
    ///
    /// Note: Right table name is automatically normalized via TableKey for case-insensitive lookups
    pub fn combine(
        left: CombinedSchema,
        right_table: impl Into<TableKey>,
        right_schema: vibesql_catalog::TableSchema,
    ) -> Self {
        let mut table_schemas = left.table_schemas;
        let left_total = left.total_columns;
        let right_columns = right_schema.columns.len();
        // TableKey automatically normalizes to lowercase
        table_schemas.insert(right_table.into(), (left_total, right_schema));
        CombinedSchema { table_schemas, total_columns: left_total + right_columns }
    }

    /// Look up a column by name (optionally qualified with table name)
    /// Uses case-insensitive matching for table/alias and column names
    pub fn get_column_index(&self, table: Option<&str>, column: &str) -> Option<usize> {
        if let Some(table_name) = table {
            // Qualified column reference (table.column)
            // TableKey normalizes to lowercase, so lookup is case-insensitive
            let key = TableKey::new(table_name);
            if let Some((start_index, schema)) = self.table_schemas.get(&key) {
                return schema.get_column_index(column).map(|idx| start_index + idx);
            }
            None
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
        }
    }

    /// Get a table schema by name (case-insensitive lookup)
    pub fn get_table(&self, table_name: &str) -> Option<&(usize, vibesql_catalog::TableSchema)> {
        self.table_schemas.get(&TableKey::new(table_name))
    }

    /// Check if a table exists (case-insensitive lookup)
    pub fn contains_table(&self, table_name: &str) -> bool {
        self.table_schemas.contains_key(&TableKey::new(table_name))
    }

    /// Get all table names as strings
    pub fn table_names(&self) -> Vec<String> {
        self.table_schemas.keys().map(|k| k.to_string()).collect()
    }

    /// Insert or update a table in the schema
    pub fn insert_table(
        &mut self,
        name: impl Into<TableKey>,
        start_index: usize,
        schema: vibesql_catalog::TableSchema,
    ) {
        self.table_schemas.insert(name.into(), (start_index, schema));
    }
}

/// Builder for incrementally constructing a CombinedSchema
///
/// Builds schemas in O(n) time instead of O(n²) by tracking
/// the column offset as tables are added.
#[derive(Debug)]
pub struct SchemaBuilder {
    table_schemas: HashMap<TableKey, (usize, vibesql_catalog::TableSchema)>,
    column_offset: usize,
}

impl SchemaBuilder {
    /// Create a new empty schema builder
    pub fn new() -> Self {
        SchemaBuilder { table_schemas: HashMap::new(), column_offset: 0 }
    }

    /// Create a schema builder initialized with an existing CombinedSchema
    ///
    /// Note: Table names are already normalized via TableKey
    pub fn from_schema(schema: CombinedSchema) -> Self {
        let column_offset = schema.total_columns;
        // TableKeys are already normalized, just pass them through
        SchemaBuilder { table_schemas: schema.table_schemas, column_offset }
    }

    /// Add a table to the schema
    ///
    /// This is an O(1) operation - columns are not copied, just indexed
    /// Note: Table names are automatically normalized via TableKey for case-insensitive lookups
    pub fn add_table(&mut self, name: impl Into<TableKey>, schema: vibesql_catalog::TableSchema) -> &mut Self {
        let num_columns = schema.columns.len();
        // TableKey automatically normalizes to lowercase
        self.table_schemas.insert(name.into(), (self.column_offset, schema));
        self.column_offset += num_columns;
        self
    }

    /// Build the final CombinedSchema
    ///
    /// This consumes the builder and produces the schema in O(1) time
    pub fn build(self) -> CombinedSchema {
        CombinedSchema { table_schemas: self.table_schemas, total_columns: self.column_offset }
    }
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_catalog::ColumnSchema;
    use vibesql_types::DataType;

    /// Helper to create a simple table schema with the given columns
    fn table_schema_with_columns(table_name: &str, columns: Vec<(&str, DataType)>) -> vibesql_catalog::TableSchema {
        let cols: Vec<ColumnSchema> = columns
            .into_iter()
            .map(|(name, data_type)| ColumnSchema::new(name.to_string(), data_type, true))
            .collect();
        vibesql_catalog::TableSchema::new(table_name.to_string(), cols)
    }

    /// Helper to create a table schema with a single column
    fn table_schema_with_column(table_name: &str, column_name: &str) -> vibesql_catalog::TableSchema {
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
            table_schema_with_columns("ORDERS", vec![("order_id", DataType::Integer), ("customer_id", DataType::Integer)]),
        );

        // Combine with right table using different case
        let combined = CombinedSchema::combine(
            left,
            "Items".to_string(),
            table_schema_with_columns("Items", vec![("item_id", DataType::Integer), ("price", DataType::DoublePrecision)]),
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
            table_schema_with_columns("USERS", vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: None })]),
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
        builder.add_table(
            "ORDERS".to_string(),
            table_schema_with_column("ORDERS", "order_id"),
        );
        builder.add_table(
            "Items".to_string(),
            table_schema_with_column("Items", "item_id"),
        );

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
            table_schema_with_columns("PRODUCTS", vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: None })]),
        );

        // Verify initial schema works
        assert!(initial.get_column_index(Some("products"), "id").is_some());

        // Create builder from schema and add another table
        let mut builder = SchemaBuilder::from_schema(initial);
        builder.add_table(
            "Categories".to_string(),
            table_schema_with_column("Categories", "cat_id"),
        );

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
        builder.add_table(
            "CUSTOMERS".to_string(),
            table_schema_with_column("CUSTOMERS", "cust_id"),
        );

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
            table_schema_with_columns("items", vec![("price", DataType::DoublePrecision), ("quantity", DataType::Integer)]),
        );

        // The correlated subquery should be able to reference J.price
        // regardless of case used by the parser/resolver
        assert!(schema.get_column_index(Some("J"), "price").is_some(),
            "Uppercase J should find price (parser case)");
        assert!(schema.get_column_index(Some("j"), "price").is_some(),
            "Lowercase j should find price (normalized case)");
    }

    #[test]
    fn test_issue_3633_multi_table_join_with_aliases() {
        // Simulates: SELECT * FROM orders O JOIN items I ON O.id = I.order_id
        let orders = CombinedSchema::from_table(
            "O".to_string(),
            table_schema_with_columns("orders", vec![("id", DataType::Integer), ("date", DataType::Date)]),
        );

        let combined = CombinedSchema::combine(
            orders,
            "I".to_string(),
            table_schema_with_columns("items", vec![("order_id", DataType::Integer), ("amount", DataType::DoublePrecision)]),
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
        let schema = CombinedSchema::from_table(
            "".to_string(),
            table_schema_with_column("", "id"),
        );

        // Empty string table should still work
        assert!(schema.get_column_index(Some(""), "id").is_some());
    }

    #[test]
    fn test_total_columns_tracking() {
        let mut builder = SchemaBuilder::new();
        builder.add_table(
            "t1".to_string(),
            table_schema_with_columns("t1", vec![("a", DataType::Integer), ("b", DataType::Integer)]),
        );
        builder.add_table(
            "t2".to_string(),
            table_schema_with_columns("t2", vec![("c", DataType::Integer)]),
        );

        let schema = builder.build();
        assert_eq!(schema.total_columns, 3);
    }
}
