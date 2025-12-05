use std::collections::HashMap;

/// Represents the combined schema from multiple tables (for JOINs)
#[derive(Debug, Clone)]
pub struct CombinedSchema {
    /// Map from table name to (start_index, TableSchema)
    /// start_index is where this table's columns begin in the combined row
    pub table_schemas: HashMap<String, (usize, vibesql_catalog::TableSchema)>,
    /// Total number of columns across all tables
    pub total_columns: usize,
}

impl CombinedSchema {
    /// Create a new combined schema from a single table
    pub fn from_table(table_name: String, schema: vibesql_catalog::TableSchema) -> Self {
        let total_columns = schema.columns.len();
        let mut table_schemas = HashMap::new();
        // Use lowercase table name for consistent lookups in predicate decomposition
        table_schemas.insert(table_name.to_lowercase(), (0, schema));
        CombinedSchema { table_schemas, total_columns }
    }

    /// Create a new combined schema from a derived table (subquery result)
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
        // Use lowercase alias for consistent lookups
        table_schemas.insert(alias.to_lowercase(), (0, schema));
        CombinedSchema { table_schemas, total_columns }
    }

    /// Combine two schemas (for JOIN operations)
    pub fn combine(
        left: CombinedSchema,
        right_table: String,
        right_schema: vibesql_catalog::TableSchema,
    ) -> Self {
        let mut table_schemas = left.table_schemas;
        let left_total = left.total_columns;
        let right_columns = right_schema.columns.len();
        // Use lowercase table name for consistent lookups
        table_schemas.insert(right_table.to_lowercase(), (left_total, right_schema));
        CombinedSchema { table_schemas, total_columns: left_total + right_columns }
    }

    /// Look up a column by name (optionally qualified with table name)
    /// Uses case-insensitive matching for table/alias and column names
    pub fn get_column_index(&self, table: Option<&str>, column: &str) -> Option<usize> {
        if let Some(table_name) = table {
            // Qualified column reference (table.column)
            // Try exact match first for performance
            if let Some((start_index, schema)) = self.table_schemas.get(table_name) {
                return schema.get_column_index(column).map(|idx| start_index + idx);
            }

            // Fall back to case-insensitive table/alias name lookup
            let table_name_lower = table_name.to_lowercase();
            for (key, (start_index, schema)) in self.table_schemas.iter() {
                if key.to_lowercase() == table_name_lower {
                    return schema.get_column_index(column).map(|idx| start_index + idx);
                }
            }
            None
        } else {
            // Unqualified column reference - search all tables
            for (start_index, schema) in self.table_schemas.values() {
                if let Some(idx) = schema.get_column_index(column) {
                    return Some(start_index + idx);
                }
            }
            None
        }
    }
}

/// Builder for incrementally constructing a CombinedSchema
///
/// Builds schemas in O(n) time instead of O(n²) by tracking
/// the column offset as tables are added.
#[derive(Debug)]
pub struct SchemaBuilder {
    table_schemas: HashMap<String, (usize, vibesql_catalog::TableSchema)>,
    column_offset: usize,
}

impl SchemaBuilder {
    /// Create a new empty schema builder
    pub fn new() -> Self {
        SchemaBuilder { table_schemas: HashMap::new(), column_offset: 0 }
    }

    /// Create a schema builder initialized with an existing CombinedSchema
    pub fn from_schema(schema: CombinedSchema) -> Self {
        let column_offset = schema.total_columns;
        SchemaBuilder { table_schemas: schema.table_schemas, column_offset }
    }

    /// Add a table to the schema
    ///
    /// This is an O(1) operation - columns are not copied, just indexed
    pub fn add_table(&mut self, name: String, schema: vibesql_catalog::TableSchema) -> &mut Self {
        let num_columns = schema.columns.len();
        self.table_schemas.insert(name, (self.column_offset, schema));
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
