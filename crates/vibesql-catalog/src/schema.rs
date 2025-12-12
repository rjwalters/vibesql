//! Schema - Named collection of database objects

use std::collections::HashMap;

use crate::{errors::CatalogError, identifier::TableIdentifier, table::TableSchema};

/// A schema - named collection of database objects (tables, views, etc.)
#[derive(Debug, Clone)]
pub struct Schema {
    pub name: String,
    /// Tables stored with canonical name as key (lowercase for unquoted, exact for quoted)
    tables: HashMap<String, TableSchema>,
    /// Track which tables were created with quoted identifiers
    table_identifiers: HashMap<String, TableIdentifier>,
    // Future: views, functions, sequences, etc.
}

impl Schema {
    /// Create a new empty schema
    pub fn new(name: String) -> Self {
        Schema { name, tables: HashMap::new(), table_identifiers: HashMap::new() }
    }

    /// Create a table in this schema using SQL:1999 identifier semantics.
    ///
    /// The `identifier` parameter determines case-sensitivity:
    /// - Quoted identifiers: stored with exact case, case-sensitive lookup
    /// - Unquoted identifiers: stored with lowercase canonical form
    pub fn create_table_with_identifier(
        &mut self,
        schema: TableSchema,
        identifier: TableIdentifier,
    ) -> Result<(), CatalogError> {
        let canonical = identifier.canonical().to_string();

        // Check if table already exists using canonical form
        if self.tables.contains_key(&canonical) {
            return Err(CatalogError::TableAlreadyExists(identifier.display().to_string()));
        }

        self.tables.insert(canonical.clone(), schema);
        self.table_identifiers.insert(canonical, identifier);
        Ok(())
    }

    /// Create a table in this schema
    /// Legacy method for backward compatibility - treats names as unquoted (case-insensitive)
    pub fn create_table_with_case_mode(
        &mut self,
        schema: TableSchema,
        case_sensitive: bool,
    ) -> Result<(), CatalogError> {
        // Convert to TableIdentifier based on case_sensitive mode
        // If case_sensitive is true, treat as if it was quoted (preserve case)
        // If case_sensitive is false, treat as unquoted (normalize to lowercase)
        let identifier = TableIdentifier::new(&schema.name, case_sensitive);
        self.create_table_with_identifier(schema, identifier)
    }

    /// Create a table in this schema (legacy method, assumes case-insensitive/unquoted)
    pub fn create_table(&mut self, schema: TableSchema) -> Result<(), CatalogError> {
        let identifier = TableIdentifier::new(&schema.name, false);
        self.create_table_with_identifier(schema, identifier)
    }

    /// Get a table schema by name using TableIdentifier for proper case handling.
    pub fn get_table_by_identifier(&self, identifier: &TableIdentifier) -> Option<&TableSchema> {
        self.tables.get(identifier.canonical())
    }

    /// Get a table schema by name with optional case-insensitive lookup
    /// Legacy method for backward compatibility
    pub fn get_table(&self, name: &str, case_sensitive: bool) -> Option<&TableSchema> {
        // Create a TableIdentifier to get proper canonical form
        let identifier = TableIdentifier::new(name, case_sensitive);
        self.tables.get(identifier.canonical())
    }

    /// Drop a table from this schema using TableIdentifier for proper case handling.
    pub fn drop_table_by_identifier(
        &mut self,
        identifier: &TableIdentifier,
    ) -> Result<(), CatalogError> {
        let canonical = identifier.canonical();
        if self.tables.remove(canonical).is_some() {
            self.table_identifiers.remove(canonical);
            Ok(())
        } else {
            Err(CatalogError::TableNotFound { table_name: identifier.display().to_string() })
        }
    }

    /// Drop a table from this schema with optional case-insensitive lookup
    /// Legacy method for backward compatibility
    pub fn drop_table(&mut self, name: &str, case_sensitive: bool) -> Result<(), CatalogError> {
        let identifier = TableIdentifier::new(name, case_sensitive);
        self.drop_table_by_identifier(&identifier)
    }

    /// List all table names in this schema (returns canonical names)
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Get the TableIdentifier for a table by its canonical name
    pub fn get_table_identifier(&self, canonical: &str) -> Option<&TableIdentifier> {
        self.table_identifiers.get(canonical)
    }

    /// Check if table exists in this schema (case-insensitive by default)
    pub fn table_exists(&self, name: &str) -> bool {
        self.get_table(name, false).is_some()
    }

    /// Check if table exists using TableIdentifier
    pub fn table_exists_by_identifier(&self, identifier: &TableIdentifier) -> bool {
        self.tables.contains_key(identifier.canonical())
    }

    /// Check if schema is empty (no tables)
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Get the number of tables in this schema
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}
