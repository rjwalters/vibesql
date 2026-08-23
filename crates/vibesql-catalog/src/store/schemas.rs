//! Schema management operations for the catalog.
//!
//! This module handles creation, deletion, and querying of database schemas.

use crate::{errors::CatalogError, schema::Schema};

impl super::Catalog {
    /// Create a new schema.
    ///
    /// The lookup key is ASCII case-folded regardless of quoting, matching
    /// the identifier-folding rule already applied to table/column names
    /// (issue #5553): a schema created as `CREATE SCHEMA "myApp"` is looked
    /// up the same way whether later referenced as `myApp`, `"myApp"`, or
    /// `"MYAPP"`. Without this, `Catalog::create_table_with_identifier`
    /// (which resolves a schema-qualified table's schema via
    /// `TableIdentifier::schema_canonical()`, always folded) could never find
    /// a schema stored under its unfolded display spelling — see #6497.
    pub fn create_schema(&mut self, name: String) -> Result<(), CatalogError> {
        let canonical = name.to_ascii_lowercase();
        if self.schemas.contains_key(&canonical) {
            return Err(CatalogError::SchemaAlreadyExists(name));
        }
        self.schemas.insert(canonical, Schema::new(name));
        Ok(())
    }

    /// Drop a schema.
    pub fn drop_schema(&mut self, name: &str, cascade: bool) -> Result<(), CatalogError> {
        let canonical = name.to_ascii_lowercase();

        // Don't allow dropping the default schema
        if canonical == crate::DEFAULT_SCHEMA {
            return Err(CatalogError::SchemaNotEmpty(crate::DEFAULT_SCHEMA.to_string()));
        }

        let schema = self
            .schemas
            .get(&canonical)
            .ok_or_else(|| CatalogError::SchemaNotFound(name.to_string()))?;

        if !cascade && !schema.is_empty() {
            return Err(CatalogError::SchemaNotEmpty(name.to_string()));
        }

        self.schemas.remove(&canonical);
        Ok(())
    }

    /// Get a schema by name (ASCII case-folded lookup; see `create_schema`).
    pub fn get_schema(&self, name: &str) -> Option<&Schema> {
        self.schemas.get(&name.to_ascii_lowercase())
    }

    /// List all schema names.
    pub fn list_schemas(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    /// Check if schema exists (ASCII case-folded lookup; see `create_schema`).
    pub fn schema_exists(&self, name: &str) -> bool {
        self.schemas.contains_key(&name.to_ascii_lowercase())
    }

    /// Set the current schema for unqualified table references.
    pub fn set_current_schema(&mut self, name: &str) -> Result<(), CatalogError> {
        let canonical = name.to_ascii_lowercase();
        if !self.schemas.contains_key(&canonical) {
            return Err(CatalogError::SchemaNotFound(name.to_string()));
        }
        // Store the folded form so it always matches the `self.schemas` key
        // space (see `create_schema`) when used to resolve unqualified table
        // references elsewhere.
        self.current_schema = canonical;
        Ok(())
    }

    /// Get the current schema name.
    pub fn get_current_schema(&self) -> &str {
        &self.current_schema
    }
}
