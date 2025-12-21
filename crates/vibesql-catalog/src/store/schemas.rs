//! Schema management operations for the catalog.
//!
//! This module handles creation, deletion, and querying of database schemas.

use crate::{errors::CatalogError, schema::Schema};

impl super::Catalog {
    /// Create a new schema.
    pub fn create_schema(&mut self, name: String) -> Result<(), CatalogError> {
        if self.schemas.contains_key(&name) {
            return Err(CatalogError::SchemaAlreadyExists(name));
        }
        self.schemas.insert(name.clone(), Schema::new(name));
        Ok(())
    }

    /// Drop a schema.
    pub fn drop_schema(&mut self, name: &str, cascade: bool) -> Result<(), CatalogError> {
        // Don't allow dropping the default schema
        if name == crate::DEFAULT_SCHEMA {
            return Err(CatalogError::SchemaNotEmpty(crate::DEFAULT_SCHEMA.to_string()));
        }

        let schema =
            self.schemas.get(name).ok_or_else(|| CatalogError::SchemaNotFound(name.to_string()))?;

        if !cascade && !schema.is_empty() {
            return Err(CatalogError::SchemaNotEmpty(name.to_string()));
        }

        self.schemas.remove(name);
        Ok(())
    }

    /// Get a schema by name.
    pub fn get_schema(&self, name: &str) -> Option<&Schema> {
        self.schemas.get(name)
    }

    /// List all schema names.
    pub fn list_schemas(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    /// Check if schema exists.
    pub fn schema_exists(&self, name: &str) -> bool {
        self.schemas.contains_key(name)
    }

    /// Set the current schema for unqualified table references.
    pub fn set_current_schema(&mut self, name: &str) -> Result<(), CatalogError> {
        if !self.schema_exists(name) {
            return Err(CatalogError::SchemaNotFound(name.to_string()));
        }
        self.current_schema = name.to_string();
        Ok(())
    }

    /// Get the current schema name.
    pub fn get_current_schema(&self) -> &str {
        &self.current_schema
    }
}
