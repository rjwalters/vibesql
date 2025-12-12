//! User-defined type management methods.

use crate::{errors::CatalogError, type_definition::TypeDefinition};

impl super::super::Catalog {
    // ============================================================================
    // Type Definition Management Methods
    // ============================================================================

    /// Create a new user-defined type.
    pub fn create_type(&mut self, type_def: TypeDefinition) -> Result<(), CatalogError> {
        // Normalize name to lowercase for case-insensitive lookup
        let type_name = type_def.name.to_lowercase();
        if self.type_definitions.contains_key(&type_name) {
            return Err(CatalogError::TypeAlreadyExists(type_name));
        }
        let mut normalized_type = type_def;
        normalized_type.name = type_name.clone();
        self.type_definitions.insert(type_name, normalized_type);
        Ok(())
    }

    /// Drop a user-defined type.
    pub fn drop_type(&mut self, name: &str, cascade: bool) -> Result<(), CatalogError> {
        let normalized_name = name.to_lowercase();
        if !self.type_definitions.contains_key(&normalized_name) {
            return Err(CatalogError::TypeNotFound(normalized_name));
        }

        // Check for dependencies if not CASCADE
        if !cascade {
            // Check if any tables use this type
            for schema in self.schemas.values() {
                for table_name in schema.list_tables() {
                    if let Some(table) = schema.get_table(&table_name, false) {
                        for column in &table.columns {
                            if let vibesql_types::DataType::UserDefined { type_name } =
                                &column.data_type
                            {
                                if type_name.to_lowercase() == normalized_name {
                                    return Err(CatalogError::TypeInUse(normalized_name.clone()));
                                }
                            }
                        }
                    }
                }
            }
        }

        self.type_definitions.remove(&normalized_name);

        // If CASCADE, also drop dependent objects (tables with columns of this type)
        if cascade {
            let mut tables_to_drop = Vec::new();
            for (schema_name, schema) in &self.schemas {
                for table_name in schema.list_tables() {
                    if let Some(table) = schema.get_table(&table_name, false) {
                        for column in &table.columns {
                            if let vibesql_types::DataType::UserDefined { type_name } =
                                &column.data_type
                            {
                                if type_name.to_lowercase() == normalized_name {
                                    tables_to_drop.push(format!("{}.{}", schema_name, table_name));
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            // Drop the dependent tables
            for qualified_table_name in tables_to_drop {
                let _ = self.drop_table(&qualified_table_name);
            }
        }

        Ok(())
    }

    /// Get a type definition by name.
    pub fn get_type(&self, name: &str) -> Option<&TypeDefinition> {
        self.type_definitions.get(&name.to_lowercase())
    }

    /// Check if a type exists.
    pub fn type_exists(&self, name: &str) -> bool {
        self.type_definitions.contains_key(&name.to_lowercase())
    }

    /// List all user-defined type names.
    pub fn list_types(&self) -> Vec<String> {
        self.type_definitions.keys().cloned().collect()
    }
}
