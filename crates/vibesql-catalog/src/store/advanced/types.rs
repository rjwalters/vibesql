//! User-defined type management methods.

use crate::{errors::CatalogError, type_definition::TypeDefinition};

impl super::super::Catalog {
    // ============================================================================
    // Type Definition Management Methods
    // ============================================================================

    /// Create a new user-defined type.
    pub fn create_type(&mut self, type_def: TypeDefinition) -> Result<(), CatalogError> {
        let type_name = type_def.name.clone();
        if self.type_definitions.contains_key(&type_name) {
            return Err(CatalogError::TypeAlreadyExists(type_name));
        }
        self.type_definitions.insert(type_name, type_def);
        Ok(())
    }

    /// Drop a user-defined type.
    pub fn drop_type(&mut self, name: &str, cascade: bool) -> Result<(), CatalogError> {
        if !self.type_definitions.contains_key(name) {
            return Err(CatalogError::TypeNotFound(name.to_string()));
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
                                if type_name == name {
                                    return Err(CatalogError::TypeInUse(name.to_string()));
                                }
                            }
                        }
                    }
                }
            }
        }

        self.type_definitions.remove(name);

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
                                if type_name == name {
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
        self.type_definitions.get(name)
    }

    /// Check if a type exists.
    pub fn type_exists(&self, name: &str) -> bool {
        self.type_definitions.contains_key(name)
    }

    /// List all user-defined type names.
    pub fn list_types(&self) -> Vec<String> {
        self.type_definitions.keys().cloned().collect()
    }
}
