//! Domain management methods.

use crate::{domain::DomainDefinition, errors::CatalogError};

impl super::super::Catalog {
    // ============================================================================
    // Domain Management Methods
    // ============================================================================

    /// Create a new domain.
    pub fn create_domain(&mut self, domain: DomainDefinition) -> Result<(), CatalogError> {
        let name = domain.name.clone();
        if self.domains.contains_key(&name) {
            return Err(CatalogError::DomainAlreadyExists(name));
        }
        self.domains.insert(name, domain);
        Ok(())
    }

    /// Get a domain definition by name.
    pub fn get_domain(&self, name: &str) -> Option<&DomainDefinition> {
        self.domains.get(name)
    }

    /// Drop a domain.
    pub fn drop_domain(&mut self, name: &str, cascade: bool) -> Result<(), CatalogError> {
        if !self.domains.contains_key(name) {
            return Err(CatalogError::DomainNotFound(name.to_string()));
        }

        // Check if any columns use this domain
        // Note: Domain support in column definitions is not fully implemented yet
        // This code provides the framework for when it is implemented
        let mut columns_using_domain = Vec::new();

        for schema in self.schemas.values() {
            for table_name in schema.list_tables() {
                if let Some(table) = schema.get_table(&table_name, false) {
                    for column in &table.columns {
                        // Check if column type is a UserDefined type that matches this domain
                        // In the future, when domains are properly integrated, we might have
                        // a DataType::Domain variant or track domain usage separately
                        if let vibesql_types::DataType::UserDefined { type_name } =
                            &column.data_type
                        {
                            // Check if this user-defined type is actually a domain
                            if self.domains.contains_key(type_name) && type_name == name {
                                columns_using_domain
                                    .push((table_name.clone(), column.name.clone()));
                            }
                        }
                    }
                }
            }
        }

        // If RESTRICT and domain is in use, return error
        if !cascade && !columns_using_domain.is_empty() {
            return Err(CatalogError::DomainInUse {
                domain_name: name.to_string(),
                dependent_columns: columns_using_domain,
            });
        }

        // If CASCADE, we would need to either:
        // 1. Convert columns to the domain's base type
        // 2. Or drop the columns/tables using the domain
        // For now, since domain support isn't fully implemented, we'll just proceed
        if cascade && !columns_using_domain.is_empty() {
            // Get the domain to find its base type
            let domain = self.domains.get(name).cloned();

            if let Some(domain_def) = domain {
                // Convert all columns using this domain to the domain's base type
                for (table_name, column_name) in columns_using_domain {
                    for schema in self.schemas.values_mut() {
                        if let Some(table) = schema.get_table(&table_name, false) {
                            let mut modified_table = table.clone();
                            for col in &mut modified_table.columns {
                                if col.name == column_name {
                                    // Replace domain type with base type
                                    col.data_type = domain_def.data_type.clone();
                                }
                            }
                            // Drop and recreate table with modified columns
                            schema.drop_table(&table_name, true)?;
                            schema.create_table(modified_table)?;
                            break;
                        }
                    }
                }
            }
        }

        self.domains.remove(name);
        Ok(())
    }

    /// Check if a domain exists.
    pub fn domain_exists(&self, name: &str) -> bool {
        self.domains.contains_key(&name.to_uppercase())
    }

    /// List all domain names.
    pub fn list_domains(&self) -> Vec<String> {
        self.domains.keys().cloned().collect()
    }
}
