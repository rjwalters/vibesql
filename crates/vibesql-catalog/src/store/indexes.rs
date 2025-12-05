//! Index management operations for the catalog
//!
//! This module provides methods for creating, dropping, and querying
//! index metadata in the database catalog.

use crate::{index::IndexMetadata, Catalog, CatalogError};

impl Catalog {
    /// Add an index to the catalog
    pub fn add_index(&mut self, index: IndexMetadata) -> Result<(), CatalogError> {
        let qualified_name = index.qualified_name();

        // Check if index already exists
        if self.indexes.contains_key(&qualified_name) {
            return Err(CatalogError::IndexAlreadyExists {
                index_name: index.name.clone(),
                table_name: index.table_name.clone(),
            });
        }

        // Verify table exists
        let case_sensitive = self.case_sensitive_identifiers;
        let table_exists = self
            .schemas
            .get(&self.current_schema)
            .and_then(|schema| schema.get_table(&index.table_name, case_sensitive))
            .is_some();

        if !table_exists {
            return Err(CatalogError::TableNotFound { table_name: index.table_name.clone() });
        }

        // Verify all columns exist in the table
        let table = self
            .schemas
            .get(&self.current_schema)
            .and_then(|schema| schema.get_table(&index.table_name, case_sensitive))
            .unwrap(); // Safe because we checked above

        for col in &index.columns {
            if !table.columns.iter().any(|c| c.name == col.column_name) {
                return Err(CatalogError::ColumnNotFound {
                    column_name: col.column_name.clone(),
                    table_name: index.table_name.clone(),
                });
            }
        }

        self.indexes.insert(qualified_name, index);
        Ok(())
    }

    /// Remove an index from the catalog
    pub fn drop_index(
        &mut self,
        table_name: &str,
        index_name: &str,
    ) -> Result<IndexMetadata, CatalogError> {
        let qualified_name = format!("{}.{}", table_name, index_name);

        self.indexes.remove(&qualified_name).ok_or_else(|| CatalogError::IndexNotFound {
            index_name: index_name.to_string(),
            table_name: table_name.to_string(),
        })
    }

    /// Get an index by table and index name
    pub fn get_index(&self, table_name: &str, index_name: &str) -> Option<&IndexMetadata> {
        let qualified_name = format!("{}.{}", table_name, index_name);
        self.indexes.get(&qualified_name)
    }

    /// Get all indexes for a specific table
    pub fn get_table_indexes(&self, table_name: &str) -> Vec<&IndexMetadata> {
        self.indexes.values().filter(|index| index.table_name == table_name).collect()
    }

    /// List all indexes in the catalog
    pub fn list_all_indexes(&self) -> Vec<&IndexMetadata> {
        self.indexes.values().collect()
    }

    /// Drop all indexes associated with a table (called when dropping table)
    pub fn drop_table_indexes(&mut self, table_name: &str) -> Vec<IndexMetadata> {
        let indexes_to_remove: Vec<String> = self
            .indexes
            .iter()
            .filter(|(_, index)| index.table_name == table_name)
            .map(|(qualified_name, _)| qualified_name.clone())
            .collect();

        indexes_to_remove
            .into_iter()
            .filter_map(|qualified_name| self.indexes.remove(&qualified_name))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        index::{IndexType, IndexedColumn, SortOrder},
        ColumnSchema, TableSchema,
    };

    fn create_test_catalog() -> Catalog {
        let mut catalog = Catalog::new();
        // Use case-insensitive identifiers to match create_table() behavior
        catalog.set_case_sensitive_identifiers(false);

        use vibesql_types::DataType;

        // Create a test table
        let table = TableSchema::new(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    true,
                ),
                ColumnSchema::new(
                    "email".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    true,
                ),
            ],
        );

        catalog.schemas.get_mut(&catalog.current_schema).unwrap().create_table(table).unwrap();

        catalog
    }

    #[test]
    fn test_add_index() {
        let mut catalog = create_test_catalog();

        let index = IndexMetadata::new(
            "idx_name".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn {
                column_name: "name".to_string(),
                order: SortOrder::Ascending,
                prefix_length: None,
            }],
            false,
        );

        assert!(catalog.add_index(index).is_ok());
        assert!(catalog.get_index("users", "idx_name").is_some());
    }

    #[test]
    fn test_add_duplicate_index() {
        let mut catalog = create_test_catalog();

        let index = IndexMetadata::new(
            "idx_name".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn {
                column_name: "name".to_string(),
                order: SortOrder::Ascending,
                prefix_length: None,
            }],
            false,
        );

        catalog.add_index(index.clone()).unwrap();
        let result = catalog.add_index(index);

        assert!(matches!(result, Err(CatalogError::IndexAlreadyExists { .. })));
    }

    #[test]
    fn test_add_index_nonexistent_table() {
        let mut catalog = create_test_catalog();

        let index = IndexMetadata::new(
            "idx_name".to_string(),
            "nonexistent".to_string(),
            IndexType::BTree,
            vec![IndexedColumn {
                column_name: "name".to_string(),
                order: SortOrder::Ascending,
                prefix_length: None,
            }],
            false,
        );

        let result = catalog.add_index(index);
        assert!(matches!(result, Err(CatalogError::TableNotFound { .. })));
    }

    #[test]
    fn test_add_index_nonexistent_column() {
        let mut catalog = create_test_catalog();

        let index = IndexMetadata::new(
            "idx_age".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn {
                column_name: "age".to_string(), // Column doesn't exist
                order: SortOrder::Ascending,
                prefix_length: None,
            }],
            false,
        );

        let result = catalog.add_index(index);
        assert!(matches!(result, Err(CatalogError::ColumnNotFound { .. })));
    }

    #[test]
    fn test_drop_index() {
        let mut catalog = create_test_catalog();

        let index = IndexMetadata::new(
            "idx_name".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn {
                column_name: "name".to_string(),
                order: SortOrder::Ascending,
                prefix_length: None,
            }],
            false,
        );

        catalog.add_index(index).unwrap();
        assert!(catalog.drop_index("users", "idx_name").is_ok());
        assert!(catalog.get_index("users", "idx_name").is_none());
    }

    #[test]
    fn test_get_table_indexes() {
        let mut catalog = create_test_catalog();

        let index1 = IndexMetadata::new(
            "idx_name".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn {
                column_name: "name".to_string(),
                order: SortOrder::Ascending,
                prefix_length: None,
            }],
            false,
        );

        let index2 = IndexMetadata::new(
            "idx_email".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn {
                column_name: "email".to_string(),
                order: SortOrder::Ascending,
                prefix_length: None,
            }],
            true,
        );

        catalog.add_index(index1).unwrap();
        catalog.add_index(index2).unwrap();

        let indexes = catalog.get_table_indexes("users");
        assert_eq!(indexes.len(), 2);
    }

    #[test]
    fn test_drop_table_indexes() {
        let mut catalog = create_test_catalog();

        let index1 = IndexMetadata::new(
            "idx_name".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn {
                column_name: "name".to_string(),
                order: SortOrder::Ascending,
                prefix_length: None,
            }],
            false,
        );

        let index2 = IndexMetadata::new(
            "idx_email".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn {
                column_name: "email".to_string(),
                order: SortOrder::Ascending,
                prefix_length: None,
            }],
            true,
        );

        catalog.add_index(index1).unwrap();
        catalog.add_index(index2).unwrap();

        let dropped = catalog.drop_table_indexes("users");
        assert_eq!(dropped.len(), 2);
        assert!(catalog.get_table_indexes("users").is_empty());
    }
}
