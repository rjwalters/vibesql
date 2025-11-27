// ============================================================================
// Database Index Operations
// ============================================================================

use super::core::Database;
use super::operations::SpatialIndexMetadata;
use crate::{Row, StorageError};
use vibesql_ast::IndexColumn;

impl Database {
    // ============================================================================
    // Index Management
    // ============================================================================

    /// Create an index
    pub fn create_index(
        &mut self,
        index_name: String,
        table_name: String,
        unique: bool,
        columns: Vec<IndexColumn>,
    ) -> Result<(), StorageError> {
        self.operations.create_index(
            &self.catalog,
            &self.tables,
            index_name,
            table_name,
            unique,
            columns,
        )
    }

    /// Check if an index exists
    pub fn index_exists(&self, index_name: &str) -> bool {
        self.operations.index_exists(index_name)
    }

    /// Get index metadata
    pub fn get_index(&self, index_name: &str) -> Option<&super::indexes::IndexMetadata> {
        self.operations.get_index(index_name)
    }

    /// Get index data
    pub fn get_index_data(&self, index_name: &str) -> Option<&super::indexes::IndexData> {
        self.operations.get_index_data(index_name)
    }

    /// Update user-defined indexes for update operation
    pub fn update_indexes_for_update(
        &mut self,
        table_name: &str,
        old_row: &Row,
        new_row: &Row,
        row_index: usize,
    ) {
        self.operations.update_indexes_for_update(
            &self.catalog,
            table_name,
            old_row,
            new_row,
            row_index,
        );
    }

    /// Update user-defined indexes for delete operation
    pub fn update_indexes_for_delete(&mut self, table_name: &str, row: &Row, row_index: usize) {
        self.operations
            .update_indexes_for_delete(&self.catalog, table_name, row, row_index);
    }

    /// Rebuild user-defined indexes after bulk operations that change row indices
    pub fn rebuild_indexes(&mut self, table_name: &str) {
        self.operations
            .rebuild_indexes(&self.catalog, &self.tables, table_name);
    }

    /// Drop an index
    pub fn drop_index(&mut self, index_name: &str) -> Result<(), StorageError> {
        self.operations.drop_index(index_name)
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<String> {
        self.operations.list_indexes()
    }

    /// List all indexes for a specific table
    pub fn list_indexes_for_table(&self, table_name: &str) -> Vec<String> {
        self.operations.list_indexes_for_table(table_name)
    }

    // ============================================================================
    // Spatial Index Methods
    // ============================================================================

    /// Create a spatial index
    pub fn create_spatial_index(
        &mut self,
        metadata: SpatialIndexMetadata,
        spatial_index: crate::index::SpatialIndex,
    ) -> Result<(), StorageError> {
        self.operations.create_spatial_index(metadata, spatial_index)
    }

    /// Check if a spatial index exists
    pub fn spatial_index_exists(&self, index_name: &str) -> bool {
        self.operations.spatial_index_exists(index_name)
    }

    /// Get spatial index metadata
    pub fn get_spatial_index_metadata(&self, index_name: &str) -> Option<&SpatialIndexMetadata> {
        self.operations.get_spatial_index_metadata(index_name)
    }

    /// Get spatial index (immutable)
    pub fn get_spatial_index(&self, index_name: &str) -> Option<&crate::index::SpatialIndex> {
        self.operations.get_spatial_index(index_name)
    }

    /// Get spatial index (mutable)
    pub fn get_spatial_index_mut(&mut self, index_name: &str) -> Option<&mut crate::index::SpatialIndex> {
        self.operations.get_spatial_index_mut(index_name)
    }

    /// Get all spatial indexes for a specific table
    pub fn get_spatial_indexes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<(&SpatialIndexMetadata, &crate::index::SpatialIndex)> {
        self.operations.get_spatial_indexes_for_table(table_name)
    }

    /// Get all spatial indexes for a specific table (mutable)
    pub fn get_spatial_indexes_for_table_mut(
        &mut self,
        table_name: &str,
    ) -> Vec<(&SpatialIndexMetadata, &mut crate::index::SpatialIndex)> {
        self.operations.get_spatial_indexes_for_table_mut(table_name)
    }

    /// Drop a spatial index
    pub fn drop_spatial_index(&mut self, index_name: &str) -> Result<(), StorageError> {
        self.operations.drop_spatial_index(index_name)
    }

    /// Drop all spatial indexes associated with a table (CASCADE behavior)
    pub fn drop_spatial_indexes_for_table(&mut self, table_name: &str) -> Vec<String> {
        self.operations.drop_spatial_indexes_for_table(table_name)
    }

    /// List all spatial indexes
    pub fn list_spatial_indexes(&self) -> Vec<String> {
        self.operations.list_spatial_indexes()
    }
}
