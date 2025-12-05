//! Index metadata definitions for catalog management
//!
//! This module provides the structures for tracking index metadata
//! in the database catalog, independent of the physical index storage.

/// Index metadata stored in the catalog
#[derive(Debug, Clone, PartialEq)]
pub struct IndexMetadata {
    /// Name of the index
    pub name: String,
    /// Name of the table this index belongs to
    pub table_name: String,
    /// Type of index
    pub index_type: IndexType,
    /// Columns included in the index
    pub columns: Vec<IndexedColumn>,
    /// Whether this index enforces uniqueness
    pub is_unique: bool,
}

/// Type of physical index structure
#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    /// B-tree index for general-purpose indexing
    BTree,
    /// Hash index for equality lookups (PRIMARY KEY, UNIQUE)
    Hash,
    /// R-tree spatial index for geometric data
    RTree,
    /// Full-text index for text search
    Fulltext,
    /// IVFFlat index for approximate nearest neighbor search on vectors
    IVFFlat {
        /// Distance metric used for similarity calculations
        metric: VectorDistanceMetric,
        /// Number of clusters/lists
        lists: u32,
    },
    /// HNSW index for high-performance approximate nearest neighbor search
    Hnsw {
        /// Distance metric used for similarity calculations
        metric: VectorDistanceMetric,
        /// Maximum number of connections per node
        m: u32,
        /// Size of dynamic candidate list during construction
        ef_construction: u32,
    },
}

/// Distance metric for vector index operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorDistanceMetric {
    /// Euclidean distance (L2 norm)
    L2,
    /// Cosine similarity
    Cosine,
    /// Inner product (dot product)
    InnerProduct,
}

/// Column specification within an index
#[derive(Debug, Clone, PartialEq)]
pub struct IndexedColumn {
    /// Column name
    pub column_name: String,
    /// Sort order for ordered indexes (B-tree, R-tree)
    pub order: SortOrder,
    /// Optional prefix length for indexed columns (MySQL/SQLite feature)
    /// When present, only the first N characters/bytes of the column value are indexed
    /// Example: UNIQUE (email(50)) indexes only first 50 characters
    pub prefix_length: Option<u64>,
}

/// Sort order for indexed columns
#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Ascending,
    Descending,
}

impl IndexMetadata {
    /// Create a new index metadata entry
    pub fn new(
        name: String,
        table_name: String,
        index_type: IndexType,
        columns: Vec<IndexedColumn>,
        is_unique: bool,
    ) -> Self {
        Self { name, table_name, index_type, columns, is_unique }
    }

    /// Get the fully qualified index name (table.index)
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.table_name, self.name)
    }

    /// Check if this index can be used for the given column
    pub fn can_index_column(&self, column_name: &str) -> bool {
        // For now, check if the column is the first column in the index
        // More sophisticated matching can be added later (composite index prefixes)
        self.columns.first().map(|col| col.column_name == column_name).unwrap_or(false)
    }

    /// Check if this index can be used for the given columns
    pub fn can_index_columns(&self, column_names: &[String]) -> bool {
        if column_names.is_empty() {
            return false;
        }

        // Check if the index columns match the query columns as a prefix
        column_names.len() <= self.columns.len()
            && column_names
                .iter()
                .zip(self.columns.iter())
                .all(|(query_col, index_col)| query_col == &index_col.column_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qualified_name() {
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

        assert_eq!(index.qualified_name(), "users.idx_name");
    }

    #[test]
    fn test_can_index_column() {
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

        assert!(index.can_index_column("name"));
        assert!(!index.can_index_column("email"));
    }

    #[test]
    fn test_can_index_columns_composite() {
        let index = IndexMetadata::new(
            "idx_name_email".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![
                IndexedColumn {
                    column_name: "name".to_string(),
                    order: SortOrder::Ascending,
                    prefix_length: None,
                },
                IndexedColumn {
                    column_name: "email".to_string(),
                    order: SortOrder::Ascending,
                    prefix_length: None,
                },
            ],
            false,
        );

        // Can use index for name prefix
        assert!(index.can_index_columns(&["name".to_string()]));

        // Can use index for name+email
        assert!(index.can_index_columns(&["name".to_string(), "email".to_string()]));

        // Cannot use index for email alone (not a prefix)
        assert!(!index.can_index_columns(&["email".to_string()]));

        // Cannot use index for wrong column
        assert!(!index.can_index_columns(&["age".to_string()]));
    }
}
