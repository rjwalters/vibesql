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

/// Column specification within an index - can be either a simple column or an expression
#[derive(Debug, Clone, PartialEq)]
pub enum IndexedColumn {
    /// Simple column reference with optional prefix length
    Column {
        /// Column name
        column_name: String,
        /// Sort order for ordered indexes (B-tree, R-tree)
        order: SortOrder,
        /// Optional prefix length for indexed columns (MySQL/SQLite feature)
        /// When present, only the first N characters/bytes of the column value are indexed
        /// Example: UNIQUE (email(50)) indexes only first 50 characters
        prefix_length: Option<u64>,
    },
    /// Expression index (functional index)
    /// Example: CREATE INDEX idx ON t(lower(name)) or CREATE INDEX idx ON t(a + b)
    Expression {
        /// The expression AST to evaluate for index keys
        expr: Box<vibesql_ast::Expression>,
        /// Sort order for ordered indexes
        order: SortOrder,
    },
}

impl IndexedColumn {
    /// Create a new simple column index
    pub fn new_column(column_name: String, order: SortOrder) -> Self {
        IndexedColumn::Column { column_name, order, prefix_length: None }
    }

    /// Create a new column index with prefix length
    pub fn new_column_with_prefix(
        column_name: String,
        order: SortOrder,
        prefix_length: u64,
    ) -> Self {
        IndexedColumn::Column { column_name, order, prefix_length: Some(prefix_length) }
    }

    /// Create a new expression index
    pub fn new_expression(expr: vibesql_ast::Expression, order: SortOrder) -> Self {
        IndexedColumn::Expression { expr: Box::new(expr), order }
    }

    /// Get the column name if this is a simple column reference
    pub fn column_name(&self) -> Option<&str> {
        match self {
            IndexedColumn::Column { column_name, .. } => Some(column_name),
            IndexedColumn::Expression { .. } => None,
        }
    }

    /// Get the sort order
    pub fn order(&self) -> &SortOrder {
        match self {
            IndexedColumn::Column { order, .. } => order,
            IndexedColumn::Expression { order, .. } => order,
        }
    }

    /// Get the prefix length if this is a column with prefix
    pub fn prefix_length(&self) -> Option<u64> {
        match self {
            IndexedColumn::Column { prefix_length, .. } => *prefix_length,
            IndexedColumn::Expression { .. } => None,
        }
    }

    /// Check if this is an expression index
    pub fn is_expression(&self) -> bool {
        matches!(self, IndexedColumn::Expression { .. })
    }

    /// Get the expression if this is an expression index
    pub fn get_expression(&self) -> Option<&vibesql_ast::Expression> {
        match self {
            IndexedColumn::Expression { expr, .. } => Some(expr),
            IndexedColumn::Column { .. } => None,
        }
    }
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
        // Expression indexes cannot be used for simple column lookups
        self.columns
            .first()
            .and_then(|col| col.column_name())
            .map(|name| name == column_name)
            .unwrap_or(false)
    }

    /// Check if this index can be used for the given columns
    pub fn can_index_columns(&self, column_names: &[String]) -> bool {
        if column_names.is_empty() {
            return false;
        }

        // Check if the index columns match the query columns as a prefix
        // Expression indexes cannot be matched by column name alone
        column_names.len() <= self.columns.len()
            && column_names.iter().zip(self.columns.iter()).all(|(query_col, index_col)| {
                index_col.column_name().map(|name| name == query_col).unwrap_or(false)
            })
    }

    /// Check if this index contains any expression columns
    pub fn has_expression_columns(&self) -> bool {
        self.columns.iter().any(|col| col.is_expression())
    }

    /// Check if this index contains only expression columns
    pub fn is_expression_index(&self) -> bool {
        !self.columns.is_empty() && self.columns.iter().all(|col| col.is_expression())
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
            vec![IndexedColumn::new_column("name".to_string(), SortOrder::Ascending)],
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
            vec![IndexedColumn::new_column("name".to_string(), SortOrder::Ascending)],
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
                IndexedColumn::new_column("name".to_string(), SortOrder::Ascending),
                IndexedColumn::new_column("email".to_string(), SortOrder::Ascending),
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

    #[test]
    fn test_indexed_column_helpers() {
        // Test column index
        let col = IndexedColumn::new_column("name".to_string(), SortOrder::Ascending);
        assert_eq!(col.column_name(), Some("name"));
        assert_eq!(*col.order(), SortOrder::Ascending);
        assert!(!col.is_expression());
        assert!(col.get_expression().is_none());
        assert!(col.prefix_length().is_none());

        // Test column with prefix
        let col_prefix =
            IndexedColumn::new_column_with_prefix("email".to_string(), SortOrder::Descending, 50);
        assert_eq!(col_prefix.column_name(), Some("email"));
        assert_eq!(*col_prefix.order(), SortOrder::Descending);
        assert_eq!(col_prefix.prefix_length(), Some(50));
        assert!(!col_prefix.is_expression());

        // Test expression index - use a literal expression
        let expr = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(42));
        let expr_col = IndexedColumn::new_expression(expr.clone(), SortOrder::Ascending);
        assert!(expr_col.column_name().is_none());
        assert_eq!(*expr_col.order(), SortOrder::Ascending);
        assert!(expr_col.is_expression());
        assert!(expr_col.get_expression().is_some());
        assert!(expr_col.prefix_length().is_none());
    }

    #[test]
    fn test_expression_index_detection() {
        // Pure column index
        let column_index = IndexMetadata::new(
            "idx_col".to_string(),
            "test".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("a".to_string(), SortOrder::Ascending)],
            false,
        );
        assert!(!column_index.has_expression_columns());
        assert!(!column_index.is_expression_index());

        // Pure expression index - use a literal expression
        let expr = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1));
        let expr_index = IndexMetadata::new(
            "idx_expr".to_string(),
            "test".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_expression(expr.clone(), SortOrder::Ascending)],
            false,
        );
        assert!(expr_index.has_expression_columns());
        assert!(expr_index.is_expression_index());

        // Mixed index (column + expression)
        let mixed_index = IndexMetadata::new(
            "idx_mixed".to_string(),
            "test".to_string(),
            IndexType::BTree,
            vec![
                IndexedColumn::new_column("a".to_string(), SortOrder::Ascending),
                IndexedColumn::new_expression(expr, SortOrder::Ascending),
            ],
            false,
        );
        assert!(mixed_index.has_expression_columns());
        assert!(!mixed_index.is_expression_index()); // Not purely expression

        // Empty index
        let empty_index = IndexMetadata::new(
            "idx_empty".to_string(),
            "test".to_string(),
            IndexType::BTree,
            vec![],
            false,
        );
        assert!(!empty_index.has_expression_columns());
        assert!(!empty_index.is_expression_index());
    }

    #[test]
    fn test_expression_index_cannot_match_columns() {
        // Use a literal expression
        let expr = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1));
        let expr_index = IndexMetadata::new(
            "idx_expr".to_string(),
            "test".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_expression(expr, SortOrder::Ascending)],
            false,
        );

        // Expression index cannot be used for simple column lookups
        assert!(!expr_index.can_index_column("x"));
        assert!(!expr_index.can_index_columns(&["x".to_string()]));
    }
}
