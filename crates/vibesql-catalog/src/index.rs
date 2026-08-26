//! Index metadata definitions for catalog management
//!
//! This module provides the structures for tracking index metadata
//! in the database catalog, independent of the physical index storage.

/// Index metadata stored in the catalog
#[derive(Debug, Clone, PartialEq)]
pub struct IndexMetadata {
    /// Name of the index
    pub name: String,
    /// Name of the schema that owns this index (e.g. `main` or a session temp
    /// schema like `temp_123`).
    ///
    /// SQLite-compatibility: an index lives in the same schema as the table it
    /// indexes. A temp-table index belongs to the session temp schema, where it
    /// is listed in `sqlite_temp_master` (not `sqlite_master`) and is dropped
    /// when the temp table is dropped or the connection closes. Tagging the
    /// owning schema lets a temp index and a main index share a name without
    /// colliding. See issue #5513.
    pub schema: String,
    /// Name of the table this index belongs to
    pub table_name: String,
    /// Type of index
    pub index_type: IndexType,
    /// Columns included in the index
    pub columns: Vec<IndexedColumn>,
    /// Whether this index enforces uniqueness
    pub is_unique: bool,
    /// Optional WHERE clause for partial indexes (CREATE INDEX ... WHERE expr).
    ///
    /// When present, the index is "partial" — only rows for which the
    /// predicate evaluates to TRUE are stored in the index. Partial indexes
    /// are recognized by the catalog but the planner conservatively excludes
    /// them from query-execution selection (see
    /// `vibesql-executor::select::scan::index_scan::selection`). The FK
    /// mismatch checker also rejects partial UNIQUE indexes as FK targets to
    /// match SQLite's behaviour (`sqlite3FkLocateIndex`).
    pub where_clause: Option<Box<vibesql_ast::Expression>>,
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
        /// Explicit `COLLATE <name>` on this index-column, if the `CREATE
        /// INDEX` statement specified one (e.g. `CREATE INDEX i ON
        /// t(d COLLATE nocase)`). `None` means the index column uses its
        /// underlying table column's declared collation (defaulting further
        /// to `BINARY`). Reported verbatim (case-preserving, matching
        /// SQLite) by `PRAGMA index_xinfo`'s `coll` column — see issue #6175.
        collation: Option<String>,
        /// Whether the column name was written as a delimited identifier
        /// (double-quoted/backtick/bracket) in the original `CREATE INDEX`
        /// statement that defined this index, e.g. `CREATE INDEX i3 ON
        /// t1("w")`. Carried over from `vibesql_ast::IndexColumn` at index
        /// creation and persisted with the index's catalog metadata so a
        /// later `ALTER TABLE ... DROP COLUMN` dependent-index check can key
        /// its "should this be a string literal in single-quotes?" hint on
        /// how the index was *originally* defined — not on anything about the
        /// column being dropped (issue #6560).
        is_quoted: bool,
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
        IndexedColumn::Column {
            column_name,
            order,
            prefix_length: None,
            collation: None,
            is_quoted: false,
        }
    }

    /// Create a new column index with prefix length
    pub fn new_column_with_prefix(
        column_name: String,
        order: SortOrder,
        prefix_length: u64,
    ) -> Self {
        IndexedColumn::Column {
            column_name,
            order,
            prefix_length: Some(prefix_length),
            collation: None,
            is_quoted: false,
        }
    }

    /// Attach an explicit `COLLATE <name>` to this index column (builder-style
    /// chaining). A no-op on an `Expression` column. See issue #6175.
    pub fn with_collation(mut self, collation: Option<String>) -> Self {
        if let IndexedColumn::Column { collation: c, .. } = &mut self {
            *c = collation;
        }
        self
    }

    /// Attach the original-source quoting bit (builder-style chaining). A
    /// no-op on an `Expression` column. See issue #6560.
    pub fn with_quoted(mut self, is_quoted: bool) -> Self {
        if let IndexedColumn::Column { is_quoted: q, .. } = &mut self {
            *q = is_quoted;
        }
        self
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

    /// Get the explicit `COLLATE` name, if this index column declared one.
    /// `None` for an `Expression` column, or a `Column` with no explicit
    /// `COLLATE` clause (see issue #6175).
    pub fn explicit_collation(&self) -> Option<&str> {
        match self {
            IndexedColumn::Column { collation, .. } => collation.as_deref(),
            IndexedColumn::Expression { .. } => None,
        }
    }

    /// Whether this column was written as a delimited identifier
    /// (double-quoted/backtick/bracket) in the `CREATE INDEX` statement that
    /// defined this index. Always `false` for an `Expression` column. See
    /// issue #6560.
    pub fn is_quoted(&self) -> bool {
        match self {
            IndexedColumn::Column { is_quoted, .. } => *is_quoted,
            IndexedColumn::Expression { .. } => false,
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
    /// Create a new index metadata entry.
    ///
    /// The owning schema defaults to [`crate::DEFAULT_SCHEMA`] (`main`). Use
    /// [`IndexMetadata::with_schema`] to tag a temp-schema index. Defaulting to
    /// `main` keeps the dozens of existing call sites (and persisted/recovered
    /// indexes, which all live in `main`) behaving exactly as before.
    pub fn new(
        name: String,
        table_name: String,
        index_type: IndexType,
        columns: Vec<IndexedColumn>,
        is_unique: bool,
    ) -> Self {
        Self {
            name,
            schema: crate::DEFAULT_SCHEMA.to_string(),
            table_name,
            index_type,
            columns,
            is_unique,
            where_clause: None,
        }
    }

    /// Set the owning schema for this index (builder-style chaining).
    ///
    /// Used by CREATE INDEX once the target table's schema has been resolved
    /// (temp shadows main per SQLite name resolution). See issue #5513.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    /// Returns the owning schema name (e.g. `main` or `temp_123`).
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Attach a partial-index predicate (CREATE INDEX ... WHERE expr).
    ///
    /// Returns `self` for builder-style chaining. Pass `None` to clear an
    /// existing predicate (the resulting index will be treated as full).
    pub fn with_where_clause(mut self, where_clause: Option<vibesql_ast::Expression>) -> Self {
        self.where_clause = where_clause.map(Box::new);
        self
    }

    /// Returns `true` when this index is partial (has a WHERE clause).
    pub fn is_partial(&self) -> bool {
        self.where_clause.is_some()
    }

    /// Get the catalog key for this index: `schema.table.index`.
    ///
    /// Including the schema lets a temp-table index and a main-table index
    /// share a name (and even a table name, when a temp table shadows a main
    /// table) without colliding in the catalog's index registry. See #5513.
    pub fn qualified_name(&self) -> String {
        format!("{}.{}.{}", self.schema, self.table_name, self.name)
    }

    /// Get the schema-less `table.index` form (no owning schema prefix).
    ///
    /// Useful for display / SHOW output that should not surface the internal
    /// session temp-schema name.
    pub fn table_qualified_name(&self) -> String {
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

        // Defaults to the `main` schema; key is schema.table.index.
        assert_eq!(index.qualified_name(), "main.users.idx_name");
        assert_eq!(index.table_qualified_name(), "users.idx_name");
        assert_eq!(index.schema(), "main");

        // A temp-schema index keys under its temp schema, so it never collides
        // with a same-named main index on a same-named table.
        let temp_index = IndexMetadata::new(
            "idx_name".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("name".to_string(), SortOrder::Ascending)],
            false,
        )
        .with_schema("temp_7");
        assert_eq!(temp_index.qualified_name(), "temp_7.users.idx_name");
        assert_ne!(temp_index.qualified_name(), index.qualified_name());
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
    fn test_is_partial_default_false() {
        let index = IndexMetadata::new(
            "idx_users_email".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("email".to_string(), SortOrder::Ascending)],
            true,
        );
        assert!(!index.is_partial());
        assert!(index.where_clause.is_none());
    }

    #[test]
    fn test_is_partial_when_where_clause_set() {
        // Use a literal expression as the predicate stand-in.
        let predicate = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1));
        let index = IndexMetadata::new(
            "idx_users_active".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("email".to_string(), SortOrder::Ascending)],
            true,
        )
        .with_where_clause(Some(predicate.clone()));
        assert!(index.is_partial());
        assert_eq!(index.where_clause.as_deref(), Some(&predicate));
    }

    #[test]
    fn test_with_where_clause_none_clears_predicate() {
        let predicate = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1));
        let index = IndexMetadata::new(
            "idx_users_active".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("email".to_string(), SortOrder::Ascending)],
            true,
        )
        .with_where_clause(Some(predicate))
        .with_where_clause(None);
        assert!(!index.is_partial());
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
