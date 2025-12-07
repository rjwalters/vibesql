//! CREATE INDEX statement execution

use vibesql_ast::CreateIndexStmt;
use vibesql_storage::{
    index::{extract_mbr_from_sql_value, SpatialIndex, SpatialIndexEntry},
    Database, SpatialIndexMetadata,
};

use crate::{errors::ExecutorError, privilege_checker::PrivilegeChecker};

/// Executor for CREATE INDEX statements
pub struct CreateIndexExecutor;

impl CreateIndexExecutor {
    /// Execute a CREATE INDEX statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The CREATE INDEX statement AST node
    /// * `database` - The database to create the index in
    ///
    /// # Returns
    ///
    /// Success message or error
    ///
    /// # Examples
    ///
    /// ```
    /// use vibesql_ast::{CreateIndexStmt, IndexColumn, OrderDirection};
    /// use vibesql_executor::CreateIndexExecutor;
    /// use vibesql_storage::Database;
    ///
    /// let mut db = Database::new();
    /// // First create a table
    /// // ... (table creation code) ...
    ///
    /// let stmt = CreateIndexStmt {
    ///     index_name: "idx_users_email".to_string(),
    ///     if_not_exists: false,
    ///     table_name: "users".to_string(),
    ///     index_type: vibesql_ast::IndexType::BTree { unique: false },
    ///     columns: vec![IndexColumn {
    ///         column_name: "email".to_string(),
    ///         direction: OrderDirection::Asc,
    ///         prefix_length: None,
    ///     }],
    /// };
    ///
    /// let result = CreateIndexExecutor::execute(&stmt, &mut db);
    /// // assert!(result.is_ok());
    /// ```
    pub fn execute(
        stmt: &CreateIndexStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        // Parse qualified table name (schema.table or just table)
        let (schema_name, table_name) =
            if let Some((schema_part, table_part)) = stmt.table_name.split_once('.') {
                (schema_part.to_string(), table_part.to_string())
            } else {
                (database.catalog.get_current_schema().to_string(), stmt.table_name.clone())
            };

        // Check CREATE privilege on the schema
        PrivilegeChecker::check_create(database, &schema_name)?;

        // Build fully qualified table name for catalog lookups
        let qualified_table_name = format!("{}.{}", schema_name, table_name);

        // Check if table exists
        if !database.catalog.table_exists(&qualified_table_name) {
            return Err(ExecutorError::TableNotFound(qualified_table_name.clone()));
        }

        // Get table schema to validate columns
        let table_schema = database
            .catalog
            .get_table(&qualified_table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(qualified_table_name.clone()))?;

        // Validate that all indexed columns exist in the table
        for index_col in &stmt.columns {
            if table_schema.get_column(&index_col.column_name).is_none() {
                let available_columns =
                    table_schema.columns.iter().map(|c| c.name.clone()).collect();
                return Err(ExecutorError::ColumnNotFound {
                    column_name: index_col.column_name.clone(),
                    table_name: qualified_table_name.clone(),
                    searched_tables: vec![qualified_table_name.clone()],
                    available_columns,
                });
            }
        }

        // Validate prefix length specifications
        for index_col in &stmt.columns {
            if let Some(prefix_len) = index_col.prefix_length {
                // Prefix length must be positive
                if prefix_len == 0 {
                    return Err(ExecutorError::InvalidIndexDefinition(format!(
                        "Prefix length must be greater than 0 for column '{}'",
                        index_col.column_name
                    )));
                }

                // Prefix length should only be used with string columns
                let column = table_schema.get_column(&index_col.column_name).unwrap(); // Safe: already validated above
                match column.data_type {
                    vibesql_types::DataType::Varchar { .. }
                    | vibesql_types::DataType::Character { .. } => {
                        // Valid string types for prefix indexing
                    }
                    _ => {
                        return Err(ExecutorError::InvalidIndexDefinition(
                            format!(
                                "Prefix length can only be specified for string columns, but column '{}' has type {:?}",
                                index_col.column_name, column.data_type
                            ),
                        ));
                    }
                }

                // Reasonable upper limit check (64KB = 65536 characters)
                // This prevents accidental extremely large prefix specifications
                const MAX_PREFIX_LENGTH: u64 = 65536;
                if prefix_len > MAX_PREFIX_LENGTH {
                    return Err(ExecutorError::InvalidIndexDefinition(format!(
                        "Prefix length {} is too large for column '{}' (maximum: {})",
                        prefix_len, index_col.column_name, MAX_PREFIX_LENGTH
                    )));
                }
            }
        }

        // Check if index already exists (either B-tree or spatial)
        let index_name = &stmt.index_name;
        let index_exists =
            database.index_exists(index_name) || database.spatial_index_exists(index_name);

        if index_exists {
            if stmt.if_not_exists {
                // IF NOT EXISTS: silently succeed if index already exists
                return Ok(format!("Index '{}' already exists (skipped)", index_name));
            } else {
                return Err(ExecutorError::IndexAlreadyExists(index_name.clone()));
            }
        }

        // Create the index based on type
        match &stmt.index_type {
            vibesql_ast::IndexType::BTree { unique } => {
                // Compute column indices early (before mutable borrows)
                let column_indices: Vec<u32> = stmt
                    .columns
                    .iter()
                    .filter_map(|col| table_schema.get_column_index(&col.column_name))
                    .map(|idx| idx as u32)
                    .collect();

                // Add to catalog first (use unqualified table name as stored in catalog)
                let index_metadata = vibesql_catalog::IndexMetadata::new(
                    index_name.clone(),
                    table_name.clone(),
                    vibesql_catalog::IndexType::BTree,
                    stmt.columns
                        .iter()
                        .map(|col| vibesql_catalog::IndexedColumn {
                            column_name: col.column_name.clone(),
                            order: match col.direction {
                                vibesql_ast::OrderDirection::Asc => {
                                    vibesql_catalog::SortOrder::Ascending
                                }
                                vibesql_ast::OrderDirection::Desc => {
                                    vibesql_catalog::SortOrder::Descending
                                }
                            },
                            prefix_length: col.prefix_length,
                        })
                        .collect(),
                    *unique,
                );
                database.catalog.add_index(index_metadata)?;

                // B-tree index (use unqualified name for storage, database handles qualification internally)
                database.create_index(
                    index_name.clone(),
                    table_name.clone(),
                    *unique,
                    stmt.columns.clone(),
                )?;

                // Emit WAL entry for persistence
                database.emit_wal_create_index(
                    index_name_to_id(index_name),
                    index_name,
                    &qualified_table_name,
                    column_indices,
                    *unique,
                );

                Ok(format!(
                    "Index '{}' created successfully on table '{}'",
                    index_name, qualified_table_name
                ))
            }
            vibesql_ast::IndexType::Fulltext => Err(ExecutorError::UnsupportedFeature(
                "FULLTEXT indexes are not yet implemented".to_string(),
            )),
            vibesql_ast::IndexType::Spatial => {
                // Spatial index validation: must be exactly 1 column
                if stmt.columns.len() != 1 {
                    return Err(ExecutorError::InvalidIndexDefinition(
                        "SPATIAL indexes must be defined on exactly one column".to_string(),
                    ));
                }

                let column_name = &stmt.columns[0].column_name;

                // Get the column index
                let col_idx = table_schema.get_column_index(column_name).ok_or_else(|| {
                    ExecutorError::ColumnNotFound {
                        column_name: column_name.clone(),
                        table_name: qualified_table_name.clone(),
                        searched_tables: vec![qualified_table_name.clone()],
                        available_columns: table_schema
                            .columns
                            .iter()
                            .map(|c| c.name.clone())
                            .collect(),
                    }
                })?;

                // Extract MBRs from all existing rows (use unqualified name, database handles qualification)
                let table = database
                    .get_table(&table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(qualified_table_name.clone()))?;

                let mut entries = Vec::new();
                // Use scan_live() to skip deleted rows and get correct physical indices
                for (row_idx, row) in table.scan_live() {
                    let geom_value = &row.values[col_idx];

                    // Extract MBR from geometry value (skip NULLs and invalid geometries)
                    if let Some(mbr) = extract_mbr_from_sql_value(geom_value) {
                        entries.push(SpatialIndexEntry { row_id: row_idx, mbr });
                    }
                }

                // Build spatial index via bulk_load (more efficient than incremental inserts)
                let spatial_index = SpatialIndex::bulk_load(column_name.clone(), entries);

                // Add to catalog first (use unqualified table name as stored in catalog)
                let index_metadata = vibesql_catalog::IndexMetadata::new(
                    index_name.clone(),
                    table_name.clone(),
                    vibesql_catalog::IndexType::RTree,
                    vec![vibesql_catalog::IndexedColumn {
                        column_name: column_name.clone(),
                        order: vibesql_catalog::SortOrder::Ascending,
                        prefix_length: None, // Spatial indexes don't support prefix indexing
                    }],
                    false,
                );
                database.catalog.add_index(index_metadata)?;

                // Store in database (use unqualified table name for storage metadata)
                let metadata = SpatialIndexMetadata {
                    index_name: index_name.clone(),
                    table_name: table_name.clone(),
                    column_name: column_name.clone(),
                    created_at: Some(chrono::Utc::now()),
                };

                database.create_spatial_index(metadata, spatial_index)?;

                // Emit WAL entry for persistence (spatial indexes are never unique)
                database.emit_wal_create_index(
                    index_name_to_id(index_name),
                    index_name,
                    &qualified_table_name,
                    vec![col_idx as u32],
                    false,
                );

                Ok(format!(
                    "Spatial index '{}' created successfully on table '{}'",
                    index_name, qualified_table_name
                ))
            }
            vibesql_ast::IndexType::IVFFlat { metric, lists } => {
                // IVFFlat index validation: must be exactly 1 column (vector column)
                if stmt.columns.len() != 1 {
                    return Err(ExecutorError::InvalidIndexDefinition(
                        "IVFFlat indexes must be defined on exactly one vector column".to_string(),
                    ));
                }

                let column_name = &stmt.columns[0].column_name;

                // Get the column index and validate it's a vector type
                let col_idx = table_schema.get_column_index(column_name).ok_or_else(|| {
                    ExecutorError::ColumnNotFound {
                        column_name: column_name.clone(),
                        table_name: qualified_table_name.clone(),
                        searched_tables: vec![qualified_table_name.clone()],
                        available_columns: table_schema
                            .columns
                            .iter()
                            .map(|c| c.name.clone())
                            .collect(),
                    }
                })?;

                // Validate column type is VECTOR
                let col_type = &table_schema.columns[col_idx].data_type;
                let dimensions = match col_type {
                    vibesql_types::DataType::Vector { dimensions } => *dimensions as usize,
                    _ => {
                        return Err(ExecutorError::InvalidIndexDefinition(format!(
                            "IVFFlat indexes can only be created on VECTOR columns, but '{}' has type {:?}",
                            column_name, col_type
                        )));
                    }
                };

                // Convert AST metric to catalog metric
                let catalog_metric = match metric {
                    vibesql_ast::VectorDistanceMetric::L2 => {
                        vibesql_catalog::VectorDistanceMetric::L2
                    }
                    vibesql_ast::VectorDistanceMetric::Cosine => {
                        vibesql_catalog::VectorDistanceMetric::Cosine
                    }
                    vibesql_ast::VectorDistanceMetric::InnerProduct => {
                        vibesql_catalog::VectorDistanceMetric::InnerProduct
                    }
                };

                // Add to catalog first
                let index_metadata = vibesql_catalog::IndexMetadata::new(
                    index_name.clone(),
                    table_name.clone(),
                    vibesql_catalog::IndexType::IVFFlat { metric: catalog_metric, lists: *lists },
                    vec![vibesql_catalog::IndexedColumn {
                        column_name: column_name.clone(),
                        order: vibesql_catalog::SortOrder::Ascending, // Not meaningful for vector indexes
                        prefix_length: None,
                    }],
                    false, // IVFFlat indexes are never unique
                );
                database.catalog.add_index(index_metadata)?;

                // Create the IVFFlat index in storage
                database.create_ivfflat_index(
                    index_name.clone(),
                    table_name.clone(),
                    column_name.clone(),
                    col_idx,
                    dimensions,
                    *lists as usize,
                    *metric,
                )?;

                // Emit WAL entry for persistence (IVFFlat indexes are never unique)
                database.emit_wal_create_index(
                    index_name_to_id(index_name),
                    index_name,
                    &qualified_table_name,
                    vec![col_idx as u32],
                    false,
                );

                Ok(format!(
                    "IVFFlat index '{}' created successfully on table '{}' column '{}'",
                    index_name, qualified_table_name, column_name
                ))
            }
            vibesql_ast::IndexType::Hnsw { metric, m, ef_construction } => {
                // HNSW index validation: must be exactly 1 column (vector column)
                if stmt.columns.len() != 1 {
                    return Err(ExecutorError::InvalidIndexDefinition(
                        "HNSW indexes must be defined on exactly one vector column".to_string(),
                    ));
                }

                let column_name = &stmt.columns[0].column_name;

                // Get the column index and validate it's a vector type
                let col_idx = table_schema.get_column_index(column_name).ok_or_else(|| {
                    ExecutorError::ColumnNotFound {
                        column_name: column_name.clone(),
                        table_name: qualified_table_name.clone(),
                        searched_tables: vec![qualified_table_name.clone()],
                        available_columns: table_schema
                            .columns
                            .iter()
                            .map(|c| c.name.clone())
                            .collect(),
                    }
                })?;

                // Validate column type is VECTOR
                let col_type = &table_schema.columns[col_idx].data_type;
                let dimensions = match col_type {
                    vibesql_types::DataType::Vector { dimensions } => *dimensions as usize,
                    _ => {
                        return Err(ExecutorError::InvalidIndexDefinition(format!(
                            "HNSW indexes can only be created on VECTOR columns, but '{}' has type {:?}",
                            column_name, col_type
                        )));
                    }
                };

                // Convert AST metric to catalog metric
                let catalog_metric = match metric {
                    vibesql_ast::VectorDistanceMetric::L2 => {
                        vibesql_catalog::VectorDistanceMetric::L2
                    }
                    vibesql_ast::VectorDistanceMetric::Cosine => {
                        vibesql_catalog::VectorDistanceMetric::Cosine
                    }
                    vibesql_ast::VectorDistanceMetric::InnerProduct => {
                        vibesql_catalog::VectorDistanceMetric::InnerProduct
                    }
                };

                // Add to catalog first
                let index_metadata = vibesql_catalog::IndexMetadata::new(
                    index_name.clone(),
                    table_name.clone(),
                    vibesql_catalog::IndexType::Hnsw {
                        metric: catalog_metric,
                        m: *m,
                        ef_construction: *ef_construction,
                    },
                    vec![vibesql_catalog::IndexedColumn {
                        column_name: column_name.clone(),
                        order: vibesql_catalog::SortOrder::Ascending, // Not meaningful for vector indexes
                        prefix_length: None,
                    }],
                    false, // HNSW indexes are never unique
                );
                database.catalog.add_index(index_metadata)?;

                // Create the HNSW index in storage
                database.create_hnsw_index(
                    index_name.clone(),
                    table_name.clone(),
                    column_name.clone(),
                    col_idx,
                    dimensions,
                    *m,
                    *ef_construction,
                    *metric,
                )?;

                // Emit WAL entry for persistence (HNSW indexes are never unique)
                database.emit_wal_create_index(
                    index_name_to_id(index_name),
                    index_name,
                    &qualified_table_name,
                    vec![col_idx as u32],
                    false,
                );

                Ok(format!(
                    "HNSW index '{}' created successfully on table '{}' column '{}'",
                    index_name, qualified_table_name, column_name
                ))
            }
        }
    }
}

/// Compute an index ID from index name using hash (for consistent mapping)
fn index_name_to_id(name: &str) -> u32 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut hasher);
    hasher.finish() as u32
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{ColumnDef, CreateTableStmt, IndexColumn, OrderDirection};
    use vibesql_storage::Row;
    use vibesql_types::{DataType, SqlValue};

    use super::*;
    use crate::CreateTableExecutor;

    fn create_test_table(db: &mut Database) {
        let stmt = CreateTableStmt {
        if_not_exists: false,
            table_name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
                ColumnDef {
                    name: "email".to_string(),
                    data_type: DataType::Varchar { max_length: Some(255) },
                    nullable: false,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Varchar { max_length: Some(100) },
                    nullable: true,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
            ],
            table_constraints: vec![],
            table_options: vec![],
        };

        CreateTableExecutor::execute(&stmt, db).unwrap();
    }

    #[test]
    fn test_create_simple_index() {
        let mut db = Database::new();
        create_test_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_users_email".to_string(),
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            "Index 'idx_users_email' created successfully on table 'public.users'"
        );

        // Verify index exists
        assert!(db.index_exists("idx_users_email"));
    }

    #[test]
    fn test_create_unique_index() {
        let mut db = Database::new();
        create_test_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_users_email_unique".to_string(),
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: true },
            columns: vec![IndexColumn {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
        assert!(db.index_exists("idx_users_email_unique"));
    }

    #[test]
    fn test_create_multi_column_index() {
        let mut db = Database::new();
        create_test_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_users_email_name".to_string(),
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![
                IndexColumn {
                    column_name: "email".to_string(),
                    direction: OrderDirection::Asc,
                    prefix_length: None,
                },
                IndexColumn {
                    column_name: "name".to_string(),
                    direction: OrderDirection::Desc,
                    prefix_length: None,
                },
            ],
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_index_duplicate_name() {
        let mut db = Database::new();
        create_test_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_users_email".to_string(),
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        // First creation succeeds
        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());

        // Second creation fails
        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::IndexAlreadyExists(_))));
    }

    #[test]
    fn test_create_index_on_nonexistent_table() {
        let mut db = Database::new();

        let stmt = CreateIndexStmt {
            index_name: "idx_nonexistent".to_string(),
            if_not_exists: false,
            table_name: "nonexistent_table".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn {
                column_name: "id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::TableNotFound(_))));
    }

    #[test]
    fn test_create_index_on_nonexistent_column() {
        let mut db = Database::new();
        create_test_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_users_nonexistent".to_string(),
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn {
                column_name: "nonexistent_column".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::ColumnNotFound { .. })));
    }

    #[test]
    fn test_create_index_if_not_exists_when_not_exists() {
        let mut db = Database::new();
        create_test_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_users_email".to_string(),
            if_not_exists: true,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            "Index 'idx_users_email' created successfully on table 'public.users'"
        );
        assert!(db.index_exists("idx_users_email"));
    }

    #[test]
    fn test_create_index_if_not_exists_when_exists() {
        let mut db = Database::new();
        create_test_table(&mut db);

        // First creation
        let stmt = CreateIndexStmt {
            index_name: "idx_users_email".to_string(),
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        };
        CreateIndexExecutor::execute(&stmt, &mut db).unwrap();

        // Second creation with IF NOT EXISTS should succeed
        let stmt_with_if_not_exists = CreateIndexStmt {
            index_name: "idx_users_email".to_string(),
            if_not_exists: true,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        };
        let result = CreateIndexExecutor::execute(&stmt_with_if_not_exists, &mut db);
        assert!(result.is_ok());
        assert!(db.index_exists("idx_users_email"));
    }

    #[test]
    fn test_create_index_with_schema_qualified_table() {
        let mut db = Database::new();
        create_test_table(&mut db);

        // Create index using schema-qualified table name (with default public schema)
        let index_stmt = CreateIndexStmt {
            index_name: "idx_users_email_qualified".to_string(),
            if_not_exists: false,
            table_name: "public.users".to_string(), // Explicitly qualify with public schema
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        let result = CreateIndexExecutor::execute(&index_stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            "Index 'idx_users_email_qualified' created successfully on table 'public.users'"
        );

        // Verify index exists
        assert!(db.index_exists("idx_users_email_qualified"));
    }

    #[test]
    fn test_create_index_on_nonexistent_schema_qualified_table() {
        let mut db = Database::new();

        // Create a custom schema
        db.catalog.create_schema("test_schema".to_string()).unwrap();

        // Try to create index on non-existent table
        let index_stmt = CreateIndexStmt {
            index_name: "idx_nonexistent".to_string(),
            if_not_exists: false,
            table_name: "test_schema.nonexistent_table".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn {
                column_name: "id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        let result = CreateIndexExecutor::execute(&index_stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::TableNotFound(_))));
    }

    // ========================================================================
    // IVFFlat Index Tests
    // ========================================================================

    fn create_vector_table(db: &mut Database) {
        let stmt = CreateTableStmt {
        if_not_exists: false,
            table_name: "documents".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
                ColumnDef {
                    name: "embedding".to_string(),
                    data_type: DataType::Vector { dimensions: 3 },
                    nullable: true,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
                ColumnDef {
                    name: "content".to_string(),
                    data_type: DataType::Varchar { max_length: Some(1000) },
                    nullable: true,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
            ],
            table_constraints: vec![],
            table_options: vec![],
        };

        CreateTableExecutor::execute(&stmt, db).unwrap();
    }

    #[test]
    fn test_create_ivfflat_index_l2() {
        let mut db = Database::new();
        create_vector_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_documents_embedding".to_string(),
            if_not_exists: false,
            table_name: "documents".to_string(),
            index_type: vibesql_ast::IndexType::IVFFlat {
                metric: vibesql_ast::VectorDistanceMetric::L2,
                lists: 4,
            },
            columns: vec![IndexColumn {
                column_name: "embedding".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok(), "IVFFlat index creation failed: {:?}", result.err());
        assert!(result
            .unwrap()
            .contains("IVFFlat index 'idx_documents_embedding' created successfully"));
        assert!(db.index_exists("idx_documents_embedding"));
    }

    #[test]
    fn test_create_ivfflat_index_cosine() {
        let mut db = Database::new();
        create_vector_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_documents_cosine".to_string(),
            if_not_exists: false,
            table_name: "documents".to_string(),
            index_type: vibesql_ast::IndexType::IVFFlat {
                metric: vibesql_ast::VectorDistanceMetric::Cosine,
                lists: 4,
            },
            columns: vec![IndexColumn {
                column_name: "embedding".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
        assert!(db.index_exists("idx_documents_cosine"));
    }

    #[test]
    fn test_create_ivfflat_index_inner_product() {
        let mut db = Database::new();
        create_vector_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_documents_ip".to_string(),
            if_not_exists: false,
            table_name: "documents".to_string(),
            index_type: vibesql_ast::IndexType::IVFFlat {
                metric: vibesql_ast::VectorDistanceMetric::InnerProduct,
                lists: 4,
            },
            columns: vec![IndexColumn {
                column_name: "embedding".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
        assert!(db.index_exists("idx_documents_ip"));
    }

    #[test]
    fn test_create_ivfflat_index_on_non_vector_column() {
        let mut db = Database::new();
        create_vector_table(&mut db);

        // Try to create IVFFlat index on a non-vector column
        let stmt = CreateIndexStmt {
            index_name: "idx_documents_content".to_string(),
            if_not_exists: false,
            table_name: "documents".to_string(),
            index_type: vibesql_ast::IndexType::IVFFlat {
                metric: vibesql_ast::VectorDistanceMetric::L2,
                lists: 4,
            },
            columns: vec![IndexColumn {
                column_name: "content".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::InvalidIndexDefinition(_))));
    }

    #[test]
    fn test_create_ivfflat_index_multiple_columns_fails() {
        let mut db = Database::new();
        create_vector_table(&mut db);

        // IVFFlat indexes must be on exactly one column
        let stmt = CreateIndexStmt {
            index_name: "idx_documents_multi".to_string(),
            if_not_exists: false,
            table_name: "documents".to_string(),
            index_type: vibesql_ast::IndexType::IVFFlat {
                metric: vibesql_ast::VectorDistanceMetric::L2,
                lists: 4,
            },
            columns: vec![
                IndexColumn {
                    column_name: "embedding".to_string(),
                    direction: OrderDirection::Asc,
                    prefix_length: None,
                },
                IndexColumn {
                    column_name: "id".to_string(),
                    direction: OrderDirection::Asc,
                    prefix_length: None,
                },
            ],
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::InvalidIndexDefinition(_))));
    }

    #[test]
    fn test_create_ivfflat_index_if_not_exists() {
        let mut db = Database::new();
        create_vector_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_documents_embedding".to_string(),
            if_not_exists: true,
            table_name: "documents".to_string(),
            index_type: vibesql_ast::IndexType::IVFFlat {
                metric: vibesql_ast::VectorDistanceMetric::L2,
                lists: 4,
            },
            columns: vec![IndexColumn {
                column_name: "embedding".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        // First creation
        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());

        // Second creation with IF NOT EXISTS should succeed
        let result2 = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result2.is_ok());
        assert!(result2.unwrap().contains("already exists"));
    }

    #[test]
    fn test_ivfflat_index_search() {
        let mut db = Database::new();
        create_vector_table(&mut db);

        // Insert test vector data
        // Row 0: [1.0, 0.0, 0.0] - should be closest to query [0.9, 0.1, 0.0]
        db.insert_row(
            "documents",
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Vector(vec![1.0, 0.0, 0.0]),
                SqlValue::Varchar(arcstr::ArcStr::from("doc1")),
            ]),
        )
        .unwrap();

        // Row 1: [0.0, 1.0, 0.0]
        db.insert_row(
            "documents",
            Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Vector(vec![0.0, 1.0, 0.0]),
                SqlValue::Varchar(arcstr::ArcStr::from("doc2")),
            ]),
        )
        .unwrap();

        // Row 2: [0.0, 0.0, 1.0]
        db.insert_row(
            "documents",
            Row::new(vec![
                SqlValue::Integer(3),
                SqlValue::Vector(vec![0.0, 0.0, 1.0]),
                SqlValue::Varchar(arcstr::ArcStr::from("doc3")),
            ]),
        )
        .unwrap();

        // Row 3: [0.5, 0.5, 0.0] - second closest to query
        db.insert_row(
            "documents",
            Row::new(vec![
                SqlValue::Integer(4),
                SqlValue::Vector(vec![0.5, 0.5, 0.0]),
                SqlValue::Varchar(arcstr::ArcStr::from("doc4")),
            ]),
        )
        .unwrap();

        // Create IVFFlat index (should build on existing data)
        let stmt = CreateIndexStmt {
            index_name: "idx_embedding".to_string(),
            if_not_exists: false,
            table_name: "documents".to_string(),
            index_type: vibesql_ast::IndexType::IVFFlat {
                metric: vibesql_ast::VectorDistanceMetric::L2,
                lists: 2, // 2 clusters for small test data
            },
            columns: vec![IndexColumn {
                column_name: "embedding".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok(), "Index creation failed: {:?}", result.err());

        // Test search API
        // Query vector near [1.0, 0.0, 0.0]
        let query_vector = vec![0.9, 0.1, 0.0];
        let results = db.search_ivfflat_index("idx_embedding", &query_vector, 2);
        assert!(results.is_ok(), "Search should succeed: {:?}", results.err());

        let neighbors = results.unwrap();
        // Should find at least the nearest vectors
        assert!(!neighbors.is_empty(), "Should find at least one neighbor");

        // The closest vector should be [1.0, 0.0, 0.0] (row 0)
        let (first_row_id, first_distance) = neighbors[0];
        assert!(first_distance >= 0.0, "Distance should be non-negative");
        // Since we inserted [1.0, 0.0, 0.0] at row 0, it should be closest
        assert_eq!(first_row_id, 0, "First result should be the closest vector");
    }

    #[test]
    fn test_ivfflat_get_indexes_for_table() {
        let mut db = Database::new();
        create_vector_table(&mut db);

        // Create IVFFlat index
        let stmt = CreateIndexStmt {
            index_name: "idx_vec".to_string(),
            if_not_exists: false,
            table_name: "documents".to_string(),
            index_type: vibesql_ast::IndexType::IVFFlat {
                metric: vibesql_ast::VectorDistanceMetric::Cosine,
                lists: 2,
            },
            columns: vec![IndexColumn {
                column_name: "embedding".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());

        // Test getting IVFFlat indexes for the table
        let ivfflat_indexes = db.get_ivfflat_indexes_for_table("documents");
        assert_eq!(ivfflat_indexes.len(), 1, "Should have one IVFFlat index");

        let (metadata, index) = &ivfflat_indexes[0];
        assert!(metadata.index_name.to_uppercase().contains("IDX_VEC"));
        assert_eq!(index.metric(), vibesql_ast::VectorDistanceMetric::Cosine);
    }

    #[test]
    fn test_ivfflat_set_probes() {
        let mut db = Database::new();
        create_vector_table(&mut db);

        // Create IVFFlat index
        let stmt = CreateIndexStmt {
            index_name: "idx_probes".to_string(),
            if_not_exists: false,
            table_name: "documents".to_string(),
            index_type: vibesql_ast::IndexType::IVFFlat {
                metric: vibesql_ast::VectorDistanceMetric::L2,
                lists: 4,
            },
            columns: vec![IndexColumn {
                column_name: "embedding".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            }],
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());

        // Set probes to search more clusters (improves recall at cost of speed)
        let set_probes_result = db.set_ivfflat_probes("idx_probes", 3);
        assert!(set_probes_result.is_ok());

        // Verify the index can still be searched
        let query_vector = vec![0.5, 0.5, 0.5];
        let search_result = db.search_ivfflat_index("idx_probes", &query_vector, 3);
        assert!(search_result.is_ok());
    }
}
