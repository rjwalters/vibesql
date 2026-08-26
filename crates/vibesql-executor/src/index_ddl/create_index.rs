//! CREATE INDEX statement execution
//!
//! This module provides the main executor for CREATE INDEX statements,
//! dispatching to specialized modules based on index type:
//!
//! - [`btree_index`] - B-tree indexes (standard and unique)
//! - [`spatial_index`] - R-tree spatial indexes
//! - [`vector_index`] - IVFFlat and HNSW vector indexes
//! - [`expression_index`] - Expression-based functional indexes
//! - [`validation`] - Pre-creation validation

use vibesql_ast::CreateIndexStmt;
use vibesql_storage::Database;

use super::{
    btree_index::create_btree_index,
    spatial_index::create_spatial_index,
    validation::{index_already_exists, validate_create_index},
    vector_index::{create_hnsw_index, create_ivfflat_index},
};
use crate::errors::ExecutorError;

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
    ///     schema: None,
    ///     if_not_exists: false,
    ///     table_name: "users".to_string(),
    ///     index_type: vibesql_ast::IndexType::BTree { unique: false },
    ///     columns: vec![IndexColumn::Column {
    ///         column_name: "email".to_string(),
    ///         direction: OrderDirection::Asc,
    ///         prefix_length: None,
    ///         collation: None,
    ///         is_quoted: false,
    ///     }],
    ///     where_clause: None,
    /// };
    ///
    /// let result = CreateIndexExecutor::execute(&stmt, &mut db);
    /// // assert!(result.is_ok());
    /// ```
    pub fn execute(
        stmt: &CreateIndexStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        // Handle IF NOT EXISTS early (before validation which also checks this)
        if stmt.if_not_exists && index_already_exists(stmt, database) {
            return Ok(format!("Index '{}' already exists (skipped)", stmt.index_name));
        }

        // Validate the CREATE INDEX statement
        let validation = validate_create_index(stmt, database)?;

        // Partial indexes (CREATE INDEX … WHERE …): the predicate has already
        // been validated for semantic legality (e.g. no window functions). The
        // catalog records the predicate via `IndexMetadata::where_clause` so
        // the FK-mismatch checker and the index-selection planner can
        // recognise and skip partial indexes. The B-tree index path
        // (`create_btree_index`) now also evaluates the predicate against
        // every existing row at build time so the initial index body only
        // contains matching rows. Subsequent INSERT/UPDATE/DELETE maintenance
        // is handled by `partial_index_maintenance` on each DML path. See
        // issue #5214.

        // Dispatch to appropriate index creation based on type
        match &stmt.index_type {
            vibesql_ast::IndexType::BTree { unique } => create_btree_index(
                database,
                stmt,
                &validation.table_name,
                &validation.qualified_table_name,
                &validation.table_schema,
                *unique,
            ),

            vibesql_ast::IndexType::Fulltext => Err(ExecutorError::UnsupportedFeature(
                "FULLTEXT indexes are not yet implemented".to_string(),
            )),

            vibesql_ast::IndexType::Spatial => create_spatial_index(
                database,
                stmt,
                &validation.table_name,
                &validation.qualified_table_name,
                &validation.table_schema,
            ),

            vibesql_ast::IndexType::IVFFlat { metric, lists } => create_ivfflat_index(
                database,
                stmt,
                &validation.table_name,
                &validation.qualified_table_name,
                &validation.table_schema,
                *metric,
                *lists,
            ),

            vibesql_ast::IndexType::Hnsw { metric, m, ef_construction } => create_hnsw_index(
                database,
                stmt,
                &validation.table_name,
                &validation.qualified_table_name,
                &validation.table_schema,
                *metric,
                *m,
                *ef_construction,
            ),
        }
    }
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
            temporary: false,
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
                    generated_expr: None,
                    is_exact_integer_type: false,
                    type_source: None,
                },
                ColumnDef {
                    name: "email".to_string(),
                    data_type: DataType::Varchar { max_length: Some(255) },
                    nullable: false,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                    generated_expr: None,
                    is_exact_integer_type: false,
                    type_source: None,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Varchar { max_length: Some(100) },
                    nullable: true,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                    generated_expr: None,
                    is_exact_integer_type: false,
                    type_source: None,
                },
            ],
            table_constraints: vec![],
            table_options: vec![],
            quoted: false,
            name_source: None,
            as_query: None,
            without_rowid: false,
            strict: false,
        };

        CreateTableExecutor::execute(&stmt, db).unwrap();
    }

    #[test]
    fn test_create_simple_index() {
        let mut db = Database::new();
        create_test_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_users_email".to_string(),
            schema: None,
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::Column {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            "Index 'idx_users_email' created successfully on table 'main.users'"
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
            schema: None,
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: true },
            columns: vec![IndexColumn::Column {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
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
            schema: None,
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![
                IndexColumn::Column {
                    column_name: "email".to_string(),
                    direction: OrderDirection::Asc,
                    prefix_length: None,
                    collation: None,
                    is_quoted: false,
                },
                IndexColumn::Column {
                    column_name: "name".to_string(),
                    direction: OrderDirection::Desc,
                    prefix_length: None,
                    collation: None,
                    is_quoted: false,
                },
            ],
            where_clause: None,
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
            schema: None,
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::Column {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
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
            schema: None,
            if_not_exists: false,
            table_name: "nonexistent_table".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::Column {
                column_name: "id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
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
            schema: None,
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::Column {
                column_name: "nonexistent_column".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        // SQLite-compatible "no such column: <name>" (issue #6560) — not the
        // generic ColumnNotFound wording. `is_quoted: false` above means no
        // "should this be a string literal in single-quotes?" hint.
        match result {
            Err(ExecutorError::NoSuchColumn { column_ref }) => {
                assert_eq!(column_ref, "nonexistent_column");
            }
            other => panic!("expected NoSuchColumn, got {other:?}"),
        }
    }

    #[test]
    fn test_create_index_on_nonexistent_quoted_column_gets_sqlite_hint() {
        // quote.test 2.1.3: `CREATE INDEX i3 ON t1("w")` where `w` names no
        // column -> `is_quoted: true` (set at parse time for a genuinely
        // delimited identifier) must earn SQLite's "should this be a string
        // literal in single-quotes?" hint (issue #6560).
        let mut db = Database::new();
        create_test_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_users_quoted_nonexistent".to_string(),
            schema: None,
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::Column {
                column_name: "nonexistent_column".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: true,
            }],
            where_clause: None,
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        match result {
            Err(ExecutorError::NoSuchColumn { column_ref }) => {
                assert_eq!(
                    column_ref,
                    "\"nonexistent_column\" - should this be a string literal in single-quotes?"
                );
            }
            other => panic!("expected NoSuchColumn, got {other:?}"),
        }
    }

    #[test]
    fn test_create_index_if_not_exists_when_not_exists() {
        let mut db = Database::new();
        create_test_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_users_email".to_string(),
            schema: None,
            if_not_exists: true,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::Column {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            "Index 'idx_users_email' created successfully on table 'main.users'"
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
            schema: None,
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::Column {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
        };
        CreateIndexExecutor::execute(&stmt, &mut db).unwrap();

        // Second creation with IF NOT EXISTS should succeed
        let stmt_with_if_not_exists = CreateIndexStmt {
            index_name: "idx_users_email".to_string(),
            schema: None,
            if_not_exists: true,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::Column {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
        };
        let result = CreateIndexExecutor::execute(&stmt_with_if_not_exists, &mut db);
        assert!(result.is_ok());
        assert!(db.index_exists("idx_users_email"));
    }

    #[test]
    fn test_create_index_with_schema_qualified_table() {
        let mut db = Database::new();
        create_test_table(&mut db);

        // Create index using schema-qualified table name (with default main schema)
        let index_stmt = CreateIndexStmt {
            index_name: "idx_users_email_qualified".to_string(),
            schema: None,
            if_not_exists: false,
            table_name: "main.users".to_string(), // Explicitly qualify with main schema
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::Column {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
        };

        let result = CreateIndexExecutor::execute(&index_stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            "Index 'idx_users_email_qualified' created successfully on table 'main.users'"
        );

        // Verify index exists
        assert!(db.index_exists("idx_users_email_qualified"));
    }

    /// Build a `CreateTableStmt` with two unconstrained columns (a, b).
    /// `temporary` selects between a main table and a TEMP table.
    fn simple_ab_table(name: &str, temporary: bool) -> CreateTableStmt {
        CreateTableStmt {
            temporary,
            if_not_exists: false,
            table_name: name.to_string(),
            columns: vec![
                ColumnDef {
                    name: "a".to_string(),
                    data_type: DataType::Integer,
                    nullable: true,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                    generated_expr: None,
                    is_exact_integer_type: false,
                    type_source: None,
                },
                ColumnDef {
                    name: "b".to_string(),
                    data_type: DataType::Integer,
                    nullable: true,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                    generated_expr: None,
                    is_exact_integer_type: false,
                    type_source: None,
                },
            ],
            table_constraints: vec![],
            table_options: vec![],
            quoted: false,
            name_source: None,
            as_query: None,
            without_rowid: false,
            strict: false,
        }
    }

    fn index_on_b(index_name: &str, table_name: &str) -> CreateIndexStmt {
        CreateIndexStmt {
            index_name: index_name.to_string(),
            schema: None,
            if_not_exists: false,
            table_name: table_name.to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::Column {
                column_name: "b".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
        }
    }

    /// Regression test for #5505: `CREATE INDEX` on an unqualified TEMP table
    /// must resolve the table in the session temp schema, not assume `main`.
    #[test]
    fn test_create_index_on_temp_table_resolves_temp_schema() {
        let mut db = Database::new();

        // CREATE TEMP TABLE tbl(a, b)
        CreateTableExecutor::execute(&simple_ab_table("tbl", true), &mut db).unwrap();

        // CREATE INDEX tbl_idx ON tbl(b) -- unqualified, must find the temp table
        let result = CreateIndexExecutor::execute(&index_on_b("tbl_idx", "tbl"), &mut db);
        assert!(
            result.is_ok(),
            "CREATE INDEX on a temp table should succeed (got {:?})",
            result.err()
        );

        // The index must be reported against the temp schema, matching sqlite3
        // which records the index in sqlite_temp_master.
        let msg = result.unwrap();
        assert!(
            msg.contains(&format!("{}.tbl", db.catalog.temp_schema_name())),
            "index should be created in the temp schema, got: {msg}"
        );

        // Index exists and is honored against live temp-table data.
        assert!(db.index_exists("tbl_idx"));

        db.insert_row("tbl", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(2)])).unwrap();
        db.insert_row("tbl", Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(4)])).unwrap();

        let rows: Vec<_> = db.get_table("tbl").unwrap().scan_live().collect();
        assert_eq!(rows.len(), 2, "temp table should hold the inserted rows");
    }

    /// Regression test for #5505: when a table of the same name exists in both
    /// `main` and the temp schema, an unqualified `CREATE INDEX` resolves to the
    /// TEMP table (temp shadows main), matching sqlite3 3.51.0.
    #[test]
    fn test_create_index_temp_shadows_main() {
        let mut db = Database::new();

        // main.t and a shadowing temp.t
        CreateTableExecutor::execute(&simple_ab_table("t", false), &mut db).unwrap();
        CreateTableExecutor::execute(&simple_ab_table("t", true), &mut db).unwrap();

        let result = CreateIndexExecutor::execute(&index_on_b("ix", "t"), &mut db);
        assert!(result.is_ok(), "CREATE INDEX should resolve to temp.t (got {:?})", result.err());

        let msg = result.unwrap();
        assert!(
            msg.contains(&format!("{}.t", db.catalog.temp_schema_name())),
            "unqualified index target must shadow to the temp table, got: {msg}"
        );
        assert!(db.index_exists("ix"));
    }

    #[test]
    fn test_create_index_on_nonexistent_schema_qualified_table() {
        let mut db = Database::new();

        // Create a custom schema
        db.catalog.create_schema("test_schema".to_string()).unwrap();

        // Try to create index on non-existent table
        let index_stmt = CreateIndexStmt {
            index_name: "idx_nonexistent".to_string(),
            schema: None,
            if_not_exists: false,
            table_name: "test_schema.nonexistent_table".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::Column {
                column_name: "id".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
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
            temporary: false,
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
                    generated_expr: None,
                    is_exact_integer_type: false,
                    type_source: None,
                },
                ColumnDef {
                    name: "embedding".to_string(),
                    data_type: DataType::Vector { dimensions: 3 },
                    nullable: true,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                    generated_expr: None,
                    is_exact_integer_type: false,
                    type_source: None,
                },
                ColumnDef {
                    name: "content".to_string(),
                    data_type: DataType::Varchar { max_length: Some(1000) },
                    nullable: true,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                    generated_expr: None,
                    is_exact_integer_type: false,
                    type_source: None,
                },
            ],
            table_constraints: vec![],
            table_options: vec![],
            quoted: false,
            name_source: None,
            as_query: None,
            without_rowid: false,
            strict: false,
        };

        CreateTableExecutor::execute(&stmt, db).unwrap();
    }

    #[test]
    fn test_create_ivfflat_index_l2() {
        let mut db = Database::new();
        create_vector_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_documents_embedding".to_string(),
            schema: None,
            if_not_exists: false,
            table_name: "documents".to_string(),
            index_type: vibesql_ast::IndexType::IVFFlat {
                metric: vibesql_ast::VectorDistanceMetric::L2,
                lists: 4,
            },
            columns: vec![IndexColumn::Column {
                column_name: "embedding".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
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
            schema: None,
            if_not_exists: false,
            table_name: "documents".to_string(),
            index_type: vibesql_ast::IndexType::IVFFlat {
                metric: vibesql_ast::VectorDistanceMetric::Cosine,
                lists: 4,
            },
            columns: vec![IndexColumn::Column {
                column_name: "embedding".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
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
            schema: None,
            if_not_exists: false,
            table_name: "documents".to_string(),
            index_type: vibesql_ast::IndexType::IVFFlat {
                metric: vibesql_ast::VectorDistanceMetric::InnerProduct,
                lists: 4,
            },
            columns: vec![IndexColumn::Column {
                column_name: "embedding".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
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
            schema: None,
            if_not_exists: false,
            table_name: "documents".to_string(),
            index_type: vibesql_ast::IndexType::IVFFlat {
                metric: vibesql_ast::VectorDistanceMetric::L2,
                lists: 4,
            },
            columns: vec![IndexColumn::Column {
                column_name: "content".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
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
            schema: None,
            if_not_exists: false,
            table_name: "documents".to_string(),
            index_type: vibesql_ast::IndexType::IVFFlat {
                metric: vibesql_ast::VectorDistanceMetric::L2,
                lists: 4,
            },
            columns: vec![
                IndexColumn::Column {
                    column_name: "embedding".to_string(),
                    direction: OrderDirection::Asc,
                    prefix_length: None,
                    collation: None,
                    is_quoted: false,
                },
                IndexColumn::Column {
                    column_name: "id".to_string(),
                    direction: OrderDirection::Asc,
                    prefix_length: None,
                    collation: None,
                    is_quoted: false,
                },
            ],
            where_clause: None,
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
            schema: None,
            if_not_exists: true,
            table_name: "documents".to_string(),
            index_type: vibesql_ast::IndexType::IVFFlat {
                metric: vibesql_ast::VectorDistanceMetric::L2,
                lists: 4,
            },
            columns: vec![IndexColumn::Column {
                column_name: "embedding".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
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
            schema: None,
            if_not_exists: false,
            table_name: "documents".to_string(),
            index_type: vibesql_ast::IndexType::IVFFlat {
                metric: vibesql_ast::VectorDistanceMetric::L2,
                lists: 2, // 2 clusters for small test data
            },
            columns: vec![IndexColumn::Column {
                column_name: "embedding".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
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
            schema: None,
            if_not_exists: false,
            table_name: "documents".to_string(),
            index_type: vibesql_ast::IndexType::IVFFlat {
                metric: vibesql_ast::VectorDistanceMetric::Cosine,
                lists: 2,
            },
            columns: vec![IndexColumn::Column {
                column_name: "embedding".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
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
            schema: None,
            if_not_exists: false,
            table_name: "documents".to_string(),
            index_type: vibesql_ast::IndexType::IVFFlat {
                metric: vibesql_ast::VectorDistanceMetric::L2,
                lists: 4,
            },
            columns: vec![IndexColumn::Column {
                column_name: "embedding".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
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

    // ========================================================================
    // Expression Index Tests
    // ========================================================================

    #[test]
    fn test_create_expression_index_lower() {
        let mut db = Database::new();
        create_test_table(&mut db);

        // Insert some test data
        db.insert_row(
            "users",
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Test@Example.COM")),
                SqlValue::Varchar(arcstr::ArcStr::from("John")),
            ]),
        )
        .unwrap();

        // Create expression index on LOWER(email)
        let stmt = CreateIndexStmt {
            index_name: "idx_users_email_lower".to_string(),
            schema: None,
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::new_expression(
                vibesql_ast::Expression::Function {
                    name: vibesql_ast::FunctionIdentifier::new("lower"),
                    args: vec![vibesql_ast::Expression::ColumnRef(
                        vibesql_ast::ColumnIdentifier::simple("email", false),
                    )],
                    character_unit: None,
                },
                OrderDirection::Asc,
            )],
            where_clause: None,
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok(), "Expression index creation failed: {:?}", result.err());
        assert!(db.index_exists("idx_users_email_lower"));
    }

    #[test]
    fn test_create_expression_index_arithmetic() {
        let mut db = Database::new();

        // Create a table with numeric columns
        let table_stmt = vibesql_ast::CreateTableStmt {
            temporary: false,
            if_not_exists: false,
            table_name: "products".to_string(),
            columns: vec![
                vibesql_ast::ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                    generated_expr: None,
                    is_exact_integer_type: false,
                    type_source: None,
                },
                vibesql_ast::ColumnDef {
                    name: "price".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                    generated_expr: None,
                    is_exact_integer_type: false,
                    type_source: None,
                },
                vibesql_ast::ColumnDef {
                    name: "discount".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                    generated_expr: None,
                    is_exact_integer_type: false,
                    type_source: None,
                },
            ],
            table_constraints: vec![],
            table_options: vec![],
            quoted: false,
            name_source: None,
            as_query: None,
            without_rowid: false,
            strict: false,
        };
        crate::CreateTableExecutor::execute(&table_stmt, &mut db).unwrap();

        // Insert test data
        db.insert_row(
            "products",
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100), SqlValue::Integer(10)]),
        )
        .unwrap();

        // Create expression index on (price - discount)
        let stmt = CreateIndexStmt {
            index_name: "idx_products_net_price".to_string(),
            schema: None,
            if_not_exists: false,
            table_name: "products".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::new_expression(
                vibesql_ast::Expression::BinaryOp {
                    op: vibesql_ast::BinaryOperator::Minus,
                    left: Box::new(vibesql_ast::Expression::ColumnRef(
                        vibesql_ast::ColumnIdentifier::simple("price", false),
                    )),
                    right: Box::new(vibesql_ast::Expression::ColumnRef(
                        vibesql_ast::ColumnIdentifier::simple("discount", false),
                    )),
                },
                OrderDirection::Asc,
            )],
            where_clause: None,
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok(), "Arithmetic expression index creation failed: {:?}", result.err());
        assert!(db.index_exists("idx_products_net_price"));
    }

    #[test]
    fn test_create_unique_expression_index() {
        let mut db = Database::new();
        create_test_table(&mut db);

        // Insert data with different emails but same lowercase
        db.insert_row(
            "users",
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("user@example.com")),
                SqlValue::Varchar(arcstr::ArcStr::from("John")),
            ]),
        )
        .unwrap();

        // Create unique expression index on LOWER(email)
        let stmt = CreateIndexStmt {
            index_name: "idx_users_email_lower_unique".to_string(),
            schema: None,
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: true },
            columns: vec![IndexColumn::new_expression(
                vibesql_ast::Expression::Function {
                    name: vibesql_ast::FunctionIdentifier::new("lower"),
                    args: vec![vibesql_ast::Expression::ColumnRef(
                        vibesql_ast::ColumnIdentifier::simple("email", false),
                    )],
                    character_unit: None,
                },
                OrderDirection::Asc,
            )],
            where_clause: None,
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok(), "Unique expression index creation failed: {:?}", result.err());
        assert!(db.index_exists("idx_users_email_lower_unique"));
    }

    #[test]
    fn test_create_expression_index_rejects_non_deterministic() {
        let mut db = Database::new();
        create_test_table(&mut db);

        // Try to create expression index on RANDOM() - should fail
        let stmt = CreateIndexStmt {
            index_name: "idx_users_random".to_string(),
            schema: None,
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::new_expression(
                vibesql_ast::Expression::Function {
                    name: vibesql_ast::FunctionIdentifier::new("random"),
                    args: vec![],
                    character_unit: None,
                },
                OrderDirection::Asc,
            )],
            where_clause: None,
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_err(), "Non-deterministic expression index should fail");
        assert!(matches!(result, Err(ExecutorError::UnsupportedFeature(_))));
    }

    #[test]
    fn test_create_expression_index_validates_column_references() {
        let mut db = Database::new();
        create_test_table(&mut db);

        // Try to create expression index with non-existent column
        let stmt = CreateIndexStmt {
            index_name: "idx_users_bad_col".to_string(),
            schema: None,
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::new_expression(
                vibesql_ast::Expression::Function {
                    name: vibesql_ast::FunctionIdentifier::new("lower"),
                    args: vec![vibesql_ast::Expression::ColumnRef(
                        vibesql_ast::ColumnIdentifier::simple("nonexistent_column", false),
                    )],
                    character_unit: None,
                },
                OrderDirection::Asc,
            )],
            where_clause: None,
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_err(), "Expression index with non-existent column should fail");
        // Expression-index column-not-found now raises the same `NoSuchColumn`
        // (SQLite `no such column: X`) variant the CHECK-constraint resolver
        // uses, so unresolved index-expression column references get the same
        // "should this be a string literal in single-quotes?" hint for
        // unqualified delimited identifiers (quote.test 2.1.2/2.1.4).
        assert!(matches!(result, Err(ExecutorError::NoSuchColumn { .. })));
    }

    #[test]
    fn test_create_expression_index_handles_null() {
        let mut db = Database::new();
        create_test_table(&mut db);

        // Insert row with NULL name
        db.insert_row(
            "users",
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("user@example.com")),
                SqlValue::Null, // NULL name
            ]),
        )
        .unwrap();

        // Create expression index on LOWER(name) - should handle NULL
        let stmt = CreateIndexStmt {
            index_name: "idx_users_name_lower".to_string(),
            schema: None,
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::new_expression(
                vibesql_ast::Expression::Function {
                    name: vibesql_ast::FunctionIdentifier::new("lower"),
                    args: vec![vibesql_ast::Expression::ColumnRef(
                        vibesql_ast::ColumnIdentifier::simple("name", false),
                    )],
                    character_unit: None,
                },
                OrderDirection::Asc,
            )],
            where_clause: None,
        };

        let result = CreateIndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok(), "Expression index with NULL should succeed: {:?}", result.err());
        assert!(db.index_exists("idx_users_name_lower"));
    }
}
