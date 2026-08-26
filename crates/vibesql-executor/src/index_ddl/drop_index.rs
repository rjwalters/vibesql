//! DROP INDEX statement execution

use vibesql_ast::DropIndexStmt;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Executor for DROP INDEX statements
pub struct DropIndexExecutor;

impl DropIndexExecutor {
    /// Execute a DROP INDEX statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The DROP INDEX statement AST node
    /// * `database` - The database to drop the index from
    ///
    /// # Returns
    ///
    /// Success message or error
    pub fn execute(stmt: &DropIndexStmt, database: &mut Database) -> Result<String, ExecutorError> {
        // An explicit `schema.` qualifier (issue #6366) scopes resolution to
        // exactly that schema — no temp-shadows-main search — matching
        // sqlite3's `DROP INDEX schema.name` semantics.
        if let Some(explicit_schema) = &stmt.schema {
            return Self::execute_schema_qualified(stmt, database, explicit_schema);
        }

        let index_name = &stmt.index_name;

        // Find which table/schema this index belongs to. With schema-aware
        // indexes (#5540 storage / #5513 catalog) a temp index and a main index
        // can share a name; an unqualified `DROP INDEX` resolves temp-shadows-
        // main, so prefer a temp-schema index over a same-named main index —
        // matching sqlite3.
        let all_indexes = database.catalog.list_all_indexes();
        let index_metadata = all_indexes
            .iter()
            .find(|idx| {
                idx.name == *index_name && vibesql_catalog::Catalog::is_temp_schema(idx.schema())
            })
            .or_else(|| all_indexes.iter().find(|idx| idx.name == *index_name));

        if let Some(metadata) = index_metadata {
            // Target the resolved index exactly via its owning schema so a temp
            // index and a same-named main index are dropped independently.
            let schema = metadata.schema().to_string();
            let qualified_table = format!("{}.{}", schema, metadata.table_name);
            let qualified_index = format!("{}.{}", schema, index_name);

            // Emit WAL entry for persistence BEFORE dropping
            database.emit_wal_drop_index(index_name_to_id(index_name), index_name);

            // Drop from catalog (schema-qualified table so the exact index goes)
            database
                .catalog
                .drop_index(&qualified_table, index_name)
                .map_err(|e| ExecutorError::Other(format!("Catalog error: {}", e)))?;

            // Check if it's a spatial index in storage. Spatial indexes are
            // schema-aware too (#5558), so target the exact index via its
            // owning schema — a temp index and a same-named main index drop
            // independently, matching the B-tree path below.
            if database.spatial_index_exists(&qualified_index) {
                database.drop_spatial_index(&qualified_index)?;
            }

            // Check if it's a B-tree index in storage (schema-qualified)
            if database.index_exists(&qualified_index) {
                database.drop_index(&qualified_index)?;
            }

            return Ok(format!("Index '{}' dropped successfully", index_name));
        }

        // Fallback: check storage without catalog metadata (for legacy indexes)
        // Check if it's a spatial index first
        if database.spatial_index_exists(index_name) {
            // Emit WAL entry for persistence BEFORE dropping
            database.emit_wal_drop_index(index_name_to_id(index_name), index_name);
            database.drop_spatial_index(index_name)?;
            return Ok(format!("Spatial index '{}' dropped successfully", index_name));
        }

        // Otherwise check if it's a B-tree index
        if database.index_exists(index_name) {
            // Emit WAL entry for persistence BEFORE dropping
            database.emit_wal_drop_index(index_name_to_id(index_name), index_name);
            database.drop_index(index_name)?;
            return Ok(format!("Index '{}' dropped successfully", index_name));
        }

        // Index not found
        if stmt.if_exists {
            // IF EXISTS: silently succeed if index doesn't exist
            Ok(format!("Index '{}' does not exist (skipped)", index_name))
        } else {
            Err(ExecutorError::IndexNotFound(index_name.clone()))
        }
    }

    /// Execute `DROP INDEX schema.index_name`, scoping resolution to exactly
    /// the named schema (no temp-shadows-main search).
    ///
    /// `temp` maps to this session's temp schema, matching the CREATE INDEX
    /// side (issue #6366). sqlite3 3.51.0 does not distinguish an unknown
    /// schema qualifier from an unknown index here — both surface as
    /// `no such index: schema.name` — so no separate schema-existence check
    /// is needed: a bogus schema simply never matches any index below.
    fn execute_schema_qualified(
        stmt: &DropIndexStmt,
        database: &mut Database,
        explicit_schema: &str,
    ) -> Result<String, ExecutorError> {
        let index_name = &stmt.index_name;

        let resolved_schema = if explicit_schema.eq_ignore_ascii_case(vibesql_catalog::TEMP_SCHEMA)
        {
            database.catalog.temp_schema_name().to_string()
        } else {
            explicit_schema.to_string()
        };

        let all_indexes = database.catalog.list_all_indexes();
        let index_metadata = all_indexes
            .iter()
            .find(|idx| idx.name == *index_name && idx.schema() == resolved_schema);

        if let Some(metadata) = index_metadata {
            let qualified_table = format!("{}.{}", resolved_schema, metadata.table_name);
            let qualified_index = format!("{}.{}", resolved_schema, index_name);

            // Emit WAL entry for persistence BEFORE dropping
            database.emit_wal_drop_index(index_name_to_id(index_name), index_name);

            // Drop from catalog (schema-qualified table so the exact index goes)
            database
                .catalog
                .drop_index(&qualified_table, index_name)
                .map_err(|e| ExecutorError::Other(format!("Catalog error: {}", e)))?;

            if database.spatial_index_exists(&qualified_index) {
                database.drop_spatial_index(&qualified_index)?;
            }

            if database.index_exists(&qualified_index) {
                database.drop_index(&qualified_index)?;
            }

            return Ok(format!("Index '{}' dropped successfully", index_name));
        }

        if stmt.if_exists {
            // IF EXISTS: silently succeed if index doesn't exist in this schema
            Ok(format!("Index '{}' does not exist (skipped)", index_name))
        } else {
            Err(ExecutorError::IndexNotFound(format!("{}.{}", explicit_schema, index_name)))
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
    use vibesql_ast::{ColumnDef, CreateIndexStmt, CreateTableStmt, IndexColumn, OrderDirection};
    use vibesql_types::DataType;

    use super::*;
    use crate::{index_ddl::create_index::CreateIndexExecutor, CreateTableExecutor};

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
    fn test_drop_index() {
        let mut db = Database::new();
        create_test_table(&mut db);

        // Create index
        let create_stmt = CreateIndexStmt {
            index_name: "idx_users_email".to_string(),
            schema: None,
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::Column {
                column_name: "email".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
        };
        CreateIndexExecutor::execute(&create_stmt, &mut db).unwrap();

        // Drop index
        let drop_stmt = DropIndexStmt {
            index_name: "idx_users_email".to_string(),
            if_exists: false,
            schema: None,
        };
        let result = DropIndexExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Index 'idx_users_email' dropped successfully");

        // Verify index no longer exists
        assert!(!db.index_exists("idx_users_email"));
    }

    #[test]
    fn test_drop_nonexistent_index() {
        let mut db = Database::new();

        let drop_stmt = DropIndexStmt {
            index_name: "nonexistent_index".to_string(),
            if_exists: false,
            schema: None,
        };
        let result = DropIndexExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::IndexNotFound(_))));
    }

    #[test]
    fn test_drop_index_if_exists_when_exists() {
        let mut db = Database::new();
        create_test_table(&mut db);

        // Create index
        let create_stmt = CreateIndexStmt {
            index_name: "idx_users_email".to_string(),
            schema: None,
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::Column {
                column_name: "email".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
        };
        CreateIndexExecutor::execute(&create_stmt, &mut db).unwrap();

        // Drop with IF EXISTS should succeed
        let drop_stmt = DropIndexStmt {
            index_name: "idx_users_email".to_string(),
            if_exists: true,
            schema: None,
        };
        let result = DropIndexExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Index 'idx_users_email' dropped successfully");
        assert!(!db.index_exists("idx_users_email"));
    }

    #[test]
    fn test_drop_index_if_exists_when_not_exists() {
        let mut db = Database::new();

        // Drop non-existent index with IF EXISTS should succeed
        let drop_stmt = DropIndexStmt {
            index_name: "nonexistent_index".to_string(),
            if_exists: true,
            schema: None,
        };
        let result = DropIndexExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());
        // Silently succeeds when index doesn't exist
    }

    #[test]
    fn test_case_insensitive_index_names() {
        let mut db = Database::new();
        create_test_table(&mut db);

        // Create index with lowercase name
        let create_stmt = CreateIndexStmt {
            index_name: "idx_test".to_string(),
            schema: None,
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::Column {
                column_name: "email".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
                collation: None,
                is_quoted: false,
            }],
            where_clause: None,
        };
        CreateIndexExecutor::execute(&create_stmt, &mut db).unwrap();

        // Drop with uppercase name should work (normalized to uppercase)
        let drop_stmt =
            DropIndexStmt { index_name: "IDX_TEST".to_string(), if_exists: false, schema: None };
        let result = DropIndexExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());
        assert!(!db.index_exists("idx_test"));
        assert!(!db.index_exists("IDX_TEST"));
    }
}
