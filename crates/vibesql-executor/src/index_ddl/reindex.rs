//! REINDEX statement execution
//!
//! REINDEX rebuilds indexes to reclaim space or improve query performance.
//! This is a no-op implementation for SQLite compatibility - the database
//! maintains indexes automatically, so explicit reindexing is not needed.

use vibesql_ast::ReindexStmt;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Executor for REINDEX statements
pub struct ReindexExecutor;

impl ReindexExecutor {
    /// Execute a REINDEX statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The REINDEX statement AST node
    /// * `database` - The database to reindex
    ///
    /// # Returns
    ///
    /// Success message or error
    ///
    /// # Implementation Note
    ///
    /// This is a no-op implementation. VibeSQL maintains indexes automatically,
    /// so explicit reindexing is not required. However, we parse and validate
    /// the target (if specified) for SQLite compatibility and better error messages.
    pub fn execute(stmt: &ReindexStmt, database: &Database) -> Result<String, ExecutorError> {
        match &stmt.target {
            None => {
                // REINDEX with no target - reindex all indexes
                // No-op: all indexes are already maintained optimally
                Ok("REINDEX completed successfully - all indexes are optimized".to_string())
            }
            Some(target) => {
                // REINDEX with specific target (database, table, or index name)
                // Validate that the target exists

                // First try as an index name
                if database.index_exists(target) {
                    // It's an index - reindexing is not needed but we pretend to succeed
                    return Ok(format!(
                        "REINDEX completed successfully - index '{}' is optimized",
                        target
                    ));
                }

                // Try as a table name
                if database.get_table(target).is_some() {
                    // It's a table - reindex all its indexes
                    return Ok(format!(
                        "REINDEX completed successfully - all indexes for table '{}' are optimized",
                        target
                    ));
                }

                // Not found - error out
                Err(ExecutorError::TableNotFound(target.clone()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{ColumnDef, CreateIndexStmt, CreateTableStmt, IndexColumn, OrderDirection};
    use vibesql_types::DataType;

    use super::*;
    use crate::{index_ddl::create_index::CreateIndexExecutor, CreateTableExecutor};

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
    fn test_reindex_all() {
        let db = Database::new();

        // REINDEX with no target should succeed
        let reindex_stmt = ReindexStmt { target: None };
        let result = ReindexExecutor::execute(&reindex_stmt, &db);
        assert!(result.is_ok());
        assert!(result.unwrap().contains("optimized"));
    }

    #[test]
    fn test_reindex_specific_index() {
        let mut db = Database::new();
        create_test_table(&mut db);

        // Create index
        let create_stmt = CreateIndexStmt {
            index_name: "idx_users_email".to_string(),
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn {
                column_name: "email".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            }],
        };
        CreateIndexExecutor::execute(&create_stmt, &mut db).unwrap();

        // Reindex the specific index
        let reindex_stmt = ReindexStmt { target: Some("idx_users_email".to_string()) };
        let result = ReindexExecutor::execute(&reindex_stmt, &db);
        assert!(result.is_ok());
        assert!(result.unwrap().contains("optimized"));
    }

    #[test]
    fn test_reindex_table() {
        let mut db = Database::new();
        create_test_table(&mut db);

        // Create an index on the table
        let create_stmt = CreateIndexStmt {
            index_name: "idx_users_email".to_string(),
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn {
                column_name: "email".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            }],
        };
        CreateIndexExecutor::execute(&create_stmt, &mut db).unwrap();

        // Reindex the table
        let reindex_stmt = ReindexStmt { target: Some("users".to_string()) };
        let result = ReindexExecutor::execute(&reindex_stmt, &db);
        assert!(result.is_ok());
        assert!(result.unwrap().contains("optimized"));
    }

    #[test]
    fn test_reindex_nonexistent_target() {
        let db = Database::new();

        // Try to reindex non-existent object
        let reindex_stmt = ReindexStmt { target: Some("nonexistent".to_string()) };
        let result = ReindexExecutor::execute(&reindex_stmt, &db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::TableNotFound(_))));
    }
}
