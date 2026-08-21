//! REINDEX statement execution
//!
//! REINDEX rebuilds indexes to reclaim space or improve query performance.
//! This is a no-op implementation for SQLite compatibility - the database
//! maintains indexes automatically, so explicit reindexing is not needed.

use vibesql_ast::ReindexStmt;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Return `true` if `name` is one of the three built-in collating sequences
/// VibeSQL implements (`BINARY`, `NOCASE`, `RTRIM`). Matching is
/// case-insensitive, mirroring the canonical set used by SELECT-time collation
/// validation. User-defined (C-API) collations are not reachable from the CLI,
/// so only the built-ins are recognized here.
fn is_builtin_collation(name: &str) -> bool {
    name.eq_ignore_ascii_case("binary")
        || name.eq_ignore_ascii_case("nocase")
        || name.eq_ignore_ascii_case("rtrim")
}

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
                // REINDEX with a specific target. SQLite resolves the name (in
                // this order) against: a collating sequence, a table, or an
                // index. If it matches none of those it raises
                // `unable to identify the object to be reindexed`.

                // A registered collating sequence. VibeSQL implements only the
                // three SQLite built-ins (BINARY, NOCASE, RTRIM); matching is
                // case-insensitive. `REINDEX nocase` / `REINDEX binary` succeed
                // (e_reindex-0.1).
                if is_builtin_collation(target) {
                    return Ok(format!(
                        "REINDEX completed successfully - collation '{}' is optimized",
                        target
                    ));
                }

                // A schema name (VibeSQL has only the implicit "main"/"temp"
                // schemas). `REINDEX main` rebuilds every index in that schema.
                if target.eq_ignore_ascii_case("main") || target.eq_ignore_ascii_case("temp") {
                    return Ok(format!(
                        "REINDEX completed successfully - schema '{}' is optimized",
                        target
                    ));
                }

                // An index name.
                if database.index_exists(target) {
                    // It's an index - reindexing is not needed but we pretend to succeed
                    return Ok(format!(
                        "REINDEX completed successfully - index '{}' is optimized",
                        target
                    ));
                }

                // A table name.
                if database.get_table(target).is_some() {
                    // It's a table - reindex all its indexes
                    return Ok(format!(
                        "REINDEX completed successfully - all indexes for table '{}' are optimized",
                        target
                    ));
                }

                // Matches nothing - SQLite-compatible error (no object name).
                Err(ExecutorError::ReindexObjectUnknown)
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
            schema: None,
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::Column {
                column_name: "email".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
                collation: None,
            }],
            where_clause: None,
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
            schema: None,
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::Column {
                column_name: "email".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
                collation: None,
            }],
            where_clause: None,
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

        // Try to reindex an object that is neither table, index, nor collation.
        // SQLite raises `unable to identify the object to be reindexed`
        // (reindex-1.9).
        let reindex_stmt = ReindexStmt { target: Some("nonexistent".to_string()) };
        let result = ReindexExecutor::execute(&reindex_stmt, &db);
        assert!(matches!(result, Err(ExecutorError::ReindexObjectUnknown)));
        assert_eq!(
            result.unwrap_err().to_string(),
            "unable to identify the object to be reindexed"
        );
    }

    #[test]
    fn test_reindex_builtin_collation() {
        let db = Database::new();

        // Built-in collation names are valid REINDEX targets and succeed even
        // with no matching table/index (e_reindex-0.1: REINDEX nocase/binary).
        for name in ["nocase", "NOCASE", "binary", "rtrim", "RTRIM"] {
            let stmt = ReindexStmt { target: Some(name.to_string()) };
            assert!(
                ReindexExecutor::execute(&stmt, &db).is_ok(),
                "REINDEX {name} should succeed"
            );
        }
    }

    #[test]
    fn test_reindex_schema_name() {
        let db = Database::new();

        // A bare schema name reindexes that whole schema.
        let stmt = ReindexStmt { target: Some("main".to_string()) };
        assert!(ReindexExecutor::execute(&stmt, &db).is_ok());
    }
}
