//! DROP TABLE statement execution

use vibesql_ast::DropTableStmt;
use vibesql_storage::Database;

use crate::{errors::ExecutorError, privilege_checker::PrivilegeChecker};

/// Executor for DROP TABLE statements
pub struct DropTableExecutor;

impl DropTableExecutor {
    /// Execute a DROP TABLE statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The DROP TABLE statement AST node
    /// * `database` - The database to drop the table from
    ///
    /// # Returns
    ///
    /// Success message or error
    ///
    /// # Examples
    ///
    /// ```
    /// use vibesql_ast::{ColumnDef, CreateTableStmt, DropTableStmt};
    /// use vibesql_executor::{CreateTableExecutor, DropTableExecutor};
    /// use vibesql_storage::Database;
    /// use vibesql_types::DataType;
    ///
    /// let mut db = Database::new();
    /// let create_stmt = CreateTableStmt {
    ///     if_not_exists: false,
    ///     table_name: "users".to_string(),
    ///     columns: vec![ColumnDef {
    ///         name: "id".to_string(),
    ///         data_type: DataType::Integer,
    ///         nullable: false,
    ///         constraints: vec![],
    ///         default_value: None,
    ///         comment: None,
    ///     }],
    ///     table_constraints: vec![],
    ///     table_options: vec![],
    /// };
    /// CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
    ///
    /// let stmt = DropTableStmt { table_name: "users".to_string(), if_exists: false };
    ///
    /// let result = DropTableExecutor::execute(&stmt, &mut db);
    /// assert!(result.is_ok());
    /// ```
    pub fn execute(stmt: &DropTableStmt, database: &mut Database) -> Result<String, ExecutorError> {
        // Check if table exists
        let table_exists = database.catalog.table_exists(&stmt.table_name);

        // If IF EXISTS is specified and table doesn't exist, succeed silently
        if stmt.if_exists && !table_exists {
            return Ok(format!("Table '{}' does not exist (IF EXISTS specified)", stmt.table_name));
        }

        // If table doesn't exist and IF EXISTS is not specified, return error
        if !table_exists {
            return Err(ExecutorError::TableNotFound(stmt.table_name.clone()));
        }

        // Check DROP privilege on the table
        PrivilegeChecker::check_drop(database, &stmt.table_name)?;

        // Drop all indexes associated with this table first
        let dropped_indexes = database.catalog.drop_table_indexes(&stmt.table_name);
        let index_count = dropped_indexes.len();

        // Drop physical indexes from storage
        for index in &dropped_indexes {
            // Try to drop from B-tree storage (ignore errors if not found)
            let _ = database.drop_index(&index.name);
            // Try to drop from spatial storage (ignore errors if not found)
            let _ = database.drop_spatial_index(&index.name);
        }

        // Drop the table from storage (this also removes from catalog)
        database
            .drop_table(&stmt.table_name)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        // Return success message
        if index_count > 0 {
            Ok(format!(
                "Table '{}' and {} associated index(es) dropped successfully",
                stmt.table_name, index_count
            ))
        } else {
            Ok(format!("Table '{}' dropped successfully", stmt.table_name))
        }
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{ColumnDef, CreateTableStmt};
    use vibesql_types::DataType;

    use super::*;
    use crate::CreateTableExecutor;

    #[test]
    fn test_drop_existing_table() {
        let mut db = Database::new();

        // Create a table first
        let create_stmt = CreateTableStmt {
        if_not_exists: false,
            table_name: "users".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            }],
            table_constraints: vec![],
            table_options: vec![],
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
        assert!(db.catalog.table_exists("users"));

        // Now drop it
        let drop_stmt = DropTableStmt { table_name: "users".to_string(), if_exists: false };

        let result = DropTableExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Table 'users' dropped successfully");

        // Verify table no longer exists
        assert!(!db.catalog.table_exists("users"));
        assert!(db.get_table("users").is_none());
    }

    #[test]
    fn test_drop_nonexistent_table_without_if_exists() {
        let mut db = Database::new();

        let drop_stmt = DropTableStmt { table_name: "nonexistent".to_string(), if_exists: false };

        let result = DropTableExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::TableNotFound(_))));
    }

    #[test]
    fn test_drop_nonexistent_table_with_if_exists() {
        let mut db = Database::new();

        let drop_stmt = DropTableStmt { table_name: "nonexistent".to_string(), if_exists: true };

        let result = DropTableExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Table 'nonexistent' does not exist (IF EXISTS specified)");
    }

    #[test]
    fn test_drop_existing_table_with_if_exists() {
        let mut db = Database::new();

        // Create a table first
        let create_stmt = CreateTableStmt {
        if_not_exists: false,
            table_name: "products".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            }],
            table_constraints: vec![],
            table_options: vec![],
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Drop it with IF EXISTS
        let drop_stmt = DropTableStmt { table_name: "products".to_string(), if_exists: true };

        let result = DropTableExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Table 'products' dropped successfully");

        // Verify table no longer exists
        assert!(!db.catalog.table_exists("products"));
    }

    #[test]
    fn test_drop_table_with_data() {
        let mut db = Database::new();

        // Create a table with data
        let create_stmt = CreateTableStmt {
        if_not_exists: false,
            table_name: "customers".to_string(),
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
                    name: "name".to_string(),
                    data_type: DataType::Varchar { max_length: Some(100) },
                    nullable: false,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
            ],
            table_constraints: vec![],
            table_options: vec![],
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Insert some data
        use vibesql_storage::Row;
        use vibesql_types::SqlValue;
        let row = Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]);
        db.insert_row("customers", row).unwrap();

        // Verify data exists
        assert_eq!(db.get_table("customers").unwrap().row_count(), 1);

        // Drop the table
        let drop_stmt = DropTableStmt { table_name: "customers".to_string(), if_exists: false };

        let result = DropTableExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());

        // Verify table and data are gone
        assert!(!db.catalog.table_exists("customers"));
        assert!(db.get_table("customers").is_none());
    }

    #[test]
    fn test_drop_and_recreate_table() {
        let mut db = Database::new();

        // Create table
        let create_stmt = CreateTableStmt {
        if_not_exists: false,
            table_name: "temp".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            }],
            table_constraints: vec![],
            table_options: vec![],
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Drop it
        let drop_stmt = DropTableStmt { table_name: "temp".to_string(), if_exists: false };
        DropTableExecutor::execute(&drop_stmt, &mut db).unwrap();

        // Recreate it
        let result = CreateTableExecutor::execute(&create_stmt, &mut db);
        assert!(result.is_ok());
        assert!(db.catalog.table_exists("temp"));
    }

    #[test]
    fn test_drop_multiple_tables() {
        let mut db = Database::new();

        // Create multiple tables
        for name in &["table1", "table2", "table3"] {
            let create_stmt = CreateTableStmt {
        if_not_exists: false,
                table_name: name.to_string(),
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                }],
                table_constraints: vec![],
                table_options: vec![],
            };
            CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
        }

        assert_eq!(db.list_tables().len(), 3);

        // Drop them one by one
        for name in &["table1", "table2", "table3"] {
            let drop_stmt = DropTableStmt { table_name: name.to_string(), if_exists: false };
            let result = DropTableExecutor::execute(&drop_stmt, &mut db);
            assert!(result.is_ok());
        }

        assert_eq!(db.list_tables().len(), 0);
    }

    #[test]
    fn test_drop_table_case_sensitivity() {
        let mut db = Database::new();

        // Create table with specific case
        let create_stmt = CreateTableStmt {
        if_not_exists: false,
            table_name: "MyTable".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            }],
            table_constraints: vec![],
            table_options: vec![],
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Try to drop with exact case - should succeed
        let drop_stmt = DropTableStmt { table_name: "MyTable".to_string(), if_exists: false };
        let result = DropTableExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());
    }

    #[test]
    fn test_drop_table_cascades_to_indexes() {
        use crate::CreateIndexExecutor;
        use vibesql_ast::{CreateIndexStmt, IndexColumn, OrderDirection};

        let mut db = Database::new();

        // Create table
        let create_stmt = CreateTableStmt {
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
            ],
            table_constraints: vec![],
            table_options: vec![],
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Create indexes on the table
        let index1_stmt = CreateIndexStmt {
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
        CreateIndexExecutor::execute(&index1_stmt, &mut db).unwrap();

        let index2_stmt = CreateIndexStmt {
            index_name: "idx_users_id".to_string(),
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn {
                column_name: "id".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            }],
        };
        CreateIndexExecutor::execute(&index2_stmt, &mut db).unwrap();

        // Verify indexes exist
        assert!(db.index_exists("idx_users_email"));
        assert!(db.index_exists("idx_users_id"));

        // Drop the table
        let drop_stmt = DropTableStmt { table_name: "users".to_string(), if_exists: false };
        let result = DropTableExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());

        // Verify table is dropped
        assert!(!db.catalog.table_exists("users"));

        // Verify indexes are also dropped (CASCADE behavior)
        assert!(!db.index_exists("idx_users_email"));
        assert!(!db.index_exists("idx_users_id"));
    }

    #[test]
    fn test_drop_and_recreate_table_with_same_index_names() {
        use crate::CreateIndexExecutor;
        use vibesql_ast::{CreateIndexStmt, IndexColumn, OrderDirection};

        let mut db = Database::new();

        // Create table
        let create_stmt = CreateTableStmt {
        if_not_exists: false,
            table_name: "products".to_string(),
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
                    name: "name".to_string(),
                    data_type: DataType::Varchar { max_length: Some(100) },
                    nullable: false,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
            ],
            table_constraints: vec![],
            table_options: vec![],
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Create index
        let index_stmt = CreateIndexStmt {
            index_name: "idx_products_name".to_string(),
            if_not_exists: false,
            table_name: "products".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn {
                column_name: "name".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
            }],
        };
        CreateIndexExecutor::execute(&index_stmt, &mut db).unwrap();

        // Verify index exists
        assert!(db.index_exists("idx_products_name"));

        // Drop the table (should cascade to drop index)
        let drop_stmt = DropTableStmt { table_name: "products".to_string(), if_exists: false };
        DropTableExecutor::execute(&drop_stmt, &mut db).unwrap();

        // Verify both table and index are dropped
        assert!(!db.catalog.table_exists("products"));
        assert!(!db.index_exists("idx_products_name"));

        // Recreate table with same name
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Create index with same name - should succeed (no IndexAlreadyExists error)
        let result = CreateIndexExecutor::execute(&index_stmt, &mut db);
        assert!(result.is_ok(), "Should be able to recreate index with same name after table drop");
        assert!(db.index_exists("idx_products_name"));
    }

    #[test]
    fn test_drop_table_without_indexes() {
        let mut db = Database::new();

        // Create table without indexes
        let create_stmt = CreateTableStmt {
        if_not_exists: false,
            table_name: "simple_table".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            }],
            table_constraints: vec![],
            table_options: vec![],
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Drop table without indexes - should still work
        let drop_stmt = DropTableStmt { table_name: "simple_table".to_string(), if_exists: false };
        let result = DropTableExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok(), "Dropping table without indexes should still work");
        assert!(!db.catalog.table_exists("simple_table"));
    }
}
