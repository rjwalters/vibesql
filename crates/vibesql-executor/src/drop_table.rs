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
    /// let create_stmt = CreateTableStmt { temporary: false,
    ///     if_not_exists: false,
    ///     table_name: "users".to_string(),
    ///     columns: vec![ColumnDef {
    ///         name: "id".to_string(),
    ///         data_type: DataType::Integer,
    ///         nullable: false,
    ///         constraints: vec![],
    ///         default_value: None,
    ///         comment: None,
    ///         generated_expr: None, is_exact_integer_type: false, type_source: None,
    ///     }],
    ///     table_constraints: vec![],
    ///     table_options: vec![],
    ///     quoted: false,
    ///     name_source: None,
    ///     as_query: None, without_rowid: false, strict: false,
    /// };
    /// CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
    ///
    /// let stmt = DropTableStmt { table_name: "users".to_string(), if_exists: false, quoted: false };
    ///
    /// let result = DropTableExecutor::execute(&stmt, &mut db);
    /// assert!(result.is_ok());
    /// ```
    pub fn execute(stmt: &DropTableStmt, database: &mut Database) -> Result<String, ExecutorError> {
        // `sqlite_master`/`sqlite_schema` may never be dropped. SQLite errors
        // `table sqlite_master may not be dropped` (sqlite3 3.51.0, table-5.2),
        // echoing the canonical (lowercase) name regardless of the user's casing
        // — `DROP TABLE SQLITE_MASTER` still reports `sqlite_master`. This guard
        // precedes the existence check because sqlite_master is a virtual table
        // that is not registered in the catalog. Other non-existent `sqlite_`
        // names fall through to the normal "no such table" path, matching
        // sqlite3 (issue #5614).
        if crate::sqlite_schema::is_sqlite_schema_table(&stmt.table_name) {
            return Err(ExecutorError::SqliteCompatError(
                "table sqlite_master may not be dropped".to_string(),
            ));
        }

        // `sqlite_sequence` (AUTOINCREMENT bookkeeping, issue #6173) may never
        // be dropped either, matching sqlite3 3.51.0 (autoinc-1.5): `table
        // sqlite_sequence may not be dropped`.
        if crate::autoincrement::is_sqlite_sequence_table(&stmt.table_name) {
            return Err(ExecutorError::SqliteCompatError(
                "table sqlite_sequence may not be dropped".to_string(),
            ));
        }

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

        // Remember whether this is an AUTOINCREMENT table (and its declared-
        // case display name) before it's removed from the catalog, so its
        // `sqlite_sequence` row can be cleaned up after the drop succeeds.
        // SQLite: dropping an AUTOINCREMENT table removes its entry from
        // `sqlite_sequence`, but `sqlite_sequence` itself stays behind
        // (autoinc-3.2/3.3/3.4, issue #6173).
        let autoincrement_display_name = database
            .catalog
            .get_table(&stmt.table_name)
            .filter(|schema| schema.is_autoincrement)
            .map(|schema| schema.name.clone());

        // Drop all indexes associated with this table first
        let dropped_indexes = database.catalog.drop_table_indexes(&stmt.table_name);
        let index_count = dropped_indexes.len();

        // Drop physical indexes from storage. Use the schema-qualified name so a
        // temp-table index and a same-named main-table index are dropped
        // independently — the storage index manager is schema-aware (#5540), and
        // a bare name would resolve temp-shadows-main and could drop the wrong
        // one.
        for index in &dropped_indexes {
            let qualified = format!("{}.{}", index.schema(), index.name);
            // Try to drop from B-tree storage (ignore errors if not found)
            let _ = database.drop_index(&qualified);
            // Try to drop from spatial storage (ignore errors if not found).
            // Spatial indexes are schema-aware (#5558), so use the
            // schema-qualified name to drop exactly this index and leave a
            // same-named index in another schema intact.
            let _ = database.drop_spatial_index(&qualified);
        }

        // Cascade-drop triggers defined ON this table, matching sqlite3 3.51.0:
        // `DROP TABLE t` removes every trigger whose `ON t` target is the dropped
        // table (temp or main). Must run while the table still exists in the
        // catalog so the trigger's schema binding can resolve. A trigger that only
        // *references* the table from its body, and a view referencing the table,
        // are intentionally left alone (sqlite3 leaves those — the view errors on
        // use). See `Catalog::drop_table_triggers`.
        let dropped_triggers = database.catalog.drop_table_triggers(&stmt.table_name);
        let trigger_count = dropped_triggers.len();

        // Drop the table from storage (this also removes from catalog)
        database
            .drop_table(&stmt.table_name)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        if let Some(display_name) = autoincrement_display_name {
            crate::autoincrement::remove_sequence_entry(database, &display_name)?;
        }

        // Return success message
        match (index_count, trigger_count) {
            (0, 0) => Ok(format!("Table '{}' dropped successfully", stmt.table_name)),
            (i, 0) => Ok(format!(
                "Table '{}' and {} associated index(es) dropped successfully",
                stmt.table_name, i
            )),
            (0, t) => Ok(format!(
                "Table '{}' and {} associated trigger(s) dropped successfully",
                stmt.table_name, t
            )),
            (i, t) => Ok(format!(
                "Table '{}', {} associated index(es), and {} associated trigger(s) dropped successfully",
                stmt.table_name, i, t
            )),
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
            temporary: false,
            if_not_exists: false,
            table_name: "users".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            }],
            table_constraints: vec![],
            table_options: vec![],
            quoted: false,
            name_source: None,
            as_query: None,
            without_rowid: false,
            strict: false,
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
        assert!(db.catalog.table_exists("users"));

        // Now drop it
        let drop_stmt =
            DropTableStmt { table_name: "users".to_string(), if_exists: false, quoted: false };

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

        let drop_stmt = DropTableStmt {
            table_name: "nonexistent".to_string(),
            if_exists: false,
            quoted: false,
        };

        let result = DropTableExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::TableNotFound(_))));
    }

    #[test]
    fn test_drop_nonexistent_table_with_if_exists() {
        let mut db = Database::new();

        let drop_stmt =
            DropTableStmt { table_name: "nonexistent".to_string(), if_exists: true, quoted: false };

        let result = DropTableExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Table 'nonexistent' does not exist (IF EXISTS specified)");
    }

    #[test]
    fn test_drop_existing_table_with_if_exists() {
        let mut db = Database::new();

        // Create a table first
        let create_stmt = CreateTableStmt {
            temporary: false,
            if_not_exists: false,
            table_name: "products".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            }],
            table_constraints: vec![],
            table_options: vec![],
            quoted: false,
            name_source: None,
            as_query: None,
            without_rowid: false,
            strict: false,
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Drop it with IF EXISTS
        let drop_stmt =
            DropTableStmt { table_name: "products".to_string(), if_exists: true, quoted: false };

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
            temporary: false,
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
                    generated_expr: None,
                    is_exact_integer_type: false,
                    type_source: None,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Varchar { max_length: Some(100) },
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
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Insert some data
        use vibesql_storage::Row;
        use vibesql_types::SqlValue;
        let row =
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]);
        db.insert_row("customers", row).unwrap();

        // Verify data exists
        assert_eq!(db.get_table("customers").unwrap().row_count(), 1);

        // Drop the table
        let drop_stmt =
            DropTableStmt { table_name: "customers".to_string(), if_exists: false, quoted: false };

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
            temporary: false,
            if_not_exists: false,
            table_name: "temp".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            }],
            table_constraints: vec![],
            table_options: vec![],
            quoted: false,
            name_source: None,
            as_query: None,
            without_rowid: false,
            strict: false,
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Drop it
        let drop_stmt =
            DropTableStmt { table_name: "temp".to_string(), if_exists: false, quoted: false };
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
                temporary: false,
                if_not_exists: false,
                table_name: name.to_string(),
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                    generated_expr: None,
                    is_exact_integer_type: false,
                    type_source: None,
                }],
                table_constraints: vec![],
                table_options: vec![],
                quoted: false,
                name_source: None,
                as_query: None,
                without_rowid: false,
                strict: false,
            };
            CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
        }

        assert_eq!(db.list_tables().len(), 3);

        // Drop them one by one
        for name in &["table1", "table2", "table3"] {
            let drop_stmt =
                DropTableStmt { table_name: name.to_string(), if_exists: false, quoted: false };
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
            temporary: false,
            if_not_exists: false,
            table_name: "MyTable".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            }],
            table_constraints: vec![],
            table_options: vec![],
            quoted: false,
            name_source: None,
            as_query: None,
            without_rowid: false,
            strict: false,
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Try to drop with exact case - should succeed
        let drop_stmt =
            DropTableStmt { table_name: "MyTable".to_string(), if_exists: false, quoted: false };
        let result = DropTableExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());
    }

    #[test]
    fn test_drop_table_cascades_to_indexes() {
        use vibesql_ast::{CreateIndexStmt, IndexColumn, OrderDirection};

        use crate::CreateIndexExecutor;

        let mut db = Database::new();

        // Create table
        let create_stmt = CreateTableStmt {
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
            ],
            table_constraints: vec![],
            table_options: vec![],
            quoted: false,
            name_source: None,
            as_query: None,
            without_rowid: false,
            strict: false,
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Create indexes on the table
        let index1_stmt = CreateIndexStmt {
            index_name: "idx_users_email".to_string(),
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
        CreateIndexExecutor::execute(&index1_stmt, &mut db).unwrap();

        let index2_stmt = CreateIndexStmt {
            index_name: "idx_users_id".to_string(),
            if_not_exists: false,
            table_name: "users".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::Column {
                column_name: "id".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
                collation: None,
            }],
            where_clause: None,
        };
        CreateIndexExecutor::execute(&index2_stmt, &mut db).unwrap();

        // Verify indexes exist
        assert!(db.index_exists("idx_users_email"));
        assert!(db.index_exists("idx_users_id"));

        // Drop the table
        let drop_stmt =
            DropTableStmt { table_name: "users".to_string(), if_exists: false, quoted: false };
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
        use vibesql_ast::{CreateIndexStmt, IndexColumn, OrderDirection};

        use crate::CreateIndexExecutor;

        let mut db = Database::new();

        // Create table
        let create_stmt = CreateTableStmt {
            temporary: false,
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
                    generated_expr: None,
                    is_exact_integer_type: false,
                    type_source: None,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Varchar { max_length: Some(100) },
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
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Create index
        let index_stmt = CreateIndexStmt {
            index_name: "idx_products_name".to_string(),
            if_not_exists: false,
            table_name: "products".to_string(),
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns: vec![IndexColumn::Column {
                column_name: "name".to_string(),
                prefix_length: None,
                direction: OrderDirection::Asc,
                collation: None,
            }],
            where_clause: None,
        };
        CreateIndexExecutor::execute(&index_stmt, &mut db).unwrap();

        // Verify index exists
        assert!(db.index_exists("idx_products_name"));

        // Drop the table (should cascade to drop index)
        let drop_stmt =
            DropTableStmt { table_name: "products".to_string(), if_exists: false, quoted: false };
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
            temporary: false,
            if_not_exists: false,
            table_name: "simple_table".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            }],
            table_constraints: vec![],
            table_options: vec![],
            quoted: false,
            name_source: None,
            as_query: None,
            without_rowid: false,
            strict: false,
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Drop table without indexes - should still work
        let drop_stmt = DropTableStmt {
            table_name: "simple_table".to_string(),
            if_exists: false,
            quoted: false,
        };
        let result = DropTableExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok(), "Dropping table without indexes should still work");
        assert!(!db.catalog.table_exists("simple_table"));
    }
}
