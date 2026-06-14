//! ANALYZE statement execution
//!
//! ANALYZE computes or recomputes table and column statistics to improve query plan optimization.
//! This forces statistics to be refreshed, which is useful after bulk data loads or schema changes.
//!
//! ## SQLite Compatibility
//!
//! VibeSQL supports the following ANALYZE syntax for SQLite compatibility:
//!
//! - `ANALYZE` - Analyzes all tables
//! - `ANALYZE table_name` - Analyzes a specific table
//! - `ANALYZE sqlite_master` - No-op for compatibility (prepares sqlite_stat1)
//! - `ANALYZE sqlite_schema` - Same as sqlite_master
//!
//! Unlike SQLite, VibeSQL stores computed statistics internally rather than in sqlite_stat1.
//! The sqlite_stat1 table is available for manual statistics override via INSERT statements.

use vibesql_ast::AnalyzeStmt;
use vibesql_storage::Database;

use crate::errors::ExecutorError;
use crate::sqlite_schema::is_sqlite_schema_table;

/// Executor for ANALYZE statements
pub struct AnalyzeExecutor;

impl AnalyzeExecutor {
    /// Execute an ANALYZE statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The ANALYZE statement AST node
    /// * `database` - The database containing tables to analyze
    ///
    /// # Returns
    ///
    /// Success message indicating how many tables were analyzed
    ///
    /// # Behavior
    ///
    /// - `ANALYZE` with no table: Analyzes all tables in the database
    /// - `ANALYZE table_name`: Analyzes the specified table
    /// - `ANALYZE table_name (cols)`: Analyzes the specified table (column list is currently
    ///   advisory)
    /// - `ANALYZE sqlite_master`: No-op for SQLite compatibility (statistics are ready)
    ///
    /// # Implementation Note
    ///
    /// Currently, when a column list is specified, all columns are analyzed anyway.
    /// This is a conservative approach that ensures all statistics are fresh.
    /// Future optimization: Only compute stats for specified columns.
    pub fn execute(stmt: &AnalyzeStmt, database: &mut Database) -> Result<String, ExecutorError> {
        match &stmt.table_name {
            None => {
                // ANALYZE with no table - analyze all tables
                let table_names = database.list_tables();
                let count = table_names.len();

                for table_name in &table_names {
                    Self::analyze_table_and_update_stats(database, table_name);
                }

                Ok(format!("ANALYZE completed - {} table(s) analyzed", count))
            }
            Some(table_name) => {
                // Special case: ANALYZE sqlite_master or sqlite_schema
                // This is used in SQLite to rebuild sqlite_stat tables
                // For VibeSQL, it's a no-op since sqlite_stat1 is virtual
                if is_sqlite_schema_table(table_name) {
                    return Ok("ANALYZE sqlite_master completed - sqlite_stat1 ready for queries"
                        .to_string());
                }

                // ANALYZE with specific table
                if database.get_table(table_name).is_none() {
                    return Err(ExecutorError::TableNotFound(table_name.clone()));
                }

                Self::analyze_table_and_update_stats(database, table_name);

                let message = if let Some(cols) = &stmt.columns {
                    // Column list specified - note that we analyze all columns anyway
                    format!(
                        "ANALYZE completed - table '{}' analyzed ({} columns specified, all columns analyzed)",
                        table_name,
                        cols.len()
                    )
                } else {
                    format!("ANALYZE completed - table '{}' analyzed", table_name)
                };

                Ok(message)
            }
        }
    }

    /// Analyze a table and update sqlite_stat1 with the computed statistics
    fn analyze_table_and_update_stats(database: &mut Database, table_name: &str) {
        // First, analyze the table to compute fresh statistics
        if let Some(table) = database.get_table_mut(table_name) {
            table.analyze();
        }

        // Get the computed statistics and update sqlite_stat1
        // Note: We only populate sqlite_stat1 with table-level row counts
        // Index statistics are computed on-demand in VibeSQL
        if let Some(table) = database.get_table_mut(table_name) {
            let stats = table.statistics();
            let row_count = stats.row_count;

            // Insert table-level statistics (idx = NULL means table stats)
            database.insert_sqlite_stat1(table_name.to_string(), None, row_count.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{ColumnDef, CreateTableStmt};
    use vibesql_types::{DataType, SqlValue};

    use super::*;
    use crate::CreateTableExecutor;

    fn create_test_table(db: &mut Database, table_name: &str) {
        let stmt = CreateTableStmt {
            temporary: false,
            if_not_exists: false,
            table_name: table_name.to_string(),
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
                },
                ColumnDef {
                    name: "age".to_string(),
                    data_type: DataType::Integer,
                    nullable: true,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                    generated_expr: None,
                    is_exact_integer_type: false,
                },
            ],
            table_constraints: vec![],
            table_options: vec![],
            quoted: false,
            name_source: None,
            as_query: None,
            without_rowid: false,
        };

        CreateTableExecutor::execute(&stmt, db).unwrap();
    }

    fn insert_test_data(db: &mut Database, table_name: &str) {
        use vibesql_storage::Row;

        let table = db.get_table_mut(table_name).unwrap();

        // Insert some test data
        table
            .insert(Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Integer(30),
            ]))
            .unwrap();
        table
            .insert(Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
                SqlValue::Integer(25),
            ]))
            .unwrap();
        table
            .insert(Row::new(vec![
                SqlValue::Integer(3),
                SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
                SqlValue::Integer(35),
            ]))
            .unwrap();
    }

    #[test]
    fn test_analyze_all_tables() {
        let mut db = Database::new();
        create_test_table(&mut db, "users");
        create_test_table(&mut db, "products");
        insert_test_data(&mut db, "users");
        insert_test_data(&mut db, "products");

        // ANALYZE with no target should analyze all tables
        let analyze_stmt = AnalyzeStmt { table_name: None, columns: None };

        let result = AnalyzeExecutor::execute(&analyze_stmt, &mut db);
        assert!(result.is_ok());
        let msg = result.unwrap();
        assert!(msg.contains("2 table(s)"));

        // Verify statistics were computed
        let users_table = db.get_table_mut("users").unwrap();
        let stats = users_table.statistics();
        assert_eq!(stats.row_count, 3);
    }

    #[test]
    fn test_analyze_specific_table() {
        let mut db = Database::new();
        create_test_table(&mut db, "users");
        insert_test_data(&mut db, "users");

        // Analyze the specific table
        let analyze_stmt = AnalyzeStmt { table_name: Some("users".to_string()), columns: None };

        let result = AnalyzeExecutor::execute(&analyze_stmt, &mut db);
        assert!(result.is_ok());
        assert!(result.unwrap().contains("'users'"));

        // Verify statistics were computed
        let table = db.get_table_mut("users").unwrap();
        let stats = table.statistics();
        assert_eq!(stats.row_count, 3);
        assert!(stats.columns.contains_key("id"));
        assert!(stats.columns.contains_key("name"));
        assert!(stats.columns.contains_key("age"));
    }

    #[test]
    fn test_analyze_with_column_list() {
        let mut db = Database::new();
        create_test_table(&mut db, "users");
        insert_test_data(&mut db, "users");

        // Analyze specific columns (currently analyzes all columns)
        let analyze_stmt = AnalyzeStmt {
            table_name: Some("users".to_string()),
            columns: Some(vec!["id".to_string(), "name".to_string()]),
        };

        let result = AnalyzeExecutor::execute(&analyze_stmt, &mut db);
        assert!(result.is_ok());
        let msg = result.unwrap();
        assert!(msg.contains("'users'"));
        assert!(msg.contains("2 columns specified"));

        // Verify all column statistics were computed (not just specified ones)
        let table = db.get_table_mut("users").unwrap();
        let stats = table.statistics();
        assert!(stats.columns.contains_key("id"));
        assert!(stats.columns.contains_key("name"));
        assert!(stats.columns.contains_key("age")); // Even though not specified
    }

    #[test]
    fn test_analyze_empty_table() {
        let mut db = Database::new();
        create_test_table(&mut db, "empty_table");

        // Analyze empty table
        let analyze_stmt =
            AnalyzeStmt { table_name: Some("empty_table".to_string()), columns: None };

        let result = AnalyzeExecutor::execute(&analyze_stmt, &mut db);
        assert!(result.is_ok());

        // Verify statistics show 0 rows
        let table = db.get_table_mut("empty_table").unwrap();
        let stats = table.statistics();
        assert_eq!(stats.row_count, 0);
    }

    #[test]
    fn test_analyze_nonexistent_table() {
        let mut db = Database::new();

        // Try to analyze non-existent table
        let analyze_stmt =
            AnalyzeStmt { table_name: Some("nonexistent".to_string()), columns: None };

        let result = AnalyzeExecutor::execute(&analyze_stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::TableNotFound(_))));
    }

    #[test]
    fn test_analyze_updates_stale_statistics() {
        let mut db = Database::new();
        create_test_table(&mut db, "users");

        // Insert initial data and analyze
        insert_test_data(&mut db, "users");
        let analyze_stmt = AnalyzeStmt { table_name: Some("users".to_string()), columns: None };
        AnalyzeExecutor::execute(&analyze_stmt, &mut db).unwrap();

        {
            let table = db.get_table_mut("users").unwrap();
            let initial_stats = table.statistics();
            assert_eq!(initial_stats.row_count, 3);
        }

        // Insert more data
        {
            use vibesql_storage::Row;
            let table = db.get_table_mut("users").unwrap();
            table
                .insert(Row::new(vec![
                    SqlValue::Integer(4),
                    SqlValue::Varchar(arcstr::ArcStr::from("Diana")),
                    SqlValue::Integer(28),
                ]))
                .unwrap();
        }

        // Re-analyze
        AnalyzeExecutor::execute(&analyze_stmt, &mut db).unwrap();

        // Verify statistics were updated
        {
            let table = db.get_table_mut("users").unwrap();
            let updated_stats = table.statistics();
            assert_eq!(updated_stats.row_count, 4);
        }
    }
}
