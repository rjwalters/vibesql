//! Read-only query execution for concurrent access.
//!
//! This module provides a `query(&self)` method that enables read-only SQL queries
//! on an immutable database reference. This enables concurrent read queries when
//! using `SharedDatabase` from the server module.
//!
//! ## Usage
//!
//! ```text
//! use vibesql_executor::readonly::ReadOnlyQuery;
//! use vibesql_storage::Database;
//!
//! let db = Database::new();
//! // ... set up tables and data ...
//!
//! // Execute read-only query without requiring &mut self
//! let result = db.query("SELECT * FROM users WHERE id = 1")?;
//! println!("Found {} rows", result.rows.len());
//! ```
//!
//! ## Thread Safety
//!
//! The `query()` method takes `&self`, enabling concurrent access when the database
//! is wrapped in `Arc<RwLock<Database>>`. Multiple readers can execute SELECT queries
//! simultaneously using `db.read().await`, while writers use `db.write().await`.
//!
//! ## Error Handling
//!
//! The `query()` method only accepts SELECT statements. Any other statement type
//! (INSERT, UPDATE, DELETE, DDL) returns a `ReadOnlyError::NotReadOnly` error.

use crate::errors::ExecutorError;
use crate::select::{SelectExecutor, SelectResult};
use vibesql_ast::Statement;
use vibesql_storage::Database;

/// Error type for read-only query operations.
#[derive(Debug)]
pub enum ReadOnlyError {
    /// The query is not a read-only SELECT statement
    NotReadOnly { statement_type: String },
    /// SQL parsing failed
    ParseError(String),
    /// Execution failed
    ExecutionError(ExecutorError),
}

impl std::fmt::Display for ReadOnlyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadOnlyError::NotReadOnly { statement_type } => {
                write!(
                    f,
                    "{} is not allowed in read-only mode. Only SELECT queries are permitted.",
                    statement_type
                )
            }
            ReadOnlyError::ParseError(msg) => write!(f, "SQL parse error: {}", msg),
            ReadOnlyError::ExecutionError(e) => write!(f, "Execution error: {:?}", e),
        }
    }
}

impl std::error::Error for ReadOnlyError {}

impl From<ExecutorError> for ReadOnlyError {
    fn from(e: ExecutorError) -> Self {
        ReadOnlyError::ExecutionError(e)
    }
}

/// Extension trait for read-only query execution on `Database`.
///
/// This trait provides a `query(&self)` method that enables executing read-only
/// SQL queries without requiring mutable access to the database. This is essential
/// for concurrent read access in multi-connection scenarios.
pub trait ReadOnlyQuery {
    /// Execute a read-only SQL query.
    ///
    /// This method parses the SQL string and executes it if it's a SELECT statement.
    /// Any other statement type (INSERT, UPDATE, DELETE, DDL) will return an error.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL query string to execute
    ///
    /// # Returns
    ///
    /// * `Ok(SelectResult)` - The query results including column names and rows
    /// * `Err(ReadOnlyError::NotReadOnly)` - If the SQL is not a SELECT statement
    /// * `Err(ReadOnlyError::ParseError)` - If the SQL cannot be parsed
    /// * `Err(ReadOnlyError::ExecutionError)` - If the query execution fails
    ///
    /// # Example
    ///
    /// ```text
    /// use vibesql_executor::readonly::ReadOnlyQuery;
    ///
    /// let db = Database::new();
    /// // ... create tables and insert data ...
    ///
    /// // Read-only query works with &self (no mutation)
    /// let result = db.query("SELECT * FROM users")?;
    /// for row in &result.rows {
    ///     println!("{:?}", row);
    /// }
    ///
    /// // DML queries are rejected
    /// let err = db.query("INSERT INTO users VALUES (1, 'test')");
    /// assert!(matches!(err, Err(ReadOnlyError::NotReadOnly { .. })));
    /// ```
    fn query(&self, sql: &str) -> Result<SelectResult, ReadOnlyError>;
}

impl ReadOnlyQuery for Database {
    fn query(&self, sql: &str) -> Result<SelectResult, ReadOnlyError> {
        // Parse the SQL
        let statement = vibesql_parser::Parser::parse_sql(sql)
            .map_err(|e| ReadOnlyError::ParseError(format!("{:?}", e)))?;

        // Only allow SELECT statements
        match &statement {
            Statement::Select(select_stmt) => {
                let executor = SelectExecutor::new(self);
                executor
                    .execute_with_columns(select_stmt.as_ref())
                    .map_err(ReadOnlyError::from)
            }
            Statement::Insert(_) => Err(ReadOnlyError::NotReadOnly {
                statement_type: "INSERT".to_string(),
            }),
            Statement::Update(_) => Err(ReadOnlyError::NotReadOnly {
                statement_type: "UPDATE".to_string(),
            }),
            Statement::Delete(_) => Err(ReadOnlyError::NotReadOnly {
                statement_type: "DELETE".to_string(),
            }),
            Statement::CreateTable(_) => Err(ReadOnlyError::NotReadOnly {
                statement_type: "CREATE TABLE".to_string(),
            }),
            Statement::DropTable(_) => Err(ReadOnlyError::NotReadOnly {
                statement_type: "DROP TABLE".to_string(),
            }),
            Statement::CreateIndex(_) => Err(ReadOnlyError::NotReadOnly {
                statement_type: "CREATE INDEX".to_string(),
            }),
            Statement::DropIndex(_) => Err(ReadOnlyError::NotReadOnly {
                statement_type: "DROP INDEX".to_string(),
            }),
            Statement::CreateView(_) => Err(ReadOnlyError::NotReadOnly {
                statement_type: "CREATE VIEW".to_string(),
            }),
            Statement::DropView(_) => Err(ReadOnlyError::NotReadOnly {
                statement_type: "DROP VIEW".to_string(),
            }),
            Statement::AlterTable(_) => Err(ReadOnlyError::NotReadOnly {
                statement_type: "ALTER TABLE".to_string(),
            }),
            Statement::TruncateTable(_) => Err(ReadOnlyError::NotReadOnly {
                statement_type: "TRUNCATE".to_string(),
            }),
            Statement::BeginTransaction(_) => Err(ReadOnlyError::NotReadOnly {
                statement_type: "BEGIN TRANSACTION".to_string(),
            }),
            Statement::Commit(_) => Err(ReadOnlyError::NotReadOnly {
                statement_type: "COMMIT".to_string(),
            }),
            Statement::Rollback(_) => Err(ReadOnlyError::NotReadOnly {
                statement_type: "ROLLBACK".to_string(),
            }),
            _ => {
                // Catch-all for other statement types
                Err(ReadOnlyError::NotReadOnly {
                    statement_type: format!("{:?}", std::mem::discriminant(&statement)),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_storage::Row;
    use vibesql_types::{DataType, SqlValue};

    fn create_test_db() -> Database {
        let mut db = Database::new();
        db.catalog.set_case_sensitive_identifiers(false);

        // Create users table
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ];
        let schema =
            TableSchema::with_primary_key("users".to_string(), columns, vec!["id".to_string()]);
        db.create_table(schema).unwrap();

        // Insert test data
        let row1 = Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]);
        let row2 = Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
        ]);
        let row3 = Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
        ]);

        db.insert_row("users", row1).unwrap();
        db.insert_row("users", row2).unwrap();
        db.insert_row("users", row3).unwrap();

        db
    }

    #[test]
    fn test_query_select_all() {
        let db = create_test_db();

        let result = db.query("SELECT * FROM users").unwrap();
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.columns.len(), 2);
    }

    #[test]
    fn test_query_select_with_where() {
        let db = create_test_db();

        let result = db.query("SELECT * FROM users WHERE id = 1").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], SqlValue::Integer(1));
        assert_eq!(
            result.rows[0].values[1],
            SqlValue::Varchar(arcstr::ArcStr::from("Alice"))
        );
    }

    #[test]
    fn test_query_select_specific_columns() {
        let db = create_test_db();

        let result = db.query("SELECT name FROM users WHERE id = 2").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0].to_lowercase(), "name");
        assert_eq!(
            result.rows[0].values[0],
            SqlValue::Varchar(arcstr::ArcStr::from("Bob"))
        );
    }

    #[test]
    fn test_query_select_count() {
        let db = create_test_db();

        let result = db.query("SELECT COUNT(*) FROM users").unwrap();
        assert_eq!(result.rows.len(), 1);
        // COUNT returns Integer (3) in this implementation
        assert_eq!(result.rows[0].values[0], SqlValue::Integer(3));
    }

    #[test]
    fn test_query_rejects_insert() {
        let db = create_test_db();

        let result = db.query("INSERT INTO users (id, name) VALUES (4, 'David')");
        assert!(matches!(
            result,
            Err(ReadOnlyError::NotReadOnly { statement_type }) if statement_type == "INSERT"
        ));
    }

    #[test]
    fn test_query_rejects_update() {
        let db = create_test_db();

        let result = db.query("UPDATE users SET name = 'Alicia' WHERE id = 1");
        assert!(matches!(
            result,
            Err(ReadOnlyError::NotReadOnly { statement_type }) if statement_type == "UPDATE"
        ));
    }

    #[test]
    fn test_query_rejects_delete() {
        let db = create_test_db();

        let result = db.query("DELETE FROM users WHERE id = 1");
        assert!(matches!(
            result,
            Err(ReadOnlyError::NotReadOnly { statement_type }) if statement_type == "DELETE"
        ));
    }

    #[test]
    fn test_query_rejects_create_table() {
        let db = create_test_db();

        let result = db.query("CREATE TABLE test (id INT)");
        assert!(matches!(
            result,
            Err(ReadOnlyError::NotReadOnly { statement_type }) if statement_type == "CREATE TABLE"
        ));
    }

    #[test]
    fn test_query_rejects_drop_table() {
        let db = create_test_db();

        let result = db.query("DROP TABLE users");
        assert!(matches!(
            result,
            Err(ReadOnlyError::NotReadOnly { statement_type }) if statement_type == "DROP TABLE"
        ));
    }

    #[test]
    fn test_query_rejects_truncate() {
        let db = create_test_db();

        let result = db.query("TRUNCATE TABLE users");
        assert!(matches!(
            result,
            Err(ReadOnlyError::NotReadOnly { statement_type }) if statement_type == "TRUNCATE"
        ));
    }

    #[test]
    fn test_query_parse_error() {
        let db = create_test_db();

        let result = db.query("SELEKT * FROM users");
        assert!(matches!(result, Err(ReadOnlyError::ParseError(_))));
    }

    #[test]
    fn test_query_execution_error_table_not_found() {
        let db = create_test_db();

        let result = db.query("SELECT * FROM nonexistent");
        assert!(matches!(result, Err(ReadOnlyError::ExecutionError(_))));
    }

    #[test]
    fn test_query_with_order_by() {
        let db = create_test_db();

        let result = db.query("SELECT * FROM users ORDER BY id DESC").unwrap();
        assert_eq!(result.rows.len(), 3);
        // First row should be id=3 (Charlie)
        assert_eq!(result.rows[0].values[0], SqlValue::Integer(3));
        // Last row should be id=1 (Alice)
        assert_eq!(result.rows[2].values[0], SqlValue::Integer(1));
    }

    #[test]
    fn test_query_with_limit() {
        let db = create_test_db();

        let result = db.query("SELECT * FROM users LIMIT 2").unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_query_with_aggregation() {
        let db = create_test_db();

        let result = db
            .query("SELECT COUNT(*), MAX(id), MIN(id) FROM users")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        // COUNT returns Integer in this implementation
        assert_eq!(result.rows[0].values[0], SqlValue::Integer(3)); // COUNT(*)
        assert_eq!(result.rows[0].values[1], SqlValue::Integer(3)); // MAX(id)
        assert_eq!(result.rows[0].values[2], SqlValue::Integer(1)); // MIN(id)
    }

    #[test]
    fn test_query_immutability() {
        let db = create_test_db();

        // Execute multiple queries on the same &db reference
        let result1 = db.query("SELECT COUNT(*) FROM users").unwrap();
        let result2 = db.query("SELECT * FROM users WHERE id = 1").unwrap();
        let result3 = db.query("SELECT name FROM users").unwrap();

        // All queries should work and return expected results
        // COUNT returns Integer in this implementation
        assert_eq!(result1.rows[0].values[0], SqlValue::Integer(3));
        assert_eq!(result2.rows.len(), 1);
        assert_eq!(result3.rows.len(), 3);
    }
}
