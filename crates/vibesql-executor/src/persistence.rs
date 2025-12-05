//! Database Persistence - Loading SQL Dumps
//!
//! Provides functionality for loading databases from SQL dump files.
//! This mirrors the save functionality in vibesql-storage but requires
//! the executor layer since it needs to parse and execute SQL statements.

use std::path::Path;

use vibesql_storage::Database;

use crate::{
    CreateIndexExecutor, CreateTableExecutor, ExecutorError, InsertExecutor, RoleExecutor,
    SchemaExecutor, UpdateExecutor, ViewExecutor,
};

/// Load database from SQL dump file
///
/// Reads SQL dump, parses statements, and executes them to recreate database state.
/// This is the shared implementation used by CLI, Python bindings, and other consumers.
///
/// # Arguments
/// * `path` - Path to the SQL dump file
///
/// # Returns
/// A new Database instance with the loaded state
///
/// # Errors
/// Returns error if:
/// - File cannot be read
/// - File is not valid SQL dump format (e.g., binary SQLite file)
/// - SQL parsing fails
/// - Statement execution fails
///
/// # Example
/// ```no_run
/// # use vibesql_executor::load_sql_dump;
/// let db = load_sql_dump("database.sql").unwrap();
/// ```
pub fn load_sql_dump<P: AsRef<Path>>(path: P) -> Result<Database, ExecutorError> {
    // Read the SQL dump file using storage utility
    let sql_content = vibesql_storage::read_sql_dump(&path).map_err(|e| {
        ExecutorError::Other(format!("Failed to read database file {:?}: {}", path.as_ref(), e))
    })?;

    // Split into individual statements using storage utility
    let statements = vibesql_storage::parse_sql_statements(&sql_content)
        .map_err(|e| ExecutorError::Other(format!("Failed to parse SQL dump: {}", e)))?;

    // Create a new database to populate
    let mut db = Database::new();

    // Execute each statement
    for (idx, stmt_sql) in statements.iter().enumerate() {
        // Skip empty statements and comments
        let trimmed = stmt_sql.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }

        // Parse the statement
        let statement = vibesql_parser::Parser::parse_sql(trimmed).map_err(|e| {
            ExecutorError::Other(format!(
                "Failed to parse statement {} in {:?}: {}\nStatement: {}",
                idx + 1,
                path.as_ref(),
                e,
                truncate_for_error(trimmed, 100)
            ))
        })?;

        // Execute the statement
        execute_statement_for_load(&mut db, statement).map_err(|e| {
            ExecutorError::Other(format!(
                "Failed to execute statement {} in {:?}: {}\nStatement: {}",
                idx + 1,
                path.as_ref(),
                e,
                truncate_for_error(trimmed, 100)
            ))
        })?;
    }

    Ok(db)
}

/// Execute a single statement during database load
///
/// Only DDL and INSERT statements are supported during load.
/// Other statement types will return an error.
fn execute_statement_for_load(
    db: &mut Database,
    statement: vibesql_ast::Statement,
) -> Result<(), ExecutorError> {
    match statement {
        vibesql_ast::Statement::CreateSchema(schema_stmt) => {
            SchemaExecutor::execute_create_schema(&schema_stmt, db)?;
        }
        vibesql_ast::Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, db)?;
        }
        vibesql_ast::Statement::CreateIndex(index_stmt) => {
            CreateIndexExecutor::execute(&index_stmt, db)?;
        }
        vibesql_ast::Statement::CreateView(view_stmt) => {
            ViewExecutor::execute_create_view(&view_stmt, db)?;
        }
        vibesql_ast::Statement::CreateRole(role_stmt) => {
            RoleExecutor::execute_create_role(&role_stmt, db)?;
        }
        vibesql_ast::Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, &insert_stmt)?;
        }
        vibesql_ast::Statement::Update(update_stmt) => {
            UpdateExecutor::execute(&update_stmt, db)?;
        }
        _ => {
            return Err(ExecutorError::Other(format!(
                "Statement type not supported in database load: {:?}",
                statement
            )));
        }
    }
    Ok(())
}

/// Truncate a string for error messages
fn truncate_for_error(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    #[test]
    fn test_load_simple_database() {
        // Create a temporary SQL dump file
        let temp_file = "/tmp/test_load_simple.sql";
        let sql_dump = r#"
-- Test database
CREATE TABLE users (id INTEGER, name VARCHAR(50));
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
"#;

        fs::write(temp_file, sql_dump).unwrap();

        // Load the database
        let db = load_sql_dump(temp_file).unwrap();

        // Verify the table exists (note: identifiers are uppercased)
        assert!(db.get_table("USERS").is_some());

        // Verify data was loaded
        let table = db.get_table("USERS").unwrap();
        assert_eq!(table.row_count(), 2);

        // Clean up
        fs::remove_file(temp_file).unwrap();
    }

    #[test]
    fn test_load_with_schema() {
        let temp_file = "/tmp/test_load_schema.sql";
        // Note: Schema-qualified INSERT statements not yet supported by parser
        // So we create the table in a schema but insert without schema qualification
        let sql_dump = r#"
CREATE SCHEMA test_schema;
CREATE TABLE test_schema.products (id INTEGER, price REAL);
"#;

        fs::write(temp_file, sql_dump).unwrap();

        let db = load_sql_dump(temp_file).unwrap();

        // Verify table exists in schema (note: identifiers are uppercased)
        assert!(db.get_table("TEST_SCHEMA.PRODUCTS").is_some());

        fs::remove_file(temp_file).unwrap();
    }

    #[test]
    fn test_load_nonexistent_file() {
        let result = load_sql_dump("/tmp/nonexistent_file.sql");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn test_load_invalid_sql() {
        let temp_file = "/tmp/test_load_invalid.sql";
        fs::write(temp_file, "THIS IS NOT VALID SQL;").unwrap();

        let result = load_sql_dump(temp_file);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to parse"));

        fs::remove_file(temp_file).unwrap();
    }

    #[test]
    fn test_load_binary_file_error() {
        let temp_file = "/tmp/test_load_binary.db";
        let mut file = fs::File::create(temp_file).unwrap();
        file.write_all(b"SQLite format 3\0").unwrap();
        file.write_all(&[0xFF, 0xFE, 0xFD]).unwrap();

        let result = load_sql_dump(temp_file);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("binary SQLite database"));

        fs::remove_file(temp_file).unwrap();
    }

    #[test]
    fn test_load_with_indexes() {
        let temp_file = "/tmp/test_load_indexes.sql";
        let sql_dump = r#"
CREATE TABLE employees (id INTEGER, name VARCHAR(100), dept VARCHAR(50));
INSERT INTO employees VALUES (1, 'Alice', 'Engineering');
INSERT INTO employees VALUES (2, 'Bob', 'Sales');
CREATE INDEX idx_dept ON employees (dept Asc);
"#;

        fs::write(temp_file, sql_dump).unwrap();

        let db = load_sql_dump(temp_file).unwrap();

        // Verify table and data (note: identifiers are uppercased)
        assert!(db.get_table("EMPLOYEES").is_some());
        let table = db.get_table("EMPLOYEES").unwrap();
        assert_eq!(table.row_count(), 2);

        // Verify index exists (note: identifiers are uppercased)
        assert!(db.get_index("IDX_DEPT").is_some());

        fs::remove_file(temp_file).unwrap();
    }

    #[test]
    fn test_load_with_roles() {
        let temp_file = "/tmp/test_load_roles.sql";
        let sql_dump = r#"
CREATE ROLE admin;
CREATE ROLE user;
CREATE TABLE data (id INTEGER);
"#;

        fs::write(temp_file, sql_dump).unwrap();

        let db = load_sql_dump(temp_file).unwrap();

        // Verify table exists (note: identifiers are uppercased)
        assert!(db.get_table("DATA").is_some());

        fs::remove_file(temp_file).unwrap();
    }
}
