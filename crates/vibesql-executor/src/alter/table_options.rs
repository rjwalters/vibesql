//! Table-level operation executors for ALTER TABLE

use vibesql_ast::RenameTableStmt;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Execute RENAME TABLE
pub(super) fn execute_rename_table(
    stmt: &RenameTableStmt,
    database: &mut Database,
) -> Result<String, ExecutorError> {
    // Check if new table name already exists
    if database.get_table(&stmt.new_table_name).is_some() {
        return Err(ExecutorError::TableAlreadyExists(stmt.new_table_name.clone()));
    }

    // Get the old table to ensure it exists
    let old_table = database
        .get_table(&stmt.table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    // Clone the table and update its schema name
    let mut new_table = old_table.clone();
    new_table.schema_mut().name = stmt.new_table_name.clone();

    // Drop old table and create new one with the renamed schema
    // This handles indexes and spatial indexes via CASCADE
    database
        .drop_table(&stmt.table_name)
        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

    database
        .create_table(new_table.schema.clone())
        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

    // Restore the data by getting the new table and setting its rows
    let restored_table = database
        .get_table_mut(&stmt.new_table_name)
        .ok_or_else(|| ExecutorError::TableAlreadyExists(stmt.new_table_name.clone()))?;

    for row in new_table.scan() {
        restored_table
            .insert(row.clone())
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
    }

    // Invalidate the database-level columnar cache for both old and new table names.
    // The old table name's cache is invalidated since the table no longer exists,
    // and the new table name's cache is invalidated to ensure fresh columnar data.
    database.invalidate_columnar_cache(&stmt.table_name);
    database.invalidate_columnar_cache(&stmt.new_table_name);

    Ok(format!("Table '{}' renamed to '{}'", stmt.table_name, stmt.new_table_name))
}
