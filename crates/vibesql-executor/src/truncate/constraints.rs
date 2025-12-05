//! Constraint validation and foreign key handling

use vibesql_storage::Database;

use crate::errors::ExecutorError;
use crate::truncate_validation::can_use_truncate;

/// Get all tables that have foreign keys referencing the given table
///
/// Returns a list of table names that directly reference the parent table.
///
/// # Arguments
///
/// * `database` - The database containing the tables
/// * `parent_table` - The table being referenced
///
/// # Returns
///
/// Vector of table names that reference the parent table
pub fn get_fk_children(
    database: &Database,
    parent_table: &str,
) -> Result<Vec<String>, ExecutorError> {
    let mut children = Vec::new();

    // Scan all tables to find foreign keys that reference this table
    for table_name in database.catalog.list_tables() {
        let child_schema = database
            .catalog
            .get_table(&table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

        for fk in &child_schema.foreign_keys {
            if fk.parent_table == parent_table && table_name != parent_table {
                children.push(table_name.clone());
                break; // Only add each child once
            }
        }
    }

    Ok(children)
}

/// Validate that TRUNCATE can be used on a table
///
/// In RESTRICT mode (default), TRUNCATE cannot be used on tables with DELETE triggers
/// or tables referenced by foreign keys.
///
/// # Arguments
///
/// * `database` - The database containing the table
/// * `table_name` - The table to validate
///
/// # Errors
///
/// Returns error if table has DELETE triggers or is referenced by foreign keys
pub fn validate_truncate_allowed(
    database: &Database,
    table_name: &str,
) -> Result<(), ExecutorError> {
    if !can_use_truncate(database, table_name)? {
        return Err(ExecutorError::Other(format!(
            "Cannot TRUNCATE table '{}': table has DELETE triggers or is referenced by foreign keys",
            table_name
        )));
    }
    Ok(())
}
