//! Trigger-related validation and coordination

use vibesql_storage::Database;

use crate::{errors::ExecutorError, truncate_validation::has_delete_triggers};

/// Validate that a table has no DELETE triggers
///
/// Used by both RESTRICT and CASCADE modes to ensure triggers won't be violated.
///
/// # Arguments
///
/// * `database` - The database containing the table
/// * `table_name` - The table to check
/// * `root_table` - The original table being truncated (for error messages)
///
/// # Errors
///
/// Returns error if table has DELETE triggers
pub fn validate_no_delete_triggers(
    database: &Database,
    table_name: &str,
    root_table: &str,
) -> Result<(), ExecutorError> {
    if has_delete_triggers(database, table_name) {
        return Err(ExecutorError::Other(format!(
            "Cannot TRUNCATE CASCADE table '{}': dependent table '{}' has DELETE triggers",
            root_table, table_name
        )));
    }
    Ok(())
}
