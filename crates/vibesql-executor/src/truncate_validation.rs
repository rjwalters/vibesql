//! Shared validation logic for TRUNCATE optimization
//!
//! This module provides validation functions used by both TRUNCATE TABLE
//! and DELETE executors to determine if the TRUNCATE optimization can be
//! safely used.

use crate::ExecutorError;
use vibesql_storage::Database;

/// Check if TRUNCATE optimization can be used for a table
///
/// TRUNCATE cannot be used if:
/// - Table has DELETE triggers (BEFORE/AFTER DELETE)
/// - Table is referenced by foreign keys from other tables (without CASCADE)
///
/// # Returns
/// - `Ok(true)` if TRUNCATE can be safely used
/// - `Ok(false)` if row-by-row deletion is required
/// - `Err` if table doesn't exist
pub(crate) fn can_use_truncate(
    database: &Database,
    table_name: &str,
) -> Result<bool, ExecutorError> {
    // Check for DELETE triggers on this table
    if has_delete_triggers(database, table_name) {
        return Ok(false);
    }

    // Check if this table is referenced by foreign keys from other tables
    if is_fk_referenced(database, table_name)? {
        return Ok(false);
    }

    Ok(true)
}

/// Check if a table has any DELETE triggers
pub(crate) fn has_delete_triggers(database: &Database, table_name: &str) -> bool {
    database
        .catalog
        .get_triggers_for_table(table_name, Some(vibesql_ast::TriggerEvent::Delete))
        .next()
        .is_some()
}

/// Check if a table is referenced by foreign keys from other tables
///
/// Returns true if any other table has a foreign key constraint referencing this table.
/// When this is true, we cannot use TRUNCATE because we need to check each row
/// for child references.
pub(crate) fn is_fk_referenced(
    database: &Database,
    parent_table_name: &str,
) -> Result<bool, ExecutorError> {
    // Scan all tables to find foreign keys that reference this table
    for table_name in database.catalog.list_tables() {
        let child_schema = database
            .catalog
            .get_table(&table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

        for fk in &child_schema.foreign_keys {
            if fk.parent_table == parent_table_name {
                return Ok(true); // Found a reference
            }
        }
    }

    Ok(false) // No references found
}
