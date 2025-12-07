//! Core truncate logic and execution

use std::collections::HashSet;
use vibesql_storage::Database;

use crate::errors::ExecutorError;
use crate::privilege_checker::PrivilegeChecker;

use super::constraints::get_fk_children;
use super::triggers::validate_no_delete_triggers;

/// Reset AUTO_INCREMENT sequences for a table
///
/// Finds all AUTO_INCREMENT columns in the table and resets their associated sequences
/// to the initial value (1).
pub fn reset_auto_increment_sequences(
    database: &mut Database,
    table_name: &str,
) -> Result<(), ExecutorError> {
    // Get table schema to find AUTO_INCREMENT columns
    let table_schema = database
        .catalog
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    // Collect sequence names first to avoid borrow checker issues
    // (we can't hold an immutable reference to table_schema while mutating sequences)
    let mut sequence_names = Vec::new();
    for column in &table_schema.columns {
        if let Some(vibesql_ast::Expression::NextValue { sequence_name }) = &column.default_value {
            sequence_names.push(sequence_name.clone());
        }
    }

    // Now reset the sequences
    for sequence_name in sequence_names {
        if let Ok(sequence) = database.catalog.get_sequence_mut(&sequence_name) {
            // Reset to start value (None means use the original start_with value)
            sequence.restart(None);
        }
        // Note: If sequence doesn't exist, we silently continue.
        // This shouldn't happen in normal operation but makes the function more robust.
    }

    Ok(())
}

/// Execute TRUNCATE operation
///
/// Clears all rows and indexes in a single operation.
/// Provides significant performance improvement over row-by-row deletion.
/// Also resets any AUTO_INCREMENT sequences to their initial values.
pub fn execute_truncate(database: &mut Database, table_name: &str) -> Result<usize, ExecutorError> {
    let table = database
        .get_table_mut(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let row_count = table.row_count();

    // Clear all data at once (O(1) operation)
    table.clear();

    // Rebuild user-defined B-tree indexes (clears them since table is now empty)
    // This ensures auto-created indexes for PK/UNIQUE constraints are also cleared
    database.rebuild_indexes(table_name);

    // Invalidate the database-level columnar cache since table data changed.
    // Note: table.clear() invalidates the table-level cache.
    // Both invalidations are necessary because they manage separate caches:
    // - Table-level cache: used by Table::scan_columnar() for SIMD filtering
    // - Database-level cache: used by Database::get_columnar() for cached access
    if row_count > 0 {
        database.invalidate_columnar_cache(table_name);
    }

    // Reset AUTO_INCREMENT sequences
    reset_auto_increment_sequences(database, table_name)?;

    // Invalidate the database-level columnar cache since table data changed.
    // Note: The table-level cache is already invalidated by table.clear().
    // Both invalidations are necessary because they manage separate caches:
    // - Table-level cache: used by Table::scan_columnar() for SIMD filtering
    // - Database-level cache: used by Database::get_columnar() for cached access
    database.invalidate_columnar_cache(table_name);

    Ok(row_count)
}

/// Execute TRUNCATE CASCADE operation
///
/// Recursively truncates all tables that reference the target table via foreign keys.
/// Uses topological sort to determine truncation order (children first).
///
/// # Arguments
///
/// * `database` - The database containing the tables
/// * `table_name` - The root table to truncate
///
/// # Returns
///
/// Total number of rows deleted across all tables
///
/// # Errors
///
/// Returns error if:
/// - Any table in the dependency chain has DELETE triggers
/// - User lacks DELETE privilege on any affected table
/// - Circular foreign key dependencies are detected
pub fn execute_truncate_cascade(
    database: &mut Database,
    table_name: &str,
) -> Result<usize, ExecutorError> {
    // Phase 1: Collect all dependent tables in topological order
    let truncate_order = collect_fk_dependencies(database, table_name)?;

    // Phase 2: Validate all tables can be truncated (no DELETE triggers)
    // and check DELETE privilege on all tables
    for tbl in &truncate_order {
        validate_no_delete_triggers(database, tbl, table_name)?;
        PrivilegeChecker::check_delete(database, tbl)?;
    }

    // Phase 3: Execute truncates in order (children first, then parents)
    let mut total_rows_deleted = 0;
    for tbl in truncate_order {
        let rows_deleted = execute_truncate(database, &tbl)?;
        total_rows_deleted += rows_deleted;
    }

    Ok(total_rows_deleted)
}

/// Collect all tables that need to be truncated via CASCADE
///
/// Uses topological sort to determine the correct truncation order.
/// Returns tables in the order they should be truncated (children before parents).
///
/// # Arguments
///
/// * `database` - The database containing the tables
/// * `root_table` - The root table to start from
///
/// # Returns
///
/// Vector of table names in truncation order (children first)
///
/// # Errors
///
/// Returns error if circular foreign key dependencies are detected
fn collect_fk_dependencies(
    database: &Database,
    root_table: &str,
) -> Result<Vec<String>, ExecutorError> {
    let mut visited = HashSet::new();
    let mut order = Vec::new();
    let mut recursion_stack = HashSet::new();

    // Recursive DFS to collect dependencies
    fn visit(
        database: &Database,
        table_name: &str,
        visited: &mut HashSet<String>,
        order: &mut Vec<String>,
        recursion_stack: &mut HashSet<String>,
    ) -> Result<(), ExecutorError> {
        // Detect cycles
        if recursion_stack.contains(table_name) {
            return Err(ExecutorError::Other(format!(
                "Circular foreign key dependency detected involving table '{}'",
                table_name
            )));
        }

        if visited.contains(table_name) {
            return Ok(());
        }

        recursion_stack.insert(table_name.to_string());
        visited.insert(table_name.to_string());

        // Find all tables that reference this table (children)
        let children = get_fk_children(database, table_name)?;
        for child in children {
            visit(database, &child, visited, order, recursion_stack)?;
        }

        recursion_stack.remove(table_name);
        order.push(table_name.to_string());

        Ok(())
    }

    visit(database, root_table, &mut visited, &mut order, &mut recursion_stack)?;

    Ok(order)
}
